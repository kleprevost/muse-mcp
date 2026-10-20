"""Thin, serialized adapter from MCP calls to the existing Muse gateway."""

import json
import re
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional

from cli import is_reply
from muse import AuthError, Gateway, GatewayError, ROUTES, VMUnavailableError, load_cookies

from .config import load_config, resolve_thread

try:
    from curl_cffi import CurlError
except (ImportError, OSError):  # pragma: no cover - mirrors muse.py's optional import
    class CurlError(Exception):
        """Placeholder when curl_cffi is unavailable."""


# Seconds to wait before each reconnect attempt after a transient failure.
RETRY_DELAYS = (1.0, 3.0)
_TRANSIENT_STATUSES = (502, 503, 504)


_SECRET_KEY = re.compile(
    r"(?:token|secret|cookie|password|credential|authorization|private_key|api[_-]?key)",
    re.IGNORECASE,
)
_SECRET_TEXT = re.compile(
    r'(?i)("?(?:access_token|auth_token|hatch_token|api[_-]?key|cookie|password|secret)"?\s*:\s*")([^"\\]*(?:\\.[^"\\]*)*)(")'
)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)((?:access_token|auth_token|hatch_token|api[_-]?key|cookie|password|secret)\s*[=:]\s*)[^\s&,;]+"
)
_BEARER = re.compile(r"(?i)(Bearer\s+)[^\s,;]+")


class MCPInputError(ValueError):
    """An invalid MCP argument that should be returned as structured data."""


def _redact_text(value: str) -> str:
    value = _BEARER.sub(r"\1[REDACTED]", value)
    value = _SECRET_ASSIGNMENT.sub(r"\1[REDACTED]", value)
    return _SECRET_TEXT.sub(r"\1[REDACTED]\3", value)


def redact(value: Any) -> Any:
    """Redact credential-shaped fields in data returned by an internal route."""
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if _SECRET_KEY.search(str(key)) else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return _redact_text(value)
    return value


def _error(code: str, message: str, **extra: Any) -> Dict[str, Any]:
    result: Dict[str, Any] = {"ok": False, "error": {"code": code, "message": message}}
    result["error"].update(extra)
    return result


def _auth_error() -> Dict[str, Any]:
    result = _error(
        "AUTH_REQUIRED",
        "Muse authentication has expired or is missing. Run `muse-mcp auth export` while logged into muse.ai in Chrome.",
    )
    result["authenticated"] = False
    return result


def _exception_error(exc: Exception, thread: Optional[str] = None) -> Dict[str, Any]:
    if isinstance(exc, AuthError):
        return _auth_error()
    if isinstance(exc, (MCPInputError, ValueError, TypeError)):
        return _error("INVALID_ARGUMENT", str(exc))
    if isinstance(exc, TimeoutError):
        return _error("TIMEOUT", "Muse did not respond before the gateway timeout.")
    if isinstance(exc, VMUnavailableError):
        return _error(
            "VM_UNAVAILABLE",
            "Muse's VM resolution is temporarily unavailable. Retry after it becomes available; `muse-mcp wake` can wake a known VM.",
        )
    if isinstance(exc, GatewayError):
        status = getattr(exc, "status", None)
        if thread and status in (400, 404):
            return _error("INVALID_THREAD", "Muse rejected the requested thread.", status=status)
        return _error("GATEWAY_ERROR", "Muse gateway request failed.", status=status)
    if isinstance(exc, (OSError, ConnectionError)):
        return _error("GATEWAY_ERROR", "Muse gateway connection failed.")
    return _error("GATEWAY_ERROR", "Muse gateway operation failed.")


def _is_transient(exc: Exception) -> bool:
    """True for failures a fresh connection may fix (not auth or bad input)."""
    if isinstance(exc, (AuthError, MCPInputError)):
        return False
    if isinstance(exc, VMUnavailableError):
        return True
    if isinstance(exc, GatewayError):
        status = getattr(exc, "status", None)
        if status in _TRANSIENT_STATUSES:
            return True
        return status == -1 and "stream reset" in str(exc)
    return isinstance(exc, (TimeoutError, ConnectionError, OSError, CurlError))


def _real_wake(cookies: str, vm_id: Optional[str]) -> None:
    """Ask muse.ai to wake the personal VM (same call as `muse-cli wake`)."""
    from curl_cffi import requests as rq
    from muse import _hatch_headers, fetch_access_token, fetch_session_info

    access_token = fetch_access_token(cookies)
    vm_id = vm_id or fetch_session_info(cookies)["vm_id"]
    response = rq.post(
        "https://muse.ai/api/hatch/vm/wake",
        headers=_hatch_headers(cookies, access_token),
        json={"vm_id": vm_id, "retry_count": 0},
        impersonate="chrome",
        timeout=15,
    )
    if response.status_code in (401, 403):
        raise AuthError(f"vm/wake -> {response.status_code}")
    if not 200 <= response.status_code < 300:
        raise GatewayError(response.status_code, "vm/wake failed")


def _is_proactive(event: Dict[str, Any]) -> bool:
    """Assistant pushes carry a self-referential reply_to in history."""
    if (event.get("event_name") or "") != "message.assistant":
        return False
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    return bool(event.get("reply_to_message_id") or payload.get("reply_to_message_id"))


def _is_streaming_reply(event: Dict[str, Any], baseline: int) -> bool:
    """A genuine reply Muse has started but not finished writing."""
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    if not meta.get("streaming"):
        return False
    return is_reply({**event, "payload": {**payload, "metadata": {**meta, "streaming": False}}}, baseline)


def _reply_row(event: Dict[str, Any]) -> Dict[str, Any]:
    row = _event_row(event)
    row.pop("raw", None)
    return row


def _first_text(*values: Any) -> str:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return ""


def _updated_time(session: Dict[str, Any]) -> Optional[str]:
    value = session.get("updated_at_ms")
    if isinstance(value, (int, float)) and value:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()
    for key in ("updated_at", "last_updated", "last_updated_at"):
        if session.get(key) is not None:
            return str(session[key])
    return None


def _unread_count(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, dict):
        for key in ("unread", "unread_count", "count", "total"):
            candidate = value.get(key)
            if isinstance(candidate, (int, float)):
                return int(candidate)
    return None


def _event_row(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    text = payload.get("display_text") or payload.get("content") or event.get("text") or ""
    role = payload.get("role") or event.get("role") or event.get("event_name")
    row = {
        "seq": event.get("seq"),
        "event_name": event.get("event_name") or event.get("event") or event.get("type"),
        "role": role,
        "message_id": payload.get("message_id") or event.get("message_id"),
        "reply_to_message_id": payload.get("reply_to_message_id") or event.get("reply_to_message_id"),
        "text": text,
        "timestamp": event.get("timestamp") or event.get("created_at") or payload.get("timestamp"),
        "proactive": _is_proactive(event),
        "raw": redact(event),
    }
    return row


class MuseClient:
    """Adapter where each invocation owns one independent Gateway instance.

    Gateway send/receive locks still protect one Noise connection. No lock is
    shared across MCP calls, so long-running sends do not block other tools.
    """

    def __init__(
        self,
        config_loader: Callable[[], Dict[str, Any]] = load_config,
        gateway_factory: Optional[Callable[..., Any]] = None,
        waker: Optional[Callable[[str, Optional[str]], None]] = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config_loader = config_loader
        self._gateway_factory = gateway_factory or Gateway
        # Only the real gateway gets the real wake call; injected fakes stay offline.
        self._waker = waker if waker is not None else (_real_wake if gateway_factory is None else None)
        self._sleep = sleep
        self._last_vm_id: Optional[str] = None

    @staticmethod
    def _cookies(config: Dict[str, Any]) -> str:
        try:
            cookies = load_cookies(config.get("cookies_file"))
        except (OSError, TypeError, AuthError) as exc:
            raise AuthError(f"could not read Muse cookies: {exc}") from exc
        if not cookies.strip():
            raise AuthError("Muse cookies are empty")
        return cookies

    @contextmanager
    def _gateway(self, config: Dict[str, Any]) -> Iterator[Any]:
        cookies = self._cookies(config)
        gateway = self._gateway_factory(cookies, vm_id=config.get("vm_id"))
        self._last_vm_id = getattr(gateway, "vm_id", None) or self._last_vm_id
        try:
            yield gateway
        finally:
            try:
                gateway.close()
            except Exception:
                pass

    def _wake(self, config: Dict[str, Any]) -> bool:
        if self._waker is None:
            return False
        try:
            self._waker(self._cookies(config), config.get("vm_id") or self._last_vm_id)
            return True
        except Exception:
            return False

    def _execute(
        self,
        operation: Callable[[Any, Dict[str, Any]], Any],
        thread: Optional[str] = None,
        can_retry: Callable[[], bool] = lambda: True,
    ) -> Any:
        """Run one operation on a fresh Gateway, reconnecting on transient failures.

        `can_retry` lets non-idempotent operations (send) refuse a retry once
        their side effect has been dispatched.
        """
        attempts = 0
        while True:
            try:
                config = self._config_loader()
                with self._gateway(config) as gateway:
                    return operation(gateway, config)
            except Exception as exc:
                if attempts >= len(RETRY_DELAYS) or not _is_transient(exc) or not can_retry():
                    error = _exception_error(exc, thread=thread)
                    if attempts:
                        error["error"]["attempts"] = attempts + 1
                    return error
                if isinstance(exc, VMUnavailableError):
                    self._wake(config)
                self._sleep(RETRY_DELAYS[attempts])
                attempts += 1

    def status(self) -> Dict[str, Any]:
        def operation(gateway: Any, _config: Dict[str, Any]) -> Dict[str, Any]:
            sessions = gateway.call_json("sessions.list")
            unread = gateway.call_json("chat.unread_count")
            identity = gateway.call_json("identity")
            try:
                health = gateway.call_json("vm.health")
            except Exception as exc:
                health = {"error": _exception_error(exc)["error"]}
            session_rows = sessions.get("sessions", []) if isinstance(sessions, dict) else []
            return redact({
                "ok": True,
                "authenticated": True,
                "vm_id": gateway.vm_id,
                "vm": {"id": gateway.vm_id, "reachable": True, "health": health},
                "identity": identity,
                "session_count": len(session_rows) if isinstance(session_rows, list) else None,
                "unread": unread,
                "unread_count": _unread_count(unread),
            })

        result = self._execute(operation)
        return result if isinstance(result, dict) else _error("GATEWAY_ERROR", "Invalid status response.")

    def threads(self, include_archived: bool = False) -> Any:
        def operation(gateway: Any, _config: Dict[str, Any]) -> List[Dict[str, Any]]:
            data = gateway.call_json("sessions.list")
            sessions = data.get("sessions", []) if isinstance(data, dict) else []
            rows = []
            for session in sessions:
                if not isinstance(session, dict):
                    continue
                if not include_archived and session.get("archived"):
                    continue
                rows.append({
                    "session_id": session.get("session_id"),
                    "title": session.get("title"),
                    "pinned": bool(session.get("pinned", False)),
                    "archived": bool(session.get("archived", False)),
                    "thread": session.get("is_thread"),
                    "updated_at": _updated_time(session),
                    "updated_at_ms": session.get("updated_at_ms"),
                })
            return redact(rows)

        return self._execute(operation)

    def history(self, thread: Optional[str] = None, limit: int = 20, raw: bool = False) -> Any:
        def operation(gateway: Any, config: Dict[str, Any]) -> Any:
            resolved = resolve_thread(config, thread)
            if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
                raise MCPInputError("limit must be a positive integer")
            params: Dict[str, Any] = {"limit": limit}
            if resolved:
                params["session_id"] = resolved
            data = gateway.call_json("chat.history", body=params)
            if raw:
                return redact(data)
            events = data.get("chat_events", []) if isinstance(data, dict) else []
            return redact([_event_row(event) for event in events if isinstance(event, dict)])

        return self._execute(operation, thread=thread)

    def send(
        self,
        message: str,
        thread: Optional[str] = None,
        wait_seconds: Optional[float] = 90,
        fire_and_forget: bool = False,
    ) -> Any:
        dispatched = {"value": False}

        def operation(gateway: Any, config: Dict[str, Any]) -> Dict[str, Any]:
            resolved = resolve_thread(config, thread)
            if not isinstance(message, str) or not message.strip():
                raise MCPInputError("message must be a non-empty string")
            if wait_seconds is not None and (not isinstance(wait_seconds, (int, float)) or isinstance(wait_seconds, bool) or wait_seconds < 0):
                raise MCPInputError("wait_seconds must be non-negative or null")
            params: Dict[str, Any] = {
                "items": [{"type": "text", "text": message}],
                "node_id": config["node_id"],
                "capabilities": {},
            }
            if resolved:
                params["session_id"] = resolved
            scope = {"session_id": resolved} if resolved else {}
            baseline = 0
            try:
                history = gateway.call_json("chat.history", body={"limit": 1, **scope})
                events = history.get("chat_events", []) if isinstance(history, dict) else []
                if events:
                    baseline = max((event.get("seq", 0) or 0) for event in events if isinstance(event, dict))
            except TimeoutError:
                # A read timeout is transient; the send can still be dispatched.
                pass
            except GatewayError:
                raise

            # Set before _open: a send that fails midway may still have reached Muse.
            dispatched["value"] = True
            stream_id = gateway._open("chat.stream", body=params)
            result: Dict[str, Any] = {"ok": True, "sent": True, "stream_id": stream_id}
            if fire_and_forget or wait_seconds == 0:
                result["waiting"] = False
                result["acknowledged"] = False
                result["note"] = "request dispatched; inspect muse_history for progress or completion"
                return result

            timeout = 90 if wait_seconds is None else float(wait_seconds)
            deadline = time.monotonic() + timeout
            partial = None
            while time.monotonic() < deadline:
                try:
                    history = gateway.call_json("chat.history", body={"limit": 10, **scope})
                    events = history.get("chat_events", []) if isinstance(history, dict) else []
                    for event in sorted(
                        (event for event in events if isinstance(event, dict)),
                        key=lambda item: item.get("seq", 0) or 0,
                    ):
                        if is_reply(event, baseline):
                            result["reply"] = _reply_row(event)
                            return result
                        if _is_streaming_reply(event, baseline):
                            partial = event
                except TimeoutError:
                    # Polling timeouts are transient; keep waiting until the caller's deadline.
                    pass
                except GatewayError:
                    raise
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(min(4.0, remaining))
            result["ok"] = False
            result["acknowledged"] = False
            result["error"] = {
                "code": "TIMEOUT",
                "message": "Muse request was dispatched, but no genuine assistant reply arrived before the requested timeout.",
                "wait_seconds": timeout,
            }
            if partial is not None:
                result["error"]["message"] = "Muse is still writing its reply; read it later with muse_history or muse_main_since."
                result["partial_reply"] = _reply_row(partial)
            result["waiting"] = True
            return result

        # Never resend once chat.stream accepted the message.
        return self._execute(operation, thread=thread, can_retry=lambda: not dispatched["value"])

    def feed(self, limit: Optional[int] = None, raw: bool = False) -> Any:
        def operation(gateway: Any, _config: Dict[str, Any]) -> Any:
            if limit is not None and (not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0):
                raise MCPInputError("limit must be a positive integer or null")
            data = gateway.call_json("feed.list", query={"limit": limit} if limit else None)
            if raw:
                return redact(data)
            rows: List[Dict[str, Any]] = []
            days = data.get("days", []) if isinstance(data, dict) else []
            for day in days:
                if not isinstance(day, dict):
                    continue
                for edition in day.get("editions", []) or []:
                    if not isinstance(edition, dict):
                        continue
                    for unit in edition.get("units", []) or []:
                        if not isinstance(unit, dict):
                            continue
                        rows.append({
                            "id": unit.get("unit_id") or unit.get("id"),
                            "unit_id": unit.get("unit_id") or unit.get("id"),
                            "title": unit.get("title"),
                            "content": unit.get("content") or unit.get("body") or unit.get("text"),
                            "date": day.get("local_date") or day.get("date"),
                            "edition": edition.get("kind") or edition.get("edition"),
                            "source": unit.get("source") or unit.get("source_name"),
                            "url": unit.get("url") or unit.get("canonical_url"),
                            "published_at": unit.get("published_at") or unit.get("created_at"),
                            "metadata": redact(unit),
                        })
            return redact(rows[:limit] if limit else rows)

        return self._execute(operation)

    def unread(self) -> Dict[str, Any]:
        def operation(gateway: Any, _config: Dict[str, Any]) -> Dict[str, Any]:
            data = gateway.call_json("chat.unread_count")
            return redact({"ok": True, "unread": data, "unread_count": _unread_count(data)})

        return self._execute(operation)

    def seen(self, thread: str) -> Any:
        def operation(gateway: Any, config: Dict[str, Any]) -> Dict[str, Any]:
            resolved = resolve_thread(config, thread)
            if not resolved:
                raise MCPInputError("thread is required")
            data = gateway.call_json("chat.mark_seen", path_params={"thread_id": resolved})
            return redact({"ok": True, "thread": resolved, "result": data})

        return self._execute(operation, thread=thread)

    def main_since(self, cursor: int = 0, limit: int = 50, raw: bool = False) -> Any:
        """Main-chat messages (yours, replies and proactive pushes) after `cursor`."""

        def operation(gateway: Any, _config: Dict[str, Any]) -> Dict[str, Any]:
            if not isinstance(cursor, int) or isinstance(cursor, bool) or cursor < 0:
                raise MCPInputError("cursor must be a non-negative integer")
            if not isinstance(limit, int) or isinstance(limit, bool) or not 0 < limit <= 200:
                raise MCPInputError("limit must be an integer between 1 and 200")
            # No session_id: chat.history reads the main chat.
            data = gateway.call_json("chat.history", body={"limit": limit})
            events = data.get("chat_events", []) if isinstance(data, dict) else []
            events = sorted(
                (event for event in events if isinstance(event, dict)),
                key=lambda item: item.get("seq", 0) or 0,
            )
            seqs = [event.get("seq", 0) or 0 for event in events]
            messages = []
            for event in events:
                if (event.get("seq", 0) or 0) <= cursor:
                    continue
                if not str(event.get("event_name") or "").startswith("message."):
                    continue
                row = _event_row(event)
                if not row["text"]:
                    continue
                if not raw:
                    row.pop("raw", None)
                messages.append(row)
            return redact({
                "ok": True,
                "cursor": cursor,
                "next_cursor": max(seqs + [cursor]),
                "messages": messages,
                # A full page that starts after the cursor may have skipped older messages.
                "possibly_missed": bool(cursor and len(events) >= limit and seqs and seqs[0] > cursor + 1),
            })

        return self._execute(operation)

    def watch(self, seconds: float = 60, max_events: int = 500, raw: bool = False) -> Any:
        """Listen to live main-chat events for `seconds` and assemble message text."""

        def operation(gateway: Any, _config: Dict[str, Any]) -> Dict[str, Any]:
            if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or not 0 < seconds <= 600:
                raise MCPInputError("seconds must be between 0 and 600")
            if not isinstance(max_events, int) or isinstance(max_events, bool) or max_events <= 0:
                raise MCPInputError("max_events must be a positive integer")
            records = gateway.subscribe_json(
                "chat.subscribe",
                body={"capabilities": {}},
                max_records=max_events,
                idle_timeout=float(seconds),
                overall_timeout=float(seconds),
            )
            messages: Dict[Any, Dict[str, Any]] = {}
            updates: List[Dict[str, Any]] = []
            counts: Dict[str, int] = {}
            for record in records:
                if not isinstance(record, dict):
                    continue
                payload = record.get("payload") if isinstance(record.get("payload"), dict) else record
                kind = str(record.get("event") or record.get("type") or record.get("event_name") or "unknown")
                counts[kind] = counts.get(kind, 0) + 1
                key = (
                    payload.get("message_seq") or record.get("message_seq")
                    or payload.get("message_id") or record.get("message_id") or record.get("seq")
                )
                if kind == "delta.text_append":
                    chunks = payload.get("chunks")
                    if isinstance(chunks, list):
                        piece = "".join(chunk for chunk in chunks if isinstance(chunk, str))
                    else:
                        piece = _first_text(payload.get("text"), payload.get("delta"), payload.get("chunk"), payload.get("content"))
                    entry = messages.setdefault(key, {"key": key, "role": payload.get("role") or "assistant", "text": "", "done": False})
                    entry["text"] += piece
                elif kind == "delta.message_done" or kind.startswith("message."):
                    entry = messages.setdefault(key, {"key": key, "role": payload.get("role") or "assistant", "text": "", "done": False})
                    full = _first_text(
                        payload.get("display_text"), payload.get("transcript"), payload.get("text"), payload.get("content")
                    )
                    if full:
                        entry["text"] = full
                    if kind.startswith("message.") and not payload.get("role"):
                        entry["role"] = kind.split(".", 1)[1]
                    entry["done"] = True
                elif kind == "sessions.updated":
                    updates.append({
                        "session_id": payload.get("session_id"),
                        "title": payload.get("title"),
                        "snippet": _first_text(payload.get("snippet"), payload.get("preview"), payload.get("text")),
                    })
            result: Dict[str, Any] = {
                "ok": True,
                "seconds": seconds,
                "messages": [entry for entry in messages.values() if entry["text"]],
                "session_updates": updates,
                "event_counts": counts,
            }
            if raw:
                result["raw"] = records
            return redact(result)

        return self._execute(operation)

    def goals(self, goal_id: Optional[str] = None) -> Any:
        def operation(gateway: Any, _config: Dict[str, Any]) -> Any:
            if goal_id:
                return redact(gateway.call_json("goals.get", path_params={"id": goal_id}))
            data = gateway.call_json("goals.list")
            goals = data.get("goals", []) if isinstance(data, dict) else []
            return redact([
                {
                    "id": goal.get("id") or goal.get("goal_id"),
                    "title": goal.get("title"),
                    "status": goal.get("status"),
                    "subtitle": goal.get("subtitle"),
                }
                for goal in goals if isinstance(goal, dict)
            ])

        return self._execute(operation)

    def ideas(self, idea_id: Optional[str] = None) -> Any:
        def operation(gateway: Any, _config: Dict[str, Any]) -> Any:
            if idea_id:
                return redact(gateway.call_json("api.idea-cards.detail", path_params={"ideaCardId": idea_id}))
            data = gateway.call_json("api.idea-cards.list")
            rows = []
            for section in data.get("sections", []) if isinstance(data, dict) else []:
                if not isinstance(section, dict):
                    continue
                for card in section.get("cards", section.get("ideas", [])) or []:
                    if isinstance(card, dict):
                        rows.append({
                            "id": card.get("id") or card.get("ideaCardId"),
                            "title": card.get("title"),
                            "section": section.get("title"),
                        })
            return redact(rows)

        return self._execute(operation)

    SESSION_ACTIONS = ("start", "rename", "pin", "unpin", "archive", "unarchive")

    def session(self, action: str, thread: Optional[str] = None, title: Optional[str] = None) -> Any:
        """Manage side chats; delete stays behind muse_raw on purpose."""

        def operation(gateway: Any, config: Dict[str, Any]) -> Dict[str, Any]:
            if action not in self.SESSION_ACTIONS:
                raise MCPInputError(f"action must be one of {', '.join(self.SESSION_ACTIONS)}")
            if action == "start":
                params: Dict[str, Any] = {"origin": "fresh", "lifecycle": "persistent"}
                if title:
                    params["title"] = title
                data = gateway.call_json("session.start", body={"method": "/api/session/start", "params": params})
                return redact({"ok": True, "action": action, "result": data})
            resolved = resolve_thread(config, thread)
            if not resolved:
                raise MCPInputError("thread is required for this action")
            if action == "rename":
                if not isinstance(title, str) or not title.strip():
                    raise MCPInputError("title is required to rename")
                body: Dict[str, Any] = {"session_id": resolved, "title": title}
            else:
                body = {"method": f"/api/session/{action}", "session_id": resolved}
            data = gateway.call_json(f"session.{action}", body=body)
            return redact({"ok": True, "action": action, "thread": resolved, "result": data})

        # Starting twice would create two chats, so never retry a start.
        return self._execute(operation, thread=thread, can_retry=lambda: action != "start")

    def wake(self) -> Dict[str, Any]:
        try:
            config = self._config_loader()
            if self._waker is None:
                return _error("WAKE_UNAVAILABLE", "No wake handler is configured.")
            self._waker(self._cookies(config), config.get("vm_id") or self._last_vm_id)
            return {"ok": True, "requested": True}
        except Exception as exc:
            return _exception_error(exc)

    def raw(
        self,
        method: str,
        body: Any = None,
        path_params: Optional[Dict[str, Any]] = None,
        query_params: Optional[Dict[str, Any]] = None,
        timeout: float = 30,
    ) -> Any:
        try:
            config = self._config_loader()
        except Exception as exc:
            return _exception_error(exc)
        raw_flag = os.environ.get("MUSE_MCP_ENABLE_RAW")
        enabled = (
            raw_flag.strip().lower() in ("1", "true", "yes", "on")
            if raw_flag is not None
            else bool(config.get("enable_raw", False))
        )
        if not enabled:
            return _error(
                "RAW_DISABLED",
                "muse_raw is disabled. Set MUSE_MCP_ENABLE_RAW=1 for this trusted local MCP server.",
            )
        if not isinstance(method, str) or not method:
            return _error("UNKNOWN_ROUTE", "A route method name is required.")
        if method not in ROUTES:
            return _error("UNKNOWN_ROUTE", "Unknown Muse route; choose a method from routes.json.", method=method)
        if path_params is not None and not isinstance(path_params, dict):
            return _error("INVALID_ARGUMENT", "path_params must be an object.")
        if query_params is not None and not isinstance(query_params, dict):
            return _error("INVALID_ARGUMENT", "query_params must be an object.")
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
            return _error("INVALID_ARGUMENT", "timeout must be a positive number.")

        def operation(gateway: Any, _config: Dict[str, Any]) -> Dict[str, Any]:
            payload = gateway.request(
                method,
                path_params=path_params,
                body=body,
                query=query_params,
                timeout=float(timeout),
            )
            if isinstance(payload, bytes):
                try:
                    text = payload.decode("utf-8")
                except UnicodeDecodeError:
                    return {
                        "ok": True,
                        "method": method,
                        "binary": True,
                        "size": len(payload),
                        "note": "binary response body omitted for safety",
                    }
                try:
                    decoded = json.loads(text) if text else {}
                    return {"ok": True, "method": method, "result": redact(decoded)}
                except json.JSONDecodeError:
                    return {"ok": True, "method": method, "result": _redact_text(text)}
            return {"ok": True, "method": method, "result": redact(payload)}

        # A timed-out POST/PATCH/DELETE may already have run; only reads retry.
        is_read = ROUTES[method].get("http") == "GET"
        return self._execute(operation, can_retry=lambda: is_read)

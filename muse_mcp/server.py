"""Official MCP stdio server exposing a small Muse primitive surface."""

from functools import partial
from typing import Any, Callable, Dict, Optional

import anyio

from mcp.server.fastmcp import FastMCP

from .client import MuseClient


mcp = FastMCP("muse-mcp")
_client = MuseClient()


def get_client() -> MuseClient:
    """Return the client; each invocation uses an independent Gateway."""
    return _client


async def _run(call: Callable[..., Any], **kwargs: Any) -> Any:
    """Run a blocking client call in a worker thread.

    FastMCP calls sync tools on the event loop, so one long send or watch
    would otherwise stall every other tool call and ping.
    """
    return await anyio.to_thread.run_sync(partial(call, **kwargs))


@mcp.tool()
async def muse_status() -> Dict[str, Any]:
    """Return Muse authentication, VM reachability, identity, sessions, and unread state."""
    return await _run(get_client().status)


@mcp.tool()
async def muse_threads(include_archived: bool = False) -> Any:
    """List persistent Muse side chats; archived chats are excluded by default."""
    return await _run(get_client().threads, include_archived=include_archived)


@mcp.tool()
async def muse_send(
    message: str,
    thread: Optional[str] = None,
    wait_seconds: Optional[float] = 90,
    fire_and_forget: bool = False,
) -> Any:
    """Send natural-language work to Muse and optionally wait for its genuine reply.

    Thread may be a session ID or a configured alias. A wait timeout is not a
    workflow limit; long browser tasks may use several minutes or more. Set
    fire_and_forget to return after the gateway accepts the message.
    """
    return await _run(
        get_client().send,
        message=message,
        thread=thread,
        wait_seconds=wait_seconds,
        fire_and_forget=fire_and_forget,
    )


@mcp.tool()
async def muse_history(
    thread: Optional[str] = None,
    limit: int = 20,
    raw: bool = False,
) -> Any:
    """Read scoped Muse history with event, reply, sequence, and proactive metadata."""
    return await _run(get_client().history, thread=thread, limit=limit, raw=raw)


@mcp.tool()
async def muse_feed(limit: Optional[int] = None, raw: bool = False) -> Any:
    """Read Muse feed items with IDs, content, dates, editions, and source metadata."""
    return await _run(get_client().feed, limit=limit, raw=raw)


@mcp.tool()
async def muse_main_since(cursor: int = 0, limit: int = 50, raw: bool = False) -> Any:
    """Read new main-chat messages after a seq cursor, including Muse's proactive pushes.

    Pass the previous call's next_cursor to get only what is new. Rows carry
    proactive=true for pushes Muse sent on its own. possibly_missed=true means
    more than `limit` messages arrived since the cursor; raise limit to catch up.
    """
    return await _run(get_client().main_since, cursor=cursor, limit=limit, raw=raw)


@mcp.tool()
async def muse_watch(seconds: float = 60, max_events: int = 500, raw: bool = False) -> Any:
    """Listen to live main-chat events for up to `seconds` (max 600) and return assembled messages.

    Side-chat replies are not streamed live; they appear only as session_updates
    snippets, so read them with muse_history.
    """
    return await _run(get_client().watch, seconds=seconds, max_events=max_events, raw=raw)


@mcp.tool()
async def muse_unread() -> Dict[str, Any]:
    """Return Muse's unread counts."""
    return await _run(get_client().unread)


@mcp.tool()
async def muse_seen(thread: str) -> Any:
    """Mark a side chat (session ID or alias) as seen."""
    return await _run(get_client().seen, thread=thread)


@mcp.tool()
async def muse_goals(goal_id: Optional[str] = None) -> Any:
    """List Muse goals, or show one goal in full when goal_id is given."""
    return await _run(get_client().goals, goal_id=goal_id)


@mcp.tool()
async def muse_ideas(idea_id: Optional[str] = None) -> Any:
    """List Muse idea cards, or show one card's detail when idea_id is given."""
    return await _run(get_client().ideas, idea_id=idea_id)


@mcp.tool()
async def muse_session(action: str, thread: Optional[str] = None, title: Optional[str] = None) -> Any:
    """Manage side chats: start (optional title), rename (thread + title), pin, unpin, archive, unarchive."""
    return await _run(get_client().session, action=action, thread=thread, title=title)


@mcp.tool()
async def muse_wake() -> Dict[str, Any]:
    """Ask muse.ai to wake the personal VM. Other tools already do this on VM_UNAVAILABLE."""
    return await _run(get_client().wake)


@mcp.tool()
async def muse_raw(
    method: str,
    body: Any = None,
    path_params: Optional[Dict[str, Any]] = None,
    query_params: Optional[Dict[str, Any]] = None,
    timeout: float = 30,
) -> Any:
    """Call one validated routes.json gateway method (advanced, unstable API).

    Muse's internal routes are unversioned and may change. Credential-shaped
    fields in route responses are redacted and raw payloads are not logged.
    """
    return await _run(
        get_client().raw,
        method=method,
        body=body,
        path_params=path_params,
        query_params=query_params,
        timeout=timeout,
    )


def main() -> None:
    """Run the server over local stdio."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

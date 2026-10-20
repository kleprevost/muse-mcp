# muse-mcp

`muse-mcp` is a local, stdio Model Context Protocol server that lets another
agent delegate natural-language browser, research, and computer-use work to a
personal [Muse](https://muse.ai/) agent. It is intentionally a thin transport
layer: Muse remains responsible for browser interaction and domain decisions.
There is no Instacart, Marketplace, checkout, notification, or workflow logic
in this project.

The server reuses the upstream `muse-cli` authentication, VM discovery, Noise
WebSocket gateway, route table, chat polling, history, and feed implementation.
Muse's gateway is an internal, unversioned API and can break when the Muse web
client changes.

## Install

Use Python 3.10+ and `uv`:

```bash
git clone <this-repository> muse-mcp
cd muse-mcp
uv venv
uv pip install --python .venv/bin/python -r requirements.txt
uv pip install --python .venv/bin/python -e .
```

The package installs the `muse-mcp` console command. The repository also keeps
the original `cli.py` entry point for upstream-compatible command-line use.

Chrome cookie export requires the external `agent-browser` executable:

```bash
npm i -g agent-browser
```

## Authenticate with Chrome

1. Log in to `https://muse.ai/` in Chrome.
2. Keep a Muse tab open and focused in that Chrome session.
3. Export the browser session:

   ```bash
   .venv/bin/muse-mcp auth export
   .venv/bin/muse-mcp doctor
   ```

`doctor` only checks authentication and Muse status; it does not authenticate
or export cookies. If authentication expires, rerun `muse-mcp auth export`
while logged into Muse, then run `muse-mcp doctor` again.

Cookies are stored at `~/.config/muse-cli/cookies.txt` with mode `0600`, and
remain compatible with the existing `muse-cli` configuration. Access and
gateway tokens are fetched for each gateway connection; they are not stored.
Manual Netscape or `name=value; ...` cookie files are also supported if they
contain `hatch_sess`. The export helper uses `agent-browser` to read the active
logged-in Chrome session.

## Run the MCP server

The default transport is local stdio; there is no listening network socket:

```bash
.venv/bin/muse-mcp
# equivalent:
.venv/bin/python -m muse_mcp
```

For an MCP client, copy `mcp-config.example.json`, replace the absolute path,
and add the `mcpServers.muse` entry to that client's configuration. A typical
entry is:

```json
{
  "mcpServers": {
    "muse": {
      "command": "/Users/me/muse-mcp/.venv/bin/muse-mcp",
      "args": []
    }
  }
}
```

## Tools

- **`muse_status`** — structured authentication, VM ID/reachability/health,
  Muse identity, session count, and unread state.
- **`muse_threads(include_archived=false)`** — persistent side chats with
  `session_id`, title, pinned/archived flags, and update metadata.
- **`muse_send(message, thread?, wait_seconds?, fire_and_forget?)`** — sends
  arbitrary natural-language instructions to the main chat or selected side
  chat. `thread` can be a session ID or an optional alias in the existing
  `~/.config/muse-cli/config.json`, for example
  `"aliases": {"instacart": "session-id-1"}`. Replies are found by scoped
  history polling, not by assuming the live stream shape. A timeout returns a
  structured `TIMEOUT` result while confirming that the message was sent.
- **`muse_history(thread?, limit=20, raw=false)`** — returns event sequence,
  event type, role, message ID, reply relationship, text, timestamp, and the
  redacted raw event metadata. `raw=true` preserves the gateway response.
- **`muse_feed(limit?, raw=false)`** — returns feed unit IDs, titles, content,
  dates, editions, source/URL metadata, and a redacted unit copy. `raw=true`
  exposes the expanded gateway response with credential-shaped fields redacted.
- **`muse_main_since(cursor=0, limit=50, raw=false)`** — main-chat messages
  with `seq` greater than `cursor`, including Muse's proactive pushes
  (`proactive=true`). Store `next_cursor` and pass it back next time to read
  only what is new. `possibly_missed=true` means more than `limit` messages
  arrived since the cursor.
- **`muse_watch(seconds=60, max_events=500, raw=false)`** — subscribes to
  `chat.subscribe` for up to 600 seconds and returns assembled message text
  (`delta.text_append` chunks joined, `delta.message_done` taken as final),
  side-chat `session_updates` snippets, and per-event counts. Side-chat replies
  are not streamed live; read them with `muse_history`.
- **`muse_unread()`** / **`muse_seen(thread)`** — unread counts; mark a side
  chat seen.
- **`muse_goals(goal_id?)`** / **`muse_ideas(idea_id?)`** — list goals or idea
  cards, or show one in full.
- **`muse_session(action, thread?, title?)`** — `start`, `rename`, `pin`,
  `unpin`, `archive`, `unarchive`. Delete is only reachable through `muse_raw`.
- **`muse_wake()`** — asks muse.ai to wake the personal VM.
- **`muse_raw(method, body?, path_params?, query_params?, timeout=30)`** — an
  advanced escape hatch for one method validated against `routes.json`. It is
  disabled by default; set `MUSE_MCP_ENABLE_RAW=1` (or `"enable_raw": true` in
  the compatible config) only for a trusted local orchestrator. Route names
  and response shapes are unstable. Known credential-shaped fields, bearer
  tokens, and obvious token assignments are redacted; binary bodies are
  omitted while their size is reported.

All errors use structured codes such as `AUTH_REQUIRED`, `VM_UNAVAILABLE`,
`TIMEOUT`, `INVALID_THREAD`, `UNKNOWN_ROUTE`, `RAW_DISABLED`, and
`GATEWAY_ERROR`. Authentication errors explain how to run
`muse-mcp auth export` without exposing cookies or tokens.

Conceptually, a calling agent can select a configured side chat and delegate:

```text
muse_threads()
→ finds "Instacart"

muse_send(
  thread=<instacart session>,
  message="Add blueberries to the list for this week's grocery run."
)
```

Or use a general browser session without any domain-specific MCP code:

```text
muse_send(
  thread=<general browser session>,
  message="Research these three products and compare current prices. Do not purchase anything."
)
```

## Reliability and security

- Each MCP operation opens and closes its own Gateway. There is no
  process-wide operation lock, so unrelated history/feed/status calls can run
  while a long send waits. Each Gateway retains its own send/receive locks;
  no Gateway is shared across concurrent MCP calls.
- Genuine replies are discovered by polling scoped history. Assistant events
  with a new sequence and an empty `reply_to_message_id` are replies; proactive
  pushes are not treated as the delegated response.
- For long computer-use jobs, `fire_and_forget=true` dispatches the request and
  closes its Gateway without waiting for completion. It does not provide a
  protocol-level acceptance acknowledgement; check later with `muse_history`.
- Transient failures (timeouts, dropped connections, stream resets, 502/503/504)
  reconnect on a fresh Gateway with 1 s then 3 s backoff. `VM_UNAVAILABLE`
  first requests a VM wake. Errors after retries report `attempts`. Auth and
  argument errors never retry. `muse_send` never retries once `chat.stream`
  accepted the message, and `muse_session(start)` never retries, so neither can
  run twice.
  A local timeout never cancels Muse's task.
- Muse VMs can sleep, restart, or be temporarily unavailable. Retry a
  structured `VM_UNAVAILABLE`; `muse-mcp wake` is available for a configured or
  known VM ID. Other protocol failures remain `GATEWAY_ERROR`.
- `muse_raw` is disabled by default and should only be enabled for a trusted
  local orchestrator. Known credential-shaped fields, bearer tokens, and
  obvious token assignments are redacted, but arbitrary route content (for
  example file contents) cannot be guaranteed secret-free.
- This server does not listen remotely, check out carts, place orders, send
  third-party messages, or make relevance notifications.
- `muse.py`, `cli.py`, `routes.json`, descriptors, and protocol notes originate
  from `nikships/muse-cli` and should generally remain close to upstream.
  Respect service rate limits and update protocol behavior from current Muse
  observations rather than guessing.

## Development

Run the mocked MCP-layer tests without a Muse account:

```bash
python -m unittest discover -s tests -v
```

After authenticating, run the low-risk live smoke path:

```bash
muse-mcp doctor
python scripts/smoke_test.py
```

The smoke script only connects, fetches identity/status, lists sessions, and
reads a small history window. It does not send messages or mutate Muse state.
There is no automated live integration test.

## Layout

```text
muse_mcp/          MCP server, serialized adapter, config, setup wrapper
muse.py            upstream HTTPS auth and Noise gateway
cli.py             preserved upstream CLI/auth/export implementation
routes.json        internal gateway method table
desc0.bin/desc1.bin protobuf framing descriptors
docs/PROTOCOL.md   reverse-engineered protocol notes
scripts/           optional low-risk live smoke command
```

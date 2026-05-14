# FinAlly — AI Trading Workstation

## Project Specification

## 1. Vision

FinAlly (Finance Ally) is a visually stunning AI-powered trading workstation that streams live market data, lets users trade a simulated portfolio, and integrates an LLM chat assistant that can analyze positions and execute trades on the user's behalf. It looks and feels like a modern Bloomberg terminal with an AI copilot.

This is the capstone project for an agentic AI coding course. It is built entirely by Coding Agents demonstrating how orchestrated AI agents can produce a production-quality full-stack application. Agents interact through files in `planning/`.

### Companion Documents

- **`planning/MARKET_DATA_SUMMARY.md`** — the market data subsystem (§6) is already implemented; this file is the source of truth for its interface, modules, and test coverage.
- **`backend/CLAUDE.md`** — backend conventions for agents working in `backend/app/`.
- **`planning/archive/`** — older planning artifacts kept for historical reference; not authoritative.

## 2. User Experience

### First Launch

The user runs a single Docker command (or a provided start script). A browser opens to `http://localhost:8000`. No login, no signup. They immediately see:

- A watchlist of 10 default tickers with live-updating prices in a grid
- $10,000 in virtual cash
- A dark, data-rich trading terminal aesthetic
- An AI chat panel ready to assist

### What the User Can Do

- **Watch prices stream** — prices flash green (uptick) or red (downtick) with subtle CSS animations that fade
- **View sparkline mini-charts** — price action beside each ticker in the watchlist, accumulated on the frontend from the SSE stream since page load (sparklines fill in progressively)
- **Click a ticker** to see a larger detailed chart in the main chart area
- **Buy and sell shares** — market orders only, instant fill at current price, no fees, no confirmation dialog
- **Monitor their portfolio** — a heatmap (treemap) showing positions sized by weight and colored by P&L, plus a P&L chart tracking total portfolio value over time
- **View a positions table** — ticker, quantity, average cost, current price, unrealized P&L, % change
- **Chat with the AI assistant** — ask about their portfolio, get analysis, and have the AI execute trades and manage the watchlist through natural language
- **Manage the watchlist** — add/remove tickers manually or via the AI chat

### Visual Design

- **Dark theme**: backgrounds around `#0d1117` or `#1a1a2e`, muted gray borders, no pure black
- **Price flash animations**: brief green/red background highlight on price change, fading over ~500ms via CSS transitions
- **Connection status indicator**: a small colored dot (green = connected, yellow = reconnecting, red = disconnected) visible in the header
- **Professional, data-dense layout**: inspired by Bloomberg/trading terminals — every pixel earns its place
- **Responsive but desktop-first**: optimized for wide screens, functional on tablet

### Color Scheme
- Accent Yellow: `#ecad0a`
- Blue Primary: `#209dd7`
- Purple Secondary: `#753991` (submit buttons)

## 3. Architecture Overview

### Single Container, Single Port

```
┌─────────────────────────────────────────────────┐
│  Docker Container (port 8000)                   │
│                                                 │
│  FastAPI (Python/uv)                            │
│  ├── /api/*          REST endpoints             │
│  ├── /api/stream/*   SSE streaming              │
│  └── /*              Static file serving         │
│                      (Next.js export)            │
│                                                 │
│  SQLite database (volume-mounted)               │
│  Background task: market data polling/sim        │
└─────────────────────────────────────────────────┘
```

- **Frontend**: Next.js with TypeScript, built as a static export (`output: 'export'`), served by FastAPI as static files
- **Backend**: FastAPI (Python), managed as a `uv` project
- **Database**: SQLite, single file at `db/finally.db`, volume-mounted for persistence
- **Real-time data**: Server-Sent Events (SSE) — simpler than WebSockets, one-way server→client push, works everywhere
- **AI integration**: LiteLLM → OpenRouter (Cerebras for fast inference), with structured outputs for trade execution
- **Market data**: Environment-variable driven — simulator by default, real data via Massive API if key provided

### Why These Choices

| Decision | Rationale |
|---|---|
| SSE over WebSockets | One-way push is all we need; simpler, no bidirectional complexity, universal browser support |
| Static Next.js export | Single origin, no CORS issues, one port, one container, simple deployment |
| SQLite over Postgres | No auth = no multi-user = no need for a database server; self-contained, zero config |
| Single Docker container | Students run one command; no docker-compose for production, no service orchestration |
| uv for Python | Fast, modern Python project management; reproducible lockfile; what students should learn |
| Market orders only | Eliminates order book, limit order logic, partial fills — dramatically simpler portfolio math |

---

## 4. Directory Structure

```
finally/
├── frontend/                 # Next.js TypeScript project (static export)
├── backend/                  # FastAPI uv project (Python)
│   ├── app/                  # Python package: api/, market/, llm/, db/, ...
│   │   └── db/               # Schema SQL, seed data, lazy initialization
│   ├── tests/                # pytest suite
│   ├── pyproject.toml        # uv-managed dependencies + lockfile
│   └── CLAUDE.md             # Backend conventions for agents
├── planning/                 # Project-wide documentation for agents
│   ├── PLAN.md               # This document
│   └── ...                   # Additional agent reference docs
├── scripts/
│   ├── start_mac.sh          # Launch Docker container (macOS/Linux)
│   ├── stop_mac.sh           # Stop Docker container (macOS/Linux)
│   ├── start_windows.ps1     # Launch Docker container (Windows PowerShell)
│   └── stop_windows.ps1      # Stop Docker container (Windows PowerShell)
├── test/                     # Playwright E2E tests + docker-compose.test.yml
├── db/                       # Volume mount target (SQLite file lives here at runtime)
│   └── .gitkeep              # Directory exists in repo; finally.db is gitignored
├── Dockerfile                # Multi-stage build (Node → Python)
├── docker-compose.yml        # Optional convenience wrapper
├── .env                      # Environment variables (gitignored, .env.example committed)
└── .gitignore
```

### Key Boundaries

- **`frontend/`** is a self-contained Next.js project. It knows nothing about Python. It talks to the backend via `/api/*` endpoints and `/api/stream/*` SSE endpoints. Internal structure is up to the Frontend Engineer agent.
- **`backend/`** is a self-contained uv project with its own `pyproject.toml`. The Python package lives at `backend/app/`, organized into submodules (`api/`, `market/`, `llm/`, `db/`, ...). Backend agents should consult `backend/CLAUDE.md` for conventions and `planning/MARKET_DATA_SUMMARY.md` for the already-complete market data subsystem.
- **`backend/app/db/`** contains schema SQL definitions and seed logic. The backend lazily initializes the database on first request — creating tables and seeding default data if the SQLite file doesn't exist or is empty.
- **`db/`** at the top level is the runtime volume mount point. The SQLite file (`db/finally.db`) is created here by the backend and persists across container restarts via Docker volume.
- **`planning/`** contains project-wide documentation, including this plan. All agents reference files here as the shared contract.
- **`test/`** contains Playwright E2E tests and supporting infrastructure (e.g., `docker-compose.test.yml`). Unit tests live within `frontend/` and `backend/` respectively, following each framework's conventions.
- **`scripts/`** contains start/stop scripts that wrap Docker commands.

---

## 5. Environment Variables

```bash
# Required: OpenRouter API key for LLM chat functionality
OPENROUTER_API_KEY=sk-or-v1-...

# Optional: Massive (Polygon.io) API key for real market data
# If not set, the built-in market simulator is used (recommended for most users)
MASSIVE_API_KEY=

# Optional: Set to "true" for deterministic mock LLM responses (testing)
LLM_MOCK=false
```

### Behavior

- If `MASSIVE_API_KEY` is set and non-empty → backend uses Massive REST API for market data
- If `MASSIVE_API_KEY` is absent or empty → backend uses the built-in market simulator
- If `LLM_MOCK=true` → backend returns deterministic mock LLM responses (for E2E tests)
- The backend reads `.env` from the project root (mounted into the container or read via docker `--env-file`)

---

## 6. Market Data

### Two Implementations, One Interface

Both the simulator and the Massive client implement the same abstract interface. The backend selects which to use based on the environment variable. All downstream code (SSE streaming, price cache, frontend) is agnostic to the source.

### Simulator (Default)

- Generates prices using geometric Brownian motion (GBM) with configurable drift and volatility per ticker
- Updates at ~500ms intervals
- Correlated moves across tickers (e.g., tech stocks move together)
- Occasional random "events" — sudden 2-5% moves on a ticker for drama
- Starts from realistic seed prices (e.g., AAPL ~$190, GOOGL ~$175, etc.)
- Runs as an in-process background task — no external dependencies

### Massive API (Optional)

- REST API polling (not WebSocket) — simpler, works on all tiers
- Polls for the union of all watched tickers on a configurable interval
- Free tier (5 calls/min): poll every 15 seconds
- Paid tiers: poll every 2-15 seconds depending on tier
- Parses REST response into the same format as the simulator

> **UX note:** The simulator is the recommended demo path. Massive's free tier polls every 15 seconds, so price flashes and sparkline fill-in will be visibly sparser than the simulator's ~500ms updates. Agents should not over-tune the UI for Massive's cadence — optimize for the simulator and accept Massive as "real but slow."

### Shared Price Cache

- A single background task (simulator or Massive poller) writes to an in-memory price cache
- The cache holds the latest price, previous price, and timestamp for each ticker
- SSE streams read from this cache and push updates to connected clients
- This architecture supports future multi-user scenarios without changes to the data layer

### SSE Streaming

- Endpoint: `GET /api/stream/prices`
- Long-lived SSE connection; client uses native `EventSource` API
- The price cache holds a monotonically increasing version counter; the SSE handler polls the cache at ~500ms and emits an event only when the version advances. This means clients receive an event per *actual* price change, not a fixed cadence — quiet markets produce quiet streams.
- Each SSE event contains ticker, price, previous price, timestamp, and change direction
- Multiple concurrent connections (e.g., two browser tabs) each get their own independent stream; there is no de-duplication
- Client handles reconnection automatically (EventSource has built-in retry)

---

## 7. Database

### SQLite with Lazy Initialization

The backend checks for the SQLite database on startup (or first request). If the file doesn't exist or tables are missing, it creates the schema and seeds default data. This means:

- No separate migration step
- No manual database setup
- Fresh Docker volumes start with a clean, seeded database automatically

### Schema

All tables include a `user_id` column defaulting to `"default"`. This is hardcoded for now (single-user) but enables future multi-user support without schema migration.

**users_profile** — User state (cash balance)
- `id` TEXT PRIMARY KEY (default: `"default"`)
- `cash_balance` REAL (default: `10000.0`)
- `created_at` TEXT (ISO timestamp)

**watchlist** — Tickers the user is watching
- `id` TEXT PRIMARY KEY (UUID)
- `user_id` TEXT (default: `"default"`)
- `ticker` TEXT
- `added_at` TEXT (ISO timestamp)
- UNIQUE constraint on `(user_id, ticker)`

**positions** — Current holdings (one row per ticker per user)
- `id` TEXT PRIMARY KEY (UUID)
- `user_id` TEXT (default: `"default"`)
- `ticker` TEXT
- `quantity` REAL (fractional shares supported)
- `avg_cost` REAL
- `updated_at` TEXT (ISO timestamp)
- UNIQUE constraint on `(user_id, ticker)`

**trades** — Trade history (append-only log)
- `id` TEXT PRIMARY KEY (UUID)
- `user_id` TEXT (default: `"default"`)
- `ticker` TEXT
- `side` TEXT (`"buy"` or `"sell"`)
- `quantity` REAL (fractional shares supported)
- `price` REAL
- `executed_at` TEXT (ISO timestamp)

**portfolio_snapshots** — Portfolio value over time (for P&L chart). Recorded every 30 seconds by a background task, and immediately after each trade execution.
- `id` TEXT PRIMARY KEY (UUID)
- `user_id` TEXT (default: `"default"`)
- `total_value` REAL
- `recorded_at` TEXT (ISO timestamp)

**chat_messages** — Conversation history with LLM
- `id` TEXT PRIMARY KEY (UUID)
- `user_id` TEXT (default: `"default"`)
- `role` TEXT (`"user"` or `"assistant"`)
- `content` TEXT
- `actions` TEXT (JSON — trades executed, watchlist changes made; null for user messages)
- `created_at` TEXT (ISO timestamp)

### Default Seed Data

- One user profile: `id="default"`, `cash_balance=10000.0`
- Ten watchlist entries: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX

### Write Serialization

All portfolio writes — trade execution, snapshot recording, watchlist mutations — run inside a single SQLite write transaction. A read-then-write sequence (e.g., load cash balance → validate → debit cash → insert position → insert trade) executes within one transaction so that a manual trade and an LLM-triggered trade hitting the same position cannot interleave and leave the cash balance negative or positions out of sync. The trade service exposes a single `execute_trade()` entry point that both the manual `/api/portfolio/trade` route and the LLM action runner call into.

---

## 8. API Endpoints

All endpoints accept and return JSON unless noted. Tickers in request bodies and path params are case-insensitive; the backend normalizes to uppercase before validation, storage, and lookup. Errors use the shape `{"error": "<machine_code>", "message": "<human text>"}` with the HTTP status codes called out per endpoint.

### Market Data

#### `GET /api/stream/prices`

Server-Sent Events stream. Each event has `data` payload:

```json
{
  "ticker": "AAPL",
  "price": 192.45,
  "previous_price": 192.10,
  "change": 0.35,
  "direction": "up",
  "timestamp": "2026-05-09T14:32:01.234Z"
}
```

`direction` is `"up"`, `"down"`, or `"flat"`. The stream emits one event per cache version advance per ticker.

### Portfolio

#### `GET /api/portfolio`

Response (200):

```json
{
  "cash": 8540.32,
  "total_value": 9502.57,
  "total_unrealized_pnl": 11.25,
  "positions": [
    {
      "ticker": "AAPL",
      "quantity": 5.0,
      "avg_cost": 190.20,
      "current_price": 192.45,
      "market_value": 962.25,
      "unrealized_pnl": 11.25,
      "unrealized_pnl_pct": 1.18
    }
  ]
}
```

`current_price` is `null` if the ticker has no cached price yet (e.g., first request before the cache is warm); in that case `market_value` and the P&L fields are also `null` and the position is excluded from `total_value` / `total_unrealized_pnl`.

#### `POST /api/portfolio/trade`

Request:

```json
{"ticker": "AAPL", "side": "buy", "quantity": 5}
```

Validation: `side` ∈ {`"buy"`, `"sell"`}; `quantity` > 0 (fractional allowed); `ticker` must have a cached price.

Response (200):

```json
{
  "trade": {
    "id": "uuid",
    "ticker": "AAPL",
    "side": "buy",
    "quantity": 5,
    "price": 192.45,
    "executed_at": "2026-05-09T14:32:01.234Z"
  },
  "cash": 7578.07
}
```

Errors:
- `400 {"error": "insufficient_cash", "message": "Need $962.25, have $500.00"}`
- `400 {"error": "insufficient_shares", "message": "Have 2.0 shares, requested 5.0"}`
- `400 {"error": "invalid_quantity", "message": "Quantity must be positive"}`
- `404 {"error": "unknown_ticker", "message": "Ticker XYZ not in market data"}`

#### `GET /api/portfolio/history`

Optional query params: `since` (ISO timestamp, default: 24h ago), `limit` (default 1000, max 5000).

Response (200):

```json
{
  "snapshots": [
    {"recorded_at": "2026-05-09T14:00:00Z", "total_value": 10000.00},
    {"recorded_at": "2026-05-09T14:00:30Z", "total_value": 10012.34}
  ]
}
```

### Watchlist

#### `GET /api/watchlist`

Response (200):

```json
{
  "watchlist": [
    {
      "ticker": "AAPL",
      "current_price": 192.45,
      "previous_price": 192.10,
      "change_pct": 0.18
    }
  ]
}
```

`current_price` / `previous_price` / `change_pct` are `null` until the cache has data for that ticker.

#### `POST /api/watchlist`

Request: `{"ticker": "PYPL"}`

Validation: ticker uppercased; must resolve in the market data source (simulator seed list or Massive lookup).

Response (201): `{"ticker": "PYPL", "added_at": "2026-05-09T14:32:01.234Z"}`

Errors:
- `400 {"error": "invalid_ticker_format", "message": "Ticker must be 1-5 letters"}`
- `404 {"error": "unknown_ticker", "message": "Ticker PYPL not available"}`
- `409 {"error": "already_watched", "message": "PYPL is already on the watchlist"}`

#### `DELETE /api/watchlist/{ticker}`

Response (204) on success, no body.

Errors:
- `404 {"error": "not_watched", "message": "PYPL is not on the watchlist"}`

### Chat

#### `POST /api/chat`

Request: `{"message": "buy 5 AAPL"}`

Response (200):

```json
{
  "message": "Bought 5 AAPL at $192.45.",
  "executed_trades": [
    {
      "ticker": "AAPL", "side": "buy", "quantity": 5,
      "status": "ok", "price": 192.45, "trade_id": "uuid"
    }
  ],
  "executed_watchlist_changes": [
    {"ticker": "PYPL", "action": "add", "status": "ok"}
  ]
}
```

For failed actions, the entry has `status: "error"` and an `error` + `message` field instead of the success fields:

```json
{"ticker": "AAPL", "side": "buy", "quantity": 100, "status": "error", "error": "insufficient_cash", "message": "Need $19245.00, have $8540.32"}
```

Action failures do not fail the chat call — the response is still 200 OK so the LLM's text and any other successful actions still reach the user. The `error` field uses the same machine codes as the trade/watchlist endpoints.

`POST /api/chat` itself only errors on infrastructure failures:
- `502 {"error": "llm_unavailable", "message": "..."}` if the LLM provider call fails after retries.

### System

#### `GET /api/health`

Response (200): `{"status": "ok"}`. No deeper probing — Docker/deployment platforms only need a 200.

---

## 9. LLM Integration

LLM calls go through the **`cerebras-inference` Claude Code skill**, which encapsulates LiteLLM-via-OpenRouter calls to `openrouter/openai/gpt-oss-120b` with Cerebras as the inference provider. The skill handles structured-output parsing and provider routing — backend code reaches for the skill rather than calling LiteLLM directly so model and provider settings stay in one place. The `OPENROUTER_API_KEY` lives in `.env`.

### How It Works

When the user sends a chat message, the backend:

1. Loads the user's current portfolio context (cash, positions with P&L, watchlist with live prices, total portfolio value)
2. Loads the last **20 messages** from the `chat_messages` table (10 user + 10 assistant turns, oldest first)
3. Constructs a prompt with the system message (see below), a fresh portfolio context block, the conversation history, and the user's new message
4. Calls the LLM via the `cerebras-inference` skill, requesting structured output matching the schema below
5. Parses the structured JSON response
6. Auto-executes any trades and watchlist changes in the response, collecting per-action outcomes
7. Persists the user message, the assistant message, and the action outcomes (as JSON) into `chat_messages`
8. Returns the complete response (including action outcomes) to the frontend. No token-by-token streaming — Cerebras is fast enough that a single loading indicator suffices.

### Structured Output Schema

The LLM is instructed to respond with JSON matching this schema:

```json
{
  "message": "Your conversational response to the user",
  "trades": [
    {"ticker": "AAPL", "side": "buy", "quantity": 10}
  ],
  "watchlist_changes": [
    {"ticker": "PYPL", "action": "add"}
  ]
}
```

- `message` (required): The conversational text shown to the user
- `trades` (optional): Array of trades to auto-execute. Each trade goes through the same validation as manual trades (sufficient cash, sufficient shares, ticker known to market data)
- `watchlist_changes` (optional): Array of watchlist modifications. Each change goes through the same validation as the watchlist endpoints (ticker resolves on `add`; ticker currently on the list for `remove`)

### Auto-Execution

Trades and watchlist changes specified by the LLM execute automatically — no confirmation dialog. This is a deliberate design choice:
- It's a simulated environment with fake money, so the stakes are zero
- It creates an impressive, fluid demo experience
- It demonstrates agentic AI capabilities — the core theme of the course

When an action fails validation, its failure is reported back in the chat response (`status: "error"` plus `error` machine code and human `message`). The frontend renders successes and failures inline beneath the assistant message — for example, "❌ Trade failed: Need $19245, have $8540". **The backend does not make a second LLM call to rewrite the message**; the original assistant text stays as-is, and the inline action chips communicate what actually happened. Simpler, faster, and the next user turn includes both the original message and the action outcomes in history so the model can self-correct.

### System Prompt

The canonical system prompt is the following. Backend agents may extend it with a portfolio context block (cash, positions, watchlist) injected as a separate system or developer message, but the core instructions stay stable:

```
You are FinAlly, an AI trading assistant embedded in a simulated trading workstation.

The user has a virtual portfolio that started at $10,000 in cash. All trades are simulated — no real money is at stake. Your job is to help the user understand their portfolio, suggest and execute trades, and manage their watchlist.

Behavior:
- Be concise and data-driven. Lead with numbers when relevant.
- When the user asks for analysis, give specific observations about concentration, P&L, and risk.
- When the user asks for or agrees to a trade, include it in `trades`. Do not ask for confirmation — trades execute automatically.
- When the user asks to follow or stop following a ticker, use `watchlist_changes`.
- If you reference a ticker the user does not currently hold or watch, you may proactively add it to the watchlist.
- If a previous turn shows that an action failed (e.g., insufficient cash), acknowledge it and adjust.

Always respond with JSON matching this schema:
{
  "message": "<conversational text shown to the user>",
  "trades": [{"ticker": "<symbol>", "side": "buy"|"sell", "quantity": <number>}],
  "watchlist_changes": [{"ticker": "<symbol>", "action": "add"|"remove"}]
}

`trades` and `watchlist_changes` are optional — omit them or use empty arrays when no action is needed.
```

### LLM Mock Mode

When `LLM_MOCK=true`, the backend returns deterministic mock responses instead of calling OpenRouter. This enables:
- Fast, free, reproducible E2E tests
- Development without an API key
- CI/CD pipelines

---

## 10. Frontend Design

### Layout

The frontend is a single-page application with a dense, terminal-inspired layout. The specific component architecture and layout system is up to the Frontend Engineer, but the UI should include these elements:

- **Watchlist panel** — grid/table of watched tickers with: ticker symbol, current price (flashing green/red on change), daily change %, and a sparkline mini-chart (accumulated from SSE since page load)
- **Main chart area** — larger chart for the currently selected ticker, with at minimum price over time. Clicking a ticker in the watchlist selects it here.
- **Portfolio heatmap** — treemap visualization where each rectangle is a position, sized by portfolio weight, colored by P&L (green = profit, red = loss)
- **P&L chart** — line chart showing total portfolio value over time, using data from `portfolio_snapshots`
- **Positions table** — tabular view of all positions: ticker, quantity, avg cost, current price, unrealized P&L, % change
- **Trade bar** — simple input area: ticker field, quantity field, buy button, sell button. Market orders, instant fill.
- **AI chat panel** — docked/collapsible sidebar. Message input, scrolling conversation history, loading indicator while waiting for LLM response. Trade executions and watchlist changes shown inline as confirmations.
- **Header** — portfolio total value (updating live), connection status indicator, cash balance

### Technical Notes

- Use `EventSource` for SSE connection to `/api/stream/prices`
- **Charting library: Lightweight Charts** (TradingView's open-source canvas library). Picked for streaming performance, small bundle, and built-in support for sparkline-style line series. All charts (sparklines, main chart, P&L) use it.
- Price flash effect: on receiving a new price, briefly apply a CSS class with background color transition, then remove it
- **Sparkline retention**: client keeps the last ~120 points per ticker (about a minute at the simulator's update cadence). Older points are dropped. Sparklines are not persisted across page reloads — they fill in progressively from the SSE stream after refresh.
- **Connection status state machine** (header dot color):
  - **Green** — `EventSource.readyState === OPEN`
  - **Yellow** — `readyState === CONNECTING` *after* a previous OPEN (i.e., a reconnection attempt). Detected by tracking whether OPEN has ever fired on the current EventSource.
  - **Red** — `readyState === CLOSED`, or `CONNECTING` on the very first attempt that has not yet succeeded
- All API calls go to the same origin (`/api/*`) — no CORS configuration needed in production
- Tailwind CSS for styling with a custom dark theme

### Development Workflow

For frontend iteration, run the backend and frontend separately:

```bash
# Terminal 1 — backend
cd backend && uv run uvicorn app.main:app --reload --port 8000

# Terminal 2 — frontend
cd frontend && npm run dev   # Next.js dev server on port 3000
```

Configure `next.config.js` with `rewrites()` to proxy `/api/*` and `/api/stream/*` to `http://localhost:8000`, so the frontend dev server forwards API calls to the backend without CORS. Production builds use `next build` with `output: 'export'` and are served as static files by FastAPI on port 8000.

---

## 11. Docker & Deployment

### Multi-Stage Dockerfile

```
Stage 1: Node 20 slim
  - Copy frontend/
  - npm ci && npm run build (produces static export — npm ci for reproducible installs from package-lock.json)

Stage 2: Python 3.12 slim
  - Install uv
  - Copy backend/
  - uv sync (install Python dependencies from lockfile)
  - Copy frontend build output into a static/ directory
  - Expose port 8000
  - CMD: uvicorn serving FastAPI app
```

FastAPI serves the static frontend files and all API routes on port 8000.

### Docker Volume

The SQLite database persists via a bind-mount of the project's `db/` directory into the container:

```bash
docker run -v "$(pwd)/db:/app/db" -p 8000:8000 --env-file .env finally
```

The `db/` directory in the project root maps to `/app/db` in the container. The backend writes `finally.db` to this path. Bind-mount (rather than a named volume) is intentional: students can inspect `db/finally.db` directly from the host with any SQLite tool, and the file survives container removal as a regular file.

### Start/Stop Scripts

**`scripts/start_mac.sh`** (macOS/Linux):
- Builds the Docker image if not already built (or if `--build` flag passed)
- Runs the container with the volume mount, port mapping, and `.env` file
- Prints the URL to access the app
- Optionally opens the browser

**`scripts/stop_mac.sh`** (macOS/Linux):
- Stops and removes the running container
- Does NOT remove the volume (data persists)

**`scripts/start_windows.ps1`** / **`scripts/stop_windows.ps1`**: PowerShell equivalents for Windows.

All scripts should be idempotent — safe to run multiple times.

### Optional Cloud Deployment

The container is designed to deploy to AWS App Runner, Render, or any container platform. A Terraform configuration for App Runner may be provided in a `deploy/` directory as a stretch goal, but is not part of the core build.

---

## 12. Testing Strategy

### Unit Tests (within `frontend/` and `backend/`)

**Backend (pytest)**:
- Market data: simulator generates valid prices, GBM math is correct, Massive API response parsing works, both implementations conform to the abstract interface
- Portfolio: trade execution logic, P&L calculations, edge cases (selling more than owned, buying with insufficient cash, selling at a loss)
- LLM: structured output parsing handles all valid schemas, graceful handling of malformed responses, trade validation within chat flow
- API routes: correct status codes, response shapes, error handling

**Frontend (React Testing Library or similar)**:
- Component rendering with mock data
- Price flash animation triggers correctly on price changes
- Watchlist CRUD operations
- Portfolio display calculations
- Chat message rendering and loading state

### E2E Tests (in `test/`)

**Infrastructure**: A separate `docker-compose.test.yml` in `test/` that spins up the app container plus a Playwright container. This keeps browser dependencies out of the production image.

**Environment**: Tests run with `LLM_MOCK=true` by default for speed and determinism.

**Key Scenarios**:
- Fresh start: default watchlist appears, $10k balance shown, prices are streaming
- Add and remove a ticker from the watchlist
- Buy shares: cash decreases, position appears, portfolio updates
- Sell shares: cash increases, position updates or disappears
- Portfolio visualization: heatmap renders with correct colors, P&L chart has data points
- AI chat (mocked): send a message, receive a response, trade execution appears inline
- SSE resilience: disconnect and verify reconnection

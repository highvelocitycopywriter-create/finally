# FinAlly — AI Trading Workstation

A Bloomberg-style AI trading workstation: live streaming prices, a simulated $10k portfolio, and an LLM chat assistant that can analyze positions and execute trades via natural language.

Built entirely by coding agents as the capstone for an agentic AI coding course. The full specification lives in [`planning/PLAN.md`](planning/PLAN.md).

## Status

Work in progress. The market data subsystem (simulator + Massive API client + SSE streaming + price cache) is implemented in `backend/app/market/` — see [`planning/MARKET_DATA_SUMMARY.md`](planning/MARKET_DATA_SUMMARY.md). Portfolio, watchlist, chat, and frontend are still to be built.

## Architecture

Single Docker container on port 8000:

- **Backend**: FastAPI (Python, managed with `uv`), SQLite, SSE streaming
- **Frontend** (planned): Next.js static export with Tailwind, Lightweight Charts
- **AI** (planned): LiteLLM via OpenRouter (Cerebras inference) with structured outputs
- **Market data**: GBM simulator by default; Massive (Polygon.io) API if `MASSIVE_API_KEY` is set

## Quick Start (backend, today)

```bash
cd backend
uv sync --extra dev
uv run pytest                 # run tests
uv run market_data_demo.py    # live terminal demo of the simulator
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `OPENROUTER_API_KEY` | Yes (for chat) | OpenRouter key for the LLM assistant |
| `MASSIVE_API_KEY` | No | Polygon.io key for real market data; omit to use the simulator |
| `LLM_MOCK` | No | `true` for deterministic mock LLM responses (testing) |

## Project Structure

```
finally/
├── backend/      FastAPI uv project (market data implemented)
├── planning/     Specification and agent contracts
├── frontend/     Next.js app (planned)
├── test/         Playwright E2E tests (planned)
└── db/           SQLite volume mount (runtime)
```

## License

See [LICENSE](LICENSE).

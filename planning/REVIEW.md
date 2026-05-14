# Review Findings

1. High — `planning/PLAN.md:448-457` turns the runtime LLM integration into a Claude Code skill dependency, which the deployed backend cannot actually call. The rest of the plan and README still describe a normal app architecture (`LiteLLM -> OpenRouter`), but these new lines say backend code should "reach for" a local agent skill instead of a Python library or HTTP client. That makes the implementation path undefined for production and for tests. Keep the skill as developer guidance if you want, but the runtime contract needs to stay expressed in application terms.

2. Medium — `planning/PLAN.md:190-191` and `planning/PLAN.md:272-287` now document `/api/stream/prices` as emitting one per-ticker event with an ISO timestamp. The implemented market subsystem does not do that: [`backend/app/market/stream.py`](/Users/ryanlemos/projects/finally/backend/app/market/stream.py:30) and [`backend/app/market/stream.py`](/Users/ryanlemos/projects/finally/backend/app/market/stream.py:75) send a full `{ticker -> update}` map whenever the cache version changes, and [`backend/app/market/models.py`](/Users/ryanlemos/projects/finally/backend/app/market/models.py:41) serializes `timestamp` as Unix seconds. Since `planning/MARKET_DATA_SUMMARY.md` is called out as the source of truth for the completed subsystem, this plan update now contradicts the existing implementation contract and will mislead the frontend/API work that follows.

3. Medium — `planning/PLAN.md:388-395` says watchlist adds must resolve against the simulator seed list and even uses `PYPL` as a `404 unknown_ticker` example. That conflicts with the implemented simulator, which explicitly accepts unknown tickers and assigns them default seed prices/params in [`backend/app/market/simulator.py`](/Users/ryanlemos/projects/finally/backend/app/market/simulator.py:146). If future watchlist/chat work follows the new plan literally, it will reject symbols that the current market layer already supports.

4. Low — `planning/PLAN.md:124` still says `db/` persists "via Docker volume", but `planning/PLAN.md:594-600` was changed to a bind-mount model. The document now contains two different persistence stories, which is likely to confuse anyone writing the Docker scripts or onboarding docs.

## Open Questions

- Was the intent to change the market data contract, or should `PLAN.md` continue to mirror the already-shipped behavior described in `planning/MARKET_DATA_SUMMARY.md`?
- If the runtime really should use OpenRouter directly, the LLM section should probably describe the Python integration boundary explicitly instead of referencing an editor/agent skill.

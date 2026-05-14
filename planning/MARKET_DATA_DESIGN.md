# Market Data Backend — Detailed Design

Implementation-ready reference for the FinAlly market data subsystem. All code in this document reflects what is actually implemented in `backend/app/market/` (8 modules, ~500 lines, 73 passing tests). Use `planning/MARKET_DATA_SUMMARY.md` for a brief status summary; use this document when you need to understand how something works or how to wire it into adjacent subsystems.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model--modelspy)
4. [Price Cache — `cache.py`](#4-price-cache--cachepy)
5. [Abstract Interface — `interface.py`](#5-abstract-interface--interfacepy)
6. [Seed Data — `seed_prices.py`](#6-seed-data--seed_pricespy)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator--simulatorpy)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client--massive_clientpy)
9. [Factory — `factory.py`](#9-factory--factorypy)
10. [SSE Streaming Endpoint — `stream.py`](#10-sse-streaming-endpoint--streampy)
11. [FastAPI Lifecycle Integration](#11-fastapi-lifecycle-integration)
12. [Watchlist Coordination](#12-watchlist-coordination)
13. [Public Package API](#13-public-package-api)
14. [Testing Strategy](#14-testing-strategy)
15. [Error Handling & Edge Cases](#15-error-handling--edge-cases)
16. [Configuration Reference](#16-configuration-reference)

---

## 1. Architecture Overview

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator, 500ms ticks (default — no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller, 15s polls (when MASSIVE_API_KEY set)
        │
        ▼  (both write to)
   PriceCache  (thread-safe, in-memory, version-stamped)
        │
        ├──→ GET /api/stream/prices  (SSE, version-based change detection)
        ├──→ GET /api/portfolio       (current_price per position)
        └──→ POST /api/portfolio/trade (price at execution time)
```

### Key design decisions

| Decision | Rationale |
|----------|-----------|
| Strategy pattern (ABC) | Simulator and Massive are drop-in replacements; downstream code is source-agnostic |
| Push to cache, not pull | Data source ticks on its own schedule; SSE reads whenever it wants — no coupling |
| `threading.Lock` (not `asyncio.Lock`) | Massive's synchronous REST client runs in a real OS thread via `asyncio.to_thread()` |
| Version counter on cache | SSE skips sends when nothing changed; cheap change detection without comparing payloads |
| GBM with Cholesky correlations | Sector-grouped moves look realistic; Cholesky is O(n²) but n < 50 |

---

## 2. File Structure

```
backend/app/market/
    __init__.py          # Public re-exports
    models.py            # PriceUpdate dataclass
    cache.py             # PriceCache — thread-safe in-memory store
    interface.py         # MarketDataSource ABC
    seed_prices.py       # Seed prices, GBM params, correlation groups
    simulator.py         # GBMSimulator + SimulatorDataSource
    massive_client.py    # MassiveDataSource (Polygon.io REST)
    factory.py           # create_market_data_source() — env-driven selection
    stream.py            # SSE FastAPI router factory

backend/tests/market/
    test_models.py           # 11 tests — PriceUpdate (100% coverage)
    test_cache.py            # 13 tests — PriceCache (100% coverage)
    test_simulator.py        # 17 tests — GBMSimulator (98% coverage)
    test_simulator_source.py # 10 tests — SimulatorDataSource (integration)
    test_factory.py          # 7 tests  — factory (100% coverage)
    test_massive.py          # 13 tests — MassiveDataSource (mocked, 56% coverage)
```

---

## 3. Data Model — `models.py`

`PriceUpdate` is the only type that leaves the market data layer. All consumers (SSE, portfolio, trade execution) use it.

```python
# backend/app/market/models.py

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,   # Unix seconds (float)
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

**Design notes:**
- `frozen=True` — immutable value object; safe to share across async tasks without copying
- `slots=True` — minor memory optimization; many instances created per second
- All computed fields (`change`, `direction`, `change_percent`) are derived from `price` and `previous_price` so they can never be inconsistent
- `timestamp` is Unix seconds (float), **not** an ISO string — the SSE client and frontend receive it as-is and format it locally
- `to_dict()` is the single serialization point used by both SSE and REST responses

---

## 4. Price Cache — `cache.py`

The central data hub. One writer (the active data source), many concurrent readers (SSE streams, trade execution, portfolio valuation).

```python
# backend/app/market/cache.py

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker."""

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price. Returns the created PriceUpdate.

        First update for a ticker: previous_price == price (direction='flat').
        Subsequent updates: previous_price is the prior price.
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Monotonic counter. Increments on every update. Used by SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why `threading.Lock`, not `asyncio.Lock`

The Massive API client calls `self._client.get_snapshot_all()` (a blocking synchronous function) inside `asyncio.to_thread()`, which runs it on a real OS thread from the default thread pool. An `asyncio.Lock` only serializes within a single event loop thread and would not protect against concurrent access from that thread pool thread. `threading.Lock` is a true OS mutex that works correctly across both event loop tasks and thread pool workers.

### Why a version counter

The SSE generator polls the cache every 500ms. Without a version counter, it would serialize and push all prices every tick even when nothing changed — wasting CPU and bandwidth, especially with Massive's 15-second poll interval where prices are stale for 14+ seconds between updates.

```python
# SSE generator sketch (simplified)
last_version = -1
while True:
    current_version = price_cache.version
    if current_version != last_version:
        last_version = current_version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

---

## 5. Abstract Interface — `interface.py`

All downstream code depends on this ABC, not on the concrete simulator or Massive classes.

```python
# backend/app/market/interface.py

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their
    own schedule. Downstream code reads from the cache — never calls the
    source directly for prices.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... runtime: add/remove tickers as watchlist changes ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... shutdown:
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates. Call exactly once."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker. Also removes it from the PriceCache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Push vs pull

The interface does not expose a `get_prices()` method. The source pushes updates into the `PriceCache` on its own schedule. Consumers read from the cache whenever they need prices. This decouples update cadence (500ms simulator, 15s Massive) from read cadence (SSE polling at 500ms, trade requests on-demand).

---

## 6. Seed Data — `seed_prices.py`

Constants only — no logic, no imports. Shared by the simulator and potentially by tests.

```python
# backend/app/market/seed_prices.py

# Realistic starting prices for the default watchlist
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more movement per tick)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},  # High volatility, lower drift
    "NVDA":  {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

# Fallback for dynamically-added tickers not in the list above
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Sector groups for the Cholesky correlation matrix
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Pairwise correlation coefficients
INTRA_TECH_CORR    = 0.6   # Tech stocks move together
INTRA_FINANCE_CORR = 0.5   # Finance stocks move together
CROSS_GROUP_CORR   = 0.3   # Between sectors, or unknown tickers
TSLA_CORR          = 0.3   # TSLA is in tech but does its own thing
```

**Unknown tickers:** When a user adds a ticker not in `SEED_PRICES` (e.g., `PYPL`), the simulator assigns it a random seed price in `[50.0, 300.0]` and uses `DEFAULT_PARAMS`. It does **not** reject the ticker. Watchlist routes should not validate against `SEED_PRICES`.

---

## 7. GBM Simulator — `simulator.py`

Two classes in one file:
- **`GBMSimulator`** — pure math engine, stateful, synchronous
- **`SimulatorDataSource`** — async wrapper, implements `MarketDataSource`, owns the background task

### 7.1 GBMSimulator — Math Engine

```python
# backend/app/market/simulator.py  (GBMSimulator only)

import math
import random
import numpy as np

from .seed_prices import (
    CORRELATION_GROUPS, CROSS_GROUP_CORR, DEFAULT_PARAMS,
    INTRA_FINANCE_CORR, INTRA_TECH_CORR, SEED_PRICES,
    TICKER_PARAMS, TSLA_CORR,
)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma²/2) * dt + sigma * sqrt(dt) * Z)

    Where Z is a correlated standard normal variable produced by
    Cholesky decomposition of a sector-based correlation matrix.

    dt ≈ 8.48e-8 (500ms as a fraction of a 252-day trading year).
    This tiny dt produces sub-cent moves per tick that accumulate naturally.
    """

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers one time step. Returns {ticker: new_price}.

        Hot path — called every 500ms. Keeps allocations minimal.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            drift = (mu - 0.5 * sigma ** 2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec → ~1 event per 50 seconds
            if random.random() < self._event_prob:
                magnitude = random.uniform(0.02, 0.05)
                sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + magnitude * sign

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add without rebuilding Cholesky (for batch init)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild Cholesky decomposition of the correlation matrix.

        Called on every add/remove. O(n²) but n < 50 in practice.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Sector-based pairwise correlation.

        - Same tech group:    0.6
        - Same finance group: 0.5
        - TSLA (either side): 0.3  (TSLA does its own thing)
        - Cross-sector:       0.3
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

### How the Cholesky math works

Cholesky decomposition converts a correlation matrix into a lower-triangular matrix `L` such that `L @ L.T == corr`. To generate correlated normals:

1. Draw `n` independent standard normals: `z_independent ~ N(0, I)`
2. Multiply: `z_correlated = L @ z_independent`

The resulting `z_correlated` has the desired correlation structure. Each ticker then uses its own `z_correlated[i]` as the stochastic term in GBM.

### 7.2 SimulatorDataSource — Async Wrapper

```python
# backend/app/market/simulator.py  (SimulatorDataSource)

import asyncio
import logging

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by GBMSimulator.

    Runs a background asyncio task that calls step() every 500ms and
    writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)

        # Seed cache immediately — SSE has prices before the first loop tick
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

**Immediate cache seeding:** `start()` calls `_cache.update()` for each ticker before launching the background loop. This means the SSE endpoint has prices to send on its very first tick, with no blank-screen delay.

**Exception resilience:** The `_run_loop` catches exceptions per-step. A single bad tick does not crash the feed.

---

## 8. Massive API Client — `massive_client.py`

Polls Polygon.io (via the `massive` package) for all watched tickers in a single API call. The synchronous REST client runs in a thread pool to avoid blocking the event loop.

```python
# backend/app/market/massive_client.py

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Immediate first poll — cache has data before the interval loop fires
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added %s (appears on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # RESTClient is synchronous — run in thread to avoid blocking the event loop
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
            logger.debug("Massive poll: %d/%d tickers updated", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — loop retries on next interval.
            # Common failures: 401 bad key, 429 rate limit, network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous REST call. Runs in asyncio.to_thread()."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Error handling summary

| Failure | Behavior |
|---------|----------|
| 401 Unauthorized | Logged as error. Poller keeps running — user can fix `.env` and restart. |
| 429 Rate Limited | Logged as error. Retries after `poll_interval` seconds. |
| Network timeout | Logged as error. Auto-retries next cycle. |
| Malformed snapshot | That ticker is skipped with a warning; others are still processed. |
| All tickers fail | Cache retains last-known prices. SSE keeps streaming stale-but-valid data. |

### UX note on Massive vs simulator

Massive's free tier polls every 15 seconds. Price flashes and sparkline updates are visibly sparser than the simulator's 500ms cadence. This is expected behavior — the frontend is tuned for the simulator's update rate, and Massive is "real but slow." Don't tune the UI for the Massive cadence.

---

## 9. Factory — `factory.py`

Selects the data source from the environment. The rest of the app never instantiates a data source directly.

```python
# backend/app/market/factory.py

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source from the environment.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

### Usage at startup

```python
cache = PriceCache()
source = create_market_data_source(cache)   # reads MASSIVE_API_KEY
initial_tickers = await load_watchlist_from_db()
await source.start(initial_tickers)
```

---

## 10. SSE Streaming Endpoint — `stream.py`

A FastAPI router factory that creates the `/api/stream/prices` SSE endpoint. Factory pattern avoids global state — the `PriceCache` is injected rather than imported.

```python
# backend/app/market/stream.py

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE router with an injected PriceCache reference."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Prevent nginx from buffering SSE
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Version-based: only emits when the cache has changed since the last send.
    Stops when the client disconnects.
    """
    yield "retry: 1000\n\n"   # Tell EventSource to reconnect after 1s

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Wire format

Each SSE event is a single `data:` line containing a JSON object. The object maps every currently-tracked ticker to its full `PriceUpdate` dict:

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1715697600.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{...},...}

```

Note the blank line terminating each event — this is the SSE spec requirement.

### Frontend EventSource usage

```javascript
const es = new EventSource('/api/stream/prices');

es.onmessage = (event) => {
    // prices: { [ticker]: { ticker, price, previous_price, timestamp, change, change_percent, direction } }
    const prices = JSON.parse(event.data);
    for (const [ticker, update] of Object.entries(prices)) {
        updateWatchlistRow(ticker, update);
        appendSparklinePoint(ticker, update.price);
        if (ticker === selectedTicker) updateMainChart(update);
    }
};

es.onerror = () => {
    setConnectionStatus('reconnecting');   // EventSource auto-reconnects
};

es.addEventListener('open', () => {
    setConnectionStatus('connected');
});
```

### Connection status state machine

Implement this in the frontend using the `readyState` property and open/error event tracking:

| State | Condition | Header dot color |
|-------|-----------|-----------------|
| Connected | `es.readyState === EventSource.OPEN` | Green |
| Reconnecting | `es.readyState === EventSource.CONNECTING` and OPEN has fired at least once | Yellow |
| Disconnected | `es.readyState === EventSource.CLOSED`, or CONNECTING on very first attempt that hasn't succeeded | Red |

### Why all tickers per event (not one event per ticker)

The SSE generator sends the full price map on each cache version change, not one event per ticker. This matches how the cache version counter works — each call to `cache.update()` increments the version, so with 10 tickers and a 500ms loop, you'd get 10 separate events per tick if you emitted one per update. A single batched event is simpler to parse and reduces frontend render thrash.

---

## 11. FastAPI Lifecycle Integration

The market data system starts and stops with the FastAPI application via the `lifespan` context manager.

```python
# backend/app/main.py

from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends

from app.market import PriceCache, MarketDataSource, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---

    # 1. Shared price cache — one instance for the app lifetime
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    # 2. Market data source (simulator or Massive, based on env)
    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # 3. Load initial tickers from the DB watchlist
    #    (DB is initialized lazily; this also triggers schema creation)
    initial_tickers = await get_watchlist_tickers_from_db()
    await source.start(initial_tickers)

    # 4. Register the SSE router (price_cache injected here, not globally)
    app.include_router(create_stream_router(price_cache))

    yield  # App is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


# Dependency functions for route injection
def get_price_cache() -> PriceCache:
    return app.state.price_cache

def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

### Injecting into route handlers

Other API routes receive the price cache and data source via FastAPI's dependency injection system:

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.get("/watchlist")
async def get_watchlist(
    price_cache: PriceCache = Depends(get_price_cache),
):
    tickers = await db.get_watchlist()
    result = []
    for ticker in tickers:
        update = price_cache.get(ticker)
        result.append({
            "ticker": ticker,
            "current_price": update.price if update else None,
            "previous_price": update.previous_price if update else None,
            "change_pct": update.change_percent if update else None,
        })
    return {"watchlist": result}


@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    price = price_cache.get_price(trade.ticker.upper())
    if price is None:
        raise HTTPException(404, detail={"error": "unknown_ticker", "message": f"Ticker {trade.ticker} not in market data"})
    # ... execute at price ...


@router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
    price_cache: PriceCache = Depends(get_price_cache),
):
    ticker = payload.ticker.upper()
    await db.insert_watchlist_entry(ticker)
    await source.add_ticker(ticker)     # Simulator seeds cache immediately
    update = price_cache.get(ticker)    # Available right away for simulator
    return {"ticker": ticker, "added_at": ..., "current_price": update.price if update else None}


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    ticker = ticker.upper()
    await db.delete_watchlist_entry(ticker)
    # Keep tracking if user holds a position (for portfolio valuation)
    position = await db.get_position(ticker)
    if not position or position.quantity == 0:
        await source.remove_ticker(ticker)
    return Response(status_code=204)
```

---

## 12. Watchlist Coordination

When the watchlist changes, the market data source must be notified so it tracks the correct set of tickers.

### Adding a ticker

```
POST /api/watchlist {"ticker": "PYPL"}
  → Validate ticker format (1-5 letters, uppercase)
  → Insert into watchlist table (SQLite)
  → await source.add_ticker("PYPL")
      Simulator:
          GBMSimulator.add_ticker("PYPL")
          → assigns random seed price (50-300) since PYPL not in SEED_PRICES
          → rebuilds Cholesky matrix
          SimulatorDataSource.add_ticker("PYPL")
          → seeds cache with initial price immediately
      Massive:
          Appends "PYPL" to self._tickers
          → appears in next poll (up to 15s delay)
  → Return 201 with ticker + added_at + current_price
```

For the simulator, `current_price` in the response is available immediately. For Massive, `current_price` may be `null` until the next poll completes.

### Removing a ticker

```
DELETE /api/watchlist/PYPL
  → Lookup in watchlist table — 404 if not found
  → Delete from watchlist table
  → Check positions table:
      If position exists and quantity > 0:
          Do NOT remove from data source (still needed for portfolio valuation)
      Else:
          await source.remove_ticker("PYPL")
          → also removes from cache
  → Return 204 No Content
```

### LLM-triggered changes

The chat endpoint calls `source.add_ticker()` / `source.remove_ticker()` through the same watchlist service functions as the REST API — not directly. This ensures database and data source state stay in sync regardless of who initiated the change.

---

## 13. Public Package API

```python
# backend/app/market/__init__.py

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

Downstream code imports from `app.market`, never from submodules:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source

# Startup
cache = PriceCache()
source = create_market_data_source(cache)
await source.start(["AAPL", "GOOGL", "MSFT", ...])

# Read prices
update: PriceUpdate | None = cache.get("AAPL")
price: float | None = cache.get_price("AAPL")
all_prices: dict[str, PriceUpdate] = cache.get_all()

# Dynamic watchlist
await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

# Shutdown
await source.stop()
```

---

## 14. Testing Strategy

All tests live in `backend/tests/market/`. Run with:

```bash
cd backend
uv run --extra dev pytest tests/market/ -v
uv run --extra dev pytest tests/market/ --cov=app/market  # with coverage
```

### 14.1 PriceUpdate model tests

```python
# backend/tests/market/test_models.py

from app.market.models import PriceUpdate


def test_direction_up():
    u = PriceUpdate(ticker="AAPL", price=191.00, previous_price=190.00)
    assert u.direction == "up"
    assert u.change == 1.00
    assert u.change_percent == pytest.approx(0.5263, rel=1e-3)

def test_direction_down():
    u = PriceUpdate(ticker="AAPL", price=189.00, previous_price=190.00)
    assert u.direction == "down"
    assert u.change == -1.00

def test_direction_flat():
    u = PriceUpdate(ticker="AAPL", price=190.00, previous_price=190.00)
    assert u.direction == "flat"
    assert u.change == 0.00

def test_to_dict_keys():
    u = PriceUpdate(ticker="AAPL", price=190.00, previous_price=189.00)
    d = u.to_dict()
    assert set(d.keys()) == {"ticker", "price", "previous_price", "timestamp", "change", "change_percent", "direction"}

def test_frozen():
    u = PriceUpdate(ticker="AAPL", price=190.00, previous_price=189.00)
    with pytest.raises(Exception):  # FrozenInstanceError
        u.price = 200.00

def test_zero_previous_price_no_division_error():
    u = PriceUpdate(ticker="X", price=100.00, previous_price=0.0)
    assert u.change_percent == 0.0
```

### 14.2 PriceCache tests

```python
# backend/tests/market/test_cache.py

from app.market.cache import PriceCache


def test_first_update_is_flat():
    cache = PriceCache()
    update = cache.update("AAPL", 190.50)
    assert update.direction == "flat"
    assert update.previous_price == 190.50

def test_subsequent_update_tracks_previous():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    update = cache.update("AAPL", 192.00)
    assert update.previous_price == 190.00
    assert update.direction == "up"

def test_version_increments_on_every_update():
    cache = PriceCache()
    v0 = cache.version
    cache.update("AAPL", 190.00)
    cache.update("GOOGL", 175.00)
    assert cache.version == v0 + 2

def test_remove_clears_ticker():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    cache.remove("AAPL")
    assert cache.get("AAPL") is None
    assert "AAPL" not in cache

def test_get_all_returns_copy():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    snapshot = cache.get_all()
    snapshot["FAKE"] = None      # Mutate the copy
    assert "FAKE" not in cache.get_all()  # Original unchanged

def test_get_price_none_for_unknown():
    cache = PriceCache()
    assert cache.get_price("NOPE") is None

def test_custom_timestamp_used():
    cache = PriceCache()
    update = cache.update("AAPL", 190.00, timestamp=1234567890.0)
    assert update.timestamp == 1234567890.0
```

### 14.3 GBMSimulator tests

```python
# backend/tests/market/test_simulator.py

import pytest
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


def test_step_returns_all_tickers():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    result = sim.step()
    assert set(result.keys()) == {"AAPL", "GOOGL"}

def test_prices_are_always_positive():
    """GBM prices can never go negative (exp() is always positive)."""
    sim = GBMSimulator(tickers=["AAPL", "TSLA"])
    for _ in range(5000):
        prices = sim.step()
        for price in prices.values():
            assert price > 0

def test_initial_prices_match_seeds():
    sim = GBMSimulator(tickers=["AAPL"])
    assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]

def test_unknown_ticker_gets_random_price_in_range():
    sim = GBMSimulator(tickers=["ZZZZ"])
    price = sim.get_price("ZZZZ")
    assert 50.0 <= price <= 300.0

def test_cholesky_none_for_single_ticker():
    sim = GBMSimulator(tickers=["AAPL"])
    assert sim._cholesky is None

def test_cholesky_built_for_two_tickers():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    assert sim._cholesky is not None

def test_add_ticker_included_in_next_step():
    sim = GBMSimulator(tickers=["AAPL"])
    sim.add_ticker("TSLA")
    result = sim.step()
    assert "TSLA" in result

def test_remove_ticker_excluded_from_next_step():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    sim.remove_ticker("GOOGL")
    result = sim.step()
    assert "GOOGL" not in result
    assert "AAPL" in result

def test_add_duplicate_is_noop():
    sim = GBMSimulator(tickers=["AAPL"])
    sim.add_ticker("AAPL")
    assert sim.get_tickers() == ["AAPL"]

def test_remove_nonexistent_is_noop():
    sim = GBMSimulator(tickers=["AAPL"])
    sim.remove_ticker("NOPE")   # Should not raise

def test_empty_step():
    sim = GBMSimulator(tickers=[])
    assert sim.step() == {}

def test_prices_change_over_many_steps():
    sim = GBMSimulator(tickers=["AAPL"])
    initial = sim.get_price("AAPL")
    for _ in range(1000):
        sim.step()
    # Extremely unlikely to be exactly the same after 1000 steps
    assert sim.get_price("AAPL") != initial
```

### 14.4 SimulatorDataSource integration tests

```python
# backend/tests/market/test_simulator_source.py

import asyncio
import pytest
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


@pytest.mark.asyncio
class TestSimulatorDataSource:

    async def test_start_populates_cache_immediately(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])
        # Cache must have data before any loop tick fires
        assert cache.get("AAPL") is not None
        assert cache.get("GOOGL") is not None
        await source.stop()

    async def test_add_ticker_seeds_cache_immediately(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10.0)
        await source.start(["AAPL"])
        await source.add_ticker("TSLA")
        assert cache.get("TSLA") is not None
        assert "TSLA" in source.get_tickers()
        await source.stop()

    async def test_remove_ticker_clears_cache(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10.0)
        await source.start(["AAPL", "TSLA"])
        await source.remove_ticker("TSLA")
        assert cache.get("TSLA") is None
        assert "TSLA" not in source.get_tickers()
        await source.stop()

    async def test_stop_is_idempotent(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        await source.stop()
        await source.stop()   # Second stop must not raise

    async def test_cache_updates_while_running(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)
        await source.start(["AAPL"])
        v0 = cache.version
        await asyncio.sleep(0.25)   # Allow ~5 update cycles
        assert cache.version > v0
        await source.stop()
```

### 14.5 MassiveDataSource tests (mocked)

```python
# backend/tests/market/test_massive.py

from unittest.mock import MagicMock, patch
import pytest
from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def _snap(ticker: str, price: float, ts_ms: int = 1707580800000) -> MagicMock:
    s = MagicMock()
    s.ticker = ticker
    s.last_trade.price = price
    s.last_trade.timestamp = ts_ms
    return s


@pytest.mark.asyncio
class TestMassiveDataSource:

    async def test_poll_updates_cache(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="key", price_cache=cache, poll_interval=999)
        source._client = MagicMock()
        source._tickers = ["AAPL", "GOOGL"]

        with patch.object(source, "_fetch_snapshots", return_value=[_snap("AAPL", 190.50), _snap("GOOGL", 175.25)]):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("GOOGL") == 175.25

    async def test_malformed_snapshot_skipped(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="key", price_cache=cache, poll_interval=999)
        source._client = MagicMock()
        source._tickers = ["AAPL", "BAD"]

        bad = MagicMock()
        bad.ticker = "BAD"
        bad.last_trade = None   # AttributeError when accessed

        with patch.object(source, "_fetch_snapshots", return_value=[_snap("AAPL", 190.50), bad]):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None

    async def test_api_error_does_not_crash(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="key", price_cache=cache, poll_interval=999)
        source._client = MagicMock()
        source._tickers = ["AAPL"]

        with patch.object(source, "_fetch_snapshots", side_effect=Exception("429 rate limit")):
            await source._poll_once()   # Must not raise

        assert cache.get_price("AAPL") is None

    async def test_add_and_remove_ticker(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="key", price_cache=cache, poll_interval=999)
        source._tickers = ["AAPL"]

        await source.add_ticker("tsla")  # lowercase input
        assert "TSLA" in source.get_tickers()

        with patch.object(source, "_fetch_snapshots", return_value=[_snap("TSLA", 250.00)]):
            await source._poll_once()
        assert cache.get_price("TSLA") == 250.00

        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get_price("TSLA") is None

    async def test_timestamp_converted_from_ms_to_seconds(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="key", price_cache=cache, poll_interval=999)
        source._client = MagicMock()
        source._tickers = ["AAPL"]

        with patch.object(source, "_fetch_snapshots", return_value=[_snap("AAPL", 190.00, ts_ms=1707580800000)]):
            await source._poll_once()

        update = cache.get("AAPL")
        assert update.timestamp == pytest.approx(1707580800.0)   # ms → seconds
```

### 14.6 Factory tests

```python
# backend/tests/market/test_factory.py

import os
from unittest.mock import patch
from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.simulator import SimulatorDataSource
from app.market.massive_client import MassiveDataSource


def test_no_key_returns_simulator():
    cache = PriceCache()
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("MASSIVE_API_KEY", None)
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)

def test_empty_key_returns_simulator():
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": ""}):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)

def test_whitespace_key_returns_simulator():
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)

def test_valid_key_returns_massive():
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "real-api-key-abc123"}):
        source = create_market_data_source(cache)
    assert isinstance(source, MassiveDataSource)
```

---

## 15. Error Handling & Edge Cases

### 15.1 Empty watchlist at startup

If the database has no watchlist entries, `source.start([])` is called. Both sources handle it gracefully — the simulator produces no prices, the Massive poller skips its API call. The SSE endpoint emits empty events. When the user adds the first ticker, the source starts tracking it immediately.

### 15.2 Price cache miss during trade

If a trade request arrives for a ticker with no cached price (race between watchlist add and simulator seeding, or Massive gap):

```python
price = price_cache.get_price(ticker)
if price is None:
    raise HTTPException(
        status_code=404,
        detail={"error": "unknown_ticker", "message": f"Ticker {ticker} not in market data"},
    )
```

The simulator avoids this by seeding the cache synchronously in `add_ticker()`. Massive has a brief gap until the next poll. The 404 with a clear error code is the correct response.

### 15.3 Ticker added while simulator loop is mid-step

`GBMSimulator.add_ticker()` modifies `self._tickers` and `self._prices` in-place. The simulator's background loop runs in the asyncio event loop (not a thread), so there is no concurrent modification — the event loop is cooperative and `asyncio.sleep()` is the only yield point. Thread safety is not needed here.

The `PriceCache` still uses `threading.Lock` because Massive's REST client runs in a thread pool.

### 15.4 Many concurrent SSE clients

Each SSE connection is an independent async generator that reads from the shared `PriceCache` via `get_all()`. The lock is held briefly (dict copy), then released. With 10 tickers and 100 concurrent connections, lock contention is negligible. There is no pub-sub fan-out — each client independently checks the version counter and reads the cache.

### 15.5 Removing a ticker with an open position

The watchlist `DELETE` handler must check for an open position before calling `source.remove_ticker()`. If the user holds shares of `AAPL` but removes it from the watchlist, `AAPL` must stay in the data source so portfolio valuation can compute unrealized P&L. The ticker disappears from the watchlist UI but continues to have a live price.

### 15.6 GBM precision and floor

GBM using the exponential formulation (`exp(drift + diffusion)`) guarantees prices are always positive — `exp()` is always > 0. Prices are rounded to 2 decimal places in `step()`. There is no explicit price floor; the math guarantees positivity.

---

## 16. Configuration Reference

| Parameter | Set via | Default | Effect |
|-----------|---------|---------|--------|
| `MASSIVE_API_KEY` | Environment variable | `""` | Empty → simulator; non-empty → Massive API |
| `SimulatorDataSource.update_interval` | Constructor | `0.5` s | Time between GBM steps |
| `SimulatorDataSource.event_probability` | Constructor | `0.001` | Probability of a random shock per ticker per tick |
| `GBMSimulator.dt` | Constructor | `~8.48e-8` | GBM time step (0.5s as fraction of a trading year) |
| `MassiveDataSource.poll_interval` | Constructor | `15.0` s | Time between Massive REST polls |
| `_generate_events.interval` | Function default | `0.5` s | SSE polling cadence |
| SSE `retry` directive | Hardcoded in generator | `1000` ms | Browser EventSource reconnection delay |

### Default watchlist tickers

`AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX`

Defined as seed data in the database layer (not in `seed_prices.py`). The simulator assigns realistic starting prices and GBM parameters from `seed_prices.py` for all ten. Any ticker added dynamically (outside this list) gets a random seed price in `[50, 300]` and default GBM parameters `sigma=0.25, mu=0.05`.

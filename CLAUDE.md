# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

This project uses `uv` for dependency management.

```bash
# Install dependencies
uv sync --all-extras --dev --frozen

# Run all tests
uv run pytest

# Run a single test
uv run pytest tests/test_client.py::test_get_eq_bars_daily_returns_dataframe

# Lint and format (ruff)
uv run ruff check --fix .
uv run ruff format .

# Type check
uv run mypy src/

# Run all pre-commit checks
uv run pre-commit run --all-files

# Build
uv build
```

## Architecture

This is a single-file async Python client (`src/async_jquants_api_client/client.py`) wrapping the JQuants API v2. It is intentionally minimal — no abstraction layers, no base classes beyond `JQuantsClientV2`.

**Key design decisions:**

- **Rate limiting**: `aiolimiter.AsyncLimiter` is configured per `Plan` enum (FREE/LIGHT/STANDARD/PREMIUM). The limiter is held in `self._limiter` and wraps every HTTP call in `_get_with_retry`. Rate limits are `_RATE_LIMITS[plan]` requests/minute.
- **Retry logic**: `tenacity` handles 429/500/502/503/504 and network errors with exponential backoff (3 attempts max). Auth errors (401/403) raise `JQuantsAuthError` immediately without retry.
- **Pagination**: `_paginate()` is an async generator that follows `pagination_key` across pages. All `get_*` methods consume it with `[item async for item in self._paginate(...)]`.
- **Range methods**: Methods named `get_*_range()` fetch all dates in the range concurrently (each date as a separate API call). This can fire many simultaneous requests — the rate limiter throttles them. Failure handling differs by whether the method caches:
  - **Without cache** (`_gather_dates_fail_fast`): the first failure cancels every remaining date and is re-raised. Nothing is durable, so continuing would only burn the rate limit for a result that is discarded.
  - **With cache** (`_gather_with_cancel(cancel_on=(JQuantsAuthError,))`): only auth errors abort early, because they can never succeed on retry. Other failures let the remaining dates finish so their per-date cache files are written, and the first failure is raised afterwards — callers resume by re-running with the same `cache_dir`. A date that has already entered its cache write is never cancelled (`protect=writing`): `asyncio.to_thread` cannot be stopped, so cancelling it would only hide whether the write succeeded.
  - **Cache reads** (`_gather_cache_reads`): all reads are awaited even when one fails, for the same reason — the threads cannot be cancelled, and leaving them running hides their errors.
- **5/15-minute bars**: Computed client-side by aggregating 1-minute data via `_aggregate_bars_n_minute()`. The API only provides 1-minute data.
- **Cache**: `get_fin_summary_range` and `get_fin_details_range` accept `cache_dir`; files are stored as `{cache_dir}/{yyyy}/v2_{endpoint}_{yyyymmdd}.csv.gz`.
- **Config loading priority**: argument > `JQUANTS_API_KEY` env var > `JQUANTS_API_CLIENT_CONFIG_FILE` env var > `jquants-api.toml` (cwd) > `~/.jquants-api/jquants-api.toml` > Google Colab path. TOML key must be under `[jquants-api-client]`.

**Module layout:**
- `client.py` — `JQuantsClientV2` class, all API methods
- `plans.py` — `Plan` enum
- `exceptions.py` — `JQuantsError`, `JQuantsAuthError`, `JQuantsAPIError`
- `constants.py` — column name lists for each endpoint (used to define empty DataFrame schemas and enforce column ordering)

**Tests** use `pytest-httpx` (`HTTPXMock`) to intercept `httpx.AsyncClient` calls. The `conftest.py` fixture creates a `Plan.PREMIUM` client (bypasses rate limiting in tests). `asyncio_mode = "auto"` is set in `pyproject.toml` so `@pytest.mark.asyncio` is not required but still present in some tests.

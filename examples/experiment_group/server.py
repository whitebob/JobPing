"""FastAPI experiment server showing the intended JobPing helper shape.

The business endpoint intentionally stays almost identical to the control
group. The mock decorator treats the handler as an arbitrary async callable
with input and output; it does not know what the handler actually does.
"""

from __future__ import annotations

import asyncio
from time import perf_counter

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from examples.experiment_group.jobping_server_mock import jobping


app = FastAPI(title="JobPing experiment group")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class RequestCounter:
    """Tracks how many remote requests are waiting at the same time."""

    def __init__(self) -> None:
        self.active_requests = 0
        self.max_active_requests = 0
        self.completed_requests = 0
        self._lock = asyncio.Lock()

    async def request_started(self) -> None:
        async with self._lock:
            self.active_requests += 1
            self.max_active_requests = max(
                self.max_active_requests,
                self.active_requests,
            )

    async def request_finished(self) -> None:
        async with self._lock:
            self.active_requests -= 1
            self.completed_requests += 1

    async def reset(self) -> None:
        async with self._lock:
            self.active_requests = 0
            self.max_active_requests = 0
            self.completed_requests = 0

    async def snapshot(self) -> dict[str, int]:
        async with self._lock:
            return {
                "active_requests": self.active_requests,
                "max_active_requests": self.max_active_requests,
                "completed_requests": self.completed_requests,
            }


counter = RequestCounter()


@app.middleware("http")
async def count_active_requests(request: Request, call_next):
    """Count only simulated work requests, not metrics or reset calls."""

    if request.url.path != "/work":
        return await call_next(request)

    await counter.request_started()
    try:
        return await call_next(request)
    finally:
        await counter.request_finished()


@app.get("/work")
@jobping.wrap()
async def do_work(request_id: int, sleep_seconds: float = 1.0) -> dict[str, float | int | str]:
    """Simulate a remote task that keeps the HTTP request open while waiting."""

    started_at = perf_counter()
    await asyncio.sleep(sleep_seconds)
    elapsed_seconds = perf_counter() - started_at

    return {
        "request_id": request_id,
        "status": "OK",
        "sleep_seconds": sleep_seconds,
        "elapsed_seconds": elapsed_seconds,
    }


@app.get("/metrics")
async def get_metrics() -> dict[str, int]:
    """Return the current active-request counters."""

    return await counter.snapshot()


@app.post("/reset")
async def reset_metrics() -> dict[str, str]:
    """Reset counters before a new experiment run."""

    await counter.reset()
    return {"status": "reset"}

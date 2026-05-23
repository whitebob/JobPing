#!/usr/bin/env python3
"""Python control client: fire many concurrent requests and wait for responses."""
import asyncio
import os
import sys
from time import perf_counter

import httpx

COUNT = int(os.environ.get("COUNT", "100"))
SLEEP = float(os.environ.get("SLEEP", "1"))
SERVER_URL = os.environ.get("SERVER_URL", "http://127.0.0.1:8888")


async def run_one(client: httpx.AsyncClient, request_id: int) -> dict:
    params = {"request_id": str(request_id), "sleep_seconds": str(SLEEP)}
    r = await client.get(f"{SERVER_URL}/work", params=params, timeout=None)
    r.raise_for_status()
    return r.json()


async def main():
    async with httpx.AsyncClient() as client:
        # reset
        await client.post(f"{SERVER_URL}/reset")
        started = perf_counter()
        tasks = [asyncio.create_task(run_one(client, i)) for i in range(COUNT)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = perf_counter() - started
        successes = sum(1 for r in results if not isinstance(r, Exception))
        print({
            "requestCount": COUNT,
            "sleepSeconds": SLEEP,
            "elapsedSeconds": elapsed,
            "successfulResponses": successes,
        })
        # if exceptions, print a count
        errs = [r for r in results if isinstance(r, Exception)]
        if errs:
            print(f"Errors: {len(errs)}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())

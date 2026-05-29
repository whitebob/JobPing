#!/usr/bin/env python3
"""Python experiment client: uses JobPing singleton to accept job refs and await results."""
import asyncio
import os
import sys
from time import perf_counter
import uuid

import httpx

from jobping import jp

COUNT = int(os.environ.get("COUNT", "100"))
SLEEP = float(os.environ.get("SLEEP", "1"))
SERVER_URL = os.environ.get("SERVER_URL", "http://127.0.0.1:8887")
BROKER_URL = os.environ.get("BROKER_URL", "http://127.0.0.1:8890")

jp.configure(broker_port=0, peer_brokers=[BROKER_URL])


@jp.unwrap()
async def run_one(client: httpx.AsyncClient, request_id: int) -> dict:
    job_id = str(uuid.uuid4())
    headers = {"x-jobping-job-id": job_id}
    params = {"request_id": str(request_id), "sleep_seconds": str(SLEEP)}
    r = await client.get(f"{SERVER_URL}/work", params=params, headers=headers, timeout=None)
    r.raise_for_status()
    return r.json()


async def main():
    await jp.start_broker()

    async with httpx.AsyncClient() as client:
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
        errs = [r for r in results if isinstance(r, Exception)]
        if errs:
            print(f"Errors: {len(errs)}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())

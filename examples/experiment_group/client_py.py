#!/usr/bin/env python3
"""Python experiment client: uses JobPing EndpointProxy to accept job refs and await results."""
import asyncio
import os
import sys
from time import perf_counter
import uuid

import httpx

from jobping.endpoint_proxy import EndpointProxy
from jobping.imp.transport_layer_ws import TransportLayerWS
from jobping.result_handoff import ResultHandoff
from jobping.state_sync import StateSync
from jobping.imp.envelope_endpoint_inmemory import EnvelopeEndpointInMemory
from jobping.jpitem_queue import JPItemQueueInMemory

COUNT = int(os.environ.get("COUNT", "100"))
SLEEP = float(os.environ.get("SLEEP", "1"))
SERVER_URL = os.environ.get("SERVER_URL", "http://127.0.0.1:8887")
BROKER_URL = os.environ.get("BROKER_URL", "http://127.0.0.1:8890")


async def run_one(client: httpx.AsyncClient, endpoint_proxy: EndpointProxy, request_id: int) -> dict:
    # create a job id
    job_id = str(uuid.uuid4())
    headers = {"x-jobping-job-id": job_id}
    params = {"request_id": str(request_id), "sleep_seconds": str(SLEEP)}
    # send initial request
    r = await client.get(f"{SERVER_URL}/work", params=params, headers=headers, timeout=None)
    r.raise_for_status()
    data = r.json()
    # if server returned a job ref (it should when using experiment server), accept and await
    if isinstance(data, dict) and data.get("jobping") == "jobping.job_ref.v1":
        endpoint_proxy.accept(job_id)
        # await result (increase timeout)
        completed = await endpoint_proxy.await_result(job_id, timeout=30.0)
        endpoint_proxy.release(job_id)
        return completed
    else:
        return data


async def main():
    # create transport and endpoint proxy — connect early so the broker
    # connection is ready before the server sends results
    transport = TransportLayerWS(BROKER_URL)
    await transport._ensure_connected()
    endpoint_proxy = EndpointProxy(
        state_sync=StateSync(transport),
        result_handoff=ResultHandoff(transport),
        queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()),
    )

    async with httpx.AsyncClient() as client:
        # reset
        await client.post(f"{SERVER_URL}/reset")
        started = perf_counter()
        tasks = [asyncio.create_task(run_one(client, endpoint_proxy, i)) for i in range(COUNT)]
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

# JobPing Python Usage

## Installation

```sh
pip install jobping
```

For development extras (FastAPI, uvicorn, pytest):

```sh
pip install jobping[dev]
```

## Server-side: decorate a handler

```py
from fastapi import FastAPI, Request
from jobping import create_jobping

app = FastAPI()
jp = create_jobping()

@app.get("/work")
@jp.wrap()
async def do_work(request: Request, request_id: int, sleep_seconds: float = 1.0):
    # Business logic unchanged. When a client sets the x-jobping-job-id header,
    # JobPing captures the job_id, defers work, returns a job_ref immediately,
    # and fulfills later via the result transport.
    import asyncio
    await asyncio.sleep(sleep_seconds)
    return {"request_id": request_id, "status": "OK"}
```

## Client-side: unwrap a callable

```py
import httpx
from jobping import create_jobping
from jobping.imp.transport_layer_ws import TransportLayerWS

jp = create_jobping(
    status_transport_layer=TransportLayerWS("http://127.0.0.1:8890"),
)

async def call_server(request_id: int) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"http://127.0.0.1:8887/work",
            params={"request_id": request_id, "sleep_seconds": 1},
            headers={"x-jobping-job-id": f"my-job-{request_id}"},
        )
        return resp.json()

# Unwrap the callable — JobPing transparently detects job_ref responses,
# accepts them, awaits the result via WebSocket, and returns the final value.
wrapped = jp.unwrap()(call_server)
result = await wrapped(42)
```

## Custom transport layers

```py
from jobping import create_jobping
from jobping.imp.transport_layer_ws import TransportLayerWS
from jobping.imp.transport_layer_https import TransportLayerHTTPS

jp = create_jobping(
    status_transport_layer=TransportLayerWS("ws://broker:8890"),
    result_transport_layer=TransportLayerHTTPS("http://broker:8890"),
)
```

## Custom queue

```py
from jobping import create_jobping, EnvelopeEndpointInMemory, JPItemQueueInMemory

jp = create_jobping(
    queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()),
)
```

## Disable JobPing at runtime

```sh
JOBPING_DISABLED=1 python my_app.py
```

When disabled, `@jp.wrap()` calls the decorated function directly with no capture, envelope, or queue behavior.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `JOBPING_WS_URL` | `http://127.0.0.1:8890` | Default WebSocket broker URL |
| `JOBPING_HTTP_BASE` | same as WS URL | Default HTTP transport base URL |
| `JOBPING_DISABLED` | (unset) | Set to `1`/`true`/`yes` to bypass JobPing |

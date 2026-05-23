# JobPing (Python)

Endpoint rendezvous bridge for JPItem state synchronization and result handoff.

```sh
pip install jobping
```

## Quick start

```py
from jobping import create_jobping
jp = create_jobping()
```

`create_jobping()` with no arguments uses sensible defaults:
- Status transport: WebSocket to `JOBPING_WS_URL` (default `http://127.0.0.1:8890`)
- Result transport: HTTP to `JOBPING_HTTP_BASE` (default same as WS URL)
- Queue: in-memory

## Requirements

Python 3.10+. Dependencies: `httpx`, `python-socketio`, `aiohttp`.

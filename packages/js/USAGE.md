# JobPing JavaScript Usage

## Installation

```sh
npm install jobping
```

## Node.js client

```js
import * as jp from "jobping";

const jobping = jp.createJobPing({
  transportLayer: new jp.TransportLayerWS({ url: "http://127.0.0.1:8890" }),
  queue: new jp.JPItemQueueInMemory(new jp.EnvelopeEndpointInMemory()),
});

const callServer = jobping.unwrap(async (requestId) => {
  const jobId = crypto.randomUUID();
  const resp = await fetch(
    `http://127.0.0.1:8887/work?request_id=${requestId}&sleep_seconds=1`,
    { headers: { "x-jobping-job-id": jobId } }
  );
  return resp.json();
});

const result = await callServer(42);
```

## Node.js server

```js
import * as jp from "jobping";

const transport = new jp.TransportLayerWS({ url: "http://127.0.0.1:8890" });
const endpointProxy = new jp.EndpointProxy({
  stateSync: new jp.StateSync({ transportLayer: transport }),
  resultHandoff: new jp.ResultHandoff({ transportLayer: transport }),
  queue: new jp.JPItemQueueInMemory(new jp.EnvelopeEndpointInMemory()),
});

// In your HTTP handler:
if (jobHeader) {
  endpointProxy.offer(jobId);
  endpointProxy.defer(jobId);
  endpointProxy.fulfillLater(jobId, async () => {
    return await doActualWork();
  });
  res.end(JSON.stringify(endpointProxy.makeJobRef(jobId)));
  return;
}
// No job header: behave normally
res.end(JSON.stringify(await doActualWork()));
```

## Browser (ESM)

```html
<script src="https://cdn.socket.io/4.8.1/socket.io.min.js"></script>
<script type="module">
import * as jp from "./jobping_browser.mjs";

const jobping = jp.createJobPing({
  transportLayer: new jp.TransportLayerWS({ url: "http://127.0.0.1:8890" }),
  queue: new jp.JPItemQueueInMemory(new jp.EnvelopeEndpointInMemory()),
});

const callServer = jobping.unwrap(async (requestId) => {
  const resp = await fetch(
    `http://127.0.0.1:8887/work?request_id=${requestId}`,
    { headers: { "x-jobping-job-id": crypto.randomUUID() } }
  );
  return resp.json();
});
</script>
```

## Browser (minified IIFE)

```html
<script src="https://cdn.socket.io/4.8.1/socket.io.min.js"></script>
<script src="./jobping_browser.min.js"></script>
<script>
const jp = window.jobping;
const jobping = jp.createJobPing({
  transportLayer: new jp.TransportLayerWS({ url: "http://127.0.0.1:8890" }),
  queue: new jp.JPItemQueueInMemory(new jp.EnvelopeEndpointInMemory()),
});
</script>
```

## API

All classes are exported from the `jobping` namespace:

| Export | Role |
|---|---|
| `createJobPing(opts)` | Factory: creates a JobPing instance |
| `JobPing` | Top-level wrapper facade (`wrap` method) |
| `EndpointProxy` | Producer/consumer rendezvous orchestration |
| `StateSync` | Lightweight job status synchronization |
| `ResultHandoff` | Boxed result delivery |
| `TransportLayerWS` | WebSocket transport (Socket.IO) |
| `TransportLayerHTTPS` | HTTP polling transport |
| `JPItemQueueInMemory` | In-memory JPItem queue |
| `EnvelopeEndpointInMemory` | In-memory envelope endpoint |
| `isJobPingDisabled()` | Check unload switch |

## Disable at runtime

```
JOBPING_DISABLED=1 node my_app.mjs
```

Or in code:

```js
globalThis.__JOBPING_DISABLED__ = true;
```

## Rebuilding the browser bundle

```sh
npm run build:browser
```

Outputs:
- `jobping_browser.mjs` — ESM for `<script type="module">`
- `jobping_browser.min.js` — minified IIFE for `<script src="...">`

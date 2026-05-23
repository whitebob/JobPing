# JobPing (JavaScript)

Endpoint rendezvous bridge for JPItem state synchronization and result handoff.

```sh
npm install jobping
```

## Quick start

```js
import * as jp from "jobping";

const jobping = jp.createJobPing({
  transportLayer: new jp.TransportLayerWS({ url: "http://127.0.0.1:8890" }),
  queue: new jp.JPItemQueueInMemory(new jp.EnvelopeEndpointInMemory()),
});
```

## Requirements

Node.js 18+ or modern browser. Runtime dependency: `socket.io-client`.

## Packages

| Package | Path | Purpose |
|---|---|---|
| `jobping` | `packages/js/` | npm package (Node + bundler) |
| `jobping_browser.mjs` | `examples/experiment_group/` | ESM bundle for `<script type="module">` |
| `jobping_browser.min.js` | `examples/experiment_group/` | Minified IIFE for `<script src="...">` |

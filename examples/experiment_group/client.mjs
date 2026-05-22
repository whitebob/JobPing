// Promise-based experiment client showing the intended JobPing helper shape.
//
// The business code stays almost identical to the control group. The mock SDK
// decorates one arbitrary async callable; it does not know or care that the
// callable happens to use fetch internally.

import { performance } from "node:perf_hooks";

import { createJobPing } from "../../packages/js/jobping.mjs";
import { MockEnvelopeEndpoint } from "../../sandbox/js/envelope_endpoint_mock.mjs";
import { MockJPItemQueue } from "../../sandbox/js/jpitem_queue_mock.mjs";
import { TransportLayerWS } from "../../packages/js/transport_layer_ws.mjs";

const brokerUrl = (() => {
  const prefix = `--brokerUrl=`;
  const value = process.argv.find((item) => item.startsWith(prefix));
  return value ? value.slice(prefix.length) : "http://127.0.0.1:8890";
})();

const jp = createJobPing({
  transportLayer: new TransportLayerWS({ url: brokerUrl }),
  queue: new MockJPItemQueue(new MockEnvelopeEndpoint()),
});

function readOption(name, fallback) {
  const prefix = `--${name}=`;
  const value = process.argv.find((item) => item.startsWith(prefix));

  if (!value) {
    return fallback;
  }

  return value.slice(prefix.length);
}

const requestCount = Number(readOption("count", "100"));
const sleepSeconds = Number(readOption("sleepSeconds", "1"));
const serverUrl = readOption("serverUrl", "http://127.0.0.1:8887");

if (!Number.isInteger(requestCount) || requestCount <= 0) {
  throw new Error("--count must be a positive integer");
}

if (!Number.isFinite(sleepSeconds) || sleepSeconds < 0) {
  throw new Error("--sleepSeconds must be a non-negative number");
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

const runOneRequest = jp.wrap(async function runOneRequest(requestId) {
  const params = new URLSearchParams({
    request_id: String(requestId),
    sleep_seconds: String(sleepSeconds),
  });

  return fetchJson(`${serverUrl}/work?${params.toString()}`);
});

async function main() {
  await fetchJson(`${serverUrl}/reset`, { method: "POST" });

  const startedAt = performance.now();

  const requests = [];
  for (let requestId = 0; requestId < requestCount; requestId += 1) {
    requests.push(runOneRequest(requestId));
  }

  const results = await Promise.all(requests);
  const elapsedSeconds = (performance.now() - startedAt) / 1000;
  const metrics = await fetchJson(`${serverUrl}/metrics`);

  console.log(
    JSON.stringify(
      {
        requestCount,
        sleepSeconds,
        elapsedSeconds,
        successfulResponses: results.length,
        remoteActiveRequests: metrics,
      },
      null,
      2,
    ),
  );
}

try {
  await main();
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}

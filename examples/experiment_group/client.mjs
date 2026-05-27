// Promise-based experiment client showing the intended JobPing helper shape.
//
// The business code stays almost identical to the control group. The mock SDK
// decorates one arbitrary async callable; it does not know or care that the
// callable happens to use fetch internally.

import { performance } from "node:perf_hooks";

import * as jp from "../../packages/js/index.mjs";

const brokerUrl = (() => {
  const prefix = `--brokerUrl=`;
  const value = process.argv.find((item) => item.startsWith(prefix));
  return value ? value.slice(prefix.length) : "http://127.0.0.1:8890";
})();

const transport = new jp.TransportLayerWS({ url: brokerUrl });
const endpointProxy = new jp.EndpointProxy({
  stateSync: new jp.StateSync({ transportLayer: transport }),
  resultHandoff: new jp.ResultHandoff({ transportLayer: transport }),
  queue: new jp.JPItemQueueInMemory(new jp.EnvelopeEndpointInMemory()),
});
const jobping = new jp.JobPing({ endpointProxy });

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

const runOneRequest = jobping.wrap(async function runOneRequest(requestId) {
  const params = new URLSearchParams({
    request_id: String(requestId),
    sleep_seconds: String(sleepSeconds),
  });

  const jobId = (typeof crypto?.randomUUID === 'function') ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(36).slice(2,9)}`;
  return fetchJson(`${serverUrl}/work?${params.toString()}`, { method: 'GET', headers: { 'x-jobping-job-id': jobId } });
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

// Ensure transport socket is closed so Node can exit cleanly when the client
// finishes. Disconnect the socket and exit with the captured exit code.
try {
  if (typeof transport !== 'undefined' && transport && typeof transport.disconnect === 'function') {
    transport.disconnect();
  }
} catch (e) {
  // ignore
}

// Give the socket a brief moment to flush then exit.
setTimeout(() => {
  // eslint-disable-next-line no-process-exit
  process.exit(process.exitCode || 0);
}, 50);

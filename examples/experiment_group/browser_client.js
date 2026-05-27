import * as jp from "./jobping_browser.mjs";

const serverUrlInput = document.getElementById("serverUrl");
const brokerUrlInput = document.getElementById("brokerUrl");
const requestCountInput = document.getElementById("requestCount");
const sleepSecondsInput = document.getElementById("sleepSeconds");
const runButton = document.getElementById("runButton");
const resetButton = document.getElementById("resetButton");
const resultOutput = document.getElementById("result");

function showResult(value) {
  if (typeof value === "string") {
    resultOutput.textContent = value;
  } else {
    resultOutput.textContent = JSON.stringify(value, null, 2);
  }
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }
  return response.json();
}

async function runExperiment() {
  const serverUrl = serverUrlInput.value.replace(/\/+$/, "");
  const brokerUrl = brokerUrlInput.value.replace(/\/+$/, "");
  const requestCount = Number(requestCountInput.value);
  const sleepSeconds = Number(sleepSecondsInput.value);

  if (!Number.isInteger(requestCount) || requestCount <= 0) {
    throw new Error("Request count must be a positive integer.");
  }
  if (!Number.isFinite(sleepSeconds) || sleepSeconds < 0) {
    throw new Error("Sleep seconds must be a non-negative number.");
  }

  const transport = new jp.TransportLayerWS({ url: brokerUrl });
  const endpointProxy = new jp.EndpointProxy({
    stateSync: new jp.StateSync({ transportLayer: transport }),
    resultHandoff: new jp.ResultHandoff({ transportLayer: transport }),
    queue: new jp.JPItemQueueInMemory(new jp.EnvelopeEndpointInMemory()),
  });
  const jobping = new jp.JobPing({ endpointProxy });

  await fetchJson(`${serverUrl}/reset`, { method: "POST" });

  const startedAt = performance.now();

  const runOne = jobping.wrap(async (requestId) => {
    const params = new URLSearchParams({
      request_id: String(requestId),
      sleep_seconds: String(sleepSeconds),
    });
    const jobId = crypto.randomUUID();
    return fetchJson(`${serverUrl}/work?${params}`, {
      headers: { "x-jobping-job-id": jobId },
    });
  });

  const promises = [];
  for (let i = 0; i < requestCount; i++) {
    promises.push(runOne(i));
  }
  const results = await Promise.all(promises);
  const elapsedSeconds = (performance.now() - startedAt) / 1000;
  const metrics = await fetchJson(`${serverUrl}/metrics`);

  transport.socket.disconnect();

  return {
    requestCount,
    sleepSeconds,
    elapsedSeconds,
    successfulResponses: results.length,
    remoteActiveRequests: metrics,
  };
}

runButton.addEventListener("click", async () => {
  runButton.disabled = true;
  resultOutput.textContent = "Running...";

  try {
    const result = await runExperiment();
    showResult(result);
  } catch (error) {
    resultOutput.textContent = String(error);
  } finally {
    runButton.disabled = false;
  }
});

resetButton.addEventListener("click", async () => {
  const serverUrl = serverUrlInput.value.replace(/\/+$/, "");
  try {
    await fetchJson(`${serverUrl}/reset`, { method: "POST" });
    showResult("Counters reset.");
  } catch (error) {
    resultOutput.textContent = String(error);
  }
});

const serverUrlInput = document.getElementById("serverUrl");
const requestCountInput = document.getElementById("requestCount");
const sleepSecondsInput = document.getElementById("sleepSeconds");
const runButton = document.getElementById("runButton");
const resultOutput = document.getElementById("result");

function showResult(value) {
  resultOutput.textContent = JSON.stringify(value, null, 2);
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

async function runOneRequest(serverUrl, requestId, sleepSeconds) {
  const params = new URLSearchParams({
    request_id: String(requestId),
    sleep_seconds: String(sleepSeconds),
  });

  return fetchJson(`${serverUrl}/work?${params.toString()}`);
}

async function runControlGroup() {
  const serverUrl = serverUrlInput.value;
  const requestCount = Number(requestCountInput.value);
  const sleepSeconds = Number(sleepSecondsInput.value);

  if (!Number.isInteger(requestCount) || requestCount <= 0) {
    throw new Error("Request count must be a positive integer.");
  }

  if (!Number.isFinite(sleepSeconds) || sleepSeconds < 0) {
    throw new Error("Sleep seconds must be a non-negative number.");
  }

  await fetchJson(`${serverUrl}/reset`, { method: "POST" });

  const startedAt = performance.now();
  const requests = [];

  for (let requestId = 0; requestId < requestCount; requestId += 1) {
    requests.push(runOneRequest(serverUrl, requestId, sleepSeconds));
  }

  const results = await Promise.all(requests);
  const elapsedSeconds = (performance.now() - startedAt) / 1000;
  const metrics = await fetchJson(`${serverUrl}/metrics`);

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
    const result = await runControlGroup();
    showResult(result);
  } catch (error) {
    resultOutput.textContent = String(error);
  } finally {
    runButton.disabled = false;
  }
});

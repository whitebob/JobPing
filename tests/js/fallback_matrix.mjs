import { existsSync } from "node:fs";
import { spawn } from "node:child_process";

const PYTHON = ".venv/bin/python";
const REQUEST_COUNT = 3;
const SLEEP_SECONDS = 0.05;

const servers = [
  {
    name: "server without JP",
    module: "examples.control_group.server:app",
    port: 8891,
  },
  {
    name: "server with JP mock",
    module: "examples.experiment_group.server:app",
    port: 8892,
  },
];

const clients = [
  {
    name: "client without JP",
    script: "examples/control_group/client.mjs",
  },
  {
    name: "client with JP mock",
    script: "examples/experiment_group/client.mjs",
  },
];

if (!existsSync(PYTHON)) {
  throw new Error(`Missing ${PYTHON}; run from the repository root with the virtualenv present.`);
}

function startServer(server) {
  const child = spawn(
    PYTHON,
    [
      "-m",
      "uvicorn",
      server.module,
      "--host",
      "127.0.0.1",
      "--port",
      String(server.port),
    ],
    {
      env: {
        ...process.env,
        PYTHONPATH: ["packages/python", "sandbox/python", ".", process.env.PYTHONPATH]
          .filter(Boolean)
          .join(":"),
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });

  return {
    ...server,
    child,
    getOutput() {
      return `${stdout}${stderr}`;
    },
  };
}

async function stopServer(server) {
  if (server.child.exitCode !== null || server.child.signalCode !== null) {
    return;
  }

  await new Promise((resolve) => {
    server.child.once("exit", resolve);
    server.child.kill("SIGTERM");
    setTimeout(() => {
      if (server.child.exitCode === null && server.child.signalCode === null) {
        server.child.kill("SIGKILL");
      }
    }, 1000).unref();
  });
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed for ${url}: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

async function waitForServer(server) {
  const metricsUrl = `http://127.0.0.1:${server.port}/metrics`;
  const startedAt = Date.now();

  while (Date.now() - startedAt < 5000) {
    if (server.child.exitCode !== null) {
      throw new Error(`${server.name} exited early:\n${server.getOutput()}`);
    }

    try {
      await fetchJson(metricsUrl);
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  throw new Error(`${server.name} did not become ready:\n${server.getOutput()}`);
}

async function runClient(client, server) {
  const serverUrl = `http://127.0.0.1:${server.port}`;
  const child = spawn(
    process.execPath,
    [
      client.script,
      `--count=${REQUEST_COUNT}`,
      `--sleepSeconds=${SLEEP_SECONDS}`,
      `--serverUrl=${serverUrl}`,
    ],
    {
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });

  const exitCode = await new Promise((resolve) => {
    child.once("exit", resolve);
  });

  if (exitCode !== 0) {
    throw new Error(`${client.name} -> ${server.name} failed:\n${stdout}${stderr}`);
  }

  const jsonStart = stdout.indexOf("{");
  if (jsonStart === -1) {
    throw new Error(`${client.name} -> ${server.name} did not print JSON:\n${stdout}`);
  }

  return JSON.parse(stdout.slice(jsonStart));
}

function assertClientResult(label, result) {
  if (result.successfulResponses !== REQUEST_COUNT) {
    throw new Error(`${label}: expected ${REQUEST_COUNT} successful responses, got ${result.successfulResponses}`);
  }

  const metrics = result.remoteActiveRequests;
  if (metrics.active_requests !== 0) {
    throw new Error(`${label}: active request leak in client result: ${JSON.stringify(metrics)}`);
  }

  if (metrics.completed_requests !== REQUEST_COUNT) {
    throw new Error(`${label}: expected ${REQUEST_COUNT} completed requests, got ${metrics.completed_requests}`);
  }

  if (metrics.max_active_requests < 1 || metrics.max_active_requests > REQUEST_COUNT) {
    throw new Error(`${label}: unexpected max_active_requests: ${metrics.max_active_requests}`);
  }
}

async function assertServerMetricsClear(label, server) {
  const metrics = await fetchJson(`http://127.0.0.1:${server.port}/metrics`);

  if (metrics.active_requests !== 0) {
    throw new Error(`${label}: server still has active requests: ${JSON.stringify(metrics)}`);
  }

  if (metrics.completed_requests !== REQUEST_COUNT) {
    throw new Error(`${label}: server completed count mismatch: ${JSON.stringify(metrics)}`);
  }
}

async function assertWorkResponseShape(server) {
  await fetch(`http://127.0.0.1:${server.port}/reset`, { method: "POST" });

  const output = await fetchJson(
    `http://127.0.0.1:${server.port}/work?request_id=42&sleep_seconds=0`,
  );
  const expectedKeys = ["elapsed_seconds", "request_id", "sleep_seconds", "status"];
  const actualKeys = Object.keys(output).sort();

  if (JSON.stringify(actualKeys) !== JSON.stringify(expectedKeys)) {
    throw new Error(`${server.name}: unexpected /work response keys: ${JSON.stringify(output)}`);
  }

  if (output.request_id !== 42 || output.status !== "OK") {
    throw new Error(`${server.name}: unexpected /work response payload: ${JSON.stringify(output)}`);
  }

  const metrics = await fetchJson(`http://127.0.0.1:${server.port}/metrics`);
  if (metrics.active_requests !== 0 || metrics.completed_requests !== 1) {
    throw new Error(`${server.name}: possible resource leak after direct /work: ${JSON.stringify(metrics)}`);
  }
}

const runningServers = servers.map(startServer);

try {
  await Promise.all(runningServers.map(waitForServer));

  for (const server of runningServers) {
    for (const client of clients) {
      const label = `${client.name} -> ${server.name}`;
      const result = await runClient(client, server);
      assertClientResult(label, result);
      await assertServerMetricsClear(label, server);
      console.log(`PASS ${label}`);
    }

    await assertWorkResponseShape(server);
    console.log(`PASS ${server.name} /work response shape and active request cleanup`);
  }
} finally {
  await Promise.all(runningServers.map(stopServer));
}

import { spawn } from "child_process";
import assert from "node:assert/strict";

function startProcess(command, args, options) {
  const proc = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"], ...options });
  proc.stdout.setEncoding("utf8");
  proc.stderr.setEncoding("utf8");
  proc.stdout.on("data", (d) => process.stdout.write(d));
  proc.stderr.on("data", (d) => process.stderr.write(d));
  return proc;
}

async function waitForUrl(url, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) return true;
    } catch (e) {
      // ignore
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  return false;
}

(async () => {
  const broker = startProcess("node", ["examples/experiment_group/socket_broker.mjs"]);

  // Start uvicorn server
  const uvicorn = startProcess(
    ".venv/bin/uvicorn",
    ["examples.experiment_group.server:app", "--host", "127.0.0.1", "--port", "8887"],
    { env: { ...process.env, PYTHONPATH: "packages/python:sandbox/python:." } },
  );

  try {
    // broker has no HTTP endpoint; just wait a moment to let it bind
    await new Promise((r) => setTimeout(r, 300));

    const serverReady = await waitForUrl("http://127.0.0.1:8887/metrics", 5000);
    if (!serverReady) throw new Error("server did not become ready");

    // run client with small workload
    const client = startProcess("node", ["examples/experiment_group/client.mjs", "--count=5", "--sleepSeconds=0.05", "--serverUrl=http://127.0.0.1:8887", "--brokerUrl=http://127.0.0.1:8890"]);

    await new Promise((resolve, reject) => {
      client.on("exit", (code) => {
        if (code === 0) resolve();
        else reject(new Error("client exited with code " + code));
      });
    });

    console.log("experiment client finished");
  } finally {
    broker.kill();
    uvicorn.kill();
  }
})();

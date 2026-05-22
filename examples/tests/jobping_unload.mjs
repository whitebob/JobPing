import assert from "node:assert/strict";
import { jobping } from "../experiment_group/jobping_client_mock.mjs";

async function captureConsoleLogs(call) {
  const originalLog = console.log;
  const logs = [];
  console.log = (...args) => {
    logs.push(args.join(" "));
  };

  try {
    return {
      result: await call(),
      logs,
    };
  } finally {
    console.log = originalLog;
  }
}

const wrapped = jobping.wrap(async (value) => ({ value, status: "OK" }));

globalThis.__JOBPING_DISABLED__ = true;
const globalDisabled = await captureConsoleLogs(() => wrapped(1));
assert.deepEqual(globalDisabled.result, { value: 1, status: "OK" });
assert.deepEqual(globalDisabled.logs, []);

globalThis.__JOBPING_DISABLED__ = false;
process.env.JOBPING_DISABLED = "true";
const envDisabled = await captureConsoleLogs(() => wrapped(2));
assert.deepEqual(envDisabled.result, { value: 2, status: "OK" });
assert.deepEqual(envDisabled.logs, []);

delete process.env.JOBPING_DISABLED;
const enabled = await captureConsoleLogs(() => wrapped(3));
assert.deepEqual(enabled.result, { value: 3, status: "OK" });
assert.notDeepEqual(enabled.logs, []);

console.log("PASS JobPing unload switch preserves original client call path");

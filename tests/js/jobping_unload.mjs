import assert from "node:assert/strict";
import { JobPing } from "../../packages/js/jobping.mjs";
import { createMockEndpointProxy } from "../../sandbox/js/create_mock_endpoint_proxy.mjs";

const jobping = new JobPing({
  endpointProxy: createMockEndpointProxy(),
});

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
// When the wrapped function returns a plain object (not a job_ref),
// wrap() passes through without entering the JobPing accept/await flow.
// No console output is produced in any code path for this case.
assert.deepEqual(enabled.logs, []);

console.log("PASS JobPing unload switch preserves original client call path");

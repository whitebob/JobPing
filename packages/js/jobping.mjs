// Client-side JobPing SDK facade.

import { EndpointProxy } from "./endpoint_proxy.mjs";
import { ResultHandoff } from "./result_handoff.mjs";
import { StateSync } from "./state_sync.mjs";

export function isJobPingDisabled() {
  if (globalThis.__JOBPING_DISABLED__ === true) {
    return true;
  }

  const value = globalThis.process?.env?.JOBPING_DISABLED;
  return typeof value === "string" && /^(1|true|yes|on)$/i.test(value);
}

export class JobPing {
  constructor({ endpointProxy }) {
    if (!endpointProxy) {
      throw new Error("JobPing requires an endpointProxy");
    }

    this.endpointProxy = endpointProxy;
  }

  wrap(wrappedCallable) {
    return async function jobpingWrappedCallable(...args) {
      if (isJobPingDisabled()) {
        return wrappedCallable(...args);
      }

      const output = await wrappedCallable(...args);

      if (!this.endpointProxy.isJobRef(output)) {
        return output;
      }

      this.endpointProxy.accept(output.job_id);

      const completedItem = await this.endpointProxy.awaitResult(output.job_id, { timeoutMs: 30000 });
      this.endpointProxy.release(output.job_id);

      return completedItem.result;
    }.bind(this);
  }
}

export const JobPingClass = JobPing;

export function createJobPing({
  transportLayer,
  queue,
  resultTransportLayer = transportLayer,
}) {
  if (!transportLayer) {
    throw new Error("createJobPing requires a transportLayer");
  }
  if (!queue) {
    throw new Error("createJobPing requires a queue");
  }

  return new JobPing({
    endpointProxy: new EndpointProxy({
      stateSync: new StateSync({ transportLayer }),
      resultHandoff: new ResultHandoff({ transportLayer: resultTransportLayer }),
      queue,
    }),
  });
}

// Mock client-side JobPing SDK for usage-first TDD examples.

import { EndpointProxy } from "./jobping_endpoint_proxy.mjs";
import { MockEnvelopeEndpoint } from "./jobping_envelope_mock.mjs";
import { MockJPItemQueue } from "./jobping_jpitem_queue_mock.mjs";
import { ResultHandoff } from "./jobping_result_handoff.mjs";
import { StateSync } from "./jobping_state_sync.mjs";
import { MockTransportAdapter } from "./jobping_transport_mock.mjs";

export function isJobPingDisabled() {
  if (globalThis.__JOBPING_DISABLED__ === true) {
    return true;
  }

  const value = globalThis.process?.env?.JOBPING_DISABLED;
  return typeof value === "string" && /^(1|true|yes|on)$/i.test(value);
}

function createDefaultEndpointProxy() {
  return new EndpointProxy({
    stateSync: new StateSync({
      transportLayer: new MockTransportAdapter(),
    }),
    resultHandoff: new ResultHandoff({
      transportLayer: new MockTransportAdapter(),
    }),
    queue: new MockJPItemQueue(new MockEnvelopeEndpoint()),
  });
}

export class JobPingClientMock {
  constructor({ endpointProxy = createDefaultEndpointProxy() } = {}) {
    this.endpointProxy = endpointProxy;
  }

  wrap(wrappedCallable) {
    return async function jobpingWrappedCallable(...args) {
      if (isJobPingDisabled()) {
        return wrappedCallable(...args);
      }

      console.log("doing client_proxy.capture_call_input");
      console.log("doing client_proxy.call_wrapped_callable");
      const output = await wrappedCallable(...args);

      console.log("doing client_proxy.inspect_call_output");
      if (!this.endpointProxy.isJobRef(output)) {
        return output;
      }

      console.log("doing client_proxy.accept_job_ref");
      this.endpointProxy.accept(output.job_id);

      console.log("doing client_proxy.await_result");
      const completedItem = await this.endpointProxy.awaitResult(output.job_id);
      this.endpointProxy.release(output.job_id);

      console.log("doing accepted_jpitem.return_result");
      return completedItem.result;
    }.bind(this);
  }
}

export const jobping = new JobPingClientMock();

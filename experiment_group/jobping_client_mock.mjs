// Mock client-side JobPing SDK for usage-first TDD examples.

export function isJobPingDisabled() {
  if (globalThis.__JOBPING_DISABLED__ === true) {
    return true;
  }

  const value = globalThis.process?.env?.JOBPING_DISABLED;
  return typeof value === "string" && /^(1|true|yes|on)$/i.test(value);
}

export const jobping = {
  wrap(wrappedCallable) {
    return async function jobpingWrappedCallable(...args) {
      if (isJobPingDisabled()) {
        return wrappedCallable(...args);
      }

      console.log("doing client_proxy.capture_call_input");
      // Wrapper layer:
      // - Treat ...args and the callable output as opaque values.
      // - Do not know HTTP, fetch, URL, server_endpoint, or websocket details.
      // - Only handle JobPing context, boxed envelope detection, accepting a
      //   peer offer, awaiting fulfillment, and releasing the JPItem.
      //
      // Transport adapter layer pseudocode:
      // const jobId = clientProxy.createJobId();
      // const output = await clientProxy.withJobContext(jobId, () =>
      //   wrappedCallable(...args),
      // );
      //
      // withJobContext is intentionally not a concrete API yet. A later adapter may
      // map that context to an HTTP header, websocket metadata, RPC metadata, etc.

      console.log("doing client_proxy.call_wrapped_callable");
      // Future flow:
      // 1. Treat ...args as opaque call input.
      // 2. Establish provisional JobPing context without committing to a transport.
      // 3. Call the wrapped callable. Its output may be a job_ref offer envelope.
      // 4. If the output is a job_ref, accept it in the endpoint JPItem queue.
      // 5. Await fulfillment locally, not through the original remote request.
      // 6. Unbox the opaque final output and resolve this Promise with it.
      // 7. Release the accepted JPItem when ownership is no longer needed.
      //
      // Pseudocode:
      const output = await wrappedCallable(...args);
      console.log("doing client_proxy.inspect_call_output");
      // if (!clientProxy.isBoxed(output)) {
      //   return output;
      // }
      console.log("doing client_proxy.accept_offer_if_boxed");
      // The offered job_id should match the client-owned JobPing context.
      // if (output.job_id !== jobId) {
      //   throw new Error("Unexpected boxed output job_id");
      // }
      // endpointQueue.accept(output.job_id);
      console.log("doing client_proxy.await_fulfillment_if_boxed");
      // const completedItem = await endpointQueue.awaitResult(output.job_id);
      // const finalOutput = completedItem.payload;
      // endpointQueue.release(output.job_id);
      // return finalOutput;
      console.log("doing accepted_jpitem.on_fulfilled_unbox_output_if_boxed");
      return output;
    };
  },
};

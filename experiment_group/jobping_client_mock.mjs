// Mock client-side JobPing SDK for usage-first TDD examples.

export const jobping = {
  wrap(wrappedCallable) {
    return async function jobpingWrappedCallable(...args) {
      console.log("doing client_proxy.capture_call_input");
      // Wrapper layer:
      // - Treat ...args and the callable output as opaque values.
      // - Do not know HTTP, fetch, URL, server_endpoint, or websocket details.
      // - Only handle JobPing context, boxed envelope detection, local JPItem wait,
      //   and unboxing once the concrete transport adapter has carried the context.
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
      // 3. Call the wrapped callable. Its output may be a boxed job_id envelope.
      // 4. If the output is boxed, create a local JPItem in client_proxy.
      // 5. Await the local JPItem, not the original remote application request.
      // 6. When client_proxy receives notification, run JPItem.on_call.
      // 7. Unbox the opaque final output and resolve this Promise with it.
      //
      // Pseudocode:
      const output = await wrappedCallable(...args);
      console.log("doing client_proxy.inspect_call_output");
      // if (!clientProxy.isBoxed(output)) {
      //   return output;
      // }
      console.log("doing client_proxy.create_local_jpitem_if_boxed");
      // The boxed job_id should match the client-owned JobPing context.
      // if (output.job_id !== jobId) {
      //   throw new Error("Unexpected boxed output job_id");
      // }
      // const localItem = clientProxy.createLocalItem(output.job_id);
      console.log("doing client_proxy.await_local_jpitem_if_boxed");
      // const boxedOutput = await localItem.wait();
      // const finalOutput = clientProxy.unbox(boxedOutput);
      // return finalOutput;
      console.log("doing local_jpitem.on_call_unbox_output_if_boxed");
      return output;
    };
  },
};

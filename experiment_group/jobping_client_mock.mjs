// Mock client-side JobPing SDK for usage-first TDD examples.

export const jobping = {
  wrap(wrappedCallable) {
    return async function jobpingWrappedCallable(...args) {
      console.log("doing client_proxy.capture_call_input");
      console.log("doing client_proxy.call_wrapped_callable");
      // Future flow:
      // 1. Treat ...args as opaque call input.
      // 2. Call the wrapped callable. Its output may be a boxed job_id envelope.
      // 3. If the output is boxed, create a local JPItem in client_proxy.
      // 4. Await the local JPItem, not the original remote application request.
      // 5. When client_proxy receives notification, run JPItem.on_call.
      // 6. Unbox the opaque final output and resolve this Promise with it.
      //
      // Pseudocode:
      // const output = await wrappedCallable(...args);
      // if (!clientProxy.isBoxed(output)) {
      //   return output;
      // }
      // const localItem = clientProxy.createLocalItem(output.job_id);
      // const boxedOutput = await localItem.wait();
      // return clientProxy.unbox(boxedOutput);
      const output = await wrappedCallable(...args);
      console.log("doing client_proxy.inspect_call_output");
      console.log("doing client_proxy.create_local_jpitem_if_boxed");
      console.log("doing client_proxy.await_local_jpitem_if_boxed");
      console.log("doing local_jpitem.on_call_unbox_output_if_boxed");
      return output;
    };
  },
};

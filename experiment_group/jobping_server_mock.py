"""Mock server-side JobPing helper for usage-first TDD examples."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar


Result = TypeVar("Result")


class JobPingServerMock:
    """Placeholder for the future integrated boxing/ping helper."""

    def wrap(
        self,
    ) -> Callable[[Callable[..., Awaitable[Result]]], Callable[..., Awaitable[Result]]]:
        def decorator(
            wrapped_callable: Callable[..., Awaitable[Result]],
        ) -> Callable[..., Awaitable[Result]]:
            @wraps(wrapped_callable)
            async def wrapper(*args: Any, **kwargs: Any) -> Result:
                print("doing server_proxy.capture_call_input")
                print("doing server_proxy.inspect_transport_context")
                # Future flow:
                # 1. Treat *args and **kwargs as opaque call input.
                # 2. Ask a transport adapter whether this request carries JobPing
                #    context. The wrapper itself should not know HTTP headers,
                #    websocket handshakes, client addresses, or framework details.
                # 3. If no valid JobPing context exists, call the wrapped callable
                #    normally and return its opaque output.
                # 4. If valid JobPing context exists, offer a producer JPItem for
                #    that job_id and return a boxed job_ref envelope quickly.
                # 5. Keep running the wrapped callable outside the open request.
                # 6. Treat the callable return value as opaque call output.
                # 7. Box that output and notify the waiting peer.
                #
                # Pseudocode:
                # job_id = transport_adapter.extract_job_id(context)
                #
                # The transport adapter may read an HTTP header, websocket metadata,
                # RPC metadata, or another carrier. That choice is intentionally not
                # a concrete API here because this mock keeps wrapper responsibilities
                # separate from protocol detection and parameter acquisition.
                #
                # if job_id is None:
                #     output = await wrapped_callable(*args, **kwargs)
                #     return output
                #
                # jp_item = endpoint_queue.offer(job_id)
                # endpoint_queue.defer(jp_item)
                # endpoint_proxy.fulfill_later(
                #     job_id=job_id,
                #     task=lambda: wrapped_callable(*args, **kwargs),
                #     on_done=lambda output: endpoint_queue.fulfill(job_id, output),
                # )
                # return box_job_ref(job_id)

                print("doing server_proxy.no_jobping_context_call_wrapped_callable")
                output = await wrapped_callable(*args, **kwargs)
                print("doing server_proxy.capture_call_output")
                # With valid JobPing context, these would run in the deferred task:
                # print("doing endpoint_queue.fulfill")
                # print("doing envelope_endpoint.send")
                # print("doing server_proxy.return_job_ref_offer")
                return output

            return wrapper

        return decorator


jobping = JobPingServerMock()

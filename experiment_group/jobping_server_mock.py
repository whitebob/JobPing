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
                # 4. If valid JobPing context exists, create/register a remote
                #    JPItem for that job_id and return a boxed envelope quickly.
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
                # jp_item = server_proxy.create_remote_jpitem(job_id)
                # server_proxy.enqueue_remote_jpitem(jp_item)
                # server_proxy.run_later(
                #     job_id=job_id,
                #     task=lambda: wrapped_callable(*args, **kwargs),
                #     on_done=lambda output: server_proxy.box_and_notify(
                #         jp_item,
                #         output,
                #     ),
                # )
                # return ResultEnvelope(job_id=job_id)

                print("doing server_proxy.no_jobping_context_call_wrapped_callable")
                output = await wrapped_callable(*args, **kwargs)
                print("doing server_proxy.capture_call_output")
                # With valid JobPing context, these would run in the deferred task:
                # print("doing server_proxy.box_output")
                # print("doing server_proxy.notify_client_proxy")
                # print("doing server_proxy.return_boxed_job_id")
                return output

            return wrapper

        return decorator


jobping = JobPingServerMock()

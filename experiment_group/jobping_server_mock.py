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
                print("doing server_proxy.enqueue_remote_jpitem")
                # Future flow:
                # 1. Treat *args and **kwargs as opaque call input.
                # 2. Create a remote JPItem with a new job_id in server_proxy.
                # 3. Return a boxed object containing that job_id immediately.
                # 4. Keep running the wrapped callable outside the open request.
                # 5. Treat the callable return value as opaque call output.
                # 6. Box that output and notify the waiting peer.
                #
                # Pseudocode:
                # call_input = CallInput(args=args, kwargs=kwargs)
                # jp_item = server_proxy.enqueue_remote_item(call_input)
                # server_proxy.run_later(
                #     job_id=jp_item.job_id,
                #     task=lambda: wrapped_callable(*args, **kwargs),
                #     on_done=lambda output: server_proxy.box_and_notify(jp_item, output),
                # )
                # return ResultEnvelope(job_id=jp_item.job_id)
                print("doing server_proxy.return_boxed_job_id")
                output = await wrapped_callable(*args, **kwargs)
                print("doing server_proxy.capture_call_output")
                print("doing server_proxy.box_output")
                print("doing server_proxy.notify_client_proxy")
                return output

            return wrapper

        return decorator


jobping = JobPingServerMock()

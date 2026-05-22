"""JobPing wrapper facade."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar

from jobping.endpoint_proxy import EndpointProxy
from jobping.result_handoff import ResultHandoff
from jobping.state_sync import StateSync
from jobping.transport_layer import TransportLayer


Result = TypeVar("Result")
JobRef = dict[str, str]


def is_jobping_disabled() -> bool:
    return os.environ.get("JOBPING_DISABLED", "").lower() in {"1", "true", "yes", "on"}


class JobPing:
    def __init__(
        self,
        *,
        endpoint_proxy: EndpointProxy,
        job_context_provider: Callable[..., str | None] | None = None,
    ) -> None:
        self.endpoint_proxy = endpoint_proxy
        self.job_context_provider = job_context_provider or (lambda *args, **kwargs: None)

    def wrap(
        self,
    ) -> Callable[
        [Callable[..., Awaitable[Result]]], Callable[..., Awaitable[Result | JobRef]]
    ]:
        def decorator(
            wrapped_callable: Callable[..., Awaitable[Result]],
        ) -> Callable[..., Awaitable[Result | JobRef]]:
            @wraps(wrapped_callable)
            async def wrapper(*args: Any, **kwargs: Any) -> Result | JobRef:
                if is_jobping_disabled():
                    return await wrapped_callable(*args, **kwargs)

                print("doing server_proxy.capture_call_input")
                print("doing server_proxy.inspect_transport_context")
                job_id = self.job_context_provider(*args, **kwargs)

                if job_id is not None:
                    print("doing endpoint_proxy.offer")
                    jp_item = self.endpoint_proxy.offer(job_id)
                    print("doing endpoint_proxy.defer")
                    self.endpoint_proxy.defer(jp_item)
                    print("doing endpoint_proxy.fulfill_later")
                    asyncio.create_task(
                        self.endpoint_proxy.fulfill_later(
                            job_id,
                            lambda: wrapped_callable(*args, **kwargs),
                        ),
                    )
                    print("doing server_proxy.return_job_ref_offer")
                    return self.endpoint_proxy.make_job_ref(job_id)

                print("doing server_proxy.no_jobping_context_call_wrapped_callable")
                output = await wrapped_callable(*args, **kwargs)
                print("doing server_proxy.capture_call_output")
                return output

            return wrapper

        return decorator

JobPingClass = JobPing


def create_jobping(
    *,
    transport_layer: TransportLayer,
    queue: Any,
    result_transport_layer: TransportLayer | None = None,
    job_context_provider: Callable[..., str | None] | None = None,
) -> JobPing:
    result_transport = result_transport_layer or transport_layer
    endpoint_proxy = EndpointProxy(
        state_sync=StateSync(transport_layer),
        result_handoff=ResultHandoff(result_transport),
        queue=queue,
    )
    return JobPing(
        endpoint_proxy=endpoint_proxy,
        job_context_provider=job_context_provider,
    )


JobPingServerMock = JobPingClass

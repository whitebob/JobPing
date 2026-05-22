"""Transport-neutral JobPing result envelope."""

from __future__ import annotations

from typing import Any, Literal, TypedDict


JOBPING_ENVELOPE_KIND = "jobping.envelope.v1"
JOBPING_RESULT = "result"

EnvelopeType = Literal["result"]


class ResultEnvelope(TypedDict):
    jobping: str
    type: Literal["result"]
    job_id: str
    payload: Any


JobPingEnvelope = ResultEnvelope


def _assert_valid_job_id(job_id: str) -> None:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")


def box_result(job_id: str, payload: Any) -> ResultEnvelope:
    _assert_valid_job_id(job_id)

    return {
        "jobping": JOBPING_ENVELOPE_KIND,
        "type": JOBPING_RESULT,
        "job_id": job_id,
        "payload": payload,
    }


def is_envelope(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and value.get("jobping") == JOBPING_ENVELOPE_KIND
        and isinstance(value.get("type"), str)
        and isinstance(value.get("job_id"), str)
        and len(value["job_id"]) > 0
    )


def is_result_envelope(value: Any) -> bool:
    return is_envelope(value) and value["type"] == JOBPING_RESULT and "payload" in value


def unbox_result(envelope: Any, expected_job_id: str | None = None) -> Any:
    if not is_result_envelope(envelope):
        raise ValueError("Expected JobPing result envelope")

    if expected_job_id is not None and envelope["job_id"] != expected_job_id:
        raise ValueError("Unexpected JobPing result job_id")

    return envelope["payload"]

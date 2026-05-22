"""Compatibility exports for code that still imports the old mock module."""

from experiment_group.jobping_result_handoff import (
    JOBPING_RESULT_HANDOFF,
    ResultHandoff,
)

MockResultHandoff = ResultHandoff

__all__ = ["JOBPING_RESULT_HANDOFF", "ResultHandoff", "MockResultHandoff"]

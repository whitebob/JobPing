"""Compatibility exports for code that still imports the old mock module."""

from examples.experiment_group.jobping_state_sync import (
    JOBPING_STATE_UPDATE,
    StateSync,
    StateUpdate,
)

MockStateSync = StateSync

__all__ = ["JOBPING_STATE_UPDATE", "StateSync", "StateUpdate", "MockStateSync"]

from jobping.server import JobPing, is_jobping_disabled
from jobping_sandbox import create_mock_endpoint_proxy

JobPingServerMock = JobPing
jobping = JobPing(endpoint_proxy=create_mock_endpoint_proxy())

__all__ = ["JobPing", "JobPingServerMock", "is_jobping_disabled", "jobping"]

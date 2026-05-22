import { JobPing } from "../../packages/js/jobping.mjs";
import { createMockEndpointProxy } from "../../sandbox/js/create_mock_endpoint_proxy.mjs";

export { JobPing, JobPing as JobPingClientMock, isJobPingDisabled } from "../../packages/js/jobping.mjs";

export const jobping = new JobPing({
  endpointProxy: createMockEndpointProxy(),
});

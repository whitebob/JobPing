export const JOBPING_STATE_UPDATE = "jobping.state_update.v1";

function assertValidJobId(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
}

function assertValidStatus(status) {
  if (typeof status !== "string" || status.length === 0) {
    throw new Error("status must be a non-empty string");
  }
}

export class StateSync {
  constructor({ transportLayer }) {
    if (!transportLayer?.sendMessage || !transportLayer?.recvMessage) {
      throw new Error("StateSync requires a transport layer with sendMessage/recvMessage");
    }

    this.transportLayer = transportLayer;
  }

  publish(jobId, status, stateContext = {}) {
    assertValidJobId(jobId);
    assertValidStatus(status);

    this.transportLayer.sendMessage({
      kind: JOBPING_STATE_UPDATE,
      job_id: jobId,
      data: {
        status,
        state_context: stateContext,
      },
    });
  }

  async waitFor(jobId, { status, timeoutMs = 1000 } = {}) {
    assertValidJobId(jobId);

    while (true) {
      const message = await this.transportLayer.recvMessage({
        kind: JOBPING_STATE_UPDATE,
        jobId,
        timeoutMs,
      });
      const state = message.data;

      if (status === undefined || state.status === status) {
        return state;
      }
    }
  }
}

import { boxResult, unboxResult } from "./envelope.mjs";

export const JOBPING_RESULT_HANDOFF = "jobping.result_handoff.v1";

function assertValidJobId(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
}

export class ResultHandoff {
  constructor({ transportLayer }) {
    if (!transportLayer?.sendMessage || !transportLayer?.recvMessage) {
      throw new Error("ResultHandoff requires a transport layer with sendMessage/recvMessage");
    }

    this.transportLayer = transportLayer;
  }

  fulfill(jobId, result) {
    assertValidJobId(jobId);

    this.transportLayer.sendMessage({
      kind: JOBPING_RESULT_HANDOFF,
      job_id: jobId,
      data: boxResult(jobId, result),
    });
  }

  async awaitResult(jobId, { timeoutMs = 1000 } = {}) {
    assertValidJobId(jobId);

    const message = await this.transportLayer.recvMessage({
      kind: JOBPING_RESULT_HANDOFF,
      jobId,
      timeoutMs,
    });

    return unboxResult(message.data, jobId);
  }
}

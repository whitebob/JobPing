// TransportLayer defines how JobPing metadata and semantic messages move.
//
// Concrete transports may use HTTP headers, WebSocket messages, SSE+POST,
// Kafka, Redis, RabbitMQ, or another carrier. This layer does not manage
// JPItem lifecycle and does not inspect business results.

export const JOBPING_JOB_ID_HEADER = "x-jobping-job-id";

export class TransportLayer {
  constructor() {
    if (new.target === TransportLayer) {
      throw new Error("TransportLayer is abstract; use a concrete implementation");
    }
  }

  attachJobId() {
    throw new Error("TransportLayer.attachJobId() must be implemented");
  }

  extractJobId() {
    throw new Error("TransportLayer.extractJobId() must be implemented");
  }

  attachEnvelope() {
    throw new Error("TransportLayer.attachEnvelope() must be implemented");
  }

  extractEnvelope() {
    throw new Error("TransportLayer.extractEnvelope() must be implemented");
  }

  sendEnvelope() {
    throw new Error("TransportLayer.sendEnvelope() must be implemented");
  }

  recvEnvelope() {
    throw new Error("TransportLayer.recvEnvelope() must be implemented");
  }

  sendMessage() {
    throw new Error("TransportLayer.sendMessage() must be implemented");
  }

  recvMessage() {
    throw new Error("TransportLayer.recvMessage() must be implemented");
  }
}

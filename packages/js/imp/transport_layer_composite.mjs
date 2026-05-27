// CompositeTransportLayer — merges multiple TransportLayer instances into one.

import { TransportLayer, JOBPING_JOB_ID_HEADER } from "../transport_layer.mjs";

export class CompositeTransportLayer extends TransportLayer {
  constructor(transports) {
    super();
    if (!Array.isArray(transports) || transports.length < 2) {
      throw new Error("CompositeTransportLayer requires at least 2 transports");
    }
    this._transports = [...transports];
  }

  get transports() {
    return [...this._transports];
  }

  // -- carrier metadata ---------------------------------------------------

  attachJobId(carrier = {}, jobId) {
    return this._transports[0].attachJobId(carrier, jobId);
  }

  extractJobId(carrier = {}) {
    return this._transports[0].extractJobId(carrier);
  }

  attachEnvelope(carrier = {}, envelope) {
    return this._transports[0].attachEnvelope(carrier, envelope);
  }

  extractEnvelope(carrier = {}) {
    return this._transports[0].extractEnvelope(carrier);
  }

  // -- message I/O --------------------------------------------------------

  sendMessage(message) {
    for (const t of this._transports) {
      t.sendMessage(message);
    }
  }

  recvMessage({ kind, jobId, timeout, timeoutMs = 1000 } = {}) {
    return Promise.race(
      this._transports.map((t) =>
        t.recvMessage({ kind, jobId, timeout, timeoutMs }).catch(() => {
          // If one transport times out, we want Promise.race to wait for
          // another to succeed.  Translate the rejection into a never-
          // resolving promise so it doesn't poison the race.
          return new Promise(() => {});
        })
      )
    );
  }

  // -- envelope I/O -------------------------------------------------------

  sendEnvelope(envelope) {
    for (const t of this._transports) {
      t.sendEnvelope(envelope);
    }
  }

  recvEnvelope({ jobId, type, timeout, timeoutMs = 1000 } = {}) {
    return Promise.race(
      this._transports.map((t) =>
        t.recvEnvelope({ jobId, type, timeout, timeoutMs }).catch(() => {
          return new Promise(() => {});
        })
      )
    );
  }
}

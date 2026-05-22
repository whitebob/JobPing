// HTTP(S) TransportLayer implementation (under imp)
// Uses fetch() to POST envelopes/messages and poll GET endpoints for new items.
// This implementation expects a simple HTTP server that exposes the following
// endpoints:
//  - POST {baseUrl}/envelope
//  - GET  {baseUrl}/envelope?jobId=...&type=...
//  - POST {baseUrl}/message
//  - GET  {baseUrl}/message?kind=...&jobId=...

import { TransportLayer, JOBPING_JOB_ID_HEADER } from "../transport_layer.mjs";

export class TransportLayerHTTPS extends TransportLayer {
  constructor({ baseUrl } = {}) {
    super();
    if (!baseUrl) throw new Error("baseUrl is required for TransportLayerHTTPS");
    this.baseUrl = baseUrl.replace(/\/+$/g, "");
  }

  attachJobId(carrier = {}, jobId) {
    if (typeof jobId !== "string" || jobId.length === 0) throw new Error("job_id must be a non-empty string");
    return {
      ...carrier,
      headers: {
        ...(carrier.headers ?? {}),
        [JOBPING_JOB_ID_HEADER]: jobId,
      },
    };
  }

  extractJobId(carrier = {}) {
    const headers = carrier.headers ?? {};
    for (const [k, v] of Object.entries(headers)) {
      if (k.toLowerCase() === JOBPING_JOB_ID_HEADER.toLowerCase()) return v;
    }
    return undefined;
  }

  attachEnvelope(carrier = {}, envelope) {
    return { ...carrier, envelope };
  }

  extractEnvelope(carrier = {}) {
    const e = carrier.envelope;
    return e && typeof e === "object" ? e : undefined;
  }

  sendEnvelope(envelope) {
    const url = `${this.baseUrl}/envelope`;
    // fire-and-forget
    fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(envelope) }).catch(() => {});
  }

  async recvEnvelope({ jobId, type, timeout = 1000 } = {}) {
    const url = new URL(`${this.baseUrl}/envelope`);
    if (jobId != null) url.searchParams.set("jobId", jobId);
    if (type != null) url.searchParams.set("type", type);

    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      try {
        const resp = await fetch(url.toString());
        if (resp.status === 200) return await resp.json();
      } catch (e) {
        // ignore and retry
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error("Timed out waiting for envelope");
  }

  sendMessage(message) {
    const url = `${this.baseUrl}/message`;
    fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(message) }).catch(() => {});
  }

  async recvMessage({ kind, jobId, timeout = 1000 } = {}) {
    const url = new URL(`${this.baseUrl}/message`);
    if (kind != null) url.searchParams.set("kind", kind);
    if (jobId != null) url.searchParams.set("jobId", jobId);

    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      try {
        const resp = await fetch(url.toString());
        if (resp.status === 200) return await resp.json();
      } catch (e) {}
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error("Timed out waiting for transport message");
  }

  size() {
    return { messages: 0, waiters: 0 };
  }
}

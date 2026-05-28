// Browser-compatible ID helpers using Web Crypto API (available in all modern
// browsers and in Node 19+).
export function createJobId() {
  return crypto.randomUUID();
}

export function createPeerId() {
  return `peer-${crypto.randomUUID()}`;
}

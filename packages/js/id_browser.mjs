// Browser-compatible createJobId using Web Crypto API (available in all modern
// browsers and in Node 19+).
export function createJobId() {
  return crypto.randomUUID();
}

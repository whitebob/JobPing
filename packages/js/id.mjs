import { randomUUID } from "node:crypto";

export function createJobId() {
  return randomUUID();
}

export function createPeerId() {
  return `peer-${randomUUID()}`;
}

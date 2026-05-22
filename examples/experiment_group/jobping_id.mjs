import { randomUUID } from "node:crypto";

export function createJobId() {
  return randomUUID();
}

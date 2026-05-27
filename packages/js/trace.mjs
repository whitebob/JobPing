// Trace analysis helpers for JobPing diagnostic data.

export class TraceNode {
  constructor({ job_id, peer_id, hop, elapsed, self_time, sub_jobs = [] }) {
    this.job_id = job_id;
    this.peer_id = peer_id;
    this.hop = hop;
    this.elapsed = elapsed;
    this.self_time = self_time;
    this.sub_jobs = sub_jobs;
  }
}

export class TraceReport {
  constructor({ root, total_elapsed, critical_path, bottleneck, call_graph }) {
    this.root = root;
    this.total_elapsed = total_elapsed;
    this.critical_path = critical_path;
    this.bottleneck = bottleneck;
    this.call_graph = call_graph;
  }
}

function parseNode(raw) {
  const subNodes = (raw.sub_jobs || []).map(parseNode);
  const elapsed = parseFloat(raw.elapsed) || 0;
  const subElapsed = subNodes.reduce((sum, sn) => sum + sn.elapsed, 0);
  return new TraceNode({
    job_id: raw.job_id || "?",
    peer_id: raw.peer_id || "?",
    hop: parseInt(raw.hop, 10) || 1,
    elapsed,
    self_time: Math.max(0, elapsed - subElapsed),
    sub_jobs: subNodes,
  });
}

function findCriticalPath(node) {
  const path = [node];
  if (!node.sub_jobs.length) return path;
  const longestChild = node.sub_jobs.reduce((a, b) => (a.elapsed > b.elapsed ? a : b));
  path.push(...findCriticalPath(longestChild));
  return path;
}

function findBottleneckNode(node) {
  let best = node;
  for (const child of node.sub_jobs) {
    const candidate = findBottleneckNode(child);
    if (candidate.self_time > best.self_time) best = candidate;
  }
  return best;
}

function buildAdjacency(node) {
  const graph = {};
  if (node.sub_jobs.length) {
    graph[node.job_id] = node.sub_jobs.map((c) => c.job_id);
  }
  for (const child of node.sub_jobs) {
    Object.assign(graph, buildAdjacency(child));
  }
  return graph;
}

export function parseTrace(raw) {
  const root = parseNode(raw);
  return new TraceReport({
    root,
    total_elapsed: root.elapsed,
    critical_path: findCriticalPath(root),
    bottleneck: findBottleneckNode(root),
    call_graph: buildAdjacency(root),
  });
}

export function findBottleneck(report) {
  const b = report.bottleneck;
  if (report.total_elapsed > 0 && b.self_time / report.total_elapsed > 0.5) {
    return (
      `Bottleneck: ${b.peer_id} (job ${b.job_id}) — ` +
      `self_time=${b.self_time.toFixed(2)}s ` +
      `(${(b.self_time / report.total_elapsed * 100).toFixed(0)}% of total ${report.total_elapsed.toFixed(2)}s)`
    );
  }
  return (
    `Balanced — max self_time is ${b.peer_id} ` +
    `(${b.self_time.toFixed(2)}s of ${report.total_elapsed.toFixed(2)}s total)`
  );
}

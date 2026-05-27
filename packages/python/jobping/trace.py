"""Trace analysis helpers for JobPing diagnostic data.

These are pure functions that consume the ``_trace`` dicts produced when
``wrap_trace()`` is used.  They do not depend on any transport or broker —
you can call them on stored trace payloads offline.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TraceNode:
    job_id: str
    peer_id: str
    hop: int
    elapsed: float          # total wall-clock including sub-calls
    self_time: float        # own time = elapsed - sum(sub.elapsed)
    sub_jobs: list[TraceNode] = field(default_factory=list)


@dataclass
class TraceReport:
    root: TraceNode
    total_elapsed: float
    critical_path: list[TraceNode]
    bottleneck: TraceNode
    call_graph: dict[str, list[str]]  # job_id → [child_job_ids]


def parse_trace(raw: dict) -> TraceReport:
    """Convert a raw ``_trace`` dict into a structured ``TraceReport``."""
    root = _parse_node(raw)
    return TraceReport(
        root=root,
        total_elapsed=root.elapsed,
        critical_path=_find_critical_path(root),
        bottleneck=_find_bottleneck(root),
        call_graph=_build_adjacency(root),
    )


def find_bottleneck(report: TraceReport) -> str:
    """Return a human-readable bottleneck summary."""
    b = report.bottleneck
    if report.total_elapsed > 0 and b.self_time / report.total_elapsed > 0.5:
        return (
            f"Bottleneck: {b.peer_id} (job {b.job_id}) — "
            f"self_time={b.self_time:.2f}s "
            f"({b.self_time / report.total_elapsed:.0%} of total {report.total_elapsed:.2f}s)"
        )
    return (
        f"Balanced — max self_time is {b.peer_id} "
        f"({b.self_time:.2f}s of {report.total_elapsed:.2f}s total)"
    )


# -- internal helpers ------------------------------------------------------

def _parse_node(raw: dict) -> TraceNode:
    sub_nodes = [_parse_node(sj) for sj in raw.get("sub_jobs", [])]
    elapsed = float(raw.get("elapsed", 0))
    sub_elapsed = sum(sn.elapsed for sn in sub_nodes)
    return TraceNode(
        job_id=raw.get("job_id", "?"),
        peer_id=raw.get("peer_id", "?"),
        hop=int(raw.get("hop", 1)),
        elapsed=elapsed,
        self_time=max(0, elapsed - sub_elapsed),
        sub_jobs=sub_nodes,
    )


def _find_critical_path(node: TraceNode) -> list[TraceNode]:
    """Longest path from *node* to a leaf."""
    path = [node]
    if not node.sub_jobs:
        return path
    longest_child = max(node.sub_jobs, key=lambda n: n.elapsed)
    path.extend(_find_critical_path(longest_child))
    return path


def _find_bottleneck(node: TraceNode) -> TraceNode:
    """Node with the largest self_time in the tree."""
    best = node
    for child in node.sub_jobs:
        candidate = _find_bottleneck(child)
        if candidate.self_time > best.self_time:
            best = candidate
    return best


def _build_adjacency(node: TraceNode) -> dict[str, list[str]]:
    graph: dict[str, list[str]] = {}
    children = [child.job_id for child in node.sub_jobs]
    if children:
        graph[node.job_id] = children
    for child in node.sub_jobs:
        graph.update(_build_adjacency(child))
    return graph

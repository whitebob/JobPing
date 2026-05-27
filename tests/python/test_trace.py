"""Tests for trace parsing and diagnostic helpers."""

from __future__ import annotations

from jobping.trace import TraceNode, TraceReport, find_bottleneck, parse_trace


def test_parse_trace_single_node():
    raw = {"job_id": "j1", "peer_id": "peer-a", "hop": 1, "elapsed": 2.5}
    report = parse_trace(raw)

    assert isinstance(report, TraceReport)
    assert report.total_elapsed == 2.5
    assert report.root.job_id == "j1"
    assert report.root.peer_id == "peer-a"
    assert report.root.hop == 1
    assert report.root.elapsed == 2.5
    assert report.root.self_time == 2.5
    assert report.root.sub_jobs == []
    assert report.critical_path == [report.root]
    assert report.bottleneck == report.root


def test_parse_trace_nested():
    raw = {
        "job_id": "root",
        "peer_id": "peer-a",
        "hop": 1,
        "elapsed": 10.0,
        "sub_jobs": [
            {"job_id": "child1", "peer_id": "peer-b", "hop": 2, "elapsed": 3.0},
            {"job_id": "child2", "peer_id": "peer-c", "hop": 2, "elapsed": 5.0},
        ],
    }
    report = parse_trace(raw)

    assert report.total_elapsed == 10.0
    # self_time = elapsed - sum(sub.elapsed) = 10 - (3+5) = 2
    assert report.root.self_time == 2.0
    assert len(report.root.sub_jobs) == 2
    assert report.root.sub_jobs[0].job_id == "child1"
    assert report.root.sub_jobs[0].hop == 2
    assert report.root.sub_jobs[0].self_time == 3.0
    assert report.root.sub_jobs[1].job_id == "child2"
    assert report.root.sub_jobs[1].self_time == 5.0
    # child2 is the longest sub-job, so the critical path goes through it
    assert len(report.critical_path) == 2
    assert report.critical_path[0].job_id == "root"
    assert report.critical_path[1].job_id == "child2"
    # bottleneck is child2 (largest self_time = 5.0)
    assert report.bottleneck.job_id == "child2"
    # call graph
    assert report.call_graph == {"root": ["child1", "child2"]}


def test_parse_trace_defaults_missing_fields():
    raw = {}  # no fields at all
    report = parse_trace(raw)

    assert report.root.job_id == "?"
    assert report.root.peer_id == "?"
    assert report.root.hop == 1
    assert report.root.elapsed == 0.0
    assert report.root.self_time == 0.0
    assert report.root.sub_jobs == []


def test_self_time_never_negative():
    """When sub-job elapsed exceeds parent elapsed, self_time is clamped to 0."""
    raw = {
        "job_id": "root",
        "elapsed": 1.0,
        "sub_jobs": [
            {"job_id": "c1", "elapsed": 2.0},
            {"job_id": "c2", "elapsed": 3.0},
        ],
    }
    report = parse_trace(raw)
    # sum(child elapsed) = 5.0 > 1.0, so clamped
    assert report.root.self_time == 0.0


def test_critical_path_ties_pick_first():
    """When two children have equal elapsed, max() returns the first one."""
    raw = {
        "job_id": "root",
        "elapsed": 10.0,
        "sub_jobs": [
            {"job_id": "child_a", "elapsed": 4.0},
            {"job_id": "child_b", "elapsed": 4.0},
        ],
    }
    report = parse_trace(raw)
    # both children have same elapsed; first one wins
    assert report.critical_path[1].job_id == "child_a"


def test_bottleneck_is_root_when_no_children():
    raw = {"job_id": "only", "elapsed": 1.5}
    report = parse_trace(raw)
    assert report.bottleneck.job_id == "only"
    assert report.bottleneck.self_time == 1.5


def test_bottleneck_deeply_nested():
    raw = {
        "job_id": "root",
        "elapsed": 20.0,
        "sub_jobs": [
            {
                "job_id": "a",
                "elapsed": 8.0,
                "sub_jobs": [
                    {"job_id": "a1", "elapsed": 2.0},
                    {"job_id": "a2", "elapsed": 5.0},  # self_time=5 inside 'a'
                ],
            },
            {"job_id": "b", "elapsed": 4.0},  # self_time=4
        ],
    }
    report = parse_trace(raw)
    # self_time breakdown: root=20-8-4=8, a=8-2-5=1, a1=2, a2=5, b=4
    # root has the largest self_time (8.0)
    assert report.bottleneck.job_id == "root"
    assert report.bottleneck.self_time == 8.0


def test_find_bottleneck_string_above_threshold():
    raw = {
        "job_id": "slow",
        "peer_id": "db-server",
        "elapsed": 10.0,
        "sub_jobs": [
            {"job_id": "fast", "peer_id": "cache", "elapsed": 1.0},
        ],
    }
    report = parse_trace(raw)
    # root self_time = 10 - 1 = 9, which is 90% > 50%
    msg = find_bottleneck(report)
    assert "Bottleneck:" in msg
    assert "db-server" in msg
    assert "90%" in msg


def test_find_bottleneck_string_below_threshold():
    raw = {
        "job_id": "root",
        "peer_id": "svc",
        "elapsed": 10.0,
        "sub_jobs": [
            {"job_id": "c1", "peer_id": "w1", "elapsed": 5.1},
        ],
    }
    report = parse_trace(raw)
    # root self_time = 4.9 (~49%), c1 self_time = 5.1 (~51%)
    # bottleneck is c1 at 51% > 50%
    msg = find_bottleneck(report)
    assert "Bottleneck:" in msg
    assert "w1" in msg


def test_find_bottleneck_string_truly_balanced():
    raw = {
        "job_id": "root",
        "peer_id": "svc",
        "elapsed": 10.0,
        "sub_jobs": [
            {"job_id": "c1", "peer_id": "w1", "elapsed": 4.0},
            {"job_id": "c2", "peer_id": "w2", "elapsed": 4.0},
        ],
    }
    report = parse_trace(raw)
    # root self_time = 2.0 (20%), c1=4.0 (40%), c2=4.0 (40%)
    # bottleneck is c1 or c2 at 40% < 50% => "Balanced"
    msg = find_bottleneck(report)
    assert "Balanced" in msg


def test_find_bottleneck_string_exactly_50_percent():
    raw = {
        "job_id": "root",
        "peer_id": "svc",
        "elapsed": 10.0,
        "sub_jobs": [
            {"job_id": "c1", "peer_id": "w1", "elapsed": 5.0},
        ],
    }
    report = parse_trace(raw)
    # root self_time = 5.0, exactly 50% => not > 0.5, so "Balanced"
    msg = find_bottleneck(report)
    assert "Balanced" in msg


def test_find_bottleneck_string_zero_elapsed():
    raw = {"job_id": "z", "elapsed": 0.0}
    report = parse_trace(raw)
    msg = find_bottleneck(report)
    assert "Balanced" in msg
    assert "0.00s of 0.00s" in msg


def test_call_graph_empty_for_leaf():
    raw = {"job_id": "leaf", "elapsed": 1.0}
    report = parse_trace(raw)
    assert report.call_graph == {}


def test_call_graph_nested():
    raw = {
        "job_id": "root",
        "elapsed": 5.0,
        "sub_jobs": [
            {"job_id": "c1", "elapsed": 2.0},
            {
                "job_id": "c2",
                "elapsed": 3.0,
                "sub_jobs": [
                    {"job_id": "gc1", "elapsed": 1.5},
                ],
            },
        ],
    }
    report = parse_trace(raw)
    # root -> [c1, c2], c2 -> [gc1]
    assert report.call_graph == {
        "root": ["c1", "c2"],
        "c2": ["gc1"],
    }


def test_truncated_node_uses_defaults():
    """A _truncated node has no job_id/elapsed fields — defaults kick in."""
    raw = {
        "job_id": "root",
        "elapsed": 2.0,
        "sub_jobs": [
            {"_truncated": True},
        ],
    }
    report = parse_trace(raw)
    truncated = report.root.sub_jobs[0]
    assert truncated.job_id == "?"
    assert truncated.peer_id == "?"
    assert truncated.elapsed == 0.0
    assert truncated.hop == 1

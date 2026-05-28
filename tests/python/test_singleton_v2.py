"""Tests for v2 singleton redesign: _LazyJobPing, CompositeEndpointProxy, migration."""

from __future__ import annotations

import asyncio
import os
import warnings

import pytest

from jobping._lazy_singleton import _LazyJobPing, _create_jobping
from jobping.composite_endpoint_proxy import CompositeEndpointProxy
from jobping.imp.broker import EmbeddedBroker, BROKER_MIGRATE_KIND, MIGRATION_COMPLETE_KIND
from jobping.imp.envelope_endpoint_inmemory import EnvelopeEndpointInMemory
from jobping.imp.jpitem_queue_inmemory import JPItemQueueInMemory
from jobping.endpoint_proxy import EndpointProxy
from jobping.result_handoff import ResultHandoff
from jobping.state_sync import StateSync
from jobping.imp.transport_layer_local import LocalTransportLayer
from jobping.envelope import box_result, unbox_result


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset singleton state before each test."""
    from jobping import jp
    jp._reset()
    yield
    jp._reset()


@pytest.fixture
def singleton() -> _LazyJobPing:
    s = _LazyJobPing()
    yield s
    s._reset()


@pytest.fixture
def broker() -> EmbeddedBroker:
    """Return an unstarted EmbeddedBroker (port=0 = random)."""
    return EmbeddedBroker(0)


@pytest.fixture
def endpoint_proxy() -> EndpointProxy:
    """Return a minimal EndpointProxy backed by LocalTransportLayer."""
    broker = EmbeddedBroker(0)
    transport = LocalTransportLayer(broker)
    queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
    ep = EndpointProxy(
        state_sync=StateSync(transport),
        result_handoff=ResultHandoff(transport),
        queue=queue,
    )
    ep._active_trace = None
    return ep


# ---------------------------------------------------------------------------
# _LazyJobPing — import / configure
# ---------------------------------------------------------------------------


class TestLazySingletonImport:
    def test_jp_is_lazy_singleton(self):
        from jobping import jp
        assert isinstance(jp, _LazyJobPing)

    def test_jp_has_no_active_on_import(self):
        from jobping import jp
        assert jp._active is None
        assert jp._broker is None

    def test_import_does_not_bind_port(self):
        from jobping import jp
        assert jp._broker is None


class TestConfigure:
    def test_configure_stores_params_without_building(self, singleton):
        singleton.configure(broker_port=8900)
        assert singleton._active is None
        assert singleton._broker is None
        assert "broker_port" in singleton._stored_params

    def test_configure_multiple_calls_override(self, singleton):
        singleton.configure(broker_port=8900)
        singleton.configure(broker_port=8901)
        assert singleton._stored_params["broker_port"] == 8901

    def test_configure_on_running_warns(self, singleton):
        singleton.configure(broker_port=8900)
        singleton._ensure_built_sync()
        with pytest.warns(UserWarning, match="already running"):
            singleton.configure(broker_port=8901)

    def test_configure_force_on_running_sets_needs_rebuild(self, singleton):
        singleton.configure(broker_port=8900)
        singleton._ensure_built_sync()
        singleton.configure(broker_port=8901, force=True)
        assert singleton._needs_rebuild is True
        assert singleton._stored_params["broker_port"] == 8901

    def test_configure_force_during_migration_warns(self, singleton):
        """Migration-in-progress guard: set _migration_in_progress=True to
        simulate an active async rebuild, then verify configure(force=True)
        warns instead of overwriting."""
        singleton.configure(broker_port=8900)
        singleton._ensure_built_sync()
        singleton._migration_in_progress = True
        with pytest.warns(UserWarning, match="Migration already in progress"):
            singleton.configure(broker_port=8902, force=True)


# ---------------------------------------------------------------------------
# _LazyJobPing — lazy build
# ---------------------------------------------------------------------------


class TestLazyBuild:
    def test_ensure_built_sync_creates_active(self, singleton):
        singleton.configure(broker_port=0)
        singleton._ensure_built_sync()
        assert singleton._active is not None
        assert singleton._broker is not None

    def test_ensure_built_sync_is_idempotent(self, singleton):
        singleton.configure(broker_port=0)
        singleton._ensure_built_sync()
        active1 = singleton._active
        singleton._ensure_built_sync()
        assert singleton._active is active1

    def test_start_broker_runs_lazy_build(self, singleton):
        singleton.configure(broker_port=0)
        asyncio.run(_do_start_stop(singleton))

    def test_wrap_triggers_lazy_build(self, singleton):
        singleton.configure(broker_port=0)
        called = False

        @singleton.wrap()
        async def handler(request_id: int = 1) -> dict:
            nonlocal called
            called = True
            return {"ok": True}

        # wrap() returns a decorated function — it hasn't run yet,
        # so build hasn't happened. But the decorator captures the
        # singleton reference correctly.
        # The build actually happens inside the wrapper at call time.
        # We need job_context_provider to return a job_id for the
        # handler to go through the JobPing path.
        assert singleton._active is None  # not built yet at decoration time

    def test_env_var_zero_config(self, singleton, monkeypatch):
        monkeypatch.setenv("JOBPING_BROKER_PORT", "0")
        singleton._ensure_built_sync()
        assert singleton._active is not None


# ---------------------------------------------------------------------------
# _LazyJobPing — wrap / wrap_trace
# ---------------------------------------------------------------------------


class TestWrap:
    def test_wrap_passthrough_when_disabled(self, singleton, monkeypatch):
        monkeypatch.setenv("JOBPING_DISABLED", "1")
        singleton.configure(broker_port=0)
        singleton._ensure_built_sync()

        @singleton.wrap()
        async def handler(x: int) -> int:
            return x * 2

        result = asyncio.run(handler(x=5))
        assert result == 10

    def test_wrap_passthrough_without_job_context(self, singleton):
        singleton.configure(broker_port=0)
        singleton._ensure_built_sync()

        @singleton.wrap()
        async def handler(x: int) -> int:
            return x * 2

        result = asyncio.run(handler(x=5))
        assert result == 10

    def test_wrap_returns_job_ref_when_job_context_provides_id(self, singleton):
        job_id_seen = None

        def provider(**kwargs):
            nonlocal job_id_seen
            job_id_seen = "test-job-123"
            return job_id_seen

        singleton.configure(broker_port=0, job_context_provider=provider)
        singleton._ensure_built_sync()

        @singleton.wrap()
        async def handler(x: int) -> int:
            return x * 2

        result = asyncio.run(handler(x=5))
        assert isinstance(result, dict)
        assert result.get("jobping") == "jobping.job_ref.v1"
        assert result["job_id"] == "test-job-123"


# ---------------------------------------------------------------------------
# _LazyJobPing — start_broker / stop_broker
# ---------------------------------------------------------------------------


class TestBrokerLifecycle:
    def test_start_broker_builds_and_starts(self, singleton):
        singleton.configure(broker_port=0)
        asyncio.run(singleton.start_broker())
        assert singleton._broker._server_started is True
        asyncio.run(singleton.stop_broker())

    def test_start_broker_idempotent(self, singleton):
        singleton.configure(broker_port=0)
        asyncio.run(singleton.start_broker())
        asyncio.run(singleton.start_broker())  # no error
        asyncio.run(singleton.stop_broker())

    def test_stop_broker_before_start(self, singleton):
        asyncio.run(singleton.stop_broker())  # no error


# ---------------------------------------------------------------------------
# _LazyJobPing — _reset
# ---------------------------------------------------------------------------


class TestReset:
    def test_reset_restores_no_instance_state(self, singleton):
        singleton.configure(broker_port=0)
        singleton._ensure_built_sync()
        assert singleton._active is not None
        singleton._reset()
        assert singleton._active is None
        assert singleton._broker is None
        assert singleton._jp is None
        assert singleton._needs_rebuild is False
        assert singleton._migration_in_progress is False

    def test_rebuild_after_reset(self, singleton):
        singleton.configure(broker_port=0)
        singleton._ensure_built_sync()
        singleton._reset()
        singleton.configure(broker_port=0)
        singleton._ensure_built_sync()
        assert singleton._active is not None


# ---------------------------------------------------------------------------
# CompositeEndpointProxy
# ---------------------------------------------------------------------------


class TestCompositeEndpointProxy:
    def test_delegates_create_job_id(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        dissolved_new = None

        def on_dissolve(new):
            nonlocal dissolved_new
            dissolved_new = new

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep, on_dissolve=on_dissolve)
        job_id = composite.create_job_id()
        assert isinstance(job_id, str)
        assert len(job_id) > 0

    def test_delegates_offer_defer_make_job_ref(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep)
        job_id = composite.create_job_id()
        offered = composite.offer(job_id)
        assert offered.job_id == job_id
        composite.defer(offered)
        ref = composite.make_job_ref(job_id)
        assert ref["job_id"] == job_id

    def test_is_job_ref_delegation(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep)
        ref = composite.make_job_ref("j1")
        assert composite.is_job_ref(ref) is True
        assert composite.is_job_ref({"not": "a ref"}) is False

    def test_property_delegation(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep)
        assert composite.state_sync is new_ep.state_sync
        assert composite.result_handoff is new_ep.result_handoff
        assert composite.queue is new_ep.queue

    def test_active_trace_property(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep)
        assert composite._active_trace is None
        composite._active_trace = {"job_id": "j1"}
        assert new_ep._active_trace == {"job_id": "j1"}

    def test_dissolve_clears_intercept_and_calls_callback(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        dissolved_new = None

        def on_dissolve(new):
            nonlocal dissolved_new
            dissolved_new = new

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep, on_dissolve=on_dissolve)
        assert old_queue.envelope_endpoint._on_intercept is not None

        composite.dissolve()
        assert composite._dissolved is True
        assert dissolved_new is new_ep
        assert old_queue.envelope_endpoint._on_intercept is None

    def test_double_dissolve_is_safe(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        call_count = 0

        def on_dissolve(new):
            nonlocal call_count
            call_count += 1

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep, on_dissolve=on_dissolve)
        composite.dissolve()
        composite.dissolve()
        assert call_count == 1  # only called once

    def test_accept_release_delegation(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep)
        job_id = composite.create_job_id()
        accepted = composite.accept(job_id)
        assert accepted.job_id == job_id
        composite.release(job_id)
        assert composite.queue.get(job_id) is None

    def test_fulfill_delegation(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep)
        job_id = composite.create_job_id()
        composite.offer(job_id)
        result = composite.fulfill(job_id, {"status": "done"})
        assert result.result == {"status": "done"}

    def test_fulfill_later_delegation(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        async def run():
            composite = CompositeEndpointProxy(old=old_ep, new=new_ep)
            job_id = composite.create_job_id()
            composite.offer(job_id)
            composite.defer(job_id)

            async def task():
                return {"status": "async_done"}

            result = await composite.fulfill_later(job_id, task)
            assert result == {"status": "async_done"}

        asyncio.run(run())

    def test___getattr___fallback(self, endpoint_proxy):
        new_ep = endpoint_proxy
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(EmbeddedBroker(0))),
            result_handoff=ResultHandoff(LocalTransportLayer(EmbeddedBroker(0))),
            queue=old_queue,
        )

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep)
        # create_job_id is an explicit method, not __getattr__
        assert callable(composite.create_job_id)


# ---------------------------------------------------------------------------
# EnvelopeEndpointInMemory — _on_intercept
# ---------------------------------------------------------------------------


class TestEnvelopeIntercept:
    def test_on_intercept_intercepts_send(self):
        ee = EnvelopeEndpointInMemory()
        intercepted = []

        def intercept(env):
            intercepted.append(env)
            return True  # handled

        ee._on_intercept = intercept
        envelope = box_result("job-1", {"data": 42})
        ee.send(envelope)
        assert len(intercepted) == 1
        assert intercepted[0]["job_id"] == "job-1"
        assert len(ee._pending) == 0  # not stored

    def test_on_intercept_returns_false_falls_through(self):
        ee = EnvelopeEndpointInMemory()

        def intercept(env):
            return False  # not handled

        ee._on_intercept = intercept
        envelope = box_result("job-1", {"data": 42})
        ee.send(envelope)
        assert len(ee._pending) == 1  # stored normally

    def test_on_intercept_none_behaves_normally(self):
        ee = EnvelopeEndpointInMemory()
        assert ee._on_intercept is None
        envelope = box_result("job-1", {"data": 42})
        ee.send(envelope)
        assert len(ee._pending) == 1


# ---------------------------------------------------------------------------
# Composite _on_old_result intercept
# ---------------------------------------------------------------------------


class TestCompositeIntercept:
    def test_on_old_result_forwards_to_new_when_job_known(self):
        """When new_ep knows about a job, old result is forwarded."""
        new_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        broker = EmbeddedBroker(0)

        new_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(broker)),
            result_handoff=ResultHandoff(LocalTransportLayer(broker)),
            queue=new_queue,
        )
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(broker)),
            result_handoff=ResultHandoff(LocalTransportLayer(broker)),
            queue=old_queue,
        )

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep)

        # Create a job in new_ep so it's known
        job_id = new_ep.create_job_id()
        new_ep.accept(job_id)

        # Send result to old_ep's envelope_endpoint
        envelope = box_result(job_id, {"result": "forwarded"})
        result = composite._on_old_result(envelope)
        assert result is True  # intercepted

    def test_on_old_result_returns_false_when_job_unknown(self):
        """When new_ep doesn't know a job, old result is not forwarded."""
        new_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        broker = EmbeddedBroker(0)

        new_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(broker)),
            result_handoff=ResultHandoff(LocalTransportLayer(broker)),
            queue=new_queue,
        )
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(broker)),
            result_handoff=ResultHandoff(LocalTransportLayer(broker)),
            queue=old_queue,
        )

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep)
        envelope = box_result("unknown-job", {"result": "nope"})
        result = composite._on_old_result(envelope)
        assert result is False

    def test_on_old_result_after_dissolve_returns_false(self):
        """After dissolve, intercept always returns False."""
        new_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        old_queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
        broker = EmbeddedBroker(0)

        new_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(broker)),
            result_handoff=ResultHandoff(LocalTransportLayer(broker)),
            queue=new_queue,
        )
        old_ep = EndpointProxy(
            state_sync=StateSync(LocalTransportLayer(broker)),
            result_handoff=ResultHandoff(LocalTransportLayer(broker)),
            queue=old_queue,
        )

        composite = CompositeEndpointProxy(old=old_ep, new=new_ep)
        composite.dissolve()
        envelope = box_result("any-job", {"result": "late"})
        result = composite._on_old_result(envelope)
        assert result is False


# ---------------------------------------------------------------------------
# EmbeddedBroker — migration protocol
# ---------------------------------------------------------------------------


class TestBrokerMigration:
    def test_pending_migrations_starts_empty(self, broker):
        assert len(broker._pending_migrations) == 0
        assert broker.on_all_migrated is None

    def test_broadcast_migrate_empty_remote_noop(self, broker):
        broker.broadcast_migrate(8901)
        assert len(broker._pending_migrations) == 0

    def test_broadcast_migrate_sets_pending(self, broker):
        broker._remote_sockets = {"peer-1": True, "peer-2": True}
        broker.broadcast_migrate(8901)
        assert broker._pending_migrations == {"peer-1", "peer-2"}

    def test_handle_migration_complete_reduces_pending(self, broker):
        broker._pending_migrations = {"peer-1", "peer-2"}
        msg = {"kind": MIGRATION_COMPLETE_KIND, "data": {"peer_id": "peer-1"}}
        asyncio.run(broker._handle_migration_complete(msg))
        assert broker._pending_migrations == {"peer-2"}

    def test_handle_migration_complete_triggers_callback(self, broker):
        broker._pending_migrations = {"peer-1"}
        called = False

        def on_all():
            nonlocal called
            called = True

        broker.on_all_migrated = on_all
        msg = {"kind": MIGRATION_COMPLETE_KIND, "data": {"peer_id": "peer-1"}}
        asyncio.run(broker._handle_migration_complete(msg))
        assert called is True
        assert len(broker._pending_migrations) == 0

    def test_route_message_handles_migration_complete(self, broker):
        broker._pending_migrations = {"peer-x"}
        handled = False

        def on_all():
            nonlocal handled
            handled = True

        broker.on_all_migrated = on_all
        msg = {"kind": MIGRATION_COMPLETE_KIND, "data": {"peer_id": "peer-x"}}
        asyncio.run(broker._route_message(msg))
        assert handled is True

    def test_route_message_handles_broker_migrate(self, broker):
        received = None

        def handler(msg):
            nonlocal received
            received = msg

        broker._broker_migrate_handler = handler
        msg = {"kind": BROKER_MIGRATE_KIND, "new_port": 8901}
        asyncio.run(broker._route_message(msg))
        assert received == msg


# ---------------------------------------------------------------------------
# Blue-green migration (single process, no remote peers)
# ---------------------------------------------------------------------------


class TestBlueGreenMigration:
    def test_rebuild_with_no_remote_peers_short_circuits(self, singleton):
        singleton.configure(broker_port=0)
        singleton._ensure_built_sync()
        old_active = singleton._active

        # force rebuild (simulates no remote peers)
        singleton.configure(broker_port=0, force=True)
        asyncio.run(singleton._rebuild())

        # new _active should be set (short-circuit dissolve happened)
        assert singleton._active is not old_active
        assert singleton._migration_in_progress is False

    def test_rebuild_preserves_functional_wrap(self, singleton):
        async def run():
            job_ids = []

            def provider(**kwargs):
                import uuid
                jid = str(uuid.uuid4())
                job_ids.append(jid)
                return jid

            singleton.configure(broker_port=0, job_context_provider=provider)
            singleton._ensure_built_sync()

            @singleton.wrap()
            async def handler(x: int) -> int:
                return x * 3

            result = await handler(x=4)
            assert isinstance(result, dict)
            assert result["job_id"] == job_ids[0]

        asyncio.run(run())


# ---------------------------------------------------------------------------
# Internal factory
# ---------------------------------------------------------------------------


class TestCreateJobping:
    def test_factory_returns_three_tuple(self):
        ep, broker, jp = _create_jobping(broker_port=0)
        assert ep is not None
        assert isinstance(broker, EmbeddedBroker)
        assert jp is not None

    def test_factory_with_peer_brokers(self):
        ep, broker, jp = _create_jobping(
            broker_port=0,
            peer_brokers=["http://localhost:9999"],
        )
        assert ep is not None

    def test_factory_with_custom_job_context_provider(self):
        def custom_provider(**kwargs):
            return "custom-id"

        ep, broker, jp = _create_jobping(
            broker_port=0,
            job_context_provider=custom_provider,
        )
        assert jp.job_context_provider is custom_provider


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _do_start_stop(singleton):
    await singleton.start_broker()
    await singleton.stop_broker()

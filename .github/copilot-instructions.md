# Copilot instructions for JobPing

## Repository status

JobPing is intended to be a tiny state-synchronization bridge between async endpoints, with Python-server and web-client packages as the first integration targets. It should stay focused on `JPItem` status synchronization only: no queue system, no workers, and no background job orchestration.

This checkout currently contains no source files, package manifests, build configuration, tests, or project documentation beyond this instruction file.

## Build, test, and lint commands

No build, test, or lint commands are currently defined in the repository. If project files are added later, update this section with the canonical commands and include how to run a single test.

## Architecture

The intended architecture is a minimal WebSocket-based proxy bridge for synchronizing `JPItem` state between async endpoints. A task has only three JobPing-visible fields: `task_id`, `status`, and status-detail `payload`. Treat `status` as the current node of a state machine: provide built-in example state machines for common task shapes, allow users to define custom states, terminal states, and legal transitions for complex tasks, and validate transitions by default with an opt-out for non-invasive integrations.

`task_id` is generated automatically when the initiating side creates a `JPItem`. Waited `JPItem`s enter the web-side proxy queue; when a request carrying a `JPItem` reaches the server, the server-side JobPing proxy queues it too. Server task callbacks publish state changes, and the bridge notifies the web proxy so the waiting caller receives the updated `JPItem`.

Non-terminal states may be re-waited with the same `task_id` for progress updates. JobPing should clean up only when the caller destroys the task ID, the server reports a terminal complete/destroyed state, or proxy disconnection policy invalidates the relationship. Application task failure without server notification is producer-owned; remote JP proxy disconnection is JobPing-owned.

Use JobPing when the valuable improvement is moving necessary blocking from a network/application-server connection to a local wait point. Do not use it for workloads that are inherently continuous streams, such as SSE-style flows or protocols that require a persistent application-level connection.

`ResultEnvelope` exists to transfer result ownership/availability across the bridge without forcing the caller to wait on the original network connection. It should feel NFS-like: callers can explicitly wait for a signal and fetch by ID themselves, or use a helper that fetches/proxies and unboxes the result behind the scenes.

Use concrete acceptance cases to guide API design: many remote file downloads sharing one per-project proxy WebSocket, and many database-write requests that disconnect after submission while local Promise callbacks wait for committed/ongoing status through the JobPing proxy.

Apply a "created means owned" rule: the application side that creates a `JPItem` owns it and has final interpretation authority over its lifecycle and state changes. JobPing should not invent an independent permission system; access decisions belong to the owning application, with only minimal monitoring/operations access exposed by JobPing itself.

Integration should be decorator/wrapper-first and non-invasive. On the Python side, ship a server-side pip package with decorator helpers for existing service entrypoints such as FastAPI handlers, backed by a framework-neutral `asyncio` API. On the JavaScript side, ship a web-side npm package with Promise-based wrappers around existing remote requests so callers can opt into `JPItem` state sync without rewriting business logic or changing result ownership.

The model is symmetric: "server" and "web" are integration roles, not fundamental protocol roles. Python-to-Python, Python-to-Node, Node-to-browser, and Node-to-Node scenarios should remain valid with the same `JPItem`, proxy, and ResultEnvelope concepts.

Design around endpoints only: web can wait for server, server can wait for web, and server can wait for server. Results can be delivered when ready, and request content can also be sent as a placeholder first and filled later; this is endpoint rendezvous, not one-way completion notification.

## Conventions

Keep the project scope narrow: `JPItem` state sync and result handoff only. Prefer simple `asyncio` APIs on the Python side and Promise-based APIs on the web side. Preserve existing service/client interfaces where possible through decorators and wrappers. Decide API shape with TDD-first usage tests before implementing bridge internals or locking protocol details. Do not interpret business-specific task payloads, retry producer work, replace true streams, or introduce durable queues, worker pools, schedulers, or broader job-processing abstractions.

Start development with a FastAPI + Promise control-group sandbox using async sleep to simulate load. Measure elapsed time and active request pressure first with FastAPI middleware counters, then optionally with TCP connection observation. Write ideal post-JobPing usage tests with mocks before implementing internals, then remove scaffolding incrementally while keeping tests green.

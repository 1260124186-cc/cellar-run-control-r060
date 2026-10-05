# Cellar Run Control

Cellar Run Control is a local Go HTTP service for coordinating a small
production cellar. It keeps formula approval, vessel availability, active
fermentation runs, gravity observations, conditioning, completion metrics, and
vessel cleaning in one deterministic process. A local-only outbox turns
formula approval, fermentation start, temperature excursions, and cleaning
completion into pending notifications for the duty room; nothing is sent over
SMS or any external push channel.

This repository is a healthy initialization baseline. It has no intentional
defects and no automated test suite. Tests are intentionally deferred to a
later engineering task; the production workflow checks in this repository
remain available for immediate build and behavior verification.

## Requirements

- Go 1.23 or later
- A writable local data directory
- No database, network service, or third-party module

## Build And Run

Build every package:

```bash
go build ./...
```

Start the service:

```bash
go run ./cmd/fermentctl serve
```

The default address is `127.0.0.1:8088`. The default snapshot directory is
`./data`.

Environment variables:

- `CELLAR_CONTROL_ADDR`: HTTP listen address.
- `CELLAR_CONTROL_DATA_DIR`: directory containing `state.json`.

Example:

```bash
CELLAR_CONTROL_ADDR=127.0.0.1:9099 \
CELLAR_CONTROL_DATA_DIR=/tmp/cellar-control \
go run ./cmd/fermentctl serve
```

## Directory Structure

```text
cmd/fermentctl                 service, local outbox terminal, and workflow checks
internal/clock                 time boundary
internal/config                environment configuration
internal/domain                entities, validation, transitions, metrics
internal/httpapi               HTTP routes, JSON decoding, errors
internal/outboxcli             on-machine read-only outbox terminal
internal/service               use-case orchestration
internal/storage               revisioned snapshot persistence
internal/workflowcheck         real HTTP production checks
```

## Public API

Health:

- `GET /healthz`

Formula workflow:

- `POST /v1/formulas`
- `GET /v1/formulas`
- `GET /v1/formulas/{id}`
- `PUT /v1/formulas/{id}/steps`
- `POST /v1/formulas/{id}/approve`
- `POST /v1/formulas/{id}/retire`

Vessel workflow:

- `POST /v1/vessels`
- `GET /v1/vessels`
- `GET /v1/vessels/{id}`
- `POST /v1/vessels/{id}/finish-cleaning`
- `POST /v1/vessels/{id}/retire`

Run workflow:

- `POST /v1/runs`
- `GET /v1/runs`
- `GET /v1/runs/{id}`
- `POST /v1/runs/{id}/reserve`
- `POST /v1/runs/{id}/start`
- `POST /v1/runs/{id}/observations`
- `POST /v1/runs/{id}/conditioning`
- `POST /v1/runs/{id}/complete`
- `POST /v1/runs/{id}/abort`

Local outbox:

- `GET /v1/outbox/messages` (optional filters: `state`, `recipient`, `type`)
- `GET /v1/outbox/messages/{id}`
- `POST /v1/outbox/messages/{id}/claim`
- `POST /v1/outbox/messages/{id}/complete` (`X-Claim-Token` header)
- `POST /v1/outbox/messages/{id}/fail` (`X-Claim-Token` header)

Business actions produce outbox messages in the same atomic snapshot revision:

- formula approval -> `formula_approved` for the `process_planner`;
- fermentation start -> `fermentation_started` for the `cellar_operator`;
- an observation above the formula's maximum fermentation temperature ->
  `temperature_excursion` (high priority) for the `cellar_operator`, linked to
  the run and vessel;
- finishing vessel cleaning -> `cleaning_finished` for the `cellar_operator`.

Message states are `pending`, `claimed`, and `handled`. A terminal claims a
message with an operator name and an optional `claim_seconds` (30-900, default
120); the response carries an opaque claim token. Only that terminal can
complete or fail the claim, using the `X-Claim-Token` header. Completing marks
the fact handled and it is never delivered again. Failing records the reason
and returns the message to `pending` so another terminal can claim it.
Duplicate claims return `message_claim_conflict`; a replay by the same holder
returns the same token. A claim is a hard lease: once `claim_seconds` elapses,
the holder's token is rejected and a background sweep (every 15 seconds)
returns the message to `pending`, where any terminal can claim it.

The same information is available from the on-machine terminal:

```bash
go run ./cmd/fermentctl outbox list [-state pending|claimed|handled] \
  [-recipient cellar_operator] [-type temperature_excursion]
go run ./cmd/fermentctl outbox show -data-dir ./data <message-id>
```

Writes accept an optional `X-Expected-Version` header. Errors use a stable
shape:

```json
{
  "error": {
    "code": "revision_conflict",
    "detail": "the aggregate version does not match X-Expected-Version",
    "field": ""
  }
}
```

## Workflow Checks

Run each production workflow check separately:

```bash
go run ./cmd/fermentctl check formula-approval
go run ./cmd/fermentctl check vessel-run-lifecycle
go run ./cmd/fermentctl check observations-completion
go run ./cmd/fermentctl check local-outbox
```

Each check creates a temporary snapshot directory, starts the real HTTP
handler on a loopback listener, issues JSON requests, verifies success and
failure behavior, and removes its temporary data.

## Persistence And Recovery

The service stores a complete revisioned snapshot at
`<data-directory>/state.json`. A write creates a temporary file in the same
directory, commits the encoded bytes durably, closes the handle, and renames
the result over the previous snapshot. The in-memory snapshot is replaced only
after that operation succeeds.

On startup, an existing snapshot is decoded and normalized. Unknown or
conflicting map indexes are not silently invented; invalid JSON or a negative
revision stops the process before HTTP serving begins.

## Local Outbox Durability

The outbox is a transactional outbox with no external transport. Each message
is written inside the same atomic snapshot revision as the business action
that creates it: if the business write fails, no message exists, and once the
revision is durable the message survives restarts. A dedup index keyed by the
business fact (for example `temperature_excursion:<run>:<sequence>`) means a
confirmed fact is never published twice, even if the action is replayed.

A claim is a short local lease, not durable ownership. When the process starts,
every message left in `claimed` state by a previous run is requeued to
`pending` (the change is persisted as a new revision), so a restart during
handling never loses a message and never leaves it stranded on a dead
terminal. Failure marks return the message to `pending` with the failure
recorded; handled messages are terminal and cannot be claimed again.

## Repair-Ready Boundaries

The baseline intentionally keeps several independent behavior surfaces visible
for later engineering evaluation:

- formula name normalization and duplicate detection;
- target gravity and process-step range validation;
- aggregate version conflict checks;
- atomic vessel reservation and release;
- run state transition guards;
- observation sequence and time ordering;
- gravity movement and range checks;
- metric calculation and rounding;
- durable snapshot replacement and startup normalization.

These are production mechanisms, not injected defects. A later task can add
automated tests or request a focused repair without changing the public API.

## Test Boundary

This stage does not create unit tests, integration tests, test fixtures, or an
E2E suite. The later test task owns systematic coverage of validation limits,
state transitions, concurrency, persistence failures, and numeric edge cases.

## Scope

The service does not control physical equipment, authenticate remote users,
send SMS, email, webhook, or other external push notifications, replicate data
across processes, or expose a browser interface. The local outbox and its
on-machine terminal stay entirely on this host. It is intended for a trusted
local network or an internal adapter.

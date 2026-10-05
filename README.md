# Cellar Run Control

Cellar Run Control is a local Go HTTP service for coordinating a small
production cellar. It keeps formula approval, vessel availability, active
fermentation runs, gravity observations, conditioning, completion metrics,
vessel cleaning, and a machine-local duty outbox in one deterministic
process.

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
cmd/fermentctl                 service and workflow-check executable
internal/clock                 time boundary
internal/config                environment configuration
internal/domain                entities, validation, transitions, metrics
internal/httpapi               HTTP routes, JSON decoding, errors
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

Local duty outbox (no SMS or external push; on-machine only):

- `GET /v1/outbox/messages` — optional filters `state`, `recipient`, `type`, `run_id`
- `GET /v1/outbox/messages/{id}`
- `POST /v1/outbox/claims` — claim the oldest eligible message (`terminal_id`, optional `recipient`)
- `POST /v1/outbox/messages/{id}/claim` — claim one chosen message
- `POST /v1/outbox/messages/{id}/complete` — complete with the exclusive `claim_token`
- `POST /v1/outbox/messages/{id}/release` — return a claim to the pending queue

A local duty room page is served at `GET /duty/`. It uses only the service's
own JSON API and contains no external scripts, styles, or network calls.

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
go run ./cmd/fermentctl check outbox-duty-room
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

## Local Duty Outbox

Business actions write a notification into the same atomic snapshot revision
as the fact they describe, so a message is never committed without its fact or
vice versa. The duty room sees four message types:

- `formula_approved` — addressed to `duty-planner`, no run link;
- `run_fermentation_started` — addressed to `duty-operator`, linked to the run;
- `temperature_breach` — addressed to `duty-observer` when an observation
  temperature exceeds the approved formula's maximum;
- `vessel_cleaning_finished` — addressed to `duty-operator`, linked to the run.

State-machine guards plus a dedup index mean a confirmed fact is never
published twice (a rejected re-approval or repeated transition emits nothing).

A message moves `pending -> claimed -> handled`. A claim is an exclusive
five-minute lease identified by a random token returned only to the claiming
terminal. Listing and reading messages never reveal the token. While the lease
is live a competing terminal cannot claim or complete the message, a forged
token is rejected, and `handled` is terminal. If the holder crashes or the
process restarts, the claim is still present in `state.json`; the original
terminal may finish within its lease, or once the lease expires any terminal
can reclaim it, which rotates the token so the old holder can no longer act.
A failed completion never removes or duplicates an unfinished message.

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
send external notifications (SMS, email, webhook, or push), or replicate data
across processes. The duty outbox and its `/duty/` page are strictly
on-machine. It is intended for a trusted local network or an internal adapter.

# IAM Policy Stream

Every request evaluated by IAM enforcement is recorded in a bounded in-memory ring of 2000 events. Each event holds: timestamp, request ID, service, operation, IAM action, resource ARN, principal ARN, decision, deciding reason, matched statement `Sid` and indices, condition keys involved, and, for implicit denies, the minimal Allow statement that would have permitted the request.

## Modes

| Flag | Env var | Effect |
|------|---------|--------|
| `--enforce-iam` | `GOPHERSTACK_ENFORCE_IAM`, `ENFORCE_IAM` | Evaluate, record and deny |
| `--iam-audit` | `GOPHERSTACK_IAM_AUDIT`, `IAM_SOFT_MODE` | Evaluate and record, never deny. Denials are logged as `IAM audit: request would be denied`. Works without `--enforce-iam` |
| `--iam-enforce-scps` | `GOPHERSTACK_IAM_ENFORCE_SCPS` | Default `true`. With enforcement on, also evaluate Organizations SCPs for member-account principals. The management account and service-linked roles are exempt |

Deny log lines carry `reason` and `matchedSid`. Reasons: `identity-policy`, `resource-policy`, `permission-boundary`, `service-control-policy`, `resource-control-policy`, `implicit-deny`, `unresolved-principal`.

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /_gopherstack/iam/policy-stream` | JSON `{events, dropped}`. Filters: `decision=allow\|deny`, `service`, `principal`, `since` (RFC3339 or a duration such as `5m`), `n` |
| `GET /_gopherstack/iam/policy-stream?follow=1` | History, then live events as NDJSON, or SSE with `Accept: text/event-stream`. Ends on client disconnect |
| `GET /_gopherstack/iam/policy-stream/suggest` | Least-privilege Allow policy aggregated from the filtered events (denied only; `include=all` adds allowed). Explicit-Deny and boundary blocks are listed in `blockedByExplicitDeny` |
| `DELETE /_gopherstack/iam/policy-stream` | Clear the ring |

## Event details

- `denyType`: `explicit` or `implicit`.
- `policy`: the deciding policy. `type` is `managed` (with ARN), `inline` (name and owner such as `user/alice`), `resource` (resource ARN), `permission-boundary` (ARN), `identity`, `service-control-policy` or `resource-control-policy`.
- `boundary`, when the caller has a permissions boundary: `arn`, `sid`, `statementIndex`, `matched`. `matched: false` means no boundary statement applied, so the boundary caused an implicit deny.

The dashboard view is `/dashboard/policystream`; it consumes `?follow=1` over SSE.

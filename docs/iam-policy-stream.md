# IAM Policy Stream

Every request evaluated by IAM enforcement is recorded in a bounded in-memory
ring (2000 events): timestamp, request id, service, operation, IAM action,
resource ARN, principal ARN, decision, deciding reason, matched statement
`Sid`/indices, the condition keys involved, and, for implicit denies, the minimal
Allow statement that would have permitted the request.

## Modes

- `--enforce-iam` (`GOPHERSTACK_ENFORCE_IAM`): evaluate, record and deny.
- `--iam-audit` (`GOPHERSTACK_IAM_AUDIT` or `IAM_SOFT_MODE`): evaluate and record
  but never deny; denials are logged as `IAM audit: request would be denied`.
  Works without `--enforce-iam`.

Deny log lines carry `reason` (`identity-policy`, `resource-policy`,
`permission-boundary`, `implicit-deny`, `unresolved-principal`) and `matchedSid`.

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /_gopherstack/iam/policy-stream` | JSON `{events, dropped}`. Filters: `decision=allow\|deny`, `service`, `principal`, `since` (RFC3339 or duration such as `5m`), `n`. |
| `GET /_gopherstack/iam/policy-stream?follow=1` | History then live events as NDJSON, or SSE with `Accept: text/event-stream`. Ends on client disconnect. |
| `GET /_gopherstack/iam/policy-stream/suggest` | Least-privilege Allow policy aggregated from the filtered events (denied only; `include=all` adds allowed). Explicit-Deny and boundary blocks are listed in `blockedByExplicitDeny`. |
| `DELETE /_gopherstack/iam/policy-stream` | Clear the ring. |

Limits: matched statements are identified by policy and statement index plus
`Sid`; policy ARNs are not tracked. Boundary denials report no `Sid`.

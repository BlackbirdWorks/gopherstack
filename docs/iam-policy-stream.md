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
- `--iam-enforce-scps` (`GOPHERSTACK_IAM_ENFORCE_SCPS`, default true): with enforcement on,
  also evaluate Organizations SCPs for member-account principals (management account and
  service-linked roles exempt).

Deny log lines carry `reason` (`identity-policy`, `resource-policy`,
`permission-boundary`, `service-control-policy`, `implicit-deny`, `unresolved-principal`) and `matchedSid`.

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /_gopherstack/iam/policy-stream` | JSON `{events, dropped}`. Filters: `decision=allow\|deny`, `service`, `principal`, `since` (RFC3339 or duration such as `5m`), `n`. |
| `GET /_gopherstack/iam/policy-stream?follow=1` | History then live events as NDJSON, or SSE with `Accept: text/event-stream`. Ends on client disconnect. |
| `GET /_gopherstack/iam/policy-stream/suggest` | Least-privilege Allow policy aggregated from the filtered events (denied only; `include=all` adds allowed). Explicit-Deny and boundary blocks are listed in `blockedByExplicitDeny`. |
| `DELETE /_gopherstack/iam/policy-stream` | Clear the ring. |

Each event also carries `denyType` (`explicit`/`implicit`), `policy` (the deciding
policy: `managed` with ARN, `inline` with name and owner such as `user/alice`,
`resource` with the resource ARN, or `permission-boundary` with ARN) and, when
the caller has a permissions boundary, `boundary` (`arn`, `sid`, `statementIndex`,
`matched`; `matched: false` means no boundary statement applied, so the boundary
caused an implicit deny). SCPs are not modeled. The dashboard view is
`/dashboard/policystream` and consumes `?follow=1` over SSE.

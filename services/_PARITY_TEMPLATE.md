---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: <name>
sdk_module: aws-sdk-go-v2/service/<name>@<version>   # version audited against
last_audit_commit: <short-sha>                       # HEAD when this manifest was written
last_audit_date: <YYYY-MM-DD>
overall: <A|B>            # A = ~1k genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  <OpName>: {wire: ok, errors: ok, state: ok, persist: ok, note: <optional>}
# Families audited as a group (when per-op is impractical):
families:
  <family>: {status: ok, note: <what was verified / what changed>}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - <description> (bd: gopherstack-xxx)
structural_gaps:          # divergences that CAN'T be fixed, ever: the underlying data source
                           # cannot exist in an emulator (no real traffic, no ML/AI engine, no
                           # billing/settlement system, no physical hardware). Does NOT block an
                           # A grade — but must be recorded here with justification, not left in
                           # gaps. NOT an escape hatch: if more implementation effort COULD
                           # produce real data, however hard, it stays in gaps and still blocks
                           # nothing — only relabel here when the data source itself is impossible.
  - <description + why no data source can ever exist> (bd: gopherstack-xxx)
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - <family/op>
leaks: {status: clean|found, note: <goroutine/janitor/map/ctx findings>}
---

## Notes
Freeform: AWS-behavior specifics worth remembering (exact algorithms, wire quirks,
error-message text, protocol = query-XML / REST-XML / REST-JSON / json-1.0), and any
"looks-wrong-but-correct" traps so the next auditor doesn't re-flag them.

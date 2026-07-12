---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: bedrockagent
sdk_module: aws-sdk-go-v2/service/bedrockagent@v1.54.0   # version audited against
last_audit_commit: 05e127fa13a618837560e0b6a56098937fc1cae4
last_audit_date: 2026-07-12
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgents: {wire: ok, errors: ok, state: ok, persist: ok}
  PrepareAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentVersions: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was unreachable: POST to the collection path (real wire method for
    ListAgentVersions) was misrouted to a fictional CreateAgentVersion op instead
    of List — fixed this sweep, see Notes"}
  GetAgentVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAgentVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAgentActionGroup: {wire: partial, errors: ok, state: ok, persist: ok,
    note: "ignores the path agentVersion, always stores under DRAFT — see gaps"}
  GetAgentActionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentActionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAgentActionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentActionGroups: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was unreachable: POST (real wire method) was misrouted to Create — fixed"}
  CreateAgentAlias: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "now auto-creates a numbered agent version when routingConfiguration is
    empty, matching real AWS (see Notes) — was previously a silent no-op that
    stored an alias routed at nothing"}
  GetAgentAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAgentAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentAliases: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was misrouted: POST (real wire method) hit Create instead of List — fixed"}
  AssociateAgentCollaborator: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAgentCollaborator: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentCollaborator: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateAgentCollaborator: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentCollaborators: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was totally unreachable: POST (real wire method) had no case at all and
    404'd — fixed"}
  CreateKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  GetKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  ListKnowledgeBases: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateAgentKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAgentKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateAgentKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentKnowledgeBases: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was totally unreachable: POST (real wire method) had no case at all and
    404'd — fixed"}
  CreateDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataSources: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was misrouted: POST (real wire method) hit Create instead of List — fixed"}
  StartIngestionJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIngestionJob: {wire: ok, errors: ok, state: ok, persist: ok}
  StopIngestionJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIngestionJobs: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was misrouted: POST (real wire method) hit Start instead of List — fixed"}
  CreateFlow: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "Status enum was SCREAMING_SNAKE_CASE (NOT_PREPARED); real FlowStatus wire
    values are Pascal-case (NotPrepared/Preparing/Prepared/Failed) — fixed"}
  GetFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFlows: {wire: ok, errors: ok, state: ok, persist: ok}
  PrepareFlow: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same FlowStatus casing fix"}
  ValidateFlowDefinition: {wire: ok, errors: ok, state: ok, persist: ok,
    note: "always returns zero validation errors — acceptable permissive-emulator behavior"}
  CreateFlowVersion: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same FlowStatus casing fix"}
  GetFlowVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFlowVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFlowVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFlowAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFlowAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFlowAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFlowAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFlowAliases: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPrompts: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePromptVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPromptVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePromptVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  IngestKnowledgeBaseDocuments: {wire: ok, errors: ok, state: ok, persist: ok}
  GetKnowledgeBaseDocuments: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteKnowledgeBaseDocuments: {wire: ok, errors: ok, state: ok, persist: ok}
  ListKnowledgeBaseDocuments: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to
    InMemoryBackend.Snapshot/Restore, registered via provider.go -> cli.go
    setupPersistence; every store.Table is registered in store_setup.go and
    round-trips (persistence_test.go). No dead-wiring found."}
  timestamps: {status: ok, note: "real bedrockagent restjson1 wire format is
    ISO8601/RFC3339 date-time strings (smithytime.ParseDateTime in the SDK
    deserializer), not epoch-seconds numbers. gopherstack uses time.Time with
    Go's default JSON marshaling (RFC3339Nano), which is wire-compatible. No fix
    needed — do not epoch-ify these per pkgs/awstime; that would be wrong here."}
  error_codes: {status: ok, note: "handleErr maps to ResourceNotFoundException/404,
    ConflictException/409, ValidationException/400, InternalServerException/500 —
    matches the real error catalog (types/errors.go: AccessDeniedException,
    ConflictException, InternalServerException, ResourceNotFoundException,
    ServiceQuotaExceededException, ThrottlingException, ValidationException)."}
gaps:
  - "CreateAgentActionGroup ignores the {agentVersion} path parameter and always
    stores the action group under DRAFT, even if a client (unusually) targets a
    different version in the URL. Real AWS action groups can only be created
    against DRAFT anyway, so this is unlikely to be hit by a real client, but a
    strict client sending a non-DRAFT version would get silently redirected
    instead of a validation error. Not fixed this sweep (low risk, would need a
    new exact-match AWS error string to validate against). (bd: TODO — file
    gopherstack-bedrockagent-actiongroup-version)"
  - "ValidateFlowDefinition always returns zero validation errors regardless of
    the definition passed — acceptable for a permissive emulator (the op still
    reads real state and returns the AWS-accurate empty-array shape); not a
    disguised no-op flag, just an easy target if flow-definition validation
    logic is ever wanted."
deferred:
  - "KBDocument/DataSource nested configuration blobs (dataSourceConfiguration,
    vectorIngestionConfiguration, knowledgeBaseConfiguration,
    storageConfiguration, actionGroupExecutor, apiSchema, functionSchema,
    guardrailConfiguration, memoryConfiguration, promptOverrideConfiguration) are
    passed through as opaque map[string]any/JSON blobs rather than typed +
    validated against the SDK's nested shape unions. This is consistent with how
    this service already treats them; deep-shape validation of these blobs was
    out of scope this sweep."
  - "IngestionJob does not model the `statistics` field AWS returns (documents
    scanned/indexed/failed counts) — every ingestion job here always indexes 0
    documents worth of statistics; not audited for wire-accuracy this sweep."
leaks: {status: clean, note: "InMemoryBackend has no background goroutines,
  timers, or janitors — every operation is synchronous request-scoped state
  mutation guarded by b.mu (lockmetrics-style single coarse sync.RWMutex,
  though not yet migrated to pkgs/lockmetrics.RWMutex — see Notes). No waiter-hang
  risk: PrepareAgent/PrepareFlow/StartIngestionJob all complete synchronously to
  their terminal status (PREPARED/COMPLETE) in the same call, matching the
  existing project pattern of skipping transient states in a synchronous
  emulator."}
---

## Notes

**Route-matcher class bug (the main finding this sweep).** bedrockagent is
restjson1. For every *nested* collection resource under an agent or knowledge
base (agent action groups, agent aliases, agent collaborators, agent-KB
associations, agent versions, data sources, ingestion jobs), the real SDK
reuses the exact same collection path for two different operations,
distinguished only by HTTP method:

| path (collection root)                                          | PUT              | POST                    |
|-------------------------------------------------------------------|------------------|--------------------------|
| `/agents/{id}/agentversions/`                                    | *(no such op)*   | `ListAgentVersions`      |
| `/agents/{id}/agentversions/{v}/actiongroups/`                   | `CreateAgentActionGroup` | `ListAgentActionGroups` |
| `/agents/{id}/agentaliases/`                                     | `CreateAgentAlias` | `ListAgentAliases`      |
| `/agents/{id}/agentversions/{v}/agentcollaborators/`             | `AssociateAgentCollaborator` | `ListAgentCollaborators` |
| `/agents/{id}/agentversions/{v}/knowledgebases/`                 | `AssociateAgentKnowledgeBase` | `ListAgentKnowledgeBases` |
| `/knowledgebases/{id}/datasources/`                               | `CreateDataSource` | `ListDataSources`       |
| `/knowledgebases/{id}/datasources/{d}/ingestionjobs/`             | `StartIngestionJob` | `ListIngestionJobs`     |

Before this sweep, gopherstack's dispatch treated POST as a synonym for the
PUT (create/associate/start) case at every one of these paths instead of
routing it to the real List op. For three families
(agentcollaborators, agent-KB associations, agent versions) POST had **no**
case at all and 404'd; for the other four (action groups, aliases, data
sources, ingestion jobs) POST **silently executed the wrong operation**
(created/started something instead of listing). Any real aws-sdk-go-v2 client
calling e.g. `ListAgentActionGroups` would have gotten either a 404 or an
accidental extra resource creation. Confirmed by reading
`aws-sdk-go-v2/service/bedrockagent/serializers.go`'s
`awsRestjson1_serializeOp*` functions directly (source of truth for
path+method, not assumption) — see `services/bedrockagent/handler.go`'s
`dispatch*` functions and `classify*` functions (both fixed in tandem; the
latter feeds `ExtractOperation`/CloudTrail/chaos, so a stale classify function
would have mislabeled telemetry even after the dispatch fix).

Real clients never send GET to these collection-root paths, but existing
gopherstack tests did, so GET was kept as extra harmless List leniency
alongside the now-correct POST case everywhere.

**CreateAgentVersion is not a real bedrockagent SDK operation.** There is no
`api_op_CreateAgentVersion.go` in aws-sdk-go-v2/service/bedrockagent — grep
the full `ls $(go list -m -f '{{.Dir}}' .../bedrockagent)/api_op_*.go` output
to confirm. gopherstack had invented one and exposed it at
`POST /agents/{agentId}/agentversions`, which collided with the real
`ListAgentVersions` wire path+method (see table above). Removed the
fictional op (`opCreateAgentVersion` const, `handleCreateAgentVersion`,
`GetSupportedOperations()` entry, `classifyAgentVersionPath` case).

Real AWS creates numbered agent versions as a side effect of
`CreateAgentAlias` when called with an empty `routingConfiguration`: Bedrock
auto-creates a new version (a snapshot of DRAFT) and points the alias at it.
(Confirmed via AWS docs, not just SDK comments — the SDK doesn't document
this behavior inline, only in the prose API reference.) Implemented this in
`InMemoryBackend.CreateAgentAlias` (backend.go): when `cfg.RoutingConfiguration`
is empty, it now calls the same internal logic as `CreateAgentVersion`
(refactored into lock-free `newAgentVersionLocked`, since `CreateAgentAlias`
already holds `b.mu`) and routes the new alias at the freshly created version.
`InMemoryBackend.CreateAgentVersion` itself is kept as an exported Go method
(implements `StorageBackend`) for internal/programmatic use — e.g.
`persistence_test.go`'s fixture calls it directly to seed a version — but is
deliberately unreachable from any HTTP route, matching the real API surface.

**FlowStatus is not SCREAMING_SNAKE_CASE.** Every other status enum in this
service (`AgentStatus`, `AgentAliasStatus`, `DataSourceStatus`,
`KnowledgeBaseStatus`, `IngestionJobStatus`) is upper-snake-case
(`NOT_PREPARED`, `IN_PROGRESS`, ...). `FlowStatus` is the one exception: real
wire values are `"Prepared"`, `"Preparing"`, `"NotPrepared"`, `"Failed"`
(Pascal-case, confirmed in `types/enums.go`). gopherstack had
`flowStatusPrepared = "PREPARED"` / `flowStatusNotPrepared = "NOT_PREPARED"`,
which is wrong for `Flow.Status` and `FlowVersion.Status` on the wire. Fixed
the two constants in `backend.go`; **do not "fix" them back** to
upper-snake-case in a future pass — that would reintroduce the bug. This is a
documented "looks-wrong-but-correct-elsewhere, actually-wrong-here" trap:
don't pattern-match FlowStatus against the other four status enums.

**Lock granularity.** `InMemoryBackend` uses a single raw `sync.RWMutex`
(`b.mu`), not `pkgs/lockmetrics.RWMutex`. This predates this sweep and is
functionally correct (coarse lock at the invariant boundary, matching the
`pkgs-catalog.md` locking rule) but doesn't get lockmetrics' per-op
instrumentation. Left as-is — swapping the mutex type across ~40 call sites
for observability-only benefit was judged out of scope for a bug-fix sweep;
flagging for a future pass.

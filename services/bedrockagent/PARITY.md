---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: bedrockagent
sdk_module: aws-sdk-go-v2/service/bedrockagent@v1.54.0   # version audited against
last_audit_commit: 80462cc485cf9dc0f8a8d0df4b22b8c17975ee18
last_audit_date: 2026-07-24
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateAgent: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "Agent had an invented 'tags' field on the wire response — real
    types.Agent has no Tags member (tags are write-only on CreateAgentInput,
    readable only via ListTagsForResource). Removed the field; CreateAgent
    still seeds b.tags[AgentARN] directly. See Notes: invented-tags-field."}
  GetAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAgent: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "cascade-delete gap: only agentVersions and the version counter were
    cleaned up; actionGroups/agentAliases/agentCollaborators/agentKBAssocs and
    the agent's + every alias's tags map entry were left as permanent ghost
    rows. Fixed — see Notes: cascade-delete."}
  ListAgents: {wire: ok, errors: ok, state: ok, persist: ok}
  PrepareAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentVersions: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was unreachable: POST to the collection path (real wire method for
    ListAgentVersions) was misrouted to a fictional CreateAgentVersion op instead
    of List — fixed this sweep, see Notes"}
  GetAgentVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAgentVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAgentActionGroup: {wire: fixed, errors: fixed, state: ok, persist: ok,
    note: "was ignoring the path agentVersion, always storing under DRAFT
    regardless of what the client sent. Real AWS constrains this URI path
    param to the literal pattern `DRAFT` (fixed length 5, per the API
    reference) — a non-DRAFT value must fail ValidationException/400.
    Verified via the AWS API reference (not just SDK source, which only
    encodes shape/required-ness, not path pattern constraints). Fixed:
    validates agentVersion == DRAFT, real ValidationException otherwise."}
  GetAgentActionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentActionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAgentActionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentActionGroups: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was unreachable: POST (real wire method) was misrouted to Create — fixed"}
  CreateAgentAlias: {wire: fixed, errors: ok, state: fixed, persist: ok,
    note: "(prior sweep) now auto-creates a numbered agent version when
    routingConfiguration is empty, matching real AWS (see Notes) — was
    previously a silent no-op that stored an alias routed at nothing.
    (this sweep) removed the invented 'tags' wire field (real
    CreateAgentAliasOutput/AgentAlias has no tags member) and, since that
    field was tags' only storage before, added the missing
    b.tags[AgentAliasArn] seed so ListTagsForResource on a freshly-created
    alias with tags no longer incorrectly returns empty — see Notes:
    invented-tags-field."}
  GetAgentAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentAlias: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "AgentAlias had an invented 'tags' wire field (see CreateAgent's
    note); also real UpdateAgentAliasInput has no tags param at all, so the
    old cfg.Tags-on-update branch was dead code for real clients. Both
    removed."}
  DeleteAgentAlias: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "now also deletes the alias's b.tags[AgentAliasArn] entry — see
    Notes: cascade-delete."}
  ListAgentAliases: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was misrouted: POST (real wire method) hit Create instead of List — fixed"}
  AssociateAgentCollaborator: {wire: ok, errors: fixed, state: ok, persist: ok,
    note: "same DRAFT-only {agentVersion} path constraint as
    CreateAgentActionGroup, confirmed via the API reference — fixed"}
  GetAgentCollaborator: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentCollaborator: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateAgentCollaborator: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentCollaborators: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was totally unreachable: POST (real wire method) had no case at all and
    404'd — fixed"}
  CreateKnowledgeBase: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "invented 'tags' wire field removed — see Notes: invented-tags-field.
    b.tags[KnowledgeBaseArn] seed was already correct, kept as-is."}
  GetKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteKnowledgeBase: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "cascade-delete gap: did not clean up dataSources (nor, transitively,
    ingestionJobs/kbDocuments under each), nor the KB's tags map entry. Fixed
    — see Notes: cascade-delete."}
  ListKnowledgeBases: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateAgentKnowledgeBase: {wire: ok, errors: fixed, state: ok, persist: ok,
    note: "same DRAFT-only {agentVersion} path constraint as
    CreateAgentActionGroup, confirmed via the API reference — fixed"}
  GetAgentKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateAgentKnowledgeBase: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentKnowledgeBases: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was totally unreachable: POST (real wire method) had no case at all and
    404'd — fixed"}
  CreateDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataSource: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "cascade-delete gap: did not clean up ingestionJobs or kbDocuments
    scoped under the data source. Fixed — see Notes: cascade-delete."}
  ListDataSources: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was misrouted: POST (real wire method) hit Create instead of List — fixed"}
  StartIngestionJob: {wire: fixed, errors: ok, state: fixed, persist: ok,
    note: "IngestionJob/IngestionJobSummary never modeled the real
    'statistics' field (numberOfDocumentsScanned/NewDocumentsIndexed/
    ModifiedDocumentsIndexed/DocumentsDeleted/DocumentsFailed/
    MetadataDocumentsScanned/MetadataDocumentsModified — all plain
    PrimitiveLong ints per the SDK deserializer, not epoch timestamps) —
    every job always reported no statistics object at all. Fixed: Statistics
    is now populated from a real backend read (the KnowledgeBaseDocument
    store's actual per-data-source document count via
    IngestKnowledgeBaseDocuments), reported as scanned+newly-indexed. See
    items_still_open for the honest limitation (deleted/failed/modified
    counts stay zero — no prior-job snapshot is tracked to diff against, so
    reporting non-zero there would be fabricated, not read from real state)."}
  GetIngestionJob: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "same Statistics fix as StartIngestionJob"}
  StopIngestionJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIngestionJobs: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was misrouted: POST (real wire method) hit Start instead of List
    — fixed (prior sweep). Summaries now also carry Statistics (this sweep,
    same fix as StartIngestionJob)."}
  CreateFlow: {wire: fixed, errors: ok, state: fixed, persist: ok,
    note: "(prior sweep) Status enum was SCREAMING_SNAKE_CASE (NOT_PREPARED);
    real FlowStatus wire values are Pascal-case
    (NotPrepared/Preparing/Prepared/Failed) — fixed. (this sweep) invented
    'tags' wire field removed — see Notes: invented-tags-field."}
  GetFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFlow: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "invented 'tags' wire field removed; real UpdateFlowInput has no
    tags param either, so the old cfg.Tags-on-update branch was dead code
    for real clients — removed"}
  DeleteFlow: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "cascade-delete gap: did not clean up flowAliases scoped under the
    flow, nor the flow's + every alias's tags map entry (flowVersions
    cleanup was already correct). Fixed — see Notes: cascade-delete."}
  ListFlows: {wire: ok, errors: ok, state: ok, persist: ok}
  PrepareFlow: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same FlowStatus casing fix"}
  ValidateFlowDefinition: {wire: ok, errors: ok, state: ok, persist: ok,
    note: "always returns zero validation errors — acceptable permissive-emulator behavior"}
  CreateFlowVersion: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same FlowStatus casing fix"}
  GetFlowVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFlowVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFlowVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFlowAlias: {wire: fixed, errors: ok, state: fixed, persist: ok,
    note: "invented 'tags' wire field removed (real CreateFlowAliasOutput has
    no tags member); that field was tags' only storage before, so also added
    the missing b.tags[AliasArn] seed — see Notes: invented-tags-field."}
  GetFlowAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFlowAlias: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "invented 'tags' wire field removed; real UpdateFlowAliasInput has
    no tags param, so the old cfg.Tags-on-update branch was dead code for
    real clients — removed"}
  DeleteFlowAlias: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "now also deletes the alias's b.tags[AliasArn] entry — see Notes:
    cascade-delete."}
  ListFlowAliases: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePrompt: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "invented 'tags' wire field removed — see Notes:
    invented-tags-field. b.tags[PromptArn] seed was already correct."}
  GetPrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePrompt: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "invented 'tags' wire field removed; real UpdatePromptInput has no
    tags param, so the old cfg.Tags-on-update branch was dead code for real
    clients — removed"}
  DeletePrompt: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "now also deletes the prompt's b.tags[PromptArn] entry (promptVersions
    cleanup was already correct) — see Notes: cascade-delete."}
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
  - "ValidateFlowDefinition always returns zero validation errors regardless of
    the definition passed — acceptable for a permissive emulator (the op still
    reads real state and returns the AWS-accurate empty-array shape); not a
    disguised no-op flag, just an easy target if flow-definition validation
    logic is ever wanted. Unchanged this sweep."
  - "Real AWS snapshots an agent's action groups, collaborators, and agent-KB
    associations into each numbered agent version at the moment
    CreateAgentAlias auto-creates it (confirmed via GetAgentActionGroup's API
    reference: its {agentVersion} path pattern is
    `(DRAFT|[0-9]{0,4}[1-9][0-9]{0,4})`, i.e. Get/List/Update/Delete accept
    non-DRAFT versions too, unlike Create/Associate which are DRAFT-only).
    gopherstack's newAgentVersionLocked only snapshots the Agent's own
    top-level fields, not these three sub-resource families, so
    GetAgentActionGroup/ListAgentCollaborators/etc. against a real numbered
    version always come back empty instead of a DRAFT-at-creation-time
    snapshot. This is a deeper feature gap (snapshot-forward propagation),
    not a simple bug; not fixed this sweep — found while verifying the new
    DRAFT-only Create/Associate validation below, listed here for the next
    sweep. (bd: TODO — file gopherstack-bedrockagent-version-snapshot)"
deferred:
  - "KBDocument/DataSource nested configuration blobs (dataSourceConfiguration,
    vectorIngestionConfiguration, knowledgeBaseConfiguration,
    storageConfiguration, actionGroupExecutor, apiSchema, functionSchema,
    guardrailConfiguration, memoryConfiguration, promptOverrideConfiguration) are
    passed through as opaque map[string]any/JSON blobs rather than typed +
    validated against the SDK's nested shape unions. This is consistent with how
    this service already treats them; deep-shape validation of these blobs was
    out of scope this sweep (unchanged)."
  - "IngestionJobStatistics' NumberOfDocumentsDeleted/NumberOfDocumentsFailed/
    NumberOfModifiedDocumentsIndexed/NumberOfMetadataDocumentsScanned/
    NumberOfMetadataDocumentsModified stay zero always (fixed this sweep:
    NumberOfDocumentsScanned/NumberOfNewDocumentsIndexed now reflect the real
    per-data-source document count). Reporting non-zero on the other five
    would require tracking a prior-job document snapshot to diff against,
    which this backend does not do — reporting a number there would be
    fabricated, not read from real state, so left at zero rather than
    invented. Also unmodeled: data sources backed by a real crawler type
    (S3/web/Confluence/etc.) have no actual external content for this
    emulator to scan, so their statistics are always zero regardless of the
    dataSourceConfiguration blob — only documents pushed via the separate
    IngestKnowledgeBaseDocuments custom-content API are counted."
leaks: {status: clean, note: "InMemoryBackend has no background goroutines,
  timers, or janitors — every operation is synchronous request-scoped state
  mutation guarded by b.mu (lockmetrics-style single coarse sync.RWMutex,
  though not yet migrated to pkgs/lockmetrics.RWMutex — see Notes). No waiter-hang
  risk: PrepareAgent/PrepareFlow/StartIngestionJob all complete synchronously to
  their terminal status (PREPARED/COMPLETE) in the same call, matching the
  existing project pattern of skipping transient states in a synchronous
  emulator. Ghost-row cascade-delete gaps fixed this sweep — see Notes:
  cascade-delete (agent/KB/data-source/flow delete family) — closes the
  'no ghost map rows after delete' requirement for every resource with
  child collections or its own tags map entry."}
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

---

## 2026-07-24 sweep

**invented-tags-field (the main finding this sweep).** Six response types —
`Agent`, `AgentAlias`, `KnowledgeBase`, `Flow`, `FlowAlias`, `Prompt` — had a
`Tags map[string]string \`json:"tags,omitempty"\`` field that gopherstack
echoed back on every Create/Get/Update response. Field-diffed each against
the real aws-sdk-go-v2 SDK types directly (`types.Agent`, `types.AgentAlias`,
`types.KnowledgeBase`, `GetFlowOutput`/`CreateFlowOutput`,
`CreateFlowAliasOutput`, `GetPromptOutput`/`CreatePromptOutput` in
`types/types.go` and the `api_op_*.go` files) — **none of the six have a
"tags" member**. Real Bedrock tags are write-only on the corresponding
`Create*Input` (confirmed `Tags map[string]string` present on
`CreateAgentInput`, `CreateAgentAliasInput`, `CreateKnowledgeBaseInput`,
`CreateFlowInput`, `CreateFlowAliasInput`, `CreatePromptInput`) and readable
only via `ListTagsForResource`; none of the `Update*Input` types accept a
tags param at all. Deleted the invented field from all six model structs
(`models.go`) and every place that populated/copied it
(agents.go/agent_aliases.go/knowledge_bases.go/flows.go/prompts.go).

This was more than a cosmetic wire-shape bug: because the invented struct
field was tags' *only* storage for `AgentAlias` and `FlowAlias` (their
`Create*` handlers never wrote into the shared `b.tags[ARN]` map the way
Agent/KnowledgeBase/Flow/Prompt did), `ListTagsForResource` on a
freshly-created alias with tags incorrectly returned empty. Fixed by adding
the missing `b.tags[al.AgentAliasARN] = ...` / `b.tags[al.AliasARN] = ...`
seeds to `CreateAgentAlias`/`CreateFlowAlias`. For the other four resource
types the `b.tags[ARN]` seed already existed independently of the invented
field, so removing the field was a pure subtraction there.

Also removed the `if cfg.Tags != nil { x.Tags = ... }` blocks from every
`Update*` backend method (`UpdateAgentAlias`, `UpdateFlow`, `UpdateFlowAlias`,
`UpdatePrompt`) — these were writing into a field that both doesn't exist on
the wire and doesn't correspond to any real `Update*Input.Tags` param (none
of the six Update input types have one), so the branch was already dead code
for any real SDK client; UpdateAgent/UpdateKnowledgeBase never had this
branch to begin with.

**cascade-delete (the second major finding).** `DeleteAgent` had a
pre-existing doc comment on it admitting it left `actionGroups`,
`agentAliases`, and `agentCollaborators` as permanent ghost rows (a bug
"preserved as-is" from a prior refactor). Extended the audit to every
resource with child collections or its own `b.tags[ARN]` entry and found the
same class of gap repeated:

| deleted resource | now cascade-deletes |
|---|---|
| `DeleteAgent` | actionGroups + agentCollaborators + agentKBAssocs (across DRAFT and every numbered version), agentAliases, agent's own tags, every alias's tags |
| `DeleteAgentAlias` | the alias's own tags entry |
| `DeleteKnowledgeBase` | dataSources, and (transitively, via a shared `deleteDataSourceChildrenLocked` helper) ingestionJobs + kbDocuments under each, KB's own tags |
| `DeleteDataSource` | ingestionJobs + kbDocuments scoped under it (same shared helper) |
| `DeleteFlow` | flowAliases, flow's own tags, every alias's tags (flowVersions cleanup was already correct) |
| `DeleteFlowAlias` | the alias's own tags entry |
| `DeletePrompt` | prompt's own tags entry (promptVersions cleanup was already correct) |

`actionGroups`/`agentCollaborators`/`agentKBAssocs` are indexed by the
composite `agentID/agentVersion` scope (`store.Index`), so cascading them on
agent delete required walking DRAFT plus every numbered `AgentVersion` row
and clearing each scope, rather than a single index lookup.
`agentAliases`/`dataSources`/`flowAliases` carry a plain by-parent index, so
no per-version walk was needed there. Data sources have no ARN/tags of their
own in the real API (no `Tags` param on `CreateDataSourceInput`, confirmed),
so `DeleteDataSource`/`DeleteKnowledgeBase` don't touch `b.tags` for the data
source row itself — only its ingestion jobs/documents (which also have no
tags) get removed.

Locked in with a new `cascade_delete_test.go` covering all seven paths above
by exercising the real `StorageBackend` API (create children → delete parent
→ assert every List/ListTagsForResource call comes back empty), not
whitebox map inspection.

**DRAFT-only {agentVersion} path constraint (the third finding).**
`CreateAgentActionGroup` ignored the `{agentVersion}` URI path segment and
always stored under `DRAFT` regardless of what was in the URL — flagged as a
known gap in the prior sweep but not fixed ("would need a new exact-match
AWS error string to validate against"). Resolved this sweep by fetching the
live AWS API reference pages (not just SDK source, which only encodes
required-ness/type, not path *pattern* constraints) for
`CreateAgentActionGroup`, `AssociateAgentCollaborator`, and
`AssociateAgentKnowledgeBase`: all three document
`agentVersion` as `Length Constraints: Fixed length of 5. Pattern: DRAFT` —
i.e. these three Create/Associate operations are DRAFT-only by contract, a
real client sending any other value gets a server-side pattern-validation
failure. Implemented as an explicit check
(`agentVersion != defaultAgentVersion` → `ValidationException`/400) in all
three backend methods, verified with new
`handler_agent_action_groups_test.go` /
`handler_agent_collaborators_test.go` / `handler_agent_knowledge_bases_test.go`
covering both the DRAFT-succeeds and non-DRAFT-rejected cases.

By contrast, `GetAgentActionGroup`'s API reference documents a *broader*
pattern — `(DRAFT|[0-9]{0,4}[1-9][0-9]{0,4})` — confirming Get/List/
Update/Delete on these three families intentionally accept numbered
versions too (real AWS snapshots action groups/collaborators/KB-associations
into each numbered version when it's created). gopherstack does not model
that snapshot-forward propagation; logged as a new gap
(`gopherstack-bedrockagent-version-snapshot`) rather than attempted this
sweep — it is a feature addition (deep-copying three sub-resource families
into every new agent version), not a bug fix, and materially larger in scope
than the rest of this pass.

**IngestionJob statistics.** Added the real `statistics` field
(`IngestionJobStatistics`: `numberOfDocumentsScanned`,
`numberOfMetadataDocumentsScanned`, `numberOfNewDocumentsIndexed`,
`numberOfModifiedDocumentsIndexed`, `numberOfMetadataDocumentsModified`,
`numberOfDocumentsDeleted`, `numberOfDocumentsFailed` — field names and
plain-int64 PrimitiveLong types confirmed against
`awsRestjson1_deserializeDocumentIngestionJobStatistics` in the SDK's
`deserializers.go`) to `IngestionJob`/`IngestionJobSummary`. Previously this
field was omitted entirely on every response. `StartIngestionJob` now
populates it from a real backend read — the actual count of
`KnowledgeBaseDocument` rows already stored for that data source (pushed via
the separate `IngestKnowledgeBaseDocuments` custom-content API) — reported
as both scanned and newly-indexed. The other five counters
(deleted/failed/modified/metadata-scanned/metadata-modified) are left at
zero rather than fabricated, since this backend does not track a prior-job
document snapshot to diff against; see `deferred` for the honest limitation.
Locked in with `handler_ingestion_jobs_test.go`.

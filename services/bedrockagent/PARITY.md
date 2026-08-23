---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: bedrockagent
sdk_module: aws-sdk-go-v2/service/bedrockagent@v1.58.4   # version audited against
last_audit_commit: d2851f3db666dbb499891c5ede8af1534c00ddf0
last_audit_date: 2026-08-07
overall: A            # RESTORED B->A (parity-5, 2026-07-31, follow-up pass): the routing
                      # bug that caused the prior A->B downgrade is fixed and proven.
                      # dispatchKBDocuments (handler.go) now assigns PUT on the base
                      # .../documents path to real IngestKnowledgeBaseDocuments and POST
                      # (GET too, as harmless leniency, matching the sibling
                      # CreateDataSource/ListDataSources and StartIngestionJob/
                      # ListIngestionJobs base-path conventions already in this file) to
                      # real ListKnowledgeBaseDocuments — see classifyDocPath
                      # (handler_knowledge_bases.go) for the matching fix to the
                      # ExtractOperation-facing classifier. The test helper this was
                      # blocked on (ingestionFixture.ingestDocs, one call site in
                      # handler_ingestion_jobs_test.go) now issues a real PUT instead of
                      # the emulator's-own-wrong-convention POST. A new regression test,
                      # TestKBDocumentsRealWireRouting, drives both operations by their
                      # real method+path and asserts each reaches its own handler; it was
                      # confirmed to fail against the pre-fix code (PUT 404'd with
                      # "unknown kb docs op") before the fix and passes after. No other
                      # issue in this file was found to justify withholding the grade
                      # restoration — the pre-existing GetPrompt/DeletePrompt promptVersion
                      # partial-gap rows below are unrelated and predate this downgrade
                      # (already present under the prior A grade). See the corrected
                      # IngestKnowledgeBaseDocuments/ListKnowledgeBaseDocuments rows and
                      # gaps entry for detail.
                      # 2026-08-08 (gopherstack-rvyd follow-up): closed the
                      # version-snapshot immutability gap left by the
                      # 2026-08-07 sweep (see gaps entry + Notes:
                      # version-snapshot-propagation) — Update/Delete/
                      # Disassociate on action groups/collaborators/KB-assocs
                      # now reject non-DRAFT agentVersion same as
                      # Create/Associate. IngestionJobStatistics' other-5-
                      # counters and opaque-blob deep-shape validation
                      # re-assessed and confirmed still correctly deferred
                      # (see deferred entries) — no fabrication introduced.
                      # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateAgent: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "Agent had an invented 'tags' field on the wire response — real
    types.Agent has no Tags member (tags are write-only on CreateAgentInput,
    readable only via ListTagsForResource). Removed the field; CreateAgent
    still seeds b.tags[AgentARN] directly. See Notes: invented-tags-field.
    2026-08-21 (gopherstack-r80d batch 7): Agent.RoleARN's json tag had
    'omitempty' despite types.Agent's required 'agentResourceRoleArn'
    (deserializers.go) -- unlike most such fixes this pass, NOT independently
    provable via a real SDK client: AgentResourceRoleArn is also required on
    CreateAgentInput/UpdateAgentInput client-side, so a real client can never
    actually trigger the empty-value path. Removed omitempty anyway as a
    harmless hardening (present-but-empty beats silently-dropped-key if this
    ever becomes reachable another way), but NOT counted as one of this
    batch's proven bugs -- see PARITY's Notes section for the full accounting."}
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
    of List — fixed this sweep, see Notes. FIXED 2026-08-21 (gopherstack-r80d
    batch 7): types.AgentVersionSummary requires 'createdAt' (deserializers.go)
    -- AgentVersionSummary had no field for it at all. Added, populated from
    the persisted AgentVersion record. Proven via
    Test_SDKRoundTrip_AgentVersion_IdleSessionTTL (wire_output_required_r80d_test.go)."}
  GetAgentVersion: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "FIXED 2026-08-21 (gopherstack-r80d batch 7): types.AgentVersion
    requires 'idleSessionTTLInSeconds' and 'agentResourceRoleArn'
    (deserializers.go) -- AgentVersion had no field at all for the former
    (the 'member with no struct field' class), and the latter was tagged
    omitempty despite being required. Unlike Agent's own RoleARN omitempty
    (see CreateAgent note), THIS one is independently provable: AgentVersion
    is a point-in-time snapshot built by newAgentVersionLocked
    (agent_versions.go), which already had the live Agent's
    IdleSessionTTLInSeconds/RoleARN in scope but simply didn't copy them --
    both now threaded through at snapshot time, no fabrication. Proven via
    Test_SDKRoundTrip_AgentVersion_IdleSessionTTL
    (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/
    restored, md5sum-verified byte-identical."}
  DeleteAgentVersion: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "(this sweep, gopherstack-rvyd) newly reachable ghost-row gap:
    once numbered versions carry their own action-group/collaborator/KB-assoc
    snapshot rows (see snapshotSubResourcesLocked below), deleting the
    version left those rows behind. Fixed via a shared
    deleteSubResourcesLocked helper, also now used by DeleteAgent's
    per-version cleanup loop (agents.go) in place of its inlined duplicate."}
  CreateAgentActionGroup: {wire: fixed, errors: fixed, state: ok, persist: ok,
    note: "was ignoring the path agentVersion, always storing under DRAFT
    regardless of what the client sent. Real AWS constrains this URI path
    param to the literal pattern `DRAFT` (fixed length 5, per the API
    reference) — a non-DRAFT value must fail ValidationException/400.
    Verified via the AWS API reference (not just SDK source, which only
    encodes shape/required-ness, not path pattern constraints). Fixed:
    validates agentVersion == DRAFT, real ValidationException otherwise."}
  GetAgentActionGroup: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "(this sweep, gopherstack-rvyd) against a numbered agentVersion,
    used to always 404 — no numbered version ever had action-group rows.
    Fixed by snapshotting DRAFT's action groups into every new numbered
    version at creation time — see Notes: version-snapshot-propagation."}
  UpdateAgentActionGroup: {wire: ok, errors: fixed, state: ok, persist: ok,
    note: "(gopherstack-rvyd follow-up, 2026-08-08) was missing the same
    DRAFT-only {agentVersion} constraint Create already had — see Notes:
    version-snapshot-propagation."}
  DeleteAgentActionGroup: {wire: ok, errors: fixed, state: ok, persist: ok,
    note: "(gopherstack-rvyd follow-up, 2026-08-08) same DRAFT-only fix as
    UpdateAgentActionGroup."}
  ListAgentActionGroups: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was unreachable: POST (real wire method) was misrouted to Create — fixed.
    FIXED 2026-08-21 (gopherstack-r80d batch 7): types.ActionGroupSummary
    requires 'updatedAt' (deserializers.go) -- ActionGroupSummary had no
    field for it at all. Added, populated from the persisted
    AgentActionGroup record. Proven via
    Test_SDKRoundTrip_ListAgentActionGroups_UpdatedAt
    (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/
    restored, md5sum-verified byte-identical."}
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
    note: "was misrouted: POST (real wire method) hit Create instead of List — fixed.
    FIXED 2026-08-21 (gopherstack-r80d batch 7): types.AgentAliasSummary
    requires 'createdAt' and 'updatedAt' (deserializers.go) -- AgentAliasSummary
    had no fields for either. Added, populated from the persisted AgentAlias
    record. Proven via Test_SDKRoundTrip_ListAgentAliases_CreatedAtUpdatedAt
    (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/
    restored, md5sum-verified byte-identical."}
  AssociateAgentCollaborator: {wire: fixed, errors: fixed, state: ok, persist: ok,
    note: "same DRAFT-only {agentVersion} path constraint as
    CreateAgentActionGroup, confirmed via the API reference — fixed.
    FIXED 2026-08-21 (gopherstack-r80d batch 7): types.AgentCollaborator's
    required 'lastUpdatedAt' (deserializers.go) was tagged 'updatedAt' on
    this struct -- a key no real deserializer for this shape reads, so
    LastUpdatedAt decoded nil on every op sharing this struct
    (Associate/Get/Update/ListAgentCollaborators). Retagged. Proven via
    Test_SDKRoundTrip_AgentCollaborator_LastUpdatedAt
    (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/
    restored, md5sum-verified byte-identical."}
  GetAgentCollaborator: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "(this sweep, gopherstack-rvyd) same numbered-version snapshot fix
    as GetAgentActionGroup — see Notes: version-snapshot-propagation. See
    AssociateAgentCollaborator's 2026-08-21 note for the lastUpdatedAt fix,
    which applies here too (shared struct)."}
  UpdateAgentCollaborator: {wire: ok, errors: fixed, state: ok, persist: ok,
    note: "(gopherstack-rvyd follow-up, 2026-08-08) same DRAFT-only fix as
    UpdateAgentActionGroup — see Notes: version-snapshot-propagation. See
    AssociateAgentCollaborator's 2026-08-21 note for the lastUpdatedAt fix."}
  DisassociateAgentCollaborator: {wire: ok, errors: fixed, state: ok, persist: ok,
    note: "(gopherstack-rvyd follow-up, 2026-08-08) same DRAFT-only fix as
    UpdateAgentActionGroup."}
  ListAgentCollaborators: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was totally unreachable: POST (real wire method) had no case at all and
    404'd — fixed. See AssociateAgentCollaborator's 2026-08-21 note for the
    lastUpdatedAt fix, which applies here too (shared struct)."}
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
  GetAgentKnowledgeBase: {wire: ok, errors: ok, state: fixed, persist: ok,
    note: "(this sweep, gopherstack-rvyd) same numbered-version snapshot fix
    as GetAgentActionGroup — see Notes: version-snapshot-propagation."}
  UpdateAgentKnowledgeBase: {wire: ok, errors: fixed, state: ok, persist: ok,
    note: "(gopherstack-rvyd follow-up, 2026-08-08) same DRAFT-only fix as
    UpdateAgentActionGroup — see Notes: version-snapshot-propagation."}
  DisassociateAgentKnowledgeBase: {wire: ok, errors: fixed, state: ok, persist: ok,
    note: "(gopherstack-rvyd follow-up, 2026-08-08) same DRAFT-only fix as
    UpdateAgentActionGroup."}
  ListAgentKnowledgeBases: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "was totally unreachable: POST (real wire method) had no case at all and
    404'd — fixed. SEPARATELY (gopherstack-dv4s, over-wide sweep): the prior
    'wire: fixed' only verified reachability, never checked for extra fields —
    the handler reused the full AgentKnowledgeBase struct (Get shape) for List,
    leaking agentId/agentVersion/createdAt. Real types.AgentKnowledgeBaseSummary
    (bedrockagent@v1.58.4, types/types.go) declares only knowledgeBaseId,
    knowledgeBaseState, updatedAt, description. Fixed with a dedicated
    AgentKnowledgeBaseSummary type."}
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
  ListFlows: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "FIXED 2026-08-21 (gopherstack-r80d batch 7): types.FlowSummary
    requires 'arn' and 'createdAt' (deserializers.go) -- FlowSummary had no
    fields for either at all. Added, populated from the persisted Flow
    record. Proven via Test_SDKRoundTrip_ListFlows_Summary
    (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/
    restored, md5sum-verified byte-identical."}
  PrepareFlow: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same FlowStatus casing fix"}
  ValidateFlowDefinition: {wire: ok, errors: ok, state: ok, persist: ok,
    note: "always returns zero validation errors — acceptable permissive-emulator behavior"}
  CreateFlowVersion: {wire: fixed, errors: ok, state: fixed, persist: ok,
    note: "same FlowStatus casing fix. FIXED 2026-08-21 (gopherstack-r80d
    batch 7): Create/GetFlowVersionOutput require 'executionRoleArn'
    (api_op_CreateFlowVersion.go/api_op_GetFlowVersion.go) -- FlowVersion had
    no field for it at all (the 'member with no struct field' class),
    despite the parent Flow's RoleARN already being in scope in
    CreateFlowVersion (flows.go) and simply not copied. Added RoleARN,
    populated from the parent Flow at snapshot time -- no fabrication.
    Proven via Test_SDKRoundTrip_CreateFlowVersion_ExecutionRoleArn
    (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/
    restored, md5sum-verified byte-identical."}
  GetFlowVersion: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "See CreateFlowVersion's 2026-08-21 note for the executionRoleArn
    fix, which applies here too (shared struct)."}
  DeleteFlowVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFlowVersions: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "(gopherstack-dv4s, over-wide sweep) prior 'wire: ok' only checked
    required fields were present, never that extras were absent. The
    FlowVersionSummary builder leaked name/description, which real
    types.FlowVersionSummary (bedrockagent@v1.58.4, types/types.go) does not
    declare (only arn, createdAt, id, status, version) — those two members
    live on the full FlowVersion (Get) type, not the List summary. Fixed by
    narrowing FlowVersionSummary itself, which is dedicated to this op only."}
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
  GetPrompt: {wire: partial, errors: ok, state: ok, persist: ok,
    note: "GAP (parity-5/phantom-triage, 2026-07-31): does not read the real
    promptVersion query parameter at all — always returns the DRAFT/latest
    version regardless of what a real client requests. See GetPromptVersion
    row below for the fabricated route this gap left in place of the real
    fix."}
  UpdatePrompt: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "invented 'tags' wire field removed; real UpdatePromptInput has no
    tags param, so the old cfg.Tags-on-update branch was dead code for real
    clients — removed"}
  DeletePrompt: {wire: partial, errors: ok, state: fixed, persist: ok,
    note: "now also deletes the prompt's b.tags[PromptArn] entry (promptVersions
    cleanup was already correct) — see Notes: cascade-delete. GAP
    (parity-5/phantom-triage, 2026-07-31): does not read the real promptVersion
    query parameter either — always deletes the whole prompt (all versions),
    never a single version. See DeletePromptVersion row below."}
  ListPrompts: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePromptVersion: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "FIXED 2026-08-21 (gopherstack-r80d batch 7): CreatePromptVersionOutput
    requires 'updatedAt' (api_op_CreatePromptVersion.go) -- PromptVersion had
    no field for it at all. Added, set equal to CreatedAt at creation time
    (a prompt version is an immutable snapshot never updated afterward, so
    the two timestamps are honestly identical, not fabricated). Proven via
    Test_SDKRoundTrip_CreatePromptVersion_UpdatedAt
    (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/
    restored, md5sum-verified byte-identical."}
  GetPromptVersion: {wire: n/a, errors: n/a, state: ok, persist: ok,
    note: "CORRECTED (parity-5/phantom-triage, 2026-07-31): this was marked
    wire: ok, which was wrong — 'GetPromptVersion' is not a real bedrock-agent
    operation at all. Real AWS gets a specific prompt version via GetPrompt's
    promptVersion query parameter (GET /prompts/{id}/?promptVersion=X), which
    GetPrompt does not implement (see that row). This op's own route
    (GET /prompts/{id}/versions/{ver}) is a gopherstack-only path a real SDK
    client never constructs. Removed from GetSupportedOperations() this pass
    (was previously incorrectly advertised as if real); route and backend
    state kept, used by this package's own tests. wire/errors marked n/a
    rather than ok/gap since there is no real wire shape for a fabricated
    operation to be correct or incorrect against."}
  DeletePromptVersion: {wire: n/a, errors: n/a, state: ok, persist: ok,
    note: "CORRECTED (parity-5/phantom-triage, 2026-07-31): same issue as
    GetPromptVersion above — not a real operation (real: DeletePrompt's
    promptVersion query param, which DeletePrompt does not implement).
    Removed from GetSupportedOperations() this pass; route/state kept for
    this package's own tests."}
  IngestKnowledgeBaseDocuments: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "Routing FIXED (parity-5, 2026-07-31, follow-up pass): dispatchKBDocuments
    (handler.go) had no case for PUT to the base .../datasources/{id}/documents
    path at all, so a real client's real, correctly-formed request 404'd
    ('unknown kb docs op'). Now routed on PUT, verified against the vendored
    SDK's IngestKnowledgeBaseDocuments.request.snap. classifyDocPath
    (handler_knowledge_bases.go), the parallel ExtractOperation-facing
    classifier, updated to match. See TestKBDocumentsRealWireRouting
    (handler_ingestion_jobs_test.go) for the regression coverage, and the
    gaps entry below for the fix history.
    Body FIXED (gopherstack-wzwn, 2026-08-13): the routing fix above only
    proved the request *reached* handleIngestKBDocs -- the body decode was
    still wrong. Each Documents[] entry's identity lives at
    content.custom.customDocumentIdentifier.id or content.s3.s3Location.uri
    (types.KnowledgeBaseDocument/DocumentContent/CustomContent/S3Content,
    verified against api_op_IngestKnowledgeBaseDocuments.go and
    serializeDocumentKnowledgeBaseDocument in the pinned SDK
    aws-sdk-go-v2/service/bedrockagent@v1.58.4); there is no top-level
    'documentId' field. The handler was reading exactly that nonexistent
    'documentId' key, so every real client's ingested documents were stored
    under an empty identity. Now decodes documentContentWire and derives
    KBDocumentIdentifier{DataSourceType, Custom, S3} from the real nested
    shape. See TestHandlerGetKBDocuments_RealWireIdentifier/
    TestHandlerDeleteKBDocuments_RealWireIdentifier (handler_knowledge_bases_test.go)."}
  GetKnowledgeBaseDocuments: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "FIXED (gopherstack-wzwn, 2026-08-13): was 'wire: ok', which was false --
    nobody had diffed the request body against the real SDK. Real
    GetKnowledgeBaseDocumentsInput.DocumentIdentifiers ([]types.DocumentIdentifier,
    a list of {dataSourceType, custom:{id}, s3:{uri}} objects -- verified against
    api_op_GetKnowledgeBaseDocuments.go and serializeDocumentDocumentIdentifier
    in the pinned SDK aws-sdk-go-v2/service/bedrockagent@v1.58.4) was being decoded
    as a struct tagged json:\"documentIds\" (a key no client sends) holding
    []string (the wrong type on top of the wrong key). req.DocumentIDs was
    therefore always empty and the op always returned an empty documentDetails
    list for every real client, silently. Now decodes documentIdentifiers as
    []KBDocumentIdentifier matching the real nested shape, and rejects an
    identifier missing the sub-object its own dataSourceType requires as
    ValidationException (declared in awsRestjson1_deserializeOpErrorGetKnowledgeBaseDocuments).
    See TestHandlerGetKBDocuments_RealWireIdentifier and
    TestHandlerKBDocuments_MissingIdentifierIsValidationException
    (handler_knowledge_bases_test.go)."}
  DeleteKnowledgeBaseDocuments: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "FIXED (gopherstack-wzwn, 2026-08-13): same bug and fix as
    GetKnowledgeBaseDocuments above, worse impact -- Delete reported success
    (202, real documentDetails-shaped body) having deleted nothing, on every
    real client, for every call. Also fixed a second, independent fabrication
    found while rewriting this op: on a found-and-deleted document the
    response set status: 'DELETED', a value that does not exist in the real
    DocumentStatus enum (types/enums.go: INDEXED, PARTIALLY_INDEXED, PENDING,
    FAILED, METADATA_PARTIALLY_INDEXED, METADATA_UPDATE_FAILED, IGNORED,
    NOT_FOUND, STARTING, IN_PROGRESS, DELETING, DELETE_IN_PROGRESS -- no
    DELETED). Now uses DELETING, the declared status for 'you submitted the
    delete job containing the document', matching this backend's existing
    synchronous-completion convention (Ingest jumps straight to INDEXED
    rather than modeling STARTING/PENDING/IN_PROGRESS). See
    TestHandlerDeleteKBDocuments_RealWireIdentifier and
    TestHandlerKBDocuments_MissingIdentifierIsValidationException
    (handler_knowledge_bases_test.go)."}
  ListKnowledgeBaseDocuments: {wire: fixed, errors: ok, state: ok, persist: ok,
    note: "FIXED (parity-5, 2026-07-31, follow-up pass): dispatchKBDocuments
    routed POST-on-base unconditionally to handleIngestKBDocs (see
    IngestKnowledgeBaseDocuments row), so a real client's real
    ListKnowledgeBaseDocuments call (POST to the base .../documents path,
    verified against the vendored SDK's ListKnowledgeBaseDocuments.request.snap)
    got Ingest's upsert behavior instead of a listing. Now routed on POST
    (GET accepted too, as harmless extra leniency matching the sibling
    CreateDataSource/ListDataSources and StartIngestionJob/ListIngestionJobs
    base-path conventions already in this file — no real client sends it, but
    it's a superset of the real API, not a divergence from it). classifyDocPath
    updated to match. See TestKBDocumentsRealWireRouting for the regression
    coverage."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok,
    note: "NEW this sweep -- SDK bump added PutResourcePolicy/GetResourcePolicy/
    DeleteResourcePolicy to aws-sdk-go-v2/service/bedrockagent (were previously
    absent from the module entirely, hence 'ok' not 'fixed': there is no prior
    gopherstack behavior to have gotten wrong). Distinct operation family from
    core Bedrock's own Put/Get/DeleteResourcePolicy despite the shared names
    (see resource_policy.go's package doc comment and Notes below) -- PUT
    /resourcepolicy/{resourceArn}, field 'policy' (not 'resourcePolicy'),
    scoped to knowledge bases only. resourceArn validated against
    knowledgeBaseResourceArnPattern AND checked against b.knowledgeBases (a
    policy cannot attach to a nonexistent KB)."}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok,
    note: "NEW this sweep, same family as PutResourcePolicy."}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok,
    note: "NEW this sweep, same family as PutResourcePolicy. DeleteKnowledgeBase
    now cascades into resourcePolicies (see Notes: cascade-delete, updated this
    sweep) so a deleted KB's policy row cannot survive it."}
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
  - "FIXED (gopherstack-wzwn, 2026-08-13): GetKnowledgeBaseDocuments and
    DeleteKnowledgeBaseDocuments decoded their request body against a struct
    tagged json:\"documentIds\" holding []string. Real clients send
    \"documentIdentifiers\", a list of {dataSourceType, custom:{id}, s3:{uri}}
    objects (types.DocumentIdentifier) -- wrong key AND wrong type, so the
    decoded slice was always empty and both ops silently no-opped on every
    real request while returning success (Delete: 202 with a real-shaped
    documentDetails body, having deleted nothing). The routing fix logged
    below (parity-5, 2026-07-31) proved the request reached the handler; it
    never proved the handler understood the body, and this survived that
    pass's TestKBDocumentsRealWireRouting because that test's own fixture
    helper (ingestionFixture.ingestDocs) sent the same invented
    'documentId'/'documentIds' shape the handler expected, not the real SDK
    shape. Sibling IngestKnowledgeBaseDocuments shared the same bug on its
    own axis: it read a top-level 'documentId' key that doesn't exist on the
    real wire either (the real identity lives inside content.custom.../
    content.s3...). All three ops now decode the real nested shape via
    KBDocumentIdentifier/documentContentWire; ListKnowledgeBaseDocuments was
    checked and has no request-body identifier to get wrong (it lists
    everything under a data source). See the three ops' rows above for
    detail and the new regression tests in handler_knowledge_bases_test.go."
  - "GetSupportedOperations phantom-triage pass (parity-5, 2026-07-31): the reverse
    sdkcheck (gopherstack-vhw2) flagged GetPromptVersion and DeletePromptVersion as
    fabricated — neither is a real bedrock-agent operation (real AWS: GetPrompt/
    DeletePrompt's promptVersion query parameter, which GetPrompt/DeletePrompt do not
    implement here — see those ops' rows). Removed both from GetSupportedOperations();
    routes/backend state kept as internal-only (used by this package's own tests,
    unreachable by a real SDK client which would never construct
    /prompts/{id}/versions/{ver}). See GetPromptVersion/DeletePromptVersion ops rows."
  - "FIXED (parity-5, 2026-07-31, follow-up pass) — was: 'SEVERE, found while
    investigating the above (parity-5/phantom-triage, 2026-07-31): dispatchKBDocuments
    (handler.go) has no case at all for PUT to the base .../documents path...
    Downgraded overall: A->B for this.' Re-verified both real wire shapes against the
    vendored SDK's request snapshots (aws-sdk-go-v2/service/bedrockagent
    IngestKnowledgeBaseDocuments.request.snap: PUT to the base
    .../datasources/{id}/documents path; ListKnowledgeBaseDocuments.request.snap: POST
    to the same base path) before touching dispatch, per
    .claude/memories/parity-principles.md #2. dispatchKBDocuments now routes PUT to
    handleIngestKBDocs and POST (GET too, as harmless leniency) to handleListKBDocs;
    classifyDocPath (handler_knowledge_bases.go, the parallel ExtractOperation-facing
    classifier) updated to match. The blocking issue named in the prior pass —
    this package's own test helper (ingestionFixture.ingestDocs,
    handler_ingestion_jobs_test.go) POSTing to ingest, matching the emulator's own
    wrong convention instead of the real SDK's — is fixed: the helper's one call site
    now issues a real PUT. Added TestKBDocumentsRealWireRouting
    (handler_ingestion_jobs_test.go), which drives both operations by their real
    method+path and asserts each reaches its own handler; confirmed failing against
    the pre-fix code (PUT 404'd with 'unknown kb docs op') before applying the fix.
    GetKnowledgeBaseDocuments (POST .../getDocuments) and DeleteKnowledgeBaseDocuments
    (POST .../deleteDocuments) were already correctly routed and are unaffected.
    Restored overall: B->A."
  - "ValidateFlowDefinition always returns zero validation errors regardless of
    the definition passed — acceptable for a permissive emulator (the op still
    reads real state and returns the AWS-accurate empty-array shape); not a
    disguised no-op flag, just an easy target if flow-definition validation
    logic is ever wanted. Unchanged this sweep."
  - "FIXED (gopherstack-rvyd, 2026-08-07). Was: 'Real AWS snapshots an
    agent's action groups, collaborators, and agent-KB associations into
    each numbered agent version at the moment CreateAgentAlias auto-creates
    it ... gopherstack's newAgentVersionLocked only snapshots the Agent's
    own top-level fields, not these three sub-resource families.' See Notes:
    version-snapshot-propagation for the fix.
    FOLLOW-UP FIXED (gopherstack-rvyd, 2026-08-08): the 2026-08-07 fix made
    numbered versions carry real snapshot rows, but UpdateAgentActionGroup/
    DeleteAgentActionGroup/UpdateAgentCollaborator/
    DisassociateAgentCollaborator/UpdateAgentKnowledgeBase/
    DisassociateAgentKnowledgeBase never got the DRAFT-only {agentVersion}
    check their Create/Associate counterparts already had (confirmed absent
    by reading each method directly, then confirmed via the live AWS API
    reference that all six document `Pattern: DRAFT`, fixed length 5, same
    as Create/Associate) — so a client could call e.g.
    UpdateAgentActionGroup(agentVersion=\"1\") and mutate or delete a
    numbered version's 'immutable' snapshot row directly, which real AWS
    rejects with ValidationException. Fixed by adding the same
    agentVersion != defaultAgentVersion check used by Create/Associate to
    all six methods. See Notes: version-snapshot-propagation."
deferred:
  - "KBDocument/DataSource nested configuration blobs (dataSourceConfiguration,
    vectorIngestionConfiguration, knowledgeBaseConfiguration,
    storageConfiguration, actionGroupExecutor, apiSchema, functionSchema,
    guardrailConfiguration, memoryConfiguration, promptOverrideConfiguration) are
    passed through as opaque map[string]any/JSON blobs rather than typed +
    validated against the SDK's nested shape unions. This is consistent with how
    this service already treats them; deep-shape validation of these blobs was
    out of scope this sweep (unchanged).
    RE-ASSESSED (gopherstack-rvyd follow-up, 2026-08-08): confirmed genuinely
    out of scope, not just deferred by default. Read the real SDK shapes for
    the three most plausible bounded candidates —
    types.ActionGroupExecutor, types.APISchema, types.FunctionSchema
    (types/types.go) — and all three are Smithy tagged unions (Go
    interface + one struct per member, e.g. ActionGroupExecutor is
    ActionGroupExecutorMemberLambda XOR ActionGroupExecutorMemberCustomControl)
    requiring XOR-presence validation, per-member wire-key discovery from
    serializers.go, and (for FunctionSchema) validating a nested []Function
    list with its own required/optional sub-fields. There are ten such blob
    families across this service, not one; validating a single one (even the
    simplest, ActionGroupExecutor) while leaving the other nine opaque would
    produce an inconsistent, arbitrarily-partial validation surface with no
    corresponding bug report or wire-shape failure driving the choice — unlike
    every other fix in this file, which closed a concrete, demonstrated gap.
    No functional gap is caused by the current opaque pass-through (requests
    round-trip correctly; this is a permissive-emulator design choice already
    used consistently across the service). Left unchanged."
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
    IngestKnowledgeBaseDocuments custom-content API are counted.
    Re-confirmed accurate and unchanged this sweep (gopherstack-rvyd,
    2026-08-07) — still no prior-job snapshot mechanism to diff against, so
    still correctly zero rather than fabricated.
    RE-VERIFIED (gopherstack-rvyd follow-up, 2026-08-08), reading
    IngestKnowledgeBaseDocuments/KBDocumentDetail directly rather than
    trusting the prior note: it is not merely that no prior-job snapshot is
    tracked -- the raw material for these counters is not retained at all.
    KBDocumentDetail (models.go) stores only
    DocumentID/KnowledgeBaseID/DataSourceID/Status; IngestKnowledgeBaseDocuments
    receives each KBDocument's Content and Metadata (models.go: KBDocument has
    both) and discards both on every call, so there is no stored content/hash
    to diff for NumberOfModifiedDocumentsIndexed and no stored metadata at all
    for NumberOfMetadataDocumentsScanned/NumberOfMetadataDocumentsModified --
    these two would be fabricated even with a diffing mechanism, since there is
    nothing to scan. NumberOfDocumentsDeleted/NumberOfDocumentsFailed are the
    only two of the five that a bounded, non-fabricated fix could theoretically
    reach (by snapshotting the live document-ID set into each IngestionJob and
    diffing on the next run), but doing so for 2 of 5 fields while the other 3
    stay permanently unreachable was judged not worth the asymmetry it would
    introduce into a single response object one sweep after the counters were
    added; the honest zero for all five remains the correct behavior until (if
    ever) content/metadata retention is added as a real feature. Left
    unchanged."
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

**2026-08-21 (gopherstack-r80d batch 7): required-response-member sweep.**
Read all 66 ops with >=1 required output field end to end (154 required
fields per `cmd/requiredoutputfields`) against the pinned
`bedrockagent@v1.58.4` SDK -- not just the flat per-op top-level fields the
tool reports, but every domain struct's OWN required members too (Agent,
AgentVersion, AgentAlias, AgentCollaborator, AgentActionGroup, DataSource,
KnowledgeBase, Flow, FlowVersion, FlowAlias, Prompt, PromptVersion, and
their `*Summary` List-element siblings), since this service's wire shapes
are almost entirely "one wrapper key = the whole nested object" (the same
structural pattern pinpoint's batch 5 already established) -- a per-op
check without also checking the nested struct's own required members would
have missed every bug found here. 8 real bugs found, all previously
undetected despite this service having already been through five general-
parity sweeps (parity-4/5, gopherstack-rvyd, gopherstack-dv4s x2):

- `FlowVersion` (Create/GetFlowVersion): missing required `executionRoleArn`
  entirely -- no struct field at all, despite the parent Flow's RoleARN
  already being in scope at snapshot time and simply not copied.
- `FlowSummary` (ListFlows): missing required `arn`/`createdAt` entirely.
- `PromptVersion` (CreatePromptVersion): missing required `updatedAt`
  entirely -- set equal to CreatedAt since prompt versions are immutable
  snapshots.
- `AgentCollaborator` (Associate/Get/Update/ListAgentCollaborators, one
  shared struct): required `lastUpdatedAt` was tagged the invented key
  `updatedAt` -- wrong wire key, not missing.
- `AgentVersion` (GetAgentVersion): missing required
  `idleSessionTTLInSeconds` entirely, and `agentResourceRoleArn` was tagged
  `omitempty` despite being required -- both fixed by threading the live
  Agent's already-known values through at snapshot time
  (`newAgentVersionLocked`).
- `AgentVersionSummary` (ListAgentVersions): missing required `createdAt`
  entirely.
- `ActionGroupSummary` (ListAgentActionGroups): missing required
  `updatedAt` entirely.
- `AgentAliasSummary` (ListAgentAliases): missing required
  `createdAt`/`updatedAt` entirely.

All 8 proven via real `aws-sdk-go-v2/service/bedrockagent` client round
trips (`wire_output_required_r80d_test.go`), each hand-reverted to confirm
it fails against the pre-fix struct/handler, restored, byte-identical via
md5sum.

**Two findings NOT counted as bugs (`Agent.RoleARN` and, by the same
reasoning, everywhere `AgentResourceRoleArn`-equivalent fields are required-
but-optional-on-input elsewhere in this service): omitempty removed as
harmless hardening, but not provable.** `types.Agent.AgentResourceRoleArn`
is required on the output but optional on `CreateAgentInput`/
`UpdateAgentInput`'s own Smithy model -- structurally identical to the
`ReferenceArn`-class bug this campaign has fixed elsewhere (e.g. omics'
`CreateMultipartReadSetUpload`). The difference here: a real
`aws-sdk-go-v2` client validates its own **required** fields client-side
before ever sending the request, but `AgentResourceRoleArn` is only
required on the *response*, not the request -- so this specific field
really can be omitted by a caller, and the bug is real. It was still left
unproven here because building a request that omits it and observing the
decoded nil pointer requires either a raw HTTP client (not the SDK
round-trip proof this campaign standardizes on) or confirming no other
codepath fills it in first; deprioritized in favor of the 8 structurally
guaranteed bugs above given the batch's time budget. Left as `omitempty`
removed (correct either way) but not claimed as one of this batch's proven
bugs -- worth a real-client proof in a future pass.

**Method note: nested-struct required members are the majority of what
this service's bug surface actually was.** `cmd/requiredoutputfields`
reports only the top-level `<Op>Output` struct's own required fields
(e.g. `GetAgentCollaboratorOutput` has exactly one required member,
`AgentCollaborator`) -- it does not recurse into that member's own type to
check ITS required fields. For a service like this one where nearly every
op's entire body is one wrapper key around a shared domain struct (the
same "single httpPayload-style body member" pattern pinpoint's batch 5
found), the tool's flat count (154 fields / 66 ops) undercounts the real
surface substantially: 6 of this batch's 8 bugs were only found by also
reading every domain struct (`Agent`, `AgentVersion`, `AgentAlias`,
`AgentCollaborator`, `AgentActionGroup`, `Flow`, `FlowVersion`, `Prompt`,
`PromptVersion`, and their `*Summary` counterparts) against its own real
SDK type, not by iterating the tool's op list alone. Future services with
this same wrapper-key structural pattern should budget for this: reading
domain structs is not optional bonus work here, it is where most of the
bugs are.

**version-snapshot-propagation (gopherstack-rvyd, 2026-08-07 sweep).** Real
AWS snapshots an agent's DRAFT action groups, collaborators, and agent-KB
associations into a numbered agent version at the moment that version is
created (confirmed via GetAgentActionGroup's API reference: its
`{agentVersion}` path pattern `(DRAFT|[0-9]{0,4}[1-9][0-9]{0,4})` accepts
non-DRAFT versions, so a real client can Get/List these against a numbered
version and expects DRAFT's content at that point in time, not an empty
result). gopherstack's `newAgentVersionLocked` (`agent_versions.go`)
previously only copied the `Agent`'s own top-level fields into the new
`AgentVersion` row; the three sub-resource families were left unsnapshotted,
so every numbered-version Get/List for them always came back empty/404.

Fixed with `snapshotSubResourcesLocked`, called from `newAgentVersionLocked`
right after the new `AgentVersion` row is written: it reads DRAFT's current
`actionGroups`/`agentCollaborators`/`agentKBAssocs` rows via their existing
`byAgentVersion` index and writes a shallow copy of each into the new
version's scope with `AgentVersion` rewritten to the new version string.
Since `newAgentVersionLocked` is the sole path to a numbered version
(`CreateAgentAlias`'s auto-create-on-empty-`routingConfiguration` and the
internal-only `CreateAgentVersion`), this closes the gap on both paths.

This introduced a new ghost-row surface for `DeleteAgentVersion`, which
previously never had anything to clean up (numbered versions never carried
sub-resource rows before this fix) — deleting a version now must also remove
its snapshot rows or they become permanent orphans. Fixed via a shared
`deleteSubResourcesLocked` helper, which also replaces `DeleteAgent`'s
previously-inlined per-version cleanup loop (`agents.go`) — same logic, one
definition. Locked in with `agent_version_snapshot_test.go`
(`TestNewAgentVersion_SnapshotsSubResources`,
`TestDeleteAgentVersion_CascadesSubResources`), both confirmed failing
against the pre-fix code before the fix landed.

**Follow-up: snapshot immutability was not enforced (gopherstack-rvyd
follow-up, 2026-08-08).** The above fix made `Get`/`List` against a numbered
version return real content, but did not check whether the *mutation* ops on
the same three families (`UpdateAgentActionGroup`, `DeleteAgentActionGroup`,
`UpdateAgentCollaborator`, `DisassociateAgentCollaborator`,
`UpdateAgentKnowledgeBase`, `DisassociateAgentKnowledgeBase`) reject non-DRAFT
versions the way `CreateAgentActionGroup`/`AssociateAgentCollaborator`/
`AssociateAgentKnowledgeBase` already did. They didn't: reading each of the
six methods directly showed none had an `agentVersion != defaultAgentVersion`
guard, so a client could call
`UpdateAgentActionGroup(agentID, "1", actionGroupID, cfg)` and silently
mutate a numbered version's snapshot row, or delete it outright — defeating
the entire point of "immutable" version snapshots the moment they became
reachable objects rather than perpetually-empty ones. Confirmed against the
live AWS API reference for all six operations: every one documents
`agentVersion` as `Pattern: DRAFT`, `Length Constraints: Fixed length of 5` —
identical to their Create/Associate counterparts. Fixed by adding the same
guard used by Create/Associate to all six methods, returning
`ValidationException`/400 for any non-DRAFT value before the method touches
its lock or map. Locked in with
`agent_version_immutability_test.go`'s
`TestMutateSubResourceRejectsNonDraftVersion` (table-driven, one subtest per
op), confirmed failing against the pre-fix code — each subtest got 404
`ResourceNotFoundException` instead of 400 `ValidationException`, since with
no DRAFT-only guard the methods fell through to a normal not-found lookup
against a numbered version that (in the test fixture) had no such row.

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

## 2026-08-21 (gopherstack-hjdd): snapshot-version guard, unbumped key rename

`bedrockagentSnapshotVersion` bumped 2 -> 3. `732c2bafa` retagged
`AgentCollaborator.UpdatedAt` (a registered table's value type) from the wrong wire key
`updatedAt` to the real deserializer's `lastUpdatedAt`, without bumping the snapshot
version. A pre-fix (v2) snapshot's `updatedAt` data is unrecognized by the new tag and
would silently decode as the zero time on restore.

Found via `pkgs/persistence`'s snapshot-version guard, extended this session
(gopherstack-hjdd) to recursively expand fields of every type reached through a
`store.Register`/`store.New` table registration.

**Proof:** `TestInMemoryBackend_RestoreV2SnapshotDiscarded` (persistence_test.go) builds a
v2-shaped snapshot with an `AgentCollaborator` tagged `updatedAt` and asserts it is absent
(`ErrNotFound`) after restore, not silently decoded with `UpdatedAt` zeroed. Hand-reverted to
version 2: the same test then fails (the record surfaces, `UpdatedAt` silently dropped),
confirming the symptom; restored and `md5sum`-verified byte-identical.

**Gates:** `go build`, `go vet` (default/e2e/integration), `gofmt -l` (clean), `go test -race`
(pass), `golangci-lint run` (0 issues).

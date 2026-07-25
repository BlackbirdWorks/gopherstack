---
service: cleanrooms
sdk_module: aws-sdk-go-v2/service/cleanrooms@v1.45.6   # version audited against
last_audit_commit: HEAD   # this pass
last_audit_date: 2026-07-24
overall: A            # systemic invented-field cleanup + several real state-machine/wire-shape bugs fixed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
families:
  Collaboration: {status: ok, note: "FIXED this pass -- see bugs 1-4: CollaborationIdentifier/memberAbilities were invented output fields (deleted from the wire), auto-membership creation added, DeleteMember/DeleteCollaboration state machines fixed"}
  Membership: {status: ok, note: "FIXED this pass -- MembershipIdentifier/collaborationIdentifier were invented output fields (deleted from the wire); paymentConfiguration (real, required) now always populated with a correct default"}
  ConfiguredTable: {status: ok, note: "FIXED this pass -- ConfiguredTableIdentifier was an invented output field (deleted from the wire); cascade delete of analysis rules on DeleteConfiguredTable re-verified real"}
  ConfiguredTableAssociation: {status: ok, note: "FIXED this pass -- *Identifier output fields deleted from the wire; cascade delete of ctaAnalysisRules on association delete re-verified real"}
  AnalysisTemplate: {status: ok, note: "FIXED this pass -- *Identifier output fields deleted from the wire"}
  Schema/SchemaAnalysisRule: {status: ok, note: "FIXED this pass -- collaborationIdentifier was an invented output field (deleted from the wire, collaborationId added); still no Create path anywhere in this backend (matches real API -- schemas are derived from ConfiguredTable+association state), pre-existing and correctly scoped as always-empty until that projection is implemented; SchemaAnalysisRule's real wire shape is actually a deeper types.AnalysisRule union this backend does not model precisely -- deferred, see gaps (unreachable in practice since schemas are never populated)"}
  ProtectedQuery: {status: ok, note: "FIXED prior pass (stuck-status bug); FIXED this pass -- membershipIdentifier was an invented output field (deleted from the wire)"}
  ProtectedJob: {status: ok, note: "FIXED prior pass (stuck-status bug); FIXED this pass -- membershipIdentifier was an invented output field (deleted from the wire)"}
  PrivacyBudgetTemplate: {status: ok, note: "FIXED this pass -- *Identifier output fields deleted from the wire"}
  PrivacyBudget: {status: partial, note: "ListPrivacyBudgets/ListCollaborationPrivacyBudgets always return an empty list -- correct per parity-principles.md rule 4 (validates parent existence then returns real, if empty, state) since there is no CreatePrivacyBudget op in the real API either (budgets are auto-computed from differential-privacy usage, which this backend does not model); documented as a deferred gap, not a stub"}
  IDMappingTable: {status: ok, note: "FIXED this pass -- *Identifier output fields deleted from the wire; Summary was missing the real inputReferenceConfig field, added"}
  IDNamespaceAssociation: {status: ok, note: "FIXED this pass -- *Identifier output fields deleted from the wire; Summary was missing the real inputReferenceProperties field, added"}
  ConfiguredAudienceModelAssociation: {status: ok, note: "FIXED this pass -- *Identifier output fields deleted from the wire"}
  CollaborationChangeRequest: {status: ok, note: "FIXED this pass -- see bug 5: the entire shape was wrong (type+details input/output that don't exist in the real API; real API uses changes[]/action). Rebuilt against CreateCollaborationChangeRequestInput/UpdateCollaborationChangeRequestInput/types.CollaborationChangeRequest with a real PENDING->APPROVED/DENIED/CANCELLED->COMMITTED/CANCELLED state machine"}
  Tags: {status: ok, note: "CRUD + ARN validation (fixed prior pass) re-verified; no change this pass"}
  RouteMatcher/classifyPath: {status: ok, note: "no change this pass; prior pass's GetCollaborationAnalysisTemplate routing fix re-verified via handler_route_matcher_test.go"}
gaps:
  - "ListPrivacyBudgets / ListCollaborationPrivacyBudgets always return an empty budget list. This matches the real API's contract shape (no CreatePrivacyBudget op exists; budgets are server-computed from differential-privacy query usage against a PrivacyBudgetTemplate) but this backend does not model DP budget consumption, so the list is always empty rather than reflecting real usage. Deferred -- would need a differential-privacy accounting model, out of scope for a wire-parity sweep."
  - "PreviewPrivacyImpact returns a fixed empty aggregationCount shape regardless of input parameters -- real AWS computes an actual privacy-impact estimate. Same DP-accounting gap as above."
  - "Collaboration.Members is kept on the wire (json:\"members\") even though it is not a real field on the real Collaboration/CreateCollaborationOutput/GetCollaborationOutput/UpdateCollaborationOutput shape (confirmed against awsRestjson1_deserializeDocumentCollaboration -- members only come from ListMembers). This is a deliberate exception, not an oversight: Members is the only backing store for ListMembers/DeleteMember and has no separate persisted representation the way tagsByArn has for Tags, so a json:\"-\" tag would silently lose every collaboration's member list across a service restart (store.Table's Snapshot/Restore round-trips through this same struct tag). Real AWS SDK/Terraform clients tolerate the extra key (every deserializer in this service ends its field switch with a default case that discards unrecognized keys), so this trades a harmless wire non-canonicality for correct state persistence. Properly removing it requires moving Members to its own store.Table (like tagsByArn), which is deferred."
  - "CollaborationChangeRequest's `changes` field is stored/returned as a generic []map[string]any pass-through (matching the convention used elsewhere for Policy/TableReference/etc unions) rather than a strongly-typed Change{Specification,SpecificationType,Types} union, and committing a change request does not actually apply the change to the parent collaboration's state (e.g. a MEMBER change's effects are not reflected on Collaboration/Membership). The state machine (PENDING/APPROVED/DENIED/CANCELLED/COMMITTED) is real; the semantic effect of a committed change is not modeled. Deferred."
  - "Collaboration's optional analyticsEngine/dataEncryptionMetadata/allowedResultRegions/autoApprovedChangeTypes/isMetricsEnabled/jobLogStatus fields, Membership's isMetricsEnabled/jobLogStatus/defaultJobResultConfiguration/mlMemberAbilities, ProtectedQuery/Job's differentialPrivacy/receiverConfigurations/queryComputePayerAccountId/jobComputePayerAccountId, AnalysisTemplate's errorMessageConfiguration/sourceMetadata/syntheticDataParameters/validations/isSyntheticData, and ConfiguredTable(Summary)'s selectedAnalysisMethods are real optional SDK fields not modeled by this backend (never populated). None are invented -- they are simply omitted (correct per the JSON protocol: an absent optional field is valid), not stubbed with fake values. Deferred as lower-value completeness work."
deferred:
  - "Schema creation/projection from ConfiguredTable+ConfiguredTableAssociation state (pre-existing gap noted in persistence_test.go; not touched this pass, out of scope)"
  - "SchemaAnalysisRule's real wire shape (types.AnalysisRule, a deeper union) is not modeled precisely; unreachable in practice since schemas are never created (see Schema/SchemaAnalysisRule family note)"
leaks: {status: clean, note: "Handler.StartWorker is a no-op (no goroutines, no timers, no channels); Reset()/Snapshot()/Restore() only touch store.Registry-managed tables and the plain tagsByArn map. No lifecycle to leak. Re-verified this pass; the createMembershipLocked refactor (shared by CreateMembership and CreateCollaboration) still runs entirely under the caller's already-held b.mu write lock with no additional goroutines."}
---

## Notes

Protocol: restjson1. This pass field-diffed every resource struct in `models.go` directly
against the real deserializer functions in
`aws-sdk-go-v2/service/cleanrooms@v1.45.6/deserializers.go`
(`awsRestjson1_deserializeDocument<Type>`), extracting the literal `case "<key>":` list for
every type -- not against this package's own handlers or the prior audit's claims (see bug 1
below: the prior audit's claim that `id`/`collaborationIdentifier` are "both real, AWS emits
both" was itself wrong and is corrected here with direct deserializer evidence).

### Bugs fixed this pass

1. **Systemic invented `*Identifier` output fields across almost every resource type.**
   `Collaboration`, `CollaborationSummary`, `Membership`, `MembershipSummary`,
   `ConfiguredTable(Summary)`, `ConfiguredTableAssociation(Summary)`,
   `ConfiguredTableAnalysisRule`, `AnalysisTemplate(Summary)`, `PrivacyBudgetTemplate(Summary)`,
   `IDMappingTable(Summary)`, `IDNamespaceAssociation(Summary)`,
   `ConfiguredAudienceModelAssociation(Summary)`, `ProtectedQuery(Summary)`,
   `ProtectedJob(Summary)`, and `Schema(Summary/AnalysisRule)` all additionally emitted a
   `collaborationIdentifier`/`membershipIdentifier`/`configuredTableIdentifier`/
   `configuredTableAssociationIdentifier`/`analysisTemplateIdentifier`/
   `privacyBudgetTemplateIdentifier`/`idMappingTableIdentifier`/
   `idNamespaceAssociationIdentifier`/`configuredAudienceModelAssociationIdentifier` key
   duplicating the real `id`/`collaborationId`/`membershipId`/etc key with the identical
   value. None of these `*Identifier` forms exist as *output* fields anywhere in the real
   API -- confirmed by grepping every `awsRestjson1_deserializeDocument*` function's field
   switch in `deserializers.go`: every one of them uses only the short form (`id`,
   `collaborationId`, `membershipId`, `configuredTableId`, ...). `*Identifier` is exclusively
   a *request* parameter name (`GetCollaborationInput.CollaborationIdentifier`, etc, which
   `handler_*.go`'s request DTOs correctly still use -- those were not touched). One
   exception found and preserved: `ConfiguredTableAssociationAnalysisRule`'s real wire key
   really is `membershipIdentifier` (not `membershipId`), confirmed directly against
   `awsRestjson1_deserializeDocumentConfiguredTableAssociationAnalysisRule` -- an intentional
   asymmetry in the real API, not a bug. Also on that same type, `membershipArn` was a fully
   invented field with zero real counterpart (removed). `Collaboration` additionally invented
   a `memberAbilities` field (real AWS has no such field on Collaboration -- abilities are
   per-member, visible only via each member's own entry) and a `tags` field (real AWS returns
   tags only from ListTagsForResource, never embedded in the resource body).
   Fixed by tagging every invented Go field `json:"-"` (kept as internal-only bookkeeping,
   since the identical value is still available on the correctly-tagged sibling field) except
   `Collaboration.Members`, which is a deliberate, documented exception (see gaps) because it
   is the sole backing store for ListMembers/DeleteMember with no alternate persisted
   representation. Locked in by `TestCollaborationWireShape`,
   `TestConfiguredTables_Create`, `TestMemberships_Create`,
   `TestAnalysisTemplateHasIDKeys`, and `TestProtectedQueries_Start` asserting the invented
   keys are absent.

2. **Persistence-safety follow-on from fix 1.** `store.Table`'s Snapshot/Restore
   (`pkgs/store`) round-trips resource state through `encoding/json` using each type's
   *exact same* struct tags used for the HTTP response -- there is no separate
   persistence-only representation in this backend (`store_setup.go`'s own doc comment notes
   this was a deliberate Phase-3.3 design choice). This means every internal read of a
   now-`json:"-"`-tagged `*Identifier` field (composite-key derivation in `store_setup.go`,
   parent lookups like `b.collaborations.Get(mem.CollaborationIdentifier)` in
   `analysis_templates.go`/`privacy_budgets.go`/`id_mapping_tables.go`/
   `id_namespace_associations.go`/`configured_audience_model_associations.go`, cascade-delete
   key construction in `configured_tables.go`/`collaborations.go`, and every
   `toXSummary`/List sort predicate) would silently read an empty string for any object that
   had been through a Restore cycle (a real gopherstack server restart), corrupting parent
   lookups and list ordering with no error raised. Fixed by redirecting every such internal
   read to the correctly-tagged sibling field (`.ID`, `.CollaborationID`, `.MembershipID`,
   `.ConfiguredTableID`) instead of the now-dead `*Identifier` field, across
   `store_setup.go`, `collaborations.go`, `memberships.go`, `configured_tables.go`,
   `configured_table_associations.go`, `analysis_templates.go`, `privacy_budgets.go`,
   `id_mapping_tables.go`, `id_namespace_associations.go`,
   `configured_audience_model_associations.go`, `protected_queries.go`, `protected_jobs.go`,
   and `schemas.go`. `Schema`/`SchemaSummary`/`SchemaAnalysisRule` gained a new
   `CollaborationID string \`json:"collaborationId"\`` field for the same reason (they
   previously had no short-form sibling at all).

3. **`DeleteMember` spliced the member out of `Collaboration.Members` instead of marking
   them `REMOVED`.** Real AWS's `DeleteMember` doc comment: "The removed member is placed in
   the Removed status and can't interact with the collaboration" -- the member entry must
   stay in `ListMembers`/`GetCollaboration.members` afterward with `status: "REMOVED"`, not
   disappear. Fixed; locked in by `TestDeleteMember_MarksRemoved`.

4. **`DeleteCollaboration` never transitioned dependent memberships.**
   `MembershipStatus` documents a real `COLLABORATION_DELETED` enum value specifically for
   this case (in addition to `ACTIVE`/`REMOVED`), but `DeleteCollaboration` previously just
   deleted the collaboration row and left every membership under it silently `ACTIVE`
   forever, with no way for a client to ever observe the collaboration is gone via
   `GetMembership`. Fixed: `DeleteCollaboration` now transitions every `ACTIVE` membership
   under the deleted collaboration to `COLLABORATION_DELETED` (matching real AWS, which
   retains memberships after collaboration deletion for audit/history rather than deleting
   them). Relatedly, `CreateCollaboration` did not auto-create a membership for the creator;
   real AWS does (confirmed by `Collaboration.membershipArn`/`membershipId`'s doc comment:
   "The unique ARN for your membership within the collaboration"), and without it there was
   no membership for `DeleteCollaboration` to ever transition in the common case of a
   collaboration with no explicitly-created memberships. Fixed via a new
   `createMembershipLocked` helper shared by `CreateMembership` and `CreateCollaboration`.
   Locked in by `TestDeleteCollaboration_CascadesMembershipToCollaborationDeleted` and the
   membership-count assertions in `TestCollaborationWireShape`/`TestMemberships_List`.

5. **`CollaborationChangeRequest` was an entirely invented shape.** The previous
   implementation's `CreateCollaborationChangeRequest(collaborationID, type, details)` /
   `UpdateCollaborationChangeRequest(collaborationID, changeRequestID, status)` and the
   `CollaborationChangeRequest{Type, Details, CollaborationArn, ChangeRequestIdentifier}`
   struct do not match the real API at all:
   - Real `CreateCollaborationChangeRequestInput` takes `changes []types.ChangeInput`
     (`{specification, specificationType}` objects), never a free-form `type`+`details` pair.
   - Real `UpdateCollaborationChangeRequestInput` takes `action`
     (`APPROVE`/`DENY`/`CANCEL`/`COMMIT` -- `ChangeRequestAction`), never a raw `status`
     write. The old code let a client set `status` to literally any string with no
     validation or transition logic.
   - Real `types.CollaborationChangeRequest` output keys (confirmed against
     `awsRestjson1_deserializeDocumentCollaborationChangeRequest`) are `approvals`, `changes`,
     `collaborationId`, `createTime`, `id`, `isAutoApproved`, `status`, `updateTime` -- there
     is no `changeRequestIdentifier`, `collaborationIdentifier`, `collaborationArn`, `type`,
     or `details` key.
   Fixed: `Changes []map[string]any` (generic pass-through, matching this service's existing
   convention for other complex unions like `Policy`/`TableReference`) replaces
   `type`+`details`; `IsAutoApproved`/`Approvals`/`CollaborationID`/`ID` added with real
   tags; the invented fields kept as `json:"-"` internal bookkeeping. `Update` now takes
   `action` and applies a real state machine
   (`changeRequestNextStatus`): `PENDING` -> `APPROVE`/`DENY`/`CANCEL` ->
   `APPROVED`/`DENIED`/`CANCELLED`; `APPROVED` -> `COMMIT`/`CANCEL` ->
   `COMMITTED`/`CANCELLED`; any other `(action, currentStatus)` pair now returns
   `ConflictException` (previously any status could be force-set at any time, including
   "reopening" a terminal request). An unrecognized `action` value returns
   `ValidationException`. Locked in by `TestCollaborationChangeRequest_Lifecycle`.

### Wire-shape spot checks (no bug found, recorded so the next audit doesn't re-check)

- `createTime`/`updateTime` are epoch-seconds JSON numbers (`float64`), matching
  `smithytime.ParseEpochSeconds` in `deserializers.go` -- correct as-is, no `awstime.Epoch`
  needed since these are already plain numeric fields serialized by `encoding/json`.
- `UntagResource`'s `tagKeys` arrives as a repeated query parameter (`?tagKeys=a&tagKeys=b`),
  confirmed against `serializers.go`'s `encoder.AddQuery("tagKeys")` binding.
  `handleUntagResource`'s query-param fallback is correct.
- `MemberSummary` (the `ListMembers`/`Collaboration.members` item shape) genuinely requires
  `paymentConfiguration` (confirmed required in `types.MemberSummary`); it was previously
  entirely absent from this backend's `MemberSummary` struct. Added, with a default computed
  per `QueryComputePaymentConfig.IsResponsible`'s documented default ("If the collaboration
  creator hasn't specified anyone as the member paying for query compute costs, then the
  member who can query is the default payer"): `{"queryCompute": {"isResponsible":
  <has CAN_QUERY ability>}}` unless the caller supplied an explicit
  `paymentConfiguration` in the member spec. Same default now applied to
  `Membership.PaymentConfiguration` (also real, required, previously could be emitted
  entirely empty).

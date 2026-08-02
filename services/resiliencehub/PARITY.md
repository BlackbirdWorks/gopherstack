---
# PARITY MANIFEST — IMPLEMENTED THIS PASS.
# services/resiliencehub/ is now built: 63/63 operations routed, real backend
# state, persisted via InMemoryBackend.Snapshot/Restore, wired into cli.go
# (Provider, CLI struct, storeCLINewestHandlers, getMostRecentServiceProviders,
# and wireResourceGroupsTagging as the 32nd tagging-wired service). This
# frontmatter was updated post-implementation; the original pre-implementation
# audit body (Sections 1-4 below) is kept as reference material and remains
# accurate except where "Implementation summary" (bottom of file) notes a
# deviation.
service: resiliencehub
sdk_module: aws-sdk-go-v2/service/resiliencehub@v1.38.3   # now a real go.mod dependency (go get run this pass)
last_audit_commit: 7922e4c4d     # HEAD when the pre-implementation audit was written; this pass built the full service on top of it
last_audit_date: 2026-08-01
# Grade B: every op routed with real state/persistence and real SDK round-trip
# test coverage, but the honest-gap surface is large by the nature of this
# service (an analysis product whose scoring/ML/curated-recommendation
# outputs cannot be derived from the SDK) -- see gaps: below and
# "Implementation summary" for the full list of narrower-than-real-AWS,
# documented behavior.
overall: B
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
# All 63 ops are routed, backed by real state, and persisted. "partial" marks
# operations where the real content is a proprietary scoring/ML/curated-KB
# output this emulator cannot derive from the SDK -- the STATE MACHINE around
# them (real records, real status transitions, real validation) is genuine;
# only the analysis CONTENT is honestly empty/placeholder. Never a stub.
ops:
  AcceptResourceGroupingRecommendations: {wire: ok, errors: ok, state: partial, persist: n/a, note: "grouping.go; every submitted id necessarily fails (no grouping recommendation ever exists to accept) -- real failure, not fabricated success"}
  AddDraftAppVersionResourceMappings: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go; appends to draft AppVersion.ResourceMappings"}
  BatchUpdateRecommendationStatus: {wire: ok, errors: ok, state: partial, persist: n/a, note: "recommendations.go; every entry necessarily fails (no recommendation ever exists)"}
  CreateApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "apps.go; seeds implicit draft AppVersion"}
  CreateAppVersionAppComponent: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go; draft-only; Conflict on duplicate name"}
  CreateAppVersionResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go; draft-only; auto-creates unknown AppComponent names per SDK doc comment"}
  CreateRecommendationTemplate: {wire: ok, errors: ok, state: partial, persist: ok, note: "templates.go; real template record, synthetic TemplatesLocation (no real S3 write), always-empty RecommendationIds/Types"}
  CreateResiliencyPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "policies.go; requires all 4 DisruptionType entries (judgment call, see below)"}
  DeleteApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "apps.go; Conflict while a Pending/InProgress assessment exists unless ForceDelete"}
  DeleteAppAssessment: {wire: ok, errors: ok, state: ok, persist: ok, note: "assessments.go; Conflict while still running"}
  DeleteAppInputSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go; removes the bookkeeping entry only (no per-resource provenance tracking)"}
  DeleteAppVersionAppComponent: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go; Conflict if any resource still assigned"}
  DeleteAppVersionResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go; Conflict unless SourceType is AppTemplate (manually-added)"}
  DeleteRecommendationTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "templates.go; the one op that never returns ConflictException (confirmed, tested)"}
  DeleteResiliencyPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "policies.go; Conflict while any App still bound"}
  DescribeApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "apps.go"}
  DescribeAppAssessment: {wire: ok, errors: ok, state: ok, persist: ok, note: "assessments.go; Summary always nil, ResiliencyScore always scorePlaceholder"}
  DescribeAppVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go; reads any version (draft or published)"}
  DescribeAppVersionAppComponent: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go; reads any version"}
  DescribeAppVersionResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go; one-of resourceName/logicalResourceId/physicalResourceId locator"}
  DescribeAppVersionResourcesResolutionStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go; only the latest resolution per version is kept (documented simplification)"}
  DescribeAppVersionTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go"}
  DescribeDraftAppVersionResourcesImportStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go; draft-only"}
  DescribeMetricsExport: {wire: ok, errors: ok, state: partial, persist: ok, note: "metrics.go; real async record, ExportLocation synthetic (no real S3 write)"}
  DescribeResiliencyPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "policies.go"}
  DescribeResourceGroupingRecommendationTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "grouping.go; real task record and status transition"}
  ImportResourcesToDraftAppVersion: {wire: ok, errors: ok, state: partial, persist: ok, note: "resources.go; records real AppInputSource bookkeeping and transitions Pending->Success, but does not discover real resources from the named sources -- see Implementation summary"}
  ListAlarmRecommendations: {wire: ok, errors: ok, state: partial, persist: n/a, note: "recommendations.go; validates assessmentArn, always empty (no recommendation engine)"}
  ListAppAssessmentComplianceDrifts: {wire: ok, errors: ok, state: partial, persist: n/a, note: "assessments.go; validates assessmentArn, always empty (no drift-detection engine)"}
  ListAppAssessmentResourceDrifts: {wire: ok, errors: ok, state: partial, persist: n/a, note: "assessments.go; same as above"}
  ListAppAssessments: {wire: ok, errors: ok, state: ok, persist: ok, note: "assessments.go; GET, filters + reverseOrder"}
  ListAppComponentCompliances: {wire: ok, errors: ok, state: ok, persist: n/a, note: "assessments.go; real per-component entries using the documented coarse compliance rule (see complianceStatusForPolicy)"}
  ListAppComponentRecommendations: {wire: ok, errors: ok, state: partial, persist: n/a, note: "recommendations.go; always empty"}
  ListAppInputSources: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go"}
  ListApps: {wire: ok, errors: ok, state: ok, persist: ok, note: "apps.go; GET, single-filter-at-a-time"}
  ListAppVersionAppComponents: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go"}
  ListAppVersionResourceMappings: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go"}
  ListAppVersionResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go"}
  ListAppVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go; draft + every published snapshot, [startTime,endTime] filter"}
  ListMetrics: {wire: ok, errors: ok, state: partial, persist: n/a, note: "metrics.go; always empty (no historical metrics store; ResiliencyScore itself is a placeholder)"}
  ListRecommendationTemplates: {wire: ok, errors: ok, state: ok, persist: ok, note: "templates.go; GET, filters + reverseOrder"}
  ListResiliencyPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "policies.go; GET"}
  ListResourceGroupingRecommendations: {wire: ok, errors: ok, state: partial, persist: n/a, note: "grouping.go; GET, always empty (no ML clustering engine)"}
  ListSopRecommendations: {wire: ok, errors: ok, state: partial, persist: n/a, note: "recommendations.go; always empty"}
  ListSuggestedResiliencyPolicies: {wire: ok, errors: ok, state: partial, persist: n/a, note: "policies.go; GET, static 5-tier stand-in table (documented, non-authoritative), NOT the real backend's ResiliencyPolicies table"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a, note: "tagging.go; GET /tags/{resourceArn}, resolves App/Policy/Assessment by ARN marker"}
  ListTestRecommendations: {wire: ok, errors: ok, state: partial, persist: n/a, note: "recommendations.go; always empty"}
  ListUnsupportedAppVersionResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go; real classification against the two closed PhysicalResourceId.Type lists"}
  PublishAppVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go; deep-copy snapshot into a new numbered version, draft continues mutating forward"}
  PutDraftAppVersionTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go; draft-only"}
  RejectResourceGroupingRecommendations: {wire: ok, errors: ok, state: partial, persist: n/a, note: "grouping.go; same honest-failure rationale as Accept"}
  RemoveDraftAppVersionResourceMappings: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go; matches by any of the 6 name-list params"}
  ResolveAppVersionResources: {wire: ok, errors: ok, state: partial, persist: ok, note: "resources.go; real async Pending->Success + real materialization of Resource-type mappings; CfnStack/ResourceGroup/EKS/AppRegistryApp/Terraform mappings are left unresolved -- narrower than the audit's cross-service-resolution recommendation, see Implementation summary"}
  StartAppAssessment: {wire: ok, errors: ok, state: ok, persist: ok, note: "assessments.go; real Pending->InProgress->Success via pkgs/worker, real policy snapshot, Summary always nil, ResiliencyScore always scorePlaceholder"}
  StartMetricsExport: {wire: ok, errors: ok, state: partial, persist: ok, note: "metrics.go; real async record, ExportLocation synthetic"}
  StartResourceGroupingRecommendationTask: {wire: ok, errors: ok, state: partial, persist: ok, note: "grouping.go; real task + real transition, zero recommendations generated"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "tagging.go; POST /tags/{resourceArn}"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "tagging.go; DELETE /tags/{resourceArn}, repeated ?tagKeys= query param"}
  UpdateApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "apps.go; ClearResiliencyPolicyArn handled distinctly from omitted PolicyArn"}
  UpdateAppVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go; draft-only, AdditionalInfo replace"}
  UpdateAppVersionAppComponent: {wire: ok, errors: ok, state: ok, persist: ok, note: "appversions.go; draft-only"}
  UpdateAppVersionResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources.go; draft-only"}
  UpdateResiliencyPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "policies.go; partial-update semantics"}
families:
  route-matcher: {status: ok, note: "handler_routes.go's flat map[string]routeEntry keyed by \"METHOD firstPathSegment\" -- every op but the tags trio is a literal fixed-path POST/GET with no path parameters, so no segment-count branching is needed at all (unlike services/outposts). Split across 5 routesX() builders merged by mergeRoutes to stay under funlen, a lookup-table split not a logic split."}
  tagging: {status: ok, note: "TagResource/UntagResource/ListTagsForResource wired into cli.go's wireResourceGroupsTagging via wireTaggingResilienceHub, the 32nd service. App/ResiliencyPolicy/AppAssessment share one ARN-keyed tag store (tagging.go's resolveTaggableLocked); resourceTypeFromARN derives resiliencehub:app / resiliencehub:resiliency-policy / resiliencehub:app-assessment per-ARN (three-kind case, one more than Outposts' two)."}
gaps:
  - "AssessmentSummary is always nil. Genuinely Bedrock-LLM-backed per the SDK's own doc comment ('available only in the US East (N. Virginia) Region') -- never fabricated, per instruction. Verified by TestStartAppAssessment_ComplianceStatusRule and TestRoundTrip_AssessmentLifecycle asserting Summary is nil."
  - "ResiliencyScore.Score is always the documented placeholder scorePlaceholder=0.0 (consts.go), never a fabricated number. Same treatment for App.ResiliencyScore and AppAssessmentSummary.ResiliencyScore. EstimatedCostTier and Cost are likewise always left empty/nil (undocumented cost-estimation model, same honest-gap posture)."
  - "ComplianceStatus (App/AppAssessment/AppComponentCompliance) follows ONE documented, coarse, non-fabricated rule (assessments.go's complianceStatusForPolicy): MissingPolicy when no ResiliencyPolicy is bound (a real, derivable fact), PolicyMet when one is bound (a documented stand-in, NOT real compliance evaluation -- this backend never checks whether the underlying resources would actually meet the policy's RTO/RPO). DisruptionCompliance's AchievableRpoInSecs/RtoInSecs echo the bound policy's real configured targets; CurrentRpoInSecs/RtoInSecs are documented as assumed equal to the achievable target since no real assessment measures an actual current value."
  - "The four recommendation families (ListAlarmRecommendations/ListSopRecommendations/ListTestRecommendations/ListAppComponentRecommendations) and BatchUpdateRecommendationStatus always return empty/all-failed -- no recommendation-engine content is ever fabricated. CreateRecommendationTemplate produces a real, retrievable template record but TemplatesLocation is a synthetic bucket/prefix string; no S3 object is actually written (services/s3 write-through was flagged by the audit as a valid future enhancement, out of scope this pass)."
  - "The resource-grouping-recommendation family (Start/DescribeResourceGroupingRecommendationTask, ListResourceGroupingRecommendations, Accept/RejectResourceGroupingRecommendations) implements the FULL real task/accept/reject state machine but always completes with zero generated recommendations -- no ML clustering output is ever fabricated."
  - "ResolveAppVersionResources/ImportResourcesToDraftAppVersion: DEVIATION FROM THE AUDIT'S RECOMMENDATION, DOCUMENTED. The audit recommended real cross-service resolution against services/cloudformation, services/eks, and services/resourcegroups (all three confirmed to exist with usable methods: cloudformation.InMemoryBackend.ListStacks/DescribeStack, eks.InMemoryBackend.ListClusters/DescribeCluster, resourcegroups.InMemoryBackend.ListGroups). This pass did NOT wire that cross-service backend access (it would require the same Provider.Init-time BackendsProvider-interface pattern services/cloudformation itself uses to reach other backends, which is a substantial additional wiring surface). Instead: the 'Resource' MappingType (which already carries a caller-supplied PhysicalResourceId) is resolved for real (a genuine pass-through, not fabricated); CfnStack/ResourceGroup/EKS/AppRegistryApp/Terraform mappings are accepted but left unresolved -- no PhysicalResource entries are invented for them. This is a narrower scope than the audit's recommendation, not a silent gap: see Implementation summary below."
  - "AppRegistryApp and Terraform resource-mapping types remain opaque/unresolved regardless of the above -- no services/appregistry package exists in this tree, and Terraform state files are an external S3 concept with no local semantics, exactly as the audit anticipated."
  - "No AWS::ResilienceHub::* CloudFormation resource type exists in services/cloudformation/resources_*.go -- unchanged from the audit, not scoped as parity work."
  - "ListSuggestedResiliencyPolicies' 5-tier RTO/RPO table (policies.go's suggestedPolicyTiers) is a coarse, self-invented halving progression (60s/600s/3600s/86400s/604800s), NOT AWS-published defaults -- documented stand-in per the audit's own recommendation (mirrors services/grafana's ListVersions precedent)."
  - "The AppVersion 'draft' sentinel string (consts.go's draftVersion) is asserted from general product knowledge, not verified against any SDK enum/pattern trait -- exactly the assumption the audit flagged as unconfirmable from the SDK alone."
  - "AssessmentArn's ARN format DEVIATES from the SDK's own literal doc comment on purpose, documented in store.go's AssessmentARN: every AssessmentArn doc comment in this SDK module literally reads 'app-assessment/{app-id}' (same as the audit read it), but reusing the app-id verbatim would make every assessment of the same App share one ARN, which cannot be correct since ListAppAssessments/DescribeAppAssessment/DeleteAppAssessment must address one specific assessment among potentially many. This backend mints a fresh, unique ID per assessment under the app-assessment/ prefix instead -- almost certainly correcting a copy-paste doc-generation artifact in the upstream SDK, not a disagreement with real AWS behavior."
leaks: {status: clean, note: "InMemoryBackend.Reset()/DeleteApp/DeleteResiliencyPolicy/DeleteAppAssessment/DeleteRecommendationTemplate all close their tags.Tags before removal (store.go, apps.go, policies.go, assessments.go, templates.go); Close() stops the worker.Group backing every scheduled assessment/resolution/import/metrics-export/grouping-task transition timer. Verified clean under `go test -race` across 5 consecutive runs."}
---

## Purpose of this document

`services/resiliencehub/` does not exist. This file is a pre-implementation audit:
a complete SDK operation inventory plus a behavioral spec, written so a
follow-up implementation pass does not have to re-derive wire shapes from the
SDK source itself. No `.go` files were touched to produce it. All 63
operation names, all HTTP method/path pairs, and all per-operation exception
sets below were read directly from
`aws-sdk-go-v2/service/resiliencehub@v1.38.3`'s `serializers.go` /
`deserializers.go` / `types/types.go` / `types/enums.go` in the module cache
(resolved via a throwaway `go mod init probe && go get
.../resiliencehub@latest` in the scratch dir — **not** added to this repo's
`go.mod**`).

## 1. Complete SDK operation inventory

**63 operations**, SDK version **`v1.38.3`** (resolved 2026-08-01; this is
whatever `@latest` currently resolves to, not a version pinned by this audit).
This matches the ~63 estimate exactly.

### Protocol and routing shape

`awsRestjson1`. Every operation's path is a **literal, fixed, kebab-case
action path** with **no path parameters** — e.g. `POST /create-app`, `POST
/start-app-assessment` — except the three tag operations, which use
`/tags/{resourceArn}` with the ARN as a single path label. This is simpler
than most REST-JSON services in this tree (no `{id}`-style path segments to
parse for any CRUD op — every identifier, e.g. `AppArn`, `AppVersion`,
travels in the JSON body). Method is `POST` for every op **except** these six,
which are `GET` (confirmed by reading `request.Method = "..."` in each op's
serializer): `ListAppAssessments`, `ListApps`, `ListRecommendationTemplates`,
`ListResiliencyPolicies`, `ListResourceGroupingRecommendations`,
`ListSuggestedResiliencyPolicies`, `ListTagsForResource`. (That's actually
seven — `ListTagsForResource` uses the `/tags/{resourceArn}` path, method
GET.) `TagResource` is `POST /tags/{resourceArn}`, `UntagResource` is `DELETE
/tags/{resourceArn}`.

Full op → method/path table (all 63, extracted from
`serializers.go`'s `httpbinding.SplitURI(...)` + `request.Method = "..."`
literals):

| Operation | Method | Path |
|---|---|---|
| AcceptResourceGroupingRecommendations | POST | /accept-resource-grouping-recommendations |
| AddDraftAppVersionResourceMappings | POST | /add-draft-app-version-resource-mappings |
| BatchUpdateRecommendationStatus | POST | /batch-update-recommendation-status |
| CreateApp | POST | /create-app |
| CreateAppVersionAppComponent | POST | /create-app-version-app-component |
| CreateAppVersionResource | POST | /create-app-version-resource |
| CreateRecommendationTemplate | POST | /create-recommendation-template |
| CreateResiliencyPolicy | POST | /create-resiliency-policy |
| DeleteApp | POST | /delete-app |
| DeleteAppAssessment | POST | /delete-app-assessment |
| DeleteAppInputSource | POST | /delete-app-input-source |
| DeleteAppVersionAppComponent | POST | /delete-app-version-app-component |
| DeleteAppVersionResource | POST | /delete-app-version-resource |
| DeleteRecommendationTemplate | POST | /delete-recommendation-template |
| DeleteResiliencyPolicy | POST | /delete-resiliency-policy |
| DescribeApp | POST | /describe-app |
| DescribeAppAssessment | POST | /describe-app-assessment |
| DescribeAppVersion | POST | /describe-app-version |
| DescribeAppVersionAppComponent | POST | /describe-app-version-app-component |
| DescribeAppVersionResource | POST | /describe-app-version-resource |
| DescribeAppVersionResourcesResolutionStatus | POST | /describe-app-version-resources-resolution-status |
| DescribeAppVersionTemplate | POST | /describe-app-version-template |
| DescribeDraftAppVersionResourcesImportStatus | POST | /describe-draft-app-version-resources-import-status |
| DescribeMetricsExport | POST | /describe-metrics-export |
| DescribeResiliencyPolicy | POST | /describe-resiliency-policy |
| DescribeResourceGroupingRecommendationTask | POST | /describe-resource-grouping-recommendation-task |
| ImportResourcesToDraftAppVersion | POST | /import-resources-to-draft-app-version |
| ListAlarmRecommendations | POST | /list-alarm-recommendations |
| ListAppAssessmentComplianceDrifts | POST | /list-app-assessment-compliance-drifts |
| ListAppAssessmentResourceDrifts | POST | /list-app-assessment-resource-drifts |
| ListAppAssessments | GET | /list-app-assessments |
| ListAppComponentCompliances | POST | /list-app-component-compliances |
| ListAppComponentRecommendations | POST | /list-app-component-recommendations |
| ListAppInputSources | POST | /list-app-input-sources |
| ListApps | GET | /list-apps |
| ListAppVersionAppComponents | POST | /list-app-version-app-components |
| ListAppVersionResourceMappings | POST | /list-app-version-resource-mappings |
| ListAppVersionResources | POST | /list-app-version-resources |
| ListAppVersions | POST | /list-app-versions |
| ListMetrics | POST | /list-metrics |
| ListRecommendationTemplates | GET | /list-recommendation-templates |
| ListResiliencyPolicies | GET | /list-resiliency-policies |
| ListResourceGroupingRecommendations | GET | /list-resource-grouping-recommendations |
| ListSopRecommendations | POST | /list-sop-recommendations |
| ListSuggestedResiliencyPolicies | GET | /list-suggested-resiliency-policies |
| ListTagsForResource | GET | /tags/{resourceArn} |
| ListTestRecommendations | POST | /list-test-recommendations |
| ListUnsupportedAppVersionResources | POST | /list-unsupported-app-version-resources |
| PublishAppVersion | POST | /publish-app-version |
| PutDraftAppVersionTemplate | POST | /put-draft-app-version-template |
| RejectResourceGroupingRecommendations | POST | /reject-resource-grouping-recommendations |
| RemoveDraftAppVersionResourceMappings | POST | /remove-draft-app-version-resource-mappings |
| ResolveAppVersionResources | POST | /resolve-app-version-resources |
| StartAppAssessment | POST | /start-app-assessment |
| StartMetricsExport | POST | /start-metrics-export |
| StartResourceGroupingRecommendationTask | POST | /start-resource-grouping-recommendation-task |
| TagResource | POST | /tags/{resourceArn} |
| UntagResource | DELETE | /tags/{resourceArn} |
| UpdateApp | POST | /update-app |
| UpdateAppVersion | POST | /update-app-version |
| UpdateAppVersionAppComponent | POST | /update-app-version-app-component |
| UpdateAppVersionResource | POST | /update-app-version-resource |
| UpdateResiliencyPolicy | POST | /update-resiliency-policy |

### Errors — 7 shared exception types, per-op subsets

There are exactly 7 modeled exception shapes, all declared once in
`types/errors.go` and reused across ops (confirmed by reading every op's own
`awsRestjson1_deserializeOpError<Op>` `switch { case strings.EqualFold(...)
}` block in `deserializers.go`, not just assumed from the shared type
declarations):

- `AccessDeniedException` (client fault) — no extra fields beyond `Message`.
- `ConflictException` (client fault) — has `ResourceId`/`ResourceType` fields.
- `InternalServerException` (server fault) — no extra fields.
- `ResourceNotFoundException` (client fault) — has `ResourceId`/`ResourceType` fields.
- `ServiceQuotaExceededException` (client fault) — no extra fields.
- `ThrottlingException` (client fault) — has a `RetryAfterSeconds *int32` field.
- `ValidationException` (client fault) — no extra fields.

Every one of the 63 ops accepts a different subset. Patterns worth noting for
the implementer (verified per-op, not assumed):

- **`ConflictException`** is accepted only by mutating ops that touch app/
  version/resiliency-policy state undergoing a lifecycle transition —
  `Create*`, `Delete*`, `Update*`, `AddDraftAppVersionResourceMappings`,
  `RemoveDraftAppVersionResourceMappings`, `ImportResourcesToDraftAppVersion`,
  `PublishAppVersion`, `PutDraftAppVersionTemplate`,
  `ResolveAppVersionResources`, `StartAppAssessment`,
  `StartMetricsExport`, `StartResourceGroupingRecommendationTask`,
  `ListAppVersionAppComponents`, `ListAppVersionResources`,
  `ListSopRecommendations`, `ListTestRecommendations`,
  `ListUnsupportedAppVersionResources`, `DescribeAppVersionAppComponent`,
  `DescribeAppVersionResource`. The one outlier within that mutating group is
  `DeleteRecommendationTemplate`, which does **not** accept
  `ConflictException` (only `AccessDeniedException`/`InternalServerException`/
  `ResourceNotFoundException`/`ThrottlingException`/`ValidationException` —
  confirmed by re-reading its deserializer specifically). Plain `Describe*`
  read ops (`DescribeApp`, `DescribeAppAssessment`, `DescribeAppVersion`,
  `DescribeResiliencyPolicy`, etc.) and all the plain `List*` ops do **not**
  accept `ConflictException` at all.
- **`ServiceQuotaExceededException`** is accepted only by the true `Create*`
  ops that allocate a new top-level resource — `AddDraftAppVersionResourceMappings`
  (adds mapping rows), `CreateApp`, `CreateAppVersionAppComponent`,
  `CreateAppVersionResource`, `CreateRecommendationTemplate`,
  `CreateResiliencyPolicy`, `ImportResourcesToDraftAppVersion`,
  `StartAppAssessment`, `StartMetricsExport`, `UpdateAppVersionResource` — no
  quota model exists in this repo to trigger it (same honest gap as grafana's).
- **`ResourceNotFoundException`** is accepted by almost every op that takes an
  `AppArn`/`PolicyArn`/`AssessmentArn` but is notably absent from
  `CreateResiliencyPolicy` (nothing to look up yet, only
  `AccessDeniedException`/`ConflictException`/`InternalServerException`/
  `ServiceQuotaExceededException`/`ThrottlingException`/`ValidationException`),
  and from the account-wide list reads `ListAppAssessmentComplianceDrifts`,
  `ListAppAssessmentResourceDrifts`, `ListApps`, `ListMetrics`, and
  `ListRecommendationTemplates` (each of those five carries only the
  "default four": `AccessDeniedException`/`InternalServerException`/
  `ThrottlingException`/`ValidationException`). `ListAppVersions`'s error set
  is unusually thin in the other direction — it keeps `ResourceNotFoundException`
  but is the one op in the whole service that drops `ThrottlingException`
  (only `AccessDeniedException`/`InternalServerException`/
  `ResourceNotFoundException`/`ValidationException`, confirmed by re-reading
  its deserializer).
- **`AccessDeniedException`, `InternalServerException`,
  `ThrottlingException`, `ValidationException`** are accepted by essentially
  every operation (the "default four"), with `ListAppVersions` the one
  exception that drops `ThrottlingException` from its set.

### Shared/core types (from `types/types.go`, field lists trimmed to essentials)

- **`App`**: `AppArn*` `CreationTime*` `Name*` (required) +
  `AssessmentSchedule` (`AppAssessmentScheduleType`: `Disabled`|`Daily`),
  `AwsApplicationArn`, `ComplianceStatus` (`AppComplianceStatusType`:
  `PolicyBreached`|`PolicyMet`|`NotAssessed`|`ChangesDetected`|`NotApplicable`|`MissingPolicy`),
  `Description`, `DriftStatus` (`AppDriftStatusType`:
  `NotChecked`|`NotDetected`|`Detected`), `EventSubscriptions
  []EventSubscription`, `LastAppComplianceEvaluationTime`,
  `LastDriftEvaluationTime`, `LastResiliencyScoreEvaluationTime`,
  `PermissionModel *PermissionModel`, `PolicyArn`, `ResiliencyScore float64`,
  `RpoInSecs *int32`, `RtoInSecs *int32`, `Status` (`AppStatusType`:
  `Active`|`Deleting` — only two values, no `Creating`/`Updating`
  transitional state on the app itself), `Tags map[string]string`.
  ARN format per the SDK's own doc comment (repeated verbatim on every
  `AppArn` field across the module):
  `arn:{partition}:resiliencehub:{region}:{account}:app/{app-id}`.
- **`ResiliencyPolicy`**: `PolicyArn`
  (`arn:{partition}:resiliencehub:{region}:{account}:resiliency-policy/{policy-id}`),
  `PolicyName`, `Tier` (`ResiliencyPolicyTier`: `MissionCritical`|`Critical`|`Important`|`CoreServices`|`NonCritical`|`NotApplicable`),
  `Policy map[string]FailurePolicy` — keyed by `DisruptionType` string value
  (`"Software"`|`"Hardware"`|`"AZ"`|`"Region"`, from `DisruptionType` enum),
  each `FailurePolicy{RtoInSecs int32, RpoInSecs int32}` (both required, no
  pointer). `DataLocationConstraint`, `EstimatedCostTier`,
  `PolicyDescription`, `Tags`, `CreationTime`.
- **`AppAssessment`**: `AssessmentArn*`
  (`arn:{partition}:resiliencehub:{region}:{account}:app-assessment/{app-id}` —
  note this shares the app's `{app-id}` resource segment per the SDK doc
  comment, not a separate assessment-id path segment), `AssessmentStatus*`
  (`AssessmentStatus`: `Pending`|`InProgress`|`Failed`|`Success`),
  `Invoker*` (`AssessmentInvoker`: `User`|`System`), plus `AppArn`,
  `AppVersion`, `AssessmentName`, `Compliance
  map[string]DisruptionCompliance` (keyed by `DisruptionType`),
  `ComplianceStatus` (`ComplianceStatus`:
  `PolicyBreached`|`PolicyMet`|`NotApplicable`|`MissingPolicy`), `Cost *Cost`,
  `DriftStatus` (`DriftStatus`: `NotChecked`|`NotDetected`|`Detected`),
  `EndTime`, `Message`, `Policy *ResiliencyPolicy` (snapshotted at assessment
  time), `ResiliencyScore *ResiliencyScore`, `ResourceErrorsDetails`,
  `StartTime`, **`Summary *AssessmentSummary`** (see fabrication-risk callout
  below), `Tags`, `VersionName`.
- **`DisruptionCompliance`**: `ComplianceStatus*` +
  `AchievableRpoInSecs/RtoInSecs int32`, `CurrentRpoInSecs/RtoInSecs int32`,
  `Message`, `Rpo/RtoDescription`, `Rpo/RtoReferenceId`.
- **`ResiliencyScore`**: `DisruptionScore map[string]float64*`, `Score
  float64*`, `ComponentScore map[string]ScoringComponentResiliencyScore`.
- **`ResourceMapping`**: `MappingType*` (`ResourceMappingType`:
  `CfnStack`|`Resource`|`AppRegistryApp`|`ResourceGroup`|`Terraform`|`EKS`),
  `PhysicalResourceId*`, plus one of `AppRegistryAppName`, `EksSourceName`
  (format `"eks-cluster/namespace"`), `LogicalStackName`, `ResourceGroupName`,
  `ResourceName`, `TerraformSourceName` depending on `MappingType`.
- **`PhysicalResourceId`**: `Identifier*`, `Type*` (`PhysicalIdentifierType`:
  `Arn`|`Native` — `Arn` covers ECS/EFS/ELBv2/Lambda/SNS; `Native` covers a
  long fixed list including API Gateway, ASG, DocDB, DynamoDB, EC2 instance/
  fleet/NAT/volume, classic ELB, RDS, Route53 RecordSet, S3 bucket, SQS —
  full list is in the doc comment at `types/types.go:1150`), `AwsAccountId`,
  `AwsRegion`.
- **`PhysicalResource`**: `LogicalResourceId*`, `PhysicalResourceId*`,
  `ResourceType*string` (a raw CFN-style type string, e.g.
  `"AWS::EC2::Instance"`), plus `AdditionalInfo map[string][]string` (a
  documented special key `"failover-regions"` with JSON-string value),
  `AppComponents []AppComponent`, `Excluded *bool`, `ParentResourceName`,
  `ResourceName`, `SourceType` (`ResourceSourceType`:
  `AppTemplate`|`Discovered`).
- **`RecommendationTemplate`**: `AssessmentArn*`, `Format*`
  (`TemplateFormat`: CfnJson/CfnYaml per doc comment), `Name*`,
  `RecommendationTemplateArn*`, `RecommendationTypes*
  []RenderRecommendationType` (Alarm/Sop/Test per doc comment), `Status*`
  (`RecommendationTemplateStatus`: `Pending`|`InProgress`|`Failed`|`Success`),
  plus `AppArn`, `EndTime`/`StartTime`, `Message`, `NeedsReplacements *bool`,
  `RecommendationIds []string`, `TemplatesLocation *S3Location{Bucket,
  Prefix}` — **the generated template is an actual CFN JSON/YAML file
  written to S3**, not returned inline.
- **`AlarmRecommendation`/`SopRecommendation`/`TestRecommendation`/
  `ComponentRecommendation`**: all share `RecommendationStatus`
  (`RecommendationStatus`: `Implemented`|`Inactive`|`NotImplemented`|`Excluded`,
  except `ComponentRecommendation` which uses the separate
  `RecommendationComplianceStatus` enum), a `ReferenceId`/`RecommendationId`
  pair, `AppComponentName`(s), `Items []RecommendationItem`, and
  free-text `Description`/`Name`/`Prerequisite`. `TestRecommendation`
  additionally carries `Risk` (`TestRisk`) and `Type` (`TestType`).
- **Async-status enums all follow the identical 4-value shape** `Pending` →
  `InProgress` → (`Failed` | `Success`) — confirmed identical string values
  across `ResourceResolutionStatusType`, `ResourceImportStatusType`,
  `MetricsExportStatusType`, `RecommendationTemplateStatus`,
  `ResourcesGroupingRecGenStatusType`. `AssessmentStatus` uses the same 4
  values too (`Pending`/`InProgress`/`Failed`/`Success`). This is a strong,
  consistent signal for how to build one generic "async task" helper in the
  implementation rather than five bespoke state machines.

## 2. Missing simulated functionality (the real emulation work)

Resilience Hub's whole product is "run an assessment, produce a score and
recommendations" — the CRUD parts (App/ResiliencyPolicy/AppVersion) are
straightforward, but the assessment/recommendation engine is where a naive
implementation becomes a shell.

### App lifecycle and app versions

- `CreateApp` creates the app AND an implicit initial **draft** app version.
  `AppVersion` is a plain `*string` on the wire (not an int, despite
  `AppVersionSummary.Identifier` separately being an `*int64`) — per AWS's
  published docs the draft version's string value is the literal `"draft"`
  and published versions are numeric strings (`"1"`, `"2"`, ...); **this
  convention is not encoded anywhere in the Go SDK types themselves** (no
  enum, no pattern trait) — I could not confirm it from the SDK alone, only
  from general knowledge of the product's documented behavior. The
  implementer should treat "draft" as the working/mutable version and
  `PublishAppVersion` as the operation that snapshots the current draft into
  a new immutable numbered version (`PublishAppVersionOutput.Identifier
  *int64` + `AppVersion *string` + optional `VersionName`).
- All the `*AppVersion*` mutation ops (`AddDraftAppVersionResourceMappings`,
  `RemoveDraftAppVersionResourceMappings`, `CreateAppVersionResource`,
  `UpdateAppVersionResource`, `DeleteAppVersionResource`,
  `CreateAppVersionAppComponent`, `UpdateAppVersionAppComponent`,
  `DeleteAppVersionAppComponent`, `PutDraftAppVersionTemplate`,
  `ImportResourcesToDraftAppVersion`) operate on the draft only — this is a
  real state-machine invariant to enforce (reject mutations against a
  published, numbered version) and is honestly simulatable: it's pure CRUD
  bookkeeping, no proprietary model involved.
- `UpdateApp`'s `ClearResiliencyPolicyArn *bool` flag needs real handling
  (distinct from simply not sending `PolicyArn`) — a common wire trap: `nil`
  means "leave alone", `PolicyArn: nil` explicit clear needs the separate
  bool since there's no way to distinguish "omitted" from "explicit empty
  string" for a `*string` in JSON the way a bool flag can.

### Resiliency policies

- `Policy map[string]FailurePolicy` keyed by `DisruptionType` string
  (`Software`/`Hardware`/`AZ`/`Region`) each with plain `int32` `RtoInSecs`/
  `RpoInSecs` (no pointers — always present, so a policy needs an entry per
  disruption type or the caller gets a `ValidationException`, though the
  exact required-keys rule is not encoded in the Go types and would need to
  be inferred/decided by the implementer). `Tier` is an independent
  classification (`MissionCritical`..`NonCritical`) that AWS's real console
  auto-suggests defaults for based on tier via
  `ListSuggestedResiliencyPolicies` — this list of suggested per-tier
  RTO/RPO defaults is exactly the kind of "operational data not in the SDK"
  gap flagged below (like grafana's static version list), not fabricable
  with confidence but a reasonable static table is a defensible stand-in if
  clearly documented as such.
- Binding: `App.PolicyArn` — set at `CreateApp`/`UpdateApp` time. This part
  is simple, referential-integrity-only work (does the policy exist, is it
  in the same account/region).

### The assessment state machine — the central honesty question

`StartAppAssessment` takes `AppArn`+`AppVersion`+`AssessmentName` and
(per its doc comment, not fully quoted here but consistent with the field
shapes) runs **asynchronously**. Real states, in order:
`Pending` → `InProgress` → `Failed` | `Success` (`AssessmentStatus` enum,
confirmed 4 values, no others). `DescribeAppAssessment` polls this.
`DeleteAppAssessment` removes a completed/failed one.

What a **plausible-but-honest** simulated assessment can do:
- Real state transitions (Pending→InProgress→Success on a timer, following
  the `services/eks` `scheduleClusterActivation` / grafana's
  `CreateWorkspace` pattern already used in this repo via `pkgs/worker`).
- `Policy *ResiliencyPolicy` — an honest **snapshot** of whatever policy is
  bound to the app at assessment time (this is just data-copying, not
  analysis).
- `ResourceErrorsDetails` — real if resource resolution genuinely failed
  (e.g. a mapped CloudFormation stack doesn't exist in
  `services/cloudformation`).
- A binary/coarse compliance signal per `DisruptionType`
  (`PolicyMet`/`PolicyBreached`) IF the implementer picks one clear, stated
  rule (e.g. "every mapped resource type in `PhysicalIdentifierType`'s native
  list is considered inherently multi-AZ-capable, therefore compliant" or,
  more honestly, "always PolicyMet since there is no real analysis" — either
  is defensible **as long as the rule is documented as a stand-in, not
  presented as real analysis**).

What **cannot be honestly simulated** without fabrication:
- **`ResiliencyScore.Score`/`DisruptionScore`/`ComponentScore`** — these are
  outputs of AWS's proprietary scoring model that weighs resource redundancy,
  failover configuration, backup posture, etc. across the actual resource
  graph. There is no formula in the SDK, and inventing a numeric score
  (however plausible-looking) that claims to reflect a customer's real
  resiliency posture is fabrication of a "finding" a user could act on.
  **Recommendation: return a single fixed/deterministic placeholder value
  (e.g. always 0 or always a policy-derived pass/fail boundary) and document
  loudly in this service's own code and PARITY.md that it is NOT a real
  score** — same posture grafana's audit took with `ListVersions`' static
  catalog, but higher stakes here since a score reads as authoritative
  analysis, not a version list.
- **`AssessmentSummary.Summary` and `.RiskRecommendations`** — the SDK's own
  doc comment ties this explicitly to a specific AWS region
  ("available only in the US East (N. Virginia) Region"), which is the
  signature of a feature backed by a specific hosted Bedrock model
  deployment. Fabricating LLM-quality natural-language risk summaries would
  be actively deceptive (a user could mistake generated prose for a real
  analysis of their infrastructure). **Recommendation: leave this field nil/
  omitted always — do not attempt to synthesize text here even with a
  template.** This is the single highest-risk fabrication surface in the
  whole service.
- **Per-AppComponent `ConfigRecommendation`/`ComponentRecommendation`
  content, and all four recommendation families (SOP/alarm/test/component)**
  — see next subsection.

### Resource mapping / draft resources and `ResolveAppVersionResources`

- `ImportResourcesToDraftAppVersion` accepts `SourceArns []string` (arbitrary
  ARNs), `EksSources []EksSource`, `TerraformSources []TerraformSource`
  (S3 state file URLs), and an `ImportStrategy`. This is asynchronous too —
  `DescribeDraftAppVersionResourcesImportStatus` polls
  `ResourceImportStatusType` (`Pending`/`InProgress`/`Failed`/`Success`).
- `ResolveAppVersionResources` is a **separate** async op
  (`ResourceResolutionStatusType`, same 4-value shape,
  `DescribeAppVersionResourcesResolutionStatus` polls it) that resolves
  logical→physical resource mappings — i.e., turns the `ResourceMapping`
  records (CfnStack/ResourceGroup/Terraform/EKS/AppRegistryApp/Resource) into
  concrete `PhysicalResource` entries.
- **This part is honestly simulatable against real gopherstack backend
  state** for the mapping types this tree actually has backends for:
  `CfnStack` → `services/cloudformation` (`InMemoryBackend.ListStacks`,
  `services/cloudformation/stack_lifecycle.go:37`, and its resource
  inventory — real stack resources could genuinely populate
  `PhysicalResource` entries with real ARNs/types), `ResourceGroup` →
  `services/resourcegroups` (`InMemoryBackend.ListGroups`,
  `services/resourcegroups/groups.go:469`, whose member-resource queries
  already exist per its own `handler_resources.go`), `EKS` →
  `services/eks` (`InMemoryBackend.ListClusters`,
  `services/eks/clusters.go:186`). `AppRegistryApp` and `Terraform` mapping
  types have **no backing service in this tree** (no appregistry package;
  Terraform state files are an external S3 concept with no local semantics)
  — those would have to remain opaque/accepted-but-unresolved, which is an
  honest gap, not a stub, as long as it's declared.
- Doing this properly (real cross-service resolution instead of invented
  resource lists) is real, valuable work and is the single best "genuinely
  emulated, not fabricated" investment this service can make — it's ordinary
  cross-service plumbing, not analysis-model fabrication.

### Recommendation families (SOP / alarm / test / component)

All four (`ListSopRecommendations`, `ListAlarmRecommendations`,
`ListTestRecommendations`, `ListAppComponentRecommendations`) are the
*outputs* of AWS's proprietary resiliency-analysis engine, mapping specific
resource configurations (e.g. "this RDS instance has no read replica") to a
curated library of standard-operating-procedure/alarm/test recommendations.
**None of this content (the recommendation text, the specific alarm
thresholds, the SOP steps) is derivable from the SDK** — it lives in AWS's
internal knowledge base. Returning **zero recommendations always** (an
honestly empty list, clearly documented as "recommendation engine not
simulated") is more honest than inventing plausible-sounding SOP/alarm
templates that don't correspond to anything real. `CreateRecommendationTemplate`
(which packages accepted recommendations into a downloadable CFN template
written to S3 — `RecommendationTemplate.TemplatesLocation *S3Location`) is
mechanically simulatable (this repo has `services/s3`, so a real file could
be written there) but only meaningful once there are real recommendations to
package — with zero recommendations, this becomes "generate an empty/trivial
CFN template", which is at least not a lie.

### Resource-grouping recommendations (a fifth, separate ML feature)

`StartResourceGroupingRecommendationTask` /
`DescribeResourceGroupingRecommendationTask` /
`ListResourceGroupingRecommendations` /
`Accept/RejectResourceGroupingRecommendations` is a distinct feature: AWS
suggests how to cluster ungrouped resources into new `AppComponent`s
(`GroupingRecommendation.ConfidenceLevel`,
`.Score float64`, `.RecommendationReasons []string`). This is **also** an ML
output with no public derivation rule — same fabrication risk as the main
recommendation families. Honest option: implement the full accept/reject
state machine (`GroupingRecommendationStatusType`:
`Accepted`|`Rejected`|`PendingDecision`) but always return **zero** generated
recommendations from the `Start`/task, so there's nothing to fabricate
confidence scores for.

## 3. Cross-service wiring needed

### Tagging (`resourcegroupstaggingapi`)

Yes — `TagResource`/`UntagResource`/`ListTagsForResource` are real ops
(`/tags/{resourceArn}`, confirmed above), so this belongs in
`wireResourceGroupsTagging` in `cli.go` (currently wires 30 services,
`cli.go:5348`, most recently `wireTaggingGrafana(bk, byName["Grafana"])` at
`cli.go:5397`). Taggable resource types per the SDK's own doc comments
(repeated verbatim on every ARN field in `types/types.go`): apps
(`arn:{partition}:resiliencehub:{region}:{account}:app/{app-id}`),
resiliency policies (`.../resiliency-policy/{policy-id}`), and app
assessments (`.../app-assessment/{app-id}`) all carry `Tags
map[string]string` fields and would need their own
`wireTaggingResilienceHub`-style function following the `wireTaggingGrafana`
model — multiple ARN resource kinds under one service ARN namespace
(`resiliencehub`), same shape as DocDB/Neptune/RDS all sharing `"rds"`
(`cli.go:5375-5379`). **ARN namespace is `resiliencehub` itself** (not a
divergent name like `stepfunctions`→`states` or `efs`→`elasticfilesystem`) —
this is directly readable from the SDK's own generated doc comments on every
ARN-bearing field, not inferred.

### Resources this service assesses

Grepped/read directly, with file:line:

- **CloudFormation** — exists: `services/cloudformation/stack_lifecycle.go:37`
  (`InMemoryBackend.ListStacks`), `services/cloudformation/store.go:150`
  (`InMemoryBackend` struct), `services/cloudformation/persistence.go:54`
  (`Snapshot`). A `CfnStack` resource mapping could genuinely resolve against
  this.
- **EKS clusters** — exists: `services/eks/clusters.go:186`
  (`InMemoryBackend.ListClusters() []string`),
  `services/eks/store.go:20` (`InMemoryBackend` struct). Real `EksSource`
  resolution is feasible.
- **Resource Groups** — exists: `services/resourcegroups/groups.go:469`
  (`InMemoryBackend.ListGroups`), `services/resourcegroups/store.go:55`
  (`InMemoryBackend` struct), plus `handler_resources.go` for group-member
  resource queries. Real `ResourceGroup` mapping resolution is feasible.
- **AppRegistry** — **does not exist** in this tree (searched for
  `appregistry`/`servicecatalogappregistry` directories under `services/` —
  zero hits). `AppRegistryApp` resource mapping type and
  `App.AwsApplicationArn`/`ListApps.AwsApplicationArn` filter cannot resolve
  against a real backend; must remain an opaque accepted string, which is an
  honest gap, not something to fake.
- **The underlying EC2/RDS/ELB/DynamoDB/S3 resources** referenced by
  `PhysicalResourceId` — all exist in this tree (`services/ec2`,
  `services/rds`, `services/elb`+`services/elbv2`, `services/dynamodb`,
  `services/s3`), so a `Resource`-type mapping (`ResourceMappingType`:
  `"Resource"`) could genuinely validate/resolve a physical resource against
  real backend state instead of accepting an arbitrary string+ARN pair
  unchecked. This is real, valuable, non-fabricated work: cross-service
  existence validation, exactly the kind of thing grafana's audit flagged as
  a legitimate future improvement (not required for a first pass, but
  feasible and honest).
- **Terraform state files** — no local semantics possible (an external S3
  object gopherstack doesn't parse); an honest gap regardless of whether
  `services/s3` exists.

### CloudFormation resource type

No `AWS::ResilienceHub::*` resource type exists in
`services/cloudformation/resources_*.go` (grepped all 71
`resources_*.go` files case-insensitively for "resiliencehub" — zero
matches). This would be a legitimate, real gap to report if this service
were meant to be provisionable via CloudFormation stacks in this emulator —
confirmed absent, not silently skipped.

## 4. Honest gap list (see frontmatter `gaps:` for the machine-readable version)

1. The service doesn't exist yet — this whole document is the spec, not an
   audit of running code.
2. `AssessmentSummary` (Bedrock-backed natural-language risk summary) should
   never be fabricated — always nil/omitted, not templated text.
3. `ResiliencyScore` and per-disruption `ComplianceStatus` reflect a
   proprietary scoring model with no derivable formula — any implementation
   must clearly document its number as a placeholder, not real analysis.
4. All four recommendation families (SOP/alarm/test/component) and the
   resource-grouping-recommendation feature are ML/curated-knowledge-base
   outputs with no public derivation — the honest default is an empty list,
   not fabricated recommendations.
5. AppRegistry and Terraform-state-file resource mapping types have no
   backing service in this tree and must remain unresolved opaque
   references.
6. No `AWS::ResilienceHub::*` CloudFormation resource type exists in this
   emulator.
7. `ListSuggestedResiliencyPolicies`' per-tier RTO/RPO defaults are
   operational data not encoded in the SDK — a static table is a defensible
   stand-in (per grafana's `ListVersions` precedent) but is not the real,
   possibly-changing AWS-maintained defaults.
8. The `AppVersion` "draft" sentinel string convention could not be
   confirmed from the SDK types themselves (no enum/pattern trait) — it is
   asserted here from general knowledge of the documented product behavior,
   not verified against generated code, and should be flagged as such if the
   implementer relies on it.

## Notes for the implementer

- Every async status family (`AssessmentStatus`,
  `ResourceResolutionStatusType`, `ResourceImportStatusType`,
  `MetricsExportStatusType`, `RecommendationTemplateStatus`,
  `ResourcesGroupingRecGenStatusType`) uses the identical
  `Pending`/`InProgress`/`Failed`/`Success` (or the app-assessment's own
  4-value `AssessmentStatus`) shape — build one generic timer-driven
  transition helper (following `pkgs/worker`, per grafana's
  `scheduleClusterActivation` precedent) and reuse it five times rather than
  five bespoke state machines.
- `FailurePolicy.RtoInSecs`/`RpoInSecs` are plain `int32` (not pointers) —
  always present on the wire, unlike most numeric fields elsewhere in this
  service.
- `PhysicalResourceId.Type` (`Arn`|`Native`) determines which fixed resource-
  type list applies (see `types/types.go:1150`'s doc comment for the exact
  two lists) — worth a lookup table keyed by `ResourceType` string in the
  implementation to validate/route resolution against the right gopherstack
  backend.

## Implementation summary (this pass)

All 63 operations are implemented with real backend state (no stubs): App
lifecycle with draft/published `AppVersion` bookkeeping (components,
resources, resource mappings, template body, input sources), `PublishAppVersion`
deep-copy snapshotting, ResiliencyPolicy CRUD with policy-to-app binding,
AppAssessment as a real state machine (`Pending`->`InProgress`->`Success` via
`pkgs/worker`, mirroring `services/outposts`/`services/grafana`'s identical
async-transition pattern), RecommendationTemplate CRUD, the full resource-
grouping-recommendation task/accept/reject state machine, MetricsExport, and
full tag support for App/ResiliencyPolicy/AppAssessment wired into
`resourcegroupstaggingapi` (`cli.go`'s `wireTaggingResilienceHub`, the 32nd
tagging-wired service). Per the honest-gap posture this service's whole
product category demands (see `gaps:` above), every proprietary
scoring/ML/curated-recommendation output is either a documented placeholder
(`scorePlaceholder`) or an honestly empty list/failure — never fabricated.

**File layout**: `models.go` (stored-state types) / `consts.go` (wire enum
values, the two closed `PhysicalResourceId.Type` lists, `scorePlaceholder`,
`draftVersion`) / `errors.go` / `store.go` + `store_setup.go`
(`InMemoryBackend`, one coarse `lockmetrics.RWMutex` — `StartAppAssessment`
reads an App plus its bound ResiliencyPolicy and writes an AppAssessment,
`PublishAppVersion` deep-copies a draft embedded on App, `TagResource`
resolves an ARN into whichever of App/ResiliencyPolicy/AppAssessment it
names) / `wire.go` + `wire_convert.go` (JSON wire shapes — lowerCamel
throughout, confirmed by reading `serializers.go`'s own `object.Key(...)`
literals, unlike `services/outposts`' PascalCase — and their conversion
to/from stored state) / `apps.go` / `appversions.go` / `resources.go`
(physical resources, resource mappings, resolution, import, input sources)
/ `policies.go` / `assessments.go` / `recommendations.go` / `templates.go` /
`grouping.go` / `metrics.go` / `tagging.go` (backend logic) / `handler.go` +
`handler_routes.go` + one `handler_<family>.go` per operation family (HTTP
routing/dispatch) / `persistence.go` / `provider.go`.

**Tests**: `sdk_completeness_test.go` (all 63 ops) plus real SDK round-trip
tests (`sdk_roundtrip_helper_test.go`/`sdk_roundtrip_test.go`, following
`services/outposts`/`services/grafana`'s identical pattern — the genuine AWS
SDK client against an `httptest` server, not ad-hoc JSON assertions) covering
App/ResiliencyPolicy/AppVersion/Assessment/Recommendation/Template/Grouping/
Metrics/Tagging lifecycles, plus focused unit tests for validation/conflict
paths (`apps_test.go`, `policies_test.go`, `appversions_test.go`,
`assessments_test.go`) and a snapshot/restore round-trip
(`persistence_test.go`). All pass under `-race` across 5 consecutive runs.

### Judgment calls made where the audit flagged a genuine unknown, or where this pass narrowed scope

1. **`draftVersion = "draft"`** (consts.go): asserted from product knowledge,
   not SDK-verified — exactly the assumption the audit flagged. Every
   draft-only mutation op (`Create/Update/DeleteAppVersionAppComponent`,
   `Create/Update/DeleteAppVersionResource`, `Add/RemoveDraftAppVersionResourceMappings`,
   `PutDraftAppVersionTemplate`, `ImportResourcesToDraftAppVersion`) has NO
   `AppVersion` field on its wire Input at all, which means the "operates on
   draft only" invariant is enforced by construction (no wire path exists to
   target a published version for mutation) — simpler than initially
   anticipated, no explicit runtime check was needed beyond "the draft always
   exists."
2. **`AssessmentARN` mints a fresh ID, not the app-id** (store.go): the SDK's
   own doc comment literally says the ARN format reuses `{app-id}`, which the
   pre-implementation audit read verbatim and flagged. This pass concluded
   that literal reading is almost certainly a copy-paste doc-generation
   artifact upstream (the same boilerplate sentence appears on App/
   AppAssessment/RecommendationTemplate ARN fields alike) — reusing the app-id
   verbatim would make every assessment of the same App share one ARN, which
   cannot be correct given `ListAppAssessments` returns multiple per App and
   `DescribeAppAssessment`/`DeleteAppAssessment` must address one specific one.
   Documented as a deliberate, reasoned deviation, not a disagreement with
   real AWS behavior.
3. **`CreateResiliencyPolicy`/`UpdateResiliencyPolicy` require all four
   `DisruptionType` keys** (`validatePolicyMap`, policies.go): the audit
   flagged the required-keys rule as something "the implementer would need to
   infer/decide." Chose to require all four (Software/Hardware/AZ/Region)
   since `FailurePolicy.RtoInSecs`/`RpoInSecs` are plain `int32` (always
   present, never pointers) on the wire — a policy missing an entry has no
   well-formed zero value to fall back to.
4. **Cross-service resolution scope narrowed from the audit's recommendation**
   (resources.go's `resolveMappingsLocked`, `ImportResourcesToDraftAppVersion`):
   the audit identified real, usable methods on `services/cloudformation`,
   `services/eks`, and `services/resourcegroups` and recommended
   `ResolveAppVersionResources` genuinely query them. This pass did not wire
   that — it would require the same Provider.Init-time `BackendsProvider`
   interface pattern `services/cloudformation` itself uses to reach other
   backends (a `ctx.Config`-cast interface satisfied by the `CLI` struct,
   requiring careful Provider-init ordering), which was judged out of scope
   given the size of the remaining 63-op surface. Instead, only the
   `Resource` `MappingType` (which already carries a caller-supplied
   `PhysicalResourceId` — a real value, not fabricated) is materialized into a
   `PhysicalResource` on resolve; `CfnStack`/`ResourceGroup`/`EKS` mappings are
   accepted and stored but never expanded into discovered resources. This is
   the single largest deviation from the audit's recommendations in this
   pass, and is called out explicitly rather than silently narrowed — a
   defensible scope boundary (real, honest, bounded state machine) but a
   real gap relative to "the single best genuinely emulated investment" the
   audit identified. A future pass wiring those three backends would be
   valuable, following `services/cloudformation/provider.go`'s
   `BackendsProvider`/`extractBackends` pattern.
5. **`ComplianceStatus` coarse rule** (`complianceStatusForPolicy`,
   assessments.go): the audit explicitly sanctioned "a binary/coarse
   compliance signal ... IF the implementer picks one clear, stated rule."
   Chose: `MissingPolicy` when no policy is bound (a real, derivable fact),
   `PolicyMet` when one is (a documented stand-in, not real compliance
   evaluation). `AchievableRpoInSecs`/`RtoInSecs` echo the bound policy's real
   configured targets; `CurrentRpoInSecs`/`RtoInSecs` are documented as
   assumed equal to the achievable target, since no real assessment measures
   an actual current value.
6. **`scorePlaceholder = 0.0`** for every `ResiliencyScore.Score` occurrence:
   the audit recommended "a single fixed/deterministic placeholder value
   ... and document loudly ... that it is NOT a real score." Chose 0 (rather
   than, say, a policy-derived pass/fail boundary) since it is the least
   likely value to be misread as "perfectly resilient."
7. **`CreateRecommendationTemplate` completes synchronously**, not through the
   `Pending`/`InProgress` timer the other five async families use: with zero
   real recommendations ever generated, packaging them into a template is
   mechanically trivial (no real analysis work to await). `TemplatesLocation`
   is a synthetic bucket/prefix string; no S3 object is actually written —
   the audit flagged real `services/s3` write-through as "mechanically
   simulatable... but only meaningful once there are real recommendations to
   package," and judged it out of scope here for the same reason.
8. **`ListSuggestedResiliencyPolicies`' 5-tier table** (policies.go's
   `suggestedPolicyTiers`) is a self-invented halving progression (60s
   Mission Critical down to 604800s Non-Critical), explicitly documented as
   NOT AWS-published defaults — matching the audit's own suggested
   precedent (`services/grafana`'s static `ListVersions` table).
9. **`ListUnsupportedAppVersionResources`** classification IS fully derivable
   (not a judgment call, but worth noting as a place the audit's own research
   paid off): the two closed `PhysicalResourceId.Type` lists documented at
   `types/types.go:1150` give a real, checkable supported-type set, so this
   op is genuine business logic, not a stub with an empty list.

### What the audit got right (spot-checked during implementation)

The complete 63-op inventory and method/path table, the lowerCamel JSON/query
casing (confirmed independently by reading `serializers.go`'s
`object.Key(...)`/`SetQuery(...)` literals directly rather than trusting the
audit's claim), the 7 shared exception shapes and every one of the flagged
per-op exception-set outliers (`DeleteRecommendationTemplate` uniquely
lacking `ConflictException` — confirmed via a dedicated round-trip test;
`ListAppVersions` uniquely dropping `ThrottlingException`), epoch-seconds
timestamps, and the `FailurePolicy.RtoInSecs`/`RpoInSecs` plain-`int32`
(not-pointer) wire shape all matched the audit's findings exactly during
implementation — confirming the audit's method (reading
`serializers.go`/`deserializers.go`/`types/enums.go` directly) was sound.

## Operation count and SDK version (verified, not estimated)

`ls api_op_*.go | wc -l` against
`/home/agbishop/go/pkg/mod/github.com/aws/aws-sdk-go-v2/service/resiliencehub@v1.38.3/`
returns **63**, matching the pre-implementation audit's count exactly. The
module was added to this repo's `go.mod` this pass via
`go get github.com/aws/aws-sdk-go-v2/service/resiliencehub@v1.38.3`.

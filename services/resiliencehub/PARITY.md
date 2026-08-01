---
# PARITY MANIFEST — PRE-IMPLEMENTATION AUDIT, NOT YET BUILT.
# services/resiliencehub/ does not exist yet (confirmed: no dir, no cli.go
# registration, no go.mod entry, no Go symbols anywhere in the tree -- see
# bd gopherstack-1gfi). This document is a wire-shape + behavior SPEC for the
# implementer, not a record of existing code. No .go files were written to
# produce it; every claim below was read directly from the SDK module cache
# or from grep/read of this repo's existing services.
service: resiliencehub
sdk_module: aws-sdk-go-v2/service/resiliencehub@v1.38.3   # resolved via `go get ...@latest` in a throwaway scratch module; NOT added to this repo's go.mod
last_audit_commit: 7922e4c4d     # HEAD at audit time; re-audit should diff services/resiliencehub/ from here once built
last_audit_date: 2026-08-01
overall: N/A-pre-implementation   # nothing built yet; see body for the full 63-op spec the implementer should work from
# Per-op status below reflects THIS AUDIT's confidence in the wire spec (verified
# against serializers.go/deserializers.go/types.go), not backend behavior -- there
# is no backend. state/persist are uniformly "gap" because nothing is implemented.
# wire/errors are "ok" where this pass read the actual generated code (all 63 were).
ops:
  AcceptResourceGroupingRecommendations: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /accept-resource-grouping-recommendations"}
  AddDraftAppVersionResourceMappings: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /add-draft-app-version-resource-mappings"}
  BatchUpdateRecommendationStatus: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /batch-update-recommendation-status"}
  CreateApp: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /create-app"}
  CreateAppVersionAppComponent: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /create-app-version-app-component"}
  CreateAppVersionResource: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /create-app-version-resource"}
  CreateRecommendationTemplate: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /create-recommendation-template"}
  CreateResiliencyPolicy: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /create-resiliency-policy"}
  DeleteApp: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /delete-app"}
  DeleteAppAssessment: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /delete-app-assessment"}
  DeleteAppInputSource: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /delete-app-input-source"}
  DeleteAppVersionAppComponent: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /delete-app-version-app-component"}
  DeleteAppVersionResource: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /delete-app-version-resource"}
  DeleteRecommendationTemplate: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /delete-recommendation-template"}
  DeleteResiliencyPolicy: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /delete-resiliency-policy"}
  DescribeApp: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-app"}
  DescribeAppAssessment: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-app-assessment"}
  DescribeAppVersion: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-app-version"}
  DescribeAppVersionAppComponent: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-app-version-app-component"}
  DescribeAppVersionResource: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-app-version-resource"}
  DescribeAppVersionResourcesResolutionStatus: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-app-version-resources-resolution-status"}
  DescribeAppVersionTemplate: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-app-version-template"}
  DescribeDraftAppVersionResourcesImportStatus: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-draft-app-version-resources-import-status"}
  DescribeMetricsExport: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-metrics-export"}
  DescribeResiliencyPolicy: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-resiliency-policy"}
  DescribeResourceGroupingRecommendationTask: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /describe-resource-grouping-recommendation-task"}
  ImportResourcesToDraftAppVersion: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /import-resources-to-draft-app-version"}
  ListAlarmRecommendations: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-alarm-recommendations"}
  ListAppAssessmentComplianceDrifts: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-app-assessment-compliance-drifts"}
  ListAppAssessmentResourceDrifts: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-app-assessment-resource-drifts"}
  ListAppAssessments: {wire: ok, errors: ok, state: gap, persist: gap, note: "GET /list-app-assessments (the only List* that is GET besides the five below)"}
  ListAppComponentCompliances: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-app-component-compliances"}
  ListAppComponentRecommendations: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-app-component-recommendations"}
  ListAppInputSources: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-app-input-sources"}
  ListApps: {wire: ok, errors: ok, state: gap, persist: gap, note: "GET /list-apps"}
  ListAppVersionAppComponents: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-app-version-app-components"}
  ListAppVersionResourceMappings: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-app-version-resource-mappings"}
  ListAppVersionResources: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-app-version-resources"}
  ListAppVersions: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-app-versions"}
  ListMetrics: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-metrics"}
  ListRecommendationTemplates: {wire: ok, errors: ok, state: gap, persist: gap, note: "GET /list-recommendation-templates"}
  ListResiliencyPolicies: {wire: ok, errors: ok, state: gap, persist: gap, note: "GET /list-resiliency-policies"}
  ListResourceGroupingRecommendations: {wire: ok, errors: ok, state: gap, persist: gap, note: "GET /list-resource-grouping-recommendations"}
  ListSopRecommendations: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-sop-recommendations"}
  ListSuggestedResiliencyPolicies: {wire: ok, errors: ok, state: gap, persist: gap, note: "GET /list-suggested-resiliency-policies"}
  ListTagsForResource: {wire: ok, errors: ok, state: gap, persist: gap, note: "GET /tags/{resourceArn}"}
  ListTestRecommendations: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-test-recommendations"}
  ListUnsupportedAppVersionResources: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /list-unsupported-app-version-resources"}
  PublishAppVersion: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /publish-app-version"}
  PutDraftAppVersionTemplate: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /put-draft-app-version-template"}
  RejectResourceGroupingRecommendations: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /reject-resource-grouping-recommendations"}
  RemoveDraftAppVersionResourceMappings: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /remove-draft-app-version-resource-mappings"}
  ResolveAppVersionResources: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /resolve-app-version-resources"}
  StartAppAssessment: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /start-app-assessment"}
  StartMetricsExport: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /start-metrics-export"}
  StartResourceGroupingRecommendationTask: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /start-resource-grouping-recommendation-task"}
  TagResource: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /tags/{resourceArn}"}
  UntagResource: {wire: ok, errors: ok, state: gap, persist: gap, note: "DELETE /tags/{resourceArn}"}
  UpdateApp: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /update-app"}
  UpdateAppVersion: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /update-app-version"}
  UpdateAppVersionAppComponent: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /update-app-version-app-component"}
  UpdateAppVersionResource: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /update-app-version-resource"}
  UpdateResiliencyPolicy: {wire: ok, errors: ok, state: gap, persist: gap, note: "POST /update-resiliency-policy"}
families:
  route-matcher: {status: deferred, note: "No handler.go exists yet. Protocol is awsRestjson1 like grafana/databrew; every op but the tags/{resourceArn} trio is a literal fixed-path POST/GET with no path parameters at all (kebab-case action name IS the path) -- unusually simple routing compared to most REST-JSON services."}
gaps:
  - "Entire service does not exist: no services/resiliencehub/ directory, no cli.go registration, not in go.mod. This document is the spec for building it, not a record of what exists (bd: gopherstack-1gfi)."
  - "AssessmentSummary (top risk recommendations + natural-language summary) is a genuine Bedrock-LLM-generated field on the real API -- the SDK's own doc comment says 'This property is available only in the US East (N. Virginia) Region', which only makes sense for a feature backed by a specific hosted model. A naive implementation would either fabricate LLM-quality prose or leave this permanently nil; see 'Honest vs. fabricated' section below for the recommendation (nil/omit, do not synthesize)."
  - "ResiliencyScore, DisruptionCompliance (achievable/current RTO/RPO), ComplianceStatus, and per-AppComponent ConfigRecommendation/ComponentRecommendation content all depend on the real product's proprietary scoring model and a catalog of AWS-recommended architecture patterns per resource type -- neither is derivable from the Go SDK types. Any simulated score is a made-up number dressed in a real-looking shape; see 'Honest vs. fabricated' below for what's defensible vs. not."
  - "No AWS::ResilienceHub::* CloudFormation resource type exists in services/cloudformation/resources_*.go (grepped, zero hits) -- cross-service resource discovery for CFN-stack-backed apps has no CFN-side support to piggyback on beyond what services/cloudformation's own stack/resource listing already exposes (see Section 3)."
  - "No services/appregistry (or servicecatalogappregistry) package exists in this tree -- the AppRegistryApp resource-mapping type and ListAppInputSources' AppRegistry source kind cannot resolve against a real backend; they can only be modeled as opaque accepted strings."
  - "StartResourceGroupingRecommendationTask / DescribeResourceGroupingRecommendationTask / ListResourceGroupingRecommendations / Accept|RejectResourceGroupingRecommendations (the whole grouping-recommendation family) depends on an ML clustering heuristic over resource metadata with no public spec -- see 'Honest vs. fabricated' below."
deferred:
  - "Every op family listed above (nothing audited at the implementation level since nothing is implemented)"
leaks: {status: clean, note: "N/A -- no code exists"}
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

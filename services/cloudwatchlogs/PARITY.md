---
service: cloudwatchlogs
sdk_module: aws-sdk-go-v2/service/cloudwatchlogs@v1.81.1
last_audit_commit: 3884816a
last_audit_date: 2026-07-25
overall: A
ops:
  CreateLookupTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-4: field-diffed against aws-sdk-go-v2@v1.80.0 api_op_CreateLookupTable.go/types.LookupTable. CreateLookupTableInput.TableBody is a plain *string of CSV content (verified against serializers.go: tableBody is serialized as a bare JSON string, no S3 reference anywhere in this op's input or output) -- so this backend genuinely parses the CSV (encoding/csv) rather than modeling a reference to data it never reads: the header row becomes TableFields, subsequent rows are counted into RecordsCount, and len(tableBody) becomes SizeBytes. Name validated against the documented alphanumeric+underscore/256-char charset; body validated against the documented 10 MB limit and real CSV syntax (malformed CSV -> InvalidParameterException). ARN is constructed as arn:{partition}:logs:{region}:{account}:lookup-table:{name} via pkgs/arn -- no ARN pattern is embedded anywhere in the SDK module (no smithy model shipped, no doc-comment pattern), so this mirrors the existing log-group ARN convention (arn.Build + \"log-group:\"+name) rather than an AWS-confirmed pattern; flagged here for anyone who later finds an authoritative pattern to check against. Response is create-only (createdAt/lookupTableArn), matching CreateLookupTableOutput exactly (no echoed metadata). Tags are accepted and stored via the handler-level tag store (h.setTags, keyed by lookupTableArn) exactly like log group tags, since types.LookupTable/GetLookupTableOutput have no Tags field of their own -- tags are wire-visible only via the generic ListTagsForResource/TagResource/UntagResource ops, which already existed."}
  GetLookupTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-4: full-content shape (description/kmsKeyId/lastUpdatedTime/lookupTableArn/lookupTableName/sizeBytes/tableBody) field-diffed against GetLookupTableOutput; unlike DescribeLookupTables this includes tableBody."}
  UpdateLookupTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-4: full replacement of TableBody (re-parsed, TableFields/RecordsCount/SizeBytes recomputed) per the doc comment (\"This is a full replacement operation\"); Description/KmsKeyId are optional *string on the real input (nil = leave unchanged), modeled the same way here rather than collapsing to plain strings (which would make \"omitted\" and \"explicitly cleared\" indistinguishable over JSON)."}
  DeleteLookupTable: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLookupTables: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-4: field-diffed against types.LookupTable -- this list shape deliberately excludes tableBody (metadata only: description/kmsKeyId/lastUpdatedTime/lookupTableArn/lookupTableName/recordsCount/sizeBytes/tableFields), matching the real SDK type used by DescribeLookupTablesOutput.LookupTables (distinct from GetLookupTableOutput's full-content shape). lookupTableNamePrefix filter and maxResults(default 50/max 100 per the doc comment)/nextToken pagination implemented via the same base64-index-cursor helpers (encodeNextToken/parseNextToken) every other paginated op in this package already uses."}
  PutSyslogConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-4: field-diffed against api_op_PutSyslogConfiguration.go/types.SyslogConfiguration. Real AWS PutSyslogConfigurationInput/DeleteSyslogConfigurationInput both require only LogGroupIdentifier (VpcEndpointId is optional on both, per the real validator -- validateOpPutSyslogConfigurationInput only requires LogGroupIdentifier), so this backend models at most one syslog configuration per log group, keyed by normalized log group identifier -- the same per-log-group-identifier keying this codebase already uses for IndexPolicy/Transformer (store_setup.go's indexPolicyKeyFn/transformerKeyFn). Improvement over those pre-existing sibling ops: this validates the log group actually exists (region-scoped groupGet lookup) and returns ResourceNotFoundException otherwise, rather than accepting an arbitrary string as those two do -- a deliberate, real behavior difference specifically called for this pass, not a pre-existing gap being silently carried forward. SourceType is always \"VPCE\" (the only real types.SyslogSourceType enum member). VpcEndpointId itself is accepted/stored/returned as an opaque string, not cross-validated against real EC2 VPC-endpoint state -- there is no VPC-endpoint modeling anywhere in this service (or a cross-service validation pattern anywhere in this codebase for ARN/ID references into another service's resources), matching how this service already treats KmsKeyId/RoleArn/DestinationArn (stored, not validated)."}
  ListSyslogConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-4: field-diffed against types.SyslogConfiguration (createdAt/logGroupArn/sourceType/vpcEndpointId). Optional logGroupIdentifier/vpcEndpointId filters plus nextToken/maxResults pagination implemented."}
  DeleteSyslogConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-4: optional vpcEndpointId scoping parameter -- when supplied it must match the stored configuration's VPC endpoint or the delete is treated as not-found, matching the real input accepting both LogGroupIdentifier(required)+VpcEndpointId(optional) as a compound identify-then-delete key."}
  GetStorageTierPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-4: field-diffed against api_op_GetStorageTierPolicy.go -- GetStorageTierPolicyInput has ZERO fields (verified: an empty struct, no LogGroupIdentifier or any other member), confirming this is an account-level singleton, not per-log-group as might be assumed from this service's existing per-log-group LogGroupClass modeling. Before any PutStorageTierPolicy call, real AWS's default active tier is STANDARD with LastUpdatedTime absent (GetStorageTierPolicyOutput.LastUpdatedTime is a nilable *int64) -- this backend synthesizes exactly that default (StorageTier=STANDARD, LastUpdatedTime omitted from the wire response) rather than requiring a Put first or fabricating a timestamp."}
  PutStorageTierPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-4: field-diffed against api_op_PutStorageTierPolicy.go/types.StorageTier -- PutStorageTierPolicyInput carries only StorageTier (no LogGroupIdentifier), confirming the account-level scope. StorageTier validated against the real 2-member enum (STANDARD/INTELLIGENT_TIERING); invalid values rejected with InvalidParameterException matching the real required-field validator (validateOpPutStorageTierPolicyInput)."}
  PutLogEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: added the two documented batch-shape constraints that were previously unenforced -- (1) a batch whose events are not in non-decreasing timestamp order now fails the whole request (InvalidParameterException) via validateChronologicalOrder, matching \"A batch of log events in a single request must be in a chronological order. Otherwise, the operation fails.\"; (2) a batch whose *valid* (post too-old/too-new/expired classification) events span more than 24 hours now fails the whole request via validateEventSpan, matching \"the time span in a single batch cannot exceed 24 hours.\" Both bypass synthetic (pre-2001-epoch) timestamps for fixture-friendliness, consistent with classifyLogEvents' existing bypass. Two existing tests (TestJanitor_SweepRetention, TestJanitor_SweepUpdatesStreamMetadata) sent a single batch spanning ~10 days, which real AWS would reject outright; split into two individually-valid PutLogEvents calls. Sequence-token/RejectedLogEventsInfo fixes from the prior pass unchanged."}
  GetLogEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "startFromHead/nextToken precedence and stable-at-boundary forward/backward tokens verified correct."}
  FilterLogEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "cross-stream interleave + stable timestamp sort verified; logStreamNames/logStreamNamePrefix mutual-exclusion validated; searchedLogStreams correctly always empty (AWS deprecated this field)."}
  GetLogGroupFields: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: previously a disguised stub always returning the 4 static built-in fields at 100% regardless of actual log content, and didn't accept logGroupIdentifier or time at all. Now does real percentage-based sampling: logGroupIdentifier is accepted (via normalizeLogGroupIdentifier) alongside logGroupName; time (epoch *seconds*, unlike almost every other timestamp field in this API) centers an 8-minute-either-side window per the doc comment, or defaults to the most recent 15 minutes; every stored event in-window is sampled, the 4 built-in fields plus any JSON top-level keys (via the existing jsonMessageFields helper) are counted, and Percent is computed per-field over the sampled count, sorted descending. Zero sampled events now correctly returns an empty list rather than fabricating 100%-present built-in fields. Synthetic (pre-2001) event timestamps bypass the window, matching this file's existing test-fixture convention."}
  CreateLogGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLogGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades streams/events/subscription filters/metric filters."}
  DescribeLogGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLogStream: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLogStream: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLogStreams: {wire: ok, errors: ok, state: ok, persist: ok, note: "orderBy=LastEventTime + prefix and descending + orderBy=LogStreamName rejection rules match AWS."}
  PutLogGroupDeletionProtection: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-7rq1): request member was `deletionProtected`, a gopherstack-invented name -- the real PutLogGroupDeletionProtectionRequest member (logs/2014-03-28/service-2.json) is `deletionProtectionEnabled`. A real client's flag was silently dropped by json.Unmarshal (wrong key = zero value = false), so every real PutLogGroupDeletionProtection call silently disabled protection regardless of the caller's intent. Fixed the json tag; both existing tests (which asserted only HTTP 200, not backend state, and used the wrong key) and a new state-asserting test cover it."}
  PutRetentionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRetentionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutSubscriptionFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "2-filter-per-group cap and update-in-place-by-name verified."}
  DescribeSubscriptionFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSubscriptionFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  PutMetricFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMetricFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMetricFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  TestMetricFilter: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: ExtractedValues was always {} (disguised stub -- computed nothing from the pattern's named fields). Now extracts every $-referenced field for JSON and space-delimited patterns."}
  ListTagsLogGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagLogGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagLogGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateExportTask: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeExportTasks: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: types.ExportTask nests Status as {code, message} (types.ExportTaskStatus) and Creation/CompletionTime under executionInfo (types.ExportTaskExecutionInfo); this handler previously serialized the internal flat ExportTask model directly onto the wire (status as a bare string, creationTime/completionTime/statusMessage as flat top-level keys), which a real SDK client's generated deserializer would silently read as nil for all four fields. Added toWireExportTask/wireExportTask (handler_export_tasks.go) to map correctly; the internal flat model is unchanged (still used for backend state/persistence)."}
  CancelExportTask: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateImportTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: initial status was the shared completenessStatusActive constant (\"ACTIVE\"), which is correct for IntegrationStatus but is not a member of types.ImportStatus (IN_PROGRESS/CANCELLED/COMPLETED/FAILED) at all. Now uses a dedicated importStatusInProgress=\"IN_PROGRESS\" constant."}
  DescribeImportTasks: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: the ImportTask model serialized its status field as \"status\", but the real wire key (types.Import.ImportStatus) is \"importStatus\" -- a real SDK client's ImportStatus field would always deserialize empty/unrecognized. Also excluded ImportRoleArn from the wire (json:\"-\"): it is accepted on CreateImportTask for this backend's own bookkeeping but is not a field on the real Import describe/list type at all."}
  CancelImportTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: accepted-for-cancellation check compared against \"ACTIVE\" (see CreateImportTask note) instead of \"IN_PROGRESS\"; a real import task (always created with status IN_PROGRESS) could therefore never actually be cancelled through this backend before this fix. Output wire shape (importId/importStatus/creationTime/lastUpdatedTime) was already correct."}
  PutDestination: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: response silently omitted accessPolicy and creationTime (both real flat fields on types.Destination); added destinationWireShape helper, used by PutDestination and DescribeDestinations."}
  DescribeDestinations: {wire: ok, errors: ok, state: ok, persist: ok, note: "same creationTime/accessPolicy fix as PutDestination. DestinationNamePrefix's PascalCase wire key (verified against the real serializer -- this operation is a smithy-model exception to the otherwise-universal lowerCamelCase convention) was already correct. fixed this pass: Limit/NextToken pagination (real DescribeDestinationsInput/Output, confirmed via api_op_DescribeDestinations.go) now implemented via the same base64-index-cursor helpers every other paginated op in this package uses -- a previous revision took no limit/nextToken at all and always returned the complete unpaginated list."}
  DeleteDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  PutDestinationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutDeliveryDestination: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: the handler built its response by hand and only ever included name/arn/outputFormat, silently dropping the target resource ARN, deliveryDestinationType, and tags from every response. The target ARN is also real-AWS-nested under deliveryDestinationConfiguration.destinationResourceArn, not a flat string (the DeliveryDestination model's own json tag, deliveryDestinationConfiguration on a bare string field, was wrong for the same reason, though it was never actually used for wire serialization). Added deliveryDestinationType as a real accepted+persisted+validated (S3/CWL/FH/XRAY) input, and deliveryDestinationWireShape to build the correct nested response for Put/Get/Describe."}
  GetDeliveryDestination: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as PutDeliveryDestination."}
  DescribeDeliveryDestinations: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as PutDeliveryDestination -- previously this list endpoint returned only name+arn per entry, nothing else."}
  DeleteDeliveryDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  PutDeliveryDestinationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDeliveryDestinationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDeliveryDestinationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutDeliverySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass -- CRITICAL: the input parser read \"resourceArns\" (plural array), but the real wire key (verified against the serializer) is \"resourceArn\" (singular string). A real SDK client's request always sent \"resourceArn\", so this backend's ResourceArns was always empty for every real client call -- the resource ARN was silently dropped, not just mis-shaped in the response. Also added service (aws-sdk-go-v2 types.DeliverySource.Service, \"the Amazon Web Services service that is sending logs\"): confirmed NOT client-supplied on PutDeliverySourceInput, so it is now derived server-side from the resource ARN's service segment via serviceFromARN, matching real AWS. Response previously returned only name+arn; now uses deliverySourceWireShape (name/arn/logType/resourceArns/service/tags)."}
  GetDeliverySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as PutDeliverySource."}
  DescribeDeliverySources: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as PutDeliverySource -- previously this list endpoint returned only name+arn per entry."}
  DeleteDeliverySource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLogAnomalyDetector: {wire: ok, errors: ok, state: ok, persist: ok}
  GetLogAnomalyDetector: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: LogAnomalyDetector.DetectorStatus serialized as \"detectorStatus\"; the real wire key (types.AnomalyDetector.AnomalyDetectorStatus / GetLogAnomalyDetectorOutput.AnomalyDetectorStatus) is \"anomalyDetectorStatus\" -- a real SDK client's status field always deserialized empty. Field renamed to AnomalyDetectorStatus (Go field + json tag both fixed) for consistency with the rest of this model. Also removed two orphaned gopherstack-invented fields with no wire representation anywhere in the real SDK and no readers anywhere in this codebase (de-stub hygiene): EvaluationLookback (\"evaluationLookback\") and FilterAnomalies (\"filterAnomalies\") -- neither exists in types.AnomalyDetector, any api_op_*AnomalyDetector*.go input, or any SDK doc comment."}
  ListLogAnomalyDetectors: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLogAnomalyDetector: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: Enabled is a *required* field on the real UpdateLogAnomalyDetectorInput (\"Use this parameter to pause or restart the anomaly detector\"), used to set/clear types.AnomalyDetectorStatusPaused -- this backend didn't accept or act on it at all, meaning a detector could never actually be paused/resumed through this API despite PAUSED being a real, reachable status value. Now enabled=false sets AnomalyDetectorStatus=PAUSED; enabled=true resumes a paused detector to ANALYZING (a no-op if not currently paused, e.g. still INITIALIZING)."}
  DeleteLogAnomalyDetector: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAnomalies: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAnomaly: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateScheduledQuery: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: executionRoleArn and queryLanguage are both real required CreateScheduledQueryInput members (confirmed via api_op_CreateScheduledQuery.go); a previous revision accepted executionRoleArn from the wire but discarded it via a `_` parameter (never stored, never returned) and never modeled queryLanguage at all, so a required member's absence was silently accepted rather than rejected. Now both are validated required, plus description/destinationConfiguration/logGroupIdentifiers/timezone/endTimeOffset/startTimeOffset/scheduleStartTime/scheduleEndTime are accepted and stored (bundled behind a new ScheduledQueryCreateParams struct to avoid an unwieldy positional-parameter signature)."}
  GetScheduledQuery: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass (2 bugs): (1) ScheduledQuery.Arn previously serialized as \"arn\"; the real wire key (GetScheduledQueryOutput.ScheduledQueryArn) is \"scheduledQueryArn\" -- fixed in an earlier pass. (2) fixed this pass: the response was wrapped under a \"scheduledQuery\" key with no real wire representation at all -- GetScheduledQueryOutput's members (confirmed via deserializers.go's awsAwsjson11_deserializeOpDocumentGetScheduledQueryOutput) sit flat at the top level of the response. The ScheduledQuery model previously covered only 6 of GetScheduledQueryOutput's ~20 members; now covers the full set (description, destinationConfiguration, executionRoleArn, lastExecutionStatus/lastTriggeredTime/lastUpdatedTime, logGroupIdentifiers, queryLanguage, scheduleType, scheduleEndTime/scheduleStartTime, startTimeOffset/endTimeOffset, timezone). scheduleType is always CUSTOMER_MANAGED (not client-settable; AWS_MANAGED queries are pre-provisioned by AWS, not created through this API)."}
  ListScheduledQueries: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: real ListScheduledQueriesOutput.ScheduledQueries is []types.ScheduledQuerySummary, a distinct, narrower shape than GetScheduledQueryOutput (no queryString/executionRoleArn/queryLanguage/logGroupIdentifiers/description/endTimeOffset/startTimeOffset/scheduleStartTime/scheduleEndTime, confirmed via types.ScheduledQuerySummary) -- a previous revision reused the full Get shape here, over-sharing fields real AWS never returns from List. New scheduledQuerySummaryToWire renders the correct narrower shape."}
  UpdateScheduledQuery: {wire: ok, errors: ok, state: ok, persist: ok, note: "still state-only, not the real API's full-replace UpdateScheduledQueryInput (executionRoleArn/queryLanguage/queryString/scheduleExpression all required, plus the same optional set as Create) -- see gaps below. lastUpdatedTime now bumped on every state change."}
  DeleteScheduledQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  GetScheduledQueryHistory: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  metric-filter-emission: {status: ok, note: "fixed (internal PutLogEvents dispatch, not an SDK op): emitMetricFilterMatches previously emitted matchCount copies of one static value regardless of MetricValue's per-event field reference (a disguised stub -- '$field' values were never actually read from the matched log event, just defaulted to 1.0/DefaultValue). Now extracts the referenced field ($name for space-delimited patterns, $.path for JSON patterns) per matched event via new compiledFilterPattern.extract; a matched-but-non-numeric-or-absent field now correctly emits no data point rather than fabricating one. Also fixed emitted Unit being hardcoded to \"\" instead of the configured MetricTransformation.Unit."}
  janitor-retention-sweep: {status: ok, note: "two-phase read-then-write lock, worker.NewGroup ticker is ctx-cancel safe, telemetry recorded. No leak."}
  subscription-filter-delivery: {status: ok, note: "goroutines bounded by workerSem + backend WaitGroup + service ctx; Close()/Drain() wait for in-flight deliveries. No leak found."}
  insights (StartQuery/GetQueryResults/StopQuery/DescribeQueries/query language): {status: ok, note: "lightly reviewed only (large ~2500 LOC subsystem across insights_*.go); query TTL eviction (evictByTTL) and cap enforcement (enforceCap) present and bounded; not exhaustively re-audited op-by-op this pass -- see deferred."}
  export/import tasks / deliveries / anomaly detectors / scheduled queries: {status: ok, note: "genuinely field-diffed and fixed this pass -- see the individual ops entries above for CreateExportTask/DescribeExportTasks/CreateImportTask/DescribeImportTasks/CancelImportTask/PutDeliveryDestination*/PutDeliverySource*/GetLogAnomalyDetector/UpdateLogAnomalyDetector/GetScheduledQuery. Several real bugs found and fixed: nested-vs-flat wire shape (ExportTask, DeliveryDestination), wrong wire key (importStatus, anomalyDetectorStatus, scheduledQueryArn, deliveryDestinationConfiguration.destinationResourceArn), wrong input wire key that silently dropped a real field entirely (PutDeliverySource's resourceArn), invented status enum values not in the real SDK (ImportStatus ACTIVE/SUCCEEDED -> IN_PROGRESS/COMPLETED), and orphaned invented fields with zero wire representation (LogAnomalyDetector.EvaluationLookback/FilterAnomalies). Follow-up pass closed the four gaps this note used to list: CreateDelivery now accepts FieldDelimiter/RecordFields/S3DeliveryConfiguration at creation time (UpdateDeliveryConfiguration also gained S3DeliveryConfiguration, which it was real-API-eligible for but hadn't implemented either); Delivery's fabricated CreationTime field is now excluded from the wire (json:\"-\") since real types.Delivery has no such member; AccountPolicy now carries AccountId/LastUpdatedTime; DescribeDestinations now implements Limit/NextToken pagination; ScheduledQuery now models the full GetScheduledQueryOutput field set and the Get/List wire-shape bugs (wrapper key, over-shared List fields) described below are fixed. UpdateScheduledQuery's state-only-update limitation (the real op is a full-replace requiring executionRoleArn/queryLanguage/queryString/scheduleExpression on every call) remains open -- see gaps."}
  account policies / data protection/resource/index policies / transformers / integrations: {status: ok, note: "spot-checked (CreateExportTask/runExport does a real synchronous S3 write via injectable ExportSink when configured; ApplyTransformer/applyJSONProcessor implements real addKeys/deleteKeys/renameKeys-style JSON processors, not a stub); AccountPolicy/ResourcePolicy/Transformer/GetIntegrationOutput top-level shapes spot-checked as flat (no nested-object bugs found), but not exhaustively re-audited op-by-op this pass -- see deferred."}
  StartLiveTail: {status: ok, note: "explicitly validation-only (log-group-identifier existence check) with a documented comment explaining the streaming HTTP/2 transport can't be served by this request/response handler -- an honest declared limitation, not a silent stub."}
  lookup tables / syslog configurations / storage tier policy (parity-4 SDK-bump additions): {status: ok, note: "10 new ops (CreateLookupTable/GetLookupTable/UpdateLookupTable/DeleteLookupTable/DescribeLookupTables, PutSyslogConfiguration/ListSyslogConfigurations/DeleteSyslogConfiguration, GetStorageTierPolicy/PutStorageTierPolicy), all newly implemented for real (lookup_tables.go, syslog_configurations.go, policies.go, handler_lookup_tables.go, handler_syslog_configurations.go, handler_storage_tier_policy.go) against aws-sdk-go-v2@v1.80.0 (bumped from v1.64.0). Two findings worth flagging for future auditors who might assume otherwise from the task framing alone: (1) lookup tables do NOT reference S3 -- CreateLookupTableInput/UpdateLookupTableInput both carry TableBody as a plain CSV *string (verified against serializers.go), so this backend parses real CSV content rather than modeling an S3 reference it would need chaos/network plumbing to honestly resolve; (2) the storage tier policy is account-level, NOT per-log-group -- GetStorageTierPolicyInput is a zero-field struct and PutStorageTierPolicyInput carries only StorageTier, confirmed by reading the real Input structs directly, so it is intentionally kept independent of LogGroup.LogGroupClass rather than invented as a per-group attribute. See the individual ops entries above for full field-diff detail per op."}
gaps:
  - (2026-08-10, sdk_module pin correction v1.80.0 -> v1.81.1) types.DestinationConfiguration gained a new member, LookupTableConfiguration, as an alternative to S3Configuration (S3Configuration is no longer `required` on the real type). This backend's ScheduledQueryDestinationConfig (models.go) models only S3Configuration -- the "full set" claim on GetScheduledQuery/CreateScheduledQuery above predates this SDK addition and no longer covers the destination union's LookupTableConfiguration branch. Not fixed this pass (pin-correction only, no behavior changes); a real client sending a lookup-table-destination scheduled query would have that field silently dropped.
  - MetricTransformation.Dimensions is accepted, validated on the wire, and persisted on the MetricFilter, but is never forwarded to the emitted CloudWatch metric: the MetricEmitter interface (backend.go) only carries namespace/name/value/unit, and its real implementation is wired in cli.go's wireCWLogsMetricEmitter, which is out of scope for this pass (SHARED FILE). Extending the interface + cli.go wiring to carry dimensions is a real fix but requires touching cli.go. (bd: gopherstack-b14)
  - RESOLVED (follow-up pass): ScheduledQuery previously modeled only a subset of GetScheduledQueryOutput (arn/name/queryString/scheduleExpression/state/creationTime) and Get's response was wrapped under a non-existent "scheduledQuery" key. Now models the full field set (description, destinationConfiguration, executionRoleArn, lastExecutionStatus/lastTriggeredTime/lastUpdatedTime, logGroupIdentifiers, queryLanguage, scheduleType, scheduleEndTime/scheduleStartTime, startTimeOffset/endTimeOffset, timezone), Get returns it flat, List renders the real, narrower ScheduledQuerySummary shape via a separate scheduledQuerySummaryToWire, and Create validates the real required executionRoleArn/queryLanguage/scheduleExpression members. Still open: UpdateScheduledQuery remains state-only rather than the real API's full-replace semantics (UpdateScheduledQueryInput requires executionRoleArn/queryLanguage/queryString/scheduleExpression on every call, plus the same optional field set as Create) -- a distinct, separate-scope reshape from the field-completeness gap just closed. (bd: gopherstack-b14)
  - RESOLVED (follow-up pass): CreateDelivery now accepts FieldDelimiter, RecordFields, and S3DeliveryConfiguration at creation time (all real CreateDeliveryInput members, confirmed via serializers.go), rather than only via the separate UpdateDeliveryConfiguration op, which also gained S3DeliveryConfiguration support it was real-API-eligible for but hadn't implemented. Delivery's CreationTime field (no equivalent on real types.Delivery) is now excluded from the wire via json:"-", matching the same bookkeeping-only pattern used elsewhere in this codebase (e.g. inspector2's FindingsReport.CreatedAt). Still open: Delivery is missing DeliveryDestinationType (real types.Delivery field, server-derived from the paired destination -- would need a destination-ARN lookup at create time, not attempted this pass). (bd: gopherstack-b14)
  - RESOLVED (follow-up pass): AccountPolicy now carries AccountId and LastUpdatedTime, both real flat fields on types.AccountPolicy, populated by PutAccountPolicy. (bd: gopherstack-b14)
  - RESOLVED (follow-up pass): DescribeDestinations now implements Limit/NextToken pagination (real DescribeDestinationsInput/Output members, confirmed via api_op_DescribeDestinations.go) via the same base64-index-cursor helpers every other paginated op in this package uses. (bd: gopherstack-b14)
  - SyslogConfiguration's VpcEndpointId is accepted/stored/returned as an opaque string, never cross-validated against real EC2 VPC-endpoint state -- there is no VPC-endpoint modeling anywhere in this service, and no established cross-service ARN/ID validation pattern anywhere in this codebase to reuse (this backend already treats KmsKeyId/RoleArn/DestinationArn the same way elsewhere). Not implemented this pass; would require either a cross-service backend dependency or a shared registry that does not currently exist.
  - LookupTable's ARN (arn:{partition}:logs:{region}:{account}:lookup-table:{name}) is constructed by analogy to this codebase's existing log-group ARN convention, not confirmed against an authoritative AWS source: no smithy model ships with the installed aws-sdk-go-v2 module and no ARN pattern appears in any doc comment for LookupTableArn. If a future pass finds the real pattern differs, only lookupTableARN (lookup_tables.go) needs to change.
  - IndexPolicy/Transformer (pre-existing, prior pass) still accept any logGroupIdentifier string without checking it resolves to a real log group, unlike the new PutSyslogConfiguration added this pass (which does validate). Noted here for consistency awareness, not fixed this pass (out of scope: pre-existing ops, not part of the parity-4 SDK-bump op set this pass covers).
deferred:
  - Insights query language/stages/parser correctness (insights_expr.go, insights_parse.go, insights_parser.go, insights_stages.go, insights_stats.go) -- not re-verified op-by-op against CloudWatch Logs Insights query syntax this pass.
  - Data Protection/Resource/Index Policies, Transformers, Integrations, Account Policies (top-level shapes spot-checked flat/no-nested-object-bugs this pass, but not exhaustively re-audited field-by-field op-by-op beyond AccountPolicy's AccountId/LastUpdatedTime fix) -- see the "account policies, data protection/resource/index policies, transformers, integrations" family note.
  - StartLiveTail streaming transport (intentionally out of scope; validation-only by design).
leaks: {status: clean, note: "Only one goroutine spawn site (scheduleFilterDelivery for subscription filter delivery), bounded by a semaphore + backend WaitGroup + ctx cancellation; Close()/Drain() join in-flight work. Janitor ticker is ctx-cancel safe via pkgs/worker. No unbounded per-request goroutines found in the areas audited this pass."}
---

## Notes

- **2026-07-25 (parity-4 SDK-bump pass): implemented 10 new operations that appeared when
  the vendored aws-sdk-go-v2/service/cloudwatchlogs module was bumped from v1.64.0 to
  v1.80.0 -- three new families: lookup tables (CreateLookupTable/GetLookupTable/
  UpdateLookupTable/DeleteLookupTable/DescribeLookupTables), syslog configurations
  (PutSyslogConfiguration/ListSyslogConfigurations/DeleteSyslogConfiguration), and the
  account-level storage tier policy (GetStorageTierPolicy/PutStorageTierPolicy). All 10 are
  real implementations (new backend state, real CSV parsing, real validation, real
  persistence via the existing store.Registry "clean table" convention), not additions to
  the `notImplemented` tracking list. Two assumptions in the initial task framing turned out
  to be wrong once checked against the real SDK types directly, and are corrected here
  rather than silently followed: lookup tables carry their CSV content directly in
  CreateLookupTableInput/UpdateLookupTableInput.TableBody (a plain `*string`) with **no S3
  reference anywhere in the op's real input or output shape** (verified against
  serializers.go, which serializes `tableBody` as a bare JSON string) -- so this backend
  genuinely parses the CSV via `encoding/csv` rather than modeling an S3 reference it would
  need to fake; and the storage tier policy is **account-level, not per-log-group**
  (`GetStorageTierPolicyInput` is a zero-field struct, `PutStorageTierPolicyInput` carries
  only `StorageTier`, both confirmed by reading the real Input structs) -- kept as an
  independent concept from `LogGroup.LogGroupClass` rather than wired together, since
  nothing in the real API connects the two. `PutSyslogConfiguration` (the one family that
  genuinely does reference a log group) validates the log group actually exists via a
  region-scoped `groupGet` lookup and returns `ResourceNotFoundException` otherwise -- a
  real improvement in reference-validation rigor over this codebase's pre-existing
  `IndexPolicy`/`Transformer` completeness-pass ops, which accept any `logGroupIdentifier`
  string unchecked (noted as a `gaps` entry for awareness, not fixed here since those are
  out of scope for this pass). `DeleteLogGroup` now cascades to remove that log group's
  syslog configuration, matching its existing cascade to streams/subscription filters/metric
  filters. `go build`/`go vet`/`go test -race`/`gofmt`/`golangci-lint` all clean; 0 banned
  (`cyclop`/`gocyclo`/`gocognit`/`funlen`) nolints, same as before this pass. Overall grade
  held at A: the new surface is genuinely field-diffed and implemented, not stubbed, with
  honestly documented, narrow remaining gaps (VpcEndpointId not cross-validated against
  EC2 VPC-endpoint state -- no such modeling exists anywhere in this codebase to reuse; the
  LookupTable ARN pattern constructed by analogy to the log-group convention rather than
  confirmed against an authoritative source, since no smithy model ships with the installed
  SDK module).

- **2026-07-23 re-audit (this pass): closed the two remaining `gaps` from the prior pass
  (GetLogGroupFields sampling, PutLogEvents chronological-order/24h-span) and did a genuine
  field-diff (not a spot-check) of the previously-deferred export/import task, delivery,
  anomaly detector, and scheduled query families against the real SDK types.** This turned up
  several real, previously-unverified bugs beyond what the "spot-checked" status in the prior
  pass's `families` entry implied:
  - **Nested-vs-flat wire shape**, the most common bug class found this pass: `ExportTask`
    (`status`/`executionInfo` serialized as flat scalars instead of `{code,message}` /
    `{creationTime,completionTime}` objects) and `DeliveryDestination` (target ARN serialized
    as a flat string instead of nested under `deliveryDestinationConfiguration.destinationResourceArn`).
    Both were caught by comparing this package's hand-rolled `map[string]any` / struct-tag
    response construction against each type's real deserializer switch statement in
    aws-sdk-go-v2's generated `deserializers.go` -- the SDK's own struct field *doc comments*
    don't show wire nesting, only the deserializer's `case "key":` structure does.
  - **Wrong wire key** (right field, wrong JSON name): `ImportTask` used `"status"` instead of
    `"importStatus"`, `LogAnomalyDetector` used `"detectorStatus"` instead of
    `"anomalyDetectorStatus"`, `ScheduledQuery` used `"arn"` instead of `"scheduledQueryArn"`.
    Each of these meant a real SDK client's corresponding Go field always deserialized to its
    zero value from this backend's responses.
  - **Wrong wire key on the *input* side, more severe than an output-shape bug**:
    `PutDeliverySource`'s handler parsed `"resourceArns"` (a plural array key this backend
    invented) instead of the real `"resourceArn"` (a singular string, per
    `PutDeliverySourceInput.ResourceArn *string` and the real serializer). A real SDK client's
    request always carries `resourceArn`, so this backend's `ResourceArns` was silently empty
    for *every* real caller, not just mis-shaped in the response -- the resource association
    was completely dropped, not just displayed wrong.
  - **Invented enum values not in the real SDK**: `ImportTask`'s initial/cancellable status
    reused the shared `completenessStatusActive` constant (`"ACTIVE"`) -- correct for
    `IntegrationStatus`, but `"ACTIVE"` is not a member of `types.ImportStatus`
    (`IN_PROGRESS`/`CANCELLED`/`COMPLETED`/`FAILED`) at all, so `CancelImportTask`'s
    accepted-state check could never match a real import task's real status. A handler test
    also asserted a seeded status of `"SUCCEEDED"`, likewise not a real `ImportStatus` member
    (real: `COMPLETED`); fixed to `IN_PROGRESS`/`COMPLETED`.
  - **Orphaned invented fields with zero wire representation**: `LogAnomalyDetector` carried
    `EvaluationLookback`/`FilterAnomalies` fields with no equivalent anywhere in the real SDK
    (not in `types.AnomalyDetector`, not in any `api_op_*AnomalyDetector*.go` input, not in any
    doc comment) and no readers anywhere in this codebase either -- pure dead weight, removed
    (de-stub hygiene, same category as the `ErrInvalidSequenceToken` cleanup in the prior pass).
  - **A real feature gap disguised as "the field is just missing"**: `UpdateLogAnomalyDetector`
    never accepted `Enabled`, a *required* field on the real `UpdateLogAnomalyDetectorInput`
    used to pause/resume a detector. Since `PAUSED` is a real, reachable `AnomalyDetectorStatus`
    value that nothing in this backend ever set, a detector could never actually be paused
    through this API even though the status existed. Implemented: `enabled=false` -> `PAUSED`,
    `enabled=true` on a paused detector -> `ANALYZING` (steady-state resume, not a restart back
    to `INITIALIZING`).

  Two existing tests (`TestJanitor_SweepRetention`, `TestJanitor_SweepUpdatesStreamMetadata`)
  sent a single `PutLogEvents` batch spanning ~10 days to exercise retention-sweep eviction --
  real AWS would reject that whole batch outright once the 24-hour-span check went in, so they
  were split into two individually-valid calls (an old-events call, then a separate recent-event
  call) rather than weakening the new validation to keep the old test shape.

  Not everything found was fixed this pass -- see `gaps` for `ScheduledQuery`'s still-missing
  `GetScheduledQueryOutput` fields (including `executionRoleArn`, which is accepted as
  `CreateScheduledQuery` input and then silently discarded, never stored), `CreateDelivery`'s
  missing `FieldDelimiter`/`RecordFields`/`S3DeliveryConfiguration` input fields,
  `AccountPolicy`'s missing `AccountId`/`LastUpdatedTime`, and `DescribeDestinations`'s missing
  pagination -- these are real gaps, correctly still open, not reclassified to `ok`.
  (All four were closed in a later follow-up pass -- see the corresponding `gaps` entries above,
  now marked RESOLVED, for what changed.)
  `go build`/`go vet`/`go test -race`/`gofmt`/`golangci-lint` all clean before and after this
  pass; 0 banned (`cyclop`/`gocyclo`/`gocognit`/`funlen`) nolints, same as before.

- **PutLogEvents sequenceToken is a no-op today.** aws-sdk-go-v2 v1.64.0's own doc comments
  (api_op_PutLogEvents.go, types.InputLogEvent.UploadSequenceToken, types.InvalidSequenceTokenException)
  are explicit and repeated: "The sequence token is now ignored in PutLogEvents actions.
  PutLogEvents actions are always accepted and never return InvalidSequenceTokenException or
  DataAlreadyAcceptedException even if the sequence token is not valid." A previous audit pass
  (see the now-updated `ops_batch2_audit_test.go` comment) had deliberately added strict
  sequence-token validation, modeling the *pre-2022* contract. This sweep removed that
  validation; NextSequenceToken is still computed and returned (many older SDKs/tools still read
  it), it just no longer gates acceptance. If a future auditor sees "AWS returns
  InvalidSequenceTokenException for a stale token" in an older blog post or Stack Overflow
  answer, that's describing pre-Feb-2022 behavior -- trust the SDK doc comment over that.

- **RejectedLogEventsInfo field is `TooOldLogEventEndIndex`, not `...StartIndex`.** The real
  wire key is `tooOldLogEventEndIndex` (aws-sdk-go-v2 types.RejectedLogEventsInfo), and per the
  field doc it's an *exclusive end* index ("too-old events form a prefix of the batch; this is
  one past the last of them"), exactly mirroring `ExpiredLogEventEndIndex`'s semantics. A real
  SDK client unmarshalling our old `tooOldLogEventStartIndex` key would have silently gotten
  `nil` for `TooOldLogEventEndIndex`. Both `TooOldLogEventEndIndex` and `ExpiredLogEventEndIndex`
  are computed as `(highest matching event index) + 1`, not the first-matching index -- watch for
  this if the field is ever renamed again.

- **Metric filter `MetricValue` field-reference extraction is real, not literal-only.**
  `MetricTransformation.MetricValue` can be a literal number (published as-is) or a
  `$`-prefixed field reference: `$name` for a *named* field in a space-delimited pattern
  (`[ip, level, size]` makes `$ip`/`$level`/`$size` addressable) or `$.path` for a JSON
  selector pattern (`{ $.level = "ERROR" }` makes any `$.foo.bar` addressable, not just paths
  that appear in the match condition). This is implemented via
  `compiledFilterPattern.extract`/`extractString`/`extractValue`, fed by
  `compileJSONFilterPatternExtract` (filter_pattern_json.go) and
  `compileSpaceFilterPatternExtract` + `spaceFieldIndex` (filter_pattern_space.go, which now
  also tracks each `spaceTerm.name` -- previously discarded entirely, even for bare/unconditioned
  terms). **Trap:** `MetricTransformation.DefaultValue` is documented as "the value to emit when
  a filter pattern does NOT match a log event" (i.e. a substitute for a quiet period with zero
  matches), **not** a fallback for a matched event whose referenced field happens to be missing
  or non-numeric. The previous implementation conflated the two and used DefaultValue (or a bare
  `1.0`) as an extraction-failure fallback, fabricating metric data points that real CloudWatch
  Logs would never emit. The fix intentionally emits *nothing* for an extraction failure on a
  matched event; genuinely wiring "publish DefaultValue once per period when this filter had zero
  matches" would require a periodic scheduler (not per-PutLogEvents-call logic) and is left as a
  gap, not attempted this pass.

- **`putLogEventsMaxMessageBytes = 256 * 1024` looks suspicious next to aws-sdk-go-v2's doc
  comment ("Each log event can be no larger than 1 MB") but was deliberately left unchanged.**
  The older, still-maintained aws-sdk-go v1 model (`service/cloudwatchlogs/api.go`) explicitly
  says "Each log event can be no larger than 256 KB" in the same field's doc comment, matching
  this codebase's long-standing constant and the widely-documented CloudWatch Logs event-size
  quota. Treat the v2 SDK's "1 MB" doc text as smithy-model doc drift (likely bled in from the
  *batch* size limit description) rather than a real per-event limit increase, unless a more
  authoritative source (e.g. the AWS Service Quotas console page) is checked and says otherwise.

- **`compileFilterPattern`'s per-pattern-kind dispatch (`{`/`[`/plain-text) is the single
  source of truth for "does this pattern have addressable fields."** Anything touching
  metric-value extraction or `TestMetricFilter.ExtractedValues` should go through
  `compiledFilterPattern.extractString`/`extractValue` or `patternFieldRefs`, not re-parse the
  pattern text directly -- the existing JSON lexer/space-term parsing is reused rather than
  duplicated so the match and extract paths can't drift out of sync.

- **2026-07-11 re-audit (this pass): the diff from 17a215e4 to this commit is the "Phase 3.3"
  datalayer refactor** (backend.go/store_setup.go/region_accessors.go/persistence.go/
  janitor.go/export.go), replacing every hand-rolled `map[string]*T` / nested
  `map[string]map[string]*T` resource field with `pkgs/store.Table[T]` (+ `store.Index` for
  the region-qualified "dirty" tables: groups, streams, subscriptionFilters, metricFilters;
  events now live inline on `LogStream.events` instead of a separate `events` map). Verified
  op-by-op that every accessor was mechanically translated with no behavior change: locking
  discipline (coarse `b.mu` still guards every Table/Index access, matching `pkgs/store`'s
  "no internal locking" contract), in-place index-key-stable mutation (`PutSubscriptionFilter`
  update-in-path, `DeleteLogStream`/`applyEvictionPlan` decrementing `group.StoredBytes` through
  the same pointer), copy-before-delete-while-iterating on every `Index.Get()` consumer
  (`deleteStreamsInGroup` et al.), and DTO round-tripping of the four now-unexported identity
  fields (`region`, `logGroupName`, inline `events`) through `persistence.go`'s
  `logGroupSnapshot`/`logStreamSnapshot`/`subscriptionFilterSnapshot`/`metricFilterSnapshot`.
  No regression found; `go build`/`go vet`/`go test -race`/`golangci-lint` all clean before and
  after this pass. One hygiene fix made: `ErrInvalidSequenceToken` (backend.go) was an orphaned
  error var -- never returned by any op (PutLogEvents stopped validating sequenceToken in the
  prior sweep, see the "PutLogEvents sequenceToken is a no-op today" note above) and its
  `handler.go` errType mapping was already removed in that same prior sweep, so the var itself
  was dead code left behind; removed it (de-stub hygiene: no orphaned symbols).

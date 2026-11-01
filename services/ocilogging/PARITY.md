---
service: ocilogging
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: ee4983645
last_audit_date: 2026-10-29
overall: B
ops:
  CreateLogGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id and no body. CREATING then ACTIVE after the transition window on the injectable clock. compartmentId and displayName required (255 characters), description up to 400 characters; a displayName must be unique per compartment (409 Conflict)."}
  GetLogGroup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; bare resource body with lifecycleState and timeLastModified."}
  ListLogGroups: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName filter; sortBy timeCreated or displayName; bare JSON array, paged with opc-next-page. isCompartmentIdInSubtree is ignored (the emulator has no compartment tree)."}
  UpdateLogGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "202 with work request; displayName, description and tags apply at once and the group reports UPDATING for the transition window. If-Match gives 412 NoEtagMatch."}
  DeleteLogGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; DELETING then gone. A group that still contains logs answers 409 Conflict."}
  ChangeLogGroupCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Work request MOVE_LOG_GROUP; applies at once."}
  CreateLog: {wire: ok, errors: partial, state: ok, persist: ok, note: "CUSTOM and SERVICE logs. retentionDuration 30 to 180 in steps of 30 (default 30). A SERVICE log needs configuration.source (OCISERVICE) naming a service and category from the emulated catalog (objectstorage read/write, functions invoke, flowlogs all, loadbalancer access/error, apigateway access/execution, events ruleexecutionlog); the same service, resource and category cannot back two logs (409). A CUSTOM log rejects a source. isEnabled false makes the log INACTIVE."}
  GetLog: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; the log must belong to the addressed log group."}
  ListLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Filters logType, sourceService, sourceResource, displayName, lifecycleState; bare array."}
  UpdateLog: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, isEnabled, retentionDuration, tags, configuration.archiving and (SERVICE) source.parameters; the service, resource and category are fixed."}
  DeleteLog: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; DELETING then gone, ingested entries are dropped with the log."}
  ChangeLogLogGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves a log to another ACTIVE log group of the same compartment; work request MOVE_LOG."}
  CreateUnifiedAgentConfiguration: {wire: ok, errors: partial, state: ok, persist: ok, note: "serviceConfiguration is stored as sent. configurationType LOGGING needs at least one source with name and sourceType and a destination.logObjectId that references an existing log; MONITORING is stored without further checks. isEnabled is required."}
  GetUnifiedAgentConfiguration: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; configurationState is always VALID because invalid configurations are rejected at write time."}
  ListUnifiedAgentConfigurations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "{items:[...]} collection (the only Logging list with an envelope); filters logId, groupId, displayName, lifecycleState."}
  UpdateUnifiedAgentConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, isEnabled and serviceConfiguration are required as in the SDK."}
  DeleteUnifiedAgentConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "202 with work request."}
  ChangeUnifiedAgentConfigurationCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Work request MOVE_CONFIGURATION."}
  ListServices: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Fixed emulated catalog of six services at /v2/registry/services."}
  GetWorkRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "IN_PROGRESS until the resource transition time, then SUCCEEDED."}
  ListWorkRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; id and status filters; bare array."}
  DeleteWorkRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; an in-progress work request answers 409."}
  ListWorkRequestErrors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Always empty: no emulated operation fails asynchronously."}
  ListWorkRequestLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Accepted and succeeded entries."}
  PutLogs: {wire: ok, errors: partial, state: ok, persist: ok, note: "POST /20200831/logs/{logId}/actions/push, 200 with no body. specversion must be 1.0; each batch needs source, type and defaultlogentrytime; each entry needs data and id; the entry time falls back to defaultlogentrytime. Data longer than 10000 characters is truncated as documented. The log must be ACTIVE (enabled): CREATING, INACTIVE and DELETING logs answer 409. The request body is limited to 1 MiB (413 PayloadTooLarge). Entries are kept per log until retentionDuration days after ingestion, with at most 50000 entries per log."}
  SearchLogs: {wire: ok, errors: partial, state: ok, persist: n/a, note: "POST /20190909/search (and /actions/search). See families.query_language. timeStart and timeEnd are required (end exclusive); limit 1 to 1000 (default 1000) with opc-next-page paging; isReturnFieldInfo returns the top level field names. Results are {data:{datetime, logContent, regionId}}."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures for API keys registered through POST /_gopherstack/oci/keys or OCI Identity; accept-any otherwise, --oci-logging-enforce-auth rejects unsigned and unknown keys."}
  error_envelope: {status: partial, note: "Errors are {code,message} with opc-request-id. InvalidParameter, MissingParameter, NotAuthenticated, NotAuthorizedOrNotFound, NoEtagMatch, IncorrectState and Conflict follow the OCI reference; the status for an ingestion into a log that is not ACTIVE (409 IncorrectState) is unverified."}
  endpoints: {status: ok, note: "Management (/20200531), ingestion (/20200831) and search (/20190909) share one listener, so a single host serves LoggingManagementClient, LoggingClient and LogSearchClient."}
  lifecycle: {status: ok, note: "CREATING, UPDATING and DELETING last 2 seconds on the injectable backend clock; transitions and retention are applied on every request and by a one-minute sweeper."}
  query_language: {status: partial, note: "Supported: search \"<compartment>[/<logGroup>[/<log>]]\" with several comma separated scopes (group and log by OCID or name); | where (keyword optional) with = != <> < <= > >=, in (...), and, or, not, parentheses, string/number/true/false/null literals and * wildcards in = and != string comparisons; | sort by f [asc|desc], ...; | top N by f [asc|desc]; | limit N; | count. Fields: datetime (epoch milliseconds), logContent (the entry data as text, or a path into the CloudEvent), regionId, and CloudEvent paths data.*, source, type, subject, id, time, oracle.*. Without sort the newest entry comes first. data.* resolves when the entry data is a JSON object. Unsupported (rejected 400): select, extend, dedup, summarize, fields, head, tail, rename, timestats, join and every other operator."}
  retention: {status: ok, note: "Entries older than retentionDuration days (measured from ingestion) are dropped on the next request or sweep."}
  service_logs: {status: partial, note: "OCI Functions writes one entry per finished invocation into the enabled SERVICE log whose source is functions/<application OCID>/invoke (in-process, wired in internal/app). Other services do not emit logs yet."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-logging-port / OCI_LOGGING_PORT (default 8160); never on the shared AWS router; cli.go reserves the port in the shared pool."}
items_still_open:
  - "Log saved searches (CreateLogSavedSearch, GetLogSavedSearch, ListLogSavedSearches, UpdateLogSavedSearch, DeleteLogSavedSearch, ChangeLogSavedSearchCompartment) are not routed."
  - "The query language lacks select, extend, dedup, summarize, timestats, joins and functions; see families.query_language."
  - "The service catalog is a fixed list of six services, and only OCI Functions feeds SERVICE logs; Object Storage, Load Balancer, API Gateway, Events and flow logs never emit entries."
  - "Unified agent configurations are stored and validated but no agent consumes them, so they never produce entries."
  - "Archiving (Object Storage export of logs) is stored on the configuration but nothing is exported."
  - "Retention, the 1 MiB request limit, the 10000 character data truncation and the log naming rules are recalled from the Oracle documentation and SDK comments, not verified against the live service; the per-compartment uniqueness of log group names is an emulator choice."
  - "isCompartmentIdInSubtree is ignored, and opc-retry-token idempotency, IAM policy checks, resource principals and instance principals are not applied; only API-key signatures are verified."
  - "Tag values and defined-tag namespaces are stored verbatim, not validated against a tag namespace; systemTags are never set."
---

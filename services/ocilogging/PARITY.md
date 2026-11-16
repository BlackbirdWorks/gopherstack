---
service: ocilogging
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: ee4983645
last_audit_date: 2026-10-29
overall: B
ops:
  CreateLogGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id and no body. CREATING then ACTIVE after the transition window on the injectable clock. compartmentId and displayName required (255 characters), description up to 400 characters; a displayName must be unique per compartment (409 Conflict)."}
  GetLogGroup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; bare resource body with lifecycleState and timeLastModified."}
  ListLogGroups: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName filter; sortBy timeCreated or displayName; bare JSON array, paged with opc-next-page. isCompartmentIdInSubtree=true also lists the groups of nested compartments when OCI Identity runs in the same process (SDK ListLogGroupsRequest docs)."}
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
  ListUnifiedAgentConfigurations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "{items:[...]} collection (the only Logging list with an envelope); filters logId, groupId, displayName, lifecycleState, isCompartmentIdInSubtree (nested compartments, with OCI Identity)."}
  UpdateUnifiedAgentConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, isEnabled and serviceConfiguration are required as in the SDK."}
  DeleteUnifiedAgentConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "202 with work request."}
  ChangeUnifiedAgentConfigurationCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Work request MOVE_CONFIGURATION."}
  CreateLogSavedSearch: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 with the LogSavedSearch body and etag, ACTIVE at once (no work request); compartmentId, name and query required, description up to 400 characters, opc-retry-token replay. Shape from oci-go-sdk logging/log_saved_search.go and create_log_saved_search_details.go; the query is stored as text and not parsed."}
  GetLogSavedSearch: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; 404 NotAuthorizedOrNotFound."}
  ListLogSavedSearches: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; logSavedSearchId and name filters, sortBy timeCreated or displayName; {items:[...]} summary collection (log_saved_search_summary_collection.go)."}
  UpdateLogSavedSearch: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 with the updated body; name, description, query and tags; If-Match gives 412 NoEtagMatch."}
  DeleteLogSavedSearch: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; removed at once."}
  ChangeLogSavedSearchCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; applies at once."}
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
  query_language: {status: partial, note: "Supported: search \"<compartment>[/<logGroup>[/<log>]]\" with several comma separated scopes (group and log by OCID or name); | where (keyword optional) with = != <> < <= > >=, in (...), and, or, not, parentheses, string/number/true/false/null literals and * wildcards in = and != string comparisons; | sort by f [asc|desc], ...; | top N by f [asc|desc]; | limit N; | count. Fields: datetime (epoch milliseconds), logContent (the entry data as text, or a path into the CloudEvent), regionId, and CloudEvent paths data.*, source, type, subject, id, time, oracle.*. Without sort the newest entry comes first. data.* resolves when the entry data is a JSON object. | select (columns, * and as aliases), | extend (as alias), | dedup by one or more expressions (first row kept), | summarize count, sum, avg, min, max, first, last (with by and as; whole stream without by), scalar functions not, contains_ci, contains_cs, rounddown (datetime in ms, interval Nd|Nh|Nm|Ns), concat, upper, lower, substr (0-based), isnull, isnotnull, arithmetic + - * / and parentheses in where, select, extend, summarize (Logging query language specification page). Queries are limited to 8192 characters, 32 operators, 32 nesting levels and 10000 summarize groups. Unsupported (rejected 400): fields, head, tail, rename, timestats, join, time_format and every other operator."}
  retention: {status: ok, note: "Entries older than retentionDuration days (measured from ingestion) are dropped on the next request or sweep."}
  service_logs: {status: partial, note: "OCI Functions writes one entry per finished invocation into the enabled SERVICE log whose source is functions/<application OCID>/invoke (in-process, wired in internal/app). Load Balancer, API Gateway and Events (rule execution: ruleexecutionlog on the rule OCID, one entry per rule action with ruleId, eventId, message and target, per the Events log reference) write through the same sink. Object Storage and flow logs do not emit."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-logging-port / OCI_LOGGING_PORT (default 8160); never on the shared AWS router; cli.go reserves the port in the shared pool."}
items_still_open:
  - "Object Storage read and write access logs are not emitted: access entries need request-level facts (client address, user agent, status, requestAction) that only the HTTP handler of services/ociobjectstorage sees, and that service has no access-log hook (only object and bucket events, events.go:63 and :32). Entry shape: Logging reference details_for_object_storage."
  - "IAM policy checks, resource principals and instance principals are not applied; only API-key signatures are verified (out of scope: IAM evaluation)."
---

## Verified against Oracle documentation

- Log and log group names: 1 to 256 characters of letters, digits, hyphens, underscores and periods, log groups starting with a letter, case-sensitive: https://docs.oracle.com/en-us/iaas/Content/Logging/Task/managinglogs.htm.

## Primary-source checks (2026-11-13)
- ocilogging: logging SDK Log comment confirms retention in 30-day increments up to 180; loggingingestion LogEntry comment confirms entries under 1 MB and data fields truncated at 10,000 characters.

## Emulator choices where the vendor is silent

- The query language page documents no limit, timestats or join, and gives no format syntax for time_format, no substr indexing base and no null handling for aggregates: timestats, join and time_format are rejected, substr is 0-based, aggregates skip missing values, summarize groups appear in the order of first occurrence, rounddown returns epoch milliseconds, and the optional third boolean argument of contains_ci and contains_cs is validated and ignored.
- The service catalog is a fixed list of six services (Object Storage, Functions, flow logs, Load Balancer, API Gateway, Events).
- The 1 MiB request-body limit is an emulator choice and not published (checked loggingingestion SDK LogEntry and Logging custom_logs.htm, which state only a per-entry limit of under 1 MB); the per-compartment uniqueness of log group names is an emulator choice.
- systemTags are never set: the SDK documents only the free-tier key (`{"orcl-cloud": {"free-tier-retained": "true"}}`) and the emulated tenancy is never free tier.

## External effects that are recorded, not performed

- Unified agent configurations are stored and validated but no agent runs on a host, so they never produce entries.
- Archiving (configuration.archiving, deprecated in favour of Service Connector Hub per the SDK Archiving docs) names no destination: with it enabled each ingested entry is counted in the persisted `archivedEntries` of the log and a "would archive" line is logged; nothing is exported.
- Flow logs: the emulator carries no VCN packet traffic, so no flow log entry exists to emit.

---
service: lambda
sdk_module: aws-sdk-go-v2/service/lambda@v1.94.1
last_audit_commit: c3b5d46a
last_audit_date: 2026-07-05
overall: A   # 641 LOC genuine fixes incl. a disguised-stub no real client could call
protocol: REST-JSON
families:
  resource_policy: {status: ok, note: FIXED RemovePermission read StatementId from query string; real SDK sends URI path segment /policy/{StatementId} — route never matched (disguised stub). Added Qualifier scoping ($LATEST rejected), EventSourceToken/PrincipalOrgID fields}
  event_source_mappings: {status: ok, note: FIXED ARN parsing dropped function name (kept only qualifier); qualified ESMs now routed via InvokeFunctionWithQualifier. Pollers PROVEN — backoff, FilterCriteria, BisectBatchOnFunctionError, ReportBatchItemFailures, MaxRecordAge}
  persistence:     {status: ok, note: FIXED permissions map never snapshotted (policies lost on restore); versionIndex + esmByFunctionARN not rebuilt on Restore — now rebuilt}
  runtime_lifecycle: {status: ok, note: PROVEN — LRU eviction, async cleanup semaphore, container stop/remove, port release, dir cleanup. Real Docker exec}
  durable_execution: {status: ok, note: PROVEN — reads/writes real durableExecutionStore despite handler_stubs.go filename}
gaps: []
deferred:
  - AddPermission FunctionUrlAuthType / InvokedViaFunctionUrl / RevisionId optimistic-concurrency (lower value)
  - function CRUD/versions/aliases/layers/provisioned-concurrency/URLs/tags — skimmed, tests green, not exhaustively re-verified
leaks: {status: clean, note: event-source pollers + janitor + container lifecycle all leak-conscious; go test -race passes}
---

## Notes
- InvocationType is a type alias (type InvocationType = string) so lambda backend satisfies sns.LambdaInvoker directly.
- ARN-parsing anti-pattern "take last colon segment" recurs — watch for it elsewhere.
- Trap: RemovePermission wire = DELETE /2015-03-31/functions/{name}/policy/{StatementId} (path, not query).

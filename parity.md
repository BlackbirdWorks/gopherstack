# gopherstack Parity Audit — full 154-service deep scan

**Goal: 100% real AWS emulation (match and exceed LocalStack's open tier).**

This document is a **complete, per-service audit** of every service in `services/`
against four axes: **AWS-emulation parity**, **performance**, **resource leaks**, and
**UI/console coverage**. It replaces the previous forward-looking roadmap (that history
lives in git). Every bullet is a concrete, code-cited finding (`file:line`) representing
work required to reach full AWS fidelity. When an item lands, delete it from here.

**How this scan was produced.** Each service's Go sources under `services/<name>/`
(`handler.go`, `backend.go`, `*_ops.go`, `handler_stubs.go`, janitors, persistence) were
read directly, and the Svelte console under `ui/src/routes/<name>/` was compared against
the backend operation surface. DynamoDB (the priority service) got a dedicated deep dive
across `dynamodb`, `dynamodbstreams`, and `dax`.

**Working principles (apply to every fix):**
- **No stubs that lie.** An advertised op must mutate/return real state or return the
  AWS-accurate error. A success envelope over a no-op is a parity bug.
- **Region/account from ctxbag.** Derive region/account/partition/request-id via
  `pkgs/awsmeta` off the request context — never a hardcoded literal.
- **Opaque pagination.** `NextToken`/`Marker` must be opaque (HMAC-signed, as in
  `pkgs/page`), stable across concurrent mutation — not a raw offset, index, name, or ARN.
- **AWS error shape.** JSON-protocol errors need `__type` (and the `x-amzn-ErrorType`
  header); query/EC2-protocol errors need the right `<Code>`. Distinct conditions need
  distinct codes — don't collapse not-found, conflict, and validation into one.
- **Bounded memory.** Every map/slice that grows with traffic needs eviction, a cap, or
  a janitor sweep. Every goroutine/ticker/timer needs context-aware shutdown.

---

## Cross-cutting / systemic findings (recur across most services)

These patterns appear in the large majority of services below; fixing them centrally
(shared helpers) is the highest-leverage parity work:

1. **Non-opaque pagination tokens.** Most `List*`/`Describe*` ops encode `NextToken` as a
   raw integer offset (`strconv.Itoa(end)`), a base64'd name, or the resource ID/ARN of
   the next item — forgeable and unstable across concurrent insert/delete. AWS tokens are
   opaque and mutation-stable. Pervasive in cloudwatchlogs, batch, cleanrooms, account,
   amplify, accessanalyzer, autoscaling, athena, appmesh, and others. `pkgs/page` (already
   used by apprunner) is the correct primitive — adopt it everywhere.
2. **Full-collection copy + sort on every list call.** Nearly every `List*` clones the
   entire map into a slice and `sort.Slice`s it per request, even for one page — O(n log n)
   per call under a lock. Maintain a sorted index or sort once.
3. **Error envelope missing `__type` / wrong codes.** Many services emit `{"message": ...}`
   without `__type`, or collapse all errors to one code (athena → `InvalidRequestException`,
   batch → `ClientException`, awsconfig → `MaxNumberOfConfigurationRecordersExceeded`),
   breaking SDK error-type branching.
4. **Mock/canned data-plane responses.** bedrock / bedrockruntime return a literal
   `"This is a mock response from Gopherstack."`; ce returns synthetic cost data; many
   `Get*Results`/`*Statistics` ops return empty/zeroed payloads while advertised.
5. **Unbounded maps/slices with no eviction.** Idempotency-token maps, event/activity
   slices, session maps, and audit-report maps commonly grow forever (see per-service
   Leaks). Several janitors hold the global write lock for the full sweep.
6. **Synchronous lifecycle / no state machine.** Long-running resources (deployments,
   jobs, clusters, restores) jump straight to terminal state (`COMPLETE`/`ACTIVE`/
   `RUNNABLE`) instead of transitioning through AWS's intermediate states.
7. **Per-request handler-map rebuilds.** Several dispatchers rebuild their entire
   op→handler map (and sub-maps) on every request (apigateway, apigatewayv2, cleanrooms).

---

## Featured deep dive — DynamoDB (priority service)

DynamoDB is broadly implemented — control plane, items, transactions, streams, backups,
PartiQL, and global tables are all real (not stubs) — but it is **not yet 100%
AWS-emulated**. Highest-impact gaps: production PITR snapshots never fire (so
`RestoreTableToPointInTime` is effectively broken in prod); GSI/LSI pagination keys omit
the base-table primary key; filter/condition expressions are re-parsed per item in the
scan/query hot paths; export/import are synchronous with empty descriptions; PartiQL drops
`NextToken`/`ORDER BY`/`DuplicateItem`; streams omit `StreamViewType`/`SizeBytes` and still
accept a forgeable legacy iterator; and DAX does zero cache/TTL emulation while collapsing
all data-plane errors to `ValidationException`.


### dynamodb

- **Parity:**
  - PITR snapshots are never captured in production: `snapshotPITRTables` is only invoked from the test-only `runOnce` (janitor.go:133), not from the production `Run` loop (janitor.go:93-111). So `RestoreTableToPointInTime` restores from an empty/stale ring in any real run. (backup_ops.go:323-325 then silently treats a missing snapshot/unparseable date as "current items" instead of erroring.)
  - GSI/LSI query & scan `LastEvaluatedKey` omit base-table primary keys: `extractKey` (item_ops.go:210-219) and `buildLastKey` (item_ops_scan.go:289-296) emit only the index PK/SK. Real DynamoDB returns the index keys **plus** the table's primary key for index pagination; without the base keys, resumption is ambiguous when index sort keys repeat.
  - Non-opaque, ambiguous pagination resumption: `findExclusiveStartIndex` (item_ops.go:534-556) and scan's `applyExclusiveStartKey` (item_ops_scan.go:381-409) match the first item whose index PK/SK equals the start key. With duplicate GSI keys this skips to the wrong position.
  - PartiQL `NextToken` accepted but ignored: `executeStatementRequest.NextToken` (partiql.go:75) is never read and `executeStatementResponse.NextToken` (partiql.go:84) is never set; a SELECT over the page limit silently truncates with no continuation.
  - PartiQL `ORDER BY` silently dropped: `partiqlWhereRe` (partiql.go:54) stops at ORDER BY but no sort is applied.
  - PartiQL INSERT never raises `DuplicateItemException`: `executePartiQLInsert` (partiql.go:501) does a plain PutItem with no `attribute_not_exists` guard, silently overwriting.
  - Wrong PartiQL error codes: parse failures return a plain `errors.New` (`ErrInvalidStatement`, partiql.go:22) and BatchExecuteStatement emits `Code: "ValidationError"` (partiql.go:209) instead of AWS `ValidationException`.
  - `ExecuteTransaction` is non-atomic and never returns `TransactionConflictException`: runs statements sequentially (extra_ops.go:1303-1305); `ReturnConsumedCapacity` is read at handler.go:2385 but dropped (never set on input handler.go:2424, no `ConsumedCapacity` in output handler.go:2393).
  - `contains()` uses JSON substring instead of list/set membership: `evalContains` stringifies via `json.Marshal`/`toString` (evaluator.go:306, 485-503).
  - `ExportTableToPointInTime` returns `ExportStatus: COMPLETED` synchronously (handler.go:1226) and `exportDescriptionFields` never populates `ExportFormat`, `ExportTime`, `BilledSizeBytes`, `ItemCount`, `ExportManifest` (store.go:836-841). `ImportTable` likewise runs synchronously (extra_ops.go:1410-1426); neither ever shows `IN_PROGRESS`.
  - `ListExports` ignores `NextToken`/`MaxResults` (store.go:846, handler.go:2561). `ListImports` ignores `TableArn`, `PageSize`, `NextToken` (extra_ops.go:1482-1507). `ListBackups` ignores `TimeRangeLowerBound`/`TimeRangeUpperBound` (backup_ops.go:152-177) and does not cap `Limit` (table_ops.go:1390 does for ListTables).
  - `RestoreTableFromBackup`/`RestoreTableToPointInTime` drop GSIs, LSIs, BillingMode, SSE, StreamSpec and ignore captured `ProvisionedThroughput`, hard-coding defaults (backup_ops.go:266-280, 406-420); restore-input overrides not honored.
  - `DescribeContinuousBackups` returns only the enabled flag; `EarliestRestorableDateTime`/`LatestRestorableDateTime` never populated (handler.go:1059-1060).
  - `CreateTable` ignores `OnDemandThroughput` (table_ops.go:197-247), never appears in DescribeTable (table_ops.go:649-661). `validateProvisionedThroughput` does not reject PROVISIONED-without-throughput nor throughput with PAY_PER_REQUEST (accuracy_audit.go:1077-1101; GSI variant 1043-1075).
  - `UpdateTable` applies BillingMode and ProvisionedThroughput with no validation and allows combining multiple mutation types in one call (table_ops.go:814-872), which real DynamoDB rejects.
  - `applyReplicaUpdate` honors only TableClassOverride + read-capacity override (table_ops.go:1009-1027); ignores per-replica GSI overrides / write capacity; `applyOneReplicaTableEntry` replicates schema but copies no items (table_ops.go:904-939).
  - `UpdateTimeToLive` does not validate empty `AttributeName` and never checks the attribute exists (table_ops.go:1346-1350).
  - Reserved-word handling: bare attribute names colliding with keywords/functions (`size`, `contains`, `Add`, `In`) tokenized as keywords not identifiers (lexer.go:265-278).
  - `validateComplexValue` does not recurse into `L` list elements (validation.go:490-495), unlike `M` (validation.go:501).

- **Performance:**
  - Filter/condition expressions re-lexed and re-parsed every call: `EvaluateExpression`/`applyUpdate`/`projectItem` build fresh `NewLexer`+`NewParser` (expressions.go:18-20, 55-57, 85-87); `ExpressionCache` only caches key schema, never ASTs.
  - That re-parse runs once per item in hot scan/query loops: `passesFilter` (item_ops_scan.go:280), `shouldIncludeInQuery` (item_ops_query.go:538), `allExprPartsMatch` (item_ops_query.go:551). Projector parse-once pattern exists (expressions.go:107) but unused for filters/conditions.
  - Query/Scan snapshot copies key schema, all GSIs, LSIs, attr defs every call (item_ops_query.go:83-114, item_ops_scan.go:84-95); GSI path shallow-copies full `Items` slice (item_ops_query.go:112-113).
  - `compareValues` uses `fmt.Sprintf("%T", …)` twice per comparison (evaluator.go:530) per item per filter element.
  - `CreateBackup` holds `table.mu.RLock` while deep-copying all items (backup_interface.go:47-57); insert path O(n) name scan + O(b log b) sort per create (backup_interface.go:68-103, store.go:805-824).
  - `exportTableToS3` buffers entire gzipped table in memory before one PutObject (import_export_s3.go:307-327) on top of a full deep copy under global `db.mu.RLock` (import_export_s3.go:359-374). `importFromS3` calls `db.PutItem` once per item (import_export_s3.go:110-118, 391).
  - `DescribeTable` recomputes `estimateTableSizeBytes` over all items every call (table_ops.go:602).

- **Leaks:**
  - GSI lifecycle timers: immediate `applyGSIDelete` removes the GSI without stopping a pending `IndexStatusTimer` from a prior delayed create (table_ops.go:1192-1198) — orphaned `time.AfterFunc`.
  - Otherwise bounded/clean: Backups (cap 10_000), exports (5_000), imports (5_000), txnTokens/txnPending, exprCache and iteratorStore (LRU+TTL) all evict and are swept by the janitor (janitor.go:104-107). Caveat: janitor `recover` (janitor.go:80-85) returns without restart — a panic stops all sweeps permanently.

### dynamodbstreams

- **Parity:**
  - All four ops implemented (not stubs): ListStreams/DescribeStream/GetShardIterator/GetRecords (streams_ops.go:544/145/289/432).
  - Records never set `StreamViewType` or `SizeBytes`: `buildSDKRecord` (streams_ops.go:689-725); model lacks fields (models/types.go:309-318). AWS returns both.
  - Unknown/empty `ShardIteratorType` silently treated as TRIM_HORIZON (streams_ops.go:395) instead of ValidationException.
  - Legacy plaintext iterator `tableName:seq:timestamp` still accepted in prod (streams_ops.go:516-538) — forgeable/cross-table.
  - Shard lifecycle fabricated from ring rotation: `splitActiveShard` fires on 1000-write wrap (store.go:372, 388-402); boundaries don't match AWS ~4h close. Placeholder open shard renders empty `StartingSequenceNumber` (streams_ops.go:233-242, 270-272); `CreationRequestDateTime` nil, `StreamLabel` literal "latest" (streams_ops.go:217-222).
  - AT/AFTER sequence numbers only range-checked vs trimSeq, not shard start/end (streams_ops.go:359-412).
  - `eventID` = `<table>-<seq>` (store.go:326) not opaque hash; error `__type` uses dynamodb namespace not dynamodbstreams (errors.go:160-172); `LimitExceededException` defined but never returned.
  - Correct: opaque random tokens, image-type gating, Limit clamp 1000, nil NextShardIterator on closed shard, pagination.

- **Performance:**
  - `GetRecords` re-parses every sequence number and linearly skips below `startSeq` per call (streams_ops.go:969-976) — O(buffer) per poll.
  - `ListStreams` insertion sort O(n²) (streams_ops.go:602-608); per-table RLock in collectEnabledStreams (streams_ops.go:629-663).
  - DescribeStream/GetShardIterator copy full shard slice per call (streams_ops.go:169-170, 316-317); double allocating image conversion (streams_ops.go:730-865, handler.go:353-411).
  - Data race risk: `streamRecordsInOrder` returns slices aliasing the live ring backing array (store.go:442-446); conversion after RLock dropped (streams_ops.go:458-462) vs concurrent `overwriteRingSlot` (store.go:382).

- **Leaks:**
  - `sweepStreamRecords` 24h compaction resets ring without advancing `streamTrimSeq` (janitor.go:655-657) — trim horizon lost; valid seq reads return empty instead of TrimmedDataAccessException.
  - Bounded/clean otherwise (ring cap 1000; iterator store TTL+sweep). Same janitor-panic caveat.

### dax

- **Parity (control plane):**
  - No lifecycle state machine — clusters born `available`: `CreateCluster` sets StatusAvailable immediately (backend.go:414); Creating/Modifying/Failed declared (models.go:9-13) never assigned.
  - `DeleteCluster` synchronous/destructive: returns deleting copy then `delete(b.clusters,...)` (backend.go:638-647).
  - `RebootNode` flips node to StatusRebooting (backend.go:821) with no worker to restore — stuck forever. Increase/Decrease replication factor mutate instantly with no modifying status (backend.go:651, 727).
  - `ParameterApplyStatus` hard-wired `in-sync` (backend.go:431, handler.go:461); `UpdateParameterGroup` never marks dependent clusters pending-reboot / never populates NodeIDsToReboot (backend.go:1068, models.go:106, handler.go:392).
  - AZ-indexing bug in `IncreaseReplicationFactor`: guard tests `i < len(input.AvailabilityZones)` but indexes `AvailabilityZones[i-len(cluster.Nodes)]` (backend.go:696-697).
  - Subnet validation cosmetic: accepts any IDs, fabricates VpcID (backend.go:1679), assigns AZ `<region>a` (backend.go:1622). `RebootNode` returns NodeNotFoundFault for empty NodeId instead of InvalidParameterValueException (backend.go:797).

- **Parity (data plane):**
  - Genuinely functional (not stub): ten item methods dispatch (server.go:404-428) to a real `dynamodb.InMemoryDB` (dataplane_server.go:33); full CBOR codec.
  - DAX caching/TTL not emulated — pure pass-through (dataplane_server.go:20-24); query-ttl-millis/record-ttl-millis stored but never consulted (models.go:75-76,141-144).
  - `ConsistentRead` dropped on single GetItem/DeleteItem (ops.go:302 decoded but unused; ops.go:170, 255); BatchGetItem does pass it (batch.go:197).
  - GetItem ignores `ProjectionExpression` (ops.go:161).
  - All backend errors flattened to ValidationException/400 (ops.go:403) — ConditionalCheckFailed/ResourceNotFound/TransactionCanceled+CancellationReasons lost.
  - Transact/Batch response metadata always empty: ConsumedCapacity, ItemCollectionMetrics, UnprocessedItems/Keys (batch.go:115-129,260-264; transact.go:324-339).

- **Performance:**
  - `decodedExpression.nameRef` linear-scans names map per ref → O(n²) (expression.go:106-118).
  - `attrListID` serializes all connections on one Mutex + strings.Join+sort per call (control.go:248-271, 288-290) on every item-write response.
  - Describe* deep-copy every record before paginating under RLock (backend.go:523-528, 1032-1035, 1341-1343).
  - `writeNonKeyAttributes` allocates fresh buffer+bufio+name-sort per returned item (ops.go:367-400; itemkey.go:34-36).

- **Leaks:**
  - Unbounded `attrToID`/`idToAttr` maps grow per distinct attr-name set for the life of Server, no eviction (control.go via server.go:90-97). `schemas` cache never invalidated (server.go:90) → stale schema mis-encodes keys after drop/recreate.
  - Rebooted nodes never recover (state leak) — RebootNode no restore timer.
  - Clean: listener/conn lifecycle, per-request 30s ctx timeout, bounded events ring (1000), no data-plane channels.

### Consolidated UI (dynamodb + dax)

- **Present:** Table list + create/delete/describe; schema/GSI/LSI display + Create-GSI modal; item CRUD incl. BatchWrite delete; separate Query and Scan UIs (filter/projection/SK operators/ScanIndexForward/Limit); PartiQL editor; backups create/list/delete; PITR toggle; TTL config; streams enable + custom stream-events/shards viewer; replicas add/remove; tags; deletion protection; read-only DAX page (Clusters/Parameter Groups/Subnet Groups via Describe*).
- **Missing:**
  - Query/Scan results don't paginate with `LastEvaluatedKey` (only Items tab does, +page.svelte:401-405); Query/Scan tabs send one Limit request, no "next page".
  - No native S3 Export (`ExportTableToPointInTime`) / Import (`ImportTable`) — UI does client-side blob download / per-item PutItem loops (+page.svelte:220-228, 760-775).
  - No backup restore wiring — Restore button toasts "not supported" (+page.svelte:1851).
  - No post-create capacity/billing-mode editing (settable only at CreateTable, +page.svelte:264-272; detail only displays).
  - No transactions UI (no TransactWriteItems/TransactGetItems/ExecuteTransaction/BatchExecuteStatement in ui/src).
  - DAX page strictly read-only — no create/update/delete cluster/param-group/subnet-group; no data-traffic-through-DAX.
  - No GSI delete/update flow (only Create-GSI modal).

---

## Per-service findings (all services)

### accessanalyzer
- **Parity:** `CreateAnalyzer` discards `archiveRules`/`configuration`/`clientToken` and coerces any unknown `type` to `ACCOUNT` instead of ValidationException (handler.go:308-311).
- **Parity:** `handleCheckAccessNotGranted`/`handleCheckNoNewAccess`/`handleCheckNoPublicAccess` are stubs always returning `"result":"PASS"`, ignoring body (handler_appendixa.go:250-270).
- **Parity:** `handleValidatePolicy` always returns empty `findings`, never parses the policy (handler_appendixa.go:653-658).
- **Parity:** `StartResourceScan` is a no-op; creates no finding/resource (backend.go:471-484).
- **Parity:** `UpdateAnalyzer` is a no-op; configuration never stored, non-AWS shape (backend_appendixa.go:583-594; handler_appendixa.go:642-651).
- **Parity:** `ListFindings` ignores `filter` (backend.go:398); `ApplyArchiveRule`/`CreateArchiveRule` archive ALL active findings regardless of filter (backend.go:253-258, backend_appendixa.go:113-120).
- **Parity:** `handleGetFindingsStatistics` returns only externalAccess stats, never unusedAccess (handler_appendixa.go:413-419).
- **Parity:** `handleGetFindingRecommendation`/`handleGetGeneratedPolicy` always empty; StartPolicyGeneration immediately SUCCEEDED with no policy (handler_appendixa.go:393-396, 459-468; backend_appendixa.go:133-138).
- **Parity:** `GetFindingV2`/`ListFindingsV2` omit V2 fields; findingDetails always `[]` (handler_appendixa.go:438-448, 580-588). `ListAnalyzedResources` drops `resourceOwnerAccount` (handler_appendixa.go:545-548).
- **Parity:** Non-opaque name-based pagination everywhere; deleted token silently resets to 0 (backend.go:427-441; backend_appendixa.go:304-320, 368-386, 480-498). List handlers swallow malformed JSON (handler.go:462; handler_appendixa.go:478, 533, 568). No maxResults bounds validation. Not-found subtypes all coerced to generic ResourceNotFoundException (handler.go:579-582). `/analyzedResource` routed but dead (handler.go:98-104, 615-616). `DeleteServiceLinkedAnalyzer` no service-linked check (handler_appendixa.go:315-322).
- **Performance:** Many ARN-keyed ops resolve analyzer via full linear scan under lock, no ARN index (backend.go:477-481, backend_appendixa.go:92-98, 203-209, 280-286, 423-429, 454-460, 508-514). `ListAnalyzedResources` scans entire global map (backend_appendixa.go:351-362); List/Get deep-copy every element under RLock.
- **Leaks:** `policyGenerations`, `accessPreviews`, `analyzedResources`, `findingRecommendations` grow unbounded, no eviction (backend.go:109-112; backend_appendixa.go:141, 223, 409, 550). No goroutines/timers.
- **UI:** Route exists but only an analyzers tab (ListAnalyzers); no UI for findings, archive rules, statistics, analyzed resources, access previews, policy generation, recommendations, check/validate. Read-only, no pagination.

### account
- **Parity:** `DescribeAccount` returns canned data (admin@example.com, fake org ARN, blank JoinedTimestamp), ignores `AccountId` (backend.go:139-151).
- **Parity:** Missing ops: EnableRegion, DisableRegion, GetRegionOptStatus, PutAccountName, CloseAccount, AcceptPrimaryEmailUpdate, StartPrimaryEmailUpdate, GetPrimaryEmail (handler.go:48-58; backend.go:82-91). Regions read-only.
- **Parity:** Alternate/contact ops ignore `AccountId` for manage-on-behalf-of-member (handler.go:227-309). `ListRegions` doesn't validate `RegionOptStatusContains`, ignores invalid/negative maxResults (handler.go:198-211). GetAlternateContact/DeleteAlternateContact don't validate type enum (handler.go:227-234, 280-286). PutContactInformation zero field validation (handler.go:297-309). Name-based reversible NextToken (backend.go:200-206). Errors via substring matching, non-AWS `InvalidAction` code (handler.go:332-339).
- **Performance/Leaks:** Accurate.
- **UI:** Route exists but only a Regions tab (ListRegions). No UI for DescribeAccount, contact info, alternate contacts. Read-only.

### acm
- **Parity:** `RequestCertificate` doesn't validate `Options.CertificateTransparencyLoggingPreference` nor `ValidationMethod` — unknown methods immediately ISSUED (handler.go:545-548; backend.go:326-328, 484-486). `KeyAlgorithm` not validated; weak key → InternalFailure/500 instead of ValidationException (backend.go:1724, 1771; handler.go:993-996).
- **Parity:** Type=PRIVATE fabricated but CertificateAuthorityArn never validated; PRIVATE cert self-signed + immediately ISSUED (handler.go:541-544). `RevokeCertificate` allows revoking IMPORTED, no InUseBy guard (backend.go:1541-1550). `ExportCertificate` returns hardcoded fake PEM chain when chain missing (backend.go:769-787). `ImportCertificate` doesn't validate key/cert match, hardcodes KeyAlgorithm EC (backend.go:614-697, 676). `PutAccountConfiguration` accepts DaysBeforeExpiry out of AWS 1-45 range (backend.go:1411-1417). `ListCertificates` ignores invalid SortBy/SortOrder; positional pagination not stable (backend.go:954-986). `RenewCertificate` on IMPORTED returns RequestInProgressException instead of ValidationException (backend.go:714).
- **Performance:** `ListCertificates` deep-copies every cert + sorts before paginating even for MaxItems=1 (backend.go:956-986); `DeleteCertificate` scans entire idempotency map under write lock (backend.go:1086-1091); janitor sweeps all certs/timers under full write lock hourly (janitor.go:88-133).
- **Leaks:** Terminal-state certs never deleted, accumulate forever (janitor.go:88-104). Janitor goroutine properly shut down.
- **UI:** Broad coverage; RevokeCertificate uses hand-rolled fetch bypassing SDK (+page.svelte:279-286). No UI for UpdateCertificateOptions or GetCertificate (PEM never viewable). Request modal omits KeyAlgorithm/CertificateAuthorityArn/Options/Tags. configDays=395 default exceeds AWS max.

### acmpca
- **Parity:** `RevocationConfiguration` hardcoded empty `{}`; CRL/OCSP config dropped at create (handler.go:274, 278, 245-249). DescribeCertificateAuthority omits Serial, OwnerAccount, LastStateChangeAt, FailureReason, KeyStorageSecurityStandard, UsageMode, RevocationConfiguration contents (handler.go:276-285). CreateCertificateAuthority doesn't validate KeyAlgorithm/SigningAlgorithm enums; ROOT CA auto-signed + ACTIVE immediately skipping CSR workflow (backend.go:275-281, 308-337). DeleteCertificateAuthority ignores PermanentDeletionTimeInDays, allows deleting CA that issued certs (backend.go:405-413, 436).
- **Parity:** `IssueCertificate` ignores SigningAlgorithm/TemplateArn/ApiPassthrough; hardcodes ECDSA template (handler.go:345-350; backend.go:1147-1155). GetCertificate returns CA's own cert as chain (handler.go:739-746). RevokeCertificate allows double-revoke (backend.go:669-673). CreatePermission silently overwrites duplicate principals (backend.go:834). CreateCertificateAuthorityAuditReport writes no S3 object (backend.go:741-749). Not-found all map to ResourceNotFoundException. ListTags ignores MaxResults/NextToken (handler.go:479-485). Positional pagination not stable; invalid tokens reset to 0 (page.go:24-66).
- **Performance:** `ListCertificates` region-wide O(n) scan despite existing `certsByCASerial` index (backend.go:690-695); `ListCertificateAuthorities`/`ListPermissions` full scan + sort (backend.go:390-396, 888-894).
- **Leaks:** DeleteCertificateAuthority only flips status; certs/permissions/policies/auditReports/index persist forever (backend.go:436); auditReports never evicted (backend.go:749).
- **UI:** Route exists but no UI for IssueCertificate, GetCertificate, RevokeCertificate, ListCertificates (ledger hardcoded empty), Import/Get CA cert, UpdateCertificateAuthority (enable/disable), RestoreCertificateAuthority, DeletePolicy, audit reports, tagging. createCA hardcodes RSA_2048/SHA256WITHRSA while backend generates EC P-256.

### amplify
- **Parity:** ~24 advertised ops (Jobs/Domains/Webhooks/BackendEnvironments/Artifacts) implemented but **unroutable** — `parseAmplifyOperation` only handles `/apps` depth 1-4 + `/tags/`; everything else falls to opUnknown→404 (handler.go:63-89, 144-171, 280-299). UpdateApp/UpdateBranch also unroutable (no POST case) (handler.go:197-228, 316-349; backend.go:457, 492).
- **Parity:** `ListArtifacts` canned empty stub; artifacts map never populated so GetArtifactURL always 404s (backend.go:1234-1235, 1208). Deployment/access-log/artifact URLs fabricated pointing at real AWS (backend.go:676, 944-945, 1201-1202). Non-opaque decimal-offset pagination (backend.go:1244-1272). Errors `{"message"}` no `__type` (handler.go:670-672). createApp/createBranch don't validate platform/stage enums (handler.go:374-388, 449-463). App/branch views omit many fields (handler.go:585-610).
- **Performance:** All List ops full-map copy + sort under RLock (backend.go:209-217, 322-332, 625-638, 1040-1051, 892-901, 1150-1159).
- **Leaks:** `DeleteApp` orphans jobs, domains, webhooks, backendEnvironments forever (backend.go:223-240); jobs never GC'd.
- **UI:** Route exists but calls unrouted commands that fail at runtime (ListJobs/ListWebhooks/CreateWebhook/StartJob/ListDomainAssociations). Only ListApps/ListBranches/CreateApp/DeleteApp work. Hardcoded fake badges/metrics.

### apigateway
- **Parity:** Non-opaque integer-index pagination (backend.go:1399-1409); default page size 500 vs AWS 25 (backend.go:293, 482, 517); `embed=methods` ignored. Several list ops return full list with no position support (handler.go:3201; backend.go:2217). `runRequestValidator` only checks json.Valid, never validates model schema (proxy.go:593-608). Canned stubs: GetSdk/GetSdkType(s)/ImportApiKeys/ImportDocumentationParts/GetDomainNameAccessAssociations/UpdateUsage (handler_stubs.go:197-259). GetUsage empty Items (backend.go:3220-3225); TestInvokeMethod fixed 200/`{}` never executes integration (backend.go:2559-2570); TestInvokeAuthorizer hardcoded allow (backend.go:2978-2985); FlushStageCache no-op (handler.go:3114-3121). Coarse error model.
- **Performance:** `dispatch` rebuilds entire op→handler map via ~20 sub-constructors + maps.Copy on EVERY request (handler.go:2729, 2535-2552, 2816-2823, 3168-3173, 3317-3324). Proxy rebuilds resource-path trie per data-plane request (proxy.go:1279-1284); GetAPIKeyByValue linear scan per apiKey-required request (backend.go:1885).
- **Leaks:** `selRegexpCache` (sync.Map) keyed by user patterns has no eviction (handler.go:621; proxy.go:982-996).
- **UI:** Route exists; resources/methods read-only (no Create/PutMethod/PutIntegration). No TestInvokeMethod UI despite docs tab listing it. No request-validator/documentation/base-path-mapping/gateway-response/client-cert/VPC-link/export UI. Pagination capped client-side at 100, >100 truncated.

### apigatewaymanagementapi
- **Parity:** `GetConnection` shape wrong — returns connectionId/postedMessages/bytesSent (none in AWS) and flat sourceIp/userAgent instead of nested Identity (backend.go:134; handler.go:221; types.go:42-46). Oversized payload → plain `{message}` 413 instead of PayloadTooLargeException (handler.go:182-198). No ForbiddenException/LimitExceededException modeling. DeleteConnection returns 204 (AWS 200) (handler.go:237).
- **Performance:** `Broadcast` holds write lock for entire fan-out with per-connection make+copy while locked (backend.go:265-286); `Stats` iterates all connections per poll (backend.go:329-331).
- **Leaks:** `connections` map unbounded; no background reaper (backend.go:75-76, 140-152).
- **UI:** Route exists; no GetConnectionCommand call; server `?q=` search path dead; oversize payload not blocked client-side.

### apigatewayv2
- **Parity:** `ImportApi`/`ReimportApi` stubs — ignore OpenAPI body, hardcode Name "imported-api", create no routes (handler.go:1195-1233). `ExportApi` shape wrong (wrapper instead of Blob body), omits components/securitySchemes (handler.go:1251-1262; backend.go:3224-3290). PreviewPortal just returns GetPortal (handler.go:920-932). CreateApi lacks Target/RouteKey/CredentialsArn quick-create (models.go:159-171). Errors `{"message"}` no x-amzn-ErrorType (models.go:406-409). Missing validation (stageName, authorizationType enum, protocol, duplicate api mapping key) (backend.go:643-645, 814-821, 951-960, 1521-1560). Many list ops ignore pagination entirely (handler.go:1983-1993, 2029-2043, 1836-1851, 1893-1908, 2083-2177, 833-843).
- **Performance:** Snapshot marshals entire backend under RLock (persistence.go:40-74); sub-collection dispatch maps rebuilt per request; create ops O(n) duplicate-key scans under write lock.
- **Leaks:** `DeletePortalProduct` doesn't remove sharing-policies entry (backend.go:3049-3062); Delete ops leave empty inner maps allocated.
- **UI:** Route exists (1501 lines); no UI for Models/Integration-Route Responses/Api Mappings/Routing Rules/Portals/Import/Reimport/Export/ResetAuthorizersCache. All Update ops imported but unused. Hardcoded supportedOperations diverges from backend.

### appconfig
- **Parity:** `StopDeployment` returns 204 empty (backend.go:1558) — AWS returns the Deployment object; `ValidateConfiguration` always nil, no real validation (backend.go:1422-1441); `StartDeployment` immediately COMPLETE/100% with no async lifecycle (backend.go:898-963); `UpdateEnvironment` drops `Monitors` (handler.go:987-1014); `UpdateConfigurationProfile` drops `Validators` (handler.go:1127-1159); errors lack `__type` envelope (handler.go:800-809); extension hop-count/action-point not validated.
- **Performance:** `ListApplications` copies+sorts whole map under RLock per call (backend.go:146-163); `DeleteApplication` O(n) over all envs/profiles under write lock (backend.go:191-235).
- **Leaks:** `versionLabelIndex` outer key never pruned on profile delete (backend.go:60).
- **UI:** route exists; StopDeployment/ValidateConfiguration/Extension ops thin.

### appconfigdata
- **Parity:** `GetLatestConfiguration` skips rate limiting when PollInterval=0 (backend.go:287); LookupSession after lock release = TOCTOU (handler.go:312-346).
- **Performance:** write lock held across crypto/rand generateToken (backend.go:317-385).
- **Leaks:** janitor sweeps sessions/graceTokens — clean.
- **UI:** route exists.

### applicationautoscaling
- **Parity:** `GetPredictiveScalingForecast` all capacity/load hardcoded 10.0 (backend.go:991-1065,1048); `DescribeScalingActivities` ignores IncludeNotScaledActivities + no pagination (backend.go:279-305); paginate token is pipe-delimited sort key, non-opaque/forgeable (backend.go:455-489); scaling-policy/scheduled-action ARNs use `autoscaling` not `application-autoscaling` prefix; PutScalingPolicy/PutScheduledAction don't validate target exists (no ObjectNotFoundException); forecast timestamps RFC3339 instead of epoch.
- **Performance:** Describe* full copy+sort every call; DeregisterScalableTarget O(n) over all policies/actions (backend.go:385-436).
- **Leaks:** `scalingActivities` slice appended uncapped, never evicted (backend.go:259-275); persistence drops scalingActivities on snapshot/restore.
- **UI:** route exists; no Predictive policy type; no edit of target/policy/action (upsert unused); LoadForecast ignored; no pagination.

### appmesh
- **Parity:** client maxResults ignored, always 100 (handler.go:1211-1216); paginateStrings uses resource name as non-opaque token; `newUID` = hex of UnixNano, collides same ns (backend.go:152).
- **Performance:** sortedKeys re-sorts full key set per list, no cache; raw RWMutex not lockmetrics (backend.go:81).
- **Leaks:** tags deleted on delete — clean.
- **UI:** route exists; maxResults pagination bug.

### apprunner
- **Parity:** not-found → InvalidParameterException is intentional/correct (backend.go:22,70); services immediately RUNNING, no provisioning lifecycle. (Other detailed apprunner findings: ServiceURL uses id not name backend.go:488; handlers return fresh newOpID() not stored op id so ListOperations can't find it handler.go:306-309,371-374,399-402,470-473,498-501; ListServicesForAutoScalingConfiguration stub empty backend.go:1091-1108; AssociateCustomDomain DNSTarget wrong handler.go:1583-1588; serviceOutput omits InstanceConfiguration/SourceConfiguration etc handler.go:253-278; ListOperations insertion order not desc backend.go:700-708; revision-number reuse after delete → ARN collision backend.go:929,1208; integer-index pagination tokens.)
- **Performance:** CreateVpcConnector O(n) name scan under write lock, no index (backend.go:1349-1355); UpdateDefaultAutoScalingConfiguration O(n) under write lock; List* double full-slice copy.
- **Leaks:** none (operations capped at 200).
- **UI:** route exists; detail reads InstanceConfiguration.Cpu/Memory which backend never returns (always —); ~29 non-service ops have zero UI; create-form Port dropped by backend; no pagination.

### appstream
- **Parity:** DescribeStacks/DescribeFleets no pagination (backend.go:226-251,370-395); storedFleet missing ComputeCapacity/VpcConfig/ImageName/etc; storedStack missing AccessEndpoints/ApplicationSettings/StorageConnectors/etc; CreateStreamingURL fabricated URLs no expiry/session; CopyImage synchronous no task lifecycle.
- **Performance:** DeleteStack/DeleteFleet O(n) scan of associations under write lock (backend.go:274-294,432-453); ListAssociatedFleets O(n) no inverted index (backend.go:537-553).
- **Leaks:** `sessions` map no TTL/janitor, unbounded (backend.go:146); `exportTasks` accumulate (backend.go:109).
- **UI:** route exists; 60+ ops mostly uncovered.

### appsync
- **Parity:** `EvaluateCode` hand-rolled JS evaluator, complex code → ErrUnsupportedJSCode (jseval.go:32,262); EvaluateMappingTemplate limited VTL; ExecuteGraphQL empty if DDB/Lambda not wired; StartSchemaMerge/Associate* record association without real schema merge; Event APIs no real event bus/WebSocket; no pagination on any List op.
- **Performance:** randomAPIID/randomAPIKeyID call crypto/rand per char under mu.Lock (backend.go:282-308,414); list ops iterate full map under RLock.
- **Leaks:** janitor clean; `sourceAssocs` orphans not pruned on API delete (backend.go:326).
- **UI:** route exists; ExecuteGraphQL/EvaluateCode/merge ops uncovered.

### athena
- **Parity:** all 4 error sentinels = InvalidRequestException, indistinguishable (backend.go:39-46); ListWorkGroups/NamedQueries/DataCatalogs/PreparedStatements no pagination (backend.go:604,739,863,1230); paginateQueryExecutionIDs token = raw exec id, forgeable (handler.go:641-667); GetQueryRuntimeStatistics zeroed; ListEngineVersions/ApplicationDPUSizes static; sessions/calculations immediately IDLE/COMPLETED; SQL parser only SELECT...WHERE col=val LIMIT n, non-SELECT silently empty (backend_sql.go).
- **Performance:** ExtractResource JSON-parses whole body per request (handler.go:148-165); deriveColMeta O(k^2) column sort (backend_sql.go:117-134); janitor holds write lock for full sweep (janitor.go:91-104).
- **Leaks:** `queryResults` never evicted (janitor deletes queryExecutions only) (backend.go:999, janitor.go:95-100); sessions/calculations no sweep (backend_extra.go:266,433); tableData append-only (backend_sql.go:35).
- **UI:** route exists; extended ops (sessions/calculations/notebooks/capacity) uncovered.

### autoscaling
- **Parity:** `GetPredictiveScalingForecast` commented stub returns nil (backend.go:2499); `AlreadyExists` vs AWS `AlreadyExistsFault` (handler.go:1076); xmlAutoScalingGroup missing MixedInstancesPolicy (handler.go:1464); xmlInstance missing InstanceType (handler.go:1414); DescribeLaunchConfigurations never returns NextToken (handler.go:618); DescribeScalingActivities/ScheduledActions/Tags/AutoScalingInstances no pagination (handler.go:653,945,1002,1024); DescribeAutoScalingGroups token = base64(lastName), non-opaque (handler.go:410).
- **Performance:** DescribeAutoScalingGroups iterates+sorts whole map per call (backend.go:471); DescribeTags O(groups×tags) (backend.go:1627); DescribeAutoScalingInstances/SetInstanceHealth ignore instanceIndex, scan all (backend.go:1661,2329).
- **Leaks:** `pendingHookTokens` never evicted (backend.go:277); `expireHookAction` time.AfterFunc not context-aware, runs after shutdown (backend.go:2711); RecordLifecycleActionHeartbeat creates new AfterFunc each call without cancelling prior (backend.go:2395).
- **UI:** route exists; no UI for launch configs, scaling activities, scheduled actions, lifecycle hooks, tags, instances, policies, warm pools.

### awsconfig
- **Parity:** BatchGetAggregateResourceConfig stub returns all unprocessed (backend.go:787); BatchGetResourceConfig same (backend.go:800); DeleteEvaluationResults no-op (backend.go:693); AssociateResourceTypes ignores ResourceTypes (backend.go:763); DescribeConfigRules ignores NextToken (handler.go:549); ListConfigurationRecorders returns raw []string not summaries (handler_ext.go:851); DescribeAggregateComplianceByConfigRules/DescribeComplianceByResource/SelectAggregateResourceConfig/SelectResourceConfig empty stubs (handler_ext.go:281,336,1130,1143); ErrAlreadyExists always → MaxNumberOfConfigurationRecordersExceeded (handler.go:357).
- **Performance:** Describe* sort every call no index (backend.go:224,319,575,475).
- **Leaks:** none found.
- **UI:** route exists (+ config alias); no UI for aggregators, delivery channels, remediation, conformance packs, org rules, compliance queries.

### backup
- **Parity:** GetBackupPlanFromJSON canned empty rules (handler.go:4095); GetBackupPlanFromTemplate always "template-plan" (handler.go:4104); ListBackupPlanTemplates hardcoded empty (handler.go:4112); ~22 stub ops (handler.go:110-159): DescribeProtectedResource, GetLegalHold, ListLegalHolds, ListProtectedResources, ListRecoveryPointsByResource, StartCopyJob, StartRestoreJob, StartReportJob, StartScanJob, PutRestoreValidationResult, etc. Leftover temp file handler.go.tmp.* in dir.
- **Performance:** ListBackupVaults sorts every call (backend.go:472); ListBackupJobsFiltered/RecoveryPointsFiltered sort before paginate (backend_parity.go:103,167); paginateByID O(n) cursor scan per page (backend_parity.go:544).
- **Leaks:** none (janitor respects ctx).
- **UI:** route exists; no UI for plans, selections, recovery points, frameworks, report plans, restore testing, legal holds, vault policies/notifications, copy jobs.

### batch
- **Parity:** all 3 sentinels → ClientException/400 (handler.go:357-360), no ResourceNotFoundException; token = base64(offset), positional (backend.go:153-155); service-job ops wired but no job state simulation — jobs stay RUNNABLE forever (no janitor advances STARTING→RUNNING→SUCCEEDED).
- **Performance:** paginateSlice sorts full slice every call (backend.go:159); sweepInactiveJobDefinitions/sweepCompletedJobs hold mu for full sweep (janitor.go:94,148).
- **Leaks:** janitor clean shutdown; no unbounded maps.
- **UI:** route exists; no UI for compute environments, job queues, job definitions, job detail, consumable resources, scheduling policies, service environments.

### bedrock
- **Parity:** InvokeModel/InvokeModelWithResponseStream/Converse/ConverseStream all return hardcoded "This is a mock response from Gopherstack." (handler.go:58); seeded FM list only 5 models, missing most catalog (backend.go:652-713); CountTokens char-count heuristic not real tokenization; some AutomatedReasoning sub-ops don't advance state.
- **Performance:** ~8 List ops sort every call no index (backend.go:873,1105,1863,1981,2094,2170,2299,2421); ListTagsForResource sorts keys per call (backend.go:1290); giant combined bedrock+bedrockagent struct, 50+ counters under one lock (backend.go:390-484).
- **Leaks:** janitor clean; no unbounded maps.
- **UI:** route exists; no UI for guardrails detail, provisioned throughput, custom models, customization/evaluation jobs, inference profiles, marketplace endpoints, invocation logging.

### bedrockagent
- **Parity:** paginate token opaque but O(n) scan per page (backend.go:2711-2721); ValidateFlowDefinition always empty errors (handler op list:187); StartIngestionJob/StopIngestionJob/GetIngestionJob no progress simulation; no janitor / no session TTL eviction despite defaultIdleSessionTTLSeconds=600 (backend.go:17); agentMemory never evicted (backend.go:526).
- **Performance:** sortedKeys re-sorts per List per resource type (backend.go:2743); raw sync.RWMutex not lockmetrics; single global lock across all resource types (backend.go:526).
- **Leaks:** no janitor (handler.go:157) → agentMemory and ingestionJobs grow unbounded.
- **UI:** NO UI route (confirmed absent). All ops uncovered.

### bedrockruntime
- **Parity:** InvokeModel/Converse/etc return hardcoded mock string (handler.go:58,322,343,443); fixed mockInputTokenCount/mockOutputTokenCount=10 (handler.go:73-74); ApplyGuardrail returns response without evaluating (handler.go:284); InvokeModelWithBidirectionalStream not a real stream (handler.go:277).
- **Performance:** janitor hourly purge; ListAsyncInvokes iterates under global lock.
- **Leaks:** `tokenIndex` not cleaned by Purge — idempotency tokens accumulate unbounded (backend.go:118,196-215); asyncInvokes janitor clean.
- **UI:** route exists; no UI for async invoke list/detail, history, guardrail apply, streaming.

### ce
- **Parity:** all cost data synthetic from seedCostLedger (backend.go:41-77); GetCostAndUsage/GetCostForecast/GetReservationUtilization/etc computed from hardcoded ratios (handler.go:907,1134); static defaultStartDate/EndDate 2024-01-01/02-01 (handler.go:23-24); GetAnomalySubscriptions/Monitors/GetDimensionValues/GetTags ignore NextPageToken (handler.go:823,681,966,1013); handleError lacks __type discriminator (handler.go:349).
- **Performance:** GetCostAndUsage recomputes from full costLedger under global lock, no caching; mapToResourceTags allocs+sorts per response (handler.go:383).
- **Leaks:** janitor clean; `commitmentAnalyses` never evicted (backend.go:456).
- **UI:** route is /costexplorer; no UI for cost categories, anomaly monitors/subscriptions, allocation tags, commitment analysis, recommendations, dimension browsers.

### cleanrooms
- **Parity:** paginate token = strconv.Itoa(end), non-opaque (backend.go:632-655); ListCollaborations ignores memberStatus filter (backend.go:823); GetQueryResults stub empty rows (backend.go:1307); DescribeChangeSetHooks returns []string{} (backend.go:1015).
- **Performance:** buildOpHandlers allocates 4 maps per request in dispatch (handler.go:1297,981-988); listItems copies whole collection per page (backend.go:591-629).
- **Leaks:** none found.
- **UI:** NO route (confirmed); ~70 ops uncovered.

### cloudcontrol
- **Parity:** CancelResourceRequest returns ErrValidation not UnsupportedActionException (backend.go:338-349); applyPatch ignores nested JSON Patch paths (backend.go:576-609); ListResources O(n) prefix scan (backend.go:218-229).
- **Performance:** ListResources O(n) HasPrefix scan over all resources (backend.go:218-229).
- **Leaks:** `requests` and `clientTokens` maps unbounded, no TTL/eviction (backend.go:104-105).
- **UI:** route exists; no UI for CancelResourceRequest, ListResources detail, progress polling.

### cloudformation
- **Parity:** ListHookResults/DescribeChangeSetHooks/BatchDescribeTypeConfigurations/ListTypeVersions/ListTypeRegistrations empty stubs (handler_ops.go:1346,1356,1017,1058,1067); ListStackRefactors/Actions return []string not summaries (handler_ops.go:1220,1230; backend_ops.go:918); ListStackSetOperations/AutoDeploymentTargets/StackInstanceResourceDrifts wrong shapes / ignore params (handler_ops.go:546,606,637; backend_ops.go:262,363); DescribeEvents ignores StackName, returns all (handler_ops.go:1366); ImportStacksToStackSet ignores stackIDs (backend_ops.go:352); ListStackSets/StackInstances/StackSetOperations/GeneratedTemplates/ResourceScans ignore nextToken — no pagination (backend_ops.go:91,190,262,517,581).
- **Performance:** DescribeEvents("") copies all events all stacks under RLock (backend_ops.go:1019-1027); pruneDriftDetections O(n) per DeleteStack (backend.go:1153-1158).
- **Leaks:** all maps unbounded, no eviction for completed/deleted (backend.go:192-222).
- **UI:** route exists; no UI for stack sets, instances, drift, change sets, type mgmt, resource scans, generated templates, refactors, hook results, stack policies, signal/rollback.

### cloudfront
- **Parity:** ~80 stub ops in GetSupportedOperations (handler.go:32-147) with minimal/empty responses; ListDistributionsBy* use raw JSON substring search → false positives (backend_batch2.go:322-388); List* no Marker/MaxItems pagination (backend.go:866); GetManagedCertificateDetails stub (backend_batch2.go:457); UpdateDistributionWithStagingConfig no-op fallback (backend_batch2.go:282-297).
- **Performance:** distributionsByConfigSearch O(n×config_size) string scan under RLock (backend_batch2.go:322-331); runInvalidationReconciler ticks every 20ms taking global write lock even when idle (backend.go:615-631).
- **Leaks:** runInvalidationReconciler goroutine+20ms ticker leak if Close/Stop never called (backend.go:614-631).
- **UI:** route exists; no UI for cache/origin/response policies, OAC, FLE, realtime logs, KV stores, key groups, public keys, tenants, trust stores, streaming dists, VPC origins, monitoring subs, invalidation detail.

### cloudtrail
- **Parity:** GetQueryResults always empty (handler.go:1336); ListQueries doesn't filter by EventDataStore (handler.go:1345); ListImportFailures always empty (handler.go:1468); Deregister/RegisterOrganizationDelegatedAdmin no-ops (backend.go:959,1470); channel IDs sequential not UUID (backend.go:758).
- **Performance:** ListTags 4 sequential map lookups per ARN (backend.go:612-625); events []Event unbounded (backend.go:227).
- **Leaks:** events slice never evicted (backend.go:227).
- **UI:** route exists; no UI for event data stores, queries, import/export, channels, dashboards, insight selectors, delegated admin.

### cloudwatch
- **Parity:** GetMetricWidgetImage empty stub (handler.go:2627); ListAlarmMuteRules/ListManagedInsightRules empty (handler.go:2640,2651); PutManagedInsightRules no-op (handler.go:2662); DescribeAlarmContributors always empty (backend.go:2256); EC2/AutoScaling alarm actions no-op, only SNS+Lambda fire (backend.go:1651).
- **Performance:** SweepExpiredMetrics holds global write lock for full sweep (backend.go:511-525); recordStreamDelivery iterates all streams under write lock on every PutMetricData (backend.go:494-507).
- **Leaks:** janitor tickers clean; tags.New() per ARN without Close() — potential per-resource goroutine leak (handler.go:94,128).
- **UI:** route exists; no UI for metric streams, anomaly detectors, insight rules, alarm mute rules, managed insight rules, widget images, contributor insights.

### cloudwatchlogs
- **Parity:** ListAnomalies always empty (backend.go:3628); GetScheduledQueryHistory always empty (backend.go:3668); UpdateAnomaly no real update (backend.go:3690); BytesScanned hardcoded 0 (backend.go:2169); StopQuery no-op (backend.go:2230); nextToken = strconv.Itoa(end) non-opaque across many ops (backend.go:1212,1305,1469,1957,1979,2270,2746,2782,2813).
- **Performance:** sweepRetention holds global write lock across O(events) per group (janitor.go:86-88); retentionTargets O(regions×groups) alloc per tick (janitor.go:109-147); collectQueryEvents O(events) snapshot copy per query (backend.go:2157).
- **Leaks:** subscription delivery goroutines bounded by workerSem, clean; compiledPatterns bounded 1024.
- **UI:** route exists; no UI for export/import tasks, anomaly detectors, scheduled queries, account policies, metric filters, query definitions, deliveries, index policies, transformers.

### codeartifact
- **Parity:** GetAssociatedPackageGroup/ListAllowedRepositoriesForGroup/ListAssociatedPackages/GetPackageVersionAsset/GetPackageVersionReadme ignore params, return empty (backend.go:1219,1352,1372,1529,1549); UpdatePackageGroupOriginConfiguration takes RLock + no-op (data race) (backend.go:1331); PutPackageOriginConfiguration fabricates Package without storing (backend.go:1631); DescribePackage/DescribePackageVersion auto-create stub on read (idempotency violation) (backend.go:724,800); GetAuthorizationToken hardcoded token (handler.go:1114); unknown op → ResourceNotFoundException not UnknownOperationException (handler.go:514); error uses "code" not "__type" (handler.go:809).
- **Performance:** Tag ops O(n) scan across domains/repos/groups, no ARN index (backend.go:541-622); ListPackages O(n^2) dedup loop (backend.go:1417-1437); CountRepositoriesInDomain O(n) per Describe/DeleteDomain (backend.go:1684).
- **Leaks:** none found.
- **UI:** route exists; no UI for package groups, origin config, version assets/readme, auth tokens, allowed repos, associated packages, upstreams.

### codebuild

- **Parity:** `DescribeCodeCoverages` always returns empty `[]CodeCoverage{}` regardless of input — services/codebuild/backend.go:1936
- **Parity:** `DescribeTestCases` always returns empty `[]TestCase{}` — services/codebuild/backend.go:1941
- **Parity:** `GetReportGroupTrend` always returns empty `map[string]any{}` — services/codebuild/backend.go:1946
- **Parity:** `ListSharedProjects` and `ListSharedReportGroups` always return `[]string{}` — services/codebuild/backend.go:1951-1958
- **Parity:** `ListCuratedEnvironmentImages` returns a single hardcoded Ubuntu/Python entry instead of the full AWS image catalogue — services/codebuild/backend.go:1961
- **Parity:** `StartSandboxConnection` returns a hardcoded `wss://localhost:9999/<id>` URL instead of a real WebSocket endpoint — services/codebuild/handler.go:1560
- **Parity:** All List operation input structs (`listProjectsInput`, `listBuildsInput`, etc.) are empty — no `nextToken` or `maxResults` pagination is implemented — services/codebuild/handler.go:472-475
- **Parity:** `ResourceNotFoundException` is mapped to HTTP 400 instead of HTTP 404 — services/codebuild/handler.go:269
- **Performance:** `StartBuild` env-override merge is an O(n*m) nested loop over existing and incoming env vars — services/codebuild/backend.go:756
- **Performance:** Janitor holds the write lock for the entire sweep iteration and deletion pass — services/codebuild/janitor.go:91
- **Leaks:** Janitor only sweeps `builds`; `sandboxesByProject`, `commandsBySandbox`, and `batchesByProject` grow without bound — services/codebuild/janitor.go:91
- **UI:** `ui/src/routes/codebuild/+page.svelte` exists; sandbox/batch ops (StartBuildBatch, CreateSandbox, ExecuteCommandOnSandbox) have no UI coverage.

### codecommit

- **Parity:** `paginateStrings` and `ListRepositories` use a decimal slice index as `nextToken` (non-opaque, client-guessable) — services/codecommit/handler.go:52
- **Parity:** `repoMetadata` serialises the ARN field as `"Arn"` (capitalised) instead of the AWS wire format `"arn"` — services/codecommit/handler.go:435
- **Parity:** `GetFile` ignores `commitSpecifier` (assigned to `_`) and always reads from the HEAD tree — services/codecommit/backend_ops.go:817
- **Parity:** `GetFolder`/`GetFolderFiles` ignore `commitSpecifier` — services/codecommit/backend_ops.go:840
- **Parity:** `DeleteFile` ignores `parentCommitID` — services/codecommit/backend_ops.go:894
- **Parity:** `ListFileCommitHistory` ignores the `filePath` parameter and returns all commits for the repo — services/codecommit/backend_ops.go:1132
- **Parity:** `EvaluatePullRequestApprovalRules` always returns `satisfied: true` without checking any rules — services/codecommit/backend_ops.go:564
- **Parity:** `GetMergeConflicts` always returns `false` (no conflicts) — services/codecommit/backend_ops.go:1204
- **Parity:** `BatchDescribeMergeConflicts` stub always returns an empty conflicts array — services/codecommit/backend.go:919
- **Performance:** `GetCommentsForComparedCommit` and `GetCommentsForPullRequest` perform full O(n) linear scans over all comments — services/codecommit/backend_ops.go:683
- **Performance:** `GetBlob` performs a full O(n) linear scan over all files to find a blob by ID — services/codecommit/backend_ops.go:1113
- **Leaks:** `DeleteCommentContent` marks a comment as deleted but never removes it from the map, causing unbounded map growth — services/codecommit/backend_ops.go:746
- **UI:** `ui/src/routes/codecommit/+page.svelte` exists; PR-lifecycle ops (CreatePullRequest, MergePullRequestByFastForward, etc.) have no UI coverage.

### codeconnections

- **Parity:** `ResourceNotFoundException` is mapped to HTTP 400 instead of HTTP 404 — services/codeconnections/handler.go:228
- **Parity:** New connections are immediately set to status `"AVAILABLE"`; AWS returns `"PENDING"` until manually approved — services/codeconnections/backend.go:205
- **Parity:** New hosts are immediately set to `"AVAILABLE"`; AWS starts them as `"PENDING"` — services/codeconnections/backend.go:430
- **Parity:** `GetRepositorySyncStatus` is a stub that always returns `"SUCCEEDED"` — services/codeconnections/backend.go:696
- **Parity:** `GetResourceSyncStatus` is a stub that always returns `"SUCCEEDED"` — services/codeconnections/backend.go:724
- **Parity:** `ListRepositorySyncDefinitions` always returns an empty list — services/codeconnections/backend.go:939
- **Parity:** `UpdateSyncBlocker` is a complete no-op, discarding both `id` and `resolvedReason` — services/codeconnections/backend.go:995
- **Parity:** `updateSyncBlockerOutput` in the handler always returns empty `LatestBlockers` — services/codeconnections/handler.go:1275
- **Performance:** `validProviderTypes()` and `validSyncTypes()` allocate a new map on every call instead of using package-level constants — services/codeconnections/backend.go:101
- **Leaks:** Per-region inner maps grow unboundedly: every new region string passed in a request creates a new nested map entry that is never evicted — services/codeconnections/backend.go:262
- **UI:** `ui/src/routes/codeconnections/+page.svelte` exists; sync-blocker and repository-sync ops have no UI coverage.

### codedeploy

- **Parity:** `CreateDeployment` immediately marks the deployment as `"Succeeded"` with a faked `completeTime = now + 5s` rather than simulating a real lifecycle — services/codedeploy/backend.go:879
- **Parity:** `ContinueDeployment` validates existence but performs no state change, silently ignoring the lifecycle transition — services/codedeploy/backend.go:1323
- **Parity:** `BatchGetDeploymentInstances` returns stubbed `InstanceSummaryItem` with hardcoded status `"Succeeded"` for every requested ID — services/codedeploy/backend.go:1244
- **Parity:** `BatchGetDeploymentTargets` returns stubbed `DeploymentTargetItem` with hardcoded status `"Succeeded"` and type `"instanceTarget"` — services/codedeploy/backend.go:1268
- **Parity:** `BatchGetApplicationRevisions` only validates that the application exists and returns revision count; it does not store or retrieve actual revision data — services/codedeploy/backend.go:1164
- **Parity:** `ListApplications`, `ListDeploymentGroups`, and `ListDeployments` have no pagination support (`listApplicationsInput` is empty) — services/codedeploy/handler.go:354
- **Parity:** `validComputePlatforms()` allocates a new map on every call at validation time — services/codedeploy/backend.go:1131
- **Leaks:** `onPremisesInstances` map: `AddTagsToOnPremisesInstances` auto-registers unknown instance names on every call, growing the map unboundedly — services/codedeploy/backend.go:1144
- **UI:** `ui/src/routes/codedeploy/+page.svelte` exists; instance/target ops (GetDeploymentInstance, GetDeploymentTarget, BatchGetDeploymentInstances) have no UI coverage.

### codepipeline

- **Parity:** `ListPipelines` handler ignores the `NextToken`/`MaxResults` fields from `listPipelinesInput` and always returns all pipelines — services/codepipeline/handler.go:516
- **Parity:** All error sentinels map to HTTP 400; `PipelineNotFoundException`/`ActionTypeNotFoundException`/`StageNotFoundException` should be HTTP 404 — services/codepipeline/handler.go:314
- **Parity:** `GetPipelineExecution` returns a synthetic stub `{Status: "Succeeded"}` for any unknown execution ID instead of returning a not-found error — services/codepipeline/backend.go:1296
- **Parity:** `RetryStageExecution` ignores `stageName` (assigned to `_`) and does not actually retry any stage actions — services/codepipeline/backend.go:1447
- **Parity:** `RollbackStage` ignores both `stageName` and `targetExecutionID` and returns a synthetic new execution — services/codepipeline/backend.go:1468
- **Parity:** `OverrideStageCondition` ignores `stageName` and `executionID` and is a no-op — services/codepipeline/backend.go:1489
- **Parity:** `StopPipelineExecution` ignores the `reason` parameter (assigned to `_`) — services/codepipeline/backend.go:1317
- **Parity:** Webhook `ListTagsForResource` always returns an empty slice; webhook tags are not stored — services/codepipeline/backend.go:679
- **Performance:** `cpParseNextToken` converts decimal index tokens (non-opaque, client-guessable), deviating from AWS's opaque cursor format — services/codepipeline/handler.go:50
- **Performance:** `DeleteCustomActionType` holds the write lock while iterating over all pipelines and all stages to check for references — services/codepipeline/backend.go:873
- **Leaks:** `executions` and `actionExecutions` slices grow without bound on every `StartPipelineExecution` call — no eviction or size cap exists — services/codepipeline/backend.go:1244
- **UI:** `ui/src/routes/codepipeline/+page.svelte` exists; job-polling ops (PollForJobs, AcknowledgeJob, PutJobSuccessResult) have no UI coverage.

### codestarconnections

- **Parity:** `ResourceNotFoundException` is mapped to HTTP 400 instead of HTTP 404 — services/codestarconnections/handler.go:220
- **Parity:** New connections are immediately set to `"AVAILABLE"`; AWS returns `"PENDING"` until the console handshake is completed — services/codestarconnections/backend.go:470
- **Parity:** New hosts are immediately set to `"AVAILABLE"`; AWS returns `"PENDING"` — services/codestarconnections/backend.go:612
- **Performance:** `validProviderTypes()`, `validSyncTypes()`, `validPublishDeploymentStatus()`, and `validTriggerResourceUpdateOn()` each allocate a new map on every call — services/codestarconnections/backend.go:101
- **Performance:** `syncConfigHasReferenceToLinkLocked` iterates over all sync configs on `DeleteRepositoryLink` with no index — services/codestarconnections/backend.go:424
- **Performance:** `connectionHasReferenceToHostLocked` iterates over all connections on every `DeleteHost` call with no index — services/codestarconnections/backend.go:411
- **Leaks:** Per-region inner maps across 10 nested maps grow unboundedly for each unique region string — services/codestarconnections/backend.go:241
- **UI:** `ui/src/routes/codestarconnections/+page.svelte` exists; sync-configuration and repository-link ops have no UI coverage.

### cognitoidentity

- **Parity:** `ListIdentityPools` uses the pool name as the `nextToken` cursor value (non-opaque, returns human-readable name) — services/cognitoidentity/backend.go:414
- **Parity:** `GetOpenIDToken` ignores the `logins` parameter (assigned to `_`) and returns a synthetic token without validating any login provider tokens — services/cognitoidentity/backend.go:641
- **Parity:** `GetCredentialsForIdentity` generates synthetic credentials with a random (non-STS) `SecretAccessKey` unrelated to any real IAM role — services/cognitoidentity/backend.go:614
- **Parity:** `mergeExistingIdentity` performs an O(n) linear scan over all identities in a pool to find a matching login — services/cognitoidentity/backend.go:544
- **Parity:** `lookupOrCreateDeveloperIdentity` also scans all identities in a pool linearly — services/cognitoidentity/backend.go:854
- **Performance:** `DeleteIdentities` rebuilds the `identitiesByPool` slice for every deleted identity with an O(n) filter pass — services/cognitoidentity/backend.go:793
- **Leaks:** Per-region nested maps (`pools`, `poolsByName`, `poolsByARN`, `identities`, etc.) grow without eviction for each new region string — services/cognitoidentity/backend.go:167
- **UI:** `ui/src/routes/cognitoidentity/+page.svelte` exists; developer-identity ops (GetOpenIDTokenForDeveloperIdentity, LookupDeveloperIdentity, MergeDeveloperIdentities) have no UI coverage.

### cognitoidp

- **Parity:** `randomAlphanumeric` in backend.go silently swallows `crypto/rand.Int` errors and substitutes the first alphanumeric character, weakening token randomness under error conditions — services/cognitoidp/backend.go:2222
- **Parity:** ~50 operations in `completenessDispatchTable` (AdminGetDevice, AdminLinkProviderForUser, AdminListDevices, etc.) are no-op stubs that accept any input and return empty responses — services/cognitoidp/handler_completeness.go:13
- **Parity:** `AssociateSoftwareToken` always returns the hardcoded RFC 6238 test secret `"JBSWY3DPEHPK3PXP"` instead of generating a per-user TOTP secret — services/cognitoidp/handler_completeness.go:236
- **Parity:** `AdminListUserAuthEvents` always returns `[]map[string]any{}` — no auth event history is stored — services/cognitoidp/handler_completeness.go:146
- **Parity:** `AdminLinkProviderForUser` is a no-op that does not link any identity provider — services/cognitoidp/handler_completeness.go:116
- **Performance:** `ListUsers` is not shown to use an index; it likely iterates over all users in the pool map on every call — services/cognitoidp/backend.go (ListUsers function).
- **Performance:** `sweepExpiredRefreshTokens` holds the write lock across the entire expiry scan plus all deletions rather than releasing between batches — services/cognitoidp/janitor.go:75
- **Leaks:** `mfaSessions` map is only cleaned by the janitor and `EvictExpiredMFASessions`; a sufficiently fast flood of unanswered challenges will grow the map faster than the 30s sweep interval can drain it — services/cognitoidp/backend.go:244
- **Leaks:** `tokenRevokedBefore` map entries are never purged after pool deletion or user deletion — services/cognitoidp/backend.go:159
- **UI:** `ui/src/routes/cognitoidp/+page.svelte` exists; completeness-pass stubs (AdminGetDevice, WebAuthn ops, UserImportJob ops) have no UI coverage.

# Audit Report: group07
Services: comprehend, databrew, datasync, detective, directoryservice, dlm, dms, docdb

---

### comprehend

- **Parity:** `listJobs` returns all jobs with no pagination and never emits `NextToken`, violating the AWS list-jobs contract. — services/comprehend/handler.go:379
- **Parity:** `listResources` similarly returns every resource with no pagination or `NextToken`. — services/comprehend/handler.go:440
- **Parity:** `detectSyntax` tags every single token as `"NOUN"` regardless of actual part of speech, producing entirely wrong POS output. — services/comprehend/handler.go:773
- **Parity:** `detectToxicContent` only checks for the literal word `"hate"` and cans all other toxic labels (INSULT, GRAPHIC, etc.) with hardcoded 0.99/0.01 scores. — services/comprehend/handler.go:868
- **Parity:** `detectPIIEntities` recognises only EMAIL and SSN patterns; PHONE_NUMBER, CREDIT_DEBIT_NUMBER, IP_ADDRESS, and a dozen other PII types are never detected. — services/comprehend/handler.go:740
- **Parity:** `dominantLanguage` maps any text with more non-ASCII than Latin characters to language code `"fr"`, producing wrong results for Arabic, Chinese, Japanese, etc. — services/comprehend/handler.go:855
- **Parity:** `handleError` maps `ErrConflict` to HTTP 400 instead of the correct HTTP 409 Conflict. — services/comprehend/handler.go:183
- **Parity:** `ListResources` calls `advanceTrainingResource` (which mutates job state) as a side-effect of a read-only list operation. — services/comprehend/backend.go:325
- **Performance:** `positiveWordList` and `negativeWordList` allocate fresh string slices on every `detectSentiment` call (hot-path allocs). — services/comprehend/handler.go:553, 560
- **Performance:** `detectEntities` performs an O(n×m) string-scan per word through several suffix/prefix lists, blowing up on long documents. — services/comprehend/handler.go:668
- **Leaks:** `StartFlywheelIteration` acquires a read lock via `GetResource`, releases it, then acquires a write lock — classic TOCTOU allowing another goroutine to delete the resource in between. — services/comprehend/backend.go:380
- **UI:** Route `ui/src/routes/comprehend/` exists; async job ops (StartDominantLanguageDetectionJob, StartEntitiesDetectionJob, etc.) have no corresponding UI panel.

---

### databrew

- **Parity:** `handleBatchDeleteRecipeVersion` is an explicit no-op that always returns success without deleting any version. — services/databrew/handler.go:1506
- **Parity:** `handleDeleteRecipeVersion` returns the recipe name without removing any version from state. — services/databrew/handler.go:1521
- **Parity:** `handleStartProjectSession` is a stub that returns only the project name with no session state. — services/databrew/handler.go:1548
- **Parity:** `handleSendProjectSessionAction` is a stub returning only the project name, ignoring the action payload. — services/databrew/handler.go:1560
- **Parity:** `MaxResults` is decoded as a `string` field and then parsed with `strconv.Atoi` rather than as a JSON integer, breaking clients that send it as a number. — services/databrew/handler.go:895
- **Parity:** `ListJobRuns` uses the RunID string directly as the `nextToken` value (non-opaque, name-based pagination). — services/databrew/backend.go:970
- **Performance:** `FindTagsByArn` does a sequential O(n) linear scan across all 6 resource-type maps to locate a resource by ARN. — services/databrew/backend.go:1242
- **Performance:** `UpdateTagsByArn` similarly scans all 6 resource-type maps sequentially on every tag update. — services/databrew/backend.go:1286
- **Leaks:** `runDelayed` uses `time.After`, which allocates a timer that cannot be stopped or GC'd early if the caller is cancelled. — services/databrew/backend.go:347
- **UI:** Route `ui/src/routes/databrew/` exists; project-session ops (StartProjectSession, SendProjectSessionAction, ViewFrame) and recipe-version ops have no UI.

---

### datasync

- **Parity:** `handleError` returns HTTP 400 for `ResourceNotFoundException` instead of the AWS-correct HTTP 404. — services/datasync/handler.go:225
- **Parity:** `StartTaskExecution` sets execution status to `"LAUNCHING"` and never auto-transitions it to `SUCCESS`, so executions are permanently stuck. — services/datasync/backend.go:726
- **Parity:** `CancelTaskExecution` deletes the execution record entirely instead of setting its status to `"CANCELLED"` or `"ERROR"`. — services/datasync/backend.go:763
- **Parity:** `ListTaskExecutions` does not validate that the supplied `taskArn` exists as a real task before returning results. — services/datasync/backend.go:804
- **Parity:** `handleListTagsForResource` sorts tag keys internally but returns a `map[string]string` whose JSON serialisation is unordered, losing the sort guarantee. — services/datasync/handler.go:866
- **Performance:** `ListAgents`, `ListLocations`, and `ListTasks` each build a full sorted copy of all ARNs on every call. — services/datasync/backend.go:457, 564, 686
- **Leaks:** Tags are stored redundantly in both `storedAgent.Tags` and the global `b.tags` map; mutations to one do not propagate to the other, risking divergence. — services/datasync/backend.go:387-406
- **UI:** Route `ui/src/routes/datasync/` exists; task-execution lifecycle ops (DescribeTaskExecution, CancelTaskExecution) have no UI coverage.

---

### detective

- **Parity:** `ListIndicators` always returns `nil, "", nil` — no indicators are ever populated, making investigation indicator data permanently empty. — services/detective/backend.go:886
- **Parity:** `ListGraphs` uses the graph ARN string itself as `nextToken` (non-opaque, name-based). — services/detective/backend.go:340
- **Parity:** `ListMembers` uses the accountID string as `nextToken` (non-opaque). — services/detective/backend.go:525
- **Parity:** `ListInvitations` uses `inv.GraphARN` as `nextToken` (non-opaque). — services/detective/backend.go:724
- **Parity:** `GetInvestigation` returns `ErrMemberNotFound` when an investigation is not found — wrong error sentinel (should be an investigation-not-found error). — services/detective/backend.go:785
- **Parity:** Investigation `Severity` is always hardcoded to `"INFORMATIONAL"` regardless of any findings or scoring logic. — services/detective/backend.go:758
- **Parity:** `CreateGraph` idempotently returns the first existing graph, but AWS allows multiple graphs per account — the idempotency logic is semantically wrong. — services/detective/backend.go:256
- **Parity:** `EnableOrganizationAdminAccount` selects an arbitrary graph ARN via non-deterministic map iteration. — services/detective/backend.go:1046
- **Performance:** `classifyPath` builds a new `postPathOps` map literal on every incoming request (hot-path alloc). — services/detective/handler.go:296
- **UI:** Route `ui/src/routes/detective/` exists; investigation ops (CreateInvestigation, ListIndicators, GetInvestigation) have no UI panel.

---

### directoryservice

- **Parity:** `DescribeDirectories` uses the directory ID string directly as `nextToken` (non-opaque, name-based pagination). — services/directoryservice/backend.go:524
- **Parity:** `DescribeSnapshots` similarly uses the snapshot ID string as `nextToken` (non-opaque). — services/directoryservice/backend.go:746
- **Parity:** `ListTagsForResource` uses the tag key string as `nextToken` (non-opaque; breaks if any key is deleted). — services/directoryservice/backend.go:906
- **Parity:** `RestoreFromSnapshot` only sets the directory stage to `Restoring` with no auto-transition back to `Active`, leaving the directory stuck. — services/directoryservice/backend.go:824
- **Parity:** `BackendSnapshot` serialises only `directories`, `snapshots`, and `aliases`; all other region state (trusts, certificates, IP routes, log subscriptions, etc.) is silently dropped on serialise/restore. — services/directoryservice/backend.go:948-958
- **Parity:** `newStoredDirectory` sets `Stage` immediately to `Active`; real AWS directories start in `Requested` → `Creating` → `Active` lifecycle. — services/directoryservice/backend.go:268
- **Performance:** `cascadeDeleteDirectory` iterates over all entries in every per-directory resource map (ip routes, regions, trusts, certs, etc.) on each directory delete — O(n) across all resources per map. — services/directoryservice/backend.go:405-479
- **Leaks:** Appendix-A region state (AD assessments, hybrid AD updates, domain controllers, LDAPS settings, etc.) is never serialised in `BackendSnapshot`, so it leaks across test resets. — services/directoryservice/backend.go:190-199
- **UI:** Route `ui/src/routes/directoryservice/` exists; SSO, trust, certificate, and conditional-forwarder ops have no UI coverage.

---

### dlm

- **Parity:** `handleUpdateLifecyclePolicy` request struct omits `PolicyDetails`, making it impossible to update policy details via this endpoint. — services/dlm/handler.go:296
- **Parity:** `GetLifecyclePolicies` iterates over a map and returns results in non-deterministic order with no pagination. — services/dlm/backend.go:165
- **Parity:** `counter` field (used for sequential policyID generation) is not serialised or restored in `Restore()`, causing policyID collision risk after a snapshot restore. — services/dlm/backend.go:315
- **Parity:** `CreateLifecyclePolicy` stores tags in both `storedPolicy.Tags` and `b.tags[policyARN]`; a mutation to one copy will not reflect in the other. — services/dlm/backend.go:130-133
- **Parity:** `handleREST` returns HTTP 501 for unknown operations instead of the AWS-standard HTTP 400 (UnrecognizedClientException). — services/dlm/handler.go:161
- **Performance:** `isKnownResource` performs an O(n) scan of all policies by ARN string comparison on every tag operation. — services/dlm/backend.go:275
- **UI:** Route `ui/src/routes/dlm/` exists; all CRUD operations are implemented in the UI.

---

### dms

- **Parity:** `handleDescribeMetadataModel` is a no-op stub that always returns an empty struct. — services/dms/handler.go:3094
- **Parity:** `handleDescribeMetadataModelAssessments`, `handleDescribeMetadataModelChildren`, `handleDescribeMetadataModelConversions`, `handleDescribeMetadataModelCreations`, `handleDescribeMetadataModelExportsAsScript`, `handleDescribeMetadataModelExportsToTarget`, and `handleDescribeMetadataModelImports` all return empty lists unconditionally. — services/dms/handler.go:3113, 3132, 3151, 3170, 3189, 3208, 3227
- **Parity:** `handleDescribeEvents` is a stub returning an empty event list, so DMS event history is never surfaced. — services/dms/handler.go:2902
- **Parity:** `handleDescribeRecommendations` always returns an empty recommendations list regardless of any assessment run. — services/dms/handler.go:3452
- **Parity:** `handleDescribeSchemas` always returns an empty schema list regardless of endpoint/connection state. — services/dms/handler.go:3780
- **Parity:** `handleDescribeFleetAdvisorDatabases` is a no-op stub returning an empty database list. — services/dms/handler.go:2986
- **Parity:** `handleDeleteFleetAdvisorDatabases` silently ignores the requested database IDs and always returns an empty list. — services/dms/handler.go:2281
- **Parity:** `DescribeReplicationInstances` uses a plain `dmsPaginate` (index-based marker) while `DescribeEndpoints` uses HMAC-signed pagination — the two core resources have inconsistent pagination security. — services/dms/handler.go:908-910, 1040-1042
- **Performance:** `DeleteEndpoint` holds the write lock while scanning the entire replication-tasks map to check for references — O(n) tasks scan under the global lock. — services/dms/backend.go:784
- **UI:** Route `ui/src/routes/dms/` exists; Fleet Advisor, metadata-model, and schema-discovery ops have no UI coverage.

---

### docdb

- **Parity:** `DescribeDBClusterParameterGroups` response omits pagination (`Marker`) even though the handler does not apply `applyDocDBMarker`, so large parameter-group lists are silently truncated. — services/docdb/handler.go:713-718
- **Parity:** `DescribeDBEngineVersions` response wraps versions directly without a `Marker` field, making the response non-paginated. — services/docdb/handler.go:837-840
- **Parity:** `DescribeGlobalClusters` has no pagination (`Marker`) applied, while the AWS API supports it. — services/docdb/handler.go:868
- **Parity:** `DescribeDBClusterParameters` always returns a hardcoded two-parameter list (`tls`, `ttl_monitor`) regardless of the parameter group contents. — services/docdb/backend.go:1738
- **Parity:** Tags for clusters and instances are stored in two places — `cluster.Tags`/`instance.Tags` AND `b.tagsStore(region)[arn]`; `AddTagsToResource` only updates the `tagsStore`, leaving the embedded `.Tags` map stale. — services/docdb/backend.go:610-611, 1394-1416
- **Parity:** `DescribePendingMaintenanceActions` always returns an empty list, suppressing any pending maintenance that real AWS would surface. — services/docdb/backend.go:1877
- **Parity:** `ModifyDBClusterParameterGroup` accepts the call but makes no changes to any parameter values — it is a no-op that returns the unchanged parameter group. — services/docdb/backend.go:1274-1289
- **Performance:** `GetClusterMembers` acquires a separate read lock and scans all instances on every `DescribeDBClusters` call (one extra scan per cluster in the response). — services/docdb/backend.go:979
- **Leaks:** `DescribeCertificates` returns a hardcoded static certificate list; the returned slice is never bounded, so callers cannot differentiate between "no certs" and "certs not found by ID". — services/docdb/backend.go:1695-1721
- **UI:** Route `ui/src/routes/docdb/` exists; global-cluster ops (CreateGlobalCluster, FailoverGlobalCluster, ModifyGlobalCluster, SwitchoverGlobalCluster) and event-subscription ops have no UI coverage.

# Audit Report: group08

Services: ec2, ecr, ecs, efs, eks, elasticache, elasticbeanstalk, elasticsearch

---

### ec2

- **Parity:** `handleStubCreateFleet` (and ~200 other stubs) return `stubResponse{Return: true}` with no real response fields, so CreateFleet, CreateVpnConnection, all IPAM ops, all TrafficMirror ops, all LocalGateway ops, and all CapacityReservation ops are no-ops — `services/ec2/handler_stubs.go:59`
- **Parity:** `handleDescribeImages` ignores pagination — no `NextToken`/`MaxResults` support — so large AMI lists are always returned in full, deviating from AWS page limits — `services/ec2/handler_ext.go:655`
- **Parity:** `DescribeInstances` `NextToken` is a raw integer offset (`strconv.Itoa(offset+maxResults)`) making it non-opaque and exploitable — `services/ec2/handler.go:618`
- **Parity:** `reconcileInstanceLifecycle` transitions ALL instances atomically every 50ms including stopped/terminated instances that do not need re-evaluated — `services/ec2/backend.go:495-508`
- **Parity:** Stub responses always set `<return>true</return>` with no per-operation shape (e.g. `CreateFleet` should return `FleetId`, `Errors`, `Instances`) — `services/ec2/handler_stubs.go:9-13`
- **Performance:** `reconcileInstanceLifecycle` acquires a write lock every 50ms and iterates all instances even when none are in transitional states, creating unnecessary contention — `services/ec2/backend.go:495-508`
- **Performance:** `DescribeTags` iterates all `b.tags` and builds a `filterSet` map on every call even when `resourceIDs` is empty, allocating unnecessarily — `services/ec2/backend.go:1442-1468`
- **Performance:** `DeleteVpc` (line ~1055) iterates all instances, ENIs, subnets, route tables and SGs in separate O(n) passes holding the write lock throughout — `services/ec2/backend.go:1055-1110`
- **Leaks:** `StartLifecycleReconciler` starts a goroutine guarded by `lifecycleOnce` but if `StopLifecycleReconciler` is never called the goroutine runs forever with no ctx cancellation — `services/ec2/backend.go:467-483`
- **UI:** UI route exists at `ui/src/routes/ec2/+page.svelte`; stubs for CreateFleet, all VPN/VPG ops, all IPAM ops, all Traffic Mirror ops have no backend state and thus no UI representation.

---

### ecr

- **Parity:** `handleDescribeRepositories` uses the repository name directly as the `nextToken` cursor, which is non-opaque and leaks internal naming — `services/ecr/handler.go:553-566`
- **Parity:** `handleListImages` constructs the next token as `imageDigest + ":" + imageTag`, a non-opaque concatenation — `services/ecr/handler.go:970-990`
- **Parity:** `handleGetAuthorizationToken` always returns `dummyPassword = "dummy-password"` regardless of registry or caller — `services/ecr/handler.go:636`
- **Parity:** `DescribePullThroughCacheRules` accepts no `nextToken`/`maxResults` parameters, so it always returns all rules with no pagination — `services/ecr/handler.go:1168-1200`
- **Parity:** `BatchGetRepositoryScanningConfiguration` returns `ScanFrequency` based solely on the `ScanOnPush` bool; real AWS returns one of `SCAN_ON_PUSH`, `CONTINUOUS_SCAN`, `MANUAL` — `services/ecr/backend.go:886-894`
- **Parity:** `GetDownloadURLForLayer` returns `ErrRepositoryNotFound` when the layer is missing, but real AWS returns `LayerInaccessibleException` or `LayersNotFoundException` — `services/ecr/backend.go:1006`
- **Performance:** `DeleteRepository` scans the entire `b.layerUploads` map O(n) to clean up uploads keyed by repo, rather than maintaining a per-repo index — `services/ecr/backend.go:588-592`
- **Performance:** `deleteByTagLocked` falls back to O(n) linear scan for images without a tag index entry — `services/ecr/backend.go:655-676`
- **Performance:** `InitiateLayerUpload` performs an O(n) scan of all layerUploads on every call to prune entries older than `layerUploadTTL` — `services/ecr/backend.go:1033-1038`
- **Leaks:** No leak concerns identified beyond the O(n) pruning already noted above.
- **UI:** UI route exists at `ui/src/routes/ecr/+page.svelte`; `GetAuthorizationToken`, `BatchCheckLayerAvailability`, `UploadLayerPart`, `CompleteLayerUpload`, and `PullThroughCache` ops have no UI surface.

---

### ecs

- **Parity:** `handleCreateDaemon` ignores all input fields and returns `daemonStub{Status: statusActive}` — `services/ecs/handler_stubs.go:56-58`
- **Parity:** `handleDiscoverPollEndpoint` always returns the hardcoded string `"https://ecs-a-1.us-east-1.amazonaws.com/"` regardless of cluster or region — `services/ecs/handler_stubs.go:164-170`
- **Parity:** `handleSubmitAttachmentStateChanges`, `handleSubmitContainerStateChange`, `handleSubmitTaskStateChange` all return `{Acknowledgment: "ACK"}` with no state tracking — `services/ecs/handler_stubs.go:178-198`
- **Parity:** `handleDescribeServiceRevisions` always returns an empty list regardless of input — `services/ecs/handler_stubs.go:143-150`
- **Parity:** All Daemon ops (CreateDaemon, DeleteDaemon, DescribeDaemon, ListDaemons, RegisterDaemonTaskDefinition, UpdateDaemon, etc.) are fully no-op stubs — `services/ecs/handler_stubs.go:1-199`
- **Performance:** `enrichCluster` iterates all tasks in the cluster map on every `DescribeClusters` call to compute `RunningTasksCount`/`PendingTasksCount` with no cached counter — `services/ecs/backend.go:511-534`
- **Performance:** `Purge` iterates all task definitions and all revisions in a nested loop, holding the write lock for the full duration — `services/ecs/backend.go:392-408`
- **Leaks:** The reconciler goroutine in `reconciler.go:57` and janitor goroutine in `janitor.go:46` both use channels for stop signals — correctly structured with `defer ticker.Stop()`.
- **UI:** UI route exists at `ui/src/routes/ecs/+page.svelte`; stub Daemon ops, DiscoverPollEndpoint, and SubmitStateChange ops have no UI representation.

---

### efs

- **Parity:** `CreateMountTarget` does not populate `VpcID`, `AvailabilityZoneName`, or `AvailabilityZoneID` on the created mount target (all remain empty strings) — `services/efs/backend.go:879-889`
- **Parity:** `DescribeEnvironmentHealth` equivalent: EFS has no lifecycle simulation — newly created file systems are immediately `available` with no `creating` phase to match real AWS — `services/efs/backend.go:581-606`
- **Performance:** `CreateFileSystem` idempotency check performs an O(n) linear scan of all file systems to match the `CreationToken` — `services/efs/backend.go:581-606`
- **Performance:** `DeleteFileSystem` performs O(n) scans of all mount targets and access points to check for conflicts before deleting — `services/efs/backend.go:706-723`
- **Performance:** `CreateMountTarget` performs an O(n) scan of all mount targets to check for duplicate subnet conflict — `services/efs/backend.go:850-858`
- **Performance:** `paginate[T]` does an O(n) linear scan to find the marker position in a sorted slice instead of binary search — `services/efs/backend.go:1711-1722`
- **Leaks:** No goroutine leaks found; EFS backend uses no background goroutines or timers.
- **UI:** UI route exists at `ui/src/routes/efs/+page.svelte`; replication, backup policy, account preferences, and resource tags ops have no UI representation.

---

### eks

- **Parity:** `DeleteCluster` cascades deletion of all nodegroups immediately, whereas real AWS requires nodegroups to be deleted first and returns `ResourceInUseException` otherwise — `services/eks/backend.go:537`
- **Parity:** No validation that nodegroup `nodeRole` or `subnets` are non-empty, which are required fields in the real AWS API — `services/eks/backend.go:591-700`
- **Parity:** `stableID` uses FNV-1a 32-bit hash (8-hex-char output) for ARN uniqueness, creating a 1-in-4 billion collision space for ARN generation — `services/eks/backend.go:1246`
- **Performance:** `findTagsForARNLocked` performs O(n×m) nested ARN scans across clusters, nodegroups, access entries, addons, fargate profiles, pod identity associations, and subscriptions — `services/eks/backend.go:887-967`
- **Performance:** `findTagInNodegroupsLocked`, `findTagInAccessEntriesAndAddonsLocked`, `findTagInProfilesAndAssocLocked` each do a separate nested O(n×m) scan through their respective maps — `services/eks/backend.go:887-946`
- **Leaks:** `CreateCluster` launches `time.AfterFunc(100ms, ...)` to transition CREATING→ACTIVE; if the cluster is deleted before the timer fires, the goroutine will try to write to a deleted cluster while holding the write lock — `services/eks/backend.go:443-450`
- **Leaks:** `CreateNodegroup` has the same `time.AfterFunc(100ms, ...)` race where the nodegroup may be deleted before the timer fires — `services/eks/backend.go:687-697`
- **UI:** UI route exists at `ui/src/routes/eks/+page.svelte`; access entries, EKS addons, Fargate profiles, pod identity associations, and OIDC provider ops have no UI representation.

---

### elasticache

- **Parity:** ElastiCache error sentinels use bare `errors.New("CacheClusterNotFound")` strings rather than `awserr.New(...)`, so AWS SDK error-type assertions will fail — `services/elasticache/backend.go:70-86`
- **Parity:** `globalReplicationGroups` map is NOT region-nested (correctly global) but is shared across all `InMemoryBackend` instances per process without synchronization to a global store, so multi-region backends will each have their own isolated view — `services/elasticache/backend.go:556`
- **Parity:** `DescribeReservedCacheNodes` and reserved node offering operations are present but the offering catalog is not seeded at startup, so `DescribeReservedCacheNodesOfferings` will always return empty — checked via `StorageBackend` interface at `services/elasticache/backend.go:251-513`
- **Performance:** `updateActions []*UpdateAction` slice grows unbounded — there is no cap or eviction when update actions complete — `services/elasticache/backend.go:571`
- **Performance:** `createClusterLocked` calls `miniredis.Run()` under the write lock for embedded-mode clusters, blocking all other operations for the duration of the Redis listener startup — `services/elasticache/backend.go:856`
- **Leaks:** Embedded `*miniredis.Miniredis` instances in `Cluster.mini` are closed on `DeleteCluster` (line 949) and `DeleteReplicationGroup` (line 1841), but if the backend is `Reset()` in tests, existing `mini` instances are replaced without calling `Close()` first, leaking their goroutines.
- **UI:** UI route exists at `ui/src/routes/elasticache/+page.svelte`; serverless cache ops, global replication groups, user/user-group management, and reserved node purchasing have no UI representation.

---

### elasticbeanstalk

- **Parity:** `resourceCreatedAt = "2026-01-01T00:00:00Z"` is used as the creation/update timestamp for ALL resources and ALL events — every application, environment, and event returns the same fixed timestamp — `services/elasticbeanstalk/backend.go:62`
- **Parity:** `ErrNotFound = awserr.New("ClientException", ...)` uses the wrong AWS error code; real Elastic Beanstalk uses `ResourceNotFoundException` or service-specific codes like `S3LocationNotInServiceRegionException` — `services/elasticbeanstalk/backend.go:32`
- **Parity:** `DescribeEnvironmentHealth` returns `"Grey"/"Terminated"` for a not-found environment name instead of returning an error, diverging from AWS behavior — `services/elasticbeanstalk/backend.go:1423-1429`
- **Parity:** `appendEvent` assigns the hardcoded `resourceCreatedAt` timestamp to every event, so all events appear to have the same time regardless of when they occurred — `services/elasticbeanstalk/backend.go:1546-1555`
- **Parity:** `configTemplateKey` concatenates app name and template name with `":"` as separator, which collides when an app name itself contains `":"` — `services/elasticbeanstalk/backend.go:246`
- **Parity:** `Application` and `Environment` structs have no `DateUpdated` field; the real AWS API returns `DateUpdated` separately from `DateCreated` — `services/elasticbeanstalk/backend.go:140-185`
- **Performance:** `CheckDNSAvailability` performs an O(n) linear scan of all environments — `services/elasticbeanstalk/backend.go:1233-1239`
- **Performance:** `AssociateEnvironmentOperationsRole` and `DisassociateEnvironmentOperationsRole` each perform separate O(n) scans of all environments — `services/elasticbeanstalk/backend.go:1213-1220` and `1439-1448`
- **Leaks:** `events map[string][]*EventRecord` slice grows unbounded per application with no cap or ring buffer — `services/elasticbeanstalk/backend.go:185`
- **UI:** UI route exists at `ui/src/routes/elasticbeanstalk/+page.svelte`; managed platform versions, composed environment operations, `RebuildEnvironment`, and `RequestEnvironmentInfo`/`RetrieveEnvironmentInfo` ops have no UI representation.

---

### elasticsearch

- **Parity:** `domainCopy` sets `cp.Tags = nil`, so any caller receiving a domain copy (e.g. `DescribeElasticsearchDomain`) cannot read the domain's tags — `services/elasticsearch/backend.go:799-803`
- **Parity:** `GetUpgradeHistory`, `GetUpgradeStatus`, `DescribeDomainAutoTunes`, and `DescribeDomainChangeProgress` all return empty/nil with no upgrade state tracked — `services/elasticsearch/backend.go:1209-1255`
- **Parity:** `CancelElasticsearchServiceSoftwareUpdate` and `StartElasticsearchServiceSoftwareUpdate` are no-ops that return the current domain state unchanged — `services/elasticsearch/backend.go:776-789` and `1262-1275`
- **Parity:** `DescribeReservedElasticsearchInstanceOfferings` returns a single hardcoded offering `"offer-t3-small-1y"` — `services/elasticsearch/backend.go:1298-1306`
- **Parity:** All four error sentinels (`ErrDomainNotFound`, `ErrConnectionNotFound`, `ErrPackageNotFound`, `ErrVpcEndpointNotFound`) share the same underlying string `"ResourceNotFoundException"`, making them indistinguishable with `errors.Is` — `services/elasticsearch/backend.go:53-66`
- **Performance:** `ListPackagesForDomain` has O(n×m) complexity: iterates `packageAssociationsStore` and calls `slices.Contains` (O(m)) for each package association — `services/elasticsearch/backend.go:1025-1041`
- **Performance:** `PurchaseReservedElasticsearchInstanceOffering` calls `DescribeReservedElasticsearchInstanceOfferings()` while holding the write lock, acquiring a second lock layer — `services/elasticsearch/backend.go:1351`
- **Leaks:** No goroutine leaks found; Elasticsearch backend uses no background goroutines or timers.
- **UI:** UI route exists at `ui/src/routes/elasticsearch/+page.svelte`; upgrade history, auto-tune management, software update operations, reserved instance purchasing, and cross-cluster connections have no UI representation.

# Audit Group 09: elb, elbv2, emr, emrserverless, eventbridge, firehose, fis, forecast

### elb

- **Parity:**
  - `DescribeInstanceHealth` always returns `"InService"` for every registered instance regardless of actual health state: `services/elb/backend.go:1659`
  - `DescribeLoadBalancerPolicies` ships only 4 canned SSL sample policies via `builtinSamplePolicies()`; AWS returns many more built-in policies: `services/elb/backend.go:1761`
  - `DescribeAccountLimits` returns only 3 hardcoded limit entries with fixed values and no `NextMarker` pagination (AWS supports pagination): `services/elb/backend.go:1612`
  - `RequestID` values are constructed from resource names (e.g. `"elb-" + name`) rather than opaque UUIDs, leaking internal identifiers to callers: `services/elb/handler.go:299`
- **Performance:**
  - `DescribeLoadBalancers` with no filter copies and sorts the entire LB map on every call: `services/elb/backend.go:744`
  - `AddTags` acquires the write lock then iterates all existing tags via `lb.Tags.Range` while holding the lock for each LB in a multi-LB batch, serializing other writers for the duration: `services/elb/backend.go:884`
- **Leaks:**
  - Offset-based pagination in `handleDescribeLoadBalancers` encodes a numeric slice index as the marker; concurrent creates or deletes between pages silently shift results: `services/elb/handler.go:344`
- **UI:**
  - UI page exists at `ui/src/routes/elb/+page.svelte`; ops `DescribeLoadBalancerPolicyTypes`, `DescribeAccountLimits`, `ApplySecurityGroupsToLoadBalancer`, and `DescribeLoadBalancerAttributes` have no corresponding UI controls.

---

### elbv2

- **Parity:**
  - `handleGetTrustStoreCaCertificatesBundle` and `handleGetTrustStoreRevocationContent` are stubs returning an empty `Location: ""` without verifying the trust store exists: `services/elbv2/handler.go:1965` and `services/elbv2/handler.go:1987`
  - `handleGetResourcePolicy` returns an empty policy string without verifying the resource ARN exists: `services/elbv2/handler.go:1952`
  - `handleModifyCapacityReservation` and `handleModifyIPPools` validate that the LB exists but otherwise return empty responses, persisting no data: `services/elbv2/handler.go:2009` and `services/elbv2/handler.go:2030`
  - `handleDescribeCapacityReservation` returns canned static data (hardcoded `DecreaseRequestsRemaining: 5`, empty `LastModifiedTime`) instead of persisted state: `services/elbv2/handler.go:1638`
  - `allSSLPolicies()` exposes only 6 SSL policies; AWS ships many more named policies: `services/elbv2/handler.go:1703`
- **Performance:**
  - `checkAllArnsFound`, `checkAllTGArnsFound`, and `checkAllLBNamesFound` are each O(n²)—for each queried name they linearly scan the result slice: `services/elbv2/backend.go:888`, `services/elbv2/backend.go:908`, `services/elbv2/backend.go:948`
  - `tgToLBArnsLocked` scans all listeners and all rules on every `DescribeTargetGroups` call with no secondary index: `services/elbv2/backend.go:1426`
  - `probeTargetHTTP` allocates a new `http.Client` on every probe invocation, preventing connection reuse: `services/elbv2/backend.go:587`
- **Leaks:**
  - `NewInMemoryBackend` starts `runHealthReconciler` in a background goroutine with no corresponding `Close()` call path when the handler is torn down without calling `Close()`: `services/elbv2/backend.go:455`
  - `runHealthReconciler` ticker fires every 40 ms unconditionally regardless of whether any targets are pending: `services/elbv2/backend.go:471`
- **UI:**
  - UI page exists at `ui/src/routes/elbv2/+page.svelte`; ops `DescribeCapacityReservation`, `ModifyCapacityReservation`, `GetTrustStoreCaCertificatesBundle`, `GetTrustStoreRevocationContent`, `GetResourcePolicy`, and `SetIpAddressType` have no corresponding UI controls.

---

### emr

- **Parity:**
  - `handleCancelSteps` always returns an empty `CancelStepsInfoList: []any{}` with no per-step cancellation status, diverging from AWS which returns per-step outcomes: `services/emr/handler.go:750`
  - `validateReleaseLabel` rejects any label not in a hardcoded allowlist of ~14 entries; real EMR accepts many more valid labels: `services/emr/backend.go:805`
  - `handleGetOnClusterAppUIPresignedURL` and `handleGetPersistentAppUIPresignedURL` return a static URL from `GetPresignedURL` without verifying the cluster or resource exists: `services/emr/handler_missing.go:218`
  - Error mapping maps `ErrNotFound` to `InvalidRequestException` (HTTP 400) where AWS uses `ClusterNotFoundException`: `services/emr/handler.go:304`
- **Performance:**
  - `ListClusters` sorts all clusters on every call with an O(n log n) sort instead of maintaining insertion order: `services/emr/backend.go:1097`
  - `gatherClusterSummaries` calls `maps.Clone` for each cluster's `StateChangeReason` on every `ListClusters` invocation: `services/emr/backend.go:1143`
  - `buildInstanceGroups` uses `b.counter.Load()` (not `Add`) for instance group IDs, producing non-unique IDs under concurrent calls: `services/emr/backend.go:824`
- **Leaks:**
  - Janitor is started with `go h.janitor.Run(ctx)` inside `StartWorker`, detaching it from the handler lifecycle and preventing clean shutdown via `Shutdown()`: `services/emr/handler.go:57`
  - Region stores (`clusters`, `studios`, etc.) are lazily created and never garbage-collected when empty, causing region keys to accumulate in the outer maps indefinitely.
- **UI:**
  - UI page exists at `ui/src/routes/emr/+page.svelte`; ops `RunJobFlow`, `DescribeJobFlows`, `SetVisibleToAllUsers`, `PutBlockPublicAccessConfiguration`, and all Studio CRUD ops beyond `ListStudios` have no UI controls.

---

### emrserverless

- **Parity:**
  - Unknown HTTP path returns HTTP 404 with `ResourceNotFoundException` instead of a routing error or `UnknownOperationException`: `services/emrserverless/handler.go:461`
  - `emrPaginate` encodes `nextToken` as a plain decimal integer offset; tokens are predictable and unstable under concurrent mutation: `services/emrserverless/backend.go:766`
  - `StartApplication` rejects transitions from STOPPED→STARTED but permits re-starting a CREATING or STOPPING application without error, diverging from AWS state machine: `services/emrserverless/backend.go:337`
  - `GetDashboardForJobRun` returns a hardcoded console URL rather than a presigned dashboard URL; AWS returns a real presigned URL: `services/emrserverless/backend.go:646`
  - `ListJobRunAttempts` always returns exactly one synthetic attempt with no `ReleaseLabel` and an empty `StateDetails`; AWS returns the actual per-attempt history: `services/emrserverless/backend.go:676`
- **Performance:**
  - `ListApplications` holds the read lock while sorting the entire application list on every call: `services/emrserverless/backend.go:271`
  - `CreateApplication` checks for name uniqueness by iterating all existing applications (O(n) scan) rather than maintaining a name index: `services/emrserverless/backend.go:202`
- **Leaks:**
  - No goroutines or background workers — no leak issues found.
- **UI:**
  - UI page exists at `ui/src/routes/emrserverless/+page.svelte`; ops `GetApplication`, `StartApplication`, `StopApplication`, `DeleteApplication`, `CancelJobRun`, `StartSession`, `TerminateSession`, and `GetDashboardForJobRun` have no UI controls (only list/summary views are shown).

---

### eventbridge

- **Parity:**
  - `paginate` encodes `nextToken` as a plain decimal integer offset; this token is non-opaque and breaks on concurrent modifications: `services/eventbridge/backend.go:1304`
  - `CreateEventBus` quota check counts only the current region's buses against `maxEventBusesPerAccount`, but the limit is per-account not per-region on real AWS: `services/eventbridge/backend.go:704`
  - `ListEventBuses` ignores the `Limit` field from the request input, always paging at the hard-coded `defaultLimit = 100`: `services/eventbridge/backend.go:785`
  - Scheduler and ArchiveJanitor goroutines are started inside `StartWorker` but `Shutdown` calls `Backend.Close()` only; if no scheduler/janitor is attached, in-flight delivery goroutines still have independent cancellation via the backend ctx (correct), but `scheduler.Run` and `archiveJanitor.Run` contexts are derived from the `StartWorker` parameter and not cancelled by `Shutdown`: `services/eventbridge/handler.go:176`
- **Performance:**
  - `PutEvents` acquires the write lock and holds it while capturing events into archives (calling `captureEventInArchives`) before releasing — archive fan-out under the write lock serializes all concurrent `PutEvents` calls: `services/eventbridge/backend.go:1206`
  - `ListEventBuses` allocates and sorts all buses on every call with no secondary index: `services/eventbridge/backend.go:776`
- **Leaks:**
  - Handler `tags` map grows without bound; entries for deleted buses and rules are never removed even though the resources no longer exist: `services/eventbridge/handler.go:136`
  - `patternCache` (`sync.Map`) grows without bound because compiled patterns are never evicted on `Reset` in the backend: `services/eventbridge/backend.go:1349`
- **UI:**
  - UI page exists at `ui/src/routes/eventbridge/+page.svelte`; ops for Pipes, Endpoints, Schema Registry (`CreateSchema`, `ListSchemas`, etc.), `PutPermission`/`RemovePermission`, `DescribeReplay`, and `ListApiDestinations` have no UI controls.

---

### firehose

- **Parity:**
  - `StartDeliveryStreamEncryption` and `StopDeliveryStreamEncryption` are no-op stubs that set `Status` but persist no real encryption configuration and return an empty response: `services/firehose/handler.go:185`
  - `ListDeliveryStreams` ignores the `ExclusiveStartDeliveryStreamName` and `Limit` query fields; all streams in the region are returned in a single page: `services/firehose/backend.go:555`
  - `UpdateDestination` only updates the S3 destination; requests to update HTTP endpoint, Redshift, OpenSearch, or Splunk destinations are silently ignored: `services/firehose/backend.go:700`
  - `DescribeDeliveryStream` does not include `DeliveryStreamEncryptionConfiguration` in the response even after `StartDeliveryStreamEncryption` is called: `services/firehose/handler.go:185`
- **Performance:**
  - `intervalFlusher` acquires the read lock once per tick to scan all streams across all regions regardless of how many streams exist; with many regions this becomes a hot-path full-map scan every second: `services/firehose/backend.go:750`
  - `ListDeliveryStreams` sorts all stream names on every call with no maintained order: `services/firehose/backend.go:562`
- **Leaks:**
  - Kinesis source pollers launched via `launchKinesisPoller` store their cancel funcs in `pollerCancel`; if `DeleteDeliveryStream` is called while the poller goroutine is between its `context.Done()` check and its next poll, the goroutine can outlive its cancel invocation by one poll cycle: `services/firehose/backend.go:497`
  - `RunFlusher` launches an `intervalFlusher` goroutine that never returns if `ctx` is not cancelled; callers must pass a properly-cancelled context, but `NewInMemoryBackend` defaults to `context.Background()`: `services/firehose/backend.go:745`
- **UI:**
  - UI page exists at `ui/src/routes/firehose/+page.svelte`; ops `TagDeliveryStream`, `UntagDeliveryStream`, `ListTagsForDeliveryStream`, and `UpdateDestination` have no UI controls.

---

### fis

- **Parity:**
  - `ListExperimentTemplates`, `ListExperiments`, and `ListActions` all use ID-based cursor pagination via `paginateWithToken`; the cursor is the ID of the last item on the page — if that item is deleted, the next-page token points to a gap and silently rewinds to page 0: `services/fis/handler.go:1436`
  - `isValidRoleArn` only checks for `"arn:aws:iam::"` prefix and `":role/"` substring, accepting malformed ARNs like `"arn:aws:iam:://role/"`: `services/fis/backend.go:601`
  - `GetSafetyLever` uses the accountID as the lever ID; if a request arrives with a different accountID path segment the backend returns `ErrSafetyLeverNotFound` rather than providing a default lever per account: `services/fis/backend.go:240`
  - `ListExperimentResolvedTargets` returns only the count of resource ARNs in `ResourceArns`, not the actual resolved ARNs; AWS returns individual `ResolvedArn` entries per resolved resource: `services/fis/backend.go:1030`
- **Performance:**
  - `DeleteExperimentTemplate` iterates the entire `tplClientTokens` map to find and remove all tokens pointing at the deleted template, O(n) in idempotency token count: `services/fis/backend.go:752`
  - `ListExperimentTemplates` and `ListExperiments` each clone every template/experiment on every list call regardless of pagination window: `services/fis/backend.go:762`
- **Leaks:**
  - Each `StartExperiment` spawns a `go b.runExperiment(expCtx, ...)` goroutine using `context.Background()` as parent; if `Shutdown` is called before the experiment completes, `StopAllExperiments` cancels the goroutines, but only via the stored `cancel()` — any goroutine that has already exited its `select` but not yet returned will run for one more action cycle: `services/fis/backend.go:848`
  - `expClientTokens` and `tplClientTokens` maps grow without bound; only template deletion cleans up template tokens, and experiment tokens are never cleaned up: `services/fis/backend.go:753`
- **UI:**
  - UI page exists at `ui/src/routes/fis/+page.svelte`; ops `CreateTargetAccountConfiguration`, `UpdateTargetAccountConfiguration`, `DeleteTargetAccountConfiguration`, `GetTargetAccountConfiguration`, `UpdateSafetyLeverState`, and `GetSafetyLever` have no UI controls.

---

### forecast

- **Parity:**
  - `delete` only transitions the resource to `statusDeleting` and leaves it in the map; the resource remains visible to subsequent `describe` and `list` calls, diverging from AWS which removes deleted resources: `services/forecast/backend.go:253`
  - `ErrNotFound` maps to HTTP 400 (`ResourceNotFoundException`) instead of HTTP 404, matching AWS Forecast's actual behavior of returning 400 for not-found but using `message` and `__type` fields correctly: `services/forecast/handler.go:360`
  - `GetAccuracyMetrics` returns synthetic deterministic metrics derived from an FNV hash of the ARN; quantile losses and RMSE are synthetic and do not reflect any actual training data: `services/forecast/backend.go:432`
  - `DeleteResourceTree` only marks the root resource as `DELETING`; dependent child resources (datasets, import jobs, forecasts) are not transitively deleted as AWS would do: `services/forecast/backend.go:407`
- **Performance:**
  - `UpdateResourceStatus` (used by `ResumeResource` and `StopResource`) iterates all resources across all kinds to find one by ARN — O(n) full-map scan with no ARN index: `services/forecast/backend.go:388`
  - `cloneMap` uses round-trip JSON marshal/unmarshal to deep-copy every resource on every read; for large `Data` maps this is expensive: `services/forecast/backend.go:359`
  - `list` sorts all resources of a kind on every call with no maintained order: `services/forecast/backend.go:268`
- **Leaks:**
  - Deleted resources (status `DELETING`) remain in the `resources` map forever since `delete` never calls `delete(items, name)`; the map grows without bound: `services/forecast/backend.go:253`
- **UI:**
  - UI page exists at `ui/src/routes/forecast/+page.svelte`; ops `CreateDataset`, `CreateDatasetImportJob`, `CreatePredictor`, `CreateForecast`, `DeleteResourceTree`, `GetAccuracyMetrics`, `TagResource`, `UntagResource`, and all `WhatIf*` operations have no UI controls (only list views for four resource types are shown).

### fsx

- **Parity:**
  - `handleDeleteBackup` returns lifecycle state `"DELETED"` instead of `"DELETING"`, inconsistent with every other delete handler in the service. `/home/user/gopherstack/services/fsx/handler.go:487`
  - `CopySnapshotAndUpdateVolume` is a no-op that returns the target volume completely unchanged, never applying the snapshot data. `/home/user/gopherstack/services/fsx/backend_resources.go:791`
  - `ReleaseFileSystemNfsV3Locks` only validates the filesystem exists and releases nothing. `/home/user/gopherstack/services/fsx/backend_resources.go:1300`
  - `StartMisconfiguredStateRecovery` only validates the filesystem exists and changes no state. `/home/user/gopherstack/services/fsx/backend_resources.go:1312`
  - `DeleteFileSystem` does not cascade-delete child SVMs, volumes, or snapshots, leaving orphaned resources that can never be listed or deleted. `/home/user/gopherstack/services/fsx/backend.go:461`
  - No `Filters` parameter is supported in any `Describe*` call, causing filter-based lookups to silently return all resources. `/home/user/gopherstack/services/fsx/backend.go:433`

- **Performance:**
  - `arnExists` iterates all 8 resource maps linearly on every tag mutation (`TagResource`/`UntagResource`/`ListTagsForResource`), making tagging O(n) across all resources. `/home/user/gopherstack/services/fsx/backend.go:800`
  - `DescribeFileSystems` pagination resolves the `nextToken` by O(n) linear scan through a sorted slice instead of a map or opaque cursor. `/home/user/gopherstack/services/fsx/backend.go:433`

- **Leaks:**
  - None identified.

- **UI:**
  - No `ui/src/routes/fsx/` directory exists; all FSx operations have zero UI coverage.

---

### glacier

- **Parity:**
  - `CreateVault` silently returns the existing vault when a duplicate name is created instead of returning a `409 ResourceInUseException`. `/home/user/gopherstack/services/glacier/backend.go:296`
  - `handleArchiveJobOutput` returns an empty `200 OK` body when archive data is absent (e.g. after a restart), rather than `404 ResourceNotFoundException`. `/home/user/gopherstack/services/glacier/handler.go:1257`
  - `ListVaults` uses the vault name as a pagination marker, making it non-opaque and breakable by renames. `/home/user/gopherstack/services/glacier/handler.go:759`
  - `ListJobs` uses the job ID directly as the pagination marker token instead of an opaque cursor. `/home/user/gopherstack/services/glacier/handler.go:1093`
  - `paginateUploadList` uses `MultipartUploadID` as the marker value exposed to callers, which is non-opaque. `/home/user/gopherstack/services/glacier/handler.go:1880`
  - `ErrVaultNotEmpty` is mapped to HTTP `409 Conflict` with error code `InvalidParameterValueException` instead of the correct `ConflictException`. `/home/user/gopherstack/services/glacier/handler.go:2073`

- **Performance:**
  - `ListJobs` acquires a full write lock (`b.mu.Lock`) solely to call `promoteJobIfReady`, even for callers that only need read access to the job list. `/home/user/gopherstack/services/glacier/backend.go:619`

- **Leaks:**
  - `archiveData` in the Handler struct and its separate `archiveMu` mutex are decoupled from the backend mutex; concurrent operations on archive data and backend vault state can interleave without a unified lock. `/home/user/gopherstack/services/glacier/handler.go:133`

- **UI:**
  - No `ui/src/routes/glacier/` directory exists; all Glacier operations have zero UI coverage.

---

### glue

- **Parity:**
  - `handleCheckSchemaVersionValidity` always returns `Valid: true` regardless of the input schema or version. `/home/user/gopherstack/services/glue/handler_stubs.go:269`
  - `handleCreateScript` always returns empty `PythonScript` and `ScalaCode` strings, never generating a script from the provided DAG. `/home/user/gopherstack/services/glue/handler_stubs.go:667`
  - `handleDeleteSchemaVersions` always returns an empty errors list, never actually deleting versions or validating they exist. `/home/user/gopherstack/services/glue/handler_stubs.go:1196`
  - `handleDescribeEntity` always returns an empty `Fields` slice, providing no entity metadata. `/home/user/gopherstack/services/glue/handler_stubs.go:1361`
  - `handleDescribeInboundIntegrations` always returns an empty integration list. `/home/user/gopherstack/services/glue/handler_stubs.go:1379`
  - `handleDeleteConnectionType` is a no-op that accepts any input and returns success without deleting anything. `/home/user/gopherstack/services/glue/handler_stubs.go:959`
  - `handleDeleteIntegrationResourceProperty` is a no-op. `/home/user/gopherstack/services/glue/handler_stubs.go:1038`
  - `handleDeleteIntegrationTableProperties` is a no-op. `/home/user/gopherstack/services/glue/handler_stubs.go:1048`
  - `handleGetDataflowGraph` always returns empty `DagNodes` and `DagEdges`, ignoring any submitted script or mapping. `/home/user/gopherstack/services/glue/handler_stubs.go:1883`
  - `handleGetEntityRecords` always returns an empty `Records` list with no filtering or pagination. `/home/user/gopherstack/services/glue/handler_stubs.go:1935`

- **Performance:**
  - None identified beyond the stubs.

- **Leaks:**
  - None identified.

- **UI:**
  - No `ui/src/routes/glue/` directory exists; all Glue operations have zero UI coverage.

---

### guardduty

- **Parity:**
  - `UpdateFindingsFeedback` is a complete no-op: it discards both `findingIDs` and `feedback` after validating the detector exists. `/home/user/gopherstack/services/guardduty/backend.go:667`
  - `GetFindingsStatistics` only tracks Low/Medium/High severity buckets and omits Critical and Informational, causing those findings to be silently miscounted as High. `/home/user/gopherstack/services/guardduty/backend.go:642`
  - `ListFindings` ignores the `FilterCriteria` parameter entirely, always returning all finding IDs for the detector. `/home/user/gopherstack/services/guardduty/backend.go:531`
  - `CreateSampleFindings` always uses a hardcoded severity value (`defaultFindingSeverity`) regardless of the finding type requested. `/home/user/gopherstack/services/guardduty/backend.go:594`

- **Performance:**
  - `CreateIPSet` does an O(n) linear scan over all existing IP sets to detect duplicate names instead of using a name-keyed index map. `/home/user/gopherstack/services/guardduty/backend.go:696`
  - `CreateThreatIntelSet` performs the same O(n) duplicate-name scan as `CreateIPSet`. `/home/user/gopherstack/services/guardduty/backend.go:820`

- **Leaks:**
  - None identified.

- **UI:**
  - No `ui/src/routes/guardduty/` directory exists; all GuardDuty operations have zero UI coverage.

---

### iam

- **Parity:**
  - All distinct "not found" error sentinels (`ErrUserNotFound`, `ErrRoleNotFound`, `ErrPolicyNotFound`, `ErrGroupNotFound`, etc.) are constructed with the identical string `"NoSuchEntity"`, making it impossible to distinguish which resource type was missing via `errors.Is` or message inspection. `/home/user/gopherstack/services/iam/backend.go:24`
  - `SimulatePrincipalPolicy` is listed in the interface but the emulator has no actual IAM policy evaluation engine, so simulation results are canned/synthetic rather than based on attached policies. `/home/user/gopherstack/services/iam/backend.go:154`
  - `GetCredentialReport` returns a raw string rather than a properly structured CSV report with all required IAM credential report columns. `/home/user/gopherstack/services/iam/backend.go:155`

- **Performance:**
  - `ListPolicies`, `ListUsers`, `ListRoles`, and `ListGroups` all accept a `marker` string but lack an index for O(1) marker resolution, requiring O(n) scans on large stores. `/home/user/gopherstack/services/iam/backend.go:102`

- **Leaks:**
  - None identified.

- **UI:**
  - No `ui/src/routes/iam/` directory exists; all IAM operations have zero UI coverage.

---

### identitystore

- **Parity:**
  - `ListUsers` performs a full map iteration and sort on every call rather than maintaining a sorted index, returning results in a non-deterministic intermediate order before the sort. `/home/user/gopherstack/services/identitystore/backend.go:420`
  - The `ExternalIds` filter in user/group lookups is not indexed, requiring a linear scan when filtering by external ID. `/home/user/gopherstack/services/identitystore/backend.go:341`

- **Performance:**
  - `ListUsers` allocates a full result slice copy and sorts it on every call even when no pagination is needed, adding unnecessary O(n log n) overhead. `/home/user/gopherstack/services/identitystore/backend.go:420`

- **Leaks:**
  - None identified; opaque base64 pagination tokens are used correctly and maps are bounded by explicit deletes.

- **UI:**
  - No `ui/src/routes/identitystore/` directory exists; all IdentityStore operations have zero UI coverage.

---

### inspector2

- **Parity:**
  - `Enable()` ignores the `resourceTypes` parameter entirely (`_ = resourceTypes`), always enabling all resource types instead of the requested subset. `/home/user/gopherstack/services/inspector2/backend.go:200`
  - `Disable()` ignores the `resourceTypes` parameter in the same way, always disabling the entire service rather than specific resource types. `/home/user/gopherstack/services/inspector2/backend.go:211`
  - `GetStatus` returns identical status for EC2, ECR, and Lambda regardless of which resource types were selectively enabled or disabled. `/home/user/gopherstack/services/inspector2/backend.go:225`

- **Performance:**
  - `CreateFilter` performs an O(n) linear scan over all existing filters to check for a duplicate name instead of maintaining a name-keyed secondary index. `/home/user/gopherstack/services/inspector2/backend.go:270`

- **Leaks:**
  - None identified.

- **UI:**
  - No `ui/src/routes/inspector2/` directory exists; all Inspector2 operations have zero UI coverage.

---

### iot

- **Parity:**
  - `dispatchStubOp` is a no-op that always returns `false, nil`, meaning any operation that falls through to it silently returns HTTP 200 with an empty body instead of a `501 NotImplemented` response. `/home/user/gopherstack/services/iot/handler_stubs.go:224`
  - `GetPercentiles` and `GetStatistics` are listed as op constants but have no backing implementation, returning empty responses via the stub path. `/home/user/gopherstack/services/iot/handler_stubs.go:109`
  - `SearchIndex` is listed as an op constant with no indexing engine behind it, always returning empty results. `/home/user/gopherstack/services/iot/handler_stubs.go:172`
  - `TestAuthorization` and `TestInvokeAuthorizer` are listed as op constants but have no policy evaluation, always returning permissive results via the stub path. `/home/user/gopherstack/services/iot/handler_stubs.go:183`

- **Performance:**
  - The backend allocates 40+ independent `map[string]*T` fields with no shared secondary indexes (e.g., no thing-by-type index), requiring full map iteration for any cross-resource lookup. `/home/user/gopherstack/services/iot/backend.go:49`
  - `GetRules` clones every topic rule on every call via `cloneTopicRule`, creating O(n) allocations even for read-only callers that do not mutate the result. `/home/user/gopherstack/services/iot/backend.go:268`

- **Leaks:**
  - None identified; the single `sync.RWMutex` guards all maps and `Reset()` reinitializes all of them.

- **UI:**
  - No `ui/src/routes/iot/` directory exists; all IoT operations have zero UI coverage.

### iotanalytics

**Parity:**
- `RunPipelineActivity` is a pass-through no-op returning payloads unchanged instead of applying pipeline transforms. services/iotanalytics/backend.go:1559
- `handleCreateChannel`/`handleCreateDatastore`/`handleCreateDataset`/`handleCreatePipeline` return HTTP 200 instead of the AWS-mandated 201 Created. services/iotanalytics/handler.go:688
- `parsePagination` base64-encodes a plaintext resource name as the nextToken, making pagination non-opaque and client-inspectable. services/iotanalytics/handler.go:662
- `handleListChannels` uses `ch.Name <= cursor` lexicographic comparison as the pagination cursor, tying token semantics to resource names. services/iotanalytics/handler.go:703
- `CreateDatasetContent` immediately marks status "SUCCEEDED" without performing any async content generation. services/iotanalytics/backend.go:1432
- `handleRunPipelineActivity` returns an empty `LogResult` field instead of per-message execution logs. services/iotanalytics/handler.go:1443

**Performance:**
- `ListChannels` deep-clones the entire channel collection (including all metadata) before applying pagination filtering. services/iotanalytics/backend.go:718
- `BatchPutMessage` acquires the global write lock and holds it for the full iteration over all submitted messages. services/iotanalytics/backend.go:1236

**Leaks:**
- Per-channel `channelMessages` maps grow unbounded until the per-channel cap is hit, with no TTL-based eviction for stale messages.  services/iotanalytics/backend.go:1236

**UI:**
- No `ui/src/routes/iotanalytics/` directory exists; none of the backend ops (CreateChannel, PutMessage, ListDatasets, RunPipelineActivity, etc.) have any UI coverage.

---

### iotdataplane

**Parity:**
- `findCursorIndex` passes the raw topic string as the nextToken, making retained-message pagination non-opaque. services/iotdataplane/handler.go:546
- `handleListThingsWithShadows` injects a non-AWS `timestamp` key into each shadow entry in the response. services/iotdataplane/handler.go:637
- `computeDelta` compares desired vs reported values using byte-exact JSON string comparison (`string(rv) != string(dv)`), which produces false deltas for semantically-equivalent but key-reordered JSON. services/iotdataplane/backend.go:296

**Performance:**
- `evictOldestRetained` performs an O(n) full map scan on every insertion when the retained-message store is at capacity. services/iotdataplane/backend.go:832
- `ListRetainedMessages` copies all message payloads (full byte slices) while holding the read lock, blocking concurrent shadow reads. services/iotdataplane/backend.go:870

**Leaks:**
- `retainedMessages` map has no TTL-based expiry; retained messages for deleted things accumulate indefinitely. services/iotdataplane/backend.go:832

**UI:**
- No `ui/src/routes/iotdataplane/` directory exists; none of the backend ops (UpdateThingShadow, ListThingsWithShadows, Publish, ListRetainedMessages, etc.) have any UI coverage.

---

### iotwireless

**Parity:**
- Bulk multicast association ops (`AssociateMulticastGroupWithFuotaTask`, `DisassociateMulticastGroupFromFuotaTask`) are no-ops that return 204 without persisting anything. services/iotwireless/handler.go:1172
- `updateMetricConfiguration` returns 204 without persisting the submitted metric configuration. services/iotwireless/handler.go:1237
- Dozens of stub handlers use hardcoded constants (`stubArn`, `stubMulticastGroupID`, `stubNetworkAnalyzerConfigurationName`) instead of real resource management. services/iotwireless/handler_stubs.go:18
- `getWirelessGatewayFirmwareInformation` returns an empty struct instead of firmware version/update fields. services/iotwireless/handler_stubs.go:162
- List stubs for EventConfigurations, PositionConfigurations, and QueuedMessages return `[]struct{}{}` (always empty). services/iotwireless/handler_stubs.go:229

**Performance:**
- No pagination is implemented on any list stub; all list operations return a single empty or canned page regardless of store size. services/iotwireless/handler_stubs.go:229

**Leaks:**
- No resource lifecycle cleanup is implemented for stub-managed resources; any state written via stub handlers is never freed.  services/iotwireless/handler_stubs.go:18

**UI:**
- No `ui/src/routes/iotwireless/` directory exists; none of the backend ops (CreateWirelessDevice, SendDataToWirelessDevice, CreateMulticastGroup, etc.) have any UI coverage.

---

### kafka

**Parity:**
- `handleListClusters`, `handleListClustersV2`, `handleListConfigurations`, `handleListReplicators`, and `handleListTopics` return all results with no nextToken pagination support. services/kafka/handler.go:1296
- Cluster creation returns ACTIVE state immediately with no async CREATING→ACTIVE state transition delay matching AWS behavior. services/kafka/backend.go:585

**Performance:**
- `CreateCluster` iterates all clusters across all regions for name-uniqueness checking, making it O(n) in total cluster count. services/kafka/backend.go:585

**Leaks:**
- No unbounded growth controls are present on the clusters map; deleted clusters are not purged from any index. services/kafka/backend.go:585

**UI:**
- No `ui/src/routes/kafka/` directory exists; none of the backend ops (CreateCluster, CreateTopic, GetBootstrapBrokers, ListClusters, etc.) have any UI coverage.

---

### kinesis

**Parity:**
- The handler's `tags` map is only cleaned up via the `OnStreamPurged` callback, so tags may leak if streams are deleted through any other path. services/kinesis/handler.go:63

**Performance:**
- `sweepRetention` in the janitor acquires the global write lock (`mu.Lock`) and holds it for the entire sweep across all regions, shards, and records, blocking all concurrent PutRecord/GetRecords operations. services/kinesis/janitor.go:79

**Leaks:**
- Kinesis janitor goroutine is started but has no explicit shutdown mechanism exposed to callers beyond the context passed at construction. services/kinesis/janitor.go:1

**UI:**
- No `ui/src/routes/kinesis/` directory exists; none of the backend ops (CreateStream, PutRecord, GetRecords, MergeShards, ListShards, etc.) have any UI coverage.

---

### kinesisanalytics

**Parity:**
- `DiscoverInputSchema` returns a hardcoded stub schema regardless of the submitted source configuration. services/kinesisanalytics/handler.go:790
- `ListApplications` uses a plain `ExclusiveStartApplicationName` string as its pagination cursor, making pagination non-opaque and name-dependent. services/kinesisanalytics/backend.go:1

**Performance:**
- `launchTransition` spawns state-transition goroutines with `context.Background()`, detaching them from any caller-provided lifecycle context. services/kinesisanalytics/backend.go:806

**Leaks:**
- Uses stdlib `sync.RWMutex` rather than the project-standard `lockmetrics.RWMutex`, so lock contention on this service is invisible to any lock-metrics tooling. services/kinesisanalytics/backend.go:153
- `cancelFuncs` entries for completed or failed state transitions are never removed, causing the map to grow unboundedly over repeated start/stop cycles. services/kinesisanalytics/backend.go:149

**UI:**
- No `ui/src/routes/kinesisanalytics/` directory exists; none of the backend ops (CreateApplication, StartApplication, StopApplication, DiscoverInputSchema, etc.) have any UI coverage.

---

### kinesisanalyticsv2

**Parity:**
- `ListApplications` uses a plain integer offset serialized via `strconv.Itoa` as its nextToken, making pagination non-opaque and position-sensitive to concurrent mutations. services/kinesisanalyticsv2/backend.go:369

**Performance:**
- `ListApplications` performs a full map iteration and sort on every call regardless of page size or token. services/kinesisanalyticsv2/backend.go:354
- The `versions` map grows without bound, appending a full application snapshot on every create and update operation with no version pruning. services/kinesisanalyticsv2/backend.go:326

**Leaks:**
- `versions` map entries for deleted applications are never removed, causing unbounded memory growth proportional to the total number of updates ever applied. services/kinesisanalyticsv2/backend.go:326

**UI:**
- No `ui/src/routes/kinesisanalyticsv2/` directory exists; none of the backend ops (CreateApplication, UpdateApplication, ListApplications, AddApplicationInput, etc.) have any UI coverage.

---

### kms

**Parity:**
- `keyIDResolutionCache` (alias→keyID) is not invalidated when a key is disabled or scheduled for deletion, allowing stale cache hits to resolve deleted or disabled keys. services/kms/backend.go:425
- `lastUsage` sync.Map entries are never removed when keys are purged by the janitor, leaking usage metadata for deleted keys. services/kms/backend.go:282

**Performance:**
- `clearResolutionCache` iterates the full cache via `Range` and deletes every entry individually (O(n) per call), and is invoked on every alias mutation. services/kms/backend.go:481

**Leaks:**
- `lastUsage` sync.Map grows indefinitely because `purgeKey` in the janitor clears keys, aliases, and grants but never deletes the corresponding `lastUsage` entry. services/kms/janitor.go:226

**UI:**
- No `ui/src/routes/kms/` directory exists; none of the backend ops (CreateKey, Encrypt, Decrypt, GenerateDataKey, ScheduleKeyDeletion, etc.) have any UI coverage.

### lakeformation

- **Parity:**
  - `handleSearchDatabasesByLFTags` passes literal `0` for MaxResults, ignoring the caller's value: `/home/user/gopherstack/services/lakeformation/handler.go:1303`
  - `handleSearchTablesByLFTags` has the same MaxResults-dropped-to-zero bug: `/home/user/gopherstack/services/lakeformation/handler.go:1314`
  - `GetTableObjects` always returns an empty list regardless of input: `/home/user/gopherstack/services/lakeformation/backend.go:2015`
  - `GetQueryStatistics` returns hardcoded all-zero counters instead of real statistics: `/home/user/gopherstack/services/lakeformation/backend.go:2065`
  - `GetDataLakePrincipal` returns a hardcoded synthetic ARN `arn:aws:iam::000000000000:user/gopherstack-user` instead of caller identity: `/home/user/gopherstack/services/lakeformation/backend.go:1632`
  - `ExtendTransaction` validates the transaction but performs no state change (documented no-op): `/home/user/gopherstack/services/lakeformation/backend.go:1858`
  - `GetWorkUnitResults` validates the query exists but always returns an empty response body: `/home/user/gopherstack/services/lakeformation/backend.go:2096`
  - Pagination token is a non-opaque decimal integer index, making cursor positions guessable and enumerable: `/home/user/gopherstack/services/lakeformation/backend.go:870`

- **Performance:**
  - `findPermissionEntry` is an O(n) linear scan called under the write lock inside `GrantPermissions`: `/home/user/gopherstack/services/lakeformation/backend.go:751`
  - `BatchGrantPermissions` calls `GrantPermissions` in a loop, acquiring and releasing the write lock once per entry (n lock cycles per batch): `/home/user/gopherstack/services/lakeformation/backend.go:702`
  - `ListPermissions` builds a full filtered copy of the permissions slice and sorts it on every call: `/home/user/gopherstack/services/lakeformation/backend.go:525`

- **Leaks:**
  - No goroutine or ticker leaks identified in lakeformation (stateless handler, no background workers).

- **UI:**
  - UI route exists at `ui/src/routes/lakeformation/+page.svelte` covering Resources, LFTags, Permissions, Transactions, DataFilters, and Expressions.
  - `GetDataLakeSettings` and `PutDataLakeSettings` have no UI panel.
  - Query planning operations (`StartQueryPlanning`, `GetQueryState`, `GetWorkUnits`, `GetWorkUnitResults`) have no UI.
  - Credential-vending operations (`GetTemporaryGlueTableCredentials`, `GetTemporaryGluePartitionCredentials`) have no UI.

---

### lambda

- **Parity:**
  - `handleInvokeWithResponseStream` uses a simple 4-byte big-endian length-prefix frame instead of the required `vnd.amazon.eventstream` multipart encoding: `/home/user/gopherstack/services/lambda/handler_stubs.go:237`
  - `handleInvokeAsync` fully delegates to the synchronous `handleInvoke`, returning a 202 but executing synchronously with no async semantics: `/home/user/gopherstack/services/lambda/handler_stubs.go:225`
  - `sweepESMs` in the janitor is a complete no-op that only records metrics and performs no ESM health checks or cleanup: `/home/user/gopherstack/services/lambda/janitor.go:96`

- **Performance:**
  - `withInvocationChain` allocates a new map on every function invocation (hot-path per-request heap allocation): `/home/user/gopherstack/services/lambda/backend.go:112`

- **Leaks:**
  - `activeConcurrencies` map entries are never deleted when a function's concurrency returns to zero, causing unbounded map growth per function over time: `/home/user/gopherstack/services/lambda/backend.go:207`
  - `cleanupSem` and `logSem` channels are never closed when `Reset()` is called, leaving goroutines blocked on them: `/home/user/gopherstack/services/lambda/backend.go:265`

- **UI:**
  - UI routes exist at `ui/src/routes/lambda/+page.svelte` and `ui/src/routes/lambda/function/+page.svelte` covering create/list/invoke/delete functions, aliases, ESMs, and layer listing.
  - Provisioned concurrency put/get/list/delete operations have no UI.
  - Code-signing config operations have no UI.
  - Function URL config operations (`CreateFunctionUrlConfig`, `GetFunctionUrlConfig`, `UpdateFunctionUrlConfig`, `DeleteFunctionUrlConfig`) have no UI.
  - Layer version policy operations (`AddLayerVersionPermission`, `RemoveLayerVersionPermission`) have no UI.
  - Runtime management configuration (`PutRuntimeManagementConfig`, `GetRuntimeManagementConfig`) has no UI.

---

### macie2

- **Parity:**
  - `EnableMacie` returns `ErrAllowListAlreadyExists` when the session is already enabled — wrong sentinel error for this condition: `/home/user/gopherstack/services/macie2/backend.go:159`
  - `ListAllowLists` ignores `maxResults` and `nextToken`, always returning all records: `/home/user/gopherstack/services/macie2/backend.go:323`
  - `ListCustomDataIdentifiers` ignores pagination parameters, always returning all records: `/home/user/gopherstack/services/macie2/backend.go:430`
  - `ListFindingsFilters` ignores pagination parameters, always returning all records: `/home/user/gopherstack/services/macie2/backend.go:642`
  - `ListFindings` ignores all filter criteria and pagination, returning all finding IDs unconditionally: `/home/user/gopherstack/services/macie2/backend.go:686`
  - `GetSensitiveDataOccurrences` always returns an empty occurrences map with no actual data reveal: `/home/user/gopherstack/services/macie2/backend_appendixa.go:958`
  - `GetSensitiveDataOccurrencesAvailability` always returns `"AVAILABLE"` regardless of session or resource state: `/home/user/gopherstack/services/macie2/backend_appendixa.go:970`

- **Performance:**
  - No significant performance issues identified beyond the full-scan list operations caused by missing pagination.

- **Leaks:**
  - No goroutine or channel leaks identified in macie2.

- **UI:**
  - UI route exists at `ui/src/routes/macie2/+page.svelte` covering session management, allow lists, custom data identifiers, findings filters, and findings.
  - Classification job operations (`CreateClassificationJob`, `DescribeClassificationJob`, `ListClassificationJobs`, `UpdateClassificationJob`) have no UI.
  - Member and invitation management operations have no UI.
  - Organization administrator delegation operations have no UI.
  - Automated discovery configuration operations have no UI.
  - Classification export configuration operations have no UI.
  - Sensitivity inspection template operations have no UI.
  - Reveal configuration and `GetSensitiveDataOccurrences` have no UI.

---

### managedblockchain

- **Parity:**
  - `handleListNetworks` returns all results with no `NextToken` or pagination support, ignoring `maxResults`/`nextToken` query params: `/home/user/gopherstack/services/managedblockchain/handler.go:657`
  - `handleListMembers` returns all results with no `NextToken` or pagination support: `/home/user/gopherstack/services/managedblockchain/handler.go:725`
  - Error sentinel values embed human-readable internal messages (e.g., `"ResourceNotFoundException: network not found"`) that leak implementation phrasing directly into HTTP error responses: `/home/user/gopherstack/services/managedblockchain/backend.go:19`

- **Performance:**
  - `CreateNetwork` performs an O(n) linear scan over all networks to check name uniqueness while holding the write lock: `/home/user/gopherstack/services/managedblockchain/backend.go:217`

- **Leaks:**
  - No goroutine or channel leaks identified in managedblockchain.

- **UI:**
  - UI route exists at `ui/src/routes/managedblockchain/+page.svelte` covering networks, members, nodes, accessors, proposals, and invitations.
  - `UpdateMember` has backend support but no UI panel.
  - `UpdateNode` has backend support but no UI panel.

---

### mediaconvert

- **Parity:**
  - `countJobsForQueueLocked` is dead O(n) code that was superseded by per-queue counters but still remains in the codebase: `/home/user/gopherstack/services/mediaconvert/backend.go:500`

- **Performance:**
  - `AdvanceJobPhase` holds the global write lock while iterating and mutating all jobs in the map: `/home/user/gopherstack/services/mediaconvert/backend.go:1007`
  - `SweepExpiredTokens` holds the write lock while doing a full scan of the token map: `/home/user/gopherstack/services/mediaconvert/backend.go:994`
  - `resolveQueueLocked` performs an O(n) linear scan over all queues when resolving by ARN: `/home/user/gopherstack/services/mediaconvert/backend.go:601`
  - `ListJobs` deep-clones every job struct on every list call regardless of filter or page size: `/home/user/gopherstack/services/mediaconvert/backend.go:896`
  - The janitor fires every 500ms and acquires the write lock twice per tick (once for `AdvanceJobPhase`, once for `SweepExpiredTokens`): `/home/user/gopherstack/services/mediaconvert/janitor.go:13`

- **Leaks:**
  - `tokenIndex` map grows unbounded between janitor sweep cycles and has no cap on total token count: `/home/user/gopherstack/services/mediaconvert/backend.go:994`

- **UI:**
  - UI route exists at `ui/src/routes/mediaconvert/+page.svelte` covering jobs, queues, job templates, and preset listing.
  - Preset create and update operations have no UI (list-only for presets).
  - `SearchJobs` has no UI.
  - `Probe` has no UI.

---

### medialive

- **Parity:**
  - `StartChannel` and `StopChannel` immediately transition channel state with no async delay or intermediate `STARTING`/`STOPPING` state simulation: `/home/user/gopherstack/services/medialive/backend.go:20`

- **Performance:**
  - `ListInputDeviceTransfers` performs an O(n devices) full scan to collect transfer state on every call: `/home/user/gopherstack/services/medialive/backend.go:1938`

- **Leaks:**
  - No goroutine or channel leaks identified in medialive.

- **UI:**
  - UI route exists at `ui/src/routes/medialive/+page.svelte` with list-only views for channels, inputs, input security groups, and multiplexes.
  - `CreateChannel`, `UpdateChannel`, `StartChannel`, `StopChannel`, and `DeleteChannel` have no UI.
  - Cluster and node management operations have no UI.
  - Signal map operations have no UI.
  - Schedule/`BatchUpdateSchedule` operations have no UI.
  - Multiplex program operations have no UI.

---

### mediapackage

- **Parity:**
  - Every `HarvestJob` is immediately set to `"SUCCEEDED"` at creation time with no async processing simulation: `/home/user/gopherstack/services/mediapackage/backend.go:23`
  - `storedPackagingConfiguration` struct is missing all packaging format fields (HLS, DASH, MSS, CMAF config sub-structs), causing those fields to be silently dropped on create and return empty on get: `/home/user/gopherstack/services/mediapackage/backend.go:133`

- **Performance:**
  - No significant performance issues identified in mediapackage.

- **Leaks:**
  - No goroutine or channel leaks identified in mediapackage.

- **UI:**
  - UI route exists at `ui/src/routes/mediapackage/+page.svelte` with read-only list views for channels, origin endpoints, and harvest jobs.
  - `CreateChannel` and `UpdateChannel` have no UI.
  - `CreateOriginEndpoint` and `UpdateOriginEndpoint` have no UI.
  - Packaging configuration management operations have no UI.
  - `CreateHarvestJob` has no UI (list-only).

---

### mediastore

- **Parity:**
  - `ExtractResource` parses the request body to extract the container name but the body may already be consumed by the time the handler runs, causing silently empty resource extraction: `/home/user/gopherstack/services/mediastore/handler.go:127`

- **Performance:**
  - No significant performance issues identified in mediastore.

- **Leaks:**
  - No goroutine or channel leaks identified in mediastore.

- **UI:**
  - UI route exists at `ui/src/routes/mediastore/+page.svelte` with full CRUD for containers and all policy operations — good coverage.
  - No significant UI gaps identified for mediastore.

# Audit Group 13: mediastoredata, mediatailor, memorydb, mq, mwaa, neptune, networkmonitor, omics

### mediastoredata

- **Parity:** `ListItems` uses the last item's `Name` as a non-opaque NextToken, leaking internal sort order to callers instead of an opaque cursor. `services/mediastoredata/backend.go:386`
- **Parity:** `handleListItems` does not validate `MaxResults` against the AWS-permitted range (1–1000), accepting any integer without error. `services/mediastoredata/handler.go:285`
- **Parity:** `UpdateObjectMetadata` is implemented in the backend but has no handler route, making it permanently unreachable via the API. `services/mediastoredata/handler.go:1` (no route present)
- **Parity:** `parseByteRange` returns 400 `InvalidRequest` on bad range values, but AWS returns `InvalidRangeException`; error code is wrong. `services/mediastoredata/handler.go:454`
- **Performance:** `ListItems` fetches all objects, copies them to a slice, sorts the slice, and then paginates on every call — O(n log n) with no index. `services/mediastoredata/backend.go:311`
- **Performance:** `PutObject` computes the SHA-256 body hash twice: once during content-hash verification (`contentSHA256`) and once when computing the ETag, doubling the hashing cost. `services/mediastoredata/backend.go:176`
- **Leaks:** No unbounded-growth control on the objects map per region; there is no eviction or max-object-count guard, so the map can grow without bound. `services/mediastoredata/backend.go:1`
- **UI:** `ui/src/routes/mediastoredata/+page.svelte` exists; `UpdateObjectMetadata` and `DeleteObject` (bulk) have no corresponding UI actions.

### mediatailor

- **Parity:** `ListLiveSources` signature is `(_ int, _ string)` — both `maxResults` and `nextToken` parameters are silently ignored, returning all results with no pagination. `services/mediatailor/backend.go:1011`
- **Parity:** `ListPrefetchSchedules` signature is `(_ int, _ string)` — `maxResults` and `nextToken` both ignored, no pagination. `services/mediatailor/backend.go:1087`
- **Parity:** `ListFunctions` and `GetChannelSchedule` both use `(_ int, _ string)` — pagination parameters are dropped, all results returned. `services/mediatailor/backend.go:1307`
- **Parity:** `handleListAlerts` is a permanent no-op stub returning an always-empty array. `services/mediatailor/handler.go:1491`
- **Parity:** `UpdateProgram` returns the stored program unmodified without applying any body fields — effectively a read operation masquerading as a write. `services/mediatailor/backend.go:1151`
- **Parity:** `Snapshot()` omits liveSources, prefetchSchedules, programs, and functions from serialized state, so those resources are lost on restore. `services/mediatailor/backend.go:252`
- **Performance:** Every `List*` operation copies the entire resource map into a slice before sorting — O(n) allocation on each call with no sorted index. `services/mediatailor/backend.go:401`
- **UI:** `ui/src/routes/mediatailor/+page.svelte` exists; `ListAlerts`, `ListPrefetchSchedules`, `ListLiveSources`, and `GetChannelSchedule` have no UI.

### memorydb

- **Parity:** `handleDescribeClusters` implements a non-opaque NextToken using the cluster name as a cursor, which exposes internal names and differs from the opaque token AWS returns. `services/memorydb/handler.go:410`
- **Parity:** `handleDescribeMultiRegionClusters` has no pagination at all — it returns all results in a single response regardless of list size. `services/memorydb/handler.go:1141`
- **Parity:** `handleDescribeEngineVersions` response never includes a `NextToken` field, so clients cannot paginate beyond the first page. `services/memorydb/handler.go:1032`
- **Parity:** `seedDefaultParameterGroupsLocked` only seeds default parameter groups into `b.defaultRegion`, leaving all other regions without them at startup. `services/memorydb/backend.go:501`
- **Performance:** `handleDescribeACLs` calls `DescribeClusters("")` on every request to compute cluster membership — O(clusters) work done inside every ACL describe. `services/memorydb/handler.go:526`
- **Performance:** `handleDescribeUsers` calls `DescribeACLs("")` on every request to count ACL memberships per user — O(acls) work on every user describe. `services/memorydb/handler.go:706`
- **Performance:** `paginateItems` uses a linear `findStartIndex` scan through a sorted slice to resolve the NextToken cursor — O(n) per page request. `services/memorydb/handler.go:1491`
- **Leaks:** Backend uses a bare `sync.RWMutex` instead of `lockmetrics.RWMutex`, so lock contention is invisible to metrics and monitoring. `services/memorydb/backend.go:318`
- **UI:** `ui/src/routes/memorydb/+page.svelte` exists; `DescribeMultiRegionClusters`, `DescribeEngineVersions`, ACL, and user management ops have no UI coverage.

### mq

- **Parity:** `DeleteConfiguration` is registered as a supported operation and has a handler route, but AWS Amazon MQ has no `DeleteConfiguration` API — this is a phantom operation. `services/mq/backend.go:37`
- **Parity:** `handleRebootBroker` responds with HTTP 200 and no body, but AWS returns HTTP 200 with an empty JSON body `{}`; the status code matches but the response body is missing. `services/mq/handler.go:677`
- **Parity:** `Promote` validates that the broker exists but makes no state change and returns a copy — it is a no-op stub. `services/mq/backend.go:1436`
- **Performance:** `DescribeBroker` acquires a full write lock (`b.mu.Lock`) just to call `promoteRebootingToRunning`, imposing write-lock cost on all read-only describe calls. `services/mq/backend.go:727`
- **Performance:** `ListBrokers` likewise holds a write lock to invoke `promoteRebootingToRunning`, blocking concurrent reads unnecessarily. `services/mq/backend.go:743`
- **Performance:** `findBrokerByCreatorRequestID` is an O(n) linear scan across all brokers on every `CreateBroker` call. `services/mq/backend.go:697`
- **Performance:** `CreateConfiguration` duplicate-name check is an O(n) linear scan through all configurations. `services/mq/backend.go:1106`
- **UI:** `ui/src/routes/mq/+page.svelte` exists; `Promote`, `RebootBroker`, and configuration management operations have no UI.

### mwaa

- **Parity:** `InvokeRestAPI` always returns `{RestAPIStatusCode: 200, RestAPIResponse: {}}` — permanently stubbed, never executes real logic. `services/mwaa/backend.go:1210`
- **Parity:** `ListEnvironmentsPage` uses the environment name as a non-opaque NextToken cursor, leaking internal names to callers. `services/mwaa/backend.go:1088`
- **Parity:** `promoteTransientStatus` promotes `CREATING→AVAILABLE` on the very first `GetEnvironment` call with no delay, never emitting intermediate statuses that AWS clients may wait for. `services/mwaa/backend.go:785`
- **Parity:** `GetMetrics` and `PublishMetrics` are non-standard operations not present in the real AWS MWAA API but are registered in `GetSupportedOperations`. `services/mwaa/handler.go:68`
- **Performance:** `GetEnvironment` acquires a write lock to call `promoteTransientStatus`, meaning all read-only `GetEnvironment` calls pay write-lock cost and block concurrent readers. `services/mwaa/backend.go:769`
- **Leaks:** `Reset()` calls `b.mu.Close()` which can panic or deadlock if any concurrent request holds the old mutex at the moment of reset. `services/mwaa/backend.go:315`
- **UI:** `ui/src/routes/mwaa/+page.svelte` exists; `InvokeRestAPI`, `PublishMetrics`, `GetMetrics`, and `CreateWebLoginToken` have no UI.

### neptune

- **Parity:** `applyNeptuneMarker` uses a numeric offset as the Marker value rather than an opaque token — clients can manipulate the pagination position by crafting integer marker strings. `services/neptune/handler.go:2646`
- **Parity:** `handleDescribeDBClusterParameterGroups` returns results without a Marker/pagination response field — no pagination is supported on this endpoint. `services/neptune/handler.go:848`
- **Parity:** `handleDescribeGlobalClusters` ignores the `Marker` and `MaxRecords` parameters entirely, always returning all global clusters. `services/neptune/handler.go:1064`
- **Parity:** `handleDescribeDBParameters` and `handleDescribeDBClusterParameters` always return an empty parameter list — no actual parameters are modeled. `services/neptune/handler.go:1321`
- **Parity:** `ApplyPendingMaintenanceAction` validates its inputs but performs no state change — it is a no-op stub that always returns nil. `services/neptune/backend.go:1720`
- **Performance:** `DescribeDBClusters` copies every matching cluster with `cloneCluster` (multiple slice copies per cluster) before returning — hot-path allocation scales linearly with cluster count. `services/neptune/backend.go:776`
- **Leaks:** No unbounded-growth control on the `tags` map; deleting a resource cleans up its ARN entry, but if tag operations reference non-existent ARNs via `validateResourceARN`, a new empty entry can be inserted. `services/neptune/backend.go:1523`
- **UI:** `ui/src/routes/neptune/+page.svelte` exists; global cluster operations (`CreateGlobalCluster`, `FailoverGlobalCluster`, `SwitchoverGlobalCluster`, `RemoveFromGlobalCluster`), event subscriptions, and `ApplyPendingMaintenanceAction` have no UI.

### networkmonitor

- **Parity:** `ListMonitors` uses `monitorName` as a non-opaque NextToken cursor; callers can determine monitor names from pagination tokens. `services/networkmonitor/backend.go:371`
- **Parity:** `aggregationPeriod` is constrained to only 30 or 60, but AWS allows any value ≥ 30 that is a multiple of 30 (up to 1800); the validation is too strict. `services/networkmonitor/backend.go:176`
- **Parity:** `UpdateMonitor` only updates `aggregationPeriod` — the `tags` field present in the AWS UpdateMonitor request is silently ignored. `services/networkmonitor/backend.go:283`
- **Parity:** `UpdateProbe` merges new tags into existing ones (`maps.Copy`) instead of replacing them; AWS `UpdateProbe` replaces the tag set. `services/networkmonitor/backend.go:551`
- **Parity:** The handler has no `ChaosServiceName`/`ChaosRegions` implementation consistent with other services — `ChaosRegions` returns only `h.DefaultRegion` ignoring multi-region isolation. `services/networkmonitor/handler.go:96`
- **Performance:** `ListMonitors` builds and sorts a full name slice on every call before applying the nextToken offset — O(n log n) with no index. `services/networkmonitor/backend.go:324`
- **Performance:** `findProbeIndex` is an O(n) linear scan through a monitor's probes slice, called on every `GetProbe`, `DeleteProbe`, and `UpdateProbe`. `services/networkmonitor/backend.go:695`
- **Leaks:** `InMemoryBackend` uses a bare `sync.RWMutex` instead of `lockmetrics.RWMutex`, making lock contention invisible to service-level metrics. `services/networkmonitor/backend.go:100`
- **UI:** No `ui/src/routes/networkmonitor/` directory exists — this service has zero UI coverage for all 12 operations.

### omics

- **Parity:** All backend write operations (CreateReferenceStore, CreateSequenceStore, etc.) always use `b.defaultRegion`, completely ignoring any region extracted from the request context — multi-region isolation is absent. `services/omics/backend.go:257`
- **Parity:** `paginateStrings` uses the resource ID as a non-opaque NextToken cursor — the opaque token requirement is violated and internal IDs are exposed. `services/omics/backend.go:204`
- **Parity:** `StartReferenceImportJob` immediately marks the job as `COMPLETED` and creates reference entries synchronously — there is no `RUNNING` state transition, so clients polling for completion see it finish instantly. `services/omics/backend.go:453`
- **Parity:** `GetReference` and `GetReadSet` return an empty byte slice `[]byte{}` for all objects since no actual reference/read-set data is stored — the data plane is stubbed. `services/omics/backend.go:483`
- **Parity:** The `opGetRunBatch` constant is defined as `"GetBatch"` and `opListRunBatches` as `"ListBatch"`, which differ from the AWS operation names (`GetRunBatch` / `ListRunBatches`), breaking operation-name parity in `GetSupportedOperations`. `services/omics/handler.go:129`
- **Performance:** `ListReferenceStores`, `ListSequenceStores`, and all other List* operations build a full ID slice and call `sort.Strings` on every request with no cached index. `services/omics/backend.go:320`
- **Leaks:** `InMemoryBackend` uses a bare `sync.RWMutex` instead of `lockmetrics.RWMutex`, so lock contention is not tracked by service metrics. `services/omics/backend.go:132`
- **UI:** No `ui/src/routes/omics/` directory exists — this service has zero UI coverage for all ~90 registered operations.

# Audit: group14 (opensearch, opsworks, organizations, personalize, pinpoint, pipes, polly, qldb)

---

### opensearch

**Parity:**
- `AcceptInboundConnection` silently auto-creates non-existent connections instead of returning a not-found error, diverging from AWS behaviour. — services/opensearch/backend.go:762
- `samlOptionsJSON` has swapped JSON tags: `IDPEntityID` serialises as `"Idp"` and `IDPMetadataContent` serialises as `"MasterBackendRole"`, producing a wrong wire shape. — services/opensearch/handler.go:513
- `CancelServiceSoftwareUpdate` returns a canned no-op response and does not alter any domain state. — services/opensearch/handler.go:799
- `AssociatePackages` (bulk variant) does not validate that the referenced packages exist, unlike the single `AssociatePackage` which does. — services/opensearch/backend.go:916
- `DeleteDomain` does not clean up `domainMaintenances`, `upgradeHistory`, `autoTunes`, or `dryRuns` entries for the deleted domain, leaving orphaned data. — services/opensearch/backend.go:633
- `CancelDomainConfigChange` always returns an empty cancelled-changes list regardless of in-progress changes. — services/opensearch/backend.go:997

**Performance:**
- `handleListDomainNames` calls `Backend.DescribeDomain` once per domain inside a loop, acquiring a separate `RLock` for each call (O(n) lock acquisitions). — services/opensearch/handler.go:1241
- `CreateApplication` performs an O(n) linear scan over all applications to check name uniqueness while holding a write lock. — services/opensearch/backend.go:1038

**Leaks:**
- No goroutine or resource leaks identified in the portions read.

**UI:**
- `ui/src/routes/opensearch/` exists; no per-operation UI gap analysis was performed.

---

### opsworks

**Parity:**
- `handleDescribeCommands` decodes a request field named `CommandIDs` but the AWS API uses `CommandIds` (capitalisation mismatch causes silent decode failure). — services/opsworks/handler.go:617
- `GetSupportedOperations` lists only 23 operations; ~30 AWS OpsWorks ops are absent including `CloneStack`, `RegisterInstance`, `GetHostnameSuggestion`, `DescribeVolumes`, and `AttachElasticLoadBalancer`. — services/opsworks/handler.go:31
- `ListTags` accepts `MaxResults` and `NextToken` parameters but ignores both, always returning the full tag set. — services/opsworks/backend.go:858
- `DeleteStack` does not cascade-delete layers, instances, apps, or deployments belonging to the removed stack, leaving orphaned child resources. — services/opsworks/backend.go:392
- `deploymentsToJSON` always formats `CompletedAt` as the creation instant, never modelling async status transitions. — services/opsworks/handler.go:740

**Performance:**
- `toStack()` calls `maps.Copy` to copy the tags map on every read path invocation, allocating on each call. — services/opsworks/backend.go:61
- `CreateInstance` uses `len(b.instances)+1` for hostname suffix generation, which is not safe under concurrent access and can produce duplicate hostnames. — services/opsworks/backend.go:506

**Leaks:**
- No goroutine or resource leaks identified.

**UI:**
- No `ui/src/routes/opsworks/` directory exists; the service has no UI coverage at all.

---

### organizations

**Parity:**
- `handleListAccounts` returns all accounts in a single response with no `NextToken` pagination. — services/organizations/handler.go:388
- `handleListOrganizationalUnitsForParent` returns all OUs with no pagination. — services/organizations/handler.go:593
- `handleListPolicies` returns all policies with no pagination. — services/organizations/handler.go:722
- `handleListChildren` returns all children with no pagination. — services/organizations/handler.go:645
- `extractErrorType` derives the error type by splitting on `:` from the Go error message string, producing fragile non-opaque error classification. — services/organizations/handler.go:1010
- `handleListHandshakesForAccount` and `handleListHandshakesForOrganization` pass `req.Filter.ActionType` through but the backend performs no actual filtering on that field. — services/organizations/handler.go:410

**Performance:**
- No performance issues identified in the handler; backend.go was not fully read.

**Leaks:**
- No goroutine or resource leaks identified.

**UI:**
- `ui/src/routes/organizations/` exists; no per-operation UI gap analysis was performed.

---

### personalize

**Parity:**
- `getRecommendations` returns a fully synthetic item list computed from a deterministic FNV hash with no reference to any campaign or model state. — services/personalize/handler.go:1295
- `getPersonalizedRanking` re-scores caller-supplied items using the same hash-based synthetic logic, never consulting actual model data. — services/personalize/handler.go:1312
- `describeAlgorithm` returns the hardcoded name `"user-personalization"` regardless of the ARN supplied in the request. — services/personalize/handler.go:1256
- `listRecipes` truncates results to `maxResults` but never emits a `nextToken`, so callers cannot retrieve subsequent pages. — services/personalize/handler.go:1240
- `GetSolutionMetrics` returns all metric values hardcoded to `0.5`. — services/personalize/backend.go:847
- `StopSolutionVersionCreation` transitions the solution version to `"STOP PENDING"` but never advances it to `"STOPPED"` (no async follow-through). — services/personalize/backend.go:831
- `ListMetricAttributionMetrics` ignores both `maxResults` and `nextToken` parameters, always returning the full list. — services/personalize/backend.go:1419

**Performance:**
- All `find*` helpers (`findDatasetGroup`, `findSolution`, `findCampaign`, `findFilter`, `findRecommender`, `findEventTracker`, `findMetricAttribution`) perform O(n) linear scans over unbounded slices. — services/personalize/backend.go:438
- Uses bare `sync.RWMutex` rather than the project-standard `lockmetrics.RWMutex`, losing lock-contention observability. — services/personalize/backend.go:253

**Leaks:**
- No goroutine or resource leaks identified.

**UI:**
- `ui/src/routes/personalize/` exists; no per-operation UI gap analysis was performed.

---

### pinpoint

**Parity:**
- `handleCreateApp` maps every backend error to HTTP 500 with no differentiation between validation, conflict, or not-found conditions. — services/pinpoint/handler.go:1065
- `SendMessages`, `SendUsersMessages`, and `SendOTPMessage` are accepted by the handler but only increment a counter (`sentMessages`/`otpCodes`); no actual message routing or channel dispatch occurs. — services/pinpoint/backend.go:81
- `DeleteApp` on the backend purges `campaigns`, `segments`, and `journeys` by scanning entire maps and comparing `ApplicationID`, resulting in O(n) full-map iteration per delete. — services/pinpoint/backend.go:282
- `GetApps` sorts the full application slice on every call rather than maintaining a sorted index. — services/pinpoint/backend.go:319

**Performance:**
- `purgeAppStateLocked` iterates `b.campaigns`, `b.segments`, and `b.journeys` in full to find entries belonging to the deleted app while the write lock is held. — services/pinpoint/backend.go:282
- `deletePrefixed` iterates the entire target map to find matching keys, which is O(n) for every app-scoped child map on each `DeleteApp` call. — services/pinpoint/backend.go:300

**Leaks:**
- No goroutine or resource leaks identified.

**UI:**
- `ui/src/routes/pinpoint/` exists; no per-operation UI gap analysis was performed.

---

### pipes

**Parity:**
- The runner only supports SQS as a pipe source; Kinesis, DynamoDB Streams, MSK, self-managed Kafka, RabbitMQ, and ActiveMQ sources accepted by the handler are silently unrouted at runtime. — services/pipes/runner.go:18
- `TargetParameters.SFNStateMachineParameters` is JSON-tagged `"StepFunctionStateMachineParameters"` in the struct but the AWS API uses `"StepFunctionStateMachineParameters"` — verify field name alignment. — services/pipes/backend.go:401
- `StartPipe` returns a `ValidationException` when the pipe already has `DesiredState=RUNNING`; AWS returns a `ConflictException` in this case. — services/pipes/backend.go:1375

**Performance:**
- `sortedPipeNames` uses a hand-rolled O(n²) bubble sort instead of `slices.Sort` or `sort.Strings`. — services/pipes/backend.go:1128
- `ListPipes` holds the read lock for the entire duration of collecting, filtering, and cloning every matching pipe, blocking writers. — services/pipes/backend.go:1090

**Leaks:**
- All delayed state-transition goroutines are tracked in `b.wg` and cancelled via `b.svcCtx`, and `Shutdown` calls `b.wg.Wait()` — no goroutine leak. — services/pipes/backend.go:925
- Handler `Shutdown` cancels the runner context and waits for it before returning, preventing runner goroutine escape. — services/pipes/handler.go:97

**UI:**
- `ui/src/routes/pipes/` exists; no per-operation UI gap analysis was performed.

---

### polly

**Parity:**
- `syntheticAudioBytes` returns a minimal stub WAV/MP3/OGG frame regardless of voice, text, or engine; the audio content is always silent and does not reflect any synthesis. — services/polly/backend.go:813
- `listLexicons` uses a non-opaque numeric offset as `NextToken` (encoded as `strconv.Itoa(end)`), which exposes internal indexing to callers instead of an opaque cursor. — services/polly/handler.go:537
- `DescribeVoices` includes only four built-in voices (Joanna, Matthew, Aditi, Amy), missing the ~60+ voices that real AWS Polly supports. — services/polly/backend.go:935
- The `speechMarks` generator always emits `"viseme":"p"` for every word regardless of phoneme content, making viseme output non-functional. — services/polly/backend.go:782
- `validateOptions` acquires `b.mu.RLock` inside `validateOptions` which is called while building a task under `b.mu.Lock` in `StartSpeechSynthesisTask`, meaning a write lock is held before a read lock is acquired in a helper — potential for missed lock-ordering issues if paths diverge. — services/polly/backend.go:495

**Performance:**
- `ListLexicons` builds and sorts a full slice clone of all lexicons on every call while holding the read lock. — services/polly/backend.go:237
- `advanceTask` is called inside `ListSpeechSynthesisTasks` which holds the write lock for the full iteration, mutating task state under lock for every listed task. — services/polly/backend.go:363

**Leaks:**
- No goroutine or resource leaks identified; no background goroutines exist in this service.

**UI:**
- `ui/src/routes/polly/` exists; no per-operation UI gap analysis was performed.

---

### qldb

**Parity:**
- The service has no Go implementation at all — only a README.md exists under `services/qldb/`; every QLDB API call will fail or be unrouted. — services/qldb/

**Performance:**
- N/A (no implementation).

**Leaks:**
- N/A (no implementation).

**UI:**
- No `ui/src/routes/qldb/` directory exists; the service has neither backend nor UI coverage.

# Audit: group15 — qldbsession, quicksight, ram, rds, rdsdata, redshift, redshiftdata, rekognition

---

### qldbsession

> **Service removed**: qldbsession reached AWS end-of-support on 2025-07-31 and has been entirely removed from gopherstack — no Go source files exist under `services/qldbsession/`. No findings apply.

---

### quicksight

- **Parity:** `buildAppendixOps` returns a hardcoded `"TopicId": "new-topic"` for all Topics CRUD operations instead of persisting real state. `services/quicksight/handler_appendixa.go:141`
- **Parity:** All four embed-URL operations (`GenerateEmbedUrlForAnonymousUser`, `GenerateEmbedUrlForRegisteredUser`, `GetDashboardEmbedUrl`, `GetSessionEmbedUrl`) return the same hardcoded `"https://embed.example.com"` string. `services/quicksight/handler_appendixa.go:101`
- **Parity:** `StartDashboardSnapshotJob` returns a hardcoded `"SnapshotJobId": "snap1"` rather than creating a real job object. `services/quicksight/handler_appendixa.go:212`
- **Parity:** `ListDashboardVersions` ignores both `maxResults` and `nextToken` parameters, returning all versions unconditionally. `services/quicksight/backend.go:1556`
- **Parity:** `ListFolders`, `ListTemplates`, `ListThemes`, `ListVPCConnections`, and `ListBrands` have no pagination — they return all items without `MaxResults`/`NextToken` support. `services/quicksight/backend_appendixa.go:227`
- **Parity:** Pagination in `paginateNamespaces` and related helpers uses the item name as the `NextToken` (non-opaque, name-based token) which leaks internal ordering to callers. `services/quicksight/backend.go:561`
- **Performance:** All seven `ActionConnectors` operations and all five `AssetBundle` operations are dispatched via `noContent`, meaning every call acquires no lock but still allocates a response object per request with zero side-effects. `services/quicksight/handler_appendixa.go:281`
- **Performance:** `DeleteUserByPrincipalID` performs a full O(n) linear scan over the entire users map on every call. `services/quicksight/backend.go:953`
- **Performance:** `ListResourceShares` (QuickSight's equivalent list helpers) call `sort.Slice` on every read without caching sorted order. `services/quicksight/backend.go:561`
- **Leaks:** The static `reqIDPlaceholder = "request-id"` is returned as the AWS request ID in every response, making all requests appear to share a single non-unique identifier. `services/quicksight/handler.go:369`
- **UI:** `ui/src/routes/quicksight/` exists; however, all appendix-A operations (Topics, AssetBundle, ActionConnectors, embed URLs, snapshot jobs) have no UI pages.

---

### ram

- **Parity:** `ramPaginate` emits `strconv.Itoa(end)` as the `NextToken`, exposing a raw integer slice index to callers instead of an opaque token. `services/ram/handler.go:787`
- **Parity:** `GetResourcePolicies` always returns `"{}"` for every resource ARN, ignoring any actual policy state. `services/ram/backend.go:1287`
- **Parity:** `ListResources` ignores both `resourceOwner` and `resourceType` filter parameters, returning all resources regardless. `services/ram/backend.go:1393`
- **Parity:** `ListPrincipals` ignores the `resourceOwner` filter parameter. `services/ram/backend.go:1427`
- **Parity:** `PromotePermissionCreatedFromPolicy` is a no-op stub that returns without modifying any state. `services/ram/backend.go:1552`
- **Performance:** `CreateResourceShare` performs an O(n) full map scan over all resource shares on every create call to check for name collisions. `services/ram/backend.go:342`
- **Performance:** `clonePermission` deep-copies the entire `Versions` map on every read path that returns a permission. `services/ram/backend.go:159`
- **Performance:** `ListResourceShares` calls `sort.Slice` on every read without caching the sorted order. `services/ram/backend.go:440`
- **Leaks:** `DeleteResourceShare` soft-deletes by marking shares as `DELETED` but never removes them from the map, causing unbounded memory growth over time. `services/ram/backend.go:534`
- **UI:** `ui/src/routes/ram/` exists; stub operations (`GetResourcePolicies`, `PromotePermissionCreatedFromPolicy`) have no corresponding UI.

---

### rds

- **Parity:** All backend error sentinels are defined with plain `errors.New` rather than `awserr.New`, so error responses lack the AWS-format `<Code>` and `<Message>` XML shape expected by real AWS clients. `services/rds/backend.go:23`
- **Parity:** `handler_stubs.go` is misleadingly named — the file now contains real backend-backed handlers (CustomDBEngineVersion, DBShardGroup, Integration, TenantDatabase, automated backups), yet the filename implies stub/canned behavior. `services/rds/handler_stubs.go:1`
- **Parity:** `GetPerformanceInsightsMetrics` returns synthesized/fake metric data rather than actual instance metrics from stored state. `services/rds/handler_stubs.go:1`
- **Parity:** The `events` slice is unbounded — every lifecycle event is appended but never trimmed, so `DescribeEvents` results grow without limit across the lifetime of a backend instance. `services/rds/backend.go:742`
- **Performance:** `runDelayed` goroutines use `b.wg.Go` (sync.WaitGroup) and are properly gated by `b.stopCh`, but every `CreateDBInstance` and `ModifyDBInstance` call spawns a new goroutine that holds a timer for `instanceTransitionDelay`, adding per-operation goroutine overhead. `services/rds/backend.go:808`
- **Performance:** `reconcileInstancesLocked` iterates over all instances on every reconciler tick, performing an O(n) scan even when no instances are transitioning. `services/rds/backend.go:888`
- **Leaks:** The background reconciler goroutine started in `NewInMemoryBackend` is only stopped if `Close()` is called; tests or callers that discard the backend without calling `Close()` will leak the goroutine indefinitely. `services/rds/backend.go:791`
- **UI:** `ui/src/routes/rds/` exists; Performance Insights, Blue/Green Deployments, DBShardGroups, Integrations, and TenantDatabases have no UI pages.

---

### rdsdata

- **Parity:** `ExecuteStatement` always returns empty rows `[][]Field{}` and `numberOfRecordsUpdated: 0` for every SQL query, with no actual SQL execution. `services/rdsdata/backend.go:190`
- **Parity:** `ExecuteSQL` always returns a single result with `{NumberOfRecordsUpdated: 0}` and no rows, ignoring the SQL content entirely. `services/rdsdata/backend.go:298`
- **Parity:** `BeginTransaction` generates sequential counter-based IDs in the format `txn-000001` rather than UUIDs or cryptographically random identifiers. `services/rdsdata/backend.go:236`
- **Parity:** The `executedStatements` map is keyed by region string and grows by adding new region keys indefinitely with no eviction policy across regions. `services/rdsdata/backend.go:1`
- **Performance:** `appendStatementLocked` allocates a full `make([]ExecutedStatement, maxExecutedStatements)` slice plus a `copy` call on every trim operation rather than using a ring buffer or in-place slice rotation. `services/rdsdata/backend.go:160`
- **Leaks:** The `executedStatements` map accumulates entries for every distinct region key seen over the process lifetime with no upper bound on the number of region buckets. `services/rdsdata/backend.go:1`
- **UI:** `ui/src/routes/rdsdata/` exists; `ExecuteSQL` (deprecated op) and `BatchExecuteStatement` have no UI exposure.

---

### redshift

- **Parity:** All backend error sentinels use plain `errors.New` rather than `awserr.New`, so error responses do not carry the AWS REST-XML `<Code>`/`<Message>` envelope expected by SDK clients. `services/redshift/backend.go:1`
- **Parity:** `handler_completeness.go` wires ~35 operations (CustomDomainAssociation, EndpointAccess, HsmClientCertificate, HsmConfiguration, Integration, IdcApplication, ScheduledAction, DeregisterNamespace, RegisterNamespace, etc.) to real backend handlers, but several read-only list operations (`ListRecommendations`, `DescribeClusterDbRevisions`, `DescribeNodeConfigurationOptions`) return empty or static data. `services/redshift/handler_completeness.go:1`
- **Parity:** `GetIdentityCenterAuthToken` returns a canned token rather than modelling actual IdC integration state. `services/redshift/handler_completeness.go:1`
- **Parity:** `ModifyAquaConfiguration` is a no-op that accepts and ignores AQUA configuration changes. `services/redshift/handler_completeness.go:1`
- **Parity:** Pagination across Redshift list operations uses plain `errors.New`-based sentinel errors that do not conform to the `InvalidPaginationToken` AWS error code. `services/redshift/backend.go:1`
- **Performance:** Multiple Redshift list operations call `sort.Slice` on every read without maintaining a sorted index. `services/redshift/backend.go:1`
- **UI:** `ui/src/routes/redshift/` exists; IdcApplication, ScheduledAction, RegisterNamespace/DeregisterNamespace, and AQUA configuration operations have no UI pages.

---

### redshiftdata

- **Parity:** `ListStatements` uses the statement UUID as the `NextToken` (an opaque UUID, not an index), which is correct; however, pagination is recomputed by re-sorting and scanning the full result set on every page request rather than using the ring buffer ordering. `services/redshiftdata/backend.go:495`
- **Parity:** `GetStatementResult` returns synthetic demo data (1 row, fixed `mockColumnSize = 256` bytes) regardless of the original SQL content, making result-set-dependent tests unreliable. `services/redshiftdata/backend.go:1`
- **Parity:** `BatchExecuteStatement` sub-statements always report `HasResultSet: false`, which is correct for DML but wrong for SELECT batch statements that AWS would return result sets for. `services/redshiftdata/backend.go:373`
- **Performance:** `ListStatements` clones every matching statement into the result slice under the read lock, then sorts the entire slice O(n log n) on every page request even for the common single-page case. `services/redshiftdata/backend.go:487`
- **Performance:** `compactRingBuffer` allocates a new `kept` slice on every call to the janitor's eviction path, adding a GC pressure spike proportional to the number of stored statements. `services/redshiftdata/backend.go:197`
- **Leaks:** The `stores` map grows one `regionStore` entry per distinct region key seen and is never shrunk; in multi-region test environments the map accumulates empty stores indefinitely. `services/redshiftdata/backend.go:255`
- **UI:** `ui/src/routes/redshiftdata/` exists; `CancelStatement` has no UI button/action, and `ListStatements` filtering by `StatementName` or `WorkgroupName` has no UI controls.

---

### rekognition

- **Parity:** `ListDatasetLabels` always returns an empty slice `[]*DatasetLabel{}` regardless of how many entries have been added via `UpdateDatasetEntries`, making label-count assertions impossible. `services/rekognition/backend_appendixa.go:657`
- **Parity:** `DistributeDatasetEntries` is a no-op that silently discards all distribution requests without modifying any dataset state. `services/rekognition/backend_appendixa.go:675`
- **Parity:** `GetFaceLivenessSessionResults` always returns `Status: "SUCCEEDED"` with `Confidence: 99.0` — no variability or failure simulation is possible. `services/rekognition/backend_appendixa.go:972`
- **Parity:** All async video jobs (`StartFaceDetection`, `StartLabelDetection`, `StartCelebrityRecognition`, etc.) immediately transition to `JobStatus: "SUCCEEDED"` with empty result sets rather than simulating async processing. `services/rekognition/backend_appendixa.go:1011`
- **Parity:** `SearchFacesByImage` and `SearchUsersByImage` use a deterministic hash-based similarity score (seed stride 7, span 24, min 75.0) that is entirely synthetic and unrelated to any image content. `services/rekognition/backend.go:1`
- **Parity:** `IndexFaces` stores face records but performs no image analysis, so `QualityFilter` and bounding-box fields in responses are always empty/default. `services/rekognition/backend.go:1`
- **Performance:** `DescribeProjects`, `DescribeProjectVersions`, `ListProjectPolicies`, and `ListUsers` all perform a full key-sort (`sort.Strings`) on every paginated list call rather than maintaining sorted indexes. `services/rekognition/backend_appendixa.go:218`
- **Leaks:** `asyncJobs` and `mediaAnalysisJobs` maps grow without bound — completed jobs are never evicted since there is no TTL or janitor for the Rekognition backend. `services/rekognition/backend_appendixa.go:1007`
- **UI:** `ui/src/routes/rekognition/` exists; image-analysis operations (`DetectFaces`, `DetectLabels`, `DetectText`, `CompareFaces`, `DetectModerationLabels`, `RecognizeCelebrities`) and all async video job operations have no UI pages.

# Audit: group16

### resourcegroups

- **Parity:** `ErrTagSyncTaskNotFound` error type field embeds `"NotFoundException: tag-sync task not found"` — the colon-separated message is part of the type string, which AWS SDKs split on `:` to extract the code, causing malformed error responses. `/home/user/gopherstack/services/resourcegroups/backend.go:43`
- **Parity:** `SearchResources` only returns explicitly grouped resources and never cross-service tagged resources discovered via provider fan-out, so cross-service tag searches yield empty results. `/home/user/gopherstack/services/resourcegroups/backend.go:1387`
- **Parity:** `ListGroupResources` and `GroupResources` use a generic `paginate[T]()` helper that encodes the item value (name/ARN string) directly as the continuation token — non-opaque, reveals internal identifiers. `/home/user/gopherstack/services/resourcegroups/backend.go:367`
- **Parity:** `GetGroupConfiguration` is wired but `PutGroupConfiguration` silently discards `Type`-level validation — any configuration type is accepted without checking against the AWS-allowed type enum. `/home/user/gopherstack/services/resourcegroups/handler.go:800`
- **Performance:** `ListTagSyncTasks` acquires a full write lock solely to evict stale tasks during a read-oriented listing operation, blocking all concurrent readers. `/home/user/gopherstack/services/resourcegroups/backend.go:1529`
- **Performance:** `groupMatchesFilters` performs an O(n) scan of per-group configuration entries for every group during `ListGroups`, making the overall list O(n²) for configuration-filtered queries. `/home/user/gopherstack/services/resourcegroups/backend.go:878`
- **Leaks:** The `groupingStatuses` map at the group level grows unboundedly — every `GroupResources` call appends a status entry per ARN with no TTL or eviction path. `/home/user/gopherstack/services/resourcegroups/backend.go:508`
- **UI:** UI route exists at `ui/src/routes/resourcegroups/`; backend operations with no UI representation include `StartTagSyncTask`, `CancelTagSyncTask`, `GetTagSyncTask`, `ListTagSyncTasks`, `GroupResources`, `UngroupResources`, `ListGroupingStatuses`, `SearchResources`, and `UpdateAccountSettings`.

---

### resourcegroupstaggingapi

- **Parity:** `GetComplianceSummary` always returns an empty `SummaryList` because the emulator has no tag policy engine — clients expecting compliance data receive a functionally empty stub. `/home/user/gopherstack/services/resourcegroupstaggingapi/backend.go:1222`
- **Parity:** `ListRequiredTags` always returns an empty `RequiredTags` list for the same reason. `/home/user/gopherstack/services/resourcegroupstaggingapi/backend.go:1288`
- **Parity:** `ExcludeCompliantResources` filter in `GetResources` always yields an empty result set because there is no tag-policy engine to determine compliance status. `/home/user/gopherstack/services/resourcegroupstaggingapi/backend.go:589`
- **Parity:** Pagination token in `GetResources` is the raw `ResourceARN` string, making it non-opaque and leaking internal ARN structure to callers. `/home/user/gopherstack/services/resourcegroupstaggingapi/backend.go:664`
- **Performance:** `GetResources`, `GetTagKeys`, and `GetTagValues` all acquire a full write lock while fanning out to registered resource providers (cross-service calls), preventing any concurrent reads during potentially slow fan-out. `/home/user/gopherstack/services/resourcegroupstaggingapi/backend.go:581`
- **Performance:** The resource cache is keyed per region with no size bound; stale entries are only invalidated on write or `Reset`, so a long-running process accumulates unbounded cached resource slices. `/home/user/gopherstack/services/resourcegroupstaggingapi/backend.go:287`
- **Leaks:** No cache eviction goroutine or TTL sweep — the per-region resource cache map grows without bound as new resource types are registered. `/home/user/gopherstack/services/resourcegroupstaggingapi/backend.go:287`
- **UI:** UI route exists at `ui/src/routes/resourcegroupstaggingapi/`; `GetComplianceSummary`, `ListRequiredTags`, `StartReportCreation`, `DescribeReportCreation`, `TagResources`, and `UntagResources` have no UI surface.

---

### rolesanywhere

- **Parity:** `handleUntagResource` splits the raw query string on `&` without URL-decoding tag keys, so percent-encoded characters in key names (e.g., spaces as `%20`) are passed through verbatim and never removed. `/home/user/gopherstack/services/rolesanywhere/handler.go:550`
- **Parity:** `handleDeleteAttributeMapping` also uses raw query-string parsing without URL-decoding, meaning attribute keys with special characters cannot be deleted. `/home/user/gopherstack/services/rolesanywhere/handler.go:856`
- **Parity:** No validation that `source.SourceType` is one of the AWS-allowed enum values (`AWS_CERTIFICATE`, `SELF_SIGNED_CERTIFICATE`, etc.) — arbitrary strings are accepted silently. `/home/user/gopherstack/services/rolesanywhere/backend.go:290`
- **Parity:** Pagination uses the item ID directly as the continuation token via `nextTokenFromSlice`, which is non-opaque and reveals internal UUID-based resource IDs. `/home/user/gopherstack/services/rolesanywhere/backend.go:1300`
- **Performance:** `CreateTrustAnchor`, `CreateProfile`, and `ImportCrl` each perform an O(n) linear scan for duplicate names under a write lock, blocking all concurrent operations during the scan. `/home/user/gopherstack/services/rolesanywhere/backend.go:290,449,700`
- **Performance:** `listRegionItems` copies all items into a slice and sorts them on every call with no caching, so any list operation is O(n log n) even when the result set fits in a single page. `/home/user/gopherstack/services/rolesanywhere/backend.go:232`
- **Leaks:** No issues found — no background goroutines and maps are bounded by explicit CRUD operations.
- **UI:** UI route exists at `ui/src/routes/rolesanywhere/`; `AttributeMapping` CRUD, `NotificationSettings`, `EnableProfile`, `DisableProfile`, `EnableTrustAnchor`, `DisableTrustAnchor`, `ListSubjects`, and `GetSubject` have no UI representation.

---

### route53

- **Parity:** Tags are stored in `h.tags` (a handler-level field with a separate `tagsMu` lock) rather than in the backend, creating a potential state divergence if the backend is reset without resetting handler tag state. `/home/user/gopherstack/services/route53/handler.go:60-80`
- **Parity:** `listTagsForResources` (batch) returns empty `ResourceTagSets` — batch tag lookup is stubbed out while single-resource `listTagsForResource` works. `/home/user/gopherstack/services/route53/handler_completeness.go:943`
- **Parity:** `listHostedZonesByName` ignores both `?dnsname=` and `?hostedzoneid=` query parameters, returning an unfiltered list regardless of the requested starting point. `/home/user/gopherstack/services/route53/handler_completeness.go:385`
- **Parity:** `listHostedZonesByVPC` always returns an empty list, making VPC-associated zone discovery non-functional. `/home/user/gopherstack/services/route53/handler_completeness.go:415`
- **Parity:** `getHealthCheckLastFailureReason` returns an empty observations array — the health check failure history is not emulated. `/home/user/gopherstack/services/route53/handler_completeness.go:647`
- **Performance:** `getHealthCheckCount` lists all health checks (up to 1000) into memory just to return `len()`, rather than maintaining a counter. `/home/user/gopherstack/services/route53/handler_completeness.go:348`
- **Performance:** `getHostedZoneCount` lists all hosted zones (up to 10000) into memory just to return `len()`. `/home/user/gopherstack/services/route53/handler_completeness.go:365`
- **Leaks:** No background goroutines detected; tag map is bounded by explicit CRUD operations.
- **UI:** UI route exists at `ui/src/routes/route53/`; `HealthCheck` CRUD, `TrafficPolicy` CRUD, `KeySigningKey` CRUD, `DNSSEC`, `CidrCollection`, `DelegationSet`, and `QueryLoggingConfig` operations have no UI representation.

---

### route53resolver

- **Parity:** `ListResolverEndpoints` and `ListResolverRules` accept `Filters` in the AWS API but the handler ignores all filter fields, returning unfiltered lists regardless of requested criteria. `/home/user/gopherstack/services/route53resolver/handler.go:395-425`
- **Parity:** Pagination tokens (via `pkgs/page`) encode a raw slice index in base64 — opaque in format but position-based, meaning insertion or deletion of items between pages can silently skip or duplicate results. `/home/user/gopherstack/pkgs/page/page.go:46`
- **Parity:** `GetResolverQueryLogConfigPolicy`, `PutResolverQueryLogConfigPolicy`, `GetFirewallRuleGroupPolicy`, and `PutFirewallRuleGroupPolicy` store/return raw policy strings with no JSON structure validation, unlike AWS which validates IAM policy syntax. `/home/user/gopherstack/services/route53resolver/handler.go:155-161`
- **Performance:** All backend operations share a single `b.mu` `RWMutex` across all resource types (endpoints, rules, firewall groups, query logs, etc.), so any write on any resource type blocks all concurrent reads across all resource types. `/home/user/gopherstack/services/route53resolver/backend.go:323`
- **Performance:** `DeleteResolverEndpoint` iterates all rules and rule-associations under a write lock to cascade-delete, creating O(n) work inside the global write lock. `/home/user/gopherstack/services/route53resolver/backend.go:658`
- **Leaks:** No background goroutines; resource maps are bounded by CRUD operations.
- **UI:** UI route exists at `ui/src/routes/route53resolver/`; Firewall rules/rule groups/domain-lists, Outpost resolvers, DNSSEC configs, QueryLog configs, and resolver configs have no UI surface.

---

### s3

- **Parity:** `handlePutBucketAbac` is a pure no-op returning 200 OK — S3 Table bucket ABAC configuration is silently discarded. `/home/user/gopherstack/services/s3/handler_stubs.go:473`
- **Parity:** `handleListDirectoryBuckets` always returns an empty list — S3 Express directory buckets are not emulated. `/home/user/gopherstack/services/s3/handler_stubs.go:418`
- **Parity:** `handleUpdateBucketMetadataInventoryTableConfig` and `handleUpdateBucketMetadataJournalTableConfig` are pure no-ops, silently dropping metadata configuration updates. `/home/user/gopherstack/services/s3/handler_stubs.go:557,566`
- **Performance:** `cleanupDefaultMultipart` in the janitor holds the full backend write lock while scanning all in-progress uploads across all buckets, blocking all S3 operations during cleanup. `/home/user/gopherstack/services/s3/janitor.go:303`
- **Performance:** `applyStorageClassTransitions` and `evictNoncurrentVersions` use nested `bucket.mu.Lock` + `obj.mu.Lock` inside the sweep loop — nested locks held sequentially over many objects can cause long tail latency under concurrent writes. `/home/user/gopherstack/services/s3/janitor.go:956-1030`
- **Leaks:** The `replicationWg` and `serviceCtx` govern replication goroutine lifecycle; if `Shutdown()` is not called the replication goroutines leak. `/home/user/gopherstack/services/s3/backend_memory.go:176`
- **UI:** UI route exists at `ui/src/routes/s3/`; S3 Intelligent-Tiering, S3 Inventory, S3 Analytics, Replication configuration, Lifecycle rules, Object Lock configuration, Select Object Content, and Batch Delete have no UI representation.

---

### s3control

- **Parity:** `handleListJobs` returns all jobs with no `MaxResults`/`NextToken` pagination support — large job lists are returned in a single untruncated response. `/home/user/gopherstack/services/s3control/handler.go:2071`
- **Parity:** `handleListAccessGrants` and `handleListAccessGrantsLocations` have no pagination support either, returning complete sets regardless of collection size. `/home/user/gopherstack/services/s3control/handler.go:829,844`
- **Parity:** `GetAccessGrantsInstanceForPrefix` ignores the `prefix` parameter entirely and returns the AGI regardless of whether it covers the requested prefix. `/home/user/gopherstack/services/s3control/backend_batch1.go:150`
- **Parity:** `GetDataAccess` (temporary credential vending) is wired but does not issue real STS-style temporary credentials — it returns canned/placeholder credential values. `/home/user/gopherstack/services/s3control/handler.go:836`
- **Performance:** All backend operations share a single global `b.mu` `RWMutex`, so any write operation (e.g., `CreateJob`) blocks all concurrent reads (e.g., `ListAccessGrants`, `GetStorageLensConfiguration`). `/home/user/gopherstack/services/s3control/backend.go:50`
- **Performance:** `ListJobs` copies all jobs into a slice without any filtering or early termination, performing a full scan on every list call even when `jobStatuses` filter could prune early. `/home/user/gopherstack/services/s3control/handler.go:2074`
- **Leaks:** No background goroutines detected; all maps are bounded by CRUD operations.
- **UI:** UI route exists at `ui/src/routes/s3control/`; the UI shows Access Points, Multi-Region Access Points, and Storage Lens; Batch Jobs, Access Grants, Access Grants Locations, Object Lambda Access Points, StorageLens Groups, and Outpost bucket operations have no UI representation.

---

### s3tables

- **Parity:** `handleGetTableBucketMetricsConfiguration` always returns an empty `configuration` map regardless of whether metrics were enabled via `PutTableBucketMetricsConfiguration` — the stored `MetricsEnabled` flag is not surfaced. `/home/user/gopherstack/services/s3tables/handler.go:748`
- **Parity:** `handleGetTableMaintenanceJobStatus` returns an empty `status` map unconditionally — no maintenance job execution state is emulated. `/home/user/gopherstack/services/s3tables/handler.go:922`
- **Parity:** `handleGetTableRecordExpirationJobStatus` is wired to `GetTableRecordExpirationJobStatus` but returns a stub response (empty status), as no job execution engine exists. `/home/user/gopherstack/services/s3tables/handler.go:264`
- **Parity:** `handleGetTableEncryption` returns a hardcoded `AES256` response regardless of whether bucket-level encryption was configured, so SSE-KMS settings are not reflected at the table level. `/home/user/gopherstack/services/s3tables/handler.go:994`
- **Parity:** `ListTableBuckets`, `ListNamespaces`, and `ListTables` have no pagination (`continuationToken`/`maxBuckets`/`maxNamespaces`/`maxTables` query params are ignored), returning complete lists. `/home/user/gopherstack/services/s3tables/handler.go:579,1152,1284`
- **Parity:** `validMetadataLocation` only checks the `s3://` prefix and `.json`/`.json.gz` suffix — it does not verify the location is under the table's warehouse path, bypassing the AWS guard. `/home/user/gopherstack/services/s3tables/backend.go:1024`
- **Performance:** `DeleteTableBucket` acquires all four mutexes (`muBuckets`, `muNamespaces`, `muTables`, `muState`) simultaneously and then performs O(n) scans over all tables and namespaces to cascade-delete, blocking all service operations during the scan. `/home/user/gopherstack/services/s3tables/backend.go:372`
- **Leaks:** No background goroutines; all resource maps are bounded by CRUD operations.
- **UI:** UI route exists at `ui/src/routes/s3tables/`; the UI only lists table buckets and tables; Namespace CRUD, Table policies, Bucket policies, Encryption config, Maintenance config, Replication config, Record expiration config, and tagging operations have no UI representation.

### sagemaker

- **Parity:** 335+ stub operations in `stubOpsSupported()` return empty ARNs and empty arrays as canned responses with no real logic. `services/sagemaker/handler_stubs.go:90`
- **Parity:** Every Create stub returns `""` as the ARN field, violating the AWS shape which requires a non-empty ARN on success. `services/sagemaker/handler_stubs.go:234`
- **Parity:** All List stubs return `[]any{}` unconditionally, ignoring any stored state. `services/sagemaker/handler_stubs.go:486`
- **Parity:** `DescribePipelineExecution` always returns `"Succeeded"` regardless of actual execution state. `services/sagemaker/handler_stubs.go:438`
- **Parity:** Stub dispatch via `stubResponseFor()` uses a single switch with no validation of required input fields, accepting malformed requests silently. `services/sagemaker/handler_stubs.go:226`
- **Performance:** Default page size constant `sagemakerDefaultPageSize = 100` is set but stub list ops bypass pagination entirely, returning full empty arrays. `services/sagemaker/backend.go:42`
- **UI:** UI route exists under `ui/src/routes/sagemaker/`; however, all 335+ stub operations lack dedicated UI panels, so only top-level navigation is present.

### sagemakerruntime

- **Parity:** `handleInvokeEndpoint` returns hardcoded string `"mock response from Gopherstack"` regardless of model input or endpoint configuration. `services/sagemakerruntime/handler.go:154`
- **Parity:** No endpoint existence check in `handleInvokeEndpoint` — any endpoint name is accepted without validation. `services/sagemakerruntime/handler.go:125`
- **Parity:** `handleInvokeEndpointWithResponseStream` returns a hardcoded stream body, never consulting stored endpoint state. `services/sagemakerruntime/handler.go:186`
- **Parity:** `RecordAsyncInvocation` stores a fake S3 OutputLocation `s3://sagemaker-runtime-mock/...` rather than using the caller-supplied location. `services/sagemakerruntime/backend.go:185`
- **Performance:** `evictOldest` performs a full O(n) map scan to find the minimum key on every call, invoked at capacity on every `RecordInvocation`, `StartSession`, and `RecordAsyncInvocation`. `services/sagemakerruntime/backend.go:210`
- **Leaks:** Handler has no `Shutdown` method and does not implement `service.Shutdowner`, so any background state is not cleanly stopped. `services/sagemakerruntime/handler.go`
- **UI:** No UI route exists for `sagemakerruntime` under `ui/src/routes/`; the service has zero UI coverage.

### scheduler

- **Parity:** `paginate` constructs `nextToken` as a transparent composite `group/name` key, not an opaque base64/UUID token, exposing internal key structure to callers. `services/scheduler/backend.go:780`
- **Parity:** `ListSchedules` and `ListScheduleGroups` do not validate that a caller-supplied `nextToken` actually belongs to the current result set, so arbitrary strings can be passed. `services/scheduler/backend.go:759`
- **Performance:** `ListSchedules` deep-clones every schedule (including `tags.Clone()`) under a read lock before filtering, allocating O(n) clones even when most will be discarded by filter. `services/scheduler/backend.go:447`
- **Performance:** `ListScheduleGroups` applies the same clone-then-filter pattern, allocating clones for all groups before applying name-prefix filter. `services/scheduler/backend.go:748`
- **Leaks:** No unbounded collection found; `evictOldest`-style cap not needed here as schedules are explicitly deleted.
- **UI:** UI route exists under `ui/src/routes/scheduler/`; `StartWorker` and `Shutdown` are both properly implemented. `services/scheduler/handler.go:192`

### secretsmanager

- **Parity:** `ListSecrets` pagination token is `strconv.Itoa(end)` — a plain integer offset, non-opaque and easily guessable. `services/secretsmanager/backend.go:765`
- **Parity:** `ValidateResourcePolicy` is a near no-op that returns success without actually parsing or enforcing the policy JSON. `services/secretsmanager/handler.go:317`
- **Parity:** `PutSecretValue` does not enforce the `ClientRequestToken` idempotency constraint documented by AWS. `services/secretsmanager/backend.go`
- **Performance:** `GetSecretValue` acquires a full write lock (`mu.Lock`) solely to update `LastAccessedDate`, blocking all concurrent reads unnecessarily. `services/secretsmanager/backend.go:403`
- **Performance:** `secretHasTagKey` calls `s.Tags.Clone()` (full map copy) inside the per-secret filter loop executed on every `ListSecrets` call. `services/secretsmanager/backend.go:832`
- **Leaks:** Rotation scheduler uses a 1-second ticker that performs an O(n) scan of all secrets on every tick, regardless of whether any rotation is due. `services/secretsmanager/backend.go:2437`
- **UI:** UI route exists under `ui/src/routes/secretsmanager/`.

### securityhub

- **Parity:** `encodeToken` returns `strconv.Itoa(offset)` — a plain integer, non-opaque pagination token. `services/securityhub/backend.go:2057`
- **Parity:** `SortCriteria` parameter in `GetFindings` is received as `_ []map[string]any` and silently discarded, so results are never sorted by caller-specified criteria. `services/securityhub/backend.go:766`
- **Parity:** `GetInsightResults` returns empty `ResultValues` regardless of the insight's filters, making the insight aggregation a no-op stub. `services/securityhub/backend.go:1047`
- **Parity:** `GetFindingHistory` returns empty `Records` for every finding, providing no historical audit trail. `services/securityhub/backend.go:923`
- **Performance:** `GetFindings` iterates the entire `b.findings` map on every call with no index or pre-filtering, O(n) per request. `services/securityhub/backend.go:775`
- **Performance:** `BatchUpdateFindings` calls `maps.Copy(f, updates)` for each finding individually, causing O(findings × updates) map operations with no batching optimization. `services/securityhub/backend.go:900`
- **UI:** UI route exists under `ui/src/routes/securityhub/`.

### serverlessrepo

- **Parity:** `handleListApplications` uses the last application's `Name` field as `nextToken`, exposing internal resource names as pagination cursors. `services/serverlessrepo/handler.go:779`
- **Parity:** `handleListApplicationVersions` uses the `SemanticVersion` string as `nextToken`, a transparent non-opaque cursor. `services/serverlessrepo/handler.go:959`
- **Parity:** `handleListApplicationDependencies` uses the `applicationId` as `nextToken`, again a transparent non-opaque cursor. `services/serverlessrepo/handler.go:1245`
- **Parity:** `handleCreateCloudFormationChangeSet` response omits the `ChangeSetArn` field that AWS returns on success. `services/serverlessrepo/handler.go:1115`
- **Parity:** `handleGetApplicationPolicy` returns a `"statements"` key (lowercase) instead of the AWS-specified `"Statements"` key, breaking JSON unmarshalling for compliant clients. `services/serverlessrepo/handler.go:1152`
- **Performance:** No significant hot-path allocation issues identified beyond pagination allocation patterns.
- **UI:** UI route exists under `ui/src/routes/serverlessrepo/`.

### servicediscovery

- **Parity:** `createNamespace` (and similar async operations) complete synchronously and immediately set status to `SUCCESS`, but return an `OperationId` implying async behavior — the operation is not actually async. `services/servicediscovery/backend.go:303`
- **Parity:** `DiscoverInstances` does not honor `MaxResults` or `NextToken` parameters, always returning the full instance list without pagination. `services/servicediscovery/backend.go:704`
- **Parity:** `GetOperation` always returns the final terminal state immediately, never simulating intermediate `PENDING`/`IN_PROGRESS` states that real AWS returns. `services/servicediscovery/backend.go`
- **Performance:** `ListNamespaces`, `ListServices`, and `ListInstances` all perform full map iteration plus a sort on every call, with no caching or index. `services/servicediscovery/backend.go:436`
- **Leaks:** `TagResource` calls `maps.Copy(ns.Tags, tags)` directly on the stored object's tag map, mutating shared state without cloning, risking data races if the map is read concurrently. `services/servicediscovery/backend.go:886`
- **UI:** UI route exists under `ui/src/routes/servicediscovery/`.

### ses

- **Parity:** `GetSendQuota` returns hardcoded values (`Max24HourSend: 200`, `MaxSendRate: 1`) without tracking actual sends against the quota. `services/ses/backend.go:142`
- **Parity:** `VerifyEmailIdentity` immediately sets identity status to `"Success"` without any async verification flow, unlike real AWS which sends a verification email. `services/ses/backend.go`
- **Parity:** SES uses form-encoded (`application/x-www-form-urlencoded`) request bodies per AWS protocol, but error response shapes should be XML; any JSON error responses would break real AWS SDK clients.
- **Leaks:** `StartWorker` starts the janitor with `go h.janitor.Run(ctx)` as a fire-and-forget goroutine with no way to wait for it to finish on shutdown. `services/ses/handler.go:65`
- **Leaks:** Handler does not implement `service.Shutdowner`, so the framework cannot signal or join the janitor goroutine on graceful shutdown. `services/ses/handler.go`
- **Leaks:** `janitor.Run` creates a `time.NewTicker` with `defer ticker.Stop()` but provides no done channel to signal completion back to the caller after the ticker is stopped. `services/ses/janitor.go:39`
- **UI:** UI route exists under `ui/src/routes/ses/`.

# Audit: group18 (sesv2, shield, sns, sqs, ssm, ssoadmin, stepfunctions, sts)

---

### sesv2

**Parity:**
- `PutAccountSuppressionAttributes`, `PutAccountVdmAttributes`, and `PutAccountDedicatedIPWarmupAttributes` are explicit no-op stubs that return nil without persisting anything. services/sesv2/backend_ops.go:78-91
- `BatchGetMetricData` returns empty timestamps/values for every query instead of synthesizing any data, making metric-based tests impossible. services/sesv2/backend.go:524-537
- `GetDomainDeliverabilityCampaign` returns an empty `map[string]any{}` with no campaign fields. services/sesv2/backend_ops.go:565-567
- `GetDomainStatisticsReport` returns an empty `map[string]any{}` with no stats or domain breakdown. services/sesv2/backend_ops.go:570-572
- All Tenant ops (`CreateTenant`, `GetTenant`, `DeleteTenant`, `ListTenants`, etc.) are full stubs returning empty maps. services/sesv2/backend_ops.go:1237-1274
- All MultiRegionEndpoint ops (`CreateMultiRegionEndpoint`, `DeleteMultiRegionEndpoint`, etc.) are stubs; the create does not persist and delete ignores the name. services/sesv2/backend_ops.go:1211-1232
- `TagResource`/`UntagResource` for email identities dispatch to `&emptyDeleteOutput{}` no-ops, so tags are never stored. services/sesv2/handler.go:595-598
- `handleListTagsForResource` always returns an empty tag list regardless of the ARN query parameter. services/sesv2/handler.go:1025

**Performance:**
- `ListEmailIdentities` builds a full slice copy and sorts it under `RLock`, blocking all concurrent readers for O(n log n). services/sesv2/backend.go:360-377
- `SendEmail` acquires the write lock, appends, then does an in-place compaction with `make`+copy when `len >= 20000`, blocking all requests during the copy. services/sesv2/backend.go:475-510

**Leaks:**
- No issues identified beyond the stub map growth for tenant/reputation/multi-region data, which is bounded only by the number of stubs called.

**UI:**
- UI route exists at `ui/src/routes/sesv2/`.
- BatchGetMetricData, all Tenant ops, all MultiRegionEndpoint ops, and all ReputationEntity ops have no corresponding UI panels.

---

### shield

**Parity:**
- `decodeOffsetToken`/`encodeOffsetToken` use base64-encoded integer offsets rather than opaque tokens; clients can enumerate or jump to arbitrary pages. services/shield/handler.go:752-772
- `handleGetAttackVectorDefinitionVersion` returns a hardcoded `"1"` string instead of fetching or computing the real taxonomy version. services/shield/handler.go:1476-1479
- `ErrNotFound` is mapped to HTTP 400 (BadRequest) with type "ResourceNotFoundException"; AWS Shield actually returns 400 for this, so the code is correct, but the error-type label does not match the HTTP status expectation callers have from other services, causing confusion. services/shield/handler.go:406

**Performance:**
- `ListProtections` backend clones every protection and sorts the slice while holding `RLock`, blocking all readers for O(n log n). services/shield/backend.go:440-462
- `ListProtectionGroups` same pattern — full clone plus sort under `RLock`. services/shield/backend.go:1096-1119
- `applyProtectionFilters` reuses `protections[:0]` which mutates the backing array returned by `ListProtections`, corrupting any concurrent reader holding a reference to the original slice. services/shield/handler.go:823
- `resolveShieldProtectionARN` falls back to an O(n) full scan of all protections when the ARN prefix parse fails. services/shield/backend.go:577-583
- `DescribeAttackStatistics` does an O(n) scan of all attacks to bucket them by month. services/shield/backend.go:1396

**Leaks:**
- `nameIndex` and `resourceARNIndex` grow unboundedly between creation events and are only pruned when `DeleteProtection` is explicitly called; no TTL or background sweep. services/shield/backend.go:561

**UI:**
- UI route exists at `ui/src/routes/shield/`.
- No UI panels for DescribeAttackStatistics, GetAttackVectorDefinitionVersion, or subscription management.

---

### sns

**Parity:**
- `CheckIfPhoneNumberIsOptedOut`, `OptInPhoneNumber`, and all SMS sandbox ops (`CreateSMSSandboxPhoneNumber`, `DeleteSMSSandboxPhoneNumber`, `VerifySMSSandboxPhoneNumber`, `ListSMSSandboxPhoneNumbers`) are listed as supported but not verified for full field parity. services/sns/handler.go:210-255
- `GetSMSSandboxAccountStatus` always returns sandbox mode without persisting any sandbox-enable/disable action. services/sns/handler.go:210-255

**Performance:**
- `evictEarliestLocked` iterates all dedup entries to find the minimum-key entry on every overflow, making it O(n) per publish on a full map. services/sns/handler.go:143-154
- `sweepExpiredLocked` is called opportunistically inside `isDuplicate` and is O(n) over all entries. services/sns/handler.go:120-140
- `fifoSeqNums` is a `sync.Map` whose keys (topic ARNs) are never deleted after a topic is removed, causing unbounded growth for high-churn topic workloads. services/sns/handler.go:161

**Leaks:**
- `fifoSeqNums` (`sync.Map`) entries are never deleted on `DeleteTopic`, so the map grows without bound for long-running servers with repeated topic creation/deletion. services/sns/handler.go:161

**UI:**
- UI route exists at `ui/src/routes/sns/`.
- No UI panels for platform applications/endpoints (SNS mobile push) or SMS sandbox operations.

---

### sqs

**Parity:**
- `lookupQueueByURL` falls back to a URL-string scan across all regions when no region is threaded through, defeating region isolation. services/sqs/backend.go:317-335

**Performance:**
- `pruneState` collects a full snapshot of all queues under `RLock` on every janitor tick, allocating a slice of all queue pointers even when most queues need no pruning. services/sqs/backend.go:213-219
- `computeMD5OfMessageAttributes` sorts and iterates all attributes on every message receive, adding O(k log k) per receive for attribute-heavy messages. services/sqs/backend.go:395-431

**Leaks:**
- No goroutine/ticker leaks found; janitor lifecycle is properly managed via `janitorStop` channel and `Close()`. services/sqs/backend.go:165-174

**UI:**
- UI route exists at `ui/src/routes/sqs/`.
- No UI panel for message-move tasks (`StartMessageMoveTask`, `CancelMessageMoveTask`, `ListMessageMoveTasks`).

---

### ssm

**Parity:**
- The hardcoded mock KMS key `"gopherstack-mock-kms-key-32byte!"` is used for all SecureString encryption when no real KMS backend is wired, meaning ciphertext is deterministic and not key-isolated. services/ssm/backend.go:57
- ~80 stub operations (all registered in `ssmStubOps`) decode JSON and delegate to backend but all backend implementations return empty structs with no field population, so no stub operation produces meaningful response data. services/ssm/handler_stubs.go:1-911
- `DescribeAssociationExecutionTargets` and `DescribeAssociationExecutions` both return empty output structs with no entries, ignoring any filter inputs. services/ssm/backend_stubs.go:190-198

**Performance:**
- `expireCommandsLocked` is called on every write path (not just periodic) and scans all commands in the region to find expired ones, adding O(n) to every command write. services/ssm/backend.go:326-338
- `paramNamesSorted` sorted-slice is maintained for prefix lookups but every `PutParameter` must insert into the sorted slice, which requires O(n) shift for non-tail insertions. services/ssm/backend.go:194

**Leaks:**
- All region-keyed sub-maps (`parameters`, `history`, `documents`, etc.) are allocated lazily per-region and never deleted even if all resources for a region are removed, leaving empty maps indefinitely. services/ssm/backend.go:221-265

**UI:**
- UI route exists at `ui/src/routes/ssm/`.
- No UI panels for automation executions, maintenance windows, patch baselines, OpsItems, inventory, compliance, or associations — the vast majority of SSM operations have no UI.

---

### ssoadmin

**Parity:**
- `paginateStrings` and `paginateBy` use a value-based cursor (the first item of the next page) as the `NextToken`, making tokens non-opaque and guessable by clients. services/ssoadmin/handler.go:44-83
- `creationStatuses`, `deletionStatuses`, and `provisioningStatuses` maps grow unboundedly and are never pruned; long-lived servers accumulate all historical provisioning records indefinitely. services/ssoadmin/backend.go:309-312
- `handleListInstances` sorts instances on every call inside the handler rather than at write time, adding O(n log n) on every list. services/ssoadmin/handler.go:468

**Performance:**
- `handleListInstances` calls `h.Backend.ListInstances()` then sorts the result slice in the handler layer on each request, adding O(n log n) per call. services/ssoadmin/handler.go:468
- All `List*` handlers do a full backend collection copy plus sort under lock on every request; with 60+ list operations this pattern is pervasive. services/ssoadmin/handler.go:457-492

**Leaks:**
- `creationStatuses` and `deletionStatuses` are write-only maps with no eviction, growing indefinitely under sustained `CreateAccountAssignment`/`DeleteAccountAssignment` load. services/ssoadmin/backend.go:309-312

**UI:**
- UI route exists at `ui/src/routes/ssoadmin/`.
- No UI panels for Application ops (CreateApplication, DeleteApplication, ListApplications, all auth-method/grant/scope/session ops), TrustedTokenIssuer ops, or ABAC/IACAC configuration.

---

### stepfunctions

**Parity:**
- `TestState` is listed as a supported operation but no per-state result is returned to callers; the operation only validates the definition. services/stepfunctions/handler.go:206
- `DescribeMapRun` / `UpdateMapRun` / `ListMapRuns` are listed as supported but MapRun data is never stored by `StartExecution` for Map state parallel branches. services/stepfunctions/handler.go:181-214

**Performance:**
- `smExecutions` stores all execution ARNs per state machine as a growing slice; `ListExecutions` must scan this entire slice even when a status filter is applied, giving O(n) per filtered list call. services/stepfunctions/backend.go:167
- `SweepTaskTokens` acquires the write lock, iterates all entries in `tasksByToken` to collect stale entries, then deletes them, blocking all concurrent operations for the full O(n) sweep. services/stepfunctions/backend.go:475-499

**Leaks:**
- `deletedExecs` tombstone entries are only cleaned up when the execution goroutine exits naturally; if a goroutine is cancelled by `DeleteStateMachine` while blocked in a service integration, the tombstone key stays in the map until `PruneExecutions` runs. services/stepfunctions/backend.go:170-172
- The handler `tags` map is stored in the `Handler` struct and never deleted when a state machine is deleted via a path that bypasses `DeleteStateMachine` (e.g. server reset), leaking tag memory. services/stepfunctions/handler.go:112-114

**UI:**
- No UI route exists for stepfunctions (`ui/src/routes/stepfunctions/` is absent).
- All operations (CreateStateMachine, StartExecution, GetExecutionHistory, ListStateMachines, ListExecutions, all alias/version ops, all activity ops) have no UI.

---

### sts

**Parity:**
- `GetCallerIdentity` returns a hardcoded `MockUserID` / root ARN when the access key is not in the sessions map, rather than returning `InvalidClientTokenId` for truly unknown keys. services/sts/backend.go:611-622
- `AssumeRoleWithSAML` accepts any SAMLAssertion string without validating its base64 encoding or XML structure, unlike real AWS which rejects malformed assertions. services/sts/handler.go:382-417
- `GetWebIdentityToken` is listed as a supported operation but is not a real AWS STS operation (the real call is `AssumeRoleWithWebIdentity`); having it in `GetSupportedOperations` inflates the supported-ops list incorrectly. services/sts/handler.go:88-100
- `DecodeAuthorizationMessage` accepts any base64 blob as a valid encoded message rather than verifying it was issued by the STS backend. services/sts/backend.go:line ~800

**Performance:**
- `evictExpiredSessionsLocked` is skipped below `sessionEvictThreshold = 256` entries but scans all sessions when triggered, adding an O(n) pass inside the write-lock on every session creation above the threshold. services/sts/backend.go:214-224
- `storeSession` acquires the mutex, calls `evictExpiredSessionsLocked`, and then stores the new session — all while holding the single `sync.Mutex`, so all 11 credential-issuing operations serialize on expiry sweeps. services/sts/backend.go:229-235

**Leaks:**
- No goroutine/ticker leaks found; janitor lifecycle is correctly managed via `ctx.Done()` and `ticker.Stop()`. services/sts/janitor.go:45-58

**UI:**
- UI route exists at `ui/src/routes/sts/`.
- No UI panels for AssumeRoleWithSAML, AssumeRoleWithWebIdentity, AssumeRoot, GetDelegatedAccessToken, GetWebIdentityToken, or DecodeAuthorizationMessage — only basic caller identity is likely surfaced.

### support

- **Parity:** `DescribeCreateCaseOptions` ignores all 4 input params (serviceCode, issueType, language, nextToken) and always returns a static canned response. — `services/support/backend.go:533`
- **Parity:** `DescribeSupportedLanguages` ignores all 3 input params and always returns the same 4 hardcoded languages regardless of filter. — `services/support/backend.go:599`
- **Parity:** `DescribeTrustedAdvisorCheckResult` always returns a canned "ok" status with empty FlaggedResources, never reflecting actual check data. — `services/support/backend.go:642`
- **Parity:** `DescribeTrustedAdvisorCheckSummaries` returns static canned summaries regardless of actual persisted check state. — `services/support/backend.go:661`
- **Parity:** Pagination token is a non-opaque base64-encoded integer offset, allowing clients to craft tokens and seek to arbitrary list positions. — `services/support/accuracy.go:470`
- **Performance:** `validCheckID` calls `trustedAdvisorChecks()` which allocates a new 8-element slice on every invocation; called once per element in the input IDs slice. — `services/support/accuracy.go:371`
- **Performance:** `DescribeTrustedAdvisorCheckRefreshStatuses` acquires a full write lock (`b.mu.Lock`) just to read statuses and increment poll counts; should use RLock with separate mutation. — `services/support/backend.go:609`
- **Leaks:** `attachmentSets` map grows unboundedly; no per-attachment eviction cap and janitor only cleans stale cases, not orphaned attachment sets. — `services/support/backend.go:68`
- **Leaks:** `checkRefreshStatuses` map is only appended to and never evicted from the backend, growing without bound. — `services/support/backend.go:74`
- **UI:** No UI route exists under `ui/src/routes/support/`; all 16 support operations have no UI coverage.

### swf

- **Parity:** `openCountsLocked` returns 0 for timer counts and child-workflow counts — those maps are never queried, so `GetWorkflowExecutionOpenCounts` always misreports those fields. — `services/swf/backend.go:1278`
- **Parity:** Count responses from `GetWorkflowExecutionOpenCounts` never set the `Truncated` field AWS returns when counts exceed 1000. — `services/swf/handler.go:1099`
- **Parity:** `handleDescribeWorkflowExecution` type-asserts to `*InMemoryBackend` directly, breaking the `Backend` interface and preventing alternative backend implementations. — `services/swf/handler.go:1099`
- **Performance:** `openCountsLocked` performs an O(n) scan over all `activeActivityTasks` plus an O(n*m) scan of all `decisionQueues` entries to count tasks for a single workflow. — `services/swf/backend.go:1262`
- **Performance:** `PollForDecisionTask` builds a full copy of the workflow history slice inside the write lock, which is unbounded for long-running workflows. — `services/swf/backend.go:1485`
- **Leaks:** `executionOrder` slice appends the workflow key again on restart without removing the previous entry, causing indefinite growth with duplicate keys. — `services/swf/backend.go:1117`
- **Leaks:** `activeActivityTasks` entries are never auto-expired on heartbeat timeout; tasks that miss heartbeats accumulate forever. — `services/swf/backend.go:1507`
- **UI:** No UI route exists under `ui/src/routes/swf/`; all SWF operations have no UI coverage.

### textract

- **Parity:** `ErrAdapterNotFound` and `ErrAdapterVersionNotFound` both map to `"InvalidParameterException"` but AWS returns `"ResourceNotFoundException"` for missing adapters and versions. — `services/textract/handler.go:255`
- **Parity:** `listAdaptersInput` is an empty struct with no `NextToken` or `MaxResults` fields, so `ListAdapters` never paginates and always returns all adapters. — `services/textract/handler.go:749`
- **Parity:** `listAdapterVersionsInput` is missing `Status` filter, `NextToken`, and `MaxResults` pagination fields present in the real AWS API. — `services/textract/handler.go:909`
- **Performance:** `trimJobsIfNeeded` sorts all stored jobs with O(n log n) on every `StartDocumentAnalysis` call just to find excess entries to evict. — `services/textract/backend.go:1186`
- **Performance:** `syntheticBlocks` allocates 8 new UUID strings and multiple Block structs on every synchronous `AnalyzeDocument` call in the hot path. — `services/textract/backend.go:716`
- **Leaks:** `runDelayed` uses `time.After(delay)` which keeps the underlying timer goroutine alive until it fires even if the context is cancelled first; should use `time.NewTimer` with `defer t.Stop()`. — `services/textract/backend.go:484`
- **UI:** No UI route exists under `ui/src/routes/textract/`; all Textract operations have no UI coverage.

### timestreamquery

- **Parity:** `QueryWithOptions` always returns empty rows (`rows := []Row{}`) regardless of the query string; no actual query execution is performed. — `services/timestreamquery/backend.go:432`
- **Parity:** The `queries` map is not region-isolated (noted in a code comment), allowing cross-region query ID collisions when multiple regions share one backend instance. — `services/timestreamquery/backend.go:118`
- **Performance:** The eviction loop in `QueryWithOptions` iterates the entire map to find any one key to delete with no LRU ordering, making it O(n) per eviction. — `services/timestreamquery/backend.go:432`
- **Performance:** `regionFromARN` uses `strings.Split` (unbounded) rather than `strings.SplitN(s, ":", 5)`, allocating an unnecessary full parts array on every call. — `services/timestreamquery/backend.go:49`
- **UI:** No UI route exists under `ui/src/routes/timestreamquery/`; all TimestreamQuery operations have no UI coverage.

### timestreamwrite

- **Parity:** `Handler` takes a concrete `*InMemoryBackend` rather than the `StorageBackend` interface, preventing alternative backend implementations and breaking the pattern used by other services. — `services/timestreamwrite/handler.go:276`
- **Leaks:** `StartWorker` launches `go janitor.Run(ctx)` with no done channel or WaitGroup, making it impossible to await janitor shutdown before process exit. — `services/timestreamwrite/handler.go:329`
- **UI:** No UI route exists under `ui/src/routes/timestreamwrite/`; all TimestreamWrite operations have no UI coverage.

### transcribe

- **Parity:** `GetTranscriptionJob` acquires a full write lock (`b.mu.Lock`) even though it only reads the job and then advances deferred state — a read-then-conditional-write that holds an exclusive lock for purely read-only jobs. — `services/transcribe/backend.go:357`
- **Parity:** `StartTextTranslationJob` (transcribe's translation-job equivalent) and `paginateList` use a plain integer string as a next-page token rather than an opaque token, letting callers seek to arbitrary offsets. — `services/transcribe/backend.go:1428`
- **Parity:** `ErrVocabularyNotFound` is mapped to `"BadRequestException"` which is documented in a comment as intentional, but `GetVocabulary` for a missing vocabulary still returns HTTP 404 via `ErrNotFound` creating an inconsistency. — `services/transcribe/backend.go:24`
- **Performance:** Every `List*Jobs` method (`ListTranscriptionJobs`, `ListCallAnalyticsJobs`, etc.) copies all matching job structs into a new slice and sorts it under the RLock on every call, with no caching. — `services/transcribe/backend.go:378`
- **Leaks:** The `resourceTags` map is never validated against existing ARNs on `TagResource`, so tagging a non-existent ARN silently grows the map without bound. — `services/transcribe/backend.go:1437`
- **UI:** No UI route exists under `ui/src/routes/transcribe/`; all Transcribe operations have no UI coverage.

### transfer

- **Parity:** `handleTestConnection` always returns `"OK"` and `"Connection to remote server is successful"` without attempting any real connectivity check. — `services/transfer/handler.go:3328`
- **Parity:** `handleTestIdentityProvider` always returns a synthetic IAM role response for SERVICE_MANAGED servers; no actual identity provider invocation is simulated. — `services/transfer/handler.go:3359`
- **Parity:** `handleDescribeSecurityPolicy` returns only a static canned policy name map rather than the full `SecurityPolicy` shape AWS returns (algorithms, protocols, etc.). — `services/transfer/handler.go:3148`
- **Parity:** `StartServer` and `StopServer` use `time.AfterFunc` goroutines for async state transitions with no cancellation or WaitGroup, leaving orphaned goroutines if the backend is Reset while a transition is in flight. — `services/transfer/backend.go:1027`
- **Performance:** `arnExists` in the `tags` validation path performs a linear scan of all terminologies and parallel data on every `TagResource` and `UntagResource` call; should maintain a reverse-ARN index. — (translate service; see also transfer: `ListServers` clones every Server under lock, performing O(n) deep copies including slice allocations for each entry) — `services/transfer/backend.go:954`
- **UI:** No UI route exists under `ui/src/routes/transfer/`; all Transfer Family operations have no UI coverage.

### translate

- **Parity:** `StartTextTranslationJob` creates a job with status `"IN_PROGRESS"` but there is no background goroutine or timer that ever advances it to `"COMPLETED"`, so jobs are permanently stuck in IN_PROGRESS. — `services/translate/backend.go:388`
- **Parity:** `arnExists` only checks `terminologies` and `parallelData` ARNs, so `TagResource`/`UntagResource`/`ListTagsForResource` on translation jobs or any other resource type silently returns `ErrNotFound`. — `services/translate/backend.go:509`
- **Parity:** `listLanguages` ignores the `DisplayLanguageCode` input parameter and always returns language names in English. — `services/translate/handler.go:549`
- **Performance:** `arnExists` performs two full linear map scans on every tag operation; an O(1) reverse-ARN index would eliminate these scans. — `services/translate/backend.go:509`
- **Performance:** `paginate` uses a linear O(n) scan to find the `nextToken` key position in the sorted slice on every paginated list call instead of using binary search. — `services/translate/backend.go:546`
- **Leaks:** `ErrNotFound`, `ErrConflict`, and `ErrValidation` are plain `errors.New()` sentinel values rather than `awserr.New()`, losing the AWS error shape and `errors.Is` compatibility used by other services. — `services/translate/backend.go:18`
- **UI:** No UI route exists under `ui/src/routes/translate/`; all Translate operations have no UI coverage.

### verifiedpermissions

**Parity:**
- `IsAuthorizedWithToken`/`BatchIsAuthorizedWithToken` accept but completely ignore the access/identity token so no token-claim-based authorization is evaluated — backend.go:1156-1226
- `TagResource`/`UntagResource`/`ListTagsForResource` silently succeed but no-op for non-policyStore resource types (policies, templates, identity sources) — backend.go:985-1058
- `errUnknownAction` is mapped to `ValidationException` instead of AWS's `UnknownOperationException` — handler.go:227
- `paginate` uses the plain resource ID as `nextToken` making it non-opaque and guessable — backend.go:1461-1483

**Performance:**
- `buildCedarPolicySet` iterates and re-parses every stored STATIC policy on every single authorization call (O(n) per auth, no caching) — backend.go:411-434
- `ListPolicyStores` copies the entire `policyStores` map into a slice under the read lock on every call — backend.go:548-562

**Leaks:**
- No goroutine, ticker, or unbounded-map leaks detected.

**UI:**
- `ui/src/routes/verifiedpermissions/` exists; no UI coverage for `BatchIsAuthorized`, `BatchGetPolicy`, `PutSchema`, `GetSchema`, and `IsAuthorizedWithToken` operations.

---

### vpclattice

**Parity:**
- `handleListTargets` silently ignores all body filter parameters (target IP/port filters), always returning unfiltered results — handler.go:1002
- `handleDeleteSNSA` returns HTTP 200 with a body instead of HTTP 202 as the AWS API specifies — handler.go:549-555
- `handleDeleteSNVA` returns HTTP 200 with a body instead of HTTP 202 as the AWS API specifies — handler.go:645-651
- `handleListSNSAs`/`handleListSNVAs` do not support filtering by VPC/service via request body unlike the real AWS REST API — handler.go:557-583

**Performance:**
- `ListServices` copies the entire services map into a slice on every call under the read lock — backend.go:843-857

**Leaks:**
- No goroutine, ticker, or unbounded-map leaks detected.

**UI:**
- No `ui/src/routes/vpclattice/` route exists; the entire service has no UI coverage.

---

### waf

**Parity:**
- `GetSampledRequests` always returns an empty slice with no actual request sampling — backend.go:1804-1805
- `GetChangeTokenStatus` always returns `INSYNC` regardless of token state due to dead-code branch — backend.go:469-478
- `GetRateBasedRuleManagedKeys` always returns an empty list (stub, never tracks blocked IPs) — backend.go:1363-1372
- All `opList*` operations ignore `NextMarker` and `Limit` pagination parameters, always returning the full collection — handler.go:327-335

**Performance:**
- All `List*` backend methods iterate the full map and sort on every call with no caching or pre-sorted index.

**Leaks:**
- `changeTokens` map grows unboundedly because `GetChangeToken` adds an entry on every call with no eviction policy — backend.go:458-465

**UI:**
- `ui/src/routes/waf/` exists; sampled-requests, rate-based rule managed-key, and change-token status ops likely have no UI.

---

### wafv2

**Parity:**
- `GetSampledRequests` returns empty results (stub) — handler.go:279
- `GetRateBasedStatementManagedKeys` returns empty results (stub) — handler.go:276
- `GetTopPathStatisticsByTraffic` returns empty data (stub) — handler.go:280
- `GetMobileSdkRelease`, `ListMobileSdkReleases`, `GenerateMobileSdkReleaseUrl` return static canned catalog data from managed_rules.go rather than real SDK release information.

**Performance:**
- `buildDispatchTable` is called on every HTTP request, allocating a new map of ~50 closures per request instead of building it once at startup — handler.go:217-299

**Leaks:**
- Per-region lazy-init index maps (16 maps per region) are added on first access and never evicted, growing unboundedly as new regions are referenced.

**UI:**
- `ui/src/routes/wafv2/` exists; managed rule set, mobile SDK release, traffic statistics, and sampling ops likely have no UI.

---

### workmail

**Parity:**
- `TestAvailabilityConfiguration` performs no real connectivity test and is a no-op stub.
- `AssumeImpersonationRole` returns a token that is never validated by any subsequent impersonation call.
- Sentinel errors use raw `errors.New` instead of `awserr` wrappers, so AWS error-type inspection via `errors.Is` will not match structured AWS error types — backend.go:19-31

**Performance:**
- `ensureOrgMaps` initializes 14 separate sub-maps for every new organization on first access — backend.go:182-203

**Leaks:**
- Uses `sync.RWMutex` directly (not `lockmetrics.RWMutex`) so no lock contention metrics are recorded — backend.go:91

**UI:**
- `ui/src/routes/workmail/` exists; mobile device access rules, availability configurations, personal access tokens, and identity provider configuration ops likely have no UI.

---

### workspaces

**Parity:**
- `DescribeWorkspaceBundles` returns only hardcoded Amazon-owned bundles and likely does not filter by `BundleIds` parameter.
- `DescribeWorkspaceDirectories` returns only registered directories with no real directory validation against an actual directory service.
- Pagination `nextToken` values are likely non-opaque resource-ID-based rather than encrypted cursor values.

**Performance:**
- Multiple list operations (DescribeWorkspaces, DescribeWorkspaceBundles) build full collection copies on every call under the lock.

**Leaks:**
- No goroutine, ticker, or unbounded-map leaks detected.

**UI:**
- `ui/src/routes/workspaces/` exists; workspace pool, image, connection alias, account management, and SAML/directory ops likely have no UI.

---

### xray

**Parity:**
- `GetInsight`, `GetInsightEvents`, `GetInsightImpactGraph`, `GetInsightSummaries` return empty/canned data since no real insight detection is derived from ingested segments — backend.go:269-275
- `ListRetrievedTraces` returns trace objects with empty `Segments` arrays rather than the actual stored segment data — handler_missing_ops.go:171-178
- `GetServiceGraph` and `GetTraceGraph` return empty service sets because no real service topology is built from segment data — handler_missing_ops.go:32-55, 114-134
- Group and sampling-rule ARNs are hardcoded to `config.DefaultRegion`/`config.DefaultAccountID`, making multi-region/account scenarios incorrect — backend.go:352-357

**Performance:**
- `GetGroupByARN`, `UpdateGroupByARN`, `DeleteGroupByARN` all do O(n) linear scans of the groups map instead of maintaining an ARN-keyed index — backend.go:432-443, 478-508, 524-548

**Leaks:**
- `traceRetrievals` and `retrievedTraces` maps grow unboundedly; the janitor evicts traces/segments but never cleans up retrieval state — backend.go:280-290, janitor.go:83-119

**UI:**
- `ui/src/routes/xray/` exists; insight, trace retrieval, indexing rule, and trace-graph ops likely have no UI.

---

## UI Parity Audit

Audit date: 2026-06-21  
Backend service count: 154 (from `/home/user/gopherstack/services/`)  
UI route count: ~155 directories in `/home/user/gopherstack/ui/src/routes/` (including non-service dirs like `+layout.svelte`, `console`, `docs`, `resources`, etc.)

---

### Services with NO UI route

The following backend services have **no corresponding UI route directory** in `ui/src/routes/`. Name-aliased services (e.g. `kafka` → `msk`, `ce` → `costexplorer`, `stepfunctions` → `sfn`) are noted separately below.

#### Truly absent (no UI route of any kind)

| Backend service | Impact |
|---|---|
| `bedrockagent` | Bedrock Agents, Knowledge Bases, and Agent Aliases cannot be browsed, created, or managed. `bedrock` route (`ui/src/routes/bedrock/+page.svelte`) only covers foundation models. |
| `cleanrooms` | AWS Clean Rooms collaborations, memberships, and configured tables have zero UI representation. |
| `dynamodbstreams` | No dedicated route. The `ui/src/routes/dynamodb/+page.svelte` (line 1625) shows an inline warning: _"The DynamoDB Streams polling backend is not configured"_. Stream shard/record browsing is completely absent. |
| `networkmonitor` | AWS Network Monitor probes and monitors cannot be managed. |
| `omics` | AWS HealthOmics sequence stores, read sets, and reference stores are fully absent. |
| `opsworks` | OpsWorks stacks, layers, instances, and apps have no UI. |
| `qldb` | Amazon QLDB ledgers cannot be listed, created, or queried. |
| `qldbsession` | No session-level PartiQL execution UI for QLDB. |
| `vpclattice` | VPC Lattice services, service networks, and listeners have no UI. |

#### Aliased / name-mapped (backend service present, UI route uses a different name)

| Backend service | UI route | Notes |
|---|---|---|
| `ce` | `ui/src/routes/costexplorer/` | Correctly aliased; covered. |
| `stepfunctions` | `ui/src/routes/sfn/` | Correctly aliased; covered. |
| `kafka` | `ui/src/routes/msk/` | Correctly aliased; covered. |
| `inspector2` | `ui/src/routes/inspector/` | Route uses `@aws-sdk/client-inspector2` (line 11 of `ui/src/routes/inspector/+page.svelte`); covered. |
| `sagemakerruntime` | `ui/src/routes/sagemakeruntime/` | **Typo**: route name is missing the second `r`. The SDK client is correct (`getSageMakerRuntimeClient`). The nav entry at `nav.ts` line 497–502 uses the typo spelling. Functionally works but breaks URL predictability. |
| `timestreamwrite` | `ui/src/routes/timestream/` | Combined route covers both write (databases/tables) and query (scheduled queries). Separate `ui/src/routes/timestreamquery/` also exists. No gap in coverage. |

#### In nav but **no route directory** (broken links)

| Nav entry | Nav `id` | Missing directory |
|---|---|---|
| Keyspaces | `keyspaces` | `ui/src/routes/keyspaces/` — clicking the sidebar link 404s |
| Kinesis Video | `kinesisvideo` | `ui/src/routes/kinesisvideo/` — clicking the sidebar link 404s |
| AppFabric | `appfabric` | `ui/src/routes/appfabric/` — clicking the sidebar link 404s |

Source: `ui/src/lib/nav.ts` lines 333–375 (database-analytics category) and line 456 (app-integration category).

---

### Thin / placeholder routes

These routes exist as directories with a `+page.svelte` but implement only **list-read** operations. No create, edit, or delete functionality is wired up. All patterns are: one `onMount` → `List*Command` → display read-only cards.

| Route | File | What it lists | What's missing |
|---|---|---|---|
| `accessanalyzer` | `ui/src/routes/accessanalyzer/+page.svelte` (106 lines) | `ListAnalyzers` only | Create analyzer, archive findings, apply archive rules |
| `account` | `ui/src/routes/account/+page.svelte` (106 lines) | `ListRegions` only | Enable/disable regions (`EnableRegion`/`DisableRegion`), manage contact info |
| `detective` | `ui/src/routes/detective/+page.svelte` (98 lines) | `ListGraphs` only | Create/delete behavior graphs, invite members, manage data sources |
| `dlm` | `ui/src/routes/dlm/+page.svelte` (106 lines) | `GetLifecyclePolicies` only | Create/update/delete lifecycle policies |
| `dax` | `ui/src/routes/dax/+page.svelte` (158 lines) | `DescribeClusters`, `DescribeParameterGroups`, `DescribeSubnetGroups` | Create/delete DAX clusters, reboot nodes |
| `datasync` | `ui/src/routes/datasync/+page.svelte` (161 lines) | `ListTasks`, `ListLocations`, `ListAgents` | Create/delete tasks and locations, start/cancel task executions |
| `directoryservice` | `ui/src/routes/directoryservice/+page.svelte` (164 lines) | `DescribeDirectories`, `DescribeSnapshots`, `DescribeTrusts` | Create/delete directories, create snapshots, add IPs |
| `macie2` | `ui/src/routes/macie2/+page.svelte` (158 lines) | `ListClassificationJobs`, `ListCustomDataIdentifiers`, `ListAllowLists` | Create classification jobs, enable Macie, manage findings |
| `rolesanywhere` | `ui/src/routes/rolesanywhere/+page.svelte` (180 lines) | `ListProfiles`, `ListTrustAnchors`, `ListSubjects`, `ListCrls` | Create/update trust anchors and profiles, import CRLs |
| `medialive` | `ui/src/routes/medialive/+page.svelte` (193 lines) | `ListChannels`, `ListInputs`, `ListInputSecurityGroups`, `ListMultiplexes` | Create/start/stop channels, create inputs |
| `mediapackage` | `ui/src/routes/mediapackage/+page.svelte` (158 lines) | `ListChannels`, `ListOriginEndpoints`, `ListHarvestJobs` | Create channels and endpoints |
| `mediatailor` | `ui/src/routes/mediatailor/+page.svelte` (191 lines) | `ListChannels`, `ListSourceLocations`, `ListPlaybackConfigurations`, `ListVodSources` | Create playback configurations, CDN config |
| `forecast` | `ui/src/routes/forecast/+page.svelte` (190 lines) | `ListDatasets`, `ListDatasetGroups`, `ListPredictors`, `ListForecasts` | Create/train predictors, generate forecasts |
| `quicksight` | `ui/src/routes/quicksight/+page.svelte` (189 lines) | `ListDataSets`, `ListDashboards`, `ListAnalyses`, `ListDataSources`, `ListUsers` | Create/publish analyses and dashboards |
| `personalize` | `ui/src/routes/personalize/+page.svelte` (222 lines) | `ListDatasets`, `ListRecipes`, `ListSolutions`, `ListCampaigns` | Create datasets, train solutions, deploy campaigns |
| `databrew` | `ui/src/routes/databrew/+page.svelte` (202 lines) | `ListDatasets`, `ListJobs`, `ListProjects`, `ListRecipes`, `ListSchedules` | Create/run DataBrew jobs and projects |
| `workmail` | `ui/src/routes/workmail/+page.svelte` (208 lines) | `ListOrganizations`, `ListUsers`, `ListGroups` | Register domains, create users/groups, manage aliases |
| `appmesh` | `ui/src/routes/appmesh/+page.svelte` (221 lines) | `ListMeshes`, `ListVirtualNodes`, `ListVirtualServices`, `ListVirtualRouters`, `ListVirtualGateways` | Create mesh resources, configure routes |
| `iotanalytics` | `ui/src/routes/iotanalytics/+page.svelte` (1 line) | — **note: 1022 lines, has more** — |  |

Note: Routes with 1 file (`+page.svelte` only, no sub-routes) and ≤ 210 lines are the highest-risk thin routes. Routes with 2 files typically have a `page.test.ts` alongside a functional but single-screen page.

Also: `ui/src/routes/awsconfig/+page.svelte` (4 lines) is a pure redirect shim to `ui/src/routes/config/+page.svelte`. Similarly `ui/src/routes/cognito/+page.svelte` (2 lines) re-exports `cognitoidp/+page.svelte`.

---

### Per-route missing features

Notable feature gaps in otherwise-substantive routes:

**EC2** (`ui/src/routes/ec2/+page.svelte`, 1639 lines)
- No create/delete VPC, subnet, internet gateway, or NAT gateway (only listing: `DescribeVpcsCommand`, `DescribeSubnetsCommand` lines 17–18).
- No create/delete security group (only rule management for existing groups).
- No AMI registration or deregistration.
- No Elastic IP allocation/release.
- No placement group management.

**ECS** (`ui/src/routes/ecs/+page.svelte`, ~1600 lines)
- "Create Cluster" button at line 381 fires `toast.info('Use the AWS console or CLI to create ECS clusters.')` — it is a **stub with no API call**.
- No `RunTaskCommand` or `StopTaskCommand` — tasks cannot be started/stopped from the UI.
- No `RegisterTaskDefinitionCommand` — task definitions cannot be created from the UI.
- `CreateServiceCommand` / `DeleteServiceCommand` absent; only `UpdateServiceCommand` is wired.

**RDS** (`ui/src/routes/rds/+page.svelte`, 766 lines)
- "Create Instance" modal at line 758 shows: _"Use the AWS console or CLI to create RDS instances"_ — **no API call made**. Only `DeleteDBInstanceCommand` and `RestoreDBInstanceFromDBSnapshotCommand` are functional.
- No create DB cluster, create subnet group, or create parameter group.

**Cognito (User Pool list)** (`ui/src/routes/cognito/+page.svelte`)
- This route is a 2-line redirect to `cognitoidp`. The `cognitoidp` route has create/delete user pool and create/delete user, but the `cognito` alias in `implementedDashboardRouteIds` at `nav.ts` line 38 duplicates `cognitoidp` (line 40), creating nav confusion.

**IAM** (`ui/src/routes/iam/+page.svelte`, 1318 lines)
- No `AttachUserPolicyCommand` or `AttachGroupPolicyCommand` — managed policies cannot be attached to users or groups, only inline policies.
- No `CreateInstanceProfileCommand` or `AddRoleToInstanceProfileCommand`.
- No `UploadServerCertificateCommand`.

**MSK (Kafka)** (`ui/src/routes/msk/+page.svelte`)
- No create/delete MSK cluster.
- No broker node configuration or scaling.

**SQS** (`ui/src/routes/sqs/+page.svelte`)
- Caps `ListQueues` at `MaxResults: 100` (line 151) with no pagination UI to load more. Large deployments silently truncate.

**Cognito IdP** (`ui/src/routes/cognitoidp/+page.svelte`, 825 lines)
- No `AttachUserToGroup` / `RemoveUserFromGroup`.
- No UI to manage App Client secrets, OAuth settings, or hosted UI.

**ElastiCache** (`ui/src/routes/elasticache/+page.svelte`)
- No create serverless cache (`CreateServerlessCacheCommand`).
- No cluster snapshot scheduling.

**Backup** (`ui/src/routes/backup/+page.svelte`)
- No create/delete backup vault.
- No start on-demand backup job (`StartBackupJobCommand` absent).

**KMS** (`ui/src/routes/kms/+page.svelte`)
- No key rotation management (`EnableKeyRotationCommand` / `DisableKeyRotationCommand`).
- No multi-region key creation.

**Inspector** (`ui/src/routes/inspector/+page.svelte`)
- Uses `Inspector2` SDK but only `ListFindings`, `ListCoverage`, `ListCoverageStatistics`. No enable/disable Inspector2, no suppression rules.

**WAFv2** (`ui/src/routes/wafv2/+page.svelte`, 911 lines)
- No add/remove rules within a Web ACL — rules can only be listed.
- IP Set creation (`CreateIPSetCommand`) is absent; only listing.

**IdentityStore** (`ui/src/routes/identitystore/+page.svelte`, 1559 lines)
- Hard-codes default `IdentityStoreId = 'd-0000000000'` (line 22); there is no UI to discover or switch the real identity store ID from the account.

---

### Cross-cutting UI gaps

**1. Region change does not re-initialize most service clients**  
`ui/src/lib/aws-client.ts` (lines 90–109) instantiates each service client with `clientConfig(region)` where `region` defaults to `"us-east-1"` at module load time. The layout fires a custom event `gopherstack:region-change` (see `ui/src/routes/+layout.svelte` line ~67), but **only 2 service routes** listen for it and re-create their clients: `ui/src/routes/s3/+page.svelte` (line 1360) and `ui/src/routes/dynamodb/+page.svelte`. All other ~150 service pages use the `get*Client()` factory once at load time and ignore subsequent region changes. Switching regions in the navbar silently leaves most service pages connected to the old region.

The newer `ui/src/lib/aws/client.ts` (which calls `getStoredRegion()` and supports region-change events) is used by only 5 routes: `s3`, `dynamodb`, and two sub-routes (`s3/[bucket]/[...objectKey]`, `dynamodb/table/[tableName]`).

**2. No AWS account switching**  
The layout (`ui/src/routes/+layout.svelte`) supports region selection but there is no account-ID selector or credential profile switcher. All clients use hardcoded credentials `accessKeyId: "test"`, `secretAccessKey: "test"` (both `aws-client.ts` line 104–107 and `aws/client.ts`). For multi-account emulation scenarios, there is no UI support.

**3. Pagination absent on most listing pages**  
Of ~155 service routes, only 7 implement a next/prev pagination UI:  
`sfn`, `dynamodb`, `lambda`, `timestreamquery`, `redshiftdata`, `cloudwatchlogs`, `ram`.  
At least 49 routes use a hard-coded `MaxResults: 100` / `MaxRecords: 100` cap (verified by grep) with no "Load more" button. On resource-heavy endpoints this silently truncates results. Example: `ui/src/routes/sqs/+page.svelte` line 151: `ListQueuesCommand({ MaxResults: 100 })`.

**4. No live/auto-refresh on most service pages**  
Auto-refresh via `setInterval` is present only in `apigatewaymanagementapi`, `glacier`, `dynamodb` (streams), `cloudwatchlogs`, and `resources` (the global health panel). The `settings/+page.svelte` includes an "Auto-refresh Service Tables" toggle (lines 240–303), but **no service page reads this setting** from localStorage; the toggle has no effect. Most service pages only refresh when the user manually clicks the Refresh button.

**5. Error display is toast-only with no persistent error state**  
All ~161 service routes use `toast.error(...)` (from `svelte-sonner`) for API errors. Toasts auto-dismiss after a few seconds, leaving no visible indicator that data failed to load. If the emulator is down or an endpoint is not implemented, the page renders an empty list with no in-page error message. Only a handful of routes (e.g. `cloudwatch`, `lambda/function`, `xray`) use an inline `let error = $state<string>('')` pattern to show persistent banners.

**6. No breadcrumb / URL-deep-link for most resources**  
Only 3 service areas implement sub-routes with URL-addressable resource detail pages:
- `ui/src/routes/s3/[bucket]/` and `s3/[bucket]/[...objectKey]/`  
- `ui/src/routes/dynamodb/table/[tableName]/`  
- `ui/src/routes/lambda/function/` (single unnamed sub-route, not parametric)

All other service routes are single-file SPAs where resource selection is client-side state only. Refreshing or deep-linking to a specific resource resets to the list view.

**7. WAFv2 scope selector is local — cannot separate regional from CloudFront scopes cleanly**  
`ui/src/routes/wafv2/+page.svelte` line 48 uses a toggle `let scope = $state<'REGIONAL' | 'CLOUDFRONT'>('REGIONAL')`. This is UI-local state and not reflected in the URL, so scope selection is lost on page refresh. The `waf` (v1 classic) route has identical limitations.

**8. Nav lists 3 services with no corresponding route directories**  
`keyspaces`, `kinesisvideo`, and `appfabric` appear in the sidebar (`nav.ts`) and `sidebarCategories` but have no `ui/src/routes/<name>/` directory. Clicking these links results in a SvelteKit 404. They are **not** in `implementedDashboardRouteIds` (so the sidebar only shows them in the "all services" drawer, not the mini sidebar), but the links are still live and broken.

**9. No multi-select / bulk actions on any resource table**  
No service route implements checkbox selection for bulk delete, bulk tag, or bulk status change. Each destructive action operates on a single resource at a time.

**10. IdentityStore hard-coded store ID**  
`ui/src/routes/identitystore/+page.svelte` line 22 defaults to `d-0000000000` as the Identity Store ID. Users must manually type their actual store ID into an input field on every visit. The Identity Store ID is not discovered from `ListInstances` (SSO Admin) or persisted to localStorage.


---

## Region isolation audit

**Requirement:** AWS resources are region-scoped — a resource created in one region must
not be visible/listable/gettable/deletable from another, and every operation must derive
its region from the request context (`awsmeta.Region(ctx)` / the ctxbag), never a hardcoded
literal or a single construction-time `b.region`/`b.defaultRegion`. (Genuinely global
services — IAM, Organizations, Route53, CloudFront, Shield — correctly share one scope.)

**Classification:** `ISOLATED` (correct per-region partitioning) · `PARTIAL` (region stored
but not filtered on read, or ARNs/URLs use a fixed region) · `LEAKS` (flat store keyed by
name/ARN/id — resources are visible across all regions) · `GLOBAL` (correctly global).

**Systemic finding:** the dominant pattern across the codebase is a **flat in-memory map
keyed by name or ARN with no region dimension**, plus ARNs/URLs built from a
construction-time default region. The fix pattern: (1) key state by `region` (e.g.
`map[region]map[id]*T` or embed `Region` in the struct and filter on every read/list/delete),
(2) populate it from `awsmeta.Region(ctx)` in Create/Put, (3) filter List/Describe/Get/Delete
by the request region, (4) build ARNs/URLs from the request region, and (5) preserve the
region dimension through persistence snapshots and janitor sweeps.

> Coverage note: this section is being filled in per service group. Groups landed so far:
> DynamoDB/Streams/DAX, and groups 2, 3, 6, 8, 9, 11. Remaining groups are queued and will
> be appended in a follow-up commit (account session-usage limit hit mid-run).

# Region Isolation Audit — Group 01

Services: dynamodb, dynamodbstreams, dax

---

### dynamodb — PARTIAL

**1. State partitioned by region? YES (correctly)**
- `InMemoryDB.Tables` is `map[string]map[string]*Table` (outer key = region).
  File: `services/dynamodb/store.go:126`
- `Backups` is a flat `map[string]*Backup` keyed by backup ARN (no region dimension).
  File: `services/dynamodb/store.go:129`
- `GlobalTables` is a flat `map[string]*StoredGlobalTable` (global by design, acceptable).
  File: `services/dynamodb/store.go:130`
- `exports` and `imports` are flat maps keyed by ARN — no region partitioning.
  File: `services/dynamodb/store.go:131-132`
- `streamARNIndex` is a flat `map[string]*Table` — region isolation happens by ARN string content (parsed on ListStreams), not by map structure.
  File: `services/dynamodb/store.go:134`

**2. Create ops store request region? YES for tables; PARTIAL/NO for backups/stream ARNs**
- `CreateTable` derives region via `getRegionFromContext(ctx, db)` at line 121, stores it in `db.Tables[region][tableName]` and in the ARN (`arn.Build("dynamodb", region, ...)` at line 134).
  File: `services/dynamodb/table_ops.go:121,134`
- `CreateBackup` calls `getRegionFromContext(ctx, db)` at line 40 and embeds it in the backup ARN (`backupARN(region, ...)` line 82). Correct.
  File: `services/dynamodb/backup_interface.go:40,82`
- `buildStreamARN` (called from `CreateTable` and `applyStreamSpec`) uses `db.defaultRegion` instead of the request region:
  ```go
  func (db *InMemoryDB) buildStreamARN(tableName string) string {
      return db.buildStreamARNInRegion(tableName, db.defaultRegion)  // BUG
  }
  ```
  File: `services/dynamodb/streams_ops.go:666-668`
  Called from: `services/dynamodb/table_ops.go:139` (CreateTable) and `services/dynamodb/table_ops.go:1223` (applyStreamSpec/UpdateTable).
  `EnableStream` correctly calls `buildStreamARNInRegion(tableName, region)` at line 95, but CreateTable and UpdateTable do not.
  File: `services/dynamodb/streams_ops.go:95`
- `CreateGlobalTable` builds the global-table ARN with `db.defaultRegion` instead of any request-derived region:
  ```go
  globalTableARN := arn.Build("dynamodb", db.defaultRegion, db.accountID, "global-table/"+name)
  ```
  File: `services/dynamodb/extra_ops.go:78`

**3. List/Describe/Get/Delete filter by request region? MOSTLY YES; exceptions below**
- `ListTables`, `DescribeTable`, `DeleteTable`, `GetItem`, `PutItem`, `Scan`, `Query`, `UpdateItem` all call `getRegionFromContext` and scope to `db.Tables[region]`.
  File: `services/dynamodb/table_ops.go:121,372,394,543`; `services/dynamodb/item_ops.go:113-125`
- `getTable` → `getTableRLock` calls `getRegionFromContext` to scope the lookup correctly.
  File: `services/dynamodb/item_ops.go:113-125`
- `ListBackups` (`collectBackupSummaries`) iterates ALL backups in `db.Backups` with NO region filter — backups from any region are returned to any region's caller.
  File: `services/dynamodb/backup_ops.go:118,152-177`
  The `listBackups` handler receives `_ context.Context` (discards ctx), and `collectBackupSummaries` has no region argument.
  File: `services/dynamodb/backup_ops.go:118`
- `DescribeBackup` / `DeleteBackup` look up by ARN only (no region check), so a backup ARN from region A is accessible from region B.
  File: `services/dynamodb/backup_interface.go:120-180`
- `listExportsWire` / `listImports` / `describeExport` / `describeImport` have no region filter — exports and imports from any region leak across regions.
  File: `services/dynamodb/store.go:846-871,899-915`; `services/dynamodb/handler.go:1274-1292`
- `ListContributorInsights` iterates `db.Tables` across ALL regions without filtering by request region.
  File: `services/dynamodb/extra_ops.go:944-973`
- `getTableByARN` (used by DeleteResourcePolicy, GetResourcePolicy, PutResourcePolicy) searches all regions:
  ```go
  for _, regionTables := range db.Tables { ... }
  ```
  This means resource policies can be fetched/deleted from any region.
  File: `services/dynamodb/extra_ops.go:900-913`
- `RestoreTableFromBackup` and `RestoreTableToPointInTime` correctly use `h.regionFromHandlerContext(ctx)` to place restored tables in the request region.
  File: `services/dynamodb/backup_ops.go:241,389`
- `GetRecords` resolves iterator by table name and calls `getTable(ctx, tableName)` which is region-scoped. However it passes `db.defaultRegion` (not request region) into stream record metadata.
  File: `services/dynamodb/streams_ops.go:472` — `collectStreamRecords(..., db.defaultRegion)` — records carry wrong `AwsRegion` when serving a non-default region.

**4. ARNs built with request region? PARTIAL**
- Table ARNs in `CreateTable`: correct — uses `getRegionFromContext(ctx, db)`.
  File: `services/dynamodb/table_ops.go:134`
- Backup ARNs: correct — uses request region.
  File: `services/dynamodb/backup_interface.go:82`
- Stream ARNs in `CreateTable` and `UpdateTable` (`applyStreamSpec`): WRONG — uses `db.defaultRegion`.
  File: `services/dynamodb/streams_ops.go:666-668`, called at `table_ops.go:139,1223`
- Global table ARN in `CreateGlobalTable`: WRONG — uses `db.defaultRegion`.
  File: `services/dynamodb/extra_ops.go:78`
- Restored table ARNs (`RestoreTableFromBackup`, `RestoreTableToPointInTime`): correct — uses `regionFromHandlerContext(ctx)`.
  File: `services/dynamodb/backup_ops.go:274,414`

**5. Persistence/janitor preserve region partitioning? YES**
- `dbSnapshot.Tables` serializes the full `map[string]map[string]*Table` including the region key, so region partitioning is preserved across restarts.
  File: `services/dynamodb/persistence.go:12-17,28-48`
- Restore correctly reassembles the nested map.
  File: `services/dynamodb/persistence.go:52-103`
- Janitor (`purgeActiveTables`) iterates `db.Tables` nested by region — correct, no cross-region contamination.
  File: `services/dynamodb/store.go:657-675`

**Summary of dynamodb bugs:**
- Stream ARNs on `CreateTable`/`UpdateTable` always embed `db.defaultRegion` regardless of request region.
- `GetRecords` emits stream records with `AwsRegion = db.defaultRegion` (wrong for non-default-region tables).
- `ListBackups`, `DescribeBackup`, `DeleteBackup` are not region-scoped — cross-region backup visibility/deletion.
- `ListExports`, `DescribeExport`, `ListImports`, `DescribeImport` are not region-scoped.
- `ListContributorInsights` returns all tables across all regions.
- `getTableByARN` (resource policy ops) is not region-scoped.
- `CreateGlobalTable` ARN uses `db.defaultRegion`.

---

### dynamodbstreams — PARTIAL

**1. State partitioned by region?**
- DynamoDB Streams has no independent store; it delegates all operations to `ddbbackend.StreamsBackend` which is the shared `InMemoryDB` backend from the `dynamodb` service. Region partitioning inherits from that backend's `db.Tables` structure.
  File: `services/dynamodbstreams/handler.go:32-33`; `services/dynamodbstreams/provider.go:17-19`

**2. Create ops store request region?**
- `EnableStream` (called internally) does correctly call `buildStreamARNInRegion(tableName, region)` where region comes from `getRegionFromContext(ctx, db)`.
  File: `services/dynamodb/streams_ops.go:90,95`
- `CreateTable` / `UpdateTable` (which is how streams are typically enabled via the DynamoDB API) call `buildStreamARN(tableName)` which uses `db.defaultRegion`. See dynamodb section.

**3. List/Describe/Get filter by request region?**
- `ListStreams`: correctly filters by region using `streamARNRegion(e.arn)` compared to `getRegionFromContext(ctx, db)`.
  File: `services/dynamodb/streams_ops.go:541-593`; specifically `services/dynamodb/streams_ops.go:646`
- `DescribeStream`, `GetShardIterator`, `GetRecords`: look up by stream ARN via `db.streamARNIndex` — not region-filtered, but an attacker would need the exact ARN; the ARN itself encodes the region so cross-region ARN leakage is prevented if stream ARNs are correct. However, since `CreateTable` mints ARNs with `db.defaultRegion`, a table in region B may get an ARN with region A embedded, causing confusion.

**4. ARNs built with request region?**
- `buildStreamARNInRegion` (used by `EnableStream` directly) is correct.
  File: `services/dynamodb/streams_ops.go:670-673`
- `buildStreamARN` (used by `CreateTable`, `UpdateTable`) is WRONG — uses `db.defaultRegion`.
  File: `services/dynamodb/streams_ops.go:666-668`

**5. Region injected into HTTP handler?**
- The DynamoDB Streams HTTP handler (`services/dynamodbstreams/handler.go`) injects region via `ddbbackend.WithRegion(ctx, region)` at line 118, so `ListStreams` (which calls `getRegionFromContext`) correctly receives the request region.
  File: `services/dynamodbstreams/handler.go:117-118`
- `GetRecords` passes `db.defaultRegion` into record metadata (AwsRegion field), not the request-derived region.
  File: `services/dynamodb/streams_ops.go:472`

**Summary of dynamodbstreams bugs:**
- Inherits the stream ARN minting bug from dynamodb (`CreateTable`/`UpdateTable` use `db.defaultRegion`).
- `GetRecords` emits records with wrong `AwsRegion` for non-default regions.
- All other isolation logic (ListStreams region filter, DescribeStream/GetShardIterator ARN-scoped lookups) is correct provided ARNs were minted correctly.

---

### dax — LEAKS

**1. State partitioned by region? NO — FLAT maps**
- `InMemoryBackend` has three flat maps:
  ```go
  clusters     map[string]*Cluster        // keyed by cluster name only
  paramGroups  map[string]*ParameterGroup // keyed by param group name only
  subnetGroups map[string]*SubnetGroup    // keyed by subnet group name only
  tags         map[string]map[string]string // keyed by ARN
  ```
  File: `services/dax/backend.go:121-130`
- No `map[region]map[name]` structure exists. All clusters from all regions (if the backend were ever shared, which it is not currently — one backend per process) would be in the same flat map.

**2. Create ops store request region? NO — uses construction-time `b.Region`**
- `CreateCluster` never reads `ctx` for region; it uses `b.Region` (set at construction time from `config.GetRegion()`).
  File: `services/dax/backend.go:371-461`
- `clusterARN` uses `b.Region`:
  ```go
  func (b *InMemoryBackend) clusterARN(name string) string {
      return fmt.Sprintf("arn:aws:dax:%s:%s:cache/%s", b.Region, b.AccountID, name)
  }
  ```
  File: `services/dax/backend.go:170-172`
- No `ctx` parameter is present on any backend method (`CreateCluster`, `DescribeClusters`, `DeleteCluster`, etc.) — the region cannot be derived from the request.
  File: `services/dax/backend.go:371,533,621` (signatures have no `ctx`)

**3. List/Describe/Get/Delete filter by request region? NO**
- `DescribeClusters` accepts no context, reads no region from request, returns ALL clusters in the flat map.
  File: `services/dax/backend.go:533-557`
- `DeleteCluster`, `UpdateCluster`, `RebootNode`, `TagResource`, `UntagResource`, `ListTags` all look up by cluster name only — no region check.
  File: `services/dax/backend.go:560-967`
- HTTP handler's `dispatch` discards context: `_ = ctx`.
  File: `services/dax/handler.go:148`
- `DescribeParameterGroups`, `DescribeSubnetGroups`, `DescribeEvents` return all items with no region filter.
  File: `services/dax/backend.go:1004-1065,1314-1373,1451-1501`

**4. ARNs built with request region? NO — uses `b.Region` (construction-time default)**
- All ARN construction uses `b.Region`:
  File: `services/dax/backend.go:170-172` (cluster), lines 1536,1543,1550,1563 (arnExists, clusterByARN)
- `arnExists` and `clusterByARN` embed `b.Region` in prefix strings for lookup — if a client in a different region sends an ARN with its region embedded, the prefix won't match and the lookup will fail silently.
  File: `services/dax/backend.go:1535-1568`

**5. Persistence preserves region? PRESERVES b.Region value; does not add region partitioning**
- `backendSnapshot` serializes `b.Region` as a single scalar — the flat map structure is preserved (not partitioned).
  File: `services/dax/persistence.go:14-22,94-118`
- Restore sets `b.Region = snap.Region` — still a single region per backend instance.
  File: `services/dax/persistence.go:144`

**Summary of dax bugs:**
- No per-request region awareness: `ctx` is discarded in `dispatch`, no `awsmeta.Region(ctx)` calls anywhere in the DAX package.
- Flat maps with no region dimension — all clusters/parameter-groups/subnet-groups visible regardless of which region the request comes from.
- ARNs always built with `b.Region` (process-level default), never the request region.
- `DescribeClusters` (and all other list ops) return all resources with no region filter.
- `arnExists`/`clusterByARN` use hardcoded `b.Region` prefix — cross-region ARN lookups will silently fail to find resources.

### accessanalyzer — LEAKS

- **State map is FLAT (no region partition):** `InMemoryBackend` declares `analyzers map[string]*Analyzer`, `archiveRules map[string]map[string]*ArchiveRule`, `findings map[string]map[string]*Finding` — all single-level maps with no outer region key. (`backend.go:104–115`)
- **Backend is instantiated with a fixed `b.region` from construction time** (provider passes `config.DefaultRegion` at init; no per-request region is ever consulted). (`provider.go:28–36`)
- **Create stores using `b.region`:** `analyzerARN` builds `arn:aws:access-analyzer:%s:%s:analyzer/%s` from `b.region` (a fixed construction-time constant, not the request region). (`backend.go:135–136`, `backend.go:158`)
- **List/Get/Delete do NOT filter by region:** `ListAnalyzers`, `GetAnalyzer`, `DeleteAnalyzer` operate on the same global `b.analyzers` map — all analyzers are visible regardless of caller's region. (`backend.go:187–221`)
- **Handler never extracts or passes a region from the request context** — no `awsmeta.Region(ctx)` call anywhere in `handler.go`.
- **Persistence snapshot contains no region key** — `Snapshot`/`Restore` serialize the flat maps without region envelope, so restore cannot distinguish regions. (`backend.go:542–602`)

---

### account — GLOBAL

- **Service is intentionally global (account-level API):** AWS Account Management API is not region-scoped; `ListRegions`, `GetAlternateContact`, `PutAlternateContact`, `GetContactInformation`, `DescribeAccount` all operate on account-level state. This matches real AWS behaviour.
- **Backend has a `region` field but never uses it for data partitioning** — it is only held at construction time and never consulted in any operation. (`backend.go:94–101`)
- **No ARNs are built for user resources** (the `DescribeAccount` ARN uses a static org format, not region-scoped). (`backend.go:143–151`)
- Classified GLOBAL intentionally — AWS Account service has no region-scoped data.

---

### acm — ISOLATED

- **State maps are region-nested:** `certs map[string]map[string]*Certificate`, `timers`, `idempotencyMap`, `accountIdempotency`, `accountConfig` all use `map[region]map[...]*T` as outer key. (`backend.go:186–201`)
- **`getRegion` reads from request context via `ctx.Value(regionContextKey{})`, falling back to `b.region`** — per-request region is used, not construction-time default alone. (`backend.go:29–36`)
- **Create uses request region:** `RequestCertificate` calls `getRegion(ctx, b.region)` then `arn.Build("acm", region, …)`. (`backend.go:288`, `backend.go:304`)
- **List/Describe/Delete filter by request region:** `ListCertificates`, `DescribeCertificate`, `DeleteCertificate`, `ExportCertificate`, `GetCertificate` all call `getRegion(ctx, b.region)` then access `b.certsStore(region)`. (`backend.go:949–955`, `backend.go:851–857`, `backend.go:1060–1065`)
- **ARNs built with request region:** `arn.Build("acm", region, b.accountID, "certificate/"+id)` where `region` comes from the request context. (`backend.go:304`, `backend.go:668`)
- **Persistence preserves region:** `backendSnapshot` serialises `b.certs` (outer key = region) and restores it intact; `Region` field also persisted. (`persistence.go:11–17`, `persistence.go:76–80`)

---

### acmpca — ISOLATED

- **State maps are region-nested:** `cas map[string]map[string]*CertificateAuthority`, `certs`, `certsByCASerial`, `permissions`, `auditReports`, `policies` — all `map[region]map[...]T`. (`backend.go:165–178`)
- **`getRegion` reads from request context** via `ctx.Value(regionContextKey{})`, falling back to `b.region`. (`backend.go:29–36`)
- **Create uses request region:** `CreateCertificateAuthority` calls `getRegion(ctx, b.region)` then `arn.Build("acm-pca", region, …)`. (`backend.go:263`, `backend.go:273`)
- **List/Describe/Delete filter by request region:** `DescribeCertificateAuthority`, `ListCertificateAuthorities`, `DeleteCertificateAuthority`, `IssueCertificate`, `GetCertificate`, `RevokeCertificate`, `ListCertificates`, `CreatePermission`, `DeletePermission`, `ListPermissions`, `PutPolicy`, `GetPolicy`, `DeletePolicy` all call `getRegion(ctx, b.region)` and use `b.casStore(region)` / `b.certsStore(region)`. (`backend.go:366`, `backend.go:385`, `backend.go:415`, `backend.go:563`, `backend.go:617`, `backend.go:649`, `backend.go:685`, `backend.go:817`, `backend.go:852`, `backend.go:879`, `backend.go:917`, `backend.go:937`, `backend.go:960`)
- **ARNs built with request region:** `arn.Build("acm-pca", region, b.accountID, caResourceIDPrefix+id)` and `arn.Build("acm-pca", region, b.accountID, …)` for certs. (`backend.go:273`, `backend.go:591`)
- **Persistence preserves region:** `backendSnapshot` comment says "region-nested backend maps (outer key = region)"; `Region` field serialised. (`persistence.go:55–165`)

---

### amplify — LEAKS

- **State map is FLAT (no region partition):** `apps map[string]*App`, `branches map[string]map[string]*Branch`, `jobs`, `domains`, `webhooks`, `backendEnvironments`, `artifacts` — all flat with no region outer key. (`backend.go:121–133`)
- **Backend is instantiated with a fixed `b.region`** from construction time via `provider.go`. (`provider.go:18–29`)
- **Create uses `b.region` (construction-time constant):** `CreateApp` uses `arn.Build("amplify", b.region, …)`. (`backend.go:161`) `CreateBranch` uses `b.region`. (`backend.go:263–268`) `CreateWebhook` uses `b.region`. (`backend.go:930–935`) `CreateBackendEnvironment` uses `b.region`. (`backend.go:1080–1082`)
- **List/Get/Delete do NOT filter by region:** `ListApps`, `GetApp`, `DeleteApp`, etc. access the same global `b.apps` map. No per-request region extraction occurs anywhere.
- **Handler never extracts region from request context** — `handler.go` only stores `DefaultRegion` at init time.
- **No persistence file** — no `Snapshot`/`Restore` methods (no persistence.go in amplify package).

---

### apigateway — LEAKS

- **State map is FLAT (no region partition):** `apis map[string]*apiData`, `apiKeys`, `basePathMappings`, `domainNames`, `usagePlans`, etc. — all flat maps with no region outer key. (`backend.go:344–358`)
- **`NewInMemoryBackend()` takes NO accountID or region parameters** — provider passes no region at init. (`backend.go:361–383`, `provider.go:16–21`)
- **Create does NOT embed region in any stored resource** — `CreateRestAPI` stores no region; `CreateDomainName` hardcodes `us-east-1` in `regionalDomain`. (`backend.go:1631`: `regionalDomain := input.DomainName + ".execute-api.us-east-1.amazonaws.com"`)
- **List/Get/Delete do NOT filter by region** — `GetRestAPIs`, `GetRestAPI`, `DeleteRestAPI` operate on the global `b.apis` map.
- **`dispatch` discards context:** `func (h *Handler) dispatch(_ context.Context, …)` — context (which would carry the region) is explicitly dropped. (`handler.go:2728`)
- **Persistence snapshot is flat** — `backendSnapshot` serialises all maps without region envelope. (`persistence.go:19–31`)

---

### apigatewaymanagementapi — GLOBAL

- **Service is connection-scoped (WebSocket management), not region-scoped in AWS:** this is an in-process signalling bus for WebSocket connections, not a multi-region data plane.
- **State map is flat:** `connections map[string]*connState` — single flat map. (`backend.go:30–34`)
- **`NewInMemoryBackend()` takes NO region or accountID** — provider passes no region. (`backend.go:37–42`, `provider.go:16–21`)
- **No ARNs are built for resources** — connection IDs are not ARNs.
- **No region filtering exists** in any CRUD operation.
- **Persistence is flat** — `backendSnapshot` has `Connections map[string]persistedConn` with no region envelope. (`persistence.go:11–14`)
- Classified GLOBAL — AWS Management API is connection-scoped, not region-partitioned at the data level.

---

### apigatewayv2 — PARTIAL

- **State map is FLAT (no region partition):** `apis map[string]*apiData`, `domainNames`, `apiMappings`, `portals`, `portalProducts`, `vpcLinks`, `routingRules` — all flat maps with no region outer key. (`backend.go:389–403`)
- **`NewInMemoryBackend()` takes NO region or accountID** — provider passes no region. (`backend.go:404–418`, `provider.go:16–21`)
- **`regionFromCtx` correctly reads `awsmeta.Region(ctx)` for ARN/endpoint construction** — uses `awsmeta.Region(ctx)` with fallback to const `defaultRegion = "us-east-1"`. (`backend.go:25–31`)
- **Create embeds request region in ARN/endpoint:** `CreateAPI` sets `APIEndpoint: "https://"+id+".execute-api."+regionFromCtx(ctx)+".amazonaws.com"`. (`backend.go:497`) `CreateDomainName` uses `regionFromCtx(ctx)` for `APIGatewayDomainName`. (`backend.go:1501`) `CreateRoutingRule` uses `regionFromCtx(ctx)` for `RoutingRuleARN`. (`backend.go:1931`)
- **BUT List/Get/Delete do NOT filter by region** — `GetAPIs`, `GetAPI`, `GetStages`, etc. return data from the global flat `b.apis` map regardless of request region — cross-region data leaks.
- **Persistence snapshot is flat** — `backendSnapshot` serialises all maps without region envelope. (`persistence.go:28–36`)
- PARTIAL: ARN construction uses request region correctly, but the backing store is a single flat map shared across all regions.

# Region Isolation Audit — Group 03

Services: appconfig, appconfigdata, applicationautoscaling, appmesh, apprunner, appstream, appsync, athena

---

### appconfig — LEAKS

- **State partitioning**: FLAT maps — `applications map[string]*Application`, `deploymentStrategies map[string]*DeploymentStrategy`, `extensions map[string]*Extension`, etc. at `services/appconfig/backend.go:41-65`. No region dimension in any key. One backend instance holds all state.
- **Construction-time region only**: `b.region` set at construction in `NewInMemoryBackend(accountID, region string)` at `services/appconfig/backend.go:68`; provider reads it once from config at `services/appconfig/provider.go:17-33`. No `awsmeta.Region(ctx)` call anywhere in the package.
- **Create ops**: `CreateApplication` (`backend.go:103`) takes no region param; ARN built via `b.appconfigARN` at `backend.go:98-100` using the fixed `b.region`. `CreateExtension` builds ARN at `backend.go:1139` with `b.appconfigARN("extension/"+id)` — same fixed region.
- **List/Get/Delete**: `ListApplications` (`backend.go:146`) returns all applications with no region filter. `ListDeploymentStrategies`, `ListExtensions`, `ListExtensionAssociations` likewise return all resources regardless of request region.
- **ARNs**: `appconfigARN` at `backend.go:98-100` uses `b.region` (construction-time), never the request context.
- **Persistence**: No persistence file found for appconfig.

---

### appconfigdata — LEAKS

- **State partitioning**: FLAT maps — `profiles map[string]*ConfigurationProfile`, `sessions map[string]*Session`, `graceTokens map[string]*graceEntry` at `services/appconfigdata/backend.go:34-45`. No region dimension.
- **Construction-time region**: `NewInMemoryBackend()` at `backend.go:48` takes no region at all — the backend has no `region` field. Provider at `services/appconfigdata/provider.go:16` calls `NewInMemoryBackend()` with no args.
- **Create ops**: `SetConfiguration` (`backend.go:152`) stores by profile key `app|env|profile` with no region. `StartSession` (`backend.go:224`) creates sessions with no region tracking.
- **List/Get**: `ListProfiles` (`backend.go:388`) returns all profiles; `ListSessions` (`backend.go:417`) returns all sessions — no region filtering.
- **ARNs**: No ARNs built in this service.
- **Handler routing**: `ChaosRegions` at `services/appconfigdata/handler.go:74` returns `[]string{config.DefaultRegion}` — hardcoded single region.

---

### applicationautoscaling — LEAKS

- **State partitioning**: FLAT maps — `scalableTargets map[string]*ScalableTarget`, `scalingPolicies map[string]*ScalingPolicy`, `scheduledActions map[string]*ScheduledAction`, `scalingActivities []*ScalingActivity` at `services/applicationautoscaling/backend.go:109-120`. No region dimension in keys.
- **Construction-time region only**: `NewInMemoryBackend(accountID, region string)` at `backend.go:138`; provider reads from config once at `services/applicationautoscaling/provider.go:17-31`. No `awsmeta.Region(ctx)` anywhere in the package.
- **Create ops**: `RegisterScalableTarget` at `backend.go:171` builds ARN using `b.region` at `backend.go:223-227`. `PutScalingPolicy` builds ARN using `b.region` at `backend.go:595-597`. `PutScheduledAction` builds ARN using `b.region` at `backend.go:792-794`. All use the fixed construction-time region.
- **List/Describe**: `DescribeScalableTargets` (`backend.go:493`) iterates all `b.scalableTargets` — no region filter. `DescribeScalingPolicies` (`backend.go:716`) and `DescribeScheduledActions` (`backend.go:870`) similarly return resources across all regions.
- **ARNs**: Built with `b.region` (construction-time) at `backend.go:223-227`, `backend.go:595-597`, `backend.go:792-794`.
- **Persistence**: `Snapshot`/`Restore` at `services/applicationautoscaling/persistence.go:17-60` serializes/restores region as part of `backendSnapshot`. Region is preserved in persistence but still tied to the single construction-time value.

---

### appmesh — LEAKS

- **State partitioning**: FLAT maps — `meshes map[string]*Mesh`, `virtualNodes map[string]map[string]*VirtualNode`, `virtualRouters map[string]map[string]*VirtualRouter`, `routes map[routeKey]*Route`, `virtualSvcs map[string]map[string]*VirtualService`, `virtualGWs map[string]map[string]*VirtualGateway`, `gatewayRoutes map[gatewayRouteKey]*GatewayRoute` at `services/appmesh/backend.go:70-82`. No region dimension in any map key.
- **Construction-time region only**: `NewInMemoryBackend(accountID, region string)` at `backend.go:85`; provider reads from config once at `services/appmesh/provider.go:22-39`. No `awsmeta.Region(ctx)` call anywhere in the package.
- **Create ops**: `CreateMesh` (`backend.go:181`) builds ARN via `b.meshARN(name)` at `backend.go:116-118` using `b.region`. All other resource ARN builders (`virtualNodeARN`, `virtualRouterARN`, `routeARN`, `virtualServiceARN`, `virtualGatewayARN`, `gatewayRouteARN`) at `backend.go:120-148` use `b.region`.
- **List ops**: `ListMeshes` (`backend.go:244`) returns all meshes; `ListVirtualNodes` (`backend.go:347`) scoped to meshName only, no region. Same pattern for all other List* methods.
- **ARNs**: All ARN helpers at `backend.go:116-148` use `b.region` (fixed construction-time).
- **Persistence**: No persistence file found for appmesh.

---

### apprunner — LEAKS

- **State partitioning**: FLAT maps — `services map[string]*storedService`, `autoScalingConfigs map[string]*storedAutoScalingConfiguration`, `connections map[string]*storedConnection`, `observabilityConfigs map[string]*storedObservabilityConfiguration`, `vpcConnectors map[string]*storedVpcConnector`, `vpcIngressConnections map[string]*storedVpcIngressConnection` at `services/apprunner/backend.go:350-367`. No region dimension in any key.
- **Construction-time region only**: `NewInMemoryBackend(accountID, region string)` at `backend.go:370`; provider reads from config once at `services/apprunner/provider.go:22-39`. No `awsmeta.Region(ctx)` anywhere in the package.
- **Create ops**: `CreateService` at `backend.go:459` builds ARN via `b.serviceARN(id)` at `backend.go:391-393` using `b.region`. Service URL built with `buildServiceURL(id, b.region)` at `backend.go:489` using `b.region`. `CreateAutoScalingConfiguration` builds ARN at `backend.go:931` via `b.asgARN` using `b.region`. `CreateConnection` at `backend.go:1122` via `b.connectionARN` using `b.region`. All use construction-time region.
- **List ops**: `ListServices` (`backend.go:588`) iterates all `b.services` with no region filter. Similarly for `ListAutoScalingConfigurations`, `ListConnections`, `ListVpcConnectors`, `ListVpcIngressConnections`.
- **ARNs**: All ARN builders at `backend.go:391-422` use `b.region` (fixed construction-time).
- **Persistence**: `Snapshot`/`Restore` at `backend.go:809-915` serializes/restores all resources with no region partitioning.

---

### appstream — LEAKS

- **State partitioning**: FLAT maps — `stacks map[string]*storedStack`, `fleets map[string]*storedFleet`, `applications map[string]*storedApplication`, `appBlocks map[string]*storedAppBlock`, etc. at `services/appstream/backend.go:124-152`. No region dimension in any key.
- **Construction-time region only**: `NewInMemoryBackend(accountID, region string)` at `backend.go:155`; provider reads from config once at `services/appstream/provider.go:22-39`. No `awsmeta.Region(ctx)` anywhere in the package.
- **Create ops**: `CreateStack` at `backend.go:199` builds ARN via `b.stackARN(name)` at `backend.go:190-192` using `b.region`. `fleetARN` at `backend.go:194-196` uses `b.region`. All ARNs use construction-time region.
- **List/Describe**: `DescribeStacks` (`backend.go:226`) returns all stacks with no region filter.
- **ARNs**: `stackARN` at `backend.go:190-192` and `fleetARN` at `backend.go:194-196` use `b.region` (fixed).
- **Persistence**: `backendSnapshot` struct at `backend.go:96-121` serializes all resources without region partitioning.

---

### appsync — LEAKS

- **State partitioning**: FLAT maps — `apis map[string]*GraphqlAPI`, `schemas map[string]*Schema`, `datasources map[string]map[string]*DataSource`, `resolvers map[string]map[string]*Resolver`, `domainNames map[string]*DomainName`, `eventAPIs map[string]*API` etc. at `services/appsync/backend.go:312-331`. No region dimension in any key.
- **Construction-time region only**: `NewInMemoryBackend(accountID, region, endpoint string)` at `backend.go:335`; provider reads from config once and sets `handler.DefaultRegion = region` at `services/appsync/provider.go:17-39`. No `awsmeta.Region(ctx)` call anywhere in the package.
- **Create ops**: `CreateGraphqlAPI` uses `b.region` and `b.accountID` (construction-time) for ARN building throughout the backend (not visible in the truncated view, but consistent with field usage). `handler.DefaultRegion` at `services/appsync/handler.go:76` is a single fixed string.
- **List ops**: `ListGraphqlAPIs`, `ListAPIs`, `ListDomainNames` — all iterate their respective maps with no region filtering.
- **ARNs**: Built with `b.region` (construction-time) via `arn.Build("appsync", b.region, b.accountID, ...)`.
- **Persistence**: No persistence file found for appsync.

---

### athena — LEAKS

- **State partitioning**: FLAT maps — `workGroups map[string]*WorkGroup`, `namedQueries map[string]*NamedQuery`, `dataCatalogs map[string]*DataCatalog`, `queryExecutions map[string]*QueryExecution`, `capacityReservations map[string]*CapacityReservation`, `notebooks map[string]*Notebook`, `sessions map[string]*Session` at `services/athena/backend.go:388-409`. No region dimension in any key.
- **Construction-time region only**: `NewInMemoryBackend(region, accountID string)` at `backend.go:412`; provider reads from config once at `services/athena/provider.go:23-43`. No `awsmeta.Region(ctx)` anywhere in the package.
- **Create ops**: `workGroupARN` at `backend.go:497-499` uses `b.region` (construction-time). All resource operations use `b.region` for ARN generation. `handler.ChaosRegions()` returns `[]string{config.DefaultRegion}` at `services/athena/handler.go:118` — hardcoded.
- **List ops**: `ListWorkGroups`, `ListNamedQueries`, `ListDataCatalogs`, `ListQueryExecutions` — all iterate flat maps with no region filter.
- **ARNs**: `workGroupARN` at `backend.go:497-499` uses `b.region` (fixed construction-time).
- **Persistence**: No persistence file found for athena (janitor exists at `janitor.go` but manages TTL, not persistence).

---

## Summary

| Service | Classification |
|---------|----------------|
| appconfig | LEAKS |
| appconfigdata | LEAKS |
| applicationautoscaling | LEAKS |
| appmesh | LEAKS |
| apprunner | LEAKS |
| appstream | LEAKS |
| appsync | LEAKS |
| athena | LEAKS |

**Root cause common to all 8 services**: Each service instantiates a single `InMemoryBackend` with one construction-time `region` (from `config.DefaultRegion` or the global config). All resource maps are flat with no region dimension in keys. No service uses `awsmeta.Region(ctx)` to derive region per-request. Consequently, all resources are visible regardless of which region the caller addresses, and ARNs embed only the single construction-time region.

### codebuild — LEAKS

- State is a FLAT map (no region dimension): `projects map[string]*Project`, `builds map[string]*Build`, `fleets map[string]*Fleet`, etc. declared at `backend.go:367-388`.
- `NewInMemoryBackend(accountID, region)` stores region at construction time as `b.region` (`backend.go:393-419`); all ops operate on the single shared flat maps regardless of which region the request came from.
- `CreateProject` stores projects into `b.projects` flat map without any region key (`backend.go:502-560`); `ListProjects` returns all names from the same flat map (`backend.go:722-734`).
- ARNs built with `b.region` (construction-time fixed value), not the request region: `buildProjectARN` uses `b.region` (`backend.go:451-453`); same for `buildBuildARN` (`backend.go:455-457`), `buildFleetARN` (`backend.go:1088-1090`), `buildReportGroupARN` (`backend.go:1092-1094`), `buildWebhookURL` (`backend.go:1096-1098`), `ImportSourceCredentials` inline ARN (`backend.go:1512`), `StartSandbox` inline ARN (`backend.go:1482`).
- Handler never extracts a per-request region and never passes it to backend calls; no `getRegion(ctx, ...)` pattern anywhere in `handler.go` or `backend.go`.
- `provider.go` reads region from config once at Init time and passes it to `NewInMemoryBackend` (`provider.go:35-49`).
- Persistence snapshot includes `Region` field but the flat maps have no region partitioning (`persistence.go:27`).
- Cross-region leak: a project created by a request targeting `us-east-1` is visible to a request targeting `eu-west-1`.

### codecommit — LEAKS

- State is a FLAT map (no region dimension): `repositories map[string]*Repository`, `branches`, `commits`, `pullRequests`, `approvalRuleTemplates` all at `backend.go:211-244`.
- `NewInMemoryBackend(accountID, region)` stores `b.region` at construction time (`backend.go:248-270`); no per-request region resolution.
- `CreateRepository` stores into `b.repositories` flat map with `b.region` baked into the ARN and clone URLs (`backend.go:316-341`).
- `ListRepositories` returns ALL repos across all regions (no filter) (`backend.go:399-416`).
- ARNs built with `b.region` (construction-time): `arn.Build("codecommit", b.region, ...)` at `backend.go:316`; `CreateApprovalRuleTemplate` at `backend.go:504`.
- Handler has no region extraction; `provider.go` reads region once at Init (`provider.go:29-40`).
- Repository struct carries a `Region` field but it is only populated at create time with `b.region` and is never used to filter reads (`backend.go:329`).

### codeconnections — ISOLATED

- State is NESTED by region: all six maps are `map[string]map[string]*T` (outer key = region) at `backend.go:78-87`.
- `NewInMemoryBackend` stores `defaultRegion` (not used as data key) and all maps start empty (`backend.go:90-102`).
- Per-request region is extracted by handler via `httputils.ExtractRegionFromRequest` and injected into `context.WithValue(ctx, regionContextKey{}, region)` at `handler.go:182-189`.
- Every backend operation calls `getRegion(ctx, b.defaultRegion)` to obtain the request region and uses it as the outer map key (e.g., `CreateConnection` at `backend.go:185`, `GetConnection` at `backend.go:226`, `ListConnections` at `backend.go:248`, `DeleteConnection` at `backend.go:275`, `CreateHost` at `backend.go:413`, etc.).
- ARNs include the request region: `arn.Build("codeconnections", region, ...)` at `backend.go:195`, `backend.go:423`, `backend.go:519` — all use the runtime `region` variable, not `b.defaultRegion`.
- Persistence not checked (no `persistence.go` in glob output for this service).

### codedeploy — LEAKS

- State is a FLAT map (no region dimension): `applications map[string]*Application`, `deploymentGroups`, `deployments`, `onPremisesInstances`, `deploymentConfigs` at `backend.go:297-307`.
- `NewInMemoryBackend(accountID, region)` stores `b.region` at construction time (`backend.go:310-326`).
- `CreateApplication` stores into `b.applications` flat map using `b.accountID` and `b.region` (`backend.go:492-528`).
- `ListApplications` returns ALL application names from the flat map without region filter (`backend.go:546-558`).
- `ListDeployments` and all Batch* ops enumerate the flat maps without any region check (`backend.go:922-961`).
- ARNs built with `b.region` (construction-time): `ApplicationARN` uses `b.region` (`backend.go:1116-1118`); `DeploymentGroupARN` uses `b.region` (`backend.go:1121-1123`); `DeploymentConfigARN` uses `b.region` (`backend.go:1126-1128`).
- Handler `handler.go` has no region extraction; `provider.go` reads region once at Init.
- `Application` and `DeploymentGroup` store `Region: b.region` as a field but it is never consulted for filtering (`backend.go:519`, `backend.go:652`).
- Persistence snapshot includes `Region` field but data maps are flat (`persistence.go:28-37`).

### codepipeline — ISOLATED

- State is NESTED by region: all maps are `map[string]map[string]*T` (outer key = region) at `backend.go:365-378`, e.g. `pipelines map[string]map[string]*Pipeline`, `webhooks map[string]map[string]*Webhook`, etc.
- `NewInMemoryBackend(accountID, region)` stores `b.region` as default; all data maps start empty (`backend.go:381-396`).
- Per-request region extracted by handler via `httputils.ExtractRegionFromRequest` and passed via `context.WithValue(ctx, regionContextKey{}, region)` at `handler.go:229/236`.
- Every backend op calls `getRegion(ctx, b.region)` and uses it as the outer store key (e.g., `CreatePipeline` at `backend.go:509`; `GetPipeline` at `backend.go:553`; `UpdatePipeline` at `backend.go:567`; `DeletePipeline` at `backend.go:590`).
- ARNs include runtime request region: `buildPipelineARN(region, name)` at `backend.go:492-494` uses the `region` local variable; `buildWebhookARN(region, name)` at `backend.go:496-498`.
- Persistence not checked (separate file not in flat-map concern).

### codestarconnections — ISOLATED

- State is NESTED by region: all ten maps are `map[string]map[string]*T` (outer key = region) at `backend.go:224-237`, e.g. `connections map[string]map[string]*Connection`, `syncBlockers map[string]map[string]*SyncBlocker`, etc.
- `NewInMemoryBackend(accountID, region)` stores `defaultRegion`; all outer maps start empty (`backend.go:241-257`).
- Per-request region extracted by handler via `httputils.ExtractRegionFromRequest` and injected into context at `handler.go:181/188`.
- Every backend op resolves `getRegion(ctx, b.defaultRegion)` and operates only on the per-region inner map (e.g., `CreateConnection` uses `connectionsStore(region)` at approximately `backend.go:262-268`; same pattern for all other ops including GetConnection, ListConnections, CreateHost, GetHost, etc.).
- ARNs include runtime request region: ARN construction passes `region` local variable (derived from ctx) not `b.defaultRegion`.
- `regionFromARN` helper parses region from an existing ARN for tag ops (`backend.go:34-42`).

### cognitoidentity — ISOLATED

- State is NESTED by region: all seven maps are `map[string]map[string]*T` (outer key = region) at `backend.go:153-163`, e.g. `pools map[string]map[string]*IdentityPool`, `identities map[string]map[string]*Identity`.
- `NewInMemoryBackend(accountID, region)` stores `b.region` as default; all outer maps start empty (`backend.go:166-180`).
- Per-request region extracted by handler via `httputils.ExtractRegionFromRequest` and injected via `context.WithValue(ctx, regionContextKey{}, region)` at `handler.go:138/145`.
- Every backend op resolves `getRegion(ctx, b.region)` and routes to the per-region store (e.g., `CreateIdentityPool` at `backend.go:255`; `DeleteIdentityPool` at `backend.go:307`; `DescribeIdentityPool` at `backend.go:352`; all identity, role, tag, and principal-tag ops follow the same pattern).
- ARNs include runtime request region: `CreateIdentityPool` builds ARN with the `region` local variable, not `b.region` (`backend.go:272-278`).
- Pool ID also encodes the request region: `poolID := region + ":" + uuid.New().String()` (`backend.go:272`).
- Tag ops resolve region from the resource ARN via `regionFromARN` (`backend.go:1062`, `backend.go:1422`, `backend.go:1455`).

### cognitoidp — LEAKS (GLOBAL)

- State is a FLAT map (no region dimension): `pools map[string]*UserPool`, `poolsByName`, `clients`, `users`, `groups`, `domains`, `resourceTags`, etc. at `backend.go:133-187` — no outer region key anywhere.
- `NewInMemoryBackend(accountID, region, endpoint)` stores `b.region` at construction time (`backend.go:232-265`); no per-request region resolution in any backend method.
- `CreateUserPool` stores into `b.pools` flat map and bakes `b.region` into the pool ID and ARN: `poolID := b.region + "_" + ...` and `ARN: fmt.Sprintf("arn:aws:cognito-idp:%s:...", b.region, ...)` at `backend.go:276-288`.
- `DescribeUserPool`, `DeleteUserPool`, `ListUserPools` and all other ops read/write the flat `b.pools` map without any region scope.
- Handler (`handler.go`) never calls `httputils.ExtractRegionFromRequest` and never injects a region into context; dispatch passes ctx through directly (`handler.go:301-308`).
- `provider.go` reads region from config once at Init and passes it to `NewInMemoryBackend` (`provider.go:27-45`); handler stores `region` as a field only for `ChaosRegions()` reporting, not for routing (`handler.go:50-55`).
- A user pool created for a request targeting `us-east-1` is visible and operable from a request targeting `eu-west-1` because all requests hit the same flat `b.pools` map.
- Persistence snapshot not specifically checked but given flat maps there is no region partitioning.

### ec2 — LEAKS

- **State map declaration**: `InMemoryBackend` holds a flat `map[string]*Instance`, `map[string]*VPC`, `map[string]*Subnet`, etc. — no region key in any map (`backend.go:193-327`). The struct has a single `Region string` field set at construction (`backend.go:318`).
- **Create ops region**: `NewInMemoryBackend(accountID, region string)` stores the construction-time region in `b.Region` (`backend.go:449-458`). No op ever calls `awsmeta.Region(ctx)` — the backend methods do not accept a `context.Context` at all (e.g. `RunInstances` at `backend.go:539`, `CreateVpc` at `backend.go:1003`).
- **List/Describe filter**: `DescribeInstances`, `DescribeVpcs`, `DescribeSubnets`, `DescribeSecurityGroups` return everything in the single flat map with no region filter (`backend.go:766-1146`).
- **ARNs**: Built using `b.Region` (construction-time fixed value): `"arn:aws:ec2:" + b.Region + ...` (`backend_batch3.go:194`, `backend_batch4.go:159`, `backend_batch5.go:609`, `backend_batch5.go:721`, `backend_advanced_networking.go:451,549`). The handler also uses `h.Region` for AZ/spot-price/region responses (`handler_ext.go:621,720,2206`).
- **Persistence**: `b.Region` is saved and restored in the snapshot (`persistence.go:193,670`); still single-region.
- **Conclusion**: One backend instance per process serves a single construction-time region; all resources created in that instance are visible regardless of which region the incoming request targets.

---

### ecr — LEAKS

- **State map declaration**: Flat `map[string]*Repository` (no region key), plus `map[string]map[string]*Image` keyed only by repo name (`backend.go:379-403`).
- **Create ops region**: Handler puts region into a local `regionContextKey{}` context key (`handler.go:258-260`). Backend's `regionFor(ctx)` reads via `awsmeta.Region(ctx)` which reads from `awsmeta.Key` — a **different** key type (`backend.go:469-475`). Result: `awsmeta.Region(ctx)` always returns `""` inside ECR ops, so `regionFor` always falls back to `b.region` (the construction-time default). `CreateRepository` calls `region := b.regionFor(ctx)` at `backend.go:503` but since the context key mismatch means it always gets `b.region`, the ARN is always `arn:aws:ecr:<b.region>:...` regardless of request region.
- **List/Describe filter**: `DescribeRepositories` iterates `b.repos` with no region filter (`backend.go:529-561`). Repos created from any region request are visible to all.
- **ARNs**: Built with `b.regionFor(ctx)` which resolves to `b.region` due to context key mismatch (`backend.go:515`).
- **Persistence**: Region field saved/restored but repos map is not region-partitioned.

---

### ecs — LEAKS

- **State map declaration**: Flat `map[string]*Cluster`, `map[string][]*TaskDefinition`, `map[string]map[string]*Service`, `map[string]map[string]*Task` — no region key in any map (`backend.go:290-315`). Single `region string` field set at construction.
- **Create ops region**: `CreateCluster` never receives a context and uses `b.region` directly for all ARNs (`backend.go:451`). `RegisterTaskDefinition` uses `b.region` (`backend.go:635-636`). `CreateService` uses `b.region` (`backend.go:894-895`). `RunTask` uses `b.region` (`backend.go:1277-1278`). No `awsmeta.Region(ctx)` call anywhere in ECS backend.
- **List/Describe filter**: `ListClusters`/`DescribeClusters` return all clusters with no region filter (`backend.go:466-508`). Handler `ChaosRegions` hardcodes `config.DefaultRegion` (`handler.go:147`).
- **ARNs**: All ARNs built with `b.region` (construction-time default) across all files (`backend.go:451`, `backend_ext.go:108-109,377-378,401,628`, `backend_refinement1.go:169,175`, `backend_new_ops.go:205,457`).
- **Persistence**: No region field in `backendSnapshot` (`persistence.go:14-27`); region is not persisted or restored.

---

### efs — ISOLATED

- **State map declaration**: All resource maps are nested by region as outer key: `fileSystems map[string]map[string]*FileSystem`, `mountTargets map[string]map[string]*MountTarget`, `accessPoints map[string]map[string]*AccessPoint`, plus all index maps similarly (`backend.go:282-298`). Per-region lazy store helpers (`fsStore`, `mtStore`, etc.) create inner maps on demand (`backend.go:338-344`).
- **Create ops region**: Handler calls `contextWithRegion(c)` which runs `httputils.ExtractRegionFromRequest(r, h.Backend.Region())` and stores it under `regionContextKey{}` (`handler.go:444-448`). Backend's `getRegion(ctx, b.region)` reads this key (`backend.go:25-31`). `CreateFileSystem` calls `region := getRegion(ctx, b.region)` then writes into `b.fsStore(region)` (`backend.go:573,578`). ARN built with request region via `arn.Build("elasticfilesystem", region, ...)` (`backend.go:609`).
- **List/Describe filter**: `DescribeFileSystems` calls `region := getRegion(ctx, b.region)` and reads from `b.fsStore(region)` only (`backend.go:652,657`). Same pattern for mount targets, access points, lifecycle policies, etc.
- **ARNs**: Built with the per-request region in every Create op.
- **Persistence**: `b.region` (default) is saved/restored, but since all data is region-keyed in the maps, region partitioning is preserved across restarts.
- **Minor note**: `managedKMSKeyARN` constant has a hardcoded `us-east-1` (`backend.go:61`) but this is a stub KMS key placeholder, not an operational resource.

---

### eks — LEAKS

- **State map declaration**: Flat `map[string]*Cluster`, `map[string]map[string]*Nodegroup`, etc. — no region dimension in any map (`backend.go:195-211`). Single `region string` field at construction.
- **Create ops region**: `CreateCluster` uses `b.region` for all ARNs and fields: `arn.Build("eks", b.region, ...)` (`backend.go:361`), `Endpoint: fmt.Sprintf("...%s.eks.amazonaws.com", ..., b.region)` (`backend.go:418`), `OIDCIssuer` with `b.region` (`backend.go:419`), `Region: b.region` (`backend.go:422`). Handler's `handleCreateCluster` never passes a region-carrying context to the backend (`handler.go:1259-1268`).
- **List/Describe filter**: `handleListClusters` calls `h.Backend.ListClusters()` which returns all clusters with no region filter (`handler.go:1326-1332`). No region parameter is consulted.
- **ARNs**: All built with `b.region` across `backend.go:361`, `backend_new_ops.go:152,407,474,526,576`, `backend_remaining_ops.go:1345,1357,1360`.
- **Persistence**: `b.region` stored/restored (`persistence.go:44,239`) but the cluster maps are not region-keyed.

---

### elasticache — ISOLATED

- **State map declaration**: All regional resource maps nested by region: `clusters map[string]map[string]*Cluster`, `replicationGroups map[string]map[string]*ReplicationGroup`, `parameterGroups map[string]map[string]*CacheParameterGroup`, etc. (`backend.go:551-572`). `globalReplicationGroups map[string]*GlobalReplicationGroup` is explicitly global (not region-nested), matching AWS semantics (`backend.go:556`).
- **Create ops region**: Handler calls `h.regionFromRequest(c)` then stores result under `regionContextKey{}` before every dispatch (`handler.go:360-363`). Backend's `getRegion(ctx, defaultRegion)` reads this key (`backend.go:27-33`). Every Create/Describe op calls `getRegion(ctx, b.region)` and operates on the per-region inner map.
- **List/Describe filter**: All Describe operations call `getRegion` and slice into the region-keyed outer map, returning only resources for that region.
- **ARNs**: Built using the per-request region resolved via `getRegion` in each Create op, via `arn.Build(...)`.
- **Persistence**: The region-nested maps are serialized as-is; partitioning is preserved.

---

### elasticbeanstalk — ISOLATED

- **State map declaration**: All maps nested by region as outer key: `applications map[string]map[string]*Application`, `environments map[string]map[string]*Environment`, `appVersions map[string]map[string]*ApplicationVersion`, etc. (`backend.go:175-190`). Per-region lazy store helpers (`applicationsStore`, `environmentsStore`, etc.) (`backend.go:275-297`).
- **Create ops region**: Handler calls `httputils.ExtractRegionFromRequest(r, h.Backend.Region())` and stores under `regionContextKey{}` before every dispatch (`handler.go:293-294`). Backend's `getRegion(ctx, defaultRegion)` reads this key (`backend.go:22-28`). Every Create op calls `getRegion` and writes into the region-specific inner map.
- **List/Describe filter**: All Describe/List ops call `getRegion` and read from the per-region store, isolating results by region.
- **ARNs**: Built via `arn.Build(...)` with the per-request region in each Create op.
- **Persistence**: Region-nested maps are serialized with region partitioning intact.

---

### elasticsearch — ISOLATED

- **State map declaration**: All resource maps nested by region: `domains map[string]map[string]*Domain`, `arnIndex map[string]map[string]string`, `packages map[string]map[string]*Package`, `inboundConnections`, `outboundConnections`, `vpcEndpoints`, `vpcAccess`, `reservedInstances` — all `map[string]map[string]...` (`backend.go:215-231`). Lazy per-region store helpers for each (`backend.go:258-336`).
- **Create ops region**: Handler's `reqContext(r)` extracts region from SigV4 scope via `httputils.ExtractRegionFromRequest` and stores under `regionContextKey{}` (`handler.go:124-128`). Backend's `getRegion(ctx, b.region)` reads this key (`backend.go:21-27`). `CreateDomain` calls `region := getRegion(ctx, b.region)` then operates on `b.domainsStore(region)` and builds ARN with request region (`backend.go:364,368,379`).
- **List/Describe filter**: `DeleteDomain`, `DescribeDomain`, and all list ops call `getRegion` and read from the per-region store (`backend.go:414,437`).
- **ARNs**: Built with per-request region: `arn.Build("es", region, b.accountID, "domain/"+name)` (`backend.go:379`).
- **Persistence**: Region-nested maps serialized with region partitioning preserved.

### elb — ISOLATED
- State nested by region: `lbs map[string]map[string]*LoadBalancer` (backend.go:373), `policies map[string]map[string]*LoadBalancerPolicy` (backend.go:376)
- Private `regionContextKey{}` + `getRegion(ctx, b.region)` used consistently on all ops
- `CreateLoadBalancer` at line 621: `region := getRegion(ctx, b.region)`; ARN at line 649 uses request `region`
- `DescribeLoadBalancers` at line 727: `b.lbsStore(getRegion(ctx, b.region))` — filtered to request region
- All List/Describe/Get/Delete ops scope to request region via `lbsStore(region)`

### elbv2 — LEAKS
- Flat maps: `loadBalancers map[string]*LoadBalancer` (backend.go:426), `targetGroups map[string]*TargetGroup` (backend.go:427), `listeners map[string]*Listener` (backend.go:428), `rules map[string]*Rule` (backend.go:429), `trustStores map[string]*TrustStore` (backend.go:430)
- `StorageBackend` interface methods have no `context.Context` parameter (lines 282–328); region isolation is structurally impossible at the interface level
- All ARN helpers use construction-time `b.region`: `lbARN()` (line 732), `tgARN()` (line 736), `listenerARN()` (line 742), `ruleARN()` (line 760), `trustStoreARN()` (line 764)
- `CreateLoadBalancer` uses `b.region` for DNS name (line 863), availability zones (line 858), canonical hosted zone (line 864)
- `DescribeLoadBalancers` scans all load balancers with no region filter (lines 972–1010)

### emr — ISOLATED
- State nested by region: `clusters map[string]map[string]*Cluster` (backend.go:679), `arnIndex map[string]map[string]string` (backend.go:680), `securityConfigs map[string]map[string]*SecurityConfiguration` (backend.go:681), `studios map[string]map[string]*Studio` (backend.go:682)
- Private `regionContextKey{}` + `getRegion(ctx, b.region)` used on all ops
- `RunJobFlow` at line 958: `region := getRegion(ctx, b.region)`; ARN at line 964 uses `region`; stored at line 1014: `b.clustersStore(region)[id] = cluster`
- `DescribeCluster` at lines 1023–1028 uses request region; `ListClusters` at line 1089 uses request region
- `AddTags` uses `regionFromARN(resourceID, getRegion(ctx, b.region))` for ARN-based lookup

### emrserverless — LEAKS
- Flat maps: `applications map[string]*Application` (backend.go:111), `applicationARNs map[string]string` (backend.go:112), `jobRunARNs map[string][2]string` (backend.go:113), `jobRuns map[string]map[string]*JobRun` keyed by appID not region (backend.go:117)
- No `context.Context` on `CreateApplication`, `GetApplication`, `ListApplications`, `UpdateApplication`, `DeleteApplication`, `StartApplication`, `StopApplication`, `StartJobRun`, `GetJobRun`, `ListJobRuns`, `CancelJobRun`
- All ARN helpers use construction-time `b.region`: `applicationARN()` (line 173), `jobRunARN()` (line 177), `sessionARN()` (line 182)

### eventbridge — PARTIAL
- Core maps ARE region-partitioned: `buses map[string]map[string]*EventBus` (line 195), `rules map[string]map[string]map[string]*Rule` (line 187), `targets map[string]map[string]map[string]*Target` (line 188), `eventSources map[string]map[string]*EventSource` (line 190), `replays map[string]map[string]*Replay` (line 191), `apiDestinations map[string]map[string]*APIDestination` (line 192), `archives map[string]map[string]*Archive` (line 196), `connections map[string]map[string]*Connection` (line 186), `endpoints map[string]map[string]*Endpoint` (line 197), `partnerSources map[string]map[string]*PartnerEventSource` (line 198), `busePolicies map[string]map[string]*EventBusPolicy` (line 199)
- FLAT (no region nesting): `pipes map[string]*Pipe` (line 200), `registries map[string]*SchemaRegistry` (line 201), `schemas map[string]map[string]*Schema` keyed by registryName not region (line 202), `schemaVersions map[string][]*SchemaVersion` (line 203), `codeBindings map[string]*CodeBinding` (line 204)
- Bus/rule ARN helpers correctly take a `region` parameter: `busARN(region, name)` (line 344), `ruleARN(region, busName, ruleName)` (line 348)
- Several ARN helpers use construction-time `b.region`: `apiDestinationARN()` (line 353), `archiveARN()` (line 357), `connectionARN()` (line 361), `endpointARN()` (line 365), `partnerSourceARN()` (line 369), `replayARN()` (line 373)
- Core ops use `getRegionFromContext(ctx, b.region)` correctly; `Reset` at line 1352 re-creates default bus only in `b.region`

### firehose — ISOLATED
- State nested by region: `streams map[string]map[string]*DeliveryStream` (backend.go:332), `pollerCancel map[string]map[string]context.CancelFunc` (backend.go:334)
- Private `regionContextKey{}` + `getRegionFromContext(ctx, b)` used consistently on all ops
- `regionStore(region)` lazily creates per-region map (line 384)
- `CreateDeliveryStream` at line 439: `region := getRegionFromContext(ctx, b)`; stream stored in `b.regionStore(region)`; ARN at line 458: `arn.Build("firehose", region, b.accountID, ...)` uses request region
- All ops (DeleteDeliveryStream, DescribeDeliveryStream, ListDeliveryStreams, PutRecord, PutRecordBatch, UpdateDestination, tag ops) consistently use `getRegionFromContext(ctx, b)`
- Tags include region: `tags.New("firehose." + region + "." + input.Name + ".tags")` (line 467)

### fis — LEAKS
- Flat maps: `templates map[string]*ExperimentTemplate` (backend.go:223), `experiments map[string]*Experiment` (backend.go:224), `templateARNIndex map[string]string` (backend.go:225), `experimentARNIndex map[string]string` (backend.go:226)
- `CreateExperimentTemplate` receives `accountID, region string` as explicit params (line 611) but stores in global flat `b.templates` (line 660) — region used for ARN construction only
- `StartExperiment` receives `accountID, region string` (line 782) but stores in global `b.experiments` (line 835)
- `ListExperimentTemplates` (lines 763–775) returns all templates globally with no region filter
- `ListExperiments` (lines 988–1000) returns all experiments globally with no region filter
- `GetExperimentTemplate`, `UpdateExperimentTemplate`, `DeleteExperimentTemplate`, `GetExperiment`, `StopExperiment` all operate on flat maps with no region scope

### forecast — LEAKS
- Flat maps: `resources map[resourceKind]map[string]*Resource` (backend.go:123), `evaluations map[string][]MonitorEvaluation` (backend.go:124), `tags map[string]map[string]string` (backend.go:125)
- No `context.Context` on any method
- `defaultRegion = "us-east-1"` hardcoded constant (backend.go:26)
- `create()` at line 204: `arn.Build("forecast", b.region, b.accountID, ...)` — uses construction-time `b.region`
- `describe()`, `list()`, `delete()`, `update()` have no region filtering
- `nameFromARN()` at line 329 uses `b.region` to reverse-engineer name from ARN
- `UpdateResourceStatus()` (lines 388–404) and `DeleteResourceTree()` (lines 406–423) scan ALL resources globally

### iotanalytics — LEAKS
- Flat maps with no region dimension: `channels map[string]*Channel`, `datastores map[string]*Datastore`, `datasets map[string]*Dataset`, `pipelines map[string]*Pipeline` at backend.go:252-258
- `StorageBackend` interface methods for List/Describe/Delete take no `ctx`, so region filtering is structurally impossible (backend.go:38-116)
- `CreateChannel` stores resource in flat `b.channels[name]` (backend.go:618); ARN uses request region but resource is globally visible
- `ListChannels` returns ALL channels with no region filter (backend.go:718)
- `DescribeChannel(name string)`, `DeleteChannel(name string)`: no region parameter, no region check

### iotdataplane — LEAKS
- `InMemoryBackend` struct has no region field or dimension: `shadows map[string]map[string]*shadowEntry`, `connections map[string]*connectionEntry`, `retainedMessages map[string]*RetainedMessage` (backend.go:129-135)
- No `awsmeta` import; no region parameter in any method signature; completely region-agnostic
- `GetThingShadow`, `UpdateThingShadow`, `ListThingsWithShadows`, `ListRetainedMessages` all operate without any region context

### iotwireless — PARTIAL
- Primary resource maps use composite `resourceKey{AccountID string; Region string; ID string}` (backend.go:177-181): `devices map[resourceKey]*WirelessDevice`, `gateways map[resourceKey]*WirelessGateway`, `serviceProfiles map[resourceKey]*ServiceProfile`, `destinations map[resourceKey]*Destination`, `deviceProfiles map[resourceKey]*DeviceProfile`, etc. (backend.go:184-210)
- `CreateWirelessDevice` stores with `resourceKey{AccountID: accountID, Region: region, ID: id}` (backend.go:378); `GetWirelessDevice` enforces isolation via same key (backend.go:407); `ListWirelessDevices` filters `k.AccountID == accountID && k.Region == region` (backend.go:423)
- ARN builders take region param (e.g., `wirelessDeviceARN` at backend.go:244)
- LEAKING secondary/association maps (all flat, no region): `partnerAccounts map[string]string`, `fuotaTaskMulticast map[string]string`, `fuotaTaskDevices map[string]string`, `multicastGroupDevices map[string]string`, `multicastGroupSessions map[string]bool`, `wirelessDeviceThings map[string]string`, `wirelessGatewayCerts map[string]string`, `wirelessGatewayThings map[string]string`, `gatewayTasks map[string]*GatewayTask`, `gatewayTaskDefs map[string]*GatewayTaskDefinition`, `positions map[string]map[string]any`, `queuedMessages map[string][]QueuedMessage`, `importTasks`, `singleImportTasks`

### kafka — ISOLATED
- All maps nested by region: `clusters map[string]map[string]*Cluster`, `configurations map[string]map[string]*Configuration`, `scramSecrets map[string]map[string][]string`, `replicators map[string]map[string]*Replicator`, `topics map[string]map[string]*Topic`, `vpcConnections map[string]map[string]*VpcConnection`, `clusterPolicies map[string]map[string]string`, `clusterOperations map[string]map[string]*ClusterOperation` (backend.go:380-388)
- `getRegion(ctx, defaultRegion)` reads from private `regionContextKey{}` (backend.go:26); `regionFromARN` extracts region from ARN (backend.go:36)
- Per-region store accessors lazily initialize region buckets: `clustersStore`, `configurationsStore`, etc. (backend.go:419-488)
- `CreateCluster`: `region := getRegion(ctx, b.region)` then `b.clustersStore(region)` (backend.go:563)
- `ListClusters`: `region := getRegion(ctx, b.region)` then `b.clustersStore(region)` (backend.go:677)
- `DeleteCluster`: `region := regionFromARN(clusterArn, getRegion(ctx, b.region))` (backend.go:704)
- ARN builders take region param (e.g., `clusterARN` at backend.go:506)

### kinesis — ISOLATED
- All maps nested by region: `streams map[string]map[string]*Stream`, `fisThroughputFaults map[string]map[string]*kinesisThrottleFault`, `resourcePolicies map[string]map[string]string` (backend.go:181-190)
- `getRegion(ctx, defaultRegion)` reads from private `regionContextKey{}` (backend.go:93); `regionFromARNOrCtx` prefers ARN-embedded region (backend.go:104)
- Per-region accessors: `streamsStore`, `streamsView`, `faultsStore`, `policiesStore`
- `CreateStream`: `region := getRegion(ctx, b.region)` then `b.streamsStore(region)` (backend.go:328); ARN built with request region (backend.go:413)
- `ListStreams`: `region := getRegion(ctx, b.region)` then `b.streamsView(region)` (backend.go:516)
- `DeleteStream`: `region := getRegion(ctx, b.region)` then `b.streamsStore(region)` (backend.go:433)
- `GetRecords`: uses region from iterator token, not ctx — correct (backend.go:829)

### kinesisanalytics — ISOLATED
- All maps nested by region: `apps map[string]map[string]*Application`, `appsByARN map[string]map[string]*Application`, `cancelFuncs map[string]context.CancelFunc` keyed as "region:name" (backend.go:146-153)
- `getRegion` reads from private `regionContextKey{}` (backend.go:24)
- Per-region accessors: `appsStore(region)`, `appsByARNStore(region)`
- `CreateApplication`: `region := getRegion(ctx, b.defaultRegion)` then `b.appsStore(region)` (backend.go:480); ARN uses request region (backend.go:210)
- `ListApplications`: `region := getRegion(ctx, b.defaultRegion)` then `b.apps[region]` (backend.go:648)
- `DescribeApplication`: `region := getRegion(ctx, b.defaultRegion)` then filters to `b.apps[region]` (backend.go:628)

### kinesisanalyticsv2 — ISOLATED
- All maps nested by region: `applications map[string]map[string]*Application`, `applicationARNs map[string]map[string]string`, `snapshots map[string]map[string][]*Snapshot`, `operations map[string]map[string][]*ApplicationOperation`, `versions map[string]map[string][]*Application` (backend.go:215-225)
- `getRegion` reads from private `regionContextKey{}` (backend.go:20); `regionFromARN` extracts from ARN (backend.go:30)
- Per-region accessors: `applicationsStore`, `arnIndexStore`, `snapshotsStore`, `versionsStore`
- `CreateApplication`: `region := getRegion(ctx, b.defaultRegion)` then `b.applicationsStore(region)` (backend.go:291); ARN uses request region (backend.go:286)
- `ListApplications`: `region := getRegion(ctx, b.defaultRegion)` then `b.applications[region]` (backend.go:348)
- `TagResource`: `region := regionFromARN(resourceARN, b.defaultRegion)` — resolves from ARN (backend.go:582)

### kms — ISOLATED
- All 9 maps nested by region: `keys map[string]map[string]*Key`, `aliases map[string]map[string]*Alias`, `grants map[string]map[string]*Grant`, `grantsByToken map[string]map[string]*Grant`, `grantsByKey map[string]map[string]map[string]*Grant`, `policies map[string]map[string]string`, `keyMaterials map[string]map[string]*keyMaterial`, `keyMaterialHistory map[string]map[string][]*keyMaterial`, `customKeyStores map[string]map[string]*CustomKeyStore` (backend.go:261-278)
- `getRegion` reads from private `regionContextKey{}` (backend.go:28)
- Per-region store accessors for all 9 maps lazily initialize region buckets
- `CreateKey`: `region := getRegion(ctx, b.defaultRegion)` (or `input.Region` override) then `b.keysStore(region)[keyID] = key` (backend.go:614); ARN uses request region (backend.go:665)
- `ListKeys`: `region := getRegion(ctx, b.defaultRegion)` then `b.keysStore(region)` (backend.go:722)
- `Encrypt`, `Decrypt`, `Sign`, etc.: all use `getRegion(ctx, b.defaultRegion)` for key material lookup (backend.go:781, 891, 1163)
- `resolveKeyID`: uses `ctxRegion := getRegion(ctx, b.defaultRegion)` for alias lookups (backend.go:419)


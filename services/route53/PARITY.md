---
service: route53
sdk_module: aws-sdk-go-v2/service/route53@v1.65.6
last_audit_commit: ee7d2bae
last_audit_date: 2026-07-23
overall: A          # this pass: closed BOTH tracked gaps (AssociateVPCWithHostedZone
                    # duplicate-VPC idempotency, CreateReusableDelegationSet HostedZoneId
                    # mode) and 3 of the 4 deferred items — CreateKeySigningKey InvalidKMSArn
                    # validation, alias-cycle depth-guard stress tests, and a genuine
                    # routing-policy bug found while re-deriving TestDNSAnswer's selection
                    # algorithm: GeoProximityLocation- and CidrRoutingConfig-routed record
                    # sets were never recognised by classifyRouting at all and silently fell
                    # through to plain first-by-SetIdentifier answers. Implemented real
                    # selectGeoProximity (bias-scaled great-circle distance) and selectCIDR
                    # (longest-prefix-match against CIDR collection locations, "*" default
                    # fallback) routing. Also removed all 6 banned cyclop/gocognit/funlen
                    # nolints in the service by decomposition (map-dispatch table for
                    # selectAnswer's routing-kind switch, extracted validate*/merge*/apply*
                    # helpers preserving exact error/precedence order). Ran the SDK-driven
                    # route53/route53resolver integration test suite against the real
                    # aws-sdk-go-v2 client (Dockerized binary) — all 45 tests pass, closing
                    # the prior pass's "unit tests only" gap.
ops:
  CreateHostedZone: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CallerReference reuse with different Name/Comment/PrivateZone now returns HostedZoneAlreadyExists (409) instead of silently returning the wrong zone; fixed this pass: DelegationSetId was parsed off the wire and then silently dropped — every zone got the same hardcoded default name servers regardless of what was requested. Now accepts a reusable delegation set (bare or /delegationset/-prefixed ID), validates it exists (NoSuchDelegationSet), and both the CreateHostedZone/GetHostedZone DelegationSet response element and the zone's auto-seeded NS/SOA records use the linked set's real name servers"}
  DeleteHostedZone: {wire: ok, errors: ok, state: ok, persist: ok}
  GetHostedZone: {wire: ok, errors: ok, state: ok, persist: ok, note: "DelegationSet response element now reflects the zone's actual linked reusable delegation set (Id + NameServers) instead of always the fixed default pair — see CreateHostedZone"}
  ListHostedZones: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-r80d) — Marker, a required output member (api_op_ListHostedZones.go: 'the value that you specified for the marker parameter in the request that produced the current response'), was never echoed back; the response struct only carried the optional NextMarker (next-page cursor). Prior wire: ok was false — see 2026-08-14 pass"}
  ListHostedZonesByName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHostedZoneComment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetHostedZoneCount: {wire: ok, errors: ok, state: ok, persist: ok}
  ChangeResourceRecordSets: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATE/DELETE/UPSERT, exact-match DELETE validation, all record-type value validators, routing-policy mutual exclusion, batch validated atomically before any mutation applied — see Notes"}
  ListResourceRecordSets: {wire: ok, errors: ok, state: ok, persist: ok, note: "Name/Type/SetIdentifier lexicographic sort + pagination cursors"}
  CountResourceRecordSets: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChange: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CallerReference reuse with a different HealthCheckConfig now returns HealthCheckAlreadyExists (409); fixed: CALCULATED HealthThreshold > len(ChildHealthChecks) now rejected (InvalidInput)"}
  GetHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHealthChecks: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-r80d) — same missing required Marker echo as ListHostedZones/ListReusableDelegationSets. Prior wire: ok was false — see 2026-08-14 pass"}
  GetHealthCheckCount: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: HealthCheckVersion was entirely missing from the wire (CreateHealthCheck/GetHealthCheck/ListHealthChecks/UpdateHealthCheck responses never emitted it, even though it's a required field in the real HealthCheck shape). Now every health check carries a Version starting at 1, incremented on each successful update; UpdateHealthCheck's optional request-side HealthCheckVersion is checked for optimistic concurrency and returns HealthCheckVersionMismatch (409) on a stale value"}
  GetHealthCheckStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  GetHealthCheckLastFailureReason: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: no longer silently returns empty tags for a nonexistent hosted zone/health check — now validates existence and returns NoSuchHostedZone/NoSuchHealthCheck (404)"}
  ListTagsForResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2 bugs: (1) HTTP route was unreachable (handler checked a bare /2013-04-01/tags path that can never match; real AWS URI is POST /2013-04-01/tags/{ResourceType}), (2) same missing-existence-check bug as ListTagsForResource"}
  ChangeTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: the handler discarded ChangeTagsForResource's error return (setTags/removeTags used `_ = ...`), so tagging a nonexistent resource silently 200'd instead of 404ing; also fixed: resource tags (b.tags) were never wired into Snapshot/Restore and were lost across a backend restore"}
  CreateKeySigningKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: duplicate name in a zone now returns KeySigningKeyAlreadyExists (409) instead of generic InvalidInput; fixed this pass: KeyManagementServiceArn was never validated — any string (including empty) was accepted. Now checked against a KMS customer-managed-key ARN pattern (arn:{aws|aws-cn|aws-us-gov}:kms:<region>:<12-digit-account>:key/<id>) and rejected with InvalidKMSArn (400, confirmed against the CreateKeySigningKey API reference's Errors section) when malformed"}
  ActivateKeySigningKey: {wire: ok, errors: ok, state: ok, persist: ok}
  DeactivateKeySigningKey: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteKeySigningKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: deleting an ACTIVE KSK returned a fabricated 'KeySigningKeyNotInactive' code that doesn't exist in the AWS API; now returns the real InvalidKeySigningKeyStatus (400)"}
  EnableHostedZoneDNSSEC: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableHostedZoneDNSSEC: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDNSSEC: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateVPCWithHostedZone: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: re-associating a VPC already associated with the same zone now returns success (idempotent no-op) instead of a fabricated InvalidInput error. AWS's documented error list has no duplicate-association error, and the one association-conflict error it does document (ConflictingDomainExists) is explicitly scoped to a *different* hosted zone with the same name, ruling it out for this case — confirmed against the AssociateVPCWithHostedZone API reference's Errors section"}
  DisassociateVPCFromHostedZone: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: VPC not associated now returns VPCAssociationNotFound (404) instead of generic InvalidInput; LastVPCAssociation guard already correct"}
  ListVPCAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHostedZonesByVPC: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-r80d) — MaxItems, a required output member (api_op_ListHostedZonesByVPC.go:36-40), was absent from the response struct entirely (not merely unset); the SDK always decoded a nil *int32. Handler now parses the optional maxitems query param (default 100, maxHZByVPC) and echoes it. Prior wire: ok was false — see 2026-08-14 pass"}
  CreateVPCAssociationAuthorization: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVPCAssociationAuthorization: {wire: ok, errors: ok, state: ok, persist: ok}
  ListVPCAssociationAuthorizations: {wire: ok, errors: ok, state: ok, persist: ok}
  CountAssociatedVPCs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCidrCollection: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: duplicate collection name now returns CidrCollectionAlreadyExistsException (400) instead of allowing an unbounded number of same-named collections"}
  ChangeCidrCollection: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: added the optional CollectionVersion request field; when supplied it is checked against the collection's current Version and a mismatch returns CidrCollectionVersionMismatchException (409)"}
  DeleteCidrCollection: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: real AWS requires a CIDR collection to be empty (no locations/CIDR blocks) before it can be deleted; gopherstack previously deleted non-empty collections unconditionally. Now returns CidrCollectionInUseException (400) when Locations is non-empty"}
  ListCidrCollections: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCidrLocations: {wire: ok, errors: ok, state: ok, persist: ok, note: "code fix: NoSuchCidrCollection -> NoSuchCidrCollectionException (real AWS shape name has the Exception suffix, confirmed against aws-sdk-go-v2 types/errors.go — unlike every other Route53 NoSuch* error)"}
  ListCidrBlocks: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateQueryLoggingConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "status fix: QueryLoggingConfigAlreadyExists 400 -> 409"}
  GetQueryLoggingConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteQueryLoggingConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  ListQueryLoggingConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReusableDelegationSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "status fix (prior pass): NoSuchDelegationSet 404 -> 400. Fixed this pass: the HostedZoneId param (real AWS's 'mark an existing hosted zone's delegation set as reusable' mode, confirmed against the CreateReusableDelegationSet API reference) was parsed off the wire and silently discarded. Now validates the zone exists (HostedZoneNotFound, 400 — a distinct wire code from NoSuchHostedZone, confirmed against the same reference), rejects private zones (a reusable delegation set can't be associated with a private hosted zone, per the operation's own doc text), rejects a zone whose delegation set was already extracted this way (DelegationSetAlreadyReusable, 400), and returns a new reusable set carrying the zone's real name servers (tracked via a backend-internal, non-wire HostedZone.DelegationSetSourceUsed bookkeeping field, confirmed to survive Snapshot/Restore). Also fixed a second, previously-untracked bug found while auditing this op: reusing a CallerReference across two CreateReusableDelegationSet calls silently created two unrelated delegation sets instead of erroring — now returns DelegationSetAlreadyCreated (400, confirmed against the same API reference), matching real AWS's non-idempotent CallerReference-reuse behavior for this specific operation (unlike CreateHostedZone/CreateHealthCheck's idempotent-retry semantics)"}
  GetReusableDelegationSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReusableDelegationSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: now returns DelegationSetInUse (400) if any hosted zone is still linked to the set, instead of deleting it out from under live zones"}
  ListReusableDelegationSets: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-r80d) — same missing required Marker echo as ListHostedZones/ListHealthChecks; handler didn't even read the marker query param. Prior wire: ok was false — see 2026-08-14 pass"}
  CountZonesByReusableDelegationSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: previously always returned 0 (hosted zones were never linked to delegation sets at all); now counts real linked zones"}
  TestDNSAnswer: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed this pass: classifyRouting never recognised GeoProximityLocation or CidrRoutingConfig at all (only Weight/Region/GeoLocation/Failover/MultiValueAnswer), so geoproximity- and CIDR-routed record sets silently fell through to routingSimple and TestDNSAnswer answered from whichever candidate sorted first by SetIdentifier instead of running real proximity/CIDR selection — a genuine wrong-answer bug, not just an unverified-but-correct algorithm. Implemented selectGeoProximity (great-circle distance from awsRegionCoords/parsed lat-lon, scaled by (1 - Bias/100) per AWS's documented bias direction — exact geometry is AWS-undocumented, so this is a faithful approximation, not a re-derivation of a public spec) and selectCIDR (longest-prefix-match against the CIDR collection's location blocks, reserved \"*\" location as the catch-all default, matching AWS's documented CIDR-routing specificity rule). Weighted/latency/failover/geolocation/multivalue selection re-read against AWS's routing-policy documentation this pass and found already correct; not fully re-derived against non-public AWS source, see deferred"}
  CreateTrafficPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "status fix: TrafficPolicyAlreadyExists 400 -> 409"}
  CreateTrafficPolicyVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTrafficPolicyInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: allowed unlimited duplicate instances for the same (hostedZoneID, name); now returns TrafficPolicyInstanceAlreadyExists (409)"}
  UpdateTrafficPolicyInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTrafficPolicyComment: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTrafficPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTrafficPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTrafficPolicyInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTrafficPolicyInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrafficPolicies: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-lx5h) — response dropped TrafficPolicyIdMarker, a required member on ListTrafficPoliciesOutput (deserializers.go's ListTrafficPoliciesOutput switch) that AWS always serializes, not just when truncated. This backend is single-page (IsTruncated always false), so the marker is emitted as an always-present empty string rather than a fabricated next-page ID. Prior wire: ok was false"}
  ListTrafficPolicyVersions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-lx5h) — same TrafficPolicyVersionMarker gap and fix as ListTrafficPolicies' TrafficPolicyIdMarker above. Prior wire: ok was false"}
  ListTrafficPolicyInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrafficPolicyInstancesByHostedZone: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrafficPolicyInstancesByPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  record_types: {status: ok, note: "A/AAAA/CNAME/MX/TXT/SPF/NS/SOA/PTR/SRV/CAA/DS/NAPTR value-format validators verified against RFC-shaped regexes; HTTPS/SVCB/SSHFP/TLSA intentionally accept any value (no AWS-documented format constraint enforced by the real service either)"}
  routing_policies: {status: ok, note: "Weighted(SetIdentifier+Weight 0-255)/Latency(Region)/Failover(PRIMARY|SECONDARY)/Geolocation/Multivalue/Geoproximity(exactly one of AWSRegion|Coordinates|LocalZoneGroup, Bias -99..99, lat/lon range-checked)/CIDR routing all validated for mutual exclusion and SetIdentifier requirement per AWS rules at ChangeResourceRecordSets time. fixed this pass: TestDNSAnswer's selection algorithm (classifyRouting/selectAnswer) never actually ran geoproximity or CIDR selection at all despite validating those fields — see TestDNSAnswer note in ops table. Weighted/latency/geo/failover/multivalue selection re-checked against AWS's public routing-policy docs and found correct (all-zero weights split equally, exact-region-match short-circuits latency, PRIMARY-healthy-else-SECONDARY failover, most-specific geolocation match, up-to-8-record multivalue cap)"}
  dnssec: {status: ok, note: "EnableHostedZoneDNSSEC requires >=1 ACTIVE KSK (KeySigningKeyWithActiveStatusNotFound), KSK lifecycle (create/activate/deactivate/delete) state machine verified"}
  errCodeLookup: {status: ok, note: "every route53 sentinel error's wire code + HTTP status cross-checked this pass against aws-sdk-go-v2/service/route53@v1.62.3 types/errors.go and the botocore api-2.json httpStatusCode field — see fixes in ops table above"}
gaps: []  # both tracked gaps (gopherstack-8l0.5, gopherstack-8l0.3) closed this pass, see ops table
deferred:
  - selectWeighted/selectLatency/selectGeo/selectFailover/multiValueAnswer were re-checked against AWS's *public* routing-policy documentation this pass (see routing_policies family note) and found correct, but not re-derived against AWS's non-public source — Route 53's exact selection algorithm (esp. latency-routing tie-breaks and geoproximity's precise bias geometry) is not fully published, so "matches documented behavior" is the strongest verification achievable without live-AWS access
leaks: {status: clean, note: "no goroutines, tickers, or background timers anywhere in services/route53 (grep for 'go func|time.After|time.Sleep|Ticker' returns nothing) — all ops are synchronous request/response; Reset()/DeleteHostedZone/DeleteHealthCheck correctly cascade-delete tags/KSKs/VPC-assocs/query-logging-configs so no orphaned map entries accumulate under normal use. b.tags itself was NOT wired into Snapshot/Restore before a prior pass (fixed then) — that was a persistence gap, not a leak, since Reset() already covered it. This pass's new HostedZone.DelegationSetSourceUsed field is backend-internal (not a new map/table) and rides along with the existing zoneDataSnapshot embedding of HostedZone, confirmed to survive Snapshot/Restore by TestSnapshotRestore_DelegationSetSourceUsed — no new lock paths, no new leak surface."}
---

## Notes

### 2026-08-29 (error-path sweep: what a typed client sees on failure)

Extracted all 71 `awsRestxml_deserializeOpError<Op>` switches from route53@v1.65.6's
deserializers.go and cross-referenced every backend/handler call site raising a sentinel
error (or a literal wire code) against its own op's modeled set. `backendErrorTable`
(handler.go) — the shared sentinel-to-wire-code table every op funnels through via
`handleBackendError` — was correct and 1:1 with errors.go's sentinels (unlike quicksight,
which collapses by category; unlike this table, which maps every sentinel to its own distinct
code, matching real AWS's fine-grained Route 53 error set). No sentinel-reuse or wrong-code
bugs found across the 62 ops resolvable by direct backend-method-name call-graph tracing
(op name == `StorageBackend` interface method name here, confirmed via interfaces.go).

**Real bug found and fixed**: `UpdateHostedZoneFeatures` never validated `HostedZoneId` at
all — `updateHostedZoneFeatures` (handler_hosted_zones.go) discarded its `path` argument
(`func (h *Handler) updateHostedZoneFeatures(c *echo.Context, _ string) error`) and
unconditionally returned success. Its own deserializer models `NoSuchHostedZone` for exactly
this case (alongside `InvalidInput`/`LimitsExceeded`/`PriorRequestNotComplete`) — a
missing-error bug (returning success where AWS raises), not a wrong-code one. Fixed by parsing
the zone ID from the path (same `TrimPrefix`/`TrimSuffix` pattern as the sibling
`disassociateVPCFromHostedZone`) and validating existence via the already-available
`GetHostedZone` backend method before returning success. Covered by
`error_path_sweep_test.go` (real `aws-sdk-go-v2/service/route53` client, `errors.As` against
`types.NoSuchHostedZone`). Persisting the `EnableAcceleratedRecovery` flag itself is a separate,
larger feature gap (the `StorageBackend` interface has no such field/method) and was left
out of scope for this error-path-only pass.

**Method note**: 9 of the 71 ops (`GetAccountLimit`, `GetCheckerIpRanges`, `GetGeoLocation`,
`GetHealthCheckLastFailureReason`, `GetHostedZoneLimit`, `GetReusableDelegationSetLimit`,
`GetTrafficPolicyInstanceCount`, `ListGeoLocations`, `UpdateHostedZoneFeatures`) have no
backend method of the same name — they're implemented as handler-layer functions instead
(`getHostedZoneLimit`, `getGeoLocation`, etc., in handler_*.go), so a naive op-name-to-method
call-graph trace misses them entirely and silently under-reports. Re-traced each by its actual
handler function name. `GetGeoLocation` raises `NoSuchGeoLocation` via a direct `xmlError(...)`
call rather than a named sentinel (correct — the code was simply invisible to sentinel-based
tracing, not missing). `GetCheckerIpRanges`/`GetTrafficPolicyInstanceCount`/`GetAccountLimit`
model no core error code at all and correctly raise none.

**Protocol**: REST-XML (path/verb routing, XML request+response bodies), matching
`aws-sdk-go-v2/service/route53`'s `awsRestxml_*` (de)serializers. Namespace
`https://route53.amazonaws.com/doc/2013-04-01/` on every response root element.

**ChangeResourceRecordSets — the core op, and its traps**:
- Change-batch validation is two-phase: every `Change` in the batch is validated
  against the *pre-batch* zone snapshot first (in submission order), and only if
  *all* validate does the second phase apply every mutation. This correctly makes
  the whole batch atomic (all-or-nothing) and matches AWS's documented behavior for
  the canonical example of deleting a CNAME and creating an alias A record for the
  same name in one batch (different `Type` values ⇒ different map keys ⇒ no
  collision during validation).
- **Trap** (not a bug, verified deliberately): a batch containing a literal
  `DELETE` followed by `CREATE` for the *exact same* `(Name, Type, SetIdentifier)`
  key will fail with "record set already exists" during the CREATE's validation,
  because validation runs against the unmutated zone. This is very unlikely to be
  a real-world pattern (UPSERT already covers "replace this record's values"), and
  AWS's own documented DELETE+CREATE example always uses two different `Type`
  values, so this was deliberately left as-is rather than "fixed" without stronger
  evidence of the real batch-processing order for the same-key case. Flagged here
  so the next auditor doesn't have to re-derive this.
- DELETE requires an exact match of TTL + resource-record values (or AliasTarget)
  against the current record, unless the request omits both (a "bare" delete by
  name+type+SetIdentifier) — `deleteValuesMatch` already encodes this correctly.
- Alias vs. non-alias TTL rule: TTL is required (and range-checked to
  `2147483647`) only when `AliasTarget == nil`; alias records must omit TTL. Already
  correct.

**Error code auditing method this pass**: every sentinel error in `backend.go`'s
`var (...)` block was cross-checked against two independent AWS sources: (1)
`aws-sdk-go-v2/service/route53@v1.62.3`'s generated `types/errors.go` (gives the
literal `ErrorCode()` wire string per exception type), and (2) the botocore
`api-2.json` model's `"error":{"httpStatusCode":N}` field per shape (gives the
real HTTP status). Six concrete mismatches were found and fixed — see the `ops`
table. The most surprising one: `NoSuchCidrCollectionException`,
`NoSuchCidrLocationException`, and `NoSuchCloudWatchLogsLogGroup` are the *only*
Route 53 "NoSuch*" errors whose wire code carries the `Exception` suffix; every
other `NoSuch*` error (NoSuchHostedZone, NoSuchHealthCheck, NoSuchChange, ...) does
not. Also surprising: `NoSuchDelegationSet` is HTTP 400, not 404, unlike every
other `NoSuch*` error in the service (confirmed twice against both sources before
trusting it).

**CallerReference idempotency — the real rule**: reusing a `CallerReference` on
`CreateHostedZone`/`CreateHealthCheck` is idempotent (returns the original
resource) *only* when every other input parameter is identical to the original
request. Reusing it with *any* different parameter returns
`HostedZoneAlreadyExists`/`HealthCheckAlreadyExists` (409) — it is not a "last
write wins" or "always return the first" semantic. A prior audit pass had gotten
this backwards and encoded the wrong behavior directly into test assertions
(`same_ref_different_name_still_idempotent`); those tests were corrected this
pass rather than left as false parity proof.

**ListTagsForResources routing — real AWS request URI**: `POST
/2013-04-01/tags/{ResourceType}` (batch lookup by `ResourceType` + a list of
`ResourceId`s in the XML body) — there is no bare `/2013-04-01/tags` endpoint.
The handler previously checked for the nonexistent bare path (which additionally
could never be reached anyway, since the outer dispatcher requires the
`/2013-04-01/tags/` prefix with a trailing slash), so a real AWS SDK client's
`ListTagsForResources` call was silently misrouted into `ChangeTagsForResource`
and 404'd. Fixed by detecting "no `/` after the ResourceType segment" instead of
comparing against a bare-prefix string.

**Tag-family disguised-stub trap**: `ChangeTagsForResource`'s backend-level
existence check was always correct — but the handler called it through two
one-line wrappers (`setTags`/`removeTags`) that discarded the returned error
(`_ = h.Backend.ChangeTagsForResource(...)`), so the real validation never
surfaced over the wire. This is the "real-looking op that's actually a disguised
stub" pattern from parity-principles.md: grepping for the backend method alone
would have shown correct-looking code; the bug was purely in the handler
throwing the result away.

## 2026-07-12 re-audit (this pass)

No local drift in `services/route53/` between the prior audit commit
(`ce30166a`, which the ledger's `last_audit_commit: 017fc20a` predates on a
squashed/rebased branch — see re-audit protocol) and this pass's start.
Per the re-audit protocol, only the `partial`-rated rows the prior ledger
flagged were re-examined; all `ok` rows were trusted unchanged. Three of the
four tracked gaps were closed:

**`HealthCheckVersion` was missing from the wire entirely, not just
unchecked.** The prior ledger described this gap as "no optimistic-concurrency
check", implying the field existed but wasn't validated. In fact
`HealthCheckVersion` — a *required* field on AWS's `HealthCheck` shape,
confirmed against `aws-sdk-go-v2/service/route53@v1.62.3` `types/types.go`
— was never serialized into `CreateHealthCheck`/`GetHealthCheck`/
`ListHealthChecks`/`UpdateHealthCheck` responses at all; `HealthCheck` had no
`Version` field on the backend struct. This is the "wrong wire shape",
not just "missing error check", bug class from parity-principles.md. Fixed:
`HealthCheck.Version` now starts at 1 and increments on every successful
`UpdateHealthCheck`; the optional request-side `HealthCheckVersion` is
checked when present and returns `HealthCheckVersionMismatch` (409,
confirmed via `botocore`'s `route53/2013-04-01/service-2.json`
`error.httpStatusCode`) on a stale value.

**`ChangeCidrCollection`/`DeleteCidrCollection`** — added the optional
`CollectionVersion` optimistic-concurrency check (mirrors the
`HealthCheckVersion` fix; `CidrCollectionVersionMismatchException`, 409) and
the "collection must be empty before it can be deleted" guard
(`CidrCollectionInUseException`, 400 — confirmed via botocore: despite the
similar name/shape to the 409 version-mismatch error, this one really is
400, not 409).

**Reusable delegation set <-> hosted zone linkage** was the largest gap:
`CreateHostedZoneRequest.DelegationSetId` was parsed off the XML wire and
then silently dropped on the floor — every hosted zone got the same
hardcoded default name-server pair no matter what was requested, and
`CountZonesByReusableDelegationSet` always returned 0 / `DeleteReusableDelegationSet`
never checked for in-use sets because zones were *structurally* never linked
to delegation sets at all (no field to link them). Fixed by adding
`HostedZone.DelegationSetID`/`NameServers` fields; `CreateHostedZone` now
resolves and validates a supplied `DelegationSetId` (accepting both the bare
`N...` form and the `/delegationset/N...` form real AWS returns, matching
the existing normalization convention in `handler_completeness.go`'s
delegation-set routes — factored out into the shared `normaliseDelegationSetID`
helper), uses the linked set's real name servers for both the
`DelegationSet` response element and the zone's auto-seeded NS/SOA records,
and `DeleteReusableDelegationSet`/`CountZonesByReusableDelegationSet` now
walk live zones instead of a permanently-empty relationship.

Not fixed in that pass: `CreateReusableDelegationSet`'s `HostedZoneId` param
and `AssociateVPCWithHostedZone`'s duplicate-VPC error code — both closed in
the 2026-07-23 pass below.

## 2026-07-23 pass (this pass)

Closed both tracked gaps from the prior audit and 3 of its 4 deferred items.
Also found and fixed a genuine wrong-answer bug in `TestDNSAnswer` while
re-deriving the routing-policy selection algorithms (the deferred item this
ledger flagged for the next audit), and removed all 6 of the service's
`//nolint:cyclop|gocognit|funlen` suppressions by decomposition per the
campaign's banned-nolint sweep.

**`AssociateVPCWithHostedZone`'s duplicate-VPC re-association** — fetched
the live AWS API reference (`API_AssociateVPCWithHostedZone.html`) and
confirmed its documented error list (`ConflictingDomainExists`,
`InvalidInput`, `InvalidVPCId`, `LimitsExceeded`, `NoSuchHostedZone`,
`NotAuthorizedException`, `PriorRequestNotComplete`,
`PublicZoneVPCAssociation`) has no error for "VPC already associated with
*this* zone", and `ConflictingDomainExists`'s documented cause is
specifically "the VPC is already associated with *another* hosted zone that
has the same name" — ruling it out for this case. Changed the backend to
treat re-association as an idempotent no-op (matches the general community
understanding reflected in Terraform's Route53 VPC-association resource
design). `TestDuplicateVPC` rewritten to assert success + a stable VPC count
of 1 instead of the previously-asserted (and now-understood-to-be-wrong)
`InvalidInput` error.

**`CreateReusableDelegationSet`'s `HostedZoneId` param** — fetched the live
AWS API reference and confirmed this is real AWS's "mark an existing hosted
zone's delegation set as reusable" mode, with its own error list
(`DelegationSetAlreadyCreated`, `DelegationSetAlreadyReusable`,
`DelegationSetNotAvailable`, `HostedZoneNotFound` [not `NoSuchHostedZone` —
a distinct wire code specific to this operation, confirmed against
`aws-sdk-go-v2/service/route53@v1.62.3` `types/errors.go`'s
`HostedZoneNotFound` type], `InvalidArgument`, `InvalidInput`,
`LimitsExceeded`). Implemented: zone-existence check (`HostedZoneNotFound`,
400), private-zone rejection (per the operation's own doc text: "You can't
associate a reusable delegation set with a private hosted zone"),
double-extraction rejection (`DelegationSetAlreadyReusable`, 400, tracked via
a new backend-internal `HostedZone.DelegationSetSourceUsed` bool — not part
of the wire `HostedZone` shape, confirmed to survive Snapshot/Restore), and
real name-server inheritance from the source zone. While implementing this,
found and fixed a second, previously-untracked bug in the *same* function:
`CreateReusableDelegationSet` never checked for `CallerReference` reuse at
all, silently creating unlimited duplicate delegation sets for the same
reference — now returns `DelegationSetAlreadyCreated` (400), matching real
AWS's error-on-reuse semantics for this specific operation (unlike
`CreateHostedZone`/`CreateHealthCheck`'s idempotent-retry-on-identical-input
semantics, which a much earlier pass had already gotten right for those two
ops — `CreateReusableDelegationSet` is documented as genuinely different:
"you must use a unique CallerReference string every time").

**`TestDNSAnswer` routing-policy re-derivation — the deferred item, and what
it actually found.** Re-reading `classifyRouting` against the full list of
routing-policy fields `ResourceRecordSet` carries (checked against
`validateRoutingPolicy`'s own mutual-exclusion list, which already covered
all seven policy fields) surfaced that `classifyRouting` only recognised
five of them — `Weight`, `Region`, `GeoLocation`, `Failover`,
`MultiValueAnswer` — never `GeoProximityLocation` or `CidrRoutingConfig`.
Record sets using either fell through to `routingSimple`, meaning
`TestDNSAnswer` answered from whichever candidate sorted first by
`SetIdentifier` instead of running any proximity or CIDR matching at all —
a silent wrong-answer bug on every geoproximity- or CIDR-routed zone, not
merely an "unverified but probably fine" algorithm. This is exactly the
"real-looking op that's actually a disguised stub" pattern from
parity-principles.md: `ChangeResourceRecordSets` validated these fields
correctly (so grepping for `GeoProximityLocation`/`CidrRoutingConfig`
handling would have shown seemingly-complete code), but the read path never
consulted them. Fixed by adding `routingGeoProximity`/`routingCIDR` kinds and
two new selectors: `selectGeoProximity` (great-circle distance from
`awsRegionCoords` or parsed `Coordinates` lat/lon, scaled by
`1 - Bias/100` — AWS documents Bias's *direction* [higher bias expands a
resource's effective service area] but not its exact geometry, so this is a
faithful approximation of the documented behavior, not a re-derivation of a
public spec) and `selectCIDR` (longest-prefix-match against the referenced
CIDR collection's location blocks, with the reserved `"*"` location as the
default fallback — this *is* a fully documented AWS rule, unlike
geoproximity's bias geometry). The other five routing kinds
(weighted/latency/geo/failover/multivalue) were re-checked against AWS's
public routing-policy documentation and found already correct.

**Alias cycle/depth handling** — added `TestTestDNSAnswerAliasCycle`
covering both a self-referencing alias (`a` -> `a`) and a two-hop cycle
(`a` -> `b` -> `a`), each run with a goroutine + 5s timeout so a regression
that broke `maxAliasDepth`'s guard would fail the test instead of hanging
the suite. Both terminate correctly with an empty answer, confirming
`resolveAlias`'s existing `depth >= maxAliasDepth` guard already handles
pathological chains — no code change needed here, just proof.

**`CreateKeySigningKey` `InvalidKMSArn`** — added `reKMSArn`, a regex
matching a well-formed KMS customer-managed-key ARN across the
standard/China/GovCloud partitions
(`arn:{aws|aws-cn|aws-us-gov}:kms:<region>:<12-digit-account>:key/<id>`),
checked after the zone-existence lookup (so `create_ksk_zone_not_found`-style
requests still 404 before ever reaching ARN validation, matching this
service's existing required-field-then-existence-then-format validation
order). Every existing test's placeholder ARNs (`"arn:kms:test"` and
similar clearly-non-ARN strings) were updated to well-formed fake ARNs.

**Banned-nolint sweep** — removed all 6 `//nolint:cyclop|gocognit|funlen` in
the service:
- `cidr_collections.go`'s `ChangeCidrCollection` (`gocognit`): extracted
  `applyCidrCollectionPut`/`applyCidrCollectionDeleteIfExists`/`applyCidrCollectionChange`.
- `handler_health_checks.go`'s `updateHealthCheck` (`gocognit,cyclop,funlen`):
  extracted `mergeHealthCheckUpdate{Strings,Numeric,Flags,Collections}`.
- `handler_record_sets.go`'s `changeResourceRecordSets` (`funlen`):
  extracted `toBackendResourceRecordSet`/`toBackendChange`.
- `record_sets.go`'s `validateRoutingPolicy` (`cyclop`): extracted
  `countRoutingPolicies`/`validateRoutingPolicyCardinality`/`validateRoutingPolicyFields`.
- `record_sets.go`'s `validateChange` (`gocognit,cyclop`): extracted
  `validateChangeType`/`validateChangeTTL`/`validateChangeCNAME`/`validateChangeRecordValues`/`validateChangeActionState`.
- `record_sets.go`'s `ListResourceRecordSets` (`gocognit,cyclop`): extracted
  `sortRecordSets`/`seekRecordSetStart`/`paginateRecordSets`.

Decomposing `selectAnswer` to add the two new routing kinds pushed it over
`cyclop`'s limit on its own; replaced the routing-kind `switch` with a
`map[routingKind]singleAnswerSelector` dispatch table built once via
`sync.OnceValue` (the established `apigatewayv2`-style pattern this campaign
uses elsewhere), keeping `routingMultiValue`/`routingSimple` — which don't
fit the "one selector function" shape — as explicit early returns.
`selectCIDR`'s own longest-prefix-match loop separately tripped `gocognit`
once written; split into `cidrBlockLongestPrefix` (single-location scan) and
`cidrCandidateMatch` (per-candidate resolution) to flatten the nesting.

**SDK-driven integration-test run** — `make build-linux` (whole-repo
monolith binary; every service links into one binary, so this isn't
route53-specific and is genuinely slow in a resource-constrained sandbox —
two earlier attempts in this pass hit multi-minute wall-clock stalls before
finally completing) followed by `go test ./test/integration/... -run
Route53` against the resulting Dockerized binary. All 45 route53 and
route53resolver integration tests passed, including
`TestIntegration_Route53_ChangeResourceRecordSets`,
`TestIntegration_Route53_WeightedRouting`,
`TestIntegration_Route53_FailoverRouting`,
`TestIntegration_Route53_TestDNSAnswerWeighted`,
`TestIntegration_Route53_HealthCheck_Lifecycle`,
`TestIntegration_Route53_DeactivateDeleteKSK`,
`TestIntegration_Route53_EnableDisableDNSSEC`, and
`TestIntegration_Route53_ResourceRecordSetsChangedWaiter` — proving this
pass's fixes against a real `aws-sdk-go-v2` client round-trip, not just unit
tests, per parity-principles.md's "unit tests are not parity proof"
guidance. No dedicated `route53_parity_test.go` exists yet (the existing
coverage is spread across `route53_test.go`/`route53_audit_test.go`/
`route53_new_ops_test.go`/`route53_waiter_test.go`); creating one consolidated
file is a housekeeping task for a future pass, not a correctness gap.

## 2026-08-14 pass (gopherstack-r80d): required output member sweep

Extracted every field marked `This member is required.` at the top level of
an `<Op>Output` struct across all 71 `route53@v1.65.6` operations (parsed
directly from the pinned SDK's `api_op_*.go` files, blank-line-separated
field blocks, case-/tag-suffix-tolerant), yielding 108 required output
members across 58 of 71 ops — validated against the extraction tool's
known-answer case (kinesis's `DescribeLimits`, 4/4 exact match, matching the
bug fixed in `be789761c`) and a negative case (kinesis's `ListShards`, 0/0)
before trusting it at route53's scale.

Every one of the 58 ops was read end-to-end (not grepped) to confirm each
required field is actually written into the response, per
parity-principles.md's "grep alone shows real-looking code, read the path to
be sure" guidance. Found and fixed **4** silently-unset required output
members, all one bug class — real AWS's `Marker` element ("the value you
specified for the marker parameter in the request that produced the current
response") being conflated with the *optional* `NextMarker` next-page
cursor, or (for `ListHostedZonesByVPC`) `MaxItems` missing from the response
struct entirely:

- `ListHostedZones`, `ListHealthChecks`, `ListReusableDelegationSets`:
  response structs only carried `NextMarker`; the required `Marker` echo of
  the request's own `marker` parameter was never wired at all.
  `ListReusableDelegationSets`'s handler didn't even parse the `marker`
  query param.
- `ListHostedZonesByVPC`: `MaxItems` — required, but the response struct had
  no field for it and the handler never read the `maxitems` query param.

All four are the same silent-zero-value class as batch one's lambda finding:
a typed SDK client decodes a `nil`/`""` for a field AWS guarantees is always
present, with no error surfaced. Each fix is covered by an SDK-driven round
trip test (`wire_output_required_r80d_test.go`) that sets the corresponding
request field to a distinguishing non-empty value and asserts it comes back
unchanged (not merely non-nil) — verified to fail against the pre-fix code
by hand-reverting each change and confirming an `md5sum`-identical restore
afterward.

The remaining 104 required output fields across the other 54 ops were all
confirmed correctly populated by reading each handler's response-construction
code. **route53 is settled for this bug class**: every required output
member across every op that has one has been read and checked, not sampled.

## 2026-08-13 pass (gopherstack-l5ir): route reachability audit

All 71 real route53 ops were extracted from `route53@v1.65.6` serializers.go
(`request.Method` + `httpbinding.SplitURI(...)` in each op's
`awsRestxml_serializeOp<Op>.HandleSerialize`) and diffed against `routeRequest`'s
dispatch tree. Found and fixed **one** op that resolved to a plausible WRONG
op rather than 404ing: `GetHealthCheckLastFailureReason`
(`GET .../healthcheck/{id}/lastfailurereason`) fell through `routeHealthCheck`'s
generic method switch (which only special-cased the `/status` suffix, not
`/lastfailurereason`) and silently returned the full `HealthCheck` object --
`GetHealthCheck`'s response shape, not the failure-reason response -- for
every real client call. The implementation (`getHealthCheckLastFailureReason`)
already existed and was already correct; it was simply unreachable. This is
exactly the "resolves to a plausible wrong op, not a 404" class of bug that a
route-table diff alone (as opposed to a real per-op resolution test) misses
-- see gopherstack-4nek's cloudfront findings for the precedent. Fixed by
checking the `/lastfailurereason` suffix before the generic switch, mirroring
the existing `/status` handling. The dead `routeCompletenessLimits` branch
that appeared to handle this path (but never could, since `routeRequest`'s
top-level switch always routes any `/healthcheck/...` path to `routeHealthCheck`
first) was removed and documented rather than left as a misleading no-op.
`extractHealthCheckOperation`/`iamActionForHealthCheck` (ExtractOperation's
and IAMAction's own, separate implementations of the same shape) carried the
identical bug and were fixed identically.

All other 70 ops, including every shared-path pair method-disambiguated on
the same URL (the tags trio, hostedzone GET/DELETE/POST, trafficpolicy
GET/DELETE/POST at both the `{Id}` and `{Id}/{Version}` depths, and
GetGeoLocation/ListGeoLocations sharing one switch case across two literal
paths, `/geolocation` vs `/geolocations`, disambiguated by a
continentcode/countrycode/subdivisioncode query filter rather than a bare
flag) were confirmed correctly routed already -- route53 was, like `mgn`
audited in the same pass, essentially clean going in. No query-parameter- or
flag-discriminated pair was found to be *mis*-disambiguated.

`ExtractOperation`, previously covering roughly half of the 71 ops (many
newer families -- CIDR sub-paths, traffic-policy `{Id}/{Version}` vs `{Id}`,
TPInstance updates, info/limit endpoints -- fell through to `"Unknown"` even
though the real HTTP dispatch handled them correctly), was extended to mirror
`routeRequest`'s real dispatch tree op-for-op. This is now backed by
`TestExtractOperation_SDKRouteTable` (`handler_paths_sdk_diff_test.go`, one
subtest per op) -- 71/71 pass, and it is the permanent regression guard for
this sweep rather than a one-off report. No existing test encoded the old
wrong behavior (none tested `GetHealthCheckLastFailureReason` via HTTP at
all), so no test corrections were needed beyond the new file.

Gates: `go build`, `go vet`, `go test -race`, `go fix -diff` (no diff),
`golangci-lint run` (0 findings, after decomposing 3 new `cyclop` violations
and adding op-name constants for 6 new `goconst` violations the extended
`ExtractOperation` introduced) all clean.

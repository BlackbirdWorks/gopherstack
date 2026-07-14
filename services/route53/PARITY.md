---
service: route53
sdk_module: aws-sdk-go-v2/service/route53@v1.62.3
last_audit_commit: ee7d2bae
last_audit_date: 2026-07-12
overall: A          # this pass: closed 3 of the 4 tracked gaps from the prior audit
                    # (~600 LOC genuine fixes + tests) — HealthCheckVersion/CollectionVersion
                    # optimistic concurrency, CidrCollectionInUse non-empty guard, and full
                    # reusable-delegation-set <-> hosted-zone linkage. No local code drift
                    # since the prior audit commit (ce30166a), so this pass targeted only the
                    # `partial` rows the prior ledger flagged; all other `ok` rows trusted
                    # unchanged per the re-audit protocol.
ops:
  CreateHostedZone: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CallerReference reuse with different Name/Comment/PrivateZone now returns HostedZoneAlreadyExists (409) instead of silently returning the wrong zone; fixed this pass: DelegationSetId was parsed off the wire and then silently dropped — every zone got the same hardcoded default name servers regardless of what was requested. Now accepts a reusable delegation set (bare or /delegationset/-prefixed ID), validates it exists (NoSuchDelegationSet), and both the CreateHostedZone/GetHostedZone DelegationSet response element and the zone's auto-seeded NS/SOA records use the linked set's real name servers"}
  DeleteHostedZone: {wire: ok, errors: ok, state: ok, persist: ok}
  GetHostedZone: {wire: ok, errors: ok, state: ok, persist: ok, note: "DelegationSet response element now reflects the zone's actual linked reusable delegation set (Id + NameServers) instead of always the fixed default pair — see CreateHostedZone"}
  ListHostedZones: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHostedZonesByName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHostedZoneComment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetHostedZoneCount: {wire: ok, errors: ok, state: ok, persist: ok}
  ChangeResourceRecordSets: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATE/DELETE/UPSERT, exact-match DELETE validation, all record-type value validators, routing-policy mutual exclusion, batch validated atomically before any mutation applied — see Notes"}
  ListResourceRecordSets: {wire: ok, errors: ok, state: ok, persist: ok, note: "Name/Type/SetIdentifier lexicographic sort + pagination cursors"}
  CountResourceRecordSets: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChange: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CallerReference reuse with a different HealthCheckConfig now returns HealthCheckAlreadyExists (409); fixed: CALCULATED HealthThreshold > len(ChildHealthChecks) now rejected (InvalidInput)"}
  GetHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHealthChecks: {wire: ok, errors: ok, state: ok, persist: ok}
  GetHealthCheckCount: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: HealthCheckVersion was entirely missing from the wire (CreateHealthCheck/GetHealthCheck/ListHealthChecks/UpdateHealthCheck responses never emitted it, even though it's a required field in the real HealthCheck shape). Now every health check carries a Version starting at 1, incremented on each successful update; UpdateHealthCheck's optional request-side HealthCheckVersion is checked for optimistic concurrency and returns HealthCheckVersionMismatch (409) on a stale value"}
  GetHealthCheckStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  GetHealthCheckLastFailureReason: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: no longer silently returns empty tags for a nonexistent hosted zone/health check — now validates existence and returns NoSuchHostedZone/NoSuchHealthCheck (404)"}
  ListTagsForResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2 bugs: (1) HTTP route was unreachable (handler checked a bare /2013-04-01/tags path that can never match; real AWS URI is POST /2013-04-01/tags/{ResourceType}), (2) same missing-existence-check bug as ListTagsForResource"}
  ChangeTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: the handler discarded ChangeTagsForResource's error return (setTags/removeTags used `_ = ...`), so tagging a nonexistent resource silently 200'd instead of 404ing; also fixed: resource tags (b.tags) were never wired into Snapshot/Restore and were lost across a backend restore"}
  CreateKeySigningKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: duplicate name in a zone now returns KeySigningKeyAlreadyExists (409) instead of generic InvalidInput"}
  ActivateKeySigningKey: {wire: ok, errors: ok, state: ok, persist: ok}
  DeactivateKeySigningKey: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteKeySigningKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: deleting an ACTIVE KSK returned a fabricated 'KeySigningKeyNotInactive' code that doesn't exist in the AWS API; now returns the real InvalidKeySigningKeyStatus (400)"}
  EnableHostedZoneDNSSEC: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableHostedZoneDNSSEC: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDNSSEC: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateVPCWithHostedZone: {wire: ok, errors: partial, state: ok, persist: ok, note: "duplicate-VPC error code unverified against real AWS, see gaps"}
  DisassociateVPCFromHostedZone: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: VPC not associated now returns VPCAssociationNotFound (404) instead of generic InvalidInput; LastVPCAssociation guard already correct"}
  ListVPCAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHostedZonesByVPC: {wire: ok, errors: ok, state: ok, persist: ok}
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
  CreateReusableDelegationSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "status fix (prior pass): NoSuchDelegationSet 404 -> 400. The hostedZoneID param remains intentionally unused — real AWS's CreateReusableDelegationSet(HostedZoneId=...) mode creates a new reusable set that reuses an *existing* hosted zone's current name servers, a distinct (and rare) code path from the always-used CallerReference-only mode; not implemented this pass, see gaps. Linkage direction (CreateHostedZone -> reusable delegation set) fixed this pass, see CreateHostedZone/DeleteReusableDelegationSet/CountZonesByReusableDelegationSet"}
  GetReusableDelegationSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReusableDelegationSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: now returns DelegationSetInUse (400) if any hosted zone is still linked to the set, instead of deleting it out from under live zones"}
  ListReusableDelegationSets: {wire: ok, errors: ok, state: ok, persist: ok}
  CountZonesByReusableDelegationSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: previously always returned 0 (hosted zones were never linked to delegation sets at all); now counts real linked zones"}
  TestDNSAnswer: {wire: ok, errors: ok, state: ok, persist: n/a, note: "not re-derived line-by-line against AWS routing-policy selection docs this pass, see deferred"}
  CreateTrafficPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "status fix: TrafficPolicyAlreadyExists 400 -> 409"}
  CreateTrafficPolicyVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTrafficPolicyInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: allowed unlimited duplicate instances for the same (hostedZoneID, name); now returns TrafficPolicyInstanceAlreadyExists (409)"}
  UpdateTrafficPolicyInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTrafficPolicyComment: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTrafficPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTrafficPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTrafficPolicyInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTrafficPolicyInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrafficPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrafficPolicyVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrafficPolicyInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrafficPolicyInstancesByHostedZone: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrafficPolicyInstancesByPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  record_types: {status: ok, note: "A/AAAA/CNAME/MX/TXT/SPF/NS/SOA/PTR/SRV/CAA/DS/NAPTR value-format validators verified against RFC-shaped regexes; HTTPS/SVCB/SSHFP/TLSA intentionally accept any value (no AWS-documented format constraint enforced by the real service either)"}
  routing_policies: {status: ok, note: "Weighted(SetIdentifier+Weight 0-255)/Latency(Region)/Failover(PRIMARY|SECONDARY)/Geolocation/Multivalue/Geoproximity(exactly one of AWSRegion|Coordinates|LocalZoneGroup, Bias -99..99, lat/lon range-checked)/CIDR routing all validated for mutual exclusion and SetIdentifier requirement per AWS rules; selection algorithm (selectAnswer) not re-derived line-by-line this pass, see deferred"}
  dnssec: {status: ok, note: "EnableHostedZoneDNSSEC requires >=1 ACTIVE KSK (KeySigningKeyWithActiveStatusNotFound), KSK lifecycle (create/activate/deactivate/delete) state machine verified"}
  errCodeLookup: {status: ok, note: "every route53 sentinel error's wire code + HTTP status cross-checked this pass against aws-sdk-go-v2/service/route53@v1.62.3 types/errors.go and the botocore api-2.json httpStatusCode field — see fixes in ops table above"}
gaps:
  - AssociateVPCWithHostedZone returns generic InvalidInput for a duplicate VPC association; could not confirm the real AWS behavior (error vs. idempotent no-op) with high confidence this pass either — the real ConflictingDomainExists error shape's documented cause ("the VPC is already associated with *another* hosted zone with the same name") rules it out as the error for this exact same-VPC-same-zone case, which is weak evidence AWS may treat re-association as an idempotent no-op rather than an error, but not strong enough to change behavior without a live-AWS check (bd: gopherstack-8l0.5)
  - CreateReusableDelegationSet's HostedZoneId param (mark an *existing* hosted zone's current delegation set as reusable) is still unimplemented/ignored — only the CallerReference-only "brand new reusable set" mode works. This pass implemented the CreateHostedZone -> reusable-delegation-set linkage in the other direction (DelegationSetId on CreateHostedZone), which was the bigger gap (bd: gopherstack-8l0.3)
deferred:
  - TestDNSAnswer / selectAnswer / collectRoutingCandidates / resolveAlias / multiValueAnswer: routing-policy answer-selection algorithms not re-derived line-by-line against AWS docs this pass (bd: gopherstack-8l0.4)
  - SDK-driven integration tests (test/integration/*_parity_test.go) not run for route53 this pass — this pass's fixes are proven by unit/handler tests only, which parity-principles.md notes is not full parity proof (bd: gopherstack-8l0.4)
  - CreateKeySigningKey does not validate kmsArn as a well-formed KMS ARN (InvalidKMSArn never returned)
  - Alias target cycle/depth handling (rrsValues/resolveAlias `depth` param) not stress-tested against pathological alias chains this pass
leaks: {status: clean, note: "no goroutines, tickers, or background timers anywhere in services/route53 (grep for 'go func|time.After|time.Sleep|Ticker' returns nothing) — all ops are synchronous request/response; Reset()/DeleteHostedZone/DeleteHealthCheck correctly cascade-delete tags/KSKs/VPC-assocs/query-logging-configs so no orphaned map entries accumulate under normal use. b.tags itself was NOT wired into Snapshot/Restore before this pass (fixed) — that was a persistence gap, not a leak, since Reset() already covered it."}
---

## Notes

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

Not fixed: `CreateReusableDelegationSet`'s `HostedZoneId` param (the
opposite-direction "mark an existing zone's current delegation set as
reusable" mode) — confirmed via botocore this is a real, distinct
`CreateReusableDelegationSet` request mode, but out of scope for this pass
given it requires a different code path (extracting an *existing* zone's
current name servers into a new reusable set, rather than assigning an
existing reusable set's name servers to a *new* zone). Tracked as a
follow-up gap. `AssociateVPCWithHostedZone`'s duplicate-VPC error code
remains unverified — this pass found that `ConflictingDomainExists`'s
documented cause is specifically about *another* hosted zone sharing the
same name, not a same-VPC-same-zone re-association, which is suggestive
(AWS may treat this as an idempotent no-op) but not conclusive enough to
change behavior without a live-AWS check.

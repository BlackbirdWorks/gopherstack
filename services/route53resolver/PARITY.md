---
service: route53resolver
sdk_module: aws-sdk-go-v2/service/route53resolver@v1.42.3
last_audit_commit: 22d69640
last_audit_date: 2026-07-12
overall: A            # genuine wire-breaking bugs found and fixed
ops:
  CreateResolverEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResolverEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResolverEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResolverEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades rules + tags + rule associations"}
  UpdateResolverEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResolverEndpointIpAddresses: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateResolverEndpointIpAddress: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateResolverEndpointIpAddress: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateResolverRule: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Tags input field was missing entirely -- silently dropped tags on create; added"}
  GetResolverRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResolverRules: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResolverRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades tags + rule associations"}
  UpdateResolverRule: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateResolverRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResolverRuleAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateResolverRule: {wire: fixed, errors: ok, state: ok, persist: ok, note: "CRITICAL: request shape was ResolverRuleAssociationId (an ID that only ever appears in Get/List responses); real API requires ResolverRuleId+VPCId. Every real SDK client call was rejected with ValidationException before this fix. Backend now looks up the association by (ResolverRuleID, VPCID) pair."}
  ListResolverRuleAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResolverRulePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResolverRulePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateResolverQueryLogConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResolverQueryLogConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResolverQueryLogConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResolverQueryLogConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades tags + associations"}
  AssociateResolverQueryLogConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResolverQueryLogConfigAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateResolverQueryLogConfig: {wire: fixed, errors: ok, state: ok, persist: ok, note: "CRITICAL: same bug class as DisassociateResolverRule -- request shape was ResolverQueryLogConfigAssociationId; real API requires ResolverQueryLogConfigId+ResourceId. Fixed the same way (lookup by pair, decrement AssociationCount on match)."}
  ListResolverQueryLogConfigAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResolverQueryLogConfigPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResolverQueryLogConfigPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFirewallRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFirewallRuleGroup: {wire: fixed, errors: ok, state: ok, persist: ok, note: "OwnerID json tag was \"OwnerID\", real wire key is \"OwnerId\" (smithy-go json decoder does exact-case map[string]interface{} key match, not case-insensitive struct-tag matching -- silently dropped on real clients)"}
  ListFirewallRuleGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFirewallRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades rules + associations + tags"}
  GetFirewallRuleGroupPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutFirewallRuleGroupPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateFirewallRuleGroup: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Tags input field was missing entirely -- added, same fix class as CreateResolverRule"}
  GetFirewallRuleGroupAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFirewallRuleGroupAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateFirewallRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly uses FirewallRuleGroupAssociationId (verified against real Input struct -- this op is NOT the same bug class as DisassociateResolverRule)"}
  UpdateFirewallRuleGroupAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFirewallDomainList: {wire: fixed, errors: ok, state: ok, persist: ok, note: "CreationTime/ModificationTime/StatusMessage were never tracked on FirewallDomainList at all (missing struct fields) -- added and wired through Create/Update/Import"}
  GetFirewallDomainList: {wire: fixed, errors: ok, state: ok, persist: ok, note: "see CreateFirewallDomainList"}
  ListFirewallDomainLists: {wire: ok, errors: ok, state: ok, persist: ok, note: "real API returns the leaner FirewallDomainListMetadata shape for List; we return the full object -- harmless (extra fields are ignored by SDK json decoders), not fixed"}
  DeleteFirewallDomainList: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFirewallDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFirewallDomains: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now bumps ModificationTime"}
  ImportFirewallDomains: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now bumps ModificationTime"}
  CreateFirewallRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "no Tags on this op in the real API -- correctly has none"}
  DeleteFirewallRule: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFirewallRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFirewallRules: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFirewallConfig: {wire: fixed, errors: ok, state: ok, persist: ok, note: "OwnerID -> OwnerId json tag (same bug class, see GetFirewallRuleGroup); AWS correctly returns no Arn for this type (verified, kept as-is)"}
  UpdateFirewallConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFirewallConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateOutpostResolver: {wire: ok, errors: ok, state: ok, persist: ok}
  GetOutpostResolver: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOutpostResolvers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteOutpostResolver: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateOutpostResolver: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResolverConfig: {wire: fixed, errors: ok, state: ok, persist: ok, note: "OwnerID -> OwnerId json tag (same bug class); real type also has no Arn field but our extra Arn field is harmless"}
  UpdateResolverConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResolverConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResolverDnssecConfig: {wire: fixed, errors: ok, state: ok, persist: ok, note: "OwnerID -> OwnerId json tag (same bug class)"}
  UpdateResolverDnssecConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResolverDnssecConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  status_lifecycle: {status: ok, note: "endpoints/rules/groups/configs all transition straight to their terminal state (OPERATIONAL/COMPLETE/CREATED) synchronously -- not a bug: clients never block polling since gopherstack has no async provisioning to simulate, matches LocalStack's general approach for this service"}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend, which uses store.Registry.SnapshotAll/RestoreAll across all 13 store.Table-backed resources plus the 4 plain maps (tags, 3 policy stores); versioned (route53resolverSnapshotVersion) with clean-discard on mismatch"}
gaps:
  - ListFirewallDomainLists returns the full FirewallDomainList shape instead of the leaner FirewallDomainListMetadata (extra fields present in real response are Status/DomainCount/CreationTime/ModificationTime/StatusMessage, none of which real AWS includes in this specific list response) -- harmless to SDK clients (unknown-field-tolerant decoders), left as-is; would need a second output struct to be byte-exact
  - ResolverConfig/FirewallConfig output structs include an `Arn` field that the real API type does not have for ResolverConfig's case it's harmless-extra (types.ResolverConfig actually has no Arn) -- not removed, zero functional impact
deferred:
  - none -- full op surface audited this pass
leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table/plain maps guarded by the single lockmetrics.RWMutex"}
---

## Notes

Protocol: awsjson1.1 (single POST, `X-Amz-Target: Route53Resolver.<Op>`). Route matcher
(`RouteMatcher`/`ExtractOperation`) is a simple prefix match/trim and correctly covers all
70 registered ops -- verified by iterating `GetSupportedOperations()` against the real SDK's
op list, no mismatches.

**Timestamps**: All Route53Resolver `*Time` fields are ISO8601 strings (RFC3339 via
`currentTime()` in backend.go), matching the real SDK's `*string` (not epoch-number) fields.
Confirmed against `aws-sdk-go-v2/service/route53resolver/types` -- every `CreationTime` /
`ModificationTime` field is typed `*string`, not `*time.Time`/epoch. Do not "fix" these to
epoch numbers in a future pass.

**"OwnerID" vs "OwnerId" wire-key bug (bug class, 5 fixes)**: this service's awsjson1.1
deserializer (verified by reading the real SDK's generated `deserializers.go`) decodes the
response body into `map[string]interface{}` and then does an *exact*, case-sensitive
`switch key { case "OwnerId": ... }` -- it does NOT go through `encoding/json`'s
case-insensitive struct-tag matching. A response field literally spelled `"OwnerID"`
(capital D) is a silent no-op on the client: the map key never matches the switch case, so
the SDK's `OwnerId` field is left nil. This is a trap for future auditors here: struct-tag
JSON casing bugs in awsjson1.1 services are NOT automatically forgiven by Go's usual
case-insensitive unmarshal behavior, because the SDK uses a hand-rolled decoder, not
`encoding/json` unmarshal-into-struct. Always grep the real `deserializers.go` for the exact
`case "..."` string, don't assume "close enough" casing is fine. Fixed in:
`firewallRuleGroupOutput.OwnerID`, `resolverQueryLogConfigOutput.OwnerID`,
`firewallConfigOutput.OwnerID`, `resolverConfigOutput.OwnerID`,
`resolverDnssecConfigOutput.OwnerID`, `resolverRuleOutput.OwnerID` (6 structs, all now tag
`json:"OwnerId"` / `json:"OwnerId,omitempty"`).

**Disassociate-by-composite-key bug class (2 fixes, both critical)**: `DisassociateResolverRule`
and `DisassociateResolverQueryLogConfig` do NOT take the opaque association ID that their
sibling `Get*Association`/`List*Associations` ops return. The real API requires the caller to
re-supply the *original* identifying pair instead:
- `DisassociateResolverRule`: `ResolverRuleId` + `VPCId` (verified against
  `api_op_DisassociateResolverRule.go` -- both `This member is required`)
- `DisassociateResolverQueryLogConfig`: `ResolverQueryLogConfigId` + `ResourceId` (verified
  against `api_op_DisassociateResolverQueryLogConfig.go`)

Before this fix, gopherstack's handlers expected `ResolverRuleAssociationId` /
`ResolverQueryLogConfigAssociationId` fields that a real SDK client never sends -- every real
`DisassociateResolverRule`/`DisassociateResolverQueryLogConfig` call from an actual
`aws-sdk-go-v2` client would hit gopherstack's own "field is required" `InvalidRequestException`
100% of the time. Unit tests didn't catch this because they hand-built the (wrong) request body
to match the handler's existing (wrong) expectations -- exactly the "unit tests are not parity
proof" trap called out in parity-principles.md #3. Both backend methods now look up the matching
association by scanning `*ByRegion` index for the (ruleID/configID, vpcID/resourceID) pair,
matching real AWS semantics where the pair is the effective identity of the association.
Note the asymmetry is real, not a typo in this codebase: `DisassociateFirewallRuleGroup` and
`UpdateFirewallRuleGroupAssociation` DO correctly use `FirewallRuleGroupAssociationId` --
verified against the real SDK, this one association type keeps the opaque ID symmetric between
Associate/Get/Update/Disassociate. Don't "fix" it to match the Resolver-rule/query-log pattern.

**Missing Tags on CreateResolverRule / AssociateFirewallRuleGroup (2 fixes)**: both real API
inputs carry a `Tags []types.Tag` field (verified against `api_op_CreateResolverRule.go` and
`api_op_AssociateFirewallRuleGroup.go`); gopherstack's input structs omitted it entirely, so
tags supplied on these two specific calls were silently discarded (never landed in the tags
store, never visible via `ListTagsForResource`). All other Tag-bearing create/associate ops
(`CreateResolverEndpoint`, `CreateFirewallRuleGroup`, `CreateFirewallDomainList`,
`CreateOutpostResolver`, `CreateResolverQueryLogConfig`) already handled this correctly and were
used as the template for the fix.

**FirewallDomainList missing timestamps**: the real `types.FirewallDomainList` has
`CreationTime`/`ModificationTime`/`StatusMessage`, but the backend struct never had storage for
them -- every Get/Create/Delete/Update/Import response silently returned them empty forever
(not a "wrong value" bug, a "field literally never existed" bug). Added the fields, set on
create, bumped on `UpdateFirewallDomains`/`ImportFirewallDomains`.

**Not bugs (verified correct, don't re-flag)**:
- Every Create* op (`CreateResolverEndpoint`, `CreateResolverRule`, `CreateFirewallRuleGroup`,
  etc.) transitions straight to its terminal status (`OPERATIONAL`/`COMPLETE`/`CREATED`)
  instead of an intermediate `CREATING` state. This is the *opposite* of the "stuck CREATING
  forever" anti-pattern the audit brief warns about -- it means clients never need to poll, and
  is intentional/harmless for a synchronous mock backend.
- `resolverConfigOutput`/`firewallConfigOutput` carrying an extra `Arn` field that the real
  API type lacks -- unknown-field-tolerant decoders ignore it, zero functional impact, not
  worth the churn to remove.
- `GetFirewallRuleGroupPolicy`/`GetResolverQueryLogConfigPolicy`/`GetResolverRulePolicy`
  returning `""` for an unset policy rather than erroring -- reasonable mock behavior for a
  void-result-style read, matches the "empty envelope after real backend logic is correct"
  guidance in parity-principles.md #4.

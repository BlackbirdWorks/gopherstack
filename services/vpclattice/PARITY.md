service: vpclattice
sdk_module: aws-sdk-go-v2/service/vpclattice@v1.22.2
last_audit_commit: b9c3a4f3
last_audit_date: 2026-07-23
overall: A            # genuine wire-shape + state-machine fixes found across ~5 op families
ops:
  CreateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "dnsEntry now includes hostedZoneId alongside domainName, matching real DnsEntry shape"}
  GetService: {wire: ok, errors: ok, state: ok, persist: ok, note: "same dnsEntry.hostedZoneId fix as Create"}
  UpdateService: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteService: {wire: ok, errors: ok, state: ok, persist: ok, note: "now rejects with ConflictException while an SNSA references the service, and cascades through listeners/rules/resourcePolicy/authPolicy/accessLogSubscriptions on success, matching the DeleteService doc comment in api_op_DeleteService.go"}
  ListServices: {wire: ok, errors: ok, state: ok, persist: ok, note: "summary dnsEntry now includes hostedZoneId too"}
  CreateServiceNetwork: {wire: ok, errors: ok, state: ok, persist: ok}
  GetServiceNetwork: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateServiceNetwork: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteServiceNetwork: {wire: ok, errors: ok, state: ok, persist: ok, note: "now rejects with ConflictException while any SNSA or SNVA references the service network, and cascades through resourcePolicy/authPolicy/accessLogSubscriptions on success, matching the DeleteServiceNetwork doc comment in api_op_DeleteServiceNetwork.go"}
  ListServiceNetworks: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateServiceNetworkServiceAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "added missing customDomainName/dnsEntry fields; dnsEntry now also carries hostedZoneId"}
  GetServiceNetworkServiceAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as Create"}
  DeleteServiceNetworkServiceAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServiceNetworkServiceAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "summary also missing customDomainName/dnsEntry, now fixed; dnsEntry now also carries hostedZoneId"}
  CreateServiceNetworkVpcAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts and echoes privateDnsEnabled"}
  GetServiceNetworkVpcAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "same privateDnsEnabled fix as Create"}
  UpdateServiceNetworkVpcAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against UpdateServiceNetworkVpcAssociationInput/Output: only securityGroupIds is accepted/echoed by the real op, no privateDnsEnabled param -- correctly left untouched"}
  DeleteServiceNetworkVpcAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServiceNetworkVpcAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "summary now carries privateDnsEnabled too"}
  CreateListener: {wire: ok, errors: ok, state: ok, persist: ok}
  GetListener: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateListener: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteListener: {wire: ok, errors: ok, state: ok, persist: ok}
  ListListeners: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRule: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "RuleSummary was missing createdAt/lastUpdatedAt"}
  BatchUpdateRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "failure entries used code/message instead of failureCode/failureMessage"}
  CreateTargetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "config missing ipAddressType/lambdaEventStructureVersion in response"}
  GetTargetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "same config fix as Create"}
  UpdateTargetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "same config fix as Create"}
  DeleteTargetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "now rejects with ConflictException while any listener rule (including a listener's default action, materialized as its default rule) still forwards to the target group, matching the DeleteTargetGroup doc comment in api_op_DeleteTargetGroup.go"}
  ListTargetGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "summary used vpcId instead of vpcIdentifier wire key; missing lastUpdatedAt/ipAddressType/lambdaEventStructureVersion"}
  RegisterTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "response was missing the successful[] list entirely; TargetFailure used code/message instead of failureCode/failureMessage"}
  DeregisterTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "same two fixes as RegisterTargets"}
  ListTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "TargetSummary reasonCode/status/port/id already correct"}
  CreateAccessLogSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessLogSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAccessLogSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAccessLogSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccessLogSubscriptions: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAuthPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAuthPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns 404 when unset (fixed in a prior audit pass, see parity_a_test.go)"}
  DeleteAuthPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  routing: {status: ok, note: "handleREST (was a ~50-case switch, nolint:gocyclo,cyclop,funlen, gocyclo=57) and classifyPath (was a flat switch, nolint:gocyclo,cyclop,funlen, gocyclo=31) were both decomposed into sync.OnceValue-built lookup tables (op-name -> handler adapter; path-collection -> create/list op + sub-classifier; method -> op for the auth-policy/resource-policy/tags singleton routes), matching the inspector2/apigatewayv2 onceOpTable convention already used elsewhere in the fleet. Both banned nolints are gone; gocyclo/cyclop/funlen all report 0 issues on the package now. Every (method, path, op) triple was preserved verbatim during the refactor -- the full existing routing/handler test suite (handler_test.go, handler_routing coverage via ExtractOperation/ExtractResource, and all handler_*_test.go CRUD tests) passes unchanged, confirming no method/path collisions or unreachable-op regressions were introduced. RouteMatcher is unchanged (still a boolean prefix chain for route eligibility, not a method/path->op mapping, so the same treatment doesn't apply there)."
  timestamps: {status: ok, note: "all createdAt/lastUpdatedAt use time.Time.Format(\"2006-01-02T15:04:05.000Z\") which smithytime.ParseDateTime (restjson1 DateTime shape) accepts; not epoch, correctly ISO-8601."}
gaps:
  - "Resource Gateway / ResourceConfiguration / ServiceNetworkResourceAssociation / ServiceNetworkVpcEndpointAssociation / DomainVerification op families are entirely unimplemented (newer VPC Lattice feature set). Already explicitly acknowledged in sdk_completeness_test.go's notImplemented list — not a silent gap, but still real missing surface. Out of scope for this pass (would blow the ~2000 LOC budget); flag for a dedicated future audit pass if these become load-bearing."
  - "GetServiceOutput/GetServiceNetworkVpcAssociationOutput failureCode/failureMessage fields (populated when a resource is stuck in a *_FAILED state) are never set because this backend's Create paths are synchronous and never fail after validation — acceptable since there's no in-progress/failed state machine to represent, but worth knowing if async failure simulation is ever added."
  - "ServiceNetworkVpcAssociation is still missing the full DnsOptions substructure (PrivateDnsPreference enum + PrivateDnsSpecifiedDomains list) — privateDnsEnabled itself is now fixed (see CreateServiceNetworkVpcAssociation/GetServiceNetworkVpcAssociation/ListServiceNetworkVpcAssociations notes above), but the richer DnsOptions object it gates (added after this backend was authored) is deferred. Not exercised by common test/client flows."
  - "PutAuthPolicy/PutResourcePolicy/CreateAccessLogSubscription's resourceID/resourceArn key into their maps by the exact literal string the caller passed (ID or ARN, un-normalized) except CreateAccessLogSubscription, which does resolve to a canonical ARN via resolveResourceARN. A caller that Puts an auth/resource policy using a service's ID and later deletes the service (whose new cascade-delete logic matches by ARN) will leave that policy orphaned in the map, since the two never shared a canonical key. Pre-existing gap (not introduced this pass); DeleteService/DeleteServiceNetwork's cascade is only guaranteed complete for policies set via the resource's ARN, which is the form real AWS clients conventionally use. Flag for a follow-up pass that normalizes PutAuthPolicy/PutResourcePolicy identifiers the same way CreateAccessLogSubscription already does."
deferred:
  - "Resource Gateway / ResourceConfiguration family (see gaps)"
leaks: {status: clean, note: "no goroutines/timers/background workers in this backend; Reset()/Snapshot()/Restore() all take the single lockmetrics.RWMutex and touch only in-memory maps/store.Table instances. No janitor loop to check. DeleteService/DeleteServiceNetwork now also cascade-delete their dependent listeners/rules/resourcePolicy/authPolicy/accessLogSubscriptions/tags instead of leaving ghost rows behind (previously: only tags were cleaned up on these two deletes; DeleteListener/DeleteTargetGroup already cascaded correctly and are unchanged)."

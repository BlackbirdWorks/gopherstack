service: vpclattice
sdk_module: aws-sdk-go-v2/service/vpclattice@v1.22.2
last_audit_commit: 6642a73c
last_audit_date: 2026-07-12
overall: A            # genuine wire-shape fixes found across ~7 op families
ops:
  CreateService: {wire: ok, errors: ok, state: ok, persist: ok}
  GetService: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateService: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteService: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServices: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateServiceNetwork: {wire: ok, errors: ok, state: ok, persist: ok}
  GetServiceNetwork: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateServiceNetwork: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteServiceNetwork: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServiceNetworks: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateServiceNetworkServiceAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "added missing customDomainName/dnsEntry fields"}
  GetServiceNetworkServiceAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as Create"}
  DeleteServiceNetworkServiceAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServiceNetworkServiceAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "summary also missing customDomainName/dnsEntry, now fixed"}
  CreateServiceNetworkVpcAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetServiceNetworkVpcAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateServiceNetworkVpcAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteServiceNetworkVpcAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServiceNetworkVpcAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
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
  DeleteTargetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
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
  routing: {status: ok, note: "RouteMatcher/classifyPath verified path-by-path against serializers.go in aws-sdk-go-v2/service/vpclattice@v1.22.2 for all 50 routed ops incl. registertargets/deregistertargets/listtargets sub-paths (all POST, all correctly matched by prefix). No method/path collisions found; no unreachable-op bugs (the class that hit backup/eks/s3control/etc)."
  timestamps: {status: ok, note: "all createdAt/lastUpdatedAt use time.Time.Format(\"2006-01-02T15:04:05.000Z\") which smithytime.ParseDateTime (restjson1 DateTime shape) accepts; not epoch, correctly ISO-8601."}
gaps:
  - "Resource Gateway / ResourceConfiguration / ServiceNetworkResourceAssociation / ServiceNetworkVpcEndpointAssociation / DomainVerification op families are entirely unimplemented (newer VPC Lattice feature set). Already explicitly acknowledged in sdk_completeness_test.go's notImplemented list — not a silent gap, but still real missing surface. Out of scope for this pass (would blow the ~2000 LOC budget); flag for a dedicated future audit pass if these become load-bearing."
  - "Service/SNSA/SNVA dnsEntry object omits hostedZoneId (real API returns {domainName, hostedZoneId}); only domainName is populated. Low priority since gopherstack has no Route53 hosted-zone concept to source a realistic value from."
  - "GetServiceOutput/GetServiceNetworkVpcAssociationOutput failureCode/failureMessage fields (populated when a resource is stuck in a *_FAILED state) are never set because this backend's Create paths are synchronous and never fail after validation — acceptable since there's no in-progress/failed state machine to represent, but worth knowing if async failure simulation is ever added."
  - "ServiceNetworkVpcAssociation is missing newer real-API fields dnsOptions, privateDnsEnabled, failureCode, failureMessage (private-DNS support, added after this backend was authored). Deferred — not exercised by common test/client flows."
  - "DeleteService/DeleteServiceNetwork/DeleteTargetGroup do not reject deletion when dependent resources exist (e.g. deleting a service with active SNSAs, or a service network with active associations). Real AWS returns a Conflict-style error in some of these cases. Not fixed this pass (behavior-changing, would need new error paths + tests); flag for follow-up bd issue if this proves to matter for negative-path test coverage."
deferred:
  - "Resource Gateway / ResourceConfiguration family (see gaps)"
leaks: {status: clean, note: "no goroutines/timers/background workers in this backend; Reset()/Snapshot()/Restore() all take the single lockmetrics.RWMutex and touch only in-memory maps/store.Table instances. No janitor loop to check."}

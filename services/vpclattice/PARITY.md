service: vpclattice
sdk_module: aws-sdk-go-v2/service/vpclattice@v1.25.5
last_audit_commit: 198990e82
last_audit_date: 2026-08-07
overall: A            # gopherstack-lx2k: Resource Gateway/ResourceConfiguration/
                      # ServiceNetworkResourceAssociation/DomainVerification families
                      # (20 SDK ops) implemented for real against v1.25.5; PutAuthPolicy/
                      # PutResourcePolicy ARN-normalization orphan bug fixed; SNVA DnsOptions
                      # field (previously silently dropped) now round-trips. See ops/gaps below.
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
  CreateServiceNetworkVpcAssociation: {wire: fixed, errors: ok, state: ok, persist: ok, note: "accepts and echoes privateDnsEnabled; FIXED this pass (gopherstack-lx2k) -- dnsOptions (privateDnsPreference + privateDnsSpecifiedDomains) was accepted on the wire and silently discarded, now stored and echoed back"}
  GetServiceNetworkVpcAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "same privateDnsEnabled fix as Create; dnsOptions now round-trips too"}
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
  PutAuthPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED this pass (gopherstack-lx2k) -- now normalizes resourceID (ID or ARN) to the resource's canonical ARN via resolvePolicyResourceARN before keying authPolicies, matching how DeleteService/DeleteServiceNetwork's cascade delete looks entries up. Previously a Put using the short ID left the entry permanently orphaned once the parent resource was deleted by ARN."}
  GetAuthPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "returns 404 when unset (fixed in a prior audit pass, see parity_a_test.go); now shares the same ARN normalization as PutAuthPolicy"}
  DeleteAuthPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same ARN normalization fix"}
  PutResourcePolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same orphan fix as PutAuthPolicy"}
  GetResourcePolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same ARN normalization fix"}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same ARN normalization fix"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateResourceGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass (gopherstack-lx2k). Note the real API's field-name inconsistency preserved verbatim: Create/Summary echo vpcIdentifier, but Get/Update/Delete echo vpcId (verified against api_op_*ResourceGateway.go/types.ResourceGatewaySummary directly, not assumed)"}
  GetResourceGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  UpdateResourceGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass -- only securityGroupIds is accepted, matching UpdateResourceGatewayInput"}
  DeleteResourceGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass -- rejects with ConflictException while any resource configuration still references the gateway"}
  ListResourceGateways: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  CreateResourceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass -- resourceConfigurationDefinition union (arnResource/dnsResource/ipResource) round-trips; CHILD type inherits ResourceGatewayId from its GROUP parent per the real API's documented behavior"}
  GetResourceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  UpdateResourceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass -- allowAssociationToShareableServiceNetwork/portRanges/resourceConfigurationDefinition, matching UpdateResourceConfigurationInput"}
  DeleteResourceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass -- rejects with ConflictException while any SNRA or CHILD configuration references it"}
  ListResourceConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  CreateServiceNetworkResourceAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass -- both DeleteServiceNetwork and DeleteResourceConfiguration now also check for a referencing SNRA before allowing delete"}
  GetServiceNetworkResourceAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  DeleteServiceNetworkResourceAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  ListServiceNetworkResourceAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  StartDomainVerification: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass -- status is always PENDING and never advances to VERIFIED, see gaps: this backend has no DNS to observe a caller-provisioned TXT record with, and fabricating VERIFIED would be worse than honestly staying PENDING"}
  GetDomainVerification: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  DeleteDomainVerification: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  ListDomainVerifications: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  ListResourceEndpointAssociations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NEW this pass -- always returns an empty page, see gaps: this resource is populated in real AWS exclusively by EC2 CreateVpcEndpoint (VPC endpoint type Resource), which this backend doesn't model; vpc-lattice itself has no Create op for it"}
  DeleteResourceEndpointAssociation: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NEW this pass -- always ResourceNotFoundException, see ListResourceEndpointAssociations note"}
  ListServiceNetworkVpcEndpointAssociations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NEW this pass -- always returns an empty page, same structural note as ListResourceEndpointAssociations (populated via EC2 CreateVpcEndpoint of type ServiceNetwork)"}
families:
  routing: {status: ok, note: "handleREST (was a ~50-case switch, nolint:gocyclo,cyclop,funlen, gocyclo=57) and classifyPath (was a flat switch, nolint:gocyclo,cyclop,funlen, gocyclo=31) were both decomposed into sync.OnceValue-built lookup tables (op-name -> handler adapter; path-collection -> create/list op + sub-classifier; method -> op for the auth-policy/resource-policy/tags singleton routes), matching the inspector2/apigatewayv2 onceOpTable convention already used elsewhere in the fleet. Both banned nolints are gone; gocyclo/cyclop/funlen all report 0 issues on the package now. Every (method, path, op) triple was preserved verbatim during the refactor -- the full existing routing/handler test suite (handler_test.go, handler_routing coverage via ExtractOperation/ExtractResource, and all handler_*_test.go CRUD tests) passes unchanged, confirming no method/path collisions or unreachable-op regressions were introduced. RouteMatcher is unchanged (still a boolean prefix chain for route eligibility, not a method/path->op mapping, so the same treatment doesn't apply there)."
  timestamps: {status: ok, note: "all createdAt/lastUpdatedAt use time.Time.Format(\"2006-01-02T15:04:05.000Z\") which smithytime.ParseDateTime (restjson1 DateTime shape) accepts; not epoch, correctly ISO-8601."}
gaps:
  - "GetServiceOutput/GetServiceNetworkVpcAssociationOutput failureCode/failureMessage fields (populated when a resource is stuck in a *_FAILED state) are never set because this backend's Create paths are synchronous and never fail after validation — acceptable since there's no in-progress/failed state machine to represent, but worth knowing if async failure simulation is ever added."
  - "ResourceEndpointAssociation and ServiceNetworkVpcEndpointAssociation lists are always empty (bd: gopherstack-lx2k). Both are populated in real AWS exclusively by EC2 CreateVpcEndpoint (VPC endpoints of type Resource/ServiceNetwork referencing a ResourceConfiguration/ServiceNetwork ARN) — vpc-lattice itself exposes no Create operation for either, and this backend has no EC2 VPC-endpoint cross-service integration to source one from. Buildable with enough cross-service work (not structural), just out of scope this pass; the wire shape and empty-vs-error behavior is honest (List returns real empty, Delete honestly 404s) rather than fabricated."
  - "DomainVerification.Status can never advance past PENDING to VERIFIED (bd: gopherstack-lx2k). Real AWS polls public DNS for a caller-provisioned TXT record; this backend has no DNS to observe. Deliberately left PENDING rather than fabricating VERIFIED — a caller relying on verification completing will need to poll forever, which is the honest reflection of what this mock can and can't do."
  - "GetResourceGateway/UpdateResourceGateway/DeleteResourceGateway's ManagedBy/ServiceManaged fields (set when a resource gateway is provisioned by another AWS service, not directly by the caller) are never populated -- this backend has no cross-service provisioning path that would ever set them, so every resource gateway here is caller-managed. Not fabricated, just never non-default."
leaks: {status: clean, note: "no goroutines/timers/background workers in this backend; Reset()/Snapshot()/Restore() all take the single lockmetrics.RWMutex and touch only in-memory maps/store.Table instances. No janitor loop to check. DeleteService/DeleteServiceNetwork now also cascade-delete their dependent listeners/rules/resourcePolicy/authPolicy/accessLogSubscriptions/tags instead of leaving ghost rows behind (previously: only tags were cleaned up on these two deletes; DeleteListener/DeleteTargetGroup already cascaded correctly and are unchanged)."

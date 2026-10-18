---
service: networkfirewall
sdk_module: aws-sdk-go-v2/service/networkfirewall@v1.76.1
last_audit_commit: 8e8351102
last_audit_date: 2026-10-17
overall: B            # B: 37 of 85 SDK ops implemented against the real client (wire shapes cross-checked with the SDK response snapshots); proxy, VPC endpoint association, availability zone, transit gateway, flow and analysis ops remain open.
ops:
  CreateFirewall: {wire: ok, errors: ok, state: ok, persist: ok, note: "VPC firewalls only; PROVISIONING for the transition window then READY/IN_SYNC; one subnet per zone; subnets and VPC checked against EC2 when wired"}
  DescribeFirewall: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns Firewall, FirewallStatus (SyncStates keyed by zone, Config keyed by policy and rule group ARNs) and UpdateToken"}
  DescribeFirewallMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "SupportedAvailabilityZones and TransitGatewayAttachmentId are transit-gateway-only; see items_still_open"}
  ListFirewalls: {wire: ok, errors: ok, state: ok, persist: ok, note: "VpcIds filter; opaque NextToken via pkgs/page"}
  DeleteFirewall: {wire: ok, errors: ok, state: ok, persist: ok, note: "DeleteProtection returns InvalidOperationException; otherwise DELETING until the window passes, then removed"}
  UpdateFirewallDescription: {wire: ok, errors: ok, state: ok, persist: ok, note: "UpdateToken optional; stale token is InvalidTokenException; token rotates on every change"}
  UpdateFirewallDeleteProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSubnetChangeProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFirewallPolicyChangeProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFirewallEncryptionConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "KMS key is not validated against the kms service"}
  AssociateFirewallPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "blocked by FirewallPolicyChangeProtection; policy must exist; sync goes PENDING then IN_SYNC"}
  AssociateSubnets: {wire: ok, errors: ok, state: ok, persist: ok, note: "blocked by SubnetChangeProtection; duplicate subnet or zone is InvalidRequestException; firewall returns to PROVISIONING"}
  DisassociateSubnets: {wire: ok, errors: ok, state: ok, persist: ok, note: "blocked by SubnetChangeProtection; last subnet cannot be removed"}
  CreateFirewallPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "stateless/stateful references must exist and match type; DryRun validates only; consumed capacity sums referenced group capacities"}
  DescribeFirewallPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFirewallPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFirewallPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "UpdateToken required; DryRun supported"}
  DeleteFirewallPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidOperationException while any firewall uses it; also drops its resource policy"}
  CreateRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "STATELESS/STATEFUL; Rules string or RuleGroup; capacity 1-30000; consumed capacity counts rules, domain targets and stateless rules (1 each)"}
  DescribeRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "ARN, or Name+Type; AnalyzeRuleGroup accepted, no AnalysisResults produced"}
  DescribeRuleGroupMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "ListingName, ProductId and VendorName are managed-group fields and never set"}
  ListRuleGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "Type filter; Scope MANAGED and ManagedType return nothing because no managed groups exist"}
  UpdateRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "UpdateToken required; capacity fixed at creation; DryRun supported"}
  DeleteRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidOperationException while any policy references it"}
  CreateTLSInspectionConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "certificates are echoed from the request, not validated against ACM"}
  DescribeTLSInspectionConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTLSInspectionConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTLSInspectionConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "UpdateToken required"}
  DeleteTLSInspectionConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidOperationException while a policy references it"}
  DescribeLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "destination keys validated per type; more than one destination change per call is InvalidRequestException"}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "policy and rule group ARNs only; document must be JSON with a Statement"}
  DescribeResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "firewalls, policies, rule groups and TLS configurations; 50-tag limit"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Firewall: {status: ok, note: "Create/Describe/List/Delete plus every update op verified through the real aws-sdk-go-v2 client (awsjson1.0, NetworkFirewall_20201112 target). Lifecycle is lazy deadlines evaluated on read (PROVISIONING to READY, DELETING to gone); the window is SetTransitionDelay (500ms default)."}
  FirewallPolicy: {status: ok, note: "Capacity and association counts are computed from live rule group and firewall state."}
  RuleGroup: {status: ok, note: "Suricata rule text is not parsed; capacity is the line count."}
  TLSInspectionConfiguration: {status: ok, note: "Stored and referenced by policies; no certificate handling."}
  Logging: {status: ok, note: "One destination change per call, as AWS enforces; no delivery to S3/CloudWatch/Firehose."}
  ResourcePolicy: {status: ok, note: "Stored per policy or rule group ARN; not evaluated for cross-account sharing."}
  Tags: {status: ok, note: "Shared by all four taggable resource kinds."}
gaps: []
items_still_open:
  - Proxy ops (CreateProxy, CreateProxyConfiguration, CreateProxyRuleGroup, CreateProxyRules, DescribeProxy*, UpdateProxy*, DeleteProxy*, ListProxies, ListProxyConfigurations, ListProxyRuleGroups, Attach/DetachRuleGroups*): not implemented (bd: gopherstack-cefu7.4)
  - VPC endpoint association ops (Create/Describe/Delete/ListVpcEndpointAssociations): not implemented (bd: gopherstack-cefu7.4)
  - Availability zone and transit gateway ops (Associate/DisassociateAvailabilityZones, UpdateAvailabilityZoneChangeProtection, Accept/Reject/DeleteNetworkFirewallTransitGatewayAttachment) and CreateFirewall with TransitGatewayId, AvailabilityZoneMappings, NatGatewayMappings, ProxySettings or VpcEndpoint (UnsupportedOperationException): not implemented (bd: gopherstack-cefu7.4)
  - Flow ops (StartFlowCapture, StartFlowFlush, DescribeFlowOperation, ListFlowOperations, ListFlowOperationResults) and analysis ops (StartAnalysisReport, ListAnalysisReports, GetAnalysisReportResults, UpdateFirewallAnalysisSettings): not implemented (bd: gopherstack-cefu7.4)
  - Container association ops and DescribeRuleGroupSummary: not implemented (bd: gopherstack-cefu7.4)
  - FirewallStatus.CapacityUsageSummary and NatGatewayAttachments never populated (bd: gopherstack-cefu7.4)
  - Firewall endpoints are generated vpce IDs; no EC2 VPC endpoint is created or removed (bd: gopherstack-cefu7.4)
  - No --lifecycle-networkfirewall flag; the transition window is only settable in code (bd: gopherstack-cefu7.4)
structural_gaps:
  - "Rule evaluation: no packets flow, so stateless/stateful rules, TLS inspection and logging never act on traffic."
  - "AnalysisResults on DescribeRuleGroup need a rule-analysis engine; none exists."
  - "Managed rule groups (AWS_MANAGED_*, partner) have no catalog to list."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak; transitions are lazy deadlines evaluated on read, no goroutines or timers."}
---

## Notes

Protocol is awsjson1.0 with PascalCase members and epoch-seconds timestamps; error bodies carry both `message` and `Message`. Firewalls are regional; other regions are served by lazily built sibling backends (pkgs/regionpeers) and ride in the snapshot's `regions` key. Rule groups are unique per (type, name): a STATEFUL and a STATELESS group may share a name. Update tokens rotate on every change; firewall updates treat an omitted token as unconditional, policy, rule group and TLS updates require it.

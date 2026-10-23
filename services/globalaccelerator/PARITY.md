---
service: globalaccelerator
sdk_module: aws-sdk-go-v2/service/globalaccelerator@v1.45.3
last_audit_commit: 0f1a634b0
last_audit_date: 2026-10-17
overall: B            # all 56 SDK operations are backed by real state and verified through the real SDK client in-package; B until the Docker integration test (test/integration/globalaccelerator_test.go) has run in CI and the open items below close.
ops:
  AddCustomRoutingEndpoints: {wire: ok, errors: ok, state: ok, persist: ok, note: "subnet CIDR resolved through EC2 (SubnetResolver); listener port capacity enforced"}
  AddEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  AdvertiseByoipCidr: {wire: ok, errors: ok, state: ok, persist: ok, note: "READY -> PENDING_ADVERTISING -> ADVERTISING"}
  AllowCustomRoutingTraffic: {wire: ok, errors: ok, state: ok, persist: ok, note: "ordered allow/deny rules per subnet endpoint; default state DENY"}
  CreateAccelerator: {wire: ok, errors: ok, state: ok, persist: ok, note: "IN_PROGRESS until the lifecycle dwell (500ms default) elapses, then DEPLOYED; two pool IPv4 addresses (75.2.0.0/16, first free), DUAL_STACK adds two IPv6 (2600:9000:a000::/48); BYOIP IpAddresses accepted from READY/ADVERTISING ranges; IdempotencyToken replays"}
  CreateCrossAccountAttachment: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomRoutingAccelerator: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomRoutingEndpointGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomRoutingListener: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEndpointGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "one group per Region per listener; defaults interval 30, threshold 3, dial 100, protocol TCP, path /; endpoint HealthState INITIAL then HEALTHY after the dwell (modeled, no probes)"}
  CreateListener: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAccelerator: {wire: ok, errors: ok, state: ok, persist: ok, note: "AcceleratorNotDisabledException while enabled, AssociatedListenerFoundException while listeners exist"}
  DeleteCrossAccountAttachment: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while an endpoint references the attachment (emulator choice, SDK does not document the code)"}
  DeleteCustomRoutingAccelerator: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCustomRoutingEndpointGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCustomRoutingListener: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEndpointGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteListener: {wire: ok, errors: ok, state: ok, persist: ok, note: "AssociatedEndpointGroupFoundException while endpoint groups exist"}
  DenyCustomRoutingTraffic: {wire: ok, errors: ok, state: ok, persist: ok}
  DeprovisionByoipCidr: {wire: ok, errors: ok, state: ok, persist: ok, note: "READY -> PENDING_DEPROVISIONING -> DEPROVISIONED; refused while accelerator IPs use the range"}
  DescribeAccelerator: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAcceleratorAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCrossAccountAttachment: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCustomRoutingAccelerator: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCustomRoutingAcceleratorAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCustomRoutingEndpointGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCustomRoutingListener: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEndpointGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeListener: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccelerators: {wire: ok, errors: ok, state: ok, persist: ok}
  ListByoipCidrs: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCrossAccountAttachments: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCrossAccountResourceAccounts: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCrossAccountResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "AcceleratorArn narrows to resources the accelerator actually uses"}
  ListCustomRoutingAccelerators: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCustomRoutingEndpointGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCustomRoutingListeners: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCustomRoutingPortMappings: {wire: ok, errors: ok, state: ok, persist: ok, note: "derived on demand from listener ports, subnet hosts and destination ports; offset NextToken"}
  ListCustomRoutingPortMappingsByDestination: {wire: ok, errors: ok, state: ok, persist: ok, note: "same derivation, filtered by subnet and destination address"}
  ListEndpointGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ListListeners: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ProvisionByoipCidr: {wire: ok, errors: ok, state: ok, persist: ok, note: "PENDING_PROVISIONING -> READY; authorization context is checked for presence only"}
  RemoveCustomRoutingEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAccelerator: {wire: ok, errors: ok, state: ok, persist: ok, note: "IPV4<->DUAL_STACK allocates/releases the IPv6 set and appends an Events entry; status returns to IN_PROGRESS"}
  UpdateAcceleratorAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "flow logs need bucket and prefix"}
  UpdateCrossAccountAttachment: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCustomRoutingAccelerator: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCustomRoutingAcceleratorAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCustomRoutingListener: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEndpointGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateListener: {wire: ok, errors: ok, state: ok, persist: ok}
  WithdrawByoipCidr: {wire: ok, errors: ok, state: ok, persist: ok, note: "ADVERTISING -> PENDING_WITHDRAWING -> READY"}
families:
  Accelerator: {status: ok, note: "Standard and custom routing accelerators share one table (Custom flag); a custom routing accelerator is invisible to the standard calls and vice versa (AcceleratorNotFoundException)."}
  Listener: {status: ok, note: "Port ranges validated (1-65535, no self overlap, no overlap with a same-protocol sibling listener, max 10); custom routing listeners carry no protocol or affinity."}
  EndpointGroup: {status: ok, note: "EndpointGroupAlreadyExistsException per Region; UpdateEndpointGroup replaces members only when EndpointConfigurations is present (empty list clears)."}
  CustomRouting: {status: ok, note: "Port mappings are computed, not stored: listener ports are consumed in order across groups, endpoints, subnet hosts (network, three reserved and broadcast addresses excluded) and destination ports."}
  CrossAccountAttachment: {status: ok, note: "Owner is the caller account; shared-with is matched on exact principal account IDs."}
  Byoip: {status: ok, note: "Transitional states are lazy deadlines evaluated on read, no goroutines."}
  CloudFormation: {status: ok, note: "AWS::GlobalAccelerator::Accelerator, Listener, EndpointGroup and CrossAccountAttachment provision, update in place and delete through the backend; Ref is the ARN and GetAtt covers AcceleratorArn, DnsName, DualStackDnsName, Ipv4Addresses, Ipv6Addresses, ListenerArn, EndpointGroupArn, AttachmentArn (list attributes are comma-joined)."}
  Tags: {status: ok, note: "Accelerators (either kind) and attachments are taggable; listener and endpoint group ARNs are rejected with InvalidArgumentException."}
gaps: []
items_still_open:
  - AccessDeniedException: never raised; no IAM or BYOIP-ownership authorization is modeled
  - TransactionInProgressException: never raised; changes are not gated on an IN_PROGRESS accelerator
  - IpSets.IpAddressDetails: NetworkZone is not emitted, only IpAddresses and IpAddressFamily
  - CreateCrossAccountAttachment: Organizations OU/organization ARN principals are accepted but not expanded to member accounts
structural_gaps:
  - "No data plane: nothing listens on the anycast addresses or DnsName, custom routing port mappings route no packets, and endpoint health is modeled (INITIAL then HEALTHY after the dwell) because no health checker or real endpoint exists."
  - "BYOIP authorization context is not cryptographically verified and FAILED_* states are never entered: there is no RIR/ROA source to fail against."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; transitions are lazy deadlines read under the backend lock, no goroutines or timers."}
---

## Notes

Protocol is awsjson1.1 with target prefix `GlobalAccelerator_V20180706.`; timestamps are epoch-seconds numbers (`awstime.Epoch`). The service is global and homed in us-west-2: ARNs have no region segment (`arn:aws:globalaccelerator::<account>:accelerator/<uuid>`) and there is one backend, not per-region siblings. Shapes and the exception set were read from the pinned SDK schemas and types, not from handler output.

Emulator choices the SDK does not document, kept here so a later audit can re-check them against AWS:

- Standard endpoints are looked up when added: ELBv2 load balancer ARNs in the ARN's region, EC2 instance IDs and EIP allocation IDs in any region; a miss is InvalidArgumentException (the SDK documents no dedicated code). Cross-account (attachment) endpoints and non-ELB ARNs are not checked. A standalone backend without a resolver accepts any well-formed ID.
- Accelerators (either kind) and attachments with tags are listed by the Resource Groups Tagging API under `globalaccelerator:accelerator` and `globalaccelerator:attachment`.
- Static addresses come from a deterministic first-free pool: IPv4 `75.2.0.0/16` (octets .0 and .255 skipped) and IPv6 `2600:9000:a000::/48`. Released addresses are reused immediately.
- Custom routing traffic starts DENY for every destination until AllowCustomRoutingTraffic.
- The IN_PROGRESS window follows `GLOBALACCELERATOR_LIFECYCLE_DELAY` (default 500ms) and restarts after any listener or endpoint group change.
- Accelerator Events text for IP address type changes is the emulator's wording.
- Quotas enforced: 20 accelerators per kind, 10 listeners per accelerator, 10 port ranges per listener, 10 endpoints per group, 50 tags, 20 attachments.
- Custom routing endpoints resolve the subnet CIDR through the EC2 backend (`internal/app/subnet_wiring.go`); a standalone backend without a resolver accepts any `subnet-` ID but maps no sockets.
- ListCrossAccountResources with AcceleratorArn returns only the resources that accelerator already uses; the SDK doc does not say which way the filter points.

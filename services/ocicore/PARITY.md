---
service: ocicore
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: 9a6a36af7
last_audit_date: 2026-10-26
overall: B
ops:
  CreateVcn: {wire: ok, errors: partial, state: ok, persist: ok, note: "Default route table, security list and DHCP options are created with it and cannot be deleted; cidrBlocks /16 to /30 (up to 16, no overlap, no 169.254/16 or multicast/reserved space); dnsLabel gives vcnDomainName <label>.oraclevcn.com; isIpv6Enabled allocates a /56 and adds the documented IPv6 default rules; 50 VCNs per region. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetVcn: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListVcns: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateVcn: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteVcn: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the VCN is referenced (see families.delete_in_use)."}
  ChangeVcnCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreateSubnet: {wire: ok, errors: partial, state: ok, persist: ok, note: "One IPv4 CIDR /16 to /30 inside a VCN CIDR with no overlap among live subnets; regional or AD-specific (availabilityDomain checked for shape only); dnsLabel needs a VCN dnsLabel and is unique per VCN; prohibitPublicIpOnVnic implies prohibitInternetIngress; defaults to the VCN default route table, DHCP options and security list; at most 5 security lists; optional /64 IPv6 block. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetSubnet: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListSubnets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateSubnet: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteSubnet: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the subnet is referenced (see families.delete_in_use)."}
  ChangeSubnetCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreateInternetGateway: {wire: ok, errors: partial, state: ok, persist: ok, note: "IsEnabled required; 1 per VCN (400 LimitExceeded); optional gateway route table. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetInternetGateway: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListInternetGateways: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateInternetGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteInternetGateway: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the internet gateway is referenced (see families.delete_in_use)."}
  ChangeInternetGatewayCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreateNatGateway: {wire: ok, errors: partial, state: ok, persist: ok, note: "Allocates an ephemeral public IP (natIp, publicIpId) or takes an unassigned reserved one; blockTraffic; 1 per VCN; releasing returns the reserved IP to AVAILABLE. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetNatGateway: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListNatGateways: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateNatGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteNatGateway: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the NAT gateway is referenced (see families.delete_in_use)."}
  ChangeNatGatewayCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreateServiceGateway: {wire: ok, errors: partial, state: ok, persist: ok, note: "Services from ListServices; the all-services entry cannot be combined with others; 1 per VCN; services can be replaced by update. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetServiceGateway: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListServiceGateways: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateServiceGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteServiceGateway: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the service gateway is referenced (see families.delete_in_use)."}
  ChangeServiceGatewayCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreateRouteTable: {wire: ok, errors: partial, state: ok, persist: ok, note: "Rules validated: destination CIDR or service CIDR label, target IGW/NAT/LPG/service gateway in the same VCN (DRG and private IP targets are accepted by OCID shape only), SERVICE_CIDR_BLOCK only with a service gateway that has the service, no duplicate destinations, 200 rules. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetRouteTable: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListRouteTables: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateRouteTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteRouteTable: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the route table is referenced (see families.delete_in_use)."}
  ChangeRouteTableCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreateSecurityList: {wire: ok, errors: partial, state: ok, persist: ok, note: "Ingress and egress rules validated (protocol, protocol options, CIDR or service source); 200 rules per direction. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetSecurityList: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListSecurityLists: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateSecurityList: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteSecurityList: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the security list is referenced (see families.delete_in_use)."}
  ChangeSecurityListCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreateNetworkSecurityGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "1000 per VCN. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetNetworkSecurityGroup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListNetworkSecurityGroups: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateNetworkSecurityGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteNetworkSecurityGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the network security group is referenced (see families.delete_in_use)."}
  ChangeNetworkSecurityGroupCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreateDhcpOptions: {wire: ok, errors: partial, state: ok, persist: ok, note: "DomainNameServer (VcnLocal, VcnLocalPlusInternet, CustomDnsServer with 1 to 3 IPv4 servers) and one SearchDomain option; one of each type. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetDhcpOptions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListDhcpOptions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateDhcpOptions: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteDhcpOptions: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the DHCP options is referenced (see families.delete_in_use)."}
  ChangeDhcpOptionsCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreateLocalPeeringGateway: {wire: ok, errors: partial, state: ok, persist: ok, note: "10 per VCN; starts NEW. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetLocalPeeringGateway: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListLocalPeeringGateways: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateLocalPeeringGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteLocalPeeringGateway: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the local peering gateway is referenced (see families.delete_in_use)."}
  ChangeLocalPeeringGatewayCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreatePublicIp: {wire: ok, errors: partial, state: ok, persist: ok, note: "Reserved addresses from 129.213.0.0/16 (50 per region); lifetime and scope filters on list. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetPublicIp: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListPublicIps: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdatePublicIp: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeletePublicIp: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the public IP is referenced (see families.delete_in_use)."}
  ChangePublicIpCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  ListServices: {wire: ok, errors: partial, state: ok, persist: ok, note: "Paged static catalogue: All IAD Services In Oracle Services Network and OCI IAD Object Storage."}
  GetService: {wire: ok, errors: partial, state: ok, persist: ok, note: "By id; 404 NotAuthorizedOrNotFound otherwise."}
  AttachServiceId: {wire: ok, errors: partial, state: ok, persist: ok, note: "Adds a service; 409 Conflict when already attached."}
  DetachServiceId: {wire: ok, errors: partial, state: ok, persist: ok, note: "Removes a service; refused with 409 while a route rule still targets it."}
  ConnectLocalPeeringGateways: {wire: ok, errors: partial, state: ok, persist: ok, note: "Both gateways PEERED with peerId and advertised CIDRs; rejects the same VCN, overlapping CIDRs and an already peered gateway; deleting one side leaves the other REVOKED."}
  AddNetworkSecurityGroupSecurityRules: {wire: ok, errors: partial, state: ok, persist: ok, note: "All-or-nothing; ingress needs source, egress destination; NETWORK_SECURITY_GROUP endpoints must be in the same VCN; 120 rules per NSG."}
  UpdateNetworkSecurityGroupSecurityRules: {wire: ok, errors: partial, state: ok, persist: ok, note: "Replaces rules by id, all-or-nothing; unknown id is 404."}
  RemoveNetworkSecurityGroupSecurityRules: {wire: ok, errors: partial, state: ok, persist: ok, note: "All-or-nothing; unknown id is 404."}
  ListNetworkSecurityGroupSecurityRules: {wire: ok, errors: partial, state: ok, persist: ok, note: "Direction filter, newest first, paged."}
  ListNetworkSecurityGroupVnics: {wire: ok, errors: partial, state: ok, persist: ok, note: "Always empty: compute and VNICs are not emulated."}
  GetPublicIpByIpAddress: {wire: ok, errors: partial, state: ok, persist: ok, note: "POST body ipAddress; 404 for unknown addresses."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures. cli.go links this service's key registry to OCI Identity, so keys uploaded through UploadApiKey authenticate here; --oci-core-enforce-auth rejects unsigned requests and unknown keys."}
  compartment_checks: {status: ok, note: "cli.go wires SetCompartmentChecker to OCI Identity: compartmentId must be the tenancy or an ACTIVE compartment, otherwise 404 NotAuthorizedOrNotFound. Standalone (unwired) the compartment is accepted unchecked."}
  error_envelope: {status: partial, note: "Errors are {code,message} with opc-request-id. InvalidParameter, MissingParameter, NotAuthorizedOrNotFound, NoEtagMatch and IncorrectState follow the OCI reference; Conflict (delete in use, duplicate dnsLabel, already peered) and LimitExceeded (400) are unverified against the live service."}
  delete_in_use: {status: ok, note: "Deleting a VCN with live dependents, a route table, security list or DHCP options referenced by a subnet or gateway, a gateway targeted by a route rule, a default route table, security list or DHCP options set, an NSG used as a rule endpoint, or a NAT-assigned public IP answers 409 Conflict."}
  lifecycle: {status: ok, note: "PROVISIONING to AVAILABLE and TERMINATING to TERMINATED take two seconds on the injectable backend clock, applied on each request; TERMINATED resources are purged after an hour. Deleting a VCN terminates its default route table, security list and DHCP options."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-core-port / OCI_CORE_PORT (default 8155); never on the shared AWS router; cli.go reserves the port in the shared pool."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
items_still_open:
  - "Compute is out of scope: there are no instances, VNICs, private IPs, attachments or ephemeral public IPs for VNICs, so subnets never hold VNICs, ListNetworkSecurityGroupVnics is always empty, CreatePublicIp with privateIpId and UpdatePublicIp assignment answer 404, and route targets of kind private IP and DRG are accepted by OCID shape only."
  - "VCN CIDR operations (AddVcnCidr, ModifyVcnCidr, RemoveVcnCidr), IPv6 prefix operations, subnet CIDR changes (UpdateSubnet rejects a different cidrBlock; only one IPv4 CIDR per subnet), BYOIP, and the VCN DNS resolver association are not implemented."
  - "Not implemented: DRGs and attachments, VCN and subnet topology, IP inventory and CIDR utilisation, public IP pools, flow logs, VTAPs, private endpoints, cross-tenancy local peering, route and security policy analysis, and Core work requests."
  - "The service catalogue holds two entries (all IAD services, OCI IAD Object Storage) for the single emulated region us-ashburn-1."
  - "Security rules are checked structurally only: overlapping or duplicate rules are accepted, stateless/stateful semantics are not evaluated, and nothing enforces traffic."
  - "Availability domain names are checked for shape (<4 letters>:US-ASHBURN-AD-<1-3>) rather than against the tenancy's list."
  - "Error codes and statuses for limits (400 LimitExceeded), duplicate dnsLabel and delete-in-use (409 Conflict) are unverified against the live service; default DHCP domainNameType for custom option sets is assumed CUSTOM_DOMAIN."
  - "opc-retry-token idempotency, resource principals and instance principals are not applied; only API-key signatures are verified."
---

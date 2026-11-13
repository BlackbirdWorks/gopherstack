---
service: ocicore
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: c620a3b53
last_audit_date: 2026-11-05
overall: B
ops:
  CreateVcn: {wire: ok, errors: partial, state: ok, persist: ok, note: "Default route table, security list and DHCP options are created with it and cannot be deleted; cidrBlocks /16 to /30 (up to 16, no overlap, no 169.254/16 or multicast/reserved space); dnsLabel gives vcnDomainName <label>.oraclevcn.com; isIpv6Enabled allocates a /56 and adds the documented IPv6 default rules; 50 VCNs per region. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetVcn: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListVcns: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateVcn: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags and the documented mutable fields; If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
  DeleteVcn: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; TERMINATING then TERMINATED after two seconds; 409 Conflict while the VCN is referenced (see families.delete_in_use)."}
  ChangeVcnCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource; the target compartment is checked against OCI Identity when wired."}
  CreateSubnet: {wire: ok, errors: partial, state: ok, persist: ok, note: "IPv4 CIDRs /16 to /30 inside a VCN CIDR with no overlap among live subnets; regional or AD-specific (availabilityDomain checked for shape only); dnsLabel needs a VCN dnsLabel and is unique per VCN; prohibitPublicIpOnVnic implies prohibitInternetIngress; defaults to the VCN default route table, DHCP options and security list; at most 5 security lists; ipv4CidrBlocks (up to 6) and ipv6CidrBlock/ipv6CidrBlocks (/64 inside a VCN IPv6 or ULA prefix). Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
  GetSubnet: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; TERMINATED resources stay readable for an hour."}
  ListSubnets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; displayName, lifecycleState filters; sortBy TIMECREATED (newest first by default) or DISPLAYNAME; paged with opc-next-page."}
  UpdateSubnet: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags, route table, DHCP options, security lists, and cidrBlock under the SDK rules (same network address, inside a VCN block, all private IPs of the old block kept, new last address unused, no overlap); the subnet is UPDATING and takes no VNICs until the clock settles. ipv6CidrBlock and ipv6CidrBlocks replace the IPv6 prefixes (409 while an IPv6 lives in a removed one). If-Match gives 412 NoEtagMatch; 409 IncorrectState once terminating."}
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
  CreateRouteTable: {wire: ok, errors: partial, state: ok, persist: ok, note: "Rules validated: destination CIDR or service CIDR label, target IGW/NAT/LPG/service gateway in the same VCN (private IP targets are looked up and must be assigned to a VNIC in the same VCN; a DRG target must be attached to the VCN), SERVICE_CIDR_BLOCK only with a service gateway that has the service, no duplicate destinations, 200 rules. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
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
  CreatePublicIp: {wire: ok, errors: partial, state: ok, persist: ok, note: "Reserved addresses from 129.213.0.0/16 (50 per region), ephemeral ones for a private IP (AVAILABILITY_DOMAIN scope), privateIpId assigns immediately; lifetime, scope and availabilityDomain filters on list. Lifecycle PROVISIONING to AVAILABLE after two seconds on the injectable clock."}
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
  ListNetworkSecurityGroupVnics: {wire: ok, errors: partial, state: ok, persist: ok, note: "VNICs whose nsgIds contain the group, with the owning instance as resourceId."}
  GetPublicIpByIpAddress: {wire: ok, errors: partial, state: ok, persist: ok, note: "POST body ipAddress; 404 for unknown addresses."}
  ListShapes: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Static catalogue: VM.Standard.E4/E5/A1/3 Flex with ocpu, memory and bandwidth options plus VM.Standard2.1/2/4, VM.Standard.E2.1.Micro and BM.Standard.E4.128; imageId and shape filters by CPU architecture."}
  ListImages: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Platform images (Oracle Linux 8 and 9, Canonical Ubuntu 22.04 and 24.04, Windows Server 2019 and 2022, x86_64 and aarch64 where offered) plus the compartment custom images; operatingSystem, operatingSystemVersion, shape, displayName and lifecycleState filters. Sizes and build dates are representative, not the live values."}
  GetImage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Platform or custom image."}
  CreateImage: {wire: ok, errors: partial, state: ok, persist: ok, note: "Custom image from an instance (RUNNING or STOPPED) inheriting operating system and architecture; PROVISIONING to AVAILABLE. imageSourceDetails (object storage import) is not supported."}
  UpdateImage: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName and tags of custom images; platform images answer 404."}
  DeleteImage: {wire: ok, errors: partial, state: ok, persist: ok, note: "Custom images move to DELETED; platform images answer 404."}
  ChangeImageCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Custom images only."}
  LaunchInstance: {wire: ok, errors: partial, state: ok, persist: ok, note: "availabilityDomain, shape, shapeConfig (flex ocpus 1..max, memory 1 GB or ocpus up to 64 GB per OCPU and the shape total; fixed shapes reject shapeConfig), image or boot volume source (architecture compatibility, bootVolumeSizeInGBs), primary VNIC in the subnet (lowest free private IP from .2, optional privateIp, ephemeral public IP unless the subnet prohibits it, nsgIds up to 5, hostnameLabel unique per subnet and needing a subnet dnsLabel), metadata with base64 user_data, fault domain, creates a boot volume and attachment. PROVISIONING to RUNNING after two seconds. launchVolumeAttachments, instanceConfigurationId, capacity reservations, dedicated hosts and platformConfig are not supported. No VM runs."}
  GetInstance: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListInstances: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; availabilityDomain, displayName, lifecycleState filters; paged."}
  UpdateInstance: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags, metadata merge, extendedMetadata, agentConfig; shape or shapeConfig changes need a STOPPED instance (409 IncorrectState) and an architecture compatible with the image. Metadata merge semantics are unverified."}
  InstanceAction: {wire: ok, errors: partial, state: ok, persist: ok, note: "STOP and SOFTSTOP (RUNNING to STOPPING to STOPPED), START (STOPPED to STARTING to RUNNING), RESET and SOFTRESET (stay RUNNING; the live interim state is unverified); wrong state is 409 IncorrectState. SENDDIAGNOSTICINTERRUPT, DIAGNOSTICREBOOT and REBOOTMIGRATE answer 400."}
  TerminateInstance: {wire: ok, errors: partial, state: ok, persist: ok, note: "preserveBootVolume; detaches VNICs (terminated, private IPs dropped, ephemeral public IPs released), volume and boot volume attachments; TERMINATING then TERMINATED. preserveDataVolumesCreatedAtLaunch has no effect (no launch attachments)."}
  ChangeInstanceCompartment: {wire: ok, errors: partial, state: ok, persist: ok, note: "Moves the instance only; its VNICs and volumes keep their compartments."}
  ListVnicAttachments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; instanceId, vnicId, availabilityDomain filters."}
  GetVnicAttachment: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Readable while DETACHED until purged."}
  AttachVnic: {wire: ok, errors: partial, state: ok, persist: ok, note: "Secondary VNIC with private IP; limited by the shape maxVnicAttachments (flex: 2 for one OCPU else one per OCPU up to 24); nicIndex unique. VLAN attachments are not supported."}
  DetachVnic: {wire: ok, errors: partial, state: ok, persist: ok, note: "ATTACHED to DETACHING to DETACHED; the VNIC is terminated; the primary VNIC answers 409 Conflict."}
  GetVnic: {wire: ok, errors: ok, state: ok, persist: n/a, note: "publicIp follows the primary private IP."}
  UpdateVnic: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags, hostnameLabel, nsgIds, skipSourceDestCheck."}
  ListPrivateIps: {wire: ok, errors: partial, state: ok, persist: n/a, note: "vnicId, or subnetId with optional ipAddress; ipState and lifetime filters; paged."}
  GetPrivateIp: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Real lookup."}
  CreatePrivateIp: {wire: ok, errors: partial, state: ok, persist: ok, note: "Secondary IP on a VNIC (EPHEMERAL default) or a RESERVED one in a subnet; 32 per VNIC; reserved .0, .1 and last addresses refused."}
  UpdatePrivateIp: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags, hostnameLabel, lifetime and moving a non-primary IP to another VNIC of the same subnet (or unassigning a reserved one)."}
  DeletePrivateIp: {wire: ok, errors: partial, state: ok, persist: ok, note: "Primary IPs and route targets answer 409 Conflict; releases an assigned public IP."}
  CreateVolume: {wire: ok, errors: partial, state: ok, persist: ok, note: "50 to 32768 GB (sizeInGBs or sizeInMBs), vpusPerGB multiples of 10 up to 120, volume or backup source, backupPolicyId assigns a policy. PROVISIONING (RESTORING from a backup) to AVAILABLE. blockVolumeReplicas (one replica in a different AD, any region AD form; the replica is PROVISIONING then AVAILABLE and its timeLastSynced is set when it settles) and a blockVolumeReplica source are supported; kmsKeyId is checked against OCI Vault when wired; autotune policies are rejected."}
  GetVolume: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListVolumes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; availabilityDomain, displayName, lifecycleState filters."}
  UpdateVolume: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags, vpusPerGB, isAutoTuneEnabled and growing sizeInGBs."}
  DeleteVolume: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict while attached; drops the policy assignment."}
  ChangeVolumeCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource."}
  CreateBootVolume: {wire: ok, errors: partial, state: ok, persist: ok, note: "Clone of an AVAILABLE boot volume or restore of an AVAILABLE boot volume backup (availabilityDomain required, RESTORING to AVAILABLE); kmsKeyId checked against OCI Vault when wired."}
  GetBootVolume: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListBootVolumes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; availabilityDomain filter."}
  UpdateBootVolume: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags, vpusPerGB and growing sizeInGBs."}
  DeleteBootVolume: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict while attached."}
  ChangeBootVolumeCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource."}
  GetBootVolumeAttachment: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Created with the instance."}
  ListBootVolumeAttachments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; instanceId, bootVolumeId, availabilityDomain filters."}
  AttachVolume: {wire: ok, errors: partial, state: ok, persist: ok, note: "type iscsi (ipv4, iqn, port 3260, optional CHAP credentials) or paravirtualized; instance RUNNING or STOPPED, volume AVAILABLE in the same AD; 32 per instance; device /dev/oracleoci/oraclevd[b-z] unique per instance; a volume attached elsewhere needs isShareable and isReadOnly. ATTACHING to ATTACHED. iscsiLoginState stays UNKNOWN; emulated attachments and attaching boot volumes are not supported."}
  GetVolumeAttachment: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Polymorphic on attachmentType."}
  ListVolumeAttachments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; instanceId, volumeId, availabilityDomain filters."}
  DetachVolume: {wire: ok, errors: partial, state: ok, persist: ok, note: "ATTACHED to DETACHING to DETACHED."}
  CreateVolumeBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "Requires an AVAILABLE volume; the first backup of a volume is FULL (unverified), later ones default INCREMENTAL; CREATING to AVAILABLE. No data exists, so unique sizes are 0. Retention settings are stored and enforced on delete; kmsKeyId is checked against OCI Vault when wired."}
  GetVolumeBackup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListVolumeBackups: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; volumeId, sourceVolumeBackupId, displayName, lifecycleState filters."}
  UpdateVolumeBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags and retention settings (retentionPeriod, isPreventDeletionEnabled, isRetentionLockEnabled, isIndefiniteRetentionEnabled)."}
  DeleteVolumeBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "TERMINATING then TERMINATED; 409 Conflict while retention settings protect the backup or it belongs to a volume group backup."}
  ChangeVolumeBackupCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource."}
  CreateVolumeBackupPolicy: {wire: ok, errors: partial, state: ok, persist: ok, note: "Stores schedules (backupType, period, retentionSeconds); they are never executed, so no scheduled backups are taken."}
  GetVolumeBackupPolicy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Predefined bronze, silver and gold policies plus custom ones."}
  ListVolumeBackupPolicies: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Predefined policies, plus the custom policies of compartmentId when given."}
  UpdateVolumeBackupPolicy: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags, schedules; predefined policies answer 404."}
  DeleteVolumeBackupPolicy: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict while assigned."}
  CreateVolumeBackupPolicyAssignment: {wire: ok, errors: partial, state: ok, persist: ok, note: "One policy per volume, boot volume or volume group (409 otherwise)."}
  GetVolumeBackupPolicyAssetAssignment: {wire: ok, errors: ok, state: ok, persist: n/a, note: "assetId required."}
  GetVolumeBackupPolicyAssignment: {wire: ok, errors: ok, state: ok, persist: n/a, note: "By id."}
  DeleteVolumeBackupPolicyAssignment: {wire: ok, errors: partial, state: ok, persist: ok, note: "204."}
  CreateDrg: {wire: ok, errors: partial, state: ok, persist: ok, note: "Creates the two default DRG route tables (VCN attachments; RPC, VC and IPSec attachments), the two default import distributions and the default export distribution (MATCH_ALL ACCEPT) described in core/drg_route_distribution.go; defaultDrgRouteTables names the table of each attachment type. 5 DRGs per tenancy (400 LimitExceeded, unverified). Lifecycle PROVISIONING to AVAILABLE on the injectable clock."}
  GetDrg: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListDrgs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; paged with opc-next-page."}
  UpdateDrg: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags and defaultDrgRouteTables (each table must belong to the DRG); If-Match gives 412."}
  DeleteDrg: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict while an attachment is live; the DRG route tables and distributions end with it (unverified)."}
  ChangeDrgCompartment: {wire: ok, errors: partial, state: ok, persist: ok, note: "Moves the DRG and, as the SDK documents, its route tables, distributions and attachments with it."}
  GetDrgRedundancyStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Derived from the live IPSEC_TUNNEL and VIRTUAL_CIRCUIT attachments: none, single or multiple IPSec, single or multiple virtual circuits, mixed; REDUNDANT is never reported because it depends on CPE and provider topology (unverified mapping)."}
  UpgradeDrg: {wire: ok, errors: partial, state: ok, persist: ok, note: "Every DRG created here is already upgraded, so a live DRG answers 409 IncorrectState; there are no legacy DRGs to upgrade."}
  GetUpgradeStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Always UPGRADED; upgradedConnections counts the live attachments."}
  CreateDrgAttachment: {wire: ok, errors: partial, state: ok, persist: ok, note: "VCN attachments via vcnId or networkDetails (type VCN, routeTableId, vcnRouteType VCN_CIDRS or SUBNET_CIDRS); the VCN may attach to one DRG (409 Conflict), drgRouteTableId must belong to the DRG and defaults to the DRG default for VCNs; drgNatPolicyId answers 404 because NAT policies are not implemented. ATTACHING to ATTACHED. IPSec, virtual circuit and remote peering attachments are registered in-process with AttachDrgNetwork because the SDK creates VCN attachments only."}
  GetDrgAttachment: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; polymorphic networkDetails (VCN, IPSEC_TUNNEL, VIRTUAL_CIRCUIT, REMOTE_PEERING_CONNECTION)."}
  ListDrgAttachments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; drgId, vcnId, networkId, attachmentType (ALL accepted), drgRouteTableId, displayName, lifecycleState filters; sortBy and sortOrder."}
  UpdateDrgAttachment: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags, drgRouteTableId, exportDrgRouteDistributionId (EXPORT distribution of the same DRG, refused for VCN attachments), VCN routeTableId and vcnRouteType."}
  DeleteDrgAttachment: {wire: ok, errors: partial, state: ok, persist: ok, note: "ATTACHING or ATTACHED to DETACHING to DETACHED; 409 Conflict while a VCN route table still has a rule targeting the DRG (unverified)."}
  GetAllDrgAttachments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Live attachments of the DRG as ids; attachmentType and isCrossTenancy filters."}
  RemoveExportDrgRouteDistribution: {wire: ok, errors: partial, state: ok, persist: ok, note: "Clears exportDrgRouteDistributionId; routes are no longer advertised through the attachment."}
  CreateDrgRouteTable: {wire: ok, errors: partial, state: ok, persist: ok, note: "importDrgRouteDistributionId must be an IMPORT distribution of the same DRG; isEcmpEnabled. Lifecycle PROVISIONING to AVAILABLE."}
  GetDrgRouteTable: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListDrgRouteTables: {wire: ok, errors: ok, state: ok, persist: n/a, note: "drgId required; displayName, importDrgRouteDistributionId, lifecycleState filters."}
  UpdateDrgRouteTable: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags, importDrgRouteDistributionId, isEcmpEnabled."}
  DeleteDrgRouteTable: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict for a default table of the DRG or one an attachment uses."}
  RemoveImportDrgRouteDistribution: {wire: ok, errors: partial, state: ok, persist: ok, note: "Clears the import distribution so the table learns no routes."}
  AddDrgRouteRules: {wire: ok, errors: partial, state: ok, persist: ok, note: "Static CIDR_BLOCK rules, all or nothing; the next hop must be a live attachment of the same DRG; a destination may appear once per table (409 Conflict, unverified)."}
  UpdateDrgRouteRules: {wire: ok, errors: partial, state: ok, persist: ok, note: "Changes static rules by id, all or nothing; unknown id is 404."}
  RemoveDrgRouteRules: {wire: ok, errors: partial, state: ok, persist: ok, note: "Removes static rules by id, all or nothing."}
  ListDrgRouteRules: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Static rules plus the DYNAMIC rules learned from the import distribution: each VCN attachment the first matching statement accepts contributes its VCN CIDRs or subnet CIDRs (vcnRouteType); isConflict marks equal destinations from different attachments while ECMP is off; routeType filter."}
  CreateDrgRouteDistribution: {wire: ok, errors: partial, state: ok, persist: ok, note: "IMPORT distributions only (the export one is created with the DRG)."}
  GetDrgRouteDistribution: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListDrgRouteDistributions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "drgId required; displayName and lifecycleState filters."}
  UpdateDrgRouteDistribution: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName and tags."}
  DeleteDrgRouteDistribution: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict for the default export distribution or one a table or attachment uses."}
  AddDrgRouteDistributionStatements: {wire: ok, errors: partial, state: ok, persist: ok, note: "ACCEPT statements with a unique priority from 0 to 65535 (lower wins); all match criteria must hold, none means any (MATCH_ALL, DRG_ATTACHMENT_TYPE, DRG_ATTACHMENT_ID), per core/drg_route_distribution_statement.go."}
  UpdateDrgRouteDistributionStatements: {wire: ok, errors: partial, state: ok, persist: ok, note: "Changes priority and matchCriteria by id, all or nothing."}
  RemoveDrgRouteDistributionStatements: {wire: ok, errors: partial, state: ok, persist: ok, note: "Removes statements by id, all or nothing."}
  ListDrgRouteDistributionStatements: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Ordered by priority; sortOrder."}
  GetWorkRequest: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Core work requests on the injectable clock: ACCEPTED, IN_PROGRESS, then SUCCEEDED after two seconds; operationType names such as ADD_VCN_CIDR are unverified. Served on /20160918/workRequests like the workrequests SDK client expects."}
  ListWorkRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; resourceId filter; newest first; finished requests are purged after an hour."}
  ListWorkRequestErrors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Always empty: these operations do not fail."}
  ListWorkRequestLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Accepted and succeeded entries; sortOrder."}
  AddVcnCidr: {wire: ok, errors: partial, state: ok, persist: ok, note: "Per the SDK documentation: valid /16 to /30, no overlap with the VCN, a peered VCN (through connected LPGs) or reserved space, at most 16 blocks; the VCN is UPDATING until the work request completes and subnets, LPGs and route tables cannot change meanwhile (409 IncorrectState). Returns opc-work-request-id."}
  ModifyVcnCidr: {wire: ok, errors: partial, state: ok, persist: ok, note: "Replaces a block under the SDK criteria: valid, no overlap with the other blocks or peers, subnets and route rule destinations inside the old block stay inside the new one. Returns opc-work-request-id."}
  RemoveVcnCidr: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict while a subnet uses the block; the last block stays. Returns opc-work-request-id."}
  AddIpv6VcnCidr: {wire: ok, errors: partial, state: ok, persist: ok, note: "An Oracle-allocated /56 (one per VCN, then 409 Conflict) or a private ULA prefix; IPv6 default security list rules are added; BYOIP details answer 400. The range 2603:c020:4000::/36 is unverified."}
  RemoveIpv6VcnCidr: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict while a subnet uses the prefix."}
  AddIpv4SubnetCidr: {wire: ok, errors: partial, state: ok, persist: ok, note: "Valid, inside a VCN block, clear of other subnets and of the subnet itself, at most 6 blocks (limit unverified); subnet UPDATING until the work request completes."}
  ModifyIpv4SubnetCidr: {wire: ok, errors: partial, state: ok, persist: ok, note: "Replaces one block; every private IP in the old block must stay inside the new one."}
  RemoveIpv4SubnetCidr: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict while a private IP uses the block; the last block stays."}
  AddIpv6SubnetCidr: {wire: ok, errors: partial, state: ok, persist: ok, note: "A /64 inside a VCN prefix not used by another subnet; etag and work request headers."}
  RemoveIpv6SubnetCidr: {wire: ok, errors: partial, state: ok, persist: ok, note: "At least one prefix remains; 409 Conflict while an IPv6 lives in the prefix."}
  CreateIpv6: {wire: ok, errors: partial, state: ok, persist: ok, note: "For a VNIC in an IPv6 subnet (lowest free address from ::2, or ipAddress, ipv6SubnetCidr, hostname, routeTableId, lifetime) or a reserved unassigned one for a subnet; 32 per VNIC (limit unverified)."}
  GetIpv6: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListIpv6s: {wire: ok, errors: ok, state: ok, persist: n/a, note: "subnetId or vnicId required; ipAddress, ipState, lifetime filters."}
  UpdateIpv6: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags, hostname, routeTableId, lifetime and moving a reserved IPv6 to another VNIC of the same subnet."}
  DeleteIpv6: {wire: ok, errors: partial, state: ok, persist: ok, note: "TERMINATING then TERMINATED."}
  Ipv6VnicDetach: {wire: ok, errors: partial, state: ok, persist: ok, note: "Unassigns a reserved IPv6; an ephemeral one cannot be detached (unverified)."}
  CreateBootVolumeBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "First backup FULL (unverified); retentionPeriod, isPreventDeletionEnabled, isRetentionLockEnabled, isIndefiniteRetentionEnabled; kmsKeyId checked against OCI Vault. CREATING to AVAILABLE."}
  GetBootVolumeBackup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListBootVolumeBackups: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; bootVolumeId, sourceBootVolumeBackupId, displayName, lifecycleState filters."}
  UpdateBootVolumeBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags, kmsKeyId and retention settings; a retention lock cannot be removed."}
  DeleteBootVolumeBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict while retention settings protect the backup or it belongs to a volume group backup."}
  ChangeBootVolumeBackupCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource."}
  CopyBootVolumeBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "Copies an AVAILABLE backup to another known region identifier: a new backup with the destination region key in its OCID, sourceBootVolumeBackupId, CREATING to AVAILABLE on the clock, opc-work-request-id. The emulator has one namespace, so the copy is listed with the source."}
  CopyVolumeBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "As CopyBootVolumeBackup, for volume backups (sourceVolumeBackupId filter on ListVolumeBackups)."}
  CopyVolumeGroupBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "Copies the group backup and every member backup."}
  CreateVolumeGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "From volumeIds (volumes and boot volumes of the AD, each in one group, at most 32 (unverified)), a volumeGroupBackupId (restores new volumes), or a volumeGroupId clone in the same AD; volumeGroupReplicas and volume group replica sources are not supported; backupPolicyId assigns a policy."}
  GetVolumeGroup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; sizeInMBs sums the members."}
  ListVolumeGroups: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; availabilityDomain, displayName, lifecycleState filters."}
  UpdateVolumeGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags and the full volumeIds list (volumes left out are removed)."}
  DeleteVolumeGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "Members survive; policy assignments are dropped."}
  ChangeVolumeGroupCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource."}
  CreateVolumeGroupBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "Backs up every member (volume and boot volume backups carry volumeGroupBackupId); retention settings flow to the members; FULL first."}
  GetVolumeGroupBackup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; volumeBackupIds lists member backups."}
  ListVolumeGroupBackups: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; volumeGroupId, displayName filters."}
  UpdateVolumeGroupBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "displayName, tags and retention settings (members follow)."}
  DeleteVolumeGroupBackup: {wire: ok, errors: partial, state: ok, persist: ok, note: "Member backups end with it; 409 Conflict while retention protects the group backup or a member."}
  ChangeVolumeGroupBackupCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the resource."}
  GetBlockVolumeReplica: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListBlockVolumeReplicas: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId optional; availabilityDomain, displayName, lifecycleState filters."}
  GetVolumeKmsKey: {wire: ok, errors: ok, state: ok, persist: n/a, note: "kmsKeyId is null for Oracle-managed keys."}
  UpdateVolumeKmsKey: {wire: ok, errors: partial, state: ok, persist: ok, note: "The key must exist and be ENABLED in OCI Vault when the Vault checker is wired (cli.go), otherwise it is accepted as given."}
  DeleteVolumeKmsKey: {wire: ok, errors: partial, state: ok, persist: ok, note: "Back to Oracle-managed keys."}
  GetBootVolumeKmsKey: {wire: ok, errors: ok, state: ok, persist: n/a, note: "kmsKeyId is null for Oracle-managed keys."}
  UpdateBootVolumeKmsKey: {wire: ok, errors: partial, state: ok, persist: ok, note: "As UpdateVolumeKmsKey."}
  DeleteBootVolumeKmsKey: {wire: ok, errors: partial, state: ok, persist: ok, note: "Back to Oracle-managed keys."}
  GetVcnTopology: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Built from live state: the VCN, subnets, route tables, security lists, NSGs, DHCP options, gateways, VNICs, instances and the DRG with CONTAINS, ASSOCIATED_WITH and ROUTES_TO relationships; etag from the content and 304 on If-None-Match. The entity object shape (entityType plus the resource fields) is unverified."}
  GetSubnetTopology: {wire: ok, errors: ok, state: ok, persist: n/a, note: "The subnet, its VCN, route table, DHCP options, security lists, route targets, VNICs and their instances."}
  GetNetworkingTopology: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Every VCN of the compartment; queryCompartmentSubtree has no effect because the compartment tree lives in OCI Identity."}
  ListIpInventory: {wire: ok, errors: partial, state: ok, persist: ok, note: "Computed at once (lifecycle-state DONE) from VCNs, subnets, VNICs and private IPs; utilization, overlappingVcnsOnly, addressTypeList, searchKeyword, sortBy, sortOrder and pagination filters; the request is kept for GetResourceIpInventory. Utilization is a percentage: used over usable addresses (all but 3) for subnets, the share claimed by subnets for VCN blocks (unverified unit)."}
  GetResourceIpInventory: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replays a stored ListIpInventory request against the current state."}
  GetSubnetIpInventory: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Every private IPv4 and IPv6 address of the subnet with lifetime, parent CIDR, public IP, DNS hostname and owner."}
  GetSubnetCidrUtilization: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Utilization of each IPv4 and IPv6 block of the subnet."}
  GetVcnOverlap: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Other VCNs of the selected compartments whose CIDR blocks overlap this VCN."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures. cli.go links this service's key registry to OCI Identity, so keys uploaded through UploadApiKey authenticate here; --oci-core-enforce-auth rejects unsigned requests and unknown keys."}
  compartment_checks: {status: ok, note: "cli.go wires SetCompartmentChecker to OCI Identity: compartmentId must be the tenancy or an ACTIVE compartment, otherwise 404 NotAuthorizedOrNotFound. Standalone (unwired) the compartment is accepted unchecked."}
  error_envelope: {status: partial, note: "Errors are {code,message} with opc-request-id. InvalidParameter, MissingParameter, NotAuthorizedOrNotFound, NoEtagMatch and IncorrectState follow the OCI reference; Conflict (delete in use, duplicate dnsLabel, already peered) and LimitExceeded (400) are unverified against the live service."}
  delete_in_use: {status: ok, note: "Deleting a VCN with live dependents, a route table, security list or DHCP options referenced by a subnet or gateway, a gateway targeted by a route rule, a default route table, security list or DHCP options set, an NSG used as a rule endpoint, or a NAT-assigned public IP answers 409 Conflict."}
  lifecycle: {status: ok, note: "PROVISIONING to AVAILABLE and TERMINATING to TERMINATED take two seconds on the injectable backend clock, applied on each request; TERMINATED resources are purged after an hour. Deleting a VCN terminates its default route table, security list and DHCP options."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-core-port / OCI_CORE_PORT (default 8155); never on the shared AWS router; cli.go reserves the port in the shared pool."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
items_still_open:
  - "Compute is control plane only: no VM executes, so serial console (console history and connections), instance agent, run command, metadata service, boot/launch options enforcement, live migration and maintenance reboot do not exist. launchVolumeAttachments, instance configurations and pools, capacity reservations, dedicated VM hosts, compute clusters, cluster placement groups, platformConfig, preemptible instances, instance credentials and vnic VLAN attachments are not implemented."
  - "Block storage gaps: boot volume replicas, volume group replicas, autotune policies, volume attachment updates, attaching a standalone boot volume to an instance, and executed backup policy schedules. Backups hold no data (unique sizes are 0). The emulator has one namespace, so a cross-region backup copy is listed next to its source and a replica in another region cannot be activated except by creating a volume in its AD from the same endpoint."
  - "Platform image and shape catalogues are static and representative: image build dates, sizes and the shape list (four flex families, four fixed VM shapes, one bare metal) are not the live per-region values; fixed shape memory/vNIC limits follow the compute shapes documentation, flex VNIC limits are approximated."
  - "Unverified against the live service: error codes for duplicate hostnameLabel (409 Conflict), first backup forced FULL, RESET staying RUNNING, merge semantics of UpdateInstance metadata, and whether terminating an instance whose private IP is a route target is refused."
  - "BYOIP and BYOIPv6 ranges, the bulk IPv6 operations (BulkCreateIpv6s and siblings), VCN and subnet IPv6 ULA allocation beyond the supplied prefixes, are not implemented. Unverified against the live service: the Oracle-allocated IPv6 /56 range, the 6 IPv4 blocks per subnet and 32 IPv6 per VNIC limits, 409 for adding a second Oracle-allocated prefix, and the work request operationType names."
  - "Not implemented: DRG NAT policies and rules, public IP pools, flow logs, VTAPs, private endpoints, cross-tenancy local peering and DRG attachments, route and security policy analysis, bulk private IP operations, and DRG upgrade of legacy DRGs (every DRG here is already upgraded). Non-VCN DRG attachments are created in-process with AttachDrgNetwork because IPSec, FastConnect and remote peering are not emulated. Unverified: the names of the default DRG route tables and distributions, 5 DRGs per tenancy, 409 for a second attachment of a VCN, the redundancy status mapping, the entity object shape of the topology responses, and the utilization unit of the IP inventory."
  - "The service catalogue holds two entries (all IAD services, OCI IAD Object Storage) for the single emulated region us-ashburn-1."
  - "Security rules are validated structurally and CanReach evaluates security lists and NSGs (stateful and stateless rules, the reply path, a stateful rule winning over a stateless one) for a flow inside one VCN, but no emulated service calls it and nothing enforces traffic; overlapping or duplicate rules are accepted because the documentation does not say they are refused (unverified)."
  - "Availability domain names are checked for shape (<4 letters>:US-ASHBURN-AD-<1-3>) rather than against the OCI Identity list: the list carries a fixed prefix, so checking it would reject availability domain names copied from a real tenancy in existing fixtures."
  - "Error codes for duplicate dnsLabel and delete-in-use (409 Conflict) are unverified against the live service; default DHCP domainNameType for custom option sets is assumed CUSTOM_DOMAIN."
  - "Resource principals and instance principals are not applied; only API-key signatures are verified."
---

## 2026-11-05 evidence

History only; the open list is `items_still_open` above.

- DRGs, attachments, route tables, route distributions and statements follow the model comments of oci-go-sdk v65.126.2
  `core/drg*.go`, `core/drg_route_distribution*.go` and `core/drg_route_rule.go` (default tables and distributions,
  all-criteria-must-match statements, priority 0 to 65535, attachment states, export distribution unsupported for VCN
  attachments).
- VCN and subnet CIDR operations, IPv6 operations and their criteria follow the operation comments in
  `core/core_virtualnetwork_client.go` (AddVcnCidr, ModifyVcnCidr, RemoveVcnCidr, AddIpv6VcnCidr, ModifyIpv4SubnetCidr,
  RemoveIpv6SubnetCidr) and `core/update_subnet_details.go`; work requests use the `workrequests` package client.
- Block storage follows `core/core_blockstorage_client.go`, `core/volume_group*.go`, `core/boot_volume_backup.go`,
  `core/block_volume_replica*.go`, `core/retention_duration.go` and the Block Volume replication and backup retention
  documentation.
- Topology and IP inventory follow `core/*topology*.go`, `core/ip_inventory*.go` and `core/inventory_*.go`.
- Recalled rather than read from the SDK, hence listed as unverified: the Oracle IPv6 allocation range, the numeric limits
  (5 DRGs, 6 IPv4 blocks per subnet, 32 IPv6 per VNIC, 32 volumes per group, one replica per volume), the redundancy status
  mapping and the conflict statuses.

## Verified against Oracle documentation

- 400 LimitExceeded for limits and 409 InvalidatedRetryToken: https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm. Concurrent creates with one opc-retry-token now create once (lookup and create run under one write lock).

## 2026-11-13 VCN DNS resolver association

GetVcnDnsResolverAssociation (core VirtualNetwork, GET /vcns/{vcnId}/dnsResolverAssociation; oci-go-sdk v65.126.2 VcnDnsResolverAssociation) returns vcnId, the VCN lifecycle state (PROVISIONING, AVAILABLE, TERMINATING, TERMINATED) and, when OCI DNS is wired, dnsResolverId of the VCN private resolver.

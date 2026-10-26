---
service: gcpcompute
sdk_module: cloud.google.com/go/compute@v1.72.0
last_audit_commit: 6e873d8a8
last_audit_date: 2026-10-26
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  Networks.Insert: {wire: ok, errors: partial, state: ok, persist: ok, note: "autoCreateSubnetworks (omitted means auto mode), routingConfig (REGIONAL or GLOBAL), mtu 1300-8896 (default 1460), networkFirewallPolicyEnforcementOrder. Auto mode creates one subnetwork per supported region named like the network from the documented auto ranges (us-central1 is 10.128.0.0/20); every network gets a deletable default-route-* internet route. Legacy IPv4Range networks are 400. Returns a global Operation."}
  Networks.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: "subnetworks is derived from the stored subnetworks; selfLinkWithId carries the numeric id."}
  Networks.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: "filter, orderBy, maxResults, pageToken."}
  Networks.Patch: {wire: ok, errors: partial, state: ok, persist: ok, note: "Only routingConfig.routingMode, mtu, description and networkFirewallPolicyEnforcementOrder are updatable; any other field is 400."}
  Networks.SwitchToCustomMode: {wire: ok, errors: ok, state: ok, persist: ok, note: "Auto to custom is one-way; the auto subnetworks stay and then block network deletion like user subnetworks."}
  Networks.Delete: {wire: ok, errors: partial, state: ok, persist: ok, note: "400 resourceInUseByAnotherResource while a firewall, user route, non-auto subnetwork, router or instance references it. Auto subnetworks and system routes are removed with the network."}
  Subnetworks.Insert: {wire: ok, errors: partial, state: ok, persist: ok, note: "ipCidrRange must be an IPv4 network address in a private range (RFC 1918, 100.64/10 and the other documented non-routable blocks), /8 to /29, and may not overlap any primary or secondary range in the network. secondaryIpRanges are validated and stored; manual subnetworks are rejected in auto mode networks. gatewayAddress is the first host."}
  Subnetworks.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Subnetworks.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Subnetworks.AggregatedList: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Every supported region is a scope; scopes without results carry the NO_RESULTS_ON_PAGE warning."}
  Subnetworks.Patch: {wire: ok, errors: partial, state: ok, persist: ok, note: "Adds secondary ranges (existing ones cannot change), privateIpGoogleAccess, description, flow-log config; fingerprint is checked (412) and regenerated."}
  Subnetworks.ExpandIpCidrRange: {wire: ok, errors: ok, state: ok, persist: ok, note: "The new range must strictly contain the old one and may not overlap other ranges in the network."}
  Subnetworks.SetPrivateIpGoogleAccess: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Subnetworks.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: "400 resourceInUseByAnotherResource while an instance, internal address or router NAT references it."}
  Firewalls.Insert: {wire: ok, errors: partial, state: ok, persist: ok, note: "Exactly one of allowed/denied; protocols tcp, udp, icmp, esp, ah, sctp, ipip, all or 0-255; ports only for tcp/udp/sctp and validated as ports or ranges; direction INGRESS (default) or EGRESS with the matching source/destination fields; priority 0-65535 (default 1000, an explicit 0 is kept); CIDR and tag validation. Rules are stored, not enforced (no data plane)."}
  Firewalls.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Firewalls.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Firewalls.Patch: {wire: ok, errors: ok, state: ok, persist: ok, note: "JSON merge into the stored rule, then the full validation again; name and network are immutable."}
  Firewalls.Update: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT replaces the rule."}
  Firewalls.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Routes.Insert: {wire: ok, errors: partial, state: ok, persist: ok, note: "destRange CIDR, priority default 1000, tags, and exactly one of nextHopGateway (default-internet-gateway only), nextHopIp or nextHopInstance (must exist). VPN tunnel, ILB, hub, peering and network next hops are 400."}
  Routes.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Routes.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Routes.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Addresses.Insert: {wire: ok, errors: partial, state: ok, persist: ok, note: "Regional and global. EXTERNAL addresses come from a per-region (global: own) 34.x pool and are unique across addresses, instance NAT IPs and router NAT IPs; a requested address must be free. Regional INTERNAL addresses are allocated from the subnetwork's usable range (first two and last two addresses are reserved) or validated when given. IPv6, global INTERNAL and prefix-length addresses are 400."}
  Addresses.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: "status RESERVED or IN_USE and users are derived from the instances and router NATs that reference the address."}
  Addresses.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Addresses.AggregatedList: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Region scopes plus global."}
  Addresses.SetLabels: {wire: ok, errors: ok, state: ok, persist: ok, note: "labelFingerprint mismatch is 412."}
  Addresses.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: "400 resourceInUseByAnotherResource while in use."}
  Routers.Insert: {wire: ok, errors: partial, state: ok, persist: ok, note: "network, bgp (asn 1-4294967294, advertiseMode, keepaliveInterval default 20) and nats are validated and stored: natIpAllocateOption AUTO_ONLY (a real external IP is allocated per NAT and kept across updates) or MANUAL_ONLY (natIps must be free external regional addresses), sourceSubnetworkIpRangesToNat modes including LIST_OF_SUBNETWORKS with subnetworks in the router's network, and the documented timeout and port defaults. interfaces and bgpPeers are stored as sent without validation. Nothing is translated: there is no data plane."}
  Routers.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Routers.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Routers.AggregatedList: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Routers.Patch: {wire: ok, errors: ok, state: ok, persist: ok, note: "JSON merge (lists are replaced) and the full validation again."}
  Routers.Update: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Routers.GetRouterStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "natStatus lists the auto-allocated and user-allocated NAT IPs and counts running instances without an external IP in the subnetworks a NAT covers; bgpPeerStatus and bestRoutes are empty."}
  Routers.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Disks.Insert: {wire: ok, errors: partial, state: ok, persist: ok, note: "type pd-standard (default), pd-balanced or pd-ssd; sizeGb from the request, else the source image or snapshot size, else the type default, at least the source size and 10-65536 GB; sourceImage (also the sourceImage query parameter) resolved through the image catalog, family URLs included, or sourceSnapshot. Labels are validated."}
  Disks.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: "users is derived from attached instances."}
  Disks.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Disks.AggregatedList: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Disks.Resize: {wire: ok, errors: ok, state: ok, persist: ok, note: "Grow only."}
  Disks.SetLabels: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Disks.CreateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "Zonal operation targeting the disk, like the real API; creates a READY global snapshot (no data is copied). storageLocations defaults to the multi-region of the disk's zone."}
  Disks.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: "400 resourceInUseByAnotherResource while attached."}
  Snapshots.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Snapshots.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Snapshots.SetLabels: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Snapshots.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Images.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Public catalog projects debian-cloud, ubuntu-os-cloud, rocky-linux-cloud and centos-cloud serve fixed point releases of debian-11/12/13, ubuntu-2004-lts, ubuntu-2204-lts, ubuntu-2404-lts-amd64, rocky-linux-8/9 and centos-stream-9; other projects serve their own custom images."}
  Images.GetFromFamily: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Newest image of the family by creation time."}
  Images.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Images.Insert: {wire: ok, errors: partial, state: ok, persist: ok, note: "Custom images from exactly one of sourceDisk, sourceSnapshot or sourceImage; rawDisk imports are 400. Writes to the public catalog projects are 403."}
  Images.SetLabels: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Images.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Instances.Insert: {wire: ok, errors: partial, state: ok, persist: ok, note: "RFC1035 name, machineType (catalog or custom-N-M in the instance's zone), first disk is the boot disk (existing source or initializeParams creating a real disk, rolled back if the insert fails), networkInterfaces resolved to a subnetwork (auto-mode networks use the regional auto subnetwork, custom mode requires one in the instance's region) with an internal IP allocated from the subnet or validated, accessConfigs with an ephemeral external IP or a reserved address, labels, metadata (fingerprint), tags (fingerprint), scheduling (preemptible/SPOT rules), deletionProtection. Status follows the injectable clock: PROVISIONING, STAGING, RUNNING."}
  Instances.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Instances.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: "filter supports = != eq ne : > >= < <=, AND/OR/NOT, parentheses and dotted fields (labels.env, networkInterfaces.networkIP); values are RE2 full matches; orderBy any field with asc/desc."}
  Instances.AggregatedList: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Every supported zone is a scope; paging is by item across scopes."}
  Instances.Start: {wire: ok, errors: ok, state: ok, persist: ok, note: "Re-allocates ephemeral external IPs; a no-op operation when already running."}
  Instances.Stop: {wire: ok, errors: ok, state: ok, persist: ok, note: "STOPPING then TERMINATED on the clock; ephemeral external IPs are released, reserved ones and the internal IP are kept."}
  Instances.Reset: {wire: ok, errors: partial, state: ok, persist: ok, note: "Requires RUNNING (400 resourceNotReady otherwise); only refreshes lastStartTimestamp."}
  Instances.SetMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "A fingerprint that does not match is 412 conditionNotMet; duplicate or malformed keys are 400; the fingerprint changes with the content."}
  Instances.SetLabels: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stale labelFingerprint is 412."}
  Instances.SetTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stale fingerprint is 412."}
  Instances.SetMachineType: {wire: ok, errors: partial, state: ok, persist: ok, note: "Only while TERMINATED (400 resourceNotReady otherwise; the exact real message is unverified)."}
  Instances.SetDeletionProtection: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Instances.AttachDisk: {wire: ok, errors: ok, state: ok, persist: ok, note: "Existing disks, or initializeParams; READ_WRITE disks attach to one instance, READ_ONLY to many."}
  Instances.DetachDisk: {wire: ok, errors: ok, state: ok, persist: ok, note: "By deviceName; the boot disk only when TERMINATED."}
  Instances.AddAccessConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Instances.DeleteAccessConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  Instances.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: "Removes autoDelete disks; deletionProtection is 400."}
  MachineTypes.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Documented e2 (micro/small/medium, standard, highmem, highcpu), n2 (standard, highmem, highcpu) and n1-standard shapes in every zone, plus custom-N-M."}
  MachineTypes.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Also aggregatedList."}
  DiskTypes.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: "pd-standard, pd-balanced, pd-ssd; also list and aggregatedList."}
  Zones.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: "46 zones in 15 regions; also list."}
  Regions.Get: {wire: ok, errors: partial, state: ok, persist: n/a, note: "zones are listed; quotas are not modelled; also list."}
  Projects.Get: {wire: ok, errors: ok, state: ok, persist: ok, note: "commonInstanceMetadata with fingerprint; usage quotas are not modelled."}
  Projects.SetCommonInstanceMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stale fingerprint is 412."}
  Operations.Get: {wire: ok, errors: ok, state: ok, persist: ok, note: "Global, regional and zonal. Status is derived from the backend clock: PENDING, then RUNNING (progress 50, startTime) after the pending delay, then DONE (progress 100, endTime)."}
  Operations.List: {wire: ok, errors: ok, state: ok, persist: ok, note: "Also aggregatedList for the global scope."}
  Operations.Wait: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sleeps on the backend clock until the operation is DONE, then returns it. The Go client's Operation.Wait polls Get instead."}
  Operations.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns {} with 200; the Go client cannot decode an empty body."}
families:
  list_semantics: {status: ok, note: "Every list and aggregatedList shares one pipeline: filter (RE2 full match, comparison, has, AND/OR/NOT), orderBy, maxResults (default and cap 500) and pageToken through pkgs/page."}
  operations_clock: {status: ok, note: "Operations and instance states are computed from timestamps on an injectable clock (InMemoryBackend.SetClock, SetTiming), never from timers; defaults are 50ms pending, 400ms done, instances staging after 100ms and running after 300ms."}
  identity_and_links: {status: ok, note: "selfLinks use https://www.googleapis.com/compute/v1/, ids are random 19-digit uint64 strings, catalog ids are stable hashes, timestamps are RFC 3339 with milliseconds. Resource references accept full URLs, project-relative paths and bare names; /compute/beta and /compute/alpha are served with the v1 shapes."}
  vm_execution: {status: gap, note: "Structural: no virtual machine runs. Instances are state only, boot disks hold no data, snapshots and images copy nothing, startup scripts never execute, NAT, firewall rules and routes are stored but never applied, and there is no serial console or guest agent."}
  persistence: {status: ok, note: "Every resource map, operations and project metadata are in backendSnapshot."}
  routing_isolation: {status: ok, note: "Own REST listener bound in StartWorker to --gcp-compute-port / GCP_COMPUTE_PORT (default 8142); fails fast if taken, reserved in the shared PortAlloc pool, never registered on the shared AWS router. Only /compute/{v1,beta,alpha} is served."}
  auth: {status: partial, note: "No request is authenticated or authorised."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; requests go through telemetry. No dashboard page."}
items_still_open:
  - "Instance templates, instance group managers, managed and unmanaged instance groups, regional disks, instantSnapshots, machine images, reservations, resource policies, health checks, backend services, forwarding rules, target pools/proxies, URL maps, SSL certificates, VPN gateways/tunnels, interconnects, network peerings (addPeering/removePeering), shared VPC, security policies and licenses are not routed (404)."
  - "Instances: update (PUT), updateNetworkInterface, setScheduling, setServiceAccount, setMinCpuPlatform, setShieldedInstanceConfig, suspend/resume, getSerialPortOutput, getGuestAttributes, bulkInsert, listReferrers and simulateMaintenanceEvent are not implemented; local SSD (SCRATCH) disks, GPUs, IPv6 and alias IP ranges are rejected or stored without effect."
  - "System subnet routes (default-route-* per subnetwork) are not generated; only the 0.0.0.0/0 internet route is created with a network. Routes cannot use VPN tunnel, ILB, hub, peering or network next hops."
  - "Global INTERNAL addresses (VPC peering ranges), IPv6 addresses and prefix-length reservations are rejected."
  - "Error message texts for state conflicts (resourceNotReady on setMachineType/reset/detach), resourceInUseByAnotherResource wording, required-field and validation messages, and the firewall rule for ingress without sources were chosen without a production reference."
  - "Only a fixed set of 15 regions, 46 zones, 9 image families, e2/n2/n1-standard machine types and 3 persistent disk types exist; image point releases are pinned, region quotas and Project usage quotas are not modelled, and the status/progress timing is a fixed model."
  - "No dashboard page, and the emulator does not enforce IAM or return permission errors."
---

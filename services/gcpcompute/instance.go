package gcpcompute

import (
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	phaseStart   = "start"
	phaseStop    = "stop"
	phaseReset   = "reset"
	stRunning    = "RUNNING"
	stTerminated = "TERMINATED"
	stStopping   = "STOPPING"
	stStaging    = "STAGING"
	stProvision  = "PROVISIONING"
	maxNICs      = 8
	maxDisks     = 128
	natType      = "ONE_TO_ONE_NAT"
	natName      = "External NAT"
)

func instTarget(project, zone string, i *computeapi.Instance) target {
	return target{project: project, scope: scopeZone, loc: zone, collection: "instances", name: i.Name, id: i.Id}
}

func (r *instRec) status(now time.Time, t Timing) string {
	el := now.Sub(r.Since)

	switch r.Phase {
	case phaseStart:
		switch {
		case el < t.Provision:
			return stProvision
		case el < t.Stage:
			return stStaging
		}
	case phaseStop:
		if el < t.Stop {
			return stStopping
		}

		return stTerminated
	case phaseSuspend:
		if el < t.Stop {
			return stSuspending
		}

		return stSuspended
	}

	return stRunning
}

func instRunning(r *instRec, now time.Time, t Timing) bool { return r.status(now, t) == stRunning }

func cpuPlatformFor(machine string) string {
	switch {
	case strings.HasPrefix(machine, "n2-"):
		return "Intel Cascade Lake"
	case strings.HasPrefix(machine, "n1-"):
		return "Intel Haswell"
	}

	return platBroadwell
}

func (b *InMemoryBackend) instViewLocked(r *instRec) *computeapi.Instance {
	out := clone(r.Inst)
	out.Status = r.status(b.now().UTC(), b.timing)

	if out.Status != stTerminated && out.Status != stStopping {
		out.CpuPlatform = cpuPlatformFor(lastSegment(out.MachineType))
	} else {
		out.CpuPlatform = ""
	}

	out.ForceSendFields = []string{"CanIpForward", "DeletionProtection"}

	return out
}

func (b *InMemoryBackend) getInstLocked(project, zone, name string) (*instRec, *APIError) {
	r, ok := b.instances[key(project, zone, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeZone, zone, "instances", name))
	}

	return r, nil
}

func (b *InMemoryBackend) touchLocked(r *instRec) { r.Inst.Fingerprint = newFingerprint() }

func (b *InMemoryBackend) resolveMachineLocked(project, zone, raw string) (string, *APIError) {
	if raw == "" {
		return "", errRequired("machineType")
	}

	r, err := refLocated(raw, "machineTypes", scopeZone, project, zone)
	if err != nil || r.loc != zone {
		return "", errField("machineType", raw, "The URL is malformed or names a different zone.")
	}

	if _, ok := findMachine(r.name); !ok {
		if _, _, custom := customMachine(r.name); !custom {
			return "", errNotFound(relPath(r.project, scopeZone, zone, "machineTypes", r.name))
		}
	}

	return zoneLink(r.project, zone, "machineTypes", r.name), nil
}

func validMetadata(m *computeapi.Metadata) *APIError {
	if m == nil {
		return nil
	}

	seen := map[string]bool{}
	total := 0

	for _, it := range m.Items {
		if it == nil || len(it.Key) == 0 || len(it.Key) > maxMetaKey || !metaKeyOK(it.Key) {
			return errField(
				"metadata.items.key",
				keyOf(it),
				"Metadata keys must be 1-128 characters of letters, digits, '-' or '_'.",
			)
		}

		if seen[it.Key] {
			return errField("metadata.items.key", it.Key, "Duplicate metadata key.")
		}

		seen[it.Key] = true

		if it.Value != nil {
			if len(*it.Value) > maxMetaValue {
				return errField("metadata.items.value", it.Key, "Metadata value is too large (limit 256 KiB).")
			}

			total += len(*it.Value)
		}

		total += len(it.Key)
	}

	if total > maxMetaTotal {
		return errField("metadata", "", "Total metadata size exceeds 512 KiB.")
	}

	return nil
}

const (
	maxMetaKey   = 128
	maxMetaValue = 256 * 1024
	maxMetaTotal = 512 * 1024
)

func keyOf(it *computeapi.MetadataItems) string {
	if it == nil {
		return ""
	}

	return it.Key
}

func metaKeyOK(k string) bool {
	for _, c := range k {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}

	return true
}

func metadataFP(items []*computeapi.MetadataItems) string {
	sorted := make([]string, 0, len(items))

	for _, it := range items {
		v := ""
		if it.Value != nil {
			v = *it.Value
		}

		sorted = append(sorted, it.Key+"\x01"+v)
	}

	sort.Strings(sorted)

	return fingerprint(sorted...)
}

func tagsFP(items []string) string {
	s := append([]string(nil), items...)
	sort.Strings(s)

	return fingerprint(s...)
}

func normalizeMetadata(m *computeapi.Metadata) *computeapi.Metadata {
	out := &computeapi.Metadata{Kind: "compute#metadata"}
	if m != nil {
		out.Items = m.Items
	}

	out.Fingerprint = metadataFP(out.Items)

	return out
}

func defaultScheduling(in *computeapi.Scheduling) (*computeapi.Scheduling, *APIError) {
	s := &computeapi.Scheduling{}
	if in != nil {
		*s = *clone(in)
	}

	spot := s.Preemptible || s.ProvisioningModel == "SPOT"

	if spot && s.AutomaticRestart != nil && *s.AutomaticRestart {
		return nil, errInvalid(
			"Invalid value for field 'resource.scheduling.automaticRestart': 'true'. Preemptible and spot " +
				"instances cannot be automatically restarted.",
		)
	}

	if s.ProvisioningModel == "" {
		s.ProvisioningModel = tierStandard
		if s.Preemptible {
			s.ProvisioningModel = "SPOT"
		}
	}

	if s.AutomaticRestart == nil {
		v := !spot
		s.AutomaticRestart = &v
	}

	if s.OnHostMaintenance == "" {
		s.OnHostMaintenance = "MIGRATE"
		if spot {
			s.OnHostMaintenance = "TERMINATE"
		}
	}

	if spot && s.InstanceTerminationAction == "" {
		s.InstanceTerminationAction = "STOP"
	}

	s.ForceSendFields = []string{"Preemptible"}

	return s, nil
}

func (b *InMemoryBackend) resolveAttachedLocked(
	project, zone, instName string,
	idx int,
	ad *computeapi.AttachedDisk,
	created *[]string,
) (*computeapi.AttachedDisk, *APIError) {
	field := "resource.disks[" + strconv.Itoa(idx) + "]"
	if ad.Type != "" && ad.Type != diskPersist {
		return nil, errField(field+".type", ad.Type, "Only PERSISTENT disks are supported.")
	}

	mode := ad.Mode
	if mode == "" {
		mode = modeReadWrite
	}

	if mode != modeReadWrite && mode != modeReadOnly {
		return nil, errField(field+".mode", mode, "")
	}

	var d *computeapi.Disk

	switch {
	case ad.Source != "":
		r, err := refLocated(ad.Source, "disks", scopeZone, project, zone)
		if err != nil || r.loc != zone {
			return nil, errField(field+".source", ad.Source, "The URL is malformed or names a different zone.")
		}

		found, ok := b.disks[key(r.project, zone, r.name)]
		if !ok {
			return nil, errNotFound(relPath(r.project, scopeZone, zone, "disks", r.name))
		}

		d = found
	case ad.InitializeParams != nil:
		p := ad.InitializeParams
		in := &computeapi.Disk{
			Name: p.DiskName, SizeGb: p.DiskSizeGb, Type: p.DiskType, SourceImage: p.SourceImage,
			SourceSnapshot: p.SourceSnapshot, Labels: p.Labels, Description: p.Description,
		}

		if in.Name == "" {
			in.Name = instName
		}

		made, err := b.createDiskLocked(project, zone, in)
		if err != nil {
			return nil, err
		}

		*created = append(*created, key(project, zone, made.Name))
		d = made
	default:
		return nil, errInvalid(
			"Invalid value for field '%s': ''. Either source or initializeParams must be specified.",
			field,
		)
	}

	return &computeapi.AttachedDisk{
		Kind:            "compute#attachedDisk",
		Type:            diskPersist,
		Mode:            mode,
		Source:          d.SelfLink,
		Index:           int64(idx),
		Boot:            ad.Boot,
		AutoDelete:      ad.AutoDelete,
		DeviceName:      deviceName(ad.DeviceName, idx),
		Interface:       ifaceOf(ad.Interface),
		DiskSizeGb:      d.SizeGb,
		GuestOsFeatures: d.GuestOsFeatures,
		Licenses:        d.Licenses,
		Architecture:    d.Architecture,
	}, nil
}

func deviceName(given string, idx int) string {
	if given != "" {
		return given
	}

	return "persistent-disk-" + strconv.Itoa(idx)
}

func ifaceOf(s string) string {
	if s == "" {
		return "SCSI"
	}

	return s
}

func (b *InMemoryBackend) rollbackDisksLocked(keys []string) {
	for _, k := range keys {
		delete(b.disks, k)
	}
}

func (b *InMemoryBackend) buildDisksLocked(
	project, zone string,
	in *computeapi.Instance,
	created *[]string,
) ([]*computeapi.AttachedDisk, *APIError) {
	if len(in.Disks) == 0 {
		return nil, errInvalid("Invalid value for field 'resource.disks': ''. At least one boot disk is required.")
	}

	if len(in.Disks) > maxDisks {
		return nil, errInvalid("Invalid value for field 'resource.disks': too many disks.")
	}

	if !in.Disks[0].Boot {
		return nil, errInvalid(
			"Invalid value for field 'resource.disks[0].boot': 'false'. The first disk must be the boot disk.",
		)
	}

	out := make([]*computeapi.AttachedDisk, 0, len(in.Disks))
	devices := map[string]bool{}
	sources := map[string]bool{}

	for i, ad := range in.Disks {
		if i > 0 && ad.Boot {
			return nil, errInvalid(
				"Invalid value for field 'resource.disks[%d].boot': 'true'. Only one boot disk is allowed.",
				i,
			)
		}

		att, err := b.resolveAttachedLocked(project, zone, in.Name, i, ad, created)
		if err != nil {
			return nil, err
		}

		if devices[att.DeviceName] || sources[att.Source] {
			return nil, errInvalid("Invalid value for field 'resource.disks[%d]': duplicate device name or source.", i)
		}

		if by := b.diskWriterLocked(att, ""); by != "" {
			return nil, errInUse("disk", trimURL(att.Source), by)
		}

		devices[att.DeviceName], sources[att.Source] = true, true
		out = append(out, att)
	}

	return out, nil
}

// diskWriterLocked returns an instance that blocks attaching ad (a read-write holder, or any holder for a
// read-write attach), ignoring the instance named skip.
func (b *InMemoryBackend) diskWriterLocked(ad *computeapi.AttachedDisk, skip string) string {
	for _, r := range b.instances {
		if r.Inst.SelfLink == skip {
			continue
		}

		for _, have := range r.Inst.Disks {
			if have.Source == ad.Source && (have.Mode != modeReadOnly || ad.Mode != modeReadOnly) {
				return trimURL(r.Inst.SelfLink)
			}
		}
	}

	return ""
}

func (b *InMemoryBackend) buildNICsLocked(
	project, zone string,
	in *computeapi.Instance,
) ([]*computeapi.NetworkInterface, *APIError) {
	if len(in.NetworkInterfaces) == 0 {
		return nil, errInvalid(
			"Invalid value for field 'resource.networkInterfaces': ''. At least one network interface is required.",
		)
	}

	if len(in.NetworkInterfaces) > maxNICs {
		return nil, errInvalid("Invalid value for field 'resource.networkInterfaces': too many network interfaces.")
	}

	region := regionOfZone(zone)
	out := make([]*computeapi.NetworkInterface, 0, len(in.NetworkInterfaces))
	nets := map[string]bool{}
	taken := map[netip.Addr]bool{}

	var pendingExt []string

	for i, nic := range in.NetworkInterfaces {
		built, err := b.buildNICLocked(project, region, i, nic, taken, &pendingExt)
		if err != nil {
			return nil, err
		}

		if nets[built.Network] {
			return nil, errInvalid(
				"Invalid value for field 'resource.networkInterfaces[%d]': network interfaces must be on different networks.",
				i,
			)
		}

		nets[built.Network] = true
		out = append(out, built)
	}

	return out, nil
}

func (b *InMemoryBackend) explicitSubnetLocked(
	project, region, field string,
	nic *computeapi.NetworkInterface,
) (*computeapi.Subnetwork, *APIError) {
	s, err := b.resolveSubnetLocked(project, region, nic.Subnetwork)
	if err != nil {
		return nil, err
	}

	if lastSegment(s.Region) != region {
		return nil, errField(
			field+".subnetwork",
			nic.Subnetwork,
			"Subnetwork is in region '"+lastSegment(s.Region)+"' but the instance is in region '"+region+"'.",
		)
	}

	if nic.Network == "" {
		return s, nil
	}

	n, err := b.resolveNetworkLocked(project, nic.Network)
	if err != nil {
		return nil, err
	}

	if n.SelfLink != s.Network {
		return nil, errField(field+".network", nic.Network, "Subnetwork does not belong to this network.")
	}

	return s, nil
}

func (b *InMemoryBackend) pickSubnetLocked(
	project, region string,
	idx int,
	nic *computeapi.NetworkInterface,
) (*computeapi.Subnetwork, *APIError) {
	field := "resource.networkInterfaces[" + strconv.Itoa(idx) + "]"

	if nic.Subnetwork != "" {
		return b.explicitSubnetLocked(project, region, field, nic)
	}

	if nic.Network == "" {
		return nil, errInvalid(
			"Invalid value for field '%s.network': ''. A network or subnetwork must be specified.",
			field,
		)
	}

	n, err := b.resolveNetworkLocked(project, nic.Network)
	if err != nil {
		return nil, err
	}

	if !n.AutoCreateSubnetworks {
		return nil, errInvalid(
			"Invalid value for field '%s.subnetwork': ''. Subnetwork must be specified for custom mode network '%s'.",
			field,
			n.Name,
		)
	}

	for _, s := range b.subnets {
		if s.Network == n.SelfLink && lastSegment(s.Region) == region {
			return s, nil
		}
	}

	return nil, errNotFound(relPath(project, scopeRegion, region, "subnetworks", n.Name))
}

func (b *InMemoryBackend) buildNICLocked(
	project, region string,
	idx int,
	in *computeapi.NetworkInterface,
	taken map[netip.Addr]bool,
	pendingExt *[]string,
) (*computeapi.NetworkInterface, *APIError) {
	s, err := b.pickSubnetLocked(project, region, idx, in)
	if err != nil {
		return nil, err
	}

	p := netip.MustParsePrefix(s.IpCidrRange)
	used := b.usedInternalLocked(s.SelfLink)

	var ip netip.Addr

	if in.NetworkIP == "" {
		for a := range taken {
			used[a] = true
		}

		var ok bool
		if ip, ok = nextFree(p, used); !ok {
			return nil, errInvalid("No free IP addresses are available in subnetwork '%s'.", s.Name)
		}
	} else {
		parsed, perr := netip.ParseAddr(in.NetworkIP)
		first, last := usableRange(p)

		if perr != nil || parsed.Compare(first) < 0 || parsed.Compare(last) > 0 {
			return nil, errField(
				"resource.networkInterfaces["+strconv.Itoa(idx)+"].networkIP",
				in.NetworkIP,
				"The IP address is not in the usable range of subnetwork '"+s.Name+"'.",
			)
		}

		if used[parsed] && !b.reservedFreeLocked(s.SelfLink, in.NetworkIP) {
			return nil, errField(
				"resource.networkInterfaces["+strconv.Itoa(idx)+"].networkIP",
				in.NetworkIP,
				"The IP address is already in use.",
			)
		}

		ip = parsed
	}

	taken[ip] = true

	nic := &computeapi.NetworkInterface{
		Kind: "compute#networkInterface", Name: "nic" + strconv.Itoa(idx), Network: s.Network, Subnetwork: s.SelfLink,
		NetworkIP: ip.String(), StackType: "IPV4_ONLY", Fingerprint: newFingerprint(), NicType: in.NicType,
		AliasIpRanges: in.AliasIpRanges,
	}

	for _, ac := range in.AccessConfigs {
		built, aerr := b.buildAccessConfigLocked(region, ac, *pendingExt)
		if aerr != nil {
			return nil, aerr
		}

		if built.NatIP != "" {
			*pendingExt = append(*pendingExt, built.NatIP)
		}

		nic.AccessConfigs = append(nic.AccessConfigs, built)
	}

	return nic, nil
}

// reservedFreeLocked reports whether ip is held only by a reserved internal address object (usable by an instance).
func (b *InMemoryBackend) reservedFreeLocked(subnet, ip string) bool {
	for _, a := range b.addresses {
		if a.Subnetwork == subnet && a.Address == ip {
			for _, r := range b.instances {
				if addressUsedBy(a, r.Inst) {
					return false
				}
			}

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) buildAccessConfigLocked(
	region string,
	in *computeapi.AccessConfig,
	pending []string,
) (*computeapi.AccessConfig, *APIError) {
	if in.Type != "" && in.Type != natType {
		return nil, errField(
			"resource.networkInterfaces.accessConfigs.type",
			in.Type,
			"Only ONE_TO_ONE_NAT is supported.",
		)
	}

	ac := &computeapi.AccessConfig{
		Kind: "compute#accessConfig", Type: natType, Name: in.Name, NetworkTier: in.NetworkTier,
		SetPublicPtr: in.SetPublicPtr, PublicPtrDomainName: in.PublicPtrDomainName,
	}

	if ac.Name == "" {
		ac.Name = natName
	}

	if ac.NetworkTier == "" {
		ac.NetworkTier = tierPremium
	}

	if in.NatIP != "" {
		if a := b.findExternalAddressLocked(region, in.NatIP); a == nil {
			return nil, errInvalid(
				"Invalid value for field 'resource.networkInterfaces.accessConfigs.natIP': '%s'. "+
					"External IP address was not found in region '%s'.",
				in.NatIP,
				region,
			)
		}

		for _, r := range b.instances {
			if usesNatIP(r.Inst, in.NatIP) {
				return nil, errInvalid(
					"Invalid value for field 'resource.networkInterfaces.accessConfigs.natIP': '%s'. IP address is already in use.",
					in.NatIP,
				)
			}
		}

		ac.NatIP = in.NatIP

		return ac, nil
	}

	ip, err := b.allocExternalLocked(region, pending...)
	if err != nil {
		return nil, err
	}

	ac.NatIP = ip

	return ac, nil
}

func usesNatIP(inst *computeapi.Instance, ip string) bool {
	for _, nic := range inst.NetworkInterfaces {
		for _, ac := range nic.AccessConfigs {
			if ac.NatIP == ip {
				return true
			}
		}
	}

	return false
}

func (b *InMemoryBackend) findExternalAddressLocked(region, ip string) *computeapi.Address {
	for _, a := range b.addresses {
		if a.AddressType != addrInternal && a.Address == ip && lastSegment(a.Region) == region && a.Region != "" {
			return a
		}
	}

	return nil
}

func (b *InMemoryBackend) isReservedIPLocked(ip string) bool {
	for _, a := range b.addresses {
		if a.AddressType != addrInternal && a.Address == ip {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) insertInstance(project, zone string, in *computeapi.Instance) (*computeapi.Operation, error) {
	b.mu.Lock("InsertInstance")
	defer b.mu.Unlock()

	inst, err := b.insertInstanceLocked(project, zone, in)
	if err != nil {
		return nil, err
	}

	return b.newOpLocked("insert", instTarget(project, zone, inst), b.timing.Stage), nil
}

func (b *InMemoryBackend) insertInstanceLocked(
	project, zone string,
	in *computeapi.Instance,
) (*computeapi.Instance, *APIError) {
	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if !zoneExists(zone) {
		return nil, errNotFound(scopePath(project, scopeZone, zone))
	}

	if _, dup := b.instances[key(project, zone, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeZone, zone, "instances", in.Name))
	}

	machine, err := b.resolveMachineLocked(project, zone, in.MachineType)
	if err != nil {
		return nil, err
	}

	if err = firstErr(validLabels(in.Labels), validMetadata(in.Metadata), validTagList(in.Tags)); err != nil {
		return nil, err
	}

	sched, err := defaultScheduling(in.Scheduling)
	if err != nil {
		return nil, err
	}

	policies, err := b.resolveResPoliciesLocked(
		project, regionOfZone(zone), in.ResourcePolicies, "resource.resourcePolicies", instancePolicyKinds)
	if err != nil {
		return nil, err
	}

	nics, err := b.buildNICsLocked(project, zone, in)
	if err != nil {
		return nil, err
	}

	var created []string

	disks, err := b.buildDisksLocked(project, zone, in, &created)
	if err != nil {
		b.rollbackDisksLocked(created)

		return nil, err
	}

	inst := b.assembleInstanceLocked(project, zone, in, machine, sched, nics, disks)
	inst.ResourcePolicies = policies
	b.instances[key(project, zone, in.Name)] = &instRec{Inst: inst, Phase: phaseStart, Since: b.now().UTC()}

	return inst, nil
}

func validTagList(t *computeapi.Tags) *APIError {
	if t == nil {
		return nil
	}

	return validTags("tags.items", t.Items)
}

func firstErr(errs ...*APIError) *APIError {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

func (b *InMemoryBackend) assembleInstanceLocked(
	project, zone string,
	in *computeapi.Instance,
	machine string,
	sched *computeapi.Scheduling,
	nics []*computeapi.NetworkInterface,
	disks []*computeapi.AttachedDisk,
) *computeapi.Instance {
	now := b.stamp()
	tags := &computeapi.Tags{}

	if in.Tags != nil {
		tags.Items = in.Tags.Items
	}

	tags.Fingerprint = tagsFP(tags.Items)

	shielded := in.ShieldedInstanceConfig
	if shielded == nil {
		shielded = &computeapi.ShieldedInstanceConfig{EnableVtpm: true, EnableIntegrityMonitoring: true}
	}

	for _, ad := range disks {
		if d, ok := b.disks[key(project, zone, lastSegment(ad.Source))]; ok {
			d.LastAttachTimestamp = now
		}
	}

	return &computeapi.Instance{
		Kind:              "compute#instance",
		Id:                newID(),
		CreationTimestamp: now,
		Name:              in.Name,
		Description:       in.Description,
		Zone: scopeURL(
			project,
			scopeZone,
			zone,
		),
		MachineType:            machine,
		SelfLink:               zoneLink(project, zone, "instances", in.Name),
		NetworkInterfaces:      nics,
		Disks:                  disks,
		Metadata:               normalizeMetadata(in.Metadata),
		Tags:                   tags,
		Labels:                 in.Labels,
		LabelFingerprint:       labelFP(in.Labels),
		Scheduling:             sched,
		ServiceAccounts:        in.ServiceAccounts,
		CanIpForward:           in.CanIpForward,
		DeletionProtection:     in.DeletionProtection,
		MinCpuPlatform:         in.MinCpuPlatform,
		Hostname:               in.Hostname,
		Fingerprint:            newFingerprint(),
		LastStartTimestamp:     now,
		ShieldedInstanceConfig: shielded,
		ReservationAffinity:    &computeapi.ReservationAffinity{ConsumeReservationType: "ANY_RESERVATION"},
		DisplayDevice:          in.DisplayDevice,
		GuestAccelerators:      in.GuestAccelerators,
	}
}

func (b *InMemoryBackend) getInstance(project, zone, name string) (*computeapi.Instance, error) {
	b.mu.RLock("GetInstance")
	defer b.mu.RUnlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	return b.instViewLocked(r), nil
}

func (b *InMemoryBackend) listInstances(project, scope, loc string) []entry {
	b.mu.RLock("ListInstances")
	defer b.mu.RUnlock()

	var out []entry

	for k, r := range b.instances {
		zone := lastSegment(r.Inst.Zone)
		if !strings.HasPrefix(k, project+"/") || (scope == scopeZone && zone != loc) {
			continue
		}

		out = append(out, entry{scope: scopeZone + "/" + zone, item: b.instViewLocked(r)})
	}

	return out
}

func (b *InMemoryBackend) deleteInstance(project, zone, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteInstance")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if r.Inst.DeletionProtection {
		return nil, errInvalid(
			"Invalid value for field 'deletionProtection': 'true'. Cannot delete instance '%s' "+
				"while deletion protection is enabled.",
			name,
		)
	}

	b.removeInstanceLocked(project, zone, r)
	b.sweepGroupsLocked()

	return b.newOpLocked("delete", instTarget(project, zone, r.Inst), b.timing.Stop), nil
}

func (b *InMemoryBackend) removeInstanceLocked(project, zone string, r *instRec) {
	delete(b.instances, key(project, zone, r.Inst.Name))

	for _, ad := range r.Inst.Disks {
		if ad.AutoDelete {
			delete(b.disks, key(project, zone, lastSegment(ad.Source)))
		}
	}

	b.dropFromGroupsLocked(r.Inst.SelfLink)
}

func (b *InMemoryBackend) startInstance(project, zone, name string) (*computeapi.Operation, error) {
	b.mu.Lock("StartInstance")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if st := r.status(b.now().UTC(), b.timing); st == stSuspended || st == stSuspending {
		return nil, errNotReady(relPath(project, scopeZone, zone, "instances", name),
			"the instance is suspended; use instances.resume")
	}

	if r.status(b.now().UTC(), b.timing) != stTerminated {
		return b.newOpLocked("start", instTarget(project, zone, r.Inst), b.timing.Pending), nil
	}

	region := regionOfZone(zone)

	var pending []string

	for _, nic := range r.Inst.NetworkInterfaces {
		for _, ac := range nic.AccessConfigs {
			if ac.NatIP != "" {
				continue
			}

			ip, aerr := b.allocExternalLocked(region, pending...)
			if aerr != nil {
				return nil, aerr
			}

			pending, ac.NatIP = append(pending, ip), ip
		}
	}

	now := b.now().UTC()
	r.Phase, r.Since = phaseStart, now
	r.Inst.LastStartTimestamp = now.Format(timeLayout)
	b.touchLocked(r)

	return b.newOpLocked("start", instTarget(project, zone, r.Inst), b.timing.Stage), nil
}

func (b *InMemoryBackend) stopInstance(project, zone, name string) (*computeapi.Operation, error) {
	b.mu.Lock("StopInstance")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	st := r.status(b.now().UTC(), b.timing)
	if st == stTerminated || st == stStopping {
		return b.newOpLocked("stop", instTarget(project, zone, r.Inst), b.timing.Pending), nil
	}

	for _, nic := range r.Inst.NetworkInterfaces {
		for _, ac := range nic.AccessConfigs {
			if ac.NatIP != "" && !b.isReservedIPLocked(ac.NatIP) {
				ac.NatIP = ""
			}
		}
	}

	now := b.now().UTC()
	r.Phase, r.Since = phaseStop, now
	r.Inst.LastStopTimestamp = now.Format(timeLayout)
	b.touchLocked(r)

	return b.newOpLocked("stop", instTarget(project, zone, r.Inst), b.timing.Stop), nil
}

func (b *InMemoryBackend) resetInstance(project, zone, name string) (*computeapi.Operation, error) {
	b.mu.Lock("ResetInstance")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if r.status(b.now().UTC(), b.timing) != stRunning {
		return nil, errNotReady(relPath(project, scopeZone, zone, "instances", name), "instance is not running")
	}

	r.Phase, r.Since = phaseReset, b.now().UTC()
	r.Inst.LastStartTimestamp = b.stamp()
	b.touchLocked(r)

	return b.newOpLocked("reset", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) setInstanceMetadata(
	project, zone, name string,
	m *computeapi.Metadata,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetInstanceMetadata")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if m.Fingerprint != r.Inst.Metadata.Fingerprint {
		return nil, errPrecondition("Supplied fingerprint does not match current metadata fingerprint.")
	}

	if err = validMetadata(m); err != nil {
		return nil, err
	}

	r.Inst.Metadata = normalizeMetadata(m)
	b.touchLocked(r)

	return b.newOpLocked("setMetadata", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) setInstanceLabels(
	project, zone, name string,
	req labelsRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetInstanceLabels")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	labels, fp, lerr := applyLabels(r.Inst.Labels, r.Inst.LabelFingerprint, req)
	if lerr != nil {
		return nil, lerr
	}

	r.Inst.Labels, r.Inst.LabelFingerprint = labels, fp
	b.touchLocked(r)

	return b.newOpLocked("setLabels", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) setInstanceTags(
	project, zone, name string,
	t *computeapi.Tags,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetInstanceTags")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if t.Fingerprint != r.Inst.Tags.Fingerprint {
		return nil, errPrecondition("Supplied fingerprint does not match current tags fingerprint.")
	}

	if err = validTags("tags.items", t.Items); err != nil {
		return nil, err
	}

	r.Inst.Tags = &computeapi.Tags{Items: t.Items, Fingerprint: tagsFP(t.Items)}
	b.touchLocked(r)

	return b.newOpLocked("setTags", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) setInstanceMachineType(project, zone, name, machine string) (*computeapi.Operation, error) {
	b.mu.Lock("SetMachineType")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if r.status(b.now().UTC(), b.timing) != stTerminated {
		return nil, errNotReady(
			relPath(project, scopeZone, zone, "instances", name),
			"the instance must be TERMINATED to change its machine type",
		)
	}

	link, err := b.resolveMachineLocked(project, zone, machine)
	if err != nil {
		return nil, err
	}

	r.Inst.MachineType = link
	b.touchLocked(r)

	return b.newOpLocked("setMachineType", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) setDeletionProtection(project, zone, name string, on bool) (*computeapi.Operation, error) {
	b.mu.Lock("SetDeletionProtection")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	r.Inst.DeletionProtection = on
	b.touchLocked(r)

	return b.newOpLocked("setDeletionProtection", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) attachDisk(
	project, zone, name string,
	ad *computeapi.AttachedDisk,
) (*computeapi.Operation, error) {
	b.mu.Lock("AttachDisk")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if len(r.Inst.Disks) >= maxDisks {
		return nil, errInvalid("Invalid value for field 'resource.disks': too many disks.")
	}

	if ad.Boot || ad.Source == "" {
		return nil, errInvalid(
			"Invalid value for field 'source': attachDisk requires an existing non-boot disk source.",
		)
	}

	idx := 0
	for _, have := range r.Inst.Disks {
		idx = max(idx, int(have.Index)+1)
	}

	var created []string

	att, err := b.resolveAttachedLocked(project, zone, name, idx, ad, &created)
	if err != nil {
		return nil, err
	}

	for _, have := range r.Inst.Disks {
		if have.DeviceName == att.DeviceName || have.Source == att.Source {
			return nil, errInvalid(
				"Invalid value for field 'deviceName': '%s'. The disk is already attached to this instance.",
				att.DeviceName,
			)
		}
	}

	if by := b.diskWriterLocked(att, r.Inst.SelfLink); by != "" {
		return nil, errInUse("disk", trimURL(att.Source), by)
	}

	r.Inst.Disks = append(r.Inst.Disks, att)

	if d, ok := b.disks[key(project, zone, lastSegment(att.Source))]; ok {
		d.LastAttachTimestamp = b.stamp()
	}

	b.touchLocked(r)

	return b.newOpLocked("attachDisk", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) detachDisk(project, zone, name, device string) (*computeapi.Operation, error) {
	b.mu.Lock("DetachDisk")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if device == "" {
		return nil, errRequired("deviceName")
	}

	for i, ad := range r.Inst.Disks {
		if ad.DeviceName != device {
			continue
		}

		if ad.Boot && r.status(b.now().UTC(), b.timing) != stTerminated {
			return nil, errNotReady(
				relPath(project, scopeZone, zone, "instances", name),
				"the boot disk can only be detached from a TERMINATED instance",
			)
		}

		r.Inst.Disks = append(r.Inst.Disks[:i], r.Inst.Disks[i+1:]...)

		if d, ok := b.disks[key(project, zone, lastSegment(ad.Source))]; ok {
			d.LastDetachTimestamp = b.stamp()
		}

		b.touchLocked(r)

		return b.newOpLocked("detachDisk", instTarget(project, zone, r.Inst), b.timing.Done), nil
	}

	return nil, errField("deviceName", device, "No disk with this device name is attached.")
}

func (b *InMemoryBackend) findNICLocked(r *instRec, name string) (*computeapi.NetworkInterface, *APIError) {
	if name == "" {
		return nil, errRequired("networkInterface")
	}

	for _, nic := range r.Inst.NetworkInterfaces {
		if nic.Name == name {
			return nic, nil
		}
	}

	return nil, errField("networkInterface", name, "No such network interface.")
}

func (b *InMemoryBackend) addAccessConfig(
	project, zone, name, nicName string,
	in *computeapi.AccessConfig,
) (*computeapi.Operation, error) {
	b.mu.Lock("AddAccessConfig")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	nic, err := b.findNICLocked(r, nicName)
	if err != nil {
		return nil, err
	}

	ac, err := b.buildAccessConfigLocked(regionOfZone(zone), in, nil)
	if err != nil {
		return nil, err
	}

	for _, have := range nic.AccessConfigs {
		if have.Name == ac.Name {
			return nil, errInvalid(
				"Invalid value for field 'accessConfig.name': '%s'. An access config with this name already exists.",
				ac.Name,
			)
		}
	}

	if in.NatIP == "" && r.status(b.now().UTC(), b.timing) == stTerminated {
		ac.NatIP = ""
	}

	nic.AccessConfigs = append(nic.AccessConfigs, ac)
	nic.Fingerprint = newFingerprint()
	b.touchLocked(r)

	return b.newOpLocked("addAccessConfig", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) deleteAccessConfig(
	project, zone, name, nicName, acName string,
) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteAccessConfig")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	nic, err := b.findNICLocked(r, nicName)
	if err != nil {
		return nil, err
	}

	for i, ac := range nic.AccessConfigs {
		if ac.Name == acName {
			nic.AccessConfigs = append(nic.AccessConfigs[:i], nic.AccessConfigs[i+1:]...)
			nic.Fingerprint = newFingerprint()
			b.touchLocked(r)

			return b.newOpLocked("deleteAccessConfig", instTarget(project, zone, r.Inst), b.timing.Done), nil
		}
	}

	return nil, errField("accessConfig", acName, "No such access config.")
}

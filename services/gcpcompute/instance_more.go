package gcpcompute

import (
	"net/netip"
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	maintMigrate   = "MIGRATE"
	maintTerminate = "TERMINATE"
)

var acceleratorTypes = []string{ //nolint:gochecknoglobals // read-only lookup table
	"nvidia-tesla-t4", "nvidia-tesla-t4-vws", "nvidia-tesla-p4", "nvidia-tesla-p100", "nvidia-tesla-v100",
	"nvidia-tesla-k80", "nvidia-tesla-a100", "nvidia-l4",
}

type setNameRequest struct {
	CurrentName string `json:"currentName"`
	Name        string `json:"name"`
}

func (b *InMemoryBackend) instOpLocked(op string, project, zone string, r *instRec) *computeapi.Operation {
	return b.newOpLocked(op, instTarget(project, zone, r.Inst), b.timing.Done)
}

func (b *InMemoryBackend) managedByGroupLocked(project, selfLink string) bool {
	for _, rec := range b.migs {
		if rec.Project != project {
			continue
		}

		for _, m := range rec.Members {
			if zoneLink(project, m.Zone, "instances", m.Name) == selfLink {
				return true
			}
		}
	}

	return false
}

func (b *InMemoryBackend) setInstanceName(
	project, zone, name string, in setNameRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetName")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if in.CurrentName != name {
		return nil, errField("currentName", in.CurrentName, "It does not match the current name of the instance.")
	}

	if err = firstErr(validName("name", in.Name), b.requireStoppedLocked(r, project, zone, "name")); err != nil {
		return nil, err
	}

	if _, dup := b.instances[key(project, zone, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeZone, zone, "instances", in.Name))
	}

	if b.managedByGroupLocked(project, r.Inst.SelfLink) {
		return nil, errInvalid(
			"Invalid value for field 'name': '%s'. The instance is managed by an instance group.",
			name,
		)
	}

	oldLink := r.Inst.SelfLink
	delete(b.instances, key(project, zone, name))

	r.Inst.Name = in.Name
	r.Inst.SelfLink = zoneLink(project, zone, "instances", in.Name)
	b.instances[key(project, zone, in.Name)] = r

	for _, g := range b.groups {
		for i, m := range g.Members {
			if m == oldLink {
				g.Members[i] = r.Inst.SelfLink
			}
		}
	}

	b.touchLocked(r)

	return b.instOpLocked("setName", project, zone, r), nil
}

func (b *InMemoryBackend) setInstanceMachineResources(
	project, zone, name string, accels []*computeapi.AcceleratorConfig,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetMachineResources")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if err = b.requireStoppedLocked(r, project, zone, "machine resources"); err != nil {
		return nil, err
	}

	out := make([]*computeapi.AcceleratorConfig, 0, len(accels))

	for _, a := range accels {
		t := lastSegment(a.AcceleratorType)
		if !slices.Contains(acceleratorTypes, t) {
			return nil, errField("guestAccelerators.acceleratorType", a.AcceleratorType, "Unknown accelerator type.")
		}

		if a.AcceleratorCount < 1 {
			return nil, errField("guestAccelerators.acceleratorCount", "", "Must be at least 1.")
		}

		out = append(out, &computeapi.AcceleratorConfig{
			AcceleratorType: zoneLink(project, zone, "acceleratorTypes", t), AcceleratorCount: a.AcceleratorCount,
		})
	}

	if len(out) > 0 {
		if machine := lastSegment(r.Inst.MachineType); !strings.HasPrefix(machine, "n1-") {
			return nil, errInvalid(
				"Invalid value for field 'guestAccelerators': GPUs cannot be attached to machine type '%s'.", machine)
		}

		if r.Inst.Scheduling == nil || r.Inst.Scheduling.OnHostMaintenance != maintTerminate {
			return nil, errInvalid(
				"Invalid value for field 'scheduling.onHostMaintenance': instances with GPUs must use TERMINATE.")
		}
	}

	r.Inst.GuestAccelerators = out
	b.touchLocked(r)

	return b.instOpLocked("setMachineResources", project, zone, r), nil
}

func (b *InMemoryBackend) setDiskAutoDelete(
	project, zone, name, device string, autoDelete string,
) (*computeapi.Operation, error) {
	if device == "" {
		return nil, errRequired("deviceName")
	}

	on, perr := strconv.ParseBool(autoDelete)
	if perr != nil {
		return nil, errField("autoDelete", autoDelete, "")
	}

	b.mu.Lock("SetDiskAutoDelete")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	for _, ad := range r.Inst.Disks {
		if ad.DeviceName == device {
			ad.AutoDelete = on
			b.touchLocked(r)

			return b.instOpLocked("setDiskAutoDelete", project, zone, r), nil
		}
	}

	return nil, errField("deviceName", device, "No such attached disk.")
}

func (b *InMemoryBackend) updateDisplayDevice(
	project, zone, name string, in *computeapi.DisplayDevice,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateDisplayDevice")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if err = b.requireStoppedLocked(r, project, zone, "display device"); err != nil {
		return nil, err
	}

	r.Inst.DisplayDevice = &computeapi.DisplayDevice{EnableDisplay: in.EnableDisplay}
	b.touchLocked(r)

	return b.instOpLocked("updateDisplayDevice", project, zone, r), nil
}

func (b *InMemoryBackend) listReferrers(project, zone, name string) ([]entry, error) {
	b.mu.RLock("ListReferrers")
	defer b.mu.RUnlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	var out []entry

	ref := func(referrer string) {
		out = append(out, entry{scope: scopeGlobal, item: &computeapi.Reference{
			Kind: "compute#reference", ReferenceType: "MEMBER_OF", Referrer: referrer, Target: r.Inst.SelfLink,
		}})
	}

	for _, g := range b.groups {
		if slices.Contains(g.Members, r.Inst.SelfLink) {
			ref(g.Group.SelfLink)
		}
	}

	for _, rec := range b.migs {
		for _, m := range liveMembers(rec) {
			if rec.Project == project && m.Zone == zone && m.Name == name {
				ref(rec.Mgr.InstanceGroup)
			}
		}
	}

	return out, nil
}

func (b *InMemoryBackend) simulateMaintenance(project, zone, name string) (*computeapi.Operation, error) {
	b.mu.Lock("SimulateMaintenanceEvent")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	now := b.now().UTC()
	if r.status(now, b.timing) != stRunning {
		return nil, errNotReady(relPath(project, scopeZone, zone, "instances", name), "instance is not running")
	}

	s := r.Inst.Scheduling
	if s != nil && s.OnHostMaintenance == maintTerminate {
		if s.AutomaticRestart != nil && *s.AutomaticRestart {
			r.Phase, r.Since = phaseStart, now
			r.Inst.LastStartTimestamp = b.stamp()
		} else {
			r.Phase, r.Since = phaseStop, now
			r.Inst.LastStopTimestamp = b.stamp()
		}

		b.touchLocked(r)
	}

	return b.instOpLocked("simulateMaintenanceEvent", project, zone, r), nil
}

func (b *InMemoryBackend) updateAccessConfig(
	project, zone, name, nicName string, in *computeapi.AccessConfig,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateAccessConfig")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	nic, err := b.findNICLocked(r, nicName)
	if err != nil {
		return nil, err
	}

	idx := slices.IndexFunc(nic.AccessConfigs, func(ac *computeapi.AccessConfig) bool { return ac.Name == in.Name })
	if idx < 0 {
		return nil, errField("accessConfig", in.Name, "No such access config.")
	}

	cur := nic.AccessConfigs[idx]
	if in.NetworkTier != "" && in.NetworkTier != cur.NetworkTier {
		return nil, errField("accessConfig.networkTier", in.NetworkTier, "The network tier cannot be changed.")
	}

	next := clone(cur)
	next.SetPublicPtr, next.PublicPtrDomainName = in.SetPublicPtr, in.PublicPtrDomainName

	if in.NatIP != cur.NatIP {
		nic.AccessConfigs = slices.Delete(slices.Clone(nic.AccessConfigs), idx, idx+1)
		built, berr := b.buildAccessConfigLocked(regionOfZone(zone), &computeapi.AccessConfig{
			Name: cur.Name, Type: cur.Type, NetworkTier: cur.NetworkTier, NatIP: in.NatIP,
		}, nil)
		nic.AccessConfigs = slices.Insert(nic.AccessConfigs, idx, cur)

		if berr != nil {
			return nil, berr
		}

		next.NatIP = built.NatIP
		if in.NatIP == "" && r.status(b.now().UTC(), b.timing) == stTerminated {
			next.NatIP = ""
		}
	}

	nic.AccessConfigs[idx] = next
	nic.Fingerprint = newFingerprint()
	b.touchLocked(r)

	return b.instOpLocked("updateAccessConfig", project, zone, r), nil
}

func (b *InMemoryBackend) updateNetworkInterface(
	project, zone, name, nicName string, in *computeapi.NetworkInterface, raw map[string]any,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateNetworkInterface")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	nic, err := b.findNICLocked(r, nicName)
	if err != nil {
		return nil, err
	}

	if in.Fingerprint != "" && in.Fingerprint != nic.Fingerprint {
		return nil, errPrecondition("Supplied fingerprint does not match current fingerprint.")
	}

	next := clone(nic)

	if err = b.updateNICStackLocked(r, next, in, nicName); err != nil {
		return nil, err
	}

	if _, ok := raw["aliasIpRanges"]; ok {
		sub := b.subnetBySelfLinkLocked(nic.Subnetwork)
		if sub == nil {
			return nil, errNotFound(trimURL(nic.Subnetwork))
		}

		if next.AliasIpRanges, err = b.resolveAliasRangesLocked(sub, nic, in.AliasIpRanges, in.AliasIpv6Ranges); err != nil {
			return nil, err
		}
	}

	if err = b.moveNICLocked(project, zone, r, next, in, raw); err != nil {
		return nil, err
	}

	*nic = *next
	nic.Fingerprint = newFingerprint()
	b.touchLocked(r)

	return b.instOpLocked("updateNetworkInterface", project, zone, r), nil
}

// nicMoveRequest returns the interface to build for a patch touching network, subnetwork or networkIP, or nil when
// the patch leaves them as they are.
func nicMoveRequest(cur, in *computeapi.NetworkInterface, raw map[string]any) *computeapi.NetworkInterface {
	_, hasNet := raw["network"]
	_, hasSub := raw["subnetwork"]
	_, hasIP := raw["networkIP"]

	req := &computeapi.NetworkInterface{Network: cur.Network, Subnetwork: cur.Subnetwork}
	if hasNet || hasSub {
		req.Network, req.Subnetwork = in.Network, in.Subnetwork
	}

	switch {
	case hasIP:
		req.NetworkIP = in.NetworkIP
	case req.Subnetwork == cur.Subnetwork:
		req.NetworkIP = cur.NetworkIP
	}

	if req.Subnetwork == cur.Subnetwork {
		req.Network = cur.Network
	}

	if req.NetworkIP == cur.NetworkIP && req.Subnetwork == cur.Subnetwork {
		return nil
	}

	return req
}

func nicIndex(inst *computeapi.Instance, name string) int {
	return slices.IndexFunc(inst.NetworkInterfaces, func(n *computeapi.NetworkInterface) bool { return n.Name == name })
}

func (b *InMemoryBackend) moveNICLocked(
	project, zone string, r *instRec, next, in *computeapi.NetworkInterface, raw map[string]any,
) *APIError {
	req := nicMoveRequest(next, in, raw)
	if req == nil {
		return nil
	}

	if err := b.requireStoppedLocked(r, project, zone, "network interface"); err != nil {
		return err
	}

	idx := nicIndex(r.Inst, next.Name)
	own := r.Inst.NetworkInterfaces[idx]
	saved := own.NetworkIP
	own.NetworkIP = ""

	built, err := b.buildNICLocked(project, regionOfZone(zone), idx, req, map[netip.Addr]bool{}, new([]string))
	own.NetworkIP = saved

	if err != nil {
		return err
	}

	for i, n := range r.Inst.NetworkInterfaces {
		if i != idx && n.Network == built.Network {
			return errInvalid("Invalid value for field 'network': network interfaces must be on different networks.")
		}
	}

	if built.Subnetwork != next.Subnetwork {
		next.AliasIpRanges = nil
	}

	next.Network, next.Subnetwork, next.NetworkIP = built.Network, built.Subnetwork, built.NetworkIP

	return nil
}

// updateNICStackLocked moves a nic between IPV4_ONLY and IPV4_IPV6, allocating or releasing its IPv6 address.
func (b *InMemoryBackend) updateNICStackLocked(
	r *instRec, next, in *computeapi.NetworkInterface, nicName string,
) *APIError {
	if in.StackType == "" || in.StackType == next.StackType || in.StackType == stackIPv4Only && next.StackType == "" {
		return nil
	}

	if in.StackType == stackIPv6Only || next.StackType == stackIPv6Only {
		return errField("networkInterface.stackType", in.StackType, "IPV6_ONLY interfaces cannot be changed.")
	}

	sub := b.subnetBySelfLinkLocked(next.Subnetwork)
	if sub == nil {
		return errNotFound(trimURL(next.Subnetwork))
	}

	tmp := &computeapi.NetworkInterface{}
	req := &computeapi.NetworkInterface{
		StackType: in.StackType, Ipv6Address: in.Ipv6Address, Ipv6AccessConfigs: in.Ipv6AccessConfigs,
	}

	if err := b.buildNICIPv6Locked(sub, req, tmp, nicIndex(r.Inst, nicName)); err != nil {
		return err
	}

	next.StackType, next.Ipv6Address, next.Ipv6AccessConfigs = tmp.StackType, tmp.Ipv6Address, tmp.Ipv6AccessConfigs
	next.Ipv6AccessType, next.InternalIpv6PrefixLength = tmp.Ipv6AccessType, tmp.InternalIpv6PrefixLength

	return nil
}

func (b *InMemoryBackend) subnetBySelfLinkLocked(link string) *computeapi.Subnetwork {
	for _, s := range b.subnets {
		if s.SelfLink == link {
			return s
		}
	}

	return nil
}

package gcpcompute

import (
	"maps"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	autoDeleteNever  = "NEVER"
	autoDeleteOnPerm = "ON_PERMANENT_INSTANCE_DELETION"
	picApplying      = "APPLYING"
	picEffective     = "EFFECTIVE"
	maxMIGBatch      = 1000
	nicPrefix        = "nic"
)

type picRec struct {
	Since  time.Time                     `json:"since"`
	Config *computeapi.PerInstanceConfig `json:"config"`
}

func validAutoDelete(field, v string) *APIError {
	switch v {
	case "", autoDeleteNever, autoDeleteOnPerm:
		return nil
	}

	return errField(field, v, "Must be NEVER or ON_PERMANENT_INSTANCE_DELETION.")
}

func nicNumber(k string) (int, bool) {
	n, err := strconv.Atoi(strings.TrimPrefix(k, nicPrefix))
	if err != nil || !strings.HasPrefix(k, nicPrefix) || n < 0 {
		return 0, false
	}

	return n, true
}

func validStatefulPolicy(p *computeapi.StatefulPolicy) *APIError {
	if p == nil || p.PreservedState == nil {
		return nil
	}

	ps := p.PreservedState
	pre := "resource.statefulPolicy.preservedState"

	for k, d := range ps.Disks {
		if k == "" {
			return errField(pre+".disks", "", "Device names must not be empty.")
		}

		if err := validAutoDelete(pre+".disks.autoDelete", d.AutoDelete); err != nil {
			return err
		}

		d.AutoDelete = cmpOr(d.AutoDelete, autoDeleteNever)
		ps.Disks[k] = d
	}

	if err := normalizePolicyIPs(pre+".externalIPs", ps.ExternalIPs); err != nil {
		return err
	}

	return normalizePolicyIPs(pre+".internalIPs", ps.InternalIPs)
}

func normalizePolicyIPs(field string, m map[string]computeapi.StatefulPolicyPreservedStateNetworkIp) *APIError {
	for k, ip := range m {
		if _, ok := nicNumber(k); !ok {
			return errField(field, k, "Keys are network interface names such as nic0.")
		}

		if err := validAutoDelete(field+".autoDelete", ip.AutoDelete); err != nil {
			return err
		}

		ip.AutoDelete = cmpOr(ip.AutoDelete, autoDeleteNever)
		m[k] = ip
	}

	return nil
}

func (rec *migRec) config(name string) *picRec {
	for _, c := range rec.Configs {
		if c.Config.Name == name {
			return c
		}
	}

	return nil
}

func (rec *migRec) policyState() *computeapi.StatefulPolicyPreservedState {
	if p := rec.Mgr.StatefulPolicy; p != nil {
		return p.PreservedState
	}

	return nil
}

func (rec *migRec) hasStatefulConfig() bool {
	ps := rec.policyState()

	return len(rec.Configs) > 0 || ps != nil && (len(ps.Disks)+len(ps.ExternalIPs)+len(ps.InternalIPs) > 0)
}

func (b *InMemoryBackend) picStatus(c *picRec) string {
	if b.now().UTC().Before(c.Since.Add(b.timing.Done)) {
		return picApplying
	}

	return picEffective
}

func (b *InMemoryBackend) picViewLocked(c *picRec) *computeapi.PerInstanceConfig {
	out := clone(c.Config)
	out.Status = b.picStatus(c)

	return out
}

func (b *InMemoryBackend) statefulStatusLocked(rec *migRec) *computeapi.InstanceGroupManagerStatusStateful {
	all := true

	for _, c := range rec.Configs {
		all = all && b.picStatus(c) == picEffective
	}

	return &computeapi.InstanceGroupManagerStatusStateful{
		HasStatefulConfig:  rec.hasStatefulConfig(),
		PerInstanceConfigs: &computeapi.InstanceGroupManagerStatusStatefulPerInstanceConfigs{AllEffective: all},
	}
}

func (b *InMemoryBackend) resolvePreservedIPLocked(project, region, field, raw string) (string, *APIError) {
	if _, err := netip.ParseAddr(raw); err == nil {
		return raw, nil
	}

	r, ok := parseRef(raw, collAddresses, project, region)
	if !ok {
		return "", errField(field, raw, "Not an IP address or reserved address reference.")
	}

	scope, loc := scopeRegion, r.loc

	if r.scope == scopeGlobal {
		scope, loc = scopeGlobal, ""
	}

	a, err := b.getAddressLocked(r.project, scope, loc, r.name)
	if err != nil {
		return "", err
	}

	return a.Address, nil
}

// validatePreservedStateLocked checks a per-instance preserved state against the member's zone.
func (b *InMemoryBackend) validatePreservedStateLocked(project, zone string, ps *computeapi.PreservedState) *APIError {
	if ps == nil {
		return nil
	}

	const f = "resource.preservedState"

	for dev, d := range ps.Disks {
		if err := b.validatePreservedDiskLocked(project, zone, f+".disks", dev, d); err != nil {
			return err
		}
	}

	region := regionOfZone(zone)

	for field, m := range map[string]map[string]computeapi.PreservedStatePreservedNetworkIp{
		"externalIPs": ps.ExternalIPs, "internalIPs": ps.InternalIPs,
	} {
		for k, ip := range m {
			if err := b.validatePreservedIPLocked(project, region, f+"."+field, k, ip); err != nil {
				return err
			}
		}
	}

	for k := range ps.Metadata {
		if !metaKeyOK(k) {
			return errField(f+".metadata", k, "Invalid metadata key.")
		}
	}

	return nil
}

func (b *InMemoryBackend) validatePreservedDiskLocked(
	project, zone, f, dev string, d computeapi.PreservedStatePreservedDisk,
) *APIError {
	if dev == "" {
		return errField(f, "", "Device names must not be empty.")
	}

	if d.Source == "" {
		return errRequired(f + ".source")
	}

	if _, err := b.attachableDiskLocked(project, zone, f, d.Source); err != nil {
		return err
	}

	if d.Mode != "" && d.Mode != modeReadWrite && d.Mode != modeReadOnly {
		return errField(f+".mode", d.Mode, "Must be READ_ONLY or READ_WRITE.")
	}

	return validAutoDelete(f+".autoDelete", d.AutoDelete)
}

func (b *InMemoryBackend) validatePreservedIPLocked(
	project, region, f, nic string, ip computeapi.PreservedStatePreservedNetworkIp,
) *APIError {
	if _, ok := nicNumber(nic); !ok {
		return errField(f, nic, "Keys are network interface names such as nic0.")
	}

	if err := validAutoDelete(f+".autoDelete", ip.AutoDelete); err != nil {
		return err
	}

	if ip.IpAddress == nil || ip.IpAddress.Address == "" {
		return errRequired(f + ".ipAddress.address")
	}

	_, err := b.resolvePreservedIPLocked(project, region, f+".ipAddress", ip.IpAddress.Address)

	return err
}

func mergePreserved(dst, src *computeapi.PreservedState) *computeapi.PreservedState {
	if dst == nil {
		return clone(src)
	}

	if src == nil {
		return dst
	}

	dst.Disks = mergeMap(dst.Disks, src.Disks)
	dst.ExternalIPs = mergeMap(dst.ExternalIPs, src.ExternalIPs)
	dst.InternalIPs = mergeMap(dst.InternalIPs, src.InternalIPs)
	dst.Metadata = mergeMap(dst.Metadata, src.Metadata)

	return dst
}

func mergeMap[V any](dst, src map[string]V) map[string]V {
	if len(src) > 0 && dst == nil {
		dst = map[string]V{}
	}

	maps.Copy(dst, src)

	return dst
}

// diskRetention returns the autoDelete setting stateful state attaches to a device, if any.
func (rec *migRec) diskRetention(name, device string) (string, bool) {
	if c := rec.config(name); c != nil && c.Config.PreservedState != nil {
		if d, ok := c.Config.PreservedState.Disks[device]; ok {
			return cmpOr(d.AutoDelete, autoDeleteNever), true
		}
	}

	if ps := rec.policyState(); ps != nil {
		if d, ok := ps.Disks[device]; ok {
			return cmpOr(d.AutoDelete, autoDeleteNever), true
		}
	}

	return "", false
}

// retainStateLocked sets up a member's stateful disks and addresses before its instance goes away: kept on
// recreate, deleted on permanent removal only when autoDelete asks for it.
func (b *InMemoryBackend) retainStateLocked(rec *migRec, project string, m *memberRec, permanent bool) {
	r, ok := b.instances[key(project, m.Zone, m.Name)]
	if !ok {
		return
	}

	for _, ad := range r.Inst.Disks {
		if mode, found := rec.diskRetention(m.Name, ad.DeviceName); found {
			ad.AutoDelete = permanent && mode == autoDeleteOnPerm
		}
	}

	ps := rec.policyState()
	if ps == nil {
		return
	}

	if permanent {
		for nic, ip := range ps.ExternalIPs {
			if ip.AutoDelete == autoDeleteOnPerm {
				delete(b.addresses, key(project, scopeRegion, regionOfZone(m.Zone), keptAddressName(m.Name, nic)))
			}
		}

		return
	}

	m.KeptInt, m.KeptExt = map[string]string{}, map[string]string{}

	for i, nic := range r.Inst.NetworkInterfaces {
		k := nicPrefix + strconv.Itoa(i)
		if _, keep := ps.InternalIPs[k]; keep {
			m.KeptInt[k] = nic.NetworkIP
		}

		if _, keep := ps.ExternalIPs[k]; keep && len(nic.AccessConfigs) > 0 && nic.AccessConfigs[0].NatIP != "" {
			m.KeptExt[k] = nic.AccessConfigs[0].NatIP
		}
	}
}

func keptAddressName(instance, nic string) string { return instance + "-" + nic }

// restoreStateLocked applies the stateful policy and per-instance config to an instance about to be created.
func (b *InMemoryBackend) restoreStateLocked(
	project string, rec *migRec, zone string, in *computeapi.Instance,
) *APIError {
	m := rec.memberNamed(in.Name)

	b.reuseDisksLocked(project, rec, zone, in)

	if c := rec.config(in.Name); c != nil && c.Config.PreservedState != nil {
		if err := b.overlayConfigLocked(project, zone, in, c.Config.PreservedState); err != nil {
			return err
		}
	}

	if m == nil {
		return nil
	}

	b.restoreIPsLocked(project, zone, in, m)

	return nil
}

func (rec *migRec) memberNamed(name string) *memberRec {
	for _, m := range rec.Members {
		if m.Name == name {
			return m
		}
	}

	return nil
}

func (b *InMemoryBackend) reuseDisksLocked(project string, rec *migRec, zone string, in *computeapi.Instance) {
	for i, ad := range in.Disks {
		dev := deviceName(ad.DeviceName, i)
		if _, ok := rec.diskRetention(in.Name, dev); !ok || ad.InitializeParams == nil {
			continue
		}

		name := ad.InitializeParams.DiskName
		if name == "" {
			name = in.Name
			if i > 0 {
				name += "-" + dev
			}
		}

		if d, found := b.disks[key(project, zone, name)]; found {
			ad.Source, ad.InitializeParams, ad.DeviceName = d.SelfLink, nil, dev
		}
	}
}

func (b *InMemoryBackend) overlayConfigLocked(
	project, zone string, in *computeapi.Instance, ps *computeapi.PreservedState,
) *APIError {
	if len(ps.Metadata) > 0 {
		if in.Metadata == nil {
			in.Metadata = &computeapi.Metadata{}
		}

		setMetadata(in.Metadata, ps.Metadata)
	}

	for _, dev := range slices.Sorted(maps.Keys(ps.Disks)) {
		pd := ps.Disks[dev]
		att := &computeapi.AttachedDisk{
			Source: pd.Source, DeviceName: dev, Mode: pd.Mode, AutoDelete: pd.AutoDelete == autoDeleteOnPerm,
		}

		if i := slices.IndexFunc(in.Disks, func(d *computeapi.AttachedDisk) bool { return d.DeviceName == dev }); i >= 0 {
			att.Boot, att.Interface = in.Disks[i].Boot, in.Disks[i].Interface
			in.Disks[i] = att

			continue
		}

		in.Disks = append(in.Disks, att)
	}

	region := regionOfZone(zone)

	for _, k := range slices.Sorted(maps.Keys(ps.InternalIPs)) {
		ip, err := b.resolvePreservedIPLocked(project, region, "internalIPs", ps.InternalIPs[k].IpAddress.Address)
		if err != nil {
			return err
		}

		if i, ok := nicNumber(k); ok && i < len(in.NetworkInterfaces) {
			in.NetworkInterfaces[i].NetworkIP = ip
		}
	}

	for _, k := range slices.Sorted(maps.Keys(ps.ExternalIPs)) {
		ip, err := b.resolvePreservedIPLocked(project, region, "externalIPs", ps.ExternalIPs[k].IpAddress.Address)
		if err != nil {
			return err
		}

		setNatIP(in, k, ip)
	}

	return nil
}

func (b *InMemoryBackend) restoreIPsLocked(project, zone string, in *computeapi.Instance, m *memberRec) {
	for k, ip := range m.KeptInt {
		if i, ok := nicNumber(k); ok && i < len(in.NetworkInterfaces) && in.NetworkInterfaces[i].NetworkIP == "" {
			in.NetworkInterfaces[i].NetworkIP = ip
		}
	}

	region := regionOfZone(zone)

	for k, ip := range m.KeptExt {
		name := keptAddressName(m.Name, k)
		if b.findExternalAddressLocked(region, ip) == nil {
			a, err := b.buildAddressLocked(project, region, &computeapi.Address{
				Name: name, AddressType: addrExternal, Address: ip,
			})
			if err != nil {
				continue
			}

			b.addresses[key(project, scopeRegion, region, name)] = a
		}

		setNatIP(in, k, ip)
	}

	m.KeptInt, m.KeptExt = nil, nil
}

func setNatIP(in *computeapi.Instance, nic, ip string) {
	i, ok := nicNumber(nic)
	if !ok || i >= len(in.NetworkInterfaces) {
		return
	}

	n := in.NetworkInterfaces[i]
	if len(n.AccessConfigs) == 0 {
		n.AccessConfigs = []*computeapi.AccessConfig{{Name: natName, Type: natType}}
	}

	n.AccessConfigs[0].NatIP = ip
}

func setMetadata(md *computeapi.Metadata, kv map[string]string) {
	for _, k := range slices.Sorted(maps.Keys(kv)) {
		v := kv[k]

		if i := slices.IndexFunc(md.Items, func(it *computeapi.MetadataItems) bool { return it.Key == k }); i >= 0 {
			md.Items[i].Value = &v

			continue
		}

		md.Items = append(md.Items, &computeapi.MetadataItems{Key: k, Value: &v})
	}
}

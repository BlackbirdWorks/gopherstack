package azurearm

import (
	"maps"
	"net/http"
	"slices"
	"strings"
)

const (
	auxPower       = "power"
	auxGeneralized = "generalized"
	auxManagedBy   = "managedBy"
	auxUpload      = "upload"
	auxSpecialized = "specialized"

	powerRunning     = "running"
	powerStopped     = "stopped"
	powerDeallocated = "deallocated"

	deleteOptionDelete = "Delete"
	deleteOptionDetach = "Detach"

	cmpFieldHardware  = "hardwareProfile"
	cmpFieldStorage   = "storageProfile"
	cmpFieldNetwork   = "networkProfile"
	cmpFieldAvSet     = "availabilitySet"
	cmpFieldOSDisk    = "osDisk"
	cmpFieldDataDisks = "dataDisks"
	cmpFieldCreateOpt = "createOption"
	cmpFieldDelOpt    = "deleteOption"
	cmpFieldLun       = "lun"
	cmpFieldStorType  = "storageAccountType"
	cmpFieldInstView  = "instanceView"
	cmpFieldImageRef  = "imageReference"

	cmpMaxLun         = 63
	cmpMaxOSDiskGB    = 4095
	cmpDefaultLinuxGB = 30
	cmpDefaultWinGB   = 127
)

//nolint:gochecknoglobals // static read-only list
var vmReadOnlyProps = []string{
	cmpFieldVMID, fieldProvisioningState, cmpFieldInstView, cmpFieldTimeMade,
}

// diskDetach records a disk leaving a VM; remove deletes it afterwards.
type diskDetach struct {
	disk   *cmpRes
	remove bool
}

type diskResize struct {
	disk *cmpRes
	gb   int
}

// diskPlan is the set of disk changes a VM write makes, computed before anything is mutated.
type diskPlan struct {
	create []*cmpRes
	attach []*cmpRes
	detach []diskDetach
	resize []diskResize
}

func parseZones(body map[string]any, old *cmpRes, limit int) ([]string, error) {
	raw, present := body[cmpFieldZones]
	zones := strList(raw)

	if !present && old != nil {
		return old.Zones, nil
	}

	if len(zones) > limit {
		return nil, cmpInvalid("A %s can be placed in at most %d availability zone(s).", cmpZoneOwner(limit), limit)
	}

	for _, z := range zones {
		if z != "1" && z != "2" && z != "3" {
			return nil, cmpInvalid("The zone '%s' is not valid; allowed values are 1, 2 and 3.", z)
		}
	}

	if old != nil && !slices.Equal(old.Zones, zones) {
		return nil, cmpChanged(cmpFieldZones)
	}

	return zones, nil
}

func cmpZoneOwner(limit int) string {
	if limit > 1 {
		return "virtual machine scale set"
	}

	return "virtual machine"
}

func parseVMSize(props map[string]any) (vmSizeDef, map[string]any, error) {
	hw := cloneProps(mapOf(props[cmpFieldHardware]))

	name := strOf(hw, "vmSize")
	if name == "" {
		return vmSizeDef{}, nil, cmpInvalid("Required parameter 'hardwareProfile.vmSize' is missing (null).")
	}

	size, ok := findVMSize(name)
	if !ok {
		return vmSizeDef{}, nil, cmpInvalid("The VM size '%s' is not available. Use the vmSizes or resourceSkus list "+
			"for the sizes offered in this location.", name)
	}

	hw["vmSize"] = size.name

	return size, hw, nil
}

func (p *ComputeProvider) putVM(id ResourceID, body map[string]any) (*cmpRes, error) {
	trusted, _ := body[vmTrustedKey].(bool)

	if err := validCmpName(id.Names[0], cmpMaxVMNameLen); err != nil {
		return nil, err
	}

	r, old, err := p.upsert(cmpVMs, id, body)
	if err != nil {
		return nil, err
	}

	if err = p.applyIdentity(r, old, body); err != nil {
		return nil, err
	}

	if r.Zones, err = parseZones(body, old, 1); err != nil {
		return nil, err
	}

	r.Extra = extraFields(body, "plan", "extendedLocation")
	props := propsOf(body)

	size, hw, err := parseVMSize(props)
	if err != nil {
		return nil, err
	}

	newProps := cloneProps(props)
	for _, k := range vmReadOnlyProps {
		delete(newProps, k)
	}

	newProps[cmpFieldHardware] = hw

	if err = p.vmAvailabilitySet(r, old, props, newProps); err != nil {
		return nil, err
	}

	if err = p.vmScaleSetRef(r, props, newProps); err != nil {
		return nil, err
	}

	osType := vmOSType(mergeStored(props, old))

	if osType, err = p.vmImageOS(r, old, props, osType); err != nil {
		return nil, err
	}

	plan, err := p.vmStorage(r, old, props, newProps, size, osType)
	if err != nil {
		return nil, err
	}

	if err = vmOSProfile(r, old, props, newProps, osType, trusted); err != nil {
		return nil, err
	}

	nics, err := vmNetwork(r, old, props, newProps)
	if err != nil {
		return nil, err
	}

	r.Props = newProps

	return r, p.commitVM(r, old, nics, plan)
}

func mergeStored(props map[string]any, old *cmpRes) map[string]any {
	if old == nil {
		return props
	}

	out := map[string]any{}
	maps.Copy(out, old.Props)
	maps.Copy(out, props)

	return out
}

func (p *ComputeProvider) vmAvailabilitySet(r, old *cmpRes, props, newProps map[string]any) error {
	as := mapOf(props[cmpFieldAvSet])
	asID := strOf(as, cmpFieldID)

	if old != nil {
		oldID := strOf(mapOf(old.Props[cmpFieldAvSet]), cmpFieldID)
		if props[cmpFieldAvSet] == nil && oldID != "" {
			newProps[cmpFieldAvSet] = deepClone(old.Props[cmpFieldAvSet])

			return nil
		}

		if !strings.EqualFold(oldID, asID) {
			return cmpChanged("availabilitySet.id")
		}
	}

	if asID == "" {
		delete(newProps, cmpFieldAvSet)

		return nil
	}

	set, ok := p.item(cmpAvSets, asID)
	if !ok {
		return netErr(
			http.StatusNotFound,
			netCodeNotFound,
			"The Resource '%s' was not found.",
			strings.TrimPrefix(asID, "/"),
		)
	}

	if set.Location != r.Location {
		return cmpInvalid(
			"The availability set %s is in location %s; the virtual machine must be in the same location.",
			asID,
			set.Location,
		)
	}

	if len(r.Zones) > 0 {
		return cmpInvalid("A virtual machine cannot be placed in both an availability zone and an availability set.")
	}

	newProps[cmpFieldAvSet] = map[string]any{cmpFieldID: set.armID()}

	return nil
}

// vmImageOS resolves an image id reference on a new machine and returns the operating system it implies.
func (p *ComputeProvider) vmImageOS(r, old *cmpRes, props map[string]any, osType string) (string, error) {
	if old != nil {
		return osType, nil
	}

	info, err := p.vmImage(r, mapOf(mapOf(props[cmpFieldStorage])[cmpFieldImageRef]))
	if err != nil || info == nil {
		return osType, err
	}

	if explicit := explicitOS(props); explicit != "" && !strings.EqualFold(explicit, info.osType) {
		return "", cmpInvalid("The resource with id '%s' has a different OS type ['%s'] than the parent gallery image "+
			"OS type generation ['%s']", strOf(mapOf(mapOf(props[cmpFieldStorage])[cmpFieldImageRef]), cmpFieldID),
			explicit, info.osType)
	}

	if strings.EqualFold(info.osState, osStateSpec) {
		r.Aux[auxSpecialized] = cmpTrue
	}

	return info.osType, nil
}

func explicitOS(props map[string]any) string {
	osp := mapOf(props[cmpFieldOSProfile])

	switch {
	case osp[cmpFieldLinuxCfg] != nil:
		return osLinux
	case osp[cmpFieldWinCfg] != nil:
		return osWindows
	}

	return strOf(mapOf(mapOf(props[cmpFieldStorage])[cmpFieldOSDisk]), cmpKeyOSType)
}

func specializedOSProfile(newProps, osp map[string]any) error {
	if len(osp) > 0 {
		return cmpInvalid("Parameter 'osProfile' is not allowed.")
	}

	delete(newProps, cmpFieldOSProfile)

	return nil
}

func vmOSProfile(r, old *cmpRes, props, newProps map[string]any, osType string, trusted bool) error {
	osp := mapOf(props[cmpFieldOSProfile])
	osDisk := mapOf(mapOf(newProps[cmpFieldStorage])[cmpFieldOSDisk])
	attach := strings.EqualFold(strOf(osDisk, cmpFieldCreateOpt), "Attach")

	if r.Aux[auxSpecialized] == cmpTrue {
		return specializedOSProfile(newProps, osp)
	}

	switch {
	case attach && len(osp) > 0:
		return cmpInvalid("Parameter 'osProfile' is not allowed.")
	case attach:
		delete(newProps, cmpFieldOSProfile)

		return nil
	case len(osp) == 0 && old == nil:
		return cmpInvalid("Required parameter 'osProfile' is missing (null).")
	case len(osp) == 0:
		newProps[cmpFieldOSProfile] = deepClone(old.Props[cmpFieldOSProfile])

		return nil
	}

	var existing map[string]any
	if trusted {
		existing = map[string]any{}
	}

	if old != nil {
		existing = mapOf(old.Props[cmpFieldOSProfile])

		for _, k := range []string{cmpFieldComputer, cmpFieldAdminUser} {
			if v := strOf(osp, k); v != "" && !strings.EqualFold(v, strOf(existing, k)) && existing != nil {
				return cmpChanged("osProfile." + k)
			}
		}
	}

	stored, err := buildOSProfile(r.Name, osType, osp, existing)
	if err != nil {
		return err
	}

	newProps[cmpFieldOSProfile] = stored

	return nil
}

func vmNetwork(r, old *cmpRes, props, newProps map[string]any) ([]nicAttachment, error) {
	np := mapOf(props[cmpFieldNetwork])
	if len(np) == 0 && old != nil {
		np = mapOf(old.Props[cmpFieldNetwork])
	}

	if len(anyList(np["networkInterfaceConfigurations"])) > 0 {
		return nil, cmpInvalid("networkProfile.networkInterfaceConfigurations is not supported; create the network " +
			"interfaces with Microsoft.Network and reference them in networkProfile.networkInterfaces.")
	}

	raw := anyList(np["networkInterfaces"])

	switch {
	case len(raw) == 0:
		return nil, cmpInvalid("Required parameter 'networkProfile.networkInterfaces' is missing (null): a virtual " +
			"machine needs at least one network interface.")
	case len(raw) > cmpMaxNICsPerVM:
		return nil, cmpInvalid("A virtual machine can have at most %d network interfaces.", cmpMaxNICsPerVM)
	}

	refs, stored, err := parseNICRefs(raw)
	if err != nil {
		return nil, err
	}

	if err = checkNICChange(r, old, refs); err != nil {
		return nil, err
	}

	newProps[cmpFieldNetwork] = map[string]any{"networkInterfaces": stored}

	return refs, nil
}

// parseNICRefs validates networkProfile.networkInterfaces; a lone interface is the primary one.
func parseNICRefs(raw []any) ([]nicAttachment, []any, error) {
	refs := make([]nicAttachment, 0, len(raw))
	stored := make([]any, 0, len(raw))
	primaries := 0

	for _, e := range raw {
		m := mapOf(e)

		nicID := strOf(m, cmpFieldID)
		if nicID == "" {
			return nil, nil, cmpInvalid("Required parameter 'networkProfile.networkInterfaces.id' is missing (null).")
		}

		mp := mapOf(m[fieldProperties])
		primary, _ := mp[cmpFieldPrimary].(bool)

		opt, err := validDeleteOption(mp)
		if err != nil {
			return nil, nil, err
		}

		if primary {
			primaries++
		}

		refs = append(refs, nicAttachment{id: nicID, primary: primary})
		stored = append(stored, map[string]any{
			cmpFieldID: nicID, fieldProperties: map[string]any{cmpFieldPrimary: primary, cmpFieldDelOpt: opt},
		})
	}

	switch {
	case len(raw) == 1:
		refs[0].primary = true
		mapOf(mapOf(stored[0])[fieldProperties])[cmpFieldPrimary] = true
	case primaries != 1:
		return nil, nil, cmpInvalid("A virtual machine with more than one network interface must have exactly one " +
			"primary network interface.")
	}

	return refs, stored, nil
}

func checkNICChange(r, old *cmpRes, refs []nicAttachment) error {
	if old == nil || r.Aux[auxPower] == powerDeallocated {
		return nil
	}

	oldIDs := vmNICIDs(old)
	newIDs := make([]string, 0, len(refs))

	for _, ref := range refs {
		newIDs = append(newIDs, ref.id)
	}

	if !sameIDSet(oldIDs, newIDs) {
		return cmpNotAllowed("Network interfaces can only be added to or removed from virtual machine %s while it is "+
			"stopped (deallocated).", r.Name)
	}

	return nil
}

func sameIDSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for _, x := range a {
		if !slices.ContainsFunc(b, func(y string) bool { return strings.EqualFold(x, y) }) {
			return false
		}
	}

	return true
}

func vmNICIDs(r *cmpRes) []string {
	raw := anyList(mapOf(r.Props[cmpFieldNetwork])["networkInterfaces"])
	out := make([]string, 0, len(raw))

	for _, e := range raw {
		out = append(out, strOf(mapOf(e), cmpFieldID))
	}

	return out
}

// commitVM claims the network interfaces first (the only step that can fail), then applies the disk plan.
func (p *ComputeProvider) commitVM(r, old *cmpRes, nics []nicAttachment, plan *diskPlan) error {
	vmID := r.armID()

	if err := p.net.claimNICs(vmID, r.Location, nics); err != nil {
		return err
	}

	if old != nil {
		keep := make([]string, 0, len(nics))
		for _, n := range nics {
			keep = append(keep, n.id)
		}

		var gone []string

		for _, id := range vmNICIDs(old) {
			if !slices.ContainsFunc(keep, func(k string) bool { return strings.EqualFold(k, id) }) {
				gone = append(gone, id)
			}
		}

		p.net.releaseNICs(vmID, gone, false)
	} else {
		r.Aux[auxPower] = powerRunning
	}

	for _, d := range plan.create {
		p.items[cmpKey(cmpDisks, d.RG, d.Name)] = d
	}

	for _, d := range plan.attach {
		d.Aux[auxManagedBy] = vmID
	}

	for _, rs := range plan.resize {
		growDisk(rs.disk, rs.gb)
	}

	for _, dt := range plan.detach {
		delete(dt.disk.Aux, auxManagedBy)

		if dt.remove {
			delete(p.items, cmpKey(cmpDisks, dt.disk.RG, dt.disk.Name))
		}
	}

	p.items[cmpKey(cmpVMs, r.RG, r.Name)] = r

	return nil
}

func (p *ComputeProvider) deleteVM(r *cmpRes) {
	vmID := r.armID()
	sp := mapOf(r.Props[cmpFieldStorage])

	disks := append([]any{sp[cmpFieldOSDisk]}, anyList(sp[cmpFieldDataDisks])...)
	for _, e := range disks {
		m := mapOf(e)

		d, ok := p.item(cmpDisks, strOf(mapOf(m[cmpFieldManagedDsk]), cmpFieldID))
		if !ok || !strings.EqualFold(d.Aux[auxManagedBy], vmID) {
			continue
		}

		delete(d.Aux, auxManagedBy)

		if strOf(m, cmpFieldDelOpt) == deleteOptionDelete {
			delete(p.items, cmpKey(cmpDisks, d.RG, d.Name))
		}
	}

	for _, e := range anyList(mapOf(r.Props[cmpFieldNetwork])["networkInterfaces"]) {
		m := mapOf(e)
		remove := strOf(mapOf(m[fieldProperties]), cmpFieldDelOpt) == deleteOptionDelete
		p.net.releaseNICs(vmID, []string{strOf(m, cmpFieldID)}, remove)
	}
}

func (p *ComputeProvider) vmProps(r *cmpRes, expand bool) map[string]any {
	props := cloneProps(r.Props)
	props[cmpFieldVMID] = r.UID
	props[fieldProvisioningState] = provisioningStateSucceeded
	props[cmpFieldTimeMade] = r.Created.Format(timeCreatedLayout)

	sp := mapOf(props[cmpFieldStorage])
	p.syncDisk(mapOf(sp[cmpFieldOSDisk]))

	for _, d := range anyList(sp[cmpFieldDataDisks]) {
		p.syncDisk(mapOf(d))
	}

	if as := mapOf(props[cmpFieldAvSet]); as != nil {
		if set, ok := p.item(cmpAvSets, strOf(as, cmpFieldID)); ok {
			as[cmpFieldID] = set.armID()
		}
	}

	if expand {
		props[cmpFieldInstView] = p.vmInstanceView(r)
	}

	return props
}

const timeCreatedLayout = "2006-01-02T15:04:05.0000000Z07:00"

func (p *ComputeProvider) syncDisk(d map[string]any) {
	md := mapOf(d[cmpFieldManagedDsk])
	if md == nil {
		return
	}

	ds, ok := p.item(cmpDisks, strOf(md, cmpFieldID))
	if !ok {
		return
	}

	md[cmpFieldID] = ds.armID()
	md[cmpFieldStorType] = strOf(ds.SKU, fieldName)
	d[cmpFieldDiskSizeGB] = diskSizeOf(ds)
}

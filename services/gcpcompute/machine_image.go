package gcpcompute

import (
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collMachineImages = "machineImages"
	gbShift           = 30
)

func machineImageTarget(project string, m *computeapi.MachineImage) target {
	return target{project: project, scope: scopeGlobal, collection: collMachineImages, name: m.Name, id: m.Id}
}

func savedDisksOf(r *instRec, b *InMemoryBackend, project, zone string) ([]*computeapi.SavedAttachedDisk, []*computeapi.SavedDisk, int64) {
	var (
		attached []*computeapi.SavedAttachedDisk
		saved    []*computeapi.SavedDisk
		total    int64
	)

	for _, ad := range r.Inst.Disks {
		d := b.disks[diskKeyFor(project, zone, ad.Source)]
		if d == nil {
			continue
		}

		bytes := d.SizeGb << gbShift
		total += bytes

		attached = append(attached, &computeapi.SavedAttachedDisk{
			Kind: "compute#savedAttachedDisk", AutoDelete: ad.AutoDelete, Boot: ad.Boot, DeviceName: ad.DeviceName,
			DiskSizeGb: d.SizeGb, DiskType: lastSegment(d.Type), GuestOsFeatures: d.GuestOsFeatures,
			Index: ad.Index, Interface: ad.Interface, Licenses: d.Licenses, Mode: ad.Mode, Source: d.SelfLink,
			StorageBytes: bytes, StorageBytesStatus: "UP_TO_DATE", Type: ad.Type,
		})
		saved = append(saved, &computeapi.SavedDisk{
			Kind: "compute#savedDisk", Architecture: d.Architecture, SourceDisk: d.SelfLink,
			StorageBytes: bytes, StorageBytesStatus: "UP_TO_DATE",
		})
	}

	return attached, saved, total
}

func (b *InMemoryBackend) insertMachineImage(
	project string, in *computeapi.MachineImage,
) (*computeapi.Operation, error) {
	b.mu.Lock("InsertMachineImage")
	defer b.mu.Unlock()

	if err := firstErr(validName("name", in.Name), validLabels(in.Labels)); err != nil {
		return nil, err
	}

	if _, dup := b.machineImages[key(project, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeGlobal, "", collMachineImages, in.Name))
	}

	if in.SourceInstance == "" {
		return nil, errRequired("sourceInstance")
	}

	r, ok := parseRef(in.SourceInstance, "instances", project, "")
	if !ok || r.scope != scopeZone || r.loc == "" {
		return nil, errField("sourceInstance", in.SourceInstance, "The URL is malformed.")
	}

	inst, err := b.getInstLocked(r.project, r.loc, r.name)
	if err != nil {
		return nil, err
	}

	attached, saved, total := savedDisksOf(inst, b, r.project, r.loc)
	i := inst.Inst

	locs := in.StorageLocations
	if len(locs) == 0 {
		locs = []string{storageLocationFor(r.loc)}
	}

	m := &computeapi.MachineImage{
		Kind: "compute#machineImage", Id: newID(), CreationTimestamp: b.stamp(), Name: in.Name,
		Description: in.Description, Status: statusReady, SelfLink: globalLink(project, collMachineImages, in.Name),
		Labels: in.Labels, LabelFingerprint: labelFP(in.Labels), SourceInstance: i.SelfLink,
		StorageLocations: locs, TotalStorageBytes: total, SavedDisks: saved, GuestFlush: in.GuestFlush,
		SourceInstanceProperties: &computeapi.SourceInstanceProperties{
			CanIpForward: i.CanIpForward, DeletionProtection: i.DeletionProtection, Description: i.Description,
			Disks: attached, GuestAccelerators: i.GuestAccelerators, Labels: i.Labels, MachineType: lastSegment(i.MachineType),
			Metadata: i.Metadata, MinCpuPlatform: i.MinCpuPlatform, NetworkInterfaces: i.NetworkInterfaces,
			Scheduling: i.Scheduling, ServiceAccounts: i.ServiceAccounts, Tags: i.Tags,
		},
	}
	m = clone(m)
	b.machineImages[key(project, in.Name)] = m

	return b.newOpLocked("insert", machineImageTarget(project, m), b.timing.Done), nil
}

func (b *InMemoryBackend) getMachineImageLocked(project, name string) (*computeapi.MachineImage, *APIError) {
	m, ok := b.machineImages[key(project, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeGlobal, "", collMachineImages, name))
	}

	return m, nil
}

func (b *InMemoryBackend) getMachineImage(project, name string) (*computeapi.MachineImage, error) {
	b.mu.RLock("GetMachineImage")
	defer b.mu.RUnlock()

	m, err := b.getMachineImageLocked(project, name)
	if err != nil {
		return nil, err
	}

	return clone(m), nil
}

func (b *InMemoryBackend) listMachineImages(project string) []entry {
	b.mu.RLock("ListMachineImages")
	defer b.mu.RUnlock()

	var out []entry

	for k, m := range b.machineImages {
		if strings.HasPrefix(k, project+"/") {
			out = append(out, entry{scope: scopeGlobal, item: clone(m)})
		}
	}

	return out
}

func (b *InMemoryBackend) deleteMachineImage(project, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteMachineImage")
	defer b.mu.Unlock()

	m, err := b.getMachineImageLocked(project, name)
	if err != nil {
		return nil, err
	}

	delete(b.machineImages, key(project, name))

	return b.newOpLocked("delete", machineImageTarget(project, m), b.timing.Done), nil
}

func (b *InMemoryBackend) setMachineImageLabels(project, name string, req labelsRequest) (*computeapi.Operation, error) {
	b.mu.Lock("SetMachineImageLabels")
	defer b.mu.Unlock()

	m, err := b.getMachineImageLocked(project, name)
	if err != nil {
		return nil, err
	}

	labels, fp, lerr := applyLabels(m.Labels, m.LabelFingerprint, req)
	if lerr != nil {
		return nil, lerr
	}

	m.Labels, m.LabelFingerprint = labels, fp

	return b.newOpLocked("setLabels", machineImageTarget(project, m), b.timing.Done), nil
}

// instanceFromMachineImage builds an instance request from a machine image with the request body merged over it;
// disks are recreated empty at the saved size and addresses are reallocated.
func (b *InMemoryBackend) instanceFromMachineImage(project, imageRef string, raw map[string]any) (*computeapi.Instance, error) {
	b.mu.RLock("InstanceFromMachineImage")
	defer b.mu.RUnlock()

	r, rerr := refGlobal(imageRef, collMachineImages, project)
	if rerr != nil {
		return nil, rerr
	}

	m, err := b.getMachineImageLocked(r.project, r.name)
	if err != nil {
		return nil, err
	}

	name, _ := raw["name"].(string)
	src := m.SourceInstanceProperties
	props := &computeapi.InstanceProperties{
		CanIpForward: src.CanIpForward, Description: src.Description, GuestAccelerators: src.GuestAccelerators,
		Labels: src.Labels, MachineType: src.MachineType, Metadata: src.Metadata, MinCpuPlatform: src.MinCpuPlatform,
		Scheduling: src.Scheduling, ServiceAccounts: src.ServiceAccounts, Tags: src.Tags,
		NetworkInterfaces: stripNICAddresses(src.NetworkInterfaces),
	}

	for _, sd := range src.Disks {
		props.Disks = append(props.Disks, &computeapi.AttachedDisk{
			Boot: sd.Boot, AutoDelete: sd.AutoDelete, DeviceName: sd.DeviceName, Mode: sd.Mode, Interface: sd.Interface,
			InitializeParams: &computeapi.AttachedDiskInitializeParams{DiskSizeGb: sd.DiskSizeGb, DiskType: sd.DiskType},
		})
	}

	inst := instanceFromProps(props, name)
	for i, d := range inst.Disks {
		d.InitializeParams.DiskName = name
		if i > 0 {
			d.InitializeParams.DiskName = name + "-" + strconv.Itoa(i)
		}
	}

	base := toDoc(inst)
	mergePatch(base, raw)

	out, derr := fromDoc[computeapi.Instance](base)
	if derr != nil {
		return nil, errInvalid("Invalid JSON payload received: %v", derr)
	}

	return out, nil
}

func stripNICAddresses(in []*computeapi.NetworkInterface) []*computeapi.NetworkInterface {
	out := make([]*computeapi.NetworkInterface, 0, len(in))

	for _, n := range in {
		c := clone(n)
		c.NetworkIP, c.Name, c.Fingerprint = "", "", ""

		for _, ac := range c.AccessConfigs {
			ac.NatIP = ""
		}

		out = append(out, c)
	}

	return out
}

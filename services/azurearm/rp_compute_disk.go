package azurearm

import (
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	diskStateUnattached = "Unattached"
	diskStateAttached   = "Attached"
	diskStateReserved   = "Reserved"
	diskStateUpload     = "ReadyToUpload"

	createEmpty     = "Empty"
	createFromImage = "FromImage"
	createCopy      = "Copy"
	createUpload    = "Upload"
	createAttach    = "Attach"

	cmpFieldCreation  = "creationData"
	cmpFieldDiskState = "diskState"
	cmpMaxDiskGB      = 32767
	cmpMaxDiskGBV2    = 65536
	bytesPerGiB       = 1 << 30
)

//nolint:gochecknoglobals // static read-only list
var diskReadOnlyProps = []string{
	cmpFieldDiskState, fieldProvisioningState, cmpFieldTimeMade, "uniqueId", "diskSizeBytes", "managedByExtended",
	"shareInfo", "completionPercent", "lastOwnershipUpdateTime", "diskSizeGB",
}

func diskSizeOf(d *cmpRes) int { return intOf(d.Props[cmpFieldDiskSizeGB]) }

func growDisk(d *cmpRes, gb int) {
	d.Props[cmpFieldDiskSizeGB] = gb
	d.Etag = newEtag()
}

func (p *ComputeProvider) diskSKU(body map[string]any, old *cmpRes, snapshot bool) (map[string]any, error) {
	name := strOf(mapOf(body[cmpFieldSKU]), fieldName)
	if name == "" && old != nil {
		name = strOf(old.SKU, fieldName)
	}

	if name == "" {
		name = defaultDiskSKU()
	}

	list := cmpDiskSKUs
	if snapshot {
		list = cmpSnapshotSKUs
	}

	canon, ok := canonicalIn(list, name)
	if !ok {
		return nil, cmpInvalid("The sku name '%s' is not valid; allowed values are %s.", name, strings.Join(list, ", "))
	}

	return map[string]any{fieldName: canon, cmpKeyTier: diskTier(canon)}, nil
}

func (p *ComputeProvider) putDisk(id ResourceID, body map[string]any, snapshot bool) (*cmpRes, error) {
	typ := cmpDisks
	if snapshot {
		typ = cmpSnaps
	}

	if err := validCmpName(id.Names[0], cmpMaxNameLen); err != nil {
		return nil, err
	}

	r, old, err := p.upsert(typ, id, body)
	if err != nil {
		return nil, err
	}

	if r.SKU, err = p.diskSKU(body, old, snapshot); err != nil {
		return nil, err
	}

	if !snapshot {
		if r.Zones, err = diskZones(body, old); err != nil {
			return nil, err
		}
	}

	r.Extra = extraFields(body, "extendedLocation")
	props := cloneProps(propsOf(body))

	for _, k := range diskReadOnlyProps {
		delete(props, k)
	}

	if old == nil {
		err = p.createDiskProps(r, props, propsOf(body), snapshot)
	} else {
		err = p.updateDiskProps(r, old, props, propsOf(body), snapshot)
	}

	if err != nil {
		return nil, err
	}

	r.Props = props
	p.items[cmpKey(typ, r.RG, r.Name)] = r

	return r, nil
}

func diskZones(body map[string]any, old *cmpRes) ([]string, error) {
	raw, present := body[cmpFieldZones]
	zones := strList(raw)

	if !present && old != nil {
		return old.Zones, nil
	}

	if len(zones) > 1 {
		return nil, cmpInvalid("A disk can be placed in only one availability zone.")
	}

	for _, z := range zones {
		if z != "1" && z != "2" && z != "3" {
			return nil, cmpInvalid("The zone '%s' is not valid; allowed values are 1, 2 and 3.", z)
		}
	}

	if old != nil && len(old.Zones) != len(zones) {
		return nil, cmpChanged(cmpFieldZones)
	}

	return zones, nil
}

func (p *ComputeProvider) sourceOf(idStr string) (*cmpRes, error) {
	if s, ok := p.item(cmpDisks, idStr); ok {
		return s, nil
	}

	if s, ok := p.item(cmpSnaps, idStr); ok {
		return s, nil
	}

	return nil, netErr(http.StatusNotFound, netCodeNotFound,
		"The source resource '%s' was not found.", strings.TrimPrefix(idStr, "/"))
}

// createDiskProps validates creationData and derives the size and operating system of a new disk or snapshot.
func (p *ComputeProvider) createDiskProps(r *cmpRes, props, raw map[string]any, snapshot bool) error {
	cd := cloneProps(mapOf(raw[cmpFieldCreation]))

	opt, ok := canonicalIn(
		[]string{createEmpty, createFromImage, createCopy, createUpload},
		strOf(cd, cmpFieldCreateOpt),
	)
	if !ok {
		return cmpInvalid("The creationData.createOption '%s' is not supported; use Empty, FromImage, Copy or Upload.",
			strOf(cd, cmpFieldCreateOpt))
	}

	if snapshot && opt != createCopy {
		return cmpInvalid("Snapshots are created with creationData.createOption 'Copy'.")
	}

	cd[cmpFieldCreateOpt] = opt
	gb := intOf(raw[cmpFieldDiskSizeGB])

	var err error

	switch opt {
	case createEmpty:
		err = checkEmptyDisk(r, gb)
	case createFromImage:
		gb, err = imageDiskSize(cd, raw, gb)
	case createCopy:
		gb, err = p.copyDisk(cd, props, gb)
	default:
		gb, err = uploadDisk(r, cd)
	}

	if err != nil {
		return err
	}

	props[cmpFieldCreation] = cd
	props[cmpFieldDiskSizeGB] = gb

	return nil
}

func checkEmptyDisk(r *cmpRes, gb int) error {
	limit := cmpMaxDiskGB
	if sku := strOf(r.SKU, fieldName); sku == "PremiumV2_LRS" || sku == "UltraSSD_LRS" {
		limit = cmpMaxDiskGBV2
	}

	if gb < 1 || gb > limit {
		return cmpInvalid("The disk size %d GB is not valid; creationData.createOption 'Empty' requires diskSizeGB "+
			"between 1 and %d.", gb, limit)
	}

	return nil
}

func imageDiskSize(cd, raw map[string]any, gb int) (int, error) {
	ir, gr := mapOf(cd["imageReference"]), mapOf(cd["galleryImageReference"])
	if strOf(ir, cmpFieldID) == "" && strOf(ir, "sharedGalleryImageId") == "" &&
		strOf(ir, "communityGalleryImageId") == "" && strOf(gr, cmpFieldID) == "" {
		return 0, cmpInvalid("Required parameter 'creationData.imageReference.id' is missing (null).")
	}

	switch {
	case gb != 0:
		return gb, nil
	case strings.EqualFold(strOf(raw, cmpKeyOSType), osWindows):
		return cmpDefaultWinGB, nil
	}

	return cmpDefaultLinuxGB, nil
}

func (p *ComputeProvider) copyDisk(cd, props map[string]any, gb int) (int, error) {
	src, err := p.sourceOf(strOf(cd, "sourceResourceId"))
	if err != nil {
		return 0, err
	}

	if gb != 0 && gb < diskSizeOf(src) {
		return 0, cmpInvalid("The disk size %d GB is smaller than the source size %d GB.", gb, diskSizeOf(src))
	}

	if gb == 0 {
		gb = diskSizeOf(src)
	}

	cd["sourceUniqueId"] = src.UID

	for _, k := range []string{cmpKeyOSType, "hyperVGeneration"} {
		if _, set := props[k]; !set && src.Props[k] != nil {
			props[k] = src.Props[k]
		}
	}

	return gb, nil
}

func uploadDisk(r *cmpRes, cd map[string]any) (int, error) {
	size := intOf(cd["uploadSizeBytes"])
	if size <= 0 {
		return 0, cmpInvalid("Required parameter 'creationData.uploadSizeBytes' is missing (null).")
	}

	r.Aux[auxUpload] = diskStateUpload

	return int(math.Ceil(float64(size) / bytesPerGiB)), nil
}

func (p *ComputeProvider) updateDiskProps(r, old *cmpRes, props, raw map[string]any, snapshot bool) error {
	cd := mapOf(old.Props[cmpFieldCreation])
	if want := strOf(mapOf(raw[cmpFieldCreation]), cmpFieldCreateOpt); want != "" &&
		!strings.EqualFold(want, strOf(cd, cmpFieldCreateOpt)) {
		return cmpChanged("creationData.createOption")
	}

	props[cmpFieldCreation] = deepClone(cd)

	cur := diskSizeOf(old)
	gb := intOf(raw[cmpFieldDiskSizeGB])

	switch {
	case gb == 0 || gb == cur:
		props[cmpFieldDiskSizeGB] = cur
	case snapshot:
		return cmpChanged(cmpFieldDiskSizeGB)
	case gb < cur:
		return cmpNotAllowed(
			"Disk resize: the new size %d GB is smaller than the current size %d GB; shrinking a disk "+
				"is not supported.",
			gb,
			cur,
		)
	default:
		if vm, ok := p.item(cmpVMs, old.Aux[auxManagedBy]); ok && vm.Aux[auxPower] != powerDeallocated {
			return cmpNotAllowed("Disk %s is attached to virtual machine %s, which must be stopped (deallocated) "+
				"before the disk is resized.", r.Name, vm.Name)
		}

		props[cmpFieldDiskSizeGB] = gb
	}

	return nil
}

func (p *ComputeProvider) diskState(r *cmpRes) string {
	if s := r.Aux[auxUpload]; s != "" {
		return s
	}

	if mb := r.Aux[auxManagedBy]; mb != "" {
		if vm, ok := p.item(cmpVMs, mb); ok && vm.Aux[auxPower] == powerDeallocated {
			return diskStateReserved
		}

		return diskStateAttached
	}

	return diskStateUnattached
}

func (p *ComputeProvider) diskProps(r *cmpRes) map[string]any {
	props := cloneProps(r.Props)
	gb := diskSizeOf(r)

	props[cmpFieldDiskSizeGB] = gb
	props["diskSizeBytes"] = int64(gb) * bytesPerGiB
	props["uniqueId"] = r.UID
	props[cmpFieldTimeMade] = r.Created.Format(timeCreatedLayout)
	props[fieldProvisioningState] = provisioningStateSucceeded
	props[cmpFieldDiskState] = p.diskState(r)

	setDefault(props, "encryption", map[string]any{fieldType: "EncryptionAtRestWithPlatformKey"})
	setDefault(props, "networkAccessPolicy", "AllowAll")
	setDefault(props, "publicNetworkAccess", "Enabled")

	if strOf(r.SKU, cmpKeyTier) == "Premium" && props[cmpKeyTier] == nil {
		props[cmpKeyTier] = premiumPerformanceTier(gb)
	}

	return props
}

func setDefault(m map[string]any, k string, v any) {
	if _, ok := m[k]; !ok {
		m[k] = v
	}
}

// premiumPerformanceTier maps a size to the baseline performance tier Azure documents for Premium SSD (P-series).
func premiumPerformanceTier(gb int) string {
	limits := []int{4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384}
	names := strings.Fields("P1 P2 P3 P4 P6 P10 P15 P20 P30 P40 P50 P60 P70")

	for i, limit := range limits {
		if gb <= limit {
			return names[i]
		}
	}

	return "P80"
}

func (p *ComputeProvider) snapshotProps(r *cmpRes) map[string]any {
	props := p.diskProps(r)
	props[cmpFieldDiskState] = diskStateUnattached
	delete(props, cmpKeyTier)
	delete(props, "networkAccessPolicy")

	return props
}

func (p *ComputeProvider) checkDiskDelete(d *cmpRes) error {
	if mb := d.Aux[auxManagedBy]; mb != "" {
		if _, ok := p.item(cmpVMs, mb); ok {
			return cmpNotAllowed("Disk %s is attached to VM %s. Detach the disk before deleting it.", d.armID(), mb)
		}
	}

	return nil
}

func newDiskRes(vm *cmpRes, name, sku string, gb int, creation map[string]any, osType string) *cmpRes {
	props := map[string]any{cmpFieldCreation: creation, cmpFieldDiskSizeGB: gb}
	if osType != "" {
		props[cmpKeyOSType] = osType
	}

	return &cmpRes{
		Type:     cmpDisks,
		RG:       vm.RG,
		Sub:      vm.Sub,
		Name:     name,
		Location: vm.Location,
		Etag:     newEtag(),
		Created:  time.Now().UTC(),
		UID:      uuid.NewString(),
		Zones:    vm.Zones,
		SKU:      map[string]any{fieldName: sku, cmpKeyTier: diskTier(sku)},
		Props:    props,
		Aux:      map[string]string{},
	}
}

package azurearm

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

func (p *ComputeProvider) vmStorage(
	r, old *cmpRes, props, newProps map[string]any, size vmSizeDef, osType string,
) (*diskPlan, error) {
	sp := cloneProps(mapOf(props[cmpFieldStorage]))

	var oldSP map[string]any

	if old != nil {
		oldSP = mapOf(old.Props[cmpFieldStorage])
		if len(sp) == 0 {
			sp = cloneProps(oldSP)
		}
	}

	if len(sp) == 0 {
		return nil, cmpInvalid("Required parameter 'storageProfile' is missing (null).")
	}

	ref := cloneProps(mapOf(sp[cmpFieldImageRef]))
	if old != nil && len(ref) == 0 {
		ref = cloneProps(mapOf(oldSP[cmpFieldImageRef]))
	}

	plan := &diskPlan{}

	osDisk, err := p.vmOSDisk(r, old, sp, oldSP, ref, size, osType, plan)
	if err != nil {
		return nil, err
	}

	data, err := p.vmDataDisks(r, old, sp, oldSP, size, plan)
	if err != nil {
		return nil, err
	}

	out := map[string]any{cmpFieldOSDisk: osDisk, cmpFieldDataDisks: data}
	if len(ref) > 0 {
		out[cmpFieldImageRef] = ref
	}

	newProps[cmpFieldStorage] = out

	return plan, nil
}

func checkSKUForSize(sku string, size vmSizeDef) error {
	if !size.premium && diskTier(sku) != skuTierStandard {
		return cmpInvalid("Requested operation cannot be performed because storage account type '%s' is not supported "+
			"for VM size '%s'.", sku, size.name)
	}

	return nil
}

func diskSKUOf(md map[string]any, size vmSizeDef) (string, error) {
	name := strOf(md, cmpFieldStorType)
	if name == "" {
		if size.premium {
			return cmpSKUPremium, nil
		}

		return defaultDiskSKU(), nil
	}

	canon, ok := canonicalIn(cmpDiskSKUs, name)
	if !ok {
		return "", cmpInvalid("The storage account type '%s' is not valid; allowed values are %s.", name,
			strings.Join(cmpDiskSKUs, ", "))
	}

	return canon, checkSKUForSize(canon, size)
}

func validDeleteOption(m map[string]any) (string, error) {
	opt := strOf(m, cmpFieldDelOpt)

	switch opt {
	case "":
		return deleteOptionDetach, nil
	case deleteOptionDelete, deleteOptionDetach:
		return opt, nil
	}

	return "", cmpInvalid("The deleteOption '%s' is not valid; allowed values are Delete and Detach.", opt)
}

func (p *ComputeProvider) vmOSDisk(
	r, old *cmpRes, sp, oldSP, ref map[string]any, size vmSizeDef, osType string, plan *diskPlan,
) (map[string]any, error) {
	osd := mapOf(sp[cmpFieldOSDisk])
	opt, ok := canonicalIn([]string{createFromImage, createAttach}, strOf(osd, cmpFieldCreateOpt))

	if old != nil {
		return p.updateOSDisk(r, old, osd, mapOf(oldSP[cmpFieldOSDisk]), plan)
	}

	if !ok {
		return nil, cmpInvalid("Required parameter 'storageProfile.osDisk.createOption' is missing (null); allowed " +
			"values are FromImage and Attach.")
	}

	del, err := validDeleteOption(osd)
	if err != nil {
		return nil, err
	}

	out := map[string]any{
		cmpKeyOSType:      osType,
		cmpFieldCreateOpt: opt,
		cmpFieldCaching:   firstNonEmpty(strOf(osd, cmpFieldCaching), cmpCachingRW),
		cmpFieldDelOpt:    del,
	}

	if v, set := osd["writeAcceleratorEnabled"]; set {
		out["writeAcceleratorEnabled"] = v
	}

	if opt == createAttach {
		return p.attachOSDisk(r, osd, out, plan)
	}

	if err = imageRefOK(ref); err != nil {
		return nil, err
	}

	return p.createOSDisk(r, osd, out, ref, size, osType, plan)
}

func (p *ComputeProvider) attachOSDisk(r *cmpRes, osd, out map[string]any, plan *diskPlan) (map[string]any, error) {
	md := mapOf(osd[cmpFieldManagedDsk])

	d, err := p.attachable(r, strOf(md, cmpFieldID))
	if err != nil {
		return nil, err
	}

	if strOf(osd, cmpKeyOSType) == "" && d.Props[cmpKeyOSType] == nil {
		return nil, cmpInvalid("Required parameter 'storageProfile.osDisk.osType' is missing (null).")
	}

	if t := firstNonEmpty(strOf(osd, cmpKeyOSType), strOf(d.Props, cmpKeyOSType)); t != "" {
		out[cmpKeyOSType] = t
	}

	out[fieldName] = d.Name
	out[cmpFieldManagedDsk] = map[string]any{cmpFieldID: d.armID(), cmpFieldStorType: strOf(d.SKU, fieldName)}
	plan.attach = append(plan.attach, d)

	return out, nil
}

func (p *ComputeProvider) createOSDisk(
	r *cmpRes, osd, out, ref map[string]any, size vmSizeDef, osType string, plan *diskPlan,
) (map[string]any, error) {
	md := mapOf(osd[cmpFieldManagedDsk])

	sku, err := diskSKUOf(md, size)
	if err != nil {
		return nil, err
	}

	gb := intOf(osd[cmpFieldDiskSizeGB])
	if gb == 0 {
		gb = cmpDefaultLinuxGB
		if osType == osWindows {
			gb = cmpDefaultWinGB
		}
	}

	if gb < 1 || gb > cmpMaxOSDiskGB {
		return nil, cmpInvalid("The OS disk size %d GB is not valid; it must be between 1 and %d.", gb, cmpMaxOSDiskGB)
	}

	name := strOf(osd, fieldName)
	if name == "" {
		name = fmt.Sprintf("%s_OsDisk_1_%s", r.Name, strings.ReplaceAll(uuid.NewString(), "-", ""))
	}

	creation := map[string]any{
		cmpFieldCreateOpt: createFromImage,
		cmpFieldImageRef:  map[string]any{cmpFieldID: platformImageID(r.Sub, r.Location, ref)},
	}

	d, err := p.newVMDisk(r, name, sku, gb, creation, osType, plan)
	if err != nil {
		return nil, err
	}

	out[fieldName] = d.Name
	out[cmpFieldManagedDsk] = map[string]any{cmpFieldID: d.armID(), cmpFieldStorType: sku}
	out[cmpFieldDiskSizeGB] = gb

	return out, nil
}

func (p *ComputeProvider) newVMDisk(
	r *cmpRes, name, sku string, gb int, creation map[string]any, osType string, plan *diskPlan,
) (*cmpRes, error) {
	if err := validCmpName(name, cmpMaxNameLen); err != nil {
		return nil, err
	}

	if _, exists := p.top(cmpDisks, r.RG, name); exists {
		return nil, netErr(http.StatusConflict, cmpCodeConflict,
			"A disk named '%s' already exists in resource group '%s'.", name, r.RG)
	}

	d := newDiskRes(r, name, sku, gb, creation, osType)
	d.Aux[auxManagedBy] = r.armID()
	plan.create = append(plan.create, d)

	return d, nil
}

func (p *ComputeProvider) updateOSDisk(
	r, old *cmpRes,
	osd, oldOS map[string]any,
	plan *diskPlan,
) (map[string]any, error) {
	out := cloneProps(oldOS)

	if c := strOf(osd, cmpFieldCaching); c != "" {
		out[cmpFieldCaching] = c
	}

	if _, set := osd[cmpFieldDelOpt]; set {
		del, err := validDeleteOption(osd)
		if err != nil {
			return nil, err
		}

		out[cmpFieldDelOpt] = del
	}

	if v, set := osd["writeAcceleratorEnabled"]; set {
		out["writeAcceleratorEnabled"] = v
	}

	gb := intOf(osd[cmpFieldDiskSizeGB])

	d, ok := p.item(cmpDisks, strOf(mapOf(oldOS[cmpFieldManagedDsk]), cmpFieldID))
	if gb == 0 || !ok || gb == diskSizeOf(d) {
		return out, nil
	}

	if err := resizeVMDisk(r, old, d, gb, plan); err != nil {
		return nil, err
	}

	return out, nil
}

// resizeVMDisk plans a grow of a disk attached to a VM, which must be deallocated.
func resizeVMDisk(r, old, d *cmpRes, gb int, plan *diskPlan) error {
	switch {
	case gb < diskSizeOf(d):
		return cmpNotAllowed("Disk resize: the new size %d GB of disk %s is smaller than the current size %d GB.",
			gb, d.Name, diskSizeOf(d))
	case old.Aux[auxPower] != powerDeallocated:
		return cmpNotAllowed("The virtual machine %s must be stopped (deallocated) before disk %s is resized.",
			r.Name, d.Name)
	}

	plan.resize = append(plan.resize, diskResize{disk: d, gb: gb})

	return nil
}

// attachable returns the existing disk idStr when it may be attached to r.
func (p *ComputeProvider) attachable(r *cmpRes, idStr string) (*cmpRes, error) {
	if idStr == "" {
		return nil, cmpInvalid("Required parameter 'managedDisk.id' is missing (null).")
	}

	d, ok := p.item(cmpDisks, idStr)
	if !ok {
		return nil, netErr(
			http.StatusNotFound,
			netCodeNotFound,
			"The Resource '%s' was not found.",
			strings.TrimPrefix(idStr, "/"),
		)
	}

	if d.Location != r.Location {
		return nil, cmpInvalid("The disk %s is in location %s; the virtual machine %s is in location %s. They must be "+
			"in the same location.", d.armID(), d.Location, r.Name, r.Location)
	}

	if user := d.Aux[auxManagedBy]; user != "" && !strings.EqualFold(user, r.armID()) {
		return nil, cmpNotAllowed("Disk %s is already attached to VM %s. A disk can be attached to only one virtual "+
			"machine.", d.armID(), user)
	}

	if d.Aux[auxUpload] != "" {
		return nil, cmpNotAllowed("Disk %s is in state %s and cannot be attached.", d.armID(), d.Aux[auxUpload])
	}

	return d, nil
}

// dataDiskReq is one validated entry of storageProfile.dataDisks.
type dataDiskReq struct {
	raw    map[string]any
	lun    int
	detach bool
}

func (p *ComputeProvider) vmDataDisks(
	r, old *cmpRes, sp, oldSP map[string]any, size vmSizeDef, plan *diskPlan,
) ([]any, error) {
	reqs, err := parseDataDisks(anyList(sp[cmpFieldDataDisks]))
	if err != nil {
		return nil, err
	}

	if len(reqs) > size.maxDisks {
		return nil, cmpNotAllowed("The maximum number of data disks allowed to be attached to a VM of this size is %d.",
			size.maxDisks)
	}

	prev := map[int]map[string]any{}

	for _, e := range anyList(oldSP[cmpFieldDataDisks]) {
		m := mapOf(e)
		prev[intOf(m[cmpFieldLun])] = m
	}

	out := make([]any, 0, len(reqs))
	seen := map[int]bool{}

	for _, q := range reqs {
		seen[q.lun] = true

		entry, kept, perr := p.planDataDisk(r, old, q, prev[q.lun], size, plan)
		if perr != nil {
			return nil, perr
		}

		if kept {
			out = append(out, entry)
		}
	}

	p.planDetaches(prev, seen, plan)

	return out, nil
}

func parseDataDisks(raw []any) ([]dataDiskReq, error) {
	reqs := make([]dataDiskReq, 0, len(raw))
	seen := map[int]bool{}

	for _, e := range raw {
		m := mapOf(e)

		lun, ok := m[cmpFieldLun].(float64)
		if !ok {
			return nil, cmpInvalid("Required parameter 'storageProfile.dataDisks.lun' is missing (null).")
		}

		l := int(lun)
		if l < 0 || l > cmpMaxLun || lun != float64(l) {
			return nil, cmpInvalid("The data disk LUN %v is not valid; it must be between 0 and %d.", lun, cmpMaxLun)
		}

		if seen[l] {
			return nil, cmpInvalid("Data disk LUN %d is used by more than one data disk; LUNs must be unique.", l)
		}

		seen[l] = true
		detach, _ := m["toBeDetached"].(bool)
		reqs = append(reqs, dataDiskReq{raw: m, lun: l, detach: detach})
	}

	return reqs, nil
}

// planDataDisk plans one data disk entry; kept is false when the entry is being detached.
func (p *ComputeProvider) planDataDisk(
	r, old *cmpRes, q dataDiskReq, prev map[string]any, size vmSizeDef, plan *diskPlan,
) (map[string]any, bool, error) {
	del, err := validDeleteOption(q.raw)
	if err != nil {
		return nil, false, err
	}

	md := mapOf(q.raw[cmpFieldManagedDsk])
	mdID := strOf(md, cmpFieldID)

	entry := map[string]any{
		cmpFieldLun:     q.lun,
		cmpFieldCaching: firstNonEmpty(strOf(q.raw, cmpFieldCaching), cmpCachingNone),
		cmpFieldDelOpt:  del,
		"toBeDetached":  false,
	}

	if v, set := q.raw["writeAcceleratorEnabled"]; set {
		entry["writeAcceleratorEnabled"] = v
	}

	switch {
	case prev != nil && (mdID == "" || strings.EqualFold(mdID, strOf(mapOf(prev[cmpFieldManagedDsk]), cmpFieldID))):
		return p.keepDataDisk(r, old, q, prev, entry, plan)
	case mdID != "":
		return p.attachDataDisk(r, q, entry, md, size, plan)
	}

	return p.createDataDisk(r, q, entry, md, size, plan)
}

func (p *ComputeProvider) keepDataDisk(
	r, old *cmpRes, q dataDiskReq, prev, entry map[string]any, plan *diskPlan,
) (map[string]any, bool, error) {
	d, ok := p.item(cmpDisks, strOf(mapOf(prev[cmpFieldManagedDsk]), cmpFieldID))
	if !ok {
		return nil, false, netErr(
			http.StatusNotFound,
			netCodeNotFound,
			"The data disk at LUN %d no longer exists.",
			q.lun,
		)
	}

	if q.detach {
		plan.detach = append(
			plan.detach,
			diskDetach{disk: d, remove: strOf(entry, cmpFieldDelOpt) == deleteOptionDelete},
		)

		return nil, false, nil
	}

	entry[fieldName] = d.Name
	entry[cmpFieldCreateOpt] = prev[cmpFieldCreateOpt]
	entry[cmpFieldManagedDsk] = map[string]any{cmpFieldID: d.armID(), cmpFieldStorType: strOf(d.SKU, fieldName)}

	if gb := intOf(q.raw[cmpFieldDiskSizeGB]); gb != 0 && gb != diskSizeOf(d) {
		if err := resizeVMDisk(r, old, d, gb, plan); err != nil {
			return nil, false, err
		}
	}

	return entry, true, nil
}

func (p *ComputeProvider) attachDataDisk(
	r *cmpRes, q dataDiskReq, entry, md map[string]any, size vmSizeDef, plan *diskPlan,
) (map[string]any, bool, error) {
	if opt := strOf(q.raw, cmpFieldCreateOpt); !strings.EqualFold(opt, createAttach) {
		return nil, false, cmpInvalid(
			"A data disk that references managedDisk.id must use createOption 'Attach', not '%s'.", opt)
	}

	d, err := p.attachable(r, strOf(md, cmpFieldID))
	if err != nil {
		return nil, false, err
	}

	if err = checkSKUForSize(strOf(d.SKU, fieldName), size); err != nil {
		return nil, false, err
	}

	if q.detach {
		return nil, false, nil
	}

	plan.attach = append(plan.attach, d)
	entry[fieldName] = d.Name
	entry[cmpFieldCreateOpt] = createAttach
	entry[cmpFieldManagedDsk] = map[string]any{cmpFieldID: d.armID(), cmpFieldStorType: strOf(d.SKU, fieldName)}

	return entry, true, nil
}

func (p *ComputeProvider) createDataDisk(
	r *cmpRes, q dataDiskReq, entry, md map[string]any, size vmSizeDef, plan *diskPlan,
) (map[string]any, bool, error) {
	if opt := strOf(q.raw, cmpFieldCreateOpt); !strings.EqualFold(opt, createEmpty) {
		return nil, false, cmpInvalid("The data disk createOption '%s' is not supported for a disk without "+
			"managedDisk.id; use 'Empty' to create a disk or 'Attach' to attach an existing one.", opt)
	}

	gb := intOf(q.raw[cmpFieldDiskSizeGB])
	if gb < 1 || gb > cmpMaxDiskGB {
		return nil, false, cmpInvalid("The data disk size %d GB is not valid; createOption 'Empty' requires "+
			"diskSizeGB between 1 and %d.", gb, cmpMaxDiskGB)
	}

	sku, err := diskSKUOf(md, size)
	if err != nil {
		return nil, false, err
	}

	if q.detach {
		return nil, false, nil
	}

	name := strOf(q.raw, fieldName)
	if name == "" {
		name = fmt.Sprintf("%s_disk%d_%s", r.Name, q.lun, strings.ReplaceAll(uuid.NewString(), "-", ""))
	}

	d, err := p.newVMDisk(r, name, sku, gb, map[string]any{cmpFieldCreateOpt: createEmpty}, "", plan)
	if err != nil {
		return nil, false, err
	}

	entry[fieldName] = d.Name
	entry[cmpFieldCreateOpt] = createEmpty
	entry[cmpFieldDiskSizeGB] = gb
	entry[cmpFieldManagedDsk] = map[string]any{cmpFieldID: d.armID(), cmpFieldStorType: sku}

	return entry, true, nil
}

func (p *ComputeProvider) planDetaches(prev map[int]map[string]any, kept map[int]bool, plan *diskPlan) {
	for lun, m := range prev {
		if kept[lun] {
			continue
		}

		d, ok := p.item(cmpDisks, strOf(mapOf(m[cmpFieldManagedDsk]), cmpFieldID))
		if !ok {
			continue
		}

		plan.detach = append(plan.detach, diskDetach{disk: d, remove: strOf(m, cmpFieldDelOpt) == deleteOptionDelete})
	}
}

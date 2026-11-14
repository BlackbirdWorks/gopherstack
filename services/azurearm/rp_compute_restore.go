package azurearm

import (
	"net/http"
	"strings"
)

const (
	cmpRPCs           = "restorePointCollections"
	kidRestorePoints  = "restorePoints"
	rpFieldSource     = "source"
	rpFieldMetadata   = "sourceMetadata"
	rpFieldExclude    = "excludeDisks"
	rpFieldConsist    = "consistencyMode"
	rpFieldSrcRP      = "sourceRestorePoint"
	rpFieldCollection = "restorePointCollectionId"
	rpFieldInstant    = "instantAccessDurationMinutes"
	rpExpandRPs       = "restorePoints"
	rpConsistCrash    = "CrashConsistent"
	rpFieldLicense    = "licenseType"
)

//nolint:gochecknoglobals // static enum from the Compute spec
var rpConsistencyModes = []string{rpConsistCrash, "FileSystemConsistent", "ApplicationConsistent"}

func (p *ComputeProvider) putRPC(id ResourceID, body map[string]any) (*cmpRes, error) {
	if err := validCmpName(id.Names[0], cmpMaxNameLen); err != nil {
		return nil, err
	}

	r, old, err := p.upsert(cmpRPCs, id, body)
	if err != nil {
		return nil, err
	}

	props := cloneProps(propsOf(body))
	for _, k := range []string{fieldProvisioningState, rpFieldCollection, "restorePoints"} {
		delete(props, k)
	}

	src := mapOf(props[rpFieldSource])
	vmID := strOf(src, cmpFieldID)

	if old != nil {
		prev := strOf(mapOf(old.Props[rpFieldSource]), cmpFieldID)
		if vmID == "" {
			vmID = prev
		}

		if !strings.EqualFold(vmID, prev) {
			return nil, cmpChanged("properties.source.id")
		}
	}

	if vmID == "" {
		return nil, cmpInvalid("The restore point collection source.id is required.")
	}

	vm, ok := p.item(cmpVMs, vmID)
	if !ok {
		return nil, notFoundRef(vmID)
	}

	props[rpFieldSource] = map[string]any{cmpFieldID: vm.armID(), fieldLocation: vm.Location}
	r.Aux[rpFieldCollection] = firstNonEmpty(r.Aux[rpFieldCollection], r.UID)
	r.Props = props
	p.items[cmpKey(cmpRPCs, r.RG, r.Name)] = r

	return r, nil
}

func (p *ComputeProvider) rpcProps(r *cmpRes, expand bool) map[string]any {
	props := cloneProps(r.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded
	props[rpFieldCollection] = r.Aux[rpFieldCollection]

	if expand {
		points := make([]any, 0, len(kidsOf(r, kidRestorePoints)))
		for _, k := range kidsOf(r, kidRestorePoints) {
			points = append(points, renderRestorePoint(p, r, k, ""))
		}

		props[rpExpandRPs] = points
	}

	return props
}

// rpSourceMetadata captures the source machine's settings at the time the restore point is created.
func (p *ComputeProvider) rpSourceMetadata(vm *cmpRes, exclude []any) (map[string]any, error) {
	meta := map[string]any{cmpFieldVMID: vm.UID, fieldLocation: vm.Location}

	for _, k := range []string{"hardwareProfile", "diagnosticsProfile", "securityProfile", rpFieldLicense, "userData"} {
		if v, ok := vm.Props[k]; ok {
			meta[k] = deepClone(v)
		}
	}

	if os := mapOf(vm.Props[cmpFieldOSProfile]); os != nil {
		prof := cloneProps(os)
		delete(prof, "adminPassword")
		delete(prof, "secrets")
		meta[cmpFieldOSProfile] = prof
	}

	sp := mapOf(vm.Props[cmpFieldStorage])
	osDisk := cloneProps(mapOf(sp[cmpFieldOSDisk]))
	dataDisks := []any{}
	skip := map[string]bool{}

	for _, e := range exclude {
		skip[strings.ToLower(strOf(mapOf(e), cmpFieldID))] = true
	}

	for _, d := range anyList(sp[cmpFieldDataDisks]) {
		dm := cloneProps(mapOf(d))
		did := strings.ToLower(strOf(mapOf(dm[cmpFieldManagedDsk]), cmpFieldID))

		if skip[did] {
			delete(skip, did)

			continue
		}

		dataDisks = append(dataDisks, dm)
	}

	for left := range skip {
		return nil, cmpInvalid("The disk '%s' in excludeDisks is not a data disk of virtual machine %s.", left, vm.Name)
	}

	meta[cmpFieldStorage] = map[string]any{cmpFieldOSDisk: osDisk, cmpFieldDataDisks: dataDisks}

	return meta, nil
}

func putRestorePoint(p *ComputeProvider, parent *cmpRes, k, old *cmpKid, body map[string]any) error {
	if err := validCmpName(k.leaf(), cmpMaxNameLen); err != nil {
		return err
	}

	if old != nil {
		return netErr(http.StatusConflict, cmpCodeConflict,
			"Restore point %s already exists in restore point collection %s and restore points cannot be modified.",
			k.leaf(), parent.Name)
	}

	props := cloneProps(propsOf(body))
	for _, f := range []string{fieldProvisioningState, rpFieldMetadata, cmpFieldInstView, "timeCreated"} {
		delete(props, f)
	}

	mode, err := enumField(props, rpFieldConsist, "properties", rpConsistencyModes, rpConsistCrash)
	if err != nil {
		return err
	}

	props[rpFieldConsist] = mode

	meta, err := p.restorePointMetadata(parent, props)
	if err != nil {
		return err
	}

	props[rpFieldMetadata] = meta
	k.Props = props

	return nil
}

func (p *ComputeProvider) restorePointMetadata(parent *cmpRes, props map[string]any) (map[string]any, error) {
	if src := strOf(mapOf(props[rpFieldSrcRP]), cmpFieldID); src != "" {
		if _, ok := p.restorePointByID(src); !ok {
			return nil, notFoundRef(src)
		}

		other, _ := p.restorePointByID(src)

		return cloneProps(mapOf(other.Props[rpFieldMetadata])), nil
	}

	vm, ok := p.item(cmpVMs, strOf(mapOf(parent.Props[rpFieldSource]), cmpFieldID))
	if !ok {
		return nil, notFoundRef(strOf(mapOf(parent.Props[rpFieldSource]), cmpFieldID))
	}

	return p.rpSourceMetadata(vm, anyList(props[rpFieldExclude]))
}

func (p *ComputeProvider) restorePointByID(idStr string) (*cmpKid, bool) {
	rid, err := ParseGenericResourcePath(idStr)
	if err != nil || len(rid.Types) != 2 || !strings.EqualFold(rid.Types[0], cmpRPCs) ||
		!strings.EqualFold(
			rid.Types[1],
			kidRestorePoints,
		) || !strings.EqualFold(rid.Namespace, namespaceMicrosoftCompute) {
		return nil, false
	}

	parent, ok := p.top(cmpRPCs, rid.ResourceGroup, rid.Names[0])
	if !ok {
		return nil, false
	}

	k := parent.Kids[kidKey(kidRestorePoints, rid.Names[1:])]

	return k, k != nil
}

func renderRestorePoint(p *ComputeProvider, parent *cmpRes, k *cmpKid, _ string) map[string]any {
	props := cloneProps(k.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded
	props["timeCreated"] = k.Created.Format(timeCreatedLayout)

	return p.kidBase(parent, k, props)
}

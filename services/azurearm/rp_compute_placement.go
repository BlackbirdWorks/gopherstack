package azurearm

import (
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	cmpPPGs        = "proximityPlacementGroups"
	cmpSSHKeys     = "sshPublicKeys"
	cmpCapGroups   = "capacityReservationGroups"
	cmpHostGroups  = "hostGroups"
	cmpDiskEncSets = "diskEncryptionSets"
	cmpDiskAccess  = "diskAccesses"

	kidCapReservations = "capacityReservations"
	kidHosts           = "hosts"

	cmpFieldPPG       = "proximityPlacementGroup"
	cmpFieldHost      = "host"
	cmpFieldHostGroup = "hostGroup"
	cmpFieldCapRes    = "capacityReservation"
	cmpFieldCapGroup  = "capacityReservationGroup"
	cmpFieldVMsAssoc  = "virtualMachinesAssociated"
	cmpFieldVMList    = "virtualMachines"
	cmpFieldStatus    = "colocationStatus"
	cmpFieldInternal  = "_"

	ppgTypeStandard = "Standard"
	ppgTypeUltra    = "Ultra"

	hostLicenseNone = "None"
	hostFDMax       = 5
	cmpMaxZones     = 3
)

func cmpIDRefs(items []*cmpRes) []any {
	out := make([]any, 0, len(items))

	for _, it := range items {
		out = append(out, map[string]any{cmpFieldID: it.armID()})
	}

	return out
}

// membersOf lists resources of the given types whose property path names the id.
func (p *ComputeProvider) membersOf(id string, types []string, path ...string) []*cmpRes {
	var out []*cmpRes

	for _, typ := range types {
		for _, r := range p.ofType(typ) {
			cur := r.Props

			for _, seg := range path[:len(path)-1] {
				cur = mapOf(cur[seg])
			}

			if ref := strOf(cur, path[len(path)-1]); ref != "" && strings.EqualFold(ref, id) {
				out = append(out, r)
			}
		}
	}

	return out
}

func notFoundRef(id string) error {
	return netErr(http.StatusNotFound, netCodeNotFound, "The Resource '%s' was not found.", strings.TrimPrefix(id, "/"))
}

// refExists checks that an ARM id in props names a stored resource of typ.
func (p *ComputeProvider) refExists(typ, id string) error {
	if id == "" {
		return nil
	}

	if _, ok := p.item(typ, id); !ok {
		return notFoundRef(id)
	}

	return nil
}

// checkPlacementRefs validates the placement references a machine, scale set or availability set carries.
func (p *ComputeProvider) checkPlacementRefs(props map[string]any) error {
	if err := p.refExists(cmpPPGs, strOf(mapOf(props[cmpFieldPPG]), cmpFieldID)); err != nil {
		return err
	}

	if err := p.refExists(cmpCapGroups,
		strOf(mapOf(mapOf(props[cmpFieldCapRes])[cmpFieldCapGroup]), cmpFieldID)); err != nil {
		return err
	}

	if hostID := strOf(mapOf(props[cmpFieldHost]), cmpFieldID); hostID != "" {
		if err := p.hostExists(hostID); err != nil {
			return err
		}
	}

	return p.refExists(cmpHostGroups, strOf(mapOf(props[cmpFieldHostGroup]), cmpFieldID))
}

func (p *ComputeProvider) hostExists(id string) error {
	rid, err := ParseGenericResourcePath(id)
	if err != nil || len(rid.Types) != 2 || len(rid.Names) != 2 || !strings.EqualFold(rid.Types[0], cmpHostGroups) {
		return notFoundRef(id)
	}

	grp, ok := p.top(cmpHostGroups, rid.ResourceGroup, rid.Names[0])
	if !ok || grp.Kids[kidKey(kidHosts, rid.Names[1:])] == nil {
		return notFoundRef(id)
	}

	return nil
}

func (p *ComputeProvider) placementMembers(id string) []*cmpRes {
	return p.membersOf(id, []string{cmpVMs, cmpAvSets, cmpVMSS}, cmpFieldPPG, cmpFieldID)
}

// ---- proximity placement groups ----

func (p *ComputeProvider) putPPG(id ResourceID, body map[string]any) (*cmpRes, error) {
	if err := validCmpName(id.Names[0], cmpMaxNameLen); err != nil {
		return nil, err
	}

	r, old, err := p.upsert(cmpPPGs, id, body)
	if err != nil {
		return nil, err
	}

	if r.Zones, err = parseZones(body, old, cmpMaxZones); err != nil {
		return nil, err
	}

	props := cloneProps(propsOf(body))
	for _, k := range []string{cmpFieldStatus, "availabilitySets", cmpFieldVMList, "virtualMachineScaleSets"} {
		delete(props, k)
	}

	typ, ok := canonicalIn(
		[]string{ppgTypeStandard, ppgTypeUltra},
		firstNonEmpty(strOf(props, "proximityPlacementGroupType"),
			ppgTypeStandard),
	)
	if !ok {
		return nil, cmpInvalid("The proximity placement group type '%s' is not valid; allowed values are %s and %s.",
			strOf(props, "proximityPlacementGroupType"), ppgTypeStandard, ppgTypeUltra)
	}

	props["proximityPlacementGroupType"] = typ
	r.Props = props
	p.items[cmpKey(cmpPPGs, r.RG, r.Name)] = r

	return r, nil
}

func (p *ComputeProvider) ppgProps(r *cmpRes) map[string]any {
	props := cloneProps(r.Props)
	members := p.placementMembers(r.armID())

	groups := map[string][]any{}

	for _, m := range members {
		groups[m.Type] = append(groups[m.Type], map[string]any{
			cmpFieldID: m.armID(),
			cmpFieldStatus: map[string]any{
				aksFieldCode: "ColocationStatus/Aligned", cmpKeyLevel: statusLevelInfo, cmpKeyDisplay: "Aligned",
			},
		})
	}

	props[cmpFieldVMList] = orEmpty(groups[cmpVMs])
	props["availabilitySets"] = orEmpty(groups[cmpAvSets])
	props["virtualMachineScaleSets"] = orEmpty(groups[cmpVMSS])

	if len(members) > 0 {
		props[cmpFieldStatus] = map[string]any{
			aksFieldCode: "ColocationStatus/Aligned", cmpKeyLevel: statusLevelInfo, cmpKeyDisplay: "Aligned",
			fieldMessage: "All resources in the proximity placement group are aligned.",
		}
	}

	return props
}

func orEmpty(l []any) []any {
	if l == nil {
		return []any{}
	}

	return l
}

func (p *ComputeProvider) checkPPGDelete(r *cmpRes) error {
	if m := p.placementMembers(r.armID()); len(m) > 0 {
		return cmpNotAllowed("Proximity placement group %s cannot be deleted because it contains %s %s.",
			r.Name, m[0].Type, m[0].Name)
	}

	return nil
}

// ---- capacity reservation groups ----

func (p *ComputeProvider) putCapGroup(id ResourceID, body map[string]any) (*cmpRes, error) {
	if err := validCmpName(id.Names[0], cmpMaxNameLen); err != nil {
		return nil, err
	}

	r, old, err := p.upsert(cmpCapGroups, id, body)
	if err != nil {
		return nil, err
	}

	if r.Zones, err = parseZones(body, old, cmpMaxZones); err != nil {
		return nil, err
	}

	if old != nil && !sameStrings(r.Zones, old.Zones) {
		return nil, cmpChanged(cmpFieldZones)
	}

	props := cloneProps(propsOf(body))
	for _, k := range []string{"capacityReservations", cmpFieldVMsAssoc, cmpFieldInstView} {
		delete(props, k)
	}

	if rt := strOf(props, "reservationType"); rt != "" {
		canon, ok := canonicalIn([]string{"Targeted", "Block", "Open"}, rt)
		if !ok {
			return nil, cmpInvalid(
				"The reservationType '%s' is not valid; allowed values are Targeted, Block and Open.",
				rt,
			)
		}

		if old != nil && !strings.EqualFold(strOf(old.Props, "reservationType"), canon) {
			return nil, cmpChanged("reservationType")
		}

		props["reservationType"] = canon
	}

	r.Props = props
	p.items[cmpKey(cmpCapGroups, r.RG, r.Name)] = r

	return r, nil
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func (p *ComputeProvider) capGroupVMs(r *cmpRes) []*cmpRes {
	return p.membersOf(r.armID(), []string{cmpVMs}, cmpFieldCapRes, cmpFieldCapGroup, cmpFieldID)
}

func (p *ComputeProvider) capGroupProps(r *cmpRes) map[string]any {
	props := cloneProps(r.Props)
	res := make([]any, 0, len(r.Kids))

	for _, k := range kidsOf(r, kidCapReservations) {
		res = append(res, map[string]any{cmpFieldID: k.armID(r)})
	}

	props["capacityReservations"] = res

	if vms := p.capGroupVMs(r); len(vms) > 0 {
		props[cmpFieldVMsAssoc] = cmpIDRefs(vms)
	}

	return props
}

func (p *ComputeProvider) checkCapGroupDelete(r *cmpRes) error {
	if len(r.Kids) > 0 || len(p.capGroupVMs(r)) > 0 {
		return cmpNotAllowed("Capacity reservation group %s cannot be deleted while it contains capacity reservations "+
			"or has associated resources.", r.Name)
	}

	return nil
}

func putCapReservation(_ *ComputeProvider, parent *cmpRes, k, old *cmpKid, body map[string]any) error {
	sku := mapOf(body[cmpFieldSKU])
	if strOf(sku, fieldName) == "" {
		return cmpInvalid("The capacity reservation sku.name is required.")
	}

	capacity, ok := sku["capacity"].(float64)
	if !ok || capacity < 0 {
		return cmpInvalid("The capacity reservation sku.capacity is required and must not be negative.")
	}

	zones := strList(body[cmpFieldZones])
	if old != nil {
		prev := strList(old.Props[cmpFieldInternal+cmpFieldZones])
		if len(zones) > 0 && !sameStrings(zones, prev) {
			return cmpChanged(cmpFieldZones)
		}

		zones = prev
	}

	if len(zones) > 1 {
		return cmpInvalid("A capacity reservation can be placed in at most 1 availability zone(s).")
	}

	if len(zones) == 1 && len(parent.Zones) > 0 && !containsFold(parent.Zones, zones[0]) {
		return cmpInvalid("The zone '%s' of the capacity reservation is not one of the zones of capacity "+
			"reservation group %s.", zones[0], parent.Name)
	}

	props := cloneProps(propsOf(body))
	for _, f := range []string{cmpFieldInstView, fieldProvisioningState, cmpFieldVMsAssoc, "reservationId",
		cmpFieldTimeMade, "platformFaultDomainCount", "provisioningTime"} {
		delete(props, f)
	}

	props[cmpFieldInternal+cmpFieldSKU] = deepClone(sku)
	props[cmpFieldInternal+cmpFieldZones] = zones
	props["reservationId"] = firstNonEmpty(strOf(oldProps(old), "reservationId"), uuid.NewString())
	k.Props = props

	return nil
}

func oldProps(k *cmpKid) map[string]any {
	if k == nil {
		return nil
	}

	return k.Props
}

func renderCapReservation(p *ComputeProvider, parent *cmpRes, k *cmpKid, _ string) map[string]any {
	props := cloneProps(k.Props)
	sku := mapOf(props[cmpFieldInternal+cmpFieldSKU])
	zones := strList(props[cmpFieldInternal+cmpFieldZones])

	delete(props, cmpFieldInternal+cmpFieldSKU)
	delete(props, cmpFieldInternal+cmpFieldZones)

	props[fieldProvisioningState] = provisioningStateSucceeded
	props[cmpFieldTimeMade] = k.Created.Format(timeCreatedLayout)
	props["provisioningTime"] = k.Created.Format(timeCreatedLayout)
	props["platformFaultDomainCount"] = 3

	assoc := []any{}

	for _, vm := range p.capGroupVMs(parent) {
		if strings.EqualFold(strOf(mapOf(vm.Props[cmpFieldHardware]), "vmSize"), strOf(sku, fieldName)) {
			assoc = append(assoc, map[string]any{cmpFieldID: vm.armID()})
		}
	}

	if len(assoc) > 0 {
		props[cmpFieldVMsAssoc] = assoc
	}

	props[cmpFieldInstView] = map[string]any{
		"utilizationInfo": map[string]any{"currentCapacity": intOf(sku["capacity"]), "virtualMachinesAllocated": assoc},
		cmpKeyStatuses: []any{
			map[string]any{
				aksFieldCode:  "ProvisioningState/succeeded",
				cmpKeyLevel:   statusLevelInfo,
				cmpKeyDisplay: "Provisioning succeeded",
			},
		},
	}

	out := p.kidBase(parent, k, props)
	out[cmpFieldSKU] = sku

	if len(zones) > 0 {
		out[cmpFieldZones] = zones
	}

	return out
}

// ---- dedicated host groups and hosts ----

func (p *ComputeProvider) putHostGroup(id ResourceID, body map[string]any) (*cmpRes, error) {
	if err := validCmpName(id.Names[0], cmpMaxNameLen); err != nil {
		return nil, err
	}

	r, old, err := p.upsert(cmpHostGroups, id, body)
	if err != nil {
		return nil, err
	}

	if r.Zones, err = parseZones(body, old, 1); err != nil {
		return nil, err
	}

	if old != nil && !sameStrings(r.Zones, old.Zones) {
		return nil, cmpChanged(cmpFieldZones)
	}

	props := cloneProps(propsOf(body))
	for _, k := range []string{"hosts", cmpFieldInstView} {
		delete(props, k)
	}

	count, ok := props["platformFaultDomainCount"].(float64)
	if !ok {
		return nil, cmpInvalid("Required property 'platformFaultDomainCount' not found in JSON. Path 'properties'.")
	}

	if count < 1 || count > hostFDMax {
		return nil, cmpInvalid("The platformFaultDomainCount %d must fall in the range 1 to %d.", int(count), hostFDMax)
	}

	if old != nil && intOf(old.Props["platformFaultDomainCount"]) != int(count) {
		return nil, cmpChanged("platformFaultDomainCount")
	}

	if mapOf(props["additionalCapabilities"])["ultraSSDEnabled"] == true && len(r.Zones) == 0 {
		return nil, cmpInvalid(
			"The ultraSSDEnabled setting can only be enabled for host groups that are created as zonal.",
		)
	}

	setDefault(props, "supportAutomaticPlacement", false)
	r.Props = props
	p.items[cmpKey(cmpHostGroups, r.RG, r.Name)] = r

	return r, nil
}

func (p *ComputeProvider) hostGroupProps(r *cmpRes) map[string]any {
	props := cloneProps(r.Props)
	hosts := make([]any, 0, len(r.Kids))

	for _, k := range kidsOf(r, kidHosts) {
		hosts = append(hosts, map[string]any{cmpFieldID: k.armID(r)})
	}

	props["hosts"] = hosts

	return props
}

func (p *ComputeProvider) checkHostGroupDelete(r *cmpRes) error {
	if len(r.Kids) > 0 {
		return cannotDeleteNested()
	}

	if len(p.membersOf(r.armID(), []string{cmpVMs, cmpVMSS}, cmpFieldHostGroup, cmpFieldID)) > 0 {
		return cmpNotAllowed(
			"Dedicated host group %s cannot be deleted because virtual machines are placed on it.",
			r.Name,
		)
	}

	return nil
}

func putHost(_ *ComputeProvider, parent *cmpRes, k, old *cmpKid, body map[string]any) error {
	sku := mapOf(body[cmpFieldSKU])
	if strOf(sku, fieldName) == "" {
		return cmpInvalid("The dedicated host sku.name is required.")
	}

	props := cloneProps(propsOf(body))
	for _, f := range []string{"hostId", cmpFieldInstView, fieldProvisioningState, cmpFieldVMList, cmpFieldTimeMade,
		"provisioningTime"} {
		delete(props, f)
	}

	if fd, present := props["platformFaultDomain"]; present {
		n := intOf(fd)
		if n < 0 || n >= intOf(parent.Props["platformFaultDomainCount"]) {
			return cmpInvalid("The platformFaultDomain %d must be between 0 and %d, the platformFaultDomainCount of "+
				"host group %s minus one.", n, intOf(parent.Props["platformFaultDomainCount"])-1, parent.Name)
		}
	}

	if lt := strOf(props, "licenseType"); lt != "" {
		canon, ok := canonicalIn([]string{hostLicenseNone, "Windows_Server_Hybrid", "Windows_Server_Perpetual"}, lt)
		if !ok {
			return cmpInvalid("The licenseType '%s' is not valid; allowed values are None, Windows_Server_Hybrid and "+
				"Windows_Server_Perpetual.", lt)
		}

		props["licenseType"] = canon
	}

	setDefault(props, "autoReplaceOnFailure", true)
	setDefault(props, "licenseType", hostLicenseNone)

	if old != nil {
		prevSKU := mapOf(old.Props[cmpFieldInternal+cmpFieldSKU])
		if !strings.EqualFold(strOf(prevSKU, fieldName), strOf(sku, fieldName)) {
			return cmpChanged("sku.name")
		}

		if _, present := props["platformFaultDomain"]; !present {
			setDefault(props, "platformFaultDomain", old.Props["platformFaultDomain"])
		} else if intOf(props["platformFaultDomain"]) != intOf(old.Props["platformFaultDomain"]) {
			return cmpChanged("platformFaultDomain")
		}

		props["hostId"] = old.Props["hostId"]
	} else {
		props["hostId"] = uuid.NewString()
	}

	props[cmpFieldInternal+cmpFieldSKU] = deepClone(sku)
	k.Props = props

	return nil
}

func hostVMs(p *ComputeProvider, parent *cmpRes, k *cmpKid) []*cmpRes {
	return p.membersOf(k.armID(parent), []string{cmpVMs, cmpVMSS}, cmpFieldHost, cmpFieldID)
}

func renderHost(p *ComputeProvider, parent *cmpRes, k *cmpKid, expand string) map[string]any {
	props := cloneProps(k.Props)
	sku := mapOf(props[cmpFieldInternal+cmpFieldSKU])

	delete(props, cmpFieldInternal+cmpFieldSKU)

	props[fieldProvisioningState] = provisioningStateSucceeded
	props[cmpFieldTimeMade] = k.Created.Format(timeCreatedLayout)
	props["provisioningTime"] = k.Created.Format(timeCreatedLayout)
	props[cmpFieldVMList] = cmpIDRefs(hostVMs(p, parent, k))

	if strings.EqualFold(expand, cmpFieldInstView) {
		props[cmpFieldInstView] = map[string]any{
			"assetId":           strOf(props, "hostId"),
			"availableCapacity": map[string]any{"allocatableVMs": []any{}},
			cmpKeyStatuses: []any{
				map[string]any{
					aksFieldCode:  "ProvisioningState/succeeded",
					cmpKeyLevel:   statusLevelInfo,
					cmpKeyDisplay: "Provisioning succeeded",
				},
			},
		}
	}

	out := p.kidBase(parent, k, props)
	out[cmpFieldSKU] = sku

	return out
}

func deleteHost(p *ComputeProvider, parent *cmpRes, k *cmpKid) error {
	if vms := hostVMs(p, parent, k); len(vms) > 0 {
		return cmpNotAllowed("Dedicated host %s cannot be deleted because virtual machine %s is placed on it.",
			k.leaf(), vms[0].Name)
	}

	return nil
}

// ---- disk encryption sets and disk accesses ----

func (p *ComputeProvider) putDiskEncSet(id ResourceID, body map[string]any) (*cmpRes, error) {
	if err := validCmpName(id.Names[0], cmpMaxNameLen); err != nil {
		return nil, err
	}

	r, old, err := p.upsert(cmpDiskEncSets, id, body)
	if err != nil {
		return nil, err
	}

	if err = p.applyIdentity(r, old, body); err != nil {
		return nil, err
	}

	props := cloneProps(propsOf(body))
	for _, k := range []string{
		"previousKeys", "autoKeyRotationError", "lastKeyRotationTimestamp", fieldProvisioningState,
	} {
		delete(props, k)
	}

	typ, ok := canonicalIn([]string{
		"EncryptionAtRestWithCustomerKey", "EncryptionAtRestWithPlatformAndCustomerKeys",
		"ConfidentialVmEncryptedWithCustomerKey",
	}, firstNonEmpty(strOf(props, "encryptionType"), "EncryptionAtRestWithCustomerKey"))
	if !ok {
		return nil, cmpInvalid("The encryptionType '%s' is not valid.", strOf(props, "encryptionType"))
	}

	props["encryptionType"] = typ

	if key := mapOf(props["activeKey"]); key != nil && !strings.HasPrefix(strings.ToLower(strOf(key, "keyUrl")),
		"https://") {
		return nil, cmpInvalid("The activeKey.keyUrl must be a fully versioned https Key Vault key URL.")
	}

	if old == nil || strOf(mapOf(old.Props["activeKey"]), "keyUrl") != strOf(mapOf(props["activeKey"]), "keyUrl") {
		r.Aux["lastKeyRotation"] = time.Now().UTC().Format(timeCreatedLayout)
	}

	r.Props = props
	p.items[cmpKey(cmpDiskEncSets, r.RG, r.Name)] = r

	return r, nil
}

func (p *ComputeProvider) diskEncSetProps(r *cmpRes) map[string]any {
	props := cloneProps(r.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded
	props["previousKeys"] = []any{}
	props["lastKeyRotationTimestamp"] = r.Aux["lastKeyRotation"]

	return props
}

func (p *ComputeProvider) disksUsing(id string, path ...string) []*cmpRes {
	return p.membersOf(id, []string{cmpDisks, cmpSnaps}, path...)
}

func (p *ComputeProvider) checkDiskEncSetDelete(r *cmpRes) error {
	if d := p.disksUsing(r.armID(), "encryption", "diskEncryptionSetId"); len(d) > 0 {
		return cmpNotAllowed(
			"Disk encryption set %s cannot be deleted because %s %s uses it.",
			r.Name,
			d[0].Type,
			d[0].Name,
		)
	}

	return nil
}

func (p *ComputeProvider) putDiskAccess(id ResourceID, body map[string]any) (*cmpRes, error) {
	if err := validCmpName(id.Names[0], cmpMaxNameLen); err != nil {
		return nil, err
	}

	r, _, err := p.upsert(cmpDiskAccess, id, body)
	if err != nil {
		return nil, err
	}

	r.Extra = extraFields(body, "extendedLocation")
	r.Props = map[string]any{}
	p.items[cmpKey(cmpDiskAccess, r.RG, r.Name)] = r

	return r, nil
}

func (p *ComputeProvider) diskAccessProps(r *cmpRes) map[string]any {
	return map[string]any{
		fieldProvisioningState: provisioningStateSucceeded,
		cmpFieldTimeMade:       r.Created.Format(timeCreatedLayout),
		acaFieldPEC:            []any{},
	}
}

func (p *ComputeProvider) checkDiskAccessDelete(r *cmpRes) error {
	if d := p.disksUsing(r.armID(), "diskAccessId"); len(d) > 0 {
		return cmpNotAllowed("Disk access %s cannot be deleted because %s %s uses it.", r.Name, d[0].Type, d[0].Name)
	}

	return nil
}

// checkDiskRefs validates the encryption set and disk access a disk or snapshot names.
func (p *ComputeProvider) checkDiskRefs(props map[string]any) error {
	if err := p.refExists(cmpDiskEncSets, strOf(mapOf(props["encryption"]), "diskEncryptionSetId")); err != nil {
		return err
	}

	return p.refExists(cmpDiskAccess, strOf(props, "diskAccessId"))
}

func (p *ComputeProvider) checkTypeDelete(typ string, r *cmpRes) error {
	switch typ {
	case cmpGalleries:
		if len(r.Kids) > 0 {
			return cannotDeleteNested()
		}
	case cmpDisks:
		return p.checkDiskDelete(r)
	case cmpAvSets:
		return p.checkAvSetDelete(r)
	case cmpPPGs:
		return p.checkPPGDelete(r)
	case cmpCapGroups:
		return p.checkCapGroupDelete(r)
	case cmpHostGroups:
		return p.checkHostGroupDelete(r)
	case cmpDiskEncSets:
		return p.checkDiskEncSetDelete(r)
	case cmpDiskAccess:
		return p.checkDiskAccessDelete(r)
	}

	return nil
}

func (p *ComputeProvider) placementProps(r *cmpRes) map[string]any {
	switch r.Type {
	case cmpPPGs:
		return p.ppgProps(r)
	case cmpSSHKeys:
		return cloneProps(r.Props)
	case cmpCapGroups:
		return p.capGroupProps(r)
	case cmpHostGroups:
		return p.hostGroupProps(r)
	case cmpDiskEncSets:
		return p.diskEncSetProps(r)
	case cmpDiskAccess:
		return p.diskAccessProps(r)
	}

	return p.vmssProps(r)
}

func (p *ComputeProvider) putPlacement(typ string, id ResourceID, body map[string]any) (*cmpRes, error) {
	switch typ {
	case cmpPPGs:
		return p.putPPG(id, body)
	case cmpSSHKeys:
		return p.putSSHKey(id, body)
	case cmpCapGroups:
		return p.putCapGroup(id, body)
	case cmpHostGroups:
		return p.putHostGroup(id, body)
	case cmpDiskEncSets:
		return p.putDiskEncSet(id, body)
	case cmpDiskAccess:
		return p.putDiskAccess(id, body)
	case cmpRPCs:
		return p.putRPC(id, body)
	}

	return p.putVMSS(id, body)
}

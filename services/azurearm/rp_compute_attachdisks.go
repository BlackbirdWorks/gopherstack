package azurearm

import (
	"maps"
	"slices"
	"strings"
)

const (
	vmActionAttachDetach = "attachDetachDataDisks"
	attachReqAttach      = "dataDisksToAttach"
	attachReqDetach      = "dataDisksToDetach"
)

// AttachDetachDataDisks applies a virtualMachines/{vm}/attachDetachDataDisks request (armcompute v9
// AttachDetachDataDisksRequest) and returns the resulting storageProfile.
func (p *ComputeProvider) AttachDetachDataDisks(id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock("AttachDetachDataDisks")
	defer p.mu.Unlock()

	r, found := p.top(cmpVMs, id.ResourceGroup, id.Names[0])
	if !found {
		return nil, ErrResourceNotFound
	}

	toAttach, toDetach := anyList(body[attachReqAttach]), anyList(body[attachReqDetach])
	if len(toAttach) == 0 && len(toDetach) == 0 {
		return nil, cmpInvalid("At least one of '%s' or '%s' is required.", attachReqAttach, attachReqDetach)
	}

	sp := cloneProps(mapOf(r.Props[cmpFieldStorage]))

	disks, err := detachRequested(wireDataDisks(anyList(sp[cmpFieldDataDisks])), toDetach)
	if err != nil {
		return nil, err
	}

	if disks, err = attachRequested(disks, toAttach); err != nil {
		return nil, err
	}

	sp[cmpFieldDataDisks] = disks

	props := cloneProps(r.Props)
	props[cmpFieldStorage] = sp
	delete(props, cmpFieldOSProfile)

	put := map[string]any{fieldLocation: r.Location, fieldProperties: props, fieldTags: tagsAny(r.Tags)}
	maps.Copy(put, r.Extra)

	if len(r.Zones) > 0 {
		put["zones"] = slices.Collect(func(yield func(any) bool) {
			for _, z := range r.Zones {
				if !yield(z) {
					return
				}
			}
		})
	}

	res, err := p.putVM(id, put)
	if err != nil {
		return nil, err
	}

	return mapOf(mapOf(p.render(res, false)[fieldProperties])[cmpFieldStorage]), nil
}

// wireDataDisks copies stored data disks with the lun as a JSON number, the shape parseDataDisks reads.
func wireDataDisks(disks []any) []any {
	out := make([]any, len(disks))

	for i, e := range disks {
		m := cloneProps(mapOf(e))
		m[cmpFieldLun] = float64(intOf(m[cmpFieldLun]))
		out[i] = m
	}

	return out
}

// detachRequested marks the listed managed disks for detach; the disks stay (deleteOption Detach).
func detachRequested(disks, toDetach []any) ([]any, error) {
	for _, raw := range toDetach {
		req := mapOf(raw)
		diskID := strOf(req, "diskId")

		if diskID == "" {
			return nil, cmpInvalid("Required parameter '%s.diskId' is missing (null).", attachReqDetach)
		}

		idx := -1

		for i, e := range disks {
			if strings.EqualFold(strOf(mapOf(mapOf(e)[cmpFieldManagedDsk]), cmpFieldID), diskID) {
				idx = i

				break
			}
		}

		if idx < 0 {
			return nil, cmpInvalid("The data disk '%s' is not attached to the virtual machine.", diskID)
		}

		entry := cloneProps(mapOf(disks[idx]))
		entry["toBeDetached"], entry[cmpFieldDelOpt] = true, deleteOptionDetach
		disks[idx] = entry
	}

	return disks, nil
}

// attachRequested appends the listed managed disks, assigning the lowest free lun when none is given.
func attachRequested(disks, toAttach []any) ([]any, error) {
	used, attached := map[int]bool{}, map[string]bool{}

	for _, e := range disks {
		m := mapOf(e)
		if detach, _ := m["toBeDetached"].(bool); !detach {
			used[intOf(m[cmpFieldLun])] = true
			attached[strings.ToLower(strOf(mapOf(m[cmpFieldManagedDsk]), cmpFieldID))] = true
		}
	}

	for _, raw := range toAttach {
		req := mapOf(raw)
		diskID := strOf(req, "diskId")

		if diskID == "" {
			return nil, cmpInvalid("Required parameter '%s.diskId' is missing (null).", attachReqAttach)
		}

		if attached[strings.ToLower(diskID)] {
			return nil, cmpNotAllowed("Disk '%s' is already attached to the virtual machine.", diskID)
		}

		attached[strings.ToLower(diskID)] = true

		lun, given := req[cmpFieldLun].(float64)
		if !given {
			for used[int(lun)] {
				lun++
			}
		}

		if used[int(lun)] {
			return nil, cmpInvalid(
				"Data disk LUN %d is used by more than one data disk; LUNs must be unique.", int(lun))
		}

		used[int(lun)] = true

		entry := map[string]any{
			cmpFieldLun: lun, cmpFieldCreateOpt: createAttach, cmpFieldManagedDsk: map[string]any{cmpFieldID: diskID},
			cmpFieldCaching: firstNonEmpty(strOf(req, cmpFieldCaching), cmpCachingNone),
			cmpFieldDelOpt:  firstNonEmpty(strOf(req, cmpFieldDelOpt), deleteOptionDetach),
		}

		if v, set := req["writeAcceleratorEnabled"]; set {
			entry["writeAcceleratorEnabled"] = v
		}

		if des := mapOf(req["diskEncryptionSet"]); len(des) > 0 {
			md := mapOf(entry[cmpFieldManagedDsk])
			md["diskEncryptionSet"] = des
			entry[cmpFieldManagedDsk] = md
		}

		disks = append(disks, entry)
	}

	return disks, nil
}

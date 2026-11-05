package gcpcompute

import (
	"slices"
	"sort"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collReservations   = "reservations"
	reservationNameKey = "compute.googleapis.com/reservation-name"
	resvAny        = "ANY_RESERVATION"
	resvSpecific   = "SPECIFIC_RESERVATION"
	resvNone       = "NO_RESERVATION"
	reservationReady   = "READY"
)

func reservationTarget(project, zone string, r *computeapi.Reservation) target {
	return target{project: project, scope: scopeZone, loc: zone, collection: collReservations, name: r.Name, id: r.Id}
}

func (b *InMemoryBackend) getReservationLocked(project, zone, name string) (*computeapi.Reservation, *APIError) {
	r, ok := b.reservations[key(project, zone, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeZone, zone, collReservations, name))
	}

	return r, nil
}

func (b *InMemoryBackend) reservationMachineLocked(project, zone string, sr *computeapi.AllocationSpecificSKUReservation) (string, *APIError) {
	if sr.SourceInstanceTemplate != "" {
		t, err := b.resolveTemplateLocked(project, sr.SourceInstanceTemplate, "specificReservation.sourceInstanceTemplate")
		if err != nil {
			return "", err
		}

		return b.resolveMachineLocked(project, zone, t.Properties.MachineType)
	}

	if sr.InstanceProperties == nil || sr.InstanceProperties.MachineType == "" {
		return "", errRequired("specificReservation.instanceProperties.machineType")
	}

	return b.resolveMachineLocked(project, zone, sr.InstanceProperties.MachineType)
}

func (b *InMemoryBackend) insertReservation(
	project, zone string, in *computeapi.Reservation,
) (*computeapi.Operation, error) {
	b.mu.Lock("InsertReservation")
	defer b.mu.Unlock()

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if !zoneExists(zone) {
		return nil, errNotFound(scopePath(project, scopeZone, zone))
	}

	if _, dup := b.reservations[key(project, zone, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeZone, zone, collReservations, in.Name))
	}

	sr := in.SpecificReservation
	if sr == nil {
		return nil, errRequired("specificReservation")
	}

	if sr.Count < 1 {
		return nil, errField("specificReservation.count", strconv.FormatInt(sr.Count, 10), "Must be at least 1.")
	}

	machine, err := b.reservationMachineLocked(project, zone, sr)
	if err != nil {
		return nil, err
	}

	props := &computeapi.AllocationSpecificSKUAllocationReservedInstanceProperties{MachineType: machine}
	if sr.InstanceProperties != nil {
		props.GuestAccelerators = sr.InstanceProperties.GuestAccelerators
		props.MinCpuPlatform = sr.InstanceProperties.MinCpuPlatform
	}

	r := &computeapi.Reservation{
		Kind: "compute#reservation", Id: newID(), CreationTimestamp: b.stamp(), Name: in.Name,
		Description: in.Description, Status: reservationReady, Zone: scopeURL(project, scopeZone, zone),
		SelfLink:                    zoneLink(project, zone, collReservations, in.Name),
		SpecificReservationRequired: in.SpecificReservationRequired, ShareSettings: in.ShareSettings,
		SpecificReservation: &computeapi.AllocationSpecificSKUReservation{
			Count: sr.Count, InstanceProperties: props, SourceInstanceTemplate: sr.SourceInstanceTemplate,
		},
	}
	b.reservations[key(project, zone, in.Name)] = r

	return b.newOpLocked("insert", reservationTarget(project, zone, r), b.timing.Done), nil
}

// reservationUsageLocked returns how many slots each reservation of the zone has consumed by live instances.
func (b *InMemoryBackend) reservationUsageLocked(project, zone string) map[string]int64 {
	used := map[string]int64{}
	now := b.now().UTC()

	var insts []*instRec

	for k, r := range b.instances {
		if strings.HasPrefix(k, project+"/"+zone+"/") {
			insts = append(insts, r)
		}
	}

	sort.Slice(insts, func(i, j int) bool { return insts[i].Inst.Name < insts[j].Inst.Name })

	for _, r := range insts {
		switch r.status(now, b.timing) {
		case stRunning, stProvision, stStaging:
		default:
			continue
		}

		if name := b.consumedReservationLocked(project, zone, r.Inst, used); name != "" {
			used[name]++
		}
	}

	return used
}

func (b *InMemoryBackend) consumedReservationLocked(
	project, zone string, inst *computeapi.Instance, used map[string]int64,
) string {
	aff := inst.ReservationAffinity
	if aff == nil || aff.ConsumeReservationType == resvNone {
		return ""
	}

	machine := lastSegment(inst.MachineType)

	var names []string

	for k, res := range b.reservations {
		if strings.HasPrefix(k, project+"/"+zone+"/") {
			names = append(names, res.Name)
		}
	}

	sort.Strings(names)

	for _, n := range names {
		res := b.reservations[key(project, zone, n)]
		if lastSegment(res.SpecificReservation.InstanceProperties.MachineType) != machine ||
			used[n] >= res.SpecificReservation.Count {
			continue
		}

		if aff.ConsumeReservationType == resvSpecific {
			if slices.ContainsFunc(aff.Values, func(v string) bool { return lastSegment(v) == n }) {
				return n
			}

			continue
		}

		if !res.SpecificReservationRequired {
			return n
		}
	}

	return ""
}

func (b *InMemoryBackend) reservationViewLocked(project, zone string, r *computeapi.Reservation) *computeapi.Reservation {
	out := clone(r)
	out.SpecificReservation.InUseCount = b.reservationUsageLocked(project, zone)[r.Name]

	return out
}

func (b *InMemoryBackend) getReservation(project, zone, name string) (*computeapi.Reservation, error) {
	b.mu.RLock("GetReservation")
	defer b.mu.RUnlock()

	r, err := b.getReservationLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	return b.reservationViewLocked(project, zone, r), nil
}

func (b *InMemoryBackend) listReservations(project, scope, loc string) []entry {
	b.mu.RLock("ListReservations")
	defer b.mu.RUnlock()

	var out []entry

	for k, r := range b.reservations {
		zone := lastSegment(r.Zone)
		if !strings.HasPrefix(k, project+"/") || (scope == scopeZone && zone != loc) {
			continue
		}

		out = append(out, entry{scope: scopeZone + "/" + zone, item: b.reservationViewLocked(project, zone, r)})
	}

	return out
}

func (b *InMemoryBackend) deleteReservation(project, zone, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteReservation")
	defer b.mu.Unlock()

	r, err := b.getReservationLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if b.reservationUsageLocked(project, zone)[name] > 0 {
		return nil, errInUse("reservation", relPath(project, scopeZone, zone, collReservations, name), "an instance")
	}

	delete(b.reservations, key(project, zone, name))

	return b.newOpLocked("delete", reservationTarget(project, zone, r), b.timing.Done), nil
}

type reservationResizeRequest struct {
	SpecificSkuCount string `json:"specificSkuCount"`
}

func (b *InMemoryBackend) setReservationCountLocked(project, zone string, r *computeapi.Reservation, count int64) *APIError {
	if count < 1 {
		return errField("specificSkuCount", strconv.FormatInt(count, 10), "Must be at least 1.")
	}

	if inUse := b.reservationUsageLocked(project, zone)[r.Name]; count < inUse {
		return errField("specificSkuCount", strconv.FormatInt(count, 10),
			"Cannot be smaller than the number of instances using the reservation ("+strconv.FormatInt(inUse, 10)+").")
	}

	r.SpecificReservation.Count = count

	return nil
}

func (b *InMemoryBackend) resizeReservation(
	project, zone, name string, in reservationResizeRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("ResizeReservation")
	defer b.mu.Unlock()

	r, err := b.getReservationLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	n, perr := strconv.ParseInt(in.SpecificSkuCount, 10, 64)
	if perr != nil {
		return nil, errField("specificSkuCount", in.SpecificSkuCount, "")
	}

	if err = b.setReservationCountLocked(project, zone, r, n); err != nil {
		return nil, err
	}

	return b.newOpLocked("resize", reservationTarget(project, zone, r), b.timing.Done), nil
}

func (b *InMemoryBackend) updateReservation(
	project, zone, name string, in *computeapi.Reservation, paths []string,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateReservation")
	defer b.mu.Unlock()

	r, err := b.getReservationLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if len(paths) == 0 {
		return nil, errRequired("paths")
	}

	for _, p := range paths {
		switch p {
		case "specificReservation.count":
			if in.SpecificReservation == nil {
				return nil, errRequired("specificReservation")
			}

			if err = b.setReservationCountLocked(project, zone, r, in.SpecificReservation.Count); err != nil {
				return nil, err
			}
		case "description":
			r.Description = in.Description
		default:
			return nil, errField("paths", p, "Only specificReservation.count and description can be updated.")
		}
	}

	return b.newOpLocked("update", reservationTarget(project, zone, r), b.timing.Done), nil
}

// normalizeAffinityLocked validates an instance's reservationAffinity and, for SPECIFIC_RESERVATION, that the
// named reservations exist, match the machine type and still have a free slot.
func (b *InMemoryBackend) normalizeAffinityLocked(
	project, zone, machine string, in *computeapi.ReservationAffinity,
) (*computeapi.ReservationAffinity, *APIError) {
	if in == nil || in.ConsumeReservationType == "" {
		return &computeapi.ReservationAffinity{ConsumeReservationType: resvAny}, nil
	}

	switch in.ConsumeReservationType {
	case resvAny, resvNone:
		if in.Key != "" || len(in.Values) > 0 {
			return nil, errInvalid(
				"Invalid value for field 'resource.reservationAffinity': key and values are only valid with SPECIFIC_RESERVATION.")
		}

		return &computeapi.ReservationAffinity{ConsumeReservationType: in.ConsumeReservationType}, nil
	case resvSpecific:
	default:
		return nil, errField("resource.reservationAffinity.consumeReservationType", in.ConsumeReservationType, "")
	}

	if in.Key != reservationNameKey || len(in.Values) == 0 {
		return nil, errInvalid("Invalid value for field 'resource.reservationAffinity': SPECIFIC_RESERVATION needs key %s and at least one value.", reservationNameKey)
	}

	usage := b.reservationUsageLocked(project, zone)
	free := false

	for _, v := range in.Values {
		res, err := b.getReservationLocked(project, zone, lastSegment(v))
		if err != nil {
			return nil, err
		}

		if lastSegment(res.SpecificReservation.InstanceProperties.MachineType) != lastSegment(machine) {
			return nil, errInvalid("Invalid value for field 'resource.reservationAffinity.values': reservation '%s' is for a different machine type.", res.Name)
		}

		free = free || usage[res.Name] < res.SpecificReservation.Count
	}

	if !free {
		return nil, errInvalid("The reservation does not have enough capacity for another instance.")
	}

	return clone(in), nil
}

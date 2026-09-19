package ec2

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---- IPAM Routing Policy Registrations ----

// Routing policy batch/delta document actions -- gopherstack's own vocabulary (see
// routingPolicyDeltaDoc's doc comment: no AWS-published schema exists for these fields).
const (
	routingPolicyActionCreate = "create"
	routingPolicyActionUpdate = "update"
	routingPolicyActionDelete = "delete"
)

// routingPolicyDeltaDoc is the JSON document this backend writes into
// IpamRoutingPolicyRegistrationDelta.DeltaJSON for a single-registration mutation. The real
// AWS wire format is documented only as "in JSON format" (api_op_CreateIpamRoutingPolicyRegistration.go
// et al. give no schema) -- this is gopherstack's own descriptive, non-fabricated record of the
// change actually applied, and BatchModifyIpamRoutingPolicyRegistrations round-trips the
// caller's own DeltaJson verbatim rather than reinterpreting it (see below).
type routingPolicyDeltaDoc struct {
	Action                          string   `json:"action"`
	Cidr                            string   `json:"cidr"`
	Description                     string   `json:"description,omitempty"`
	Asns                            []string `json:"asns,omitempty"`
	MaxLength                       int32    `json:"maxLength,omitempty"`
	PermitMoreSpecificAnnouncements bool     `json:"permitMoreSpecificAnnouncements,omitempty"`
}

// newRoutingPolicyDeltaLocked builds and stores a published delta for assocID. Must be called
// with b.mu held for writing.
func (b *InMemoryBackend) newRoutingPolicyDeltaLocked(
	assocID string,
	doc routingPolicyDeltaDoc,
) *IpamRoutingPolicyRegistrationDelta {
	body, _ := json.Marshal(doc)

	delta := &IpamRoutingPolicyRegistrationDelta{
		DeltaID:                           "ipam-rpr-delta-" + uuid.New().String()[:8],
		IpamInternetRegistryAssociationID: assocID,
		DeltaJSON:                         string(body),
		State:                             ipamRoutingPolicyDeltaStatePublished,
		CreatedAt:                         time.Now().UTC(),
	}
	b.ipamRoutingPolicyRegistrationDeltas.Put(delta)

	return delta
}

// CreateIpamRoutingPolicyRegistration creates a routing policy registration (a Route Origin
// Authorization) for a CIDR prefix under an internet registry association. The trailing force
// bool (also accepted by Modify/Delete/BatchModify below) only affects conflicts with
// currently-announced BGP routes; this backend has no BGP monitoring to conflict with, so it
// is accepted but has no effect.
func (b *InMemoryBackend) CreateIpamRoutingPolicyRegistration(
	assocID, cidr string, asns []string, description string, maxLength int32, permitMoreSpecific, _ bool,
) (*IpamRoutingPolicyRegistrationDelta, error) {
	if assocID == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	if len(asns) == 0 {
		return nil, fmt.Errorf("%w: Asns is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIpamRoutingPolicyRegistration")
	defer b.mu.Unlock()

	if !b.ipamInternetRegistryAssociations.Has(assocID) {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, assocID)
	}

	key := assocID + "|" + cidr
	if b.ipamRoutingPolicyRegistrations.Has(key) {
		return nil, fmt.Errorf(
			"%w: a routing policy registration already exists for %s in association %s",
			ErrIpamRoutingPolicyRegistrationExists, cidr, assocID,
		)
	}

	reg := &IpamRoutingPolicyRegistration{
		IpamInternetRegistryAssociationID: assocID,
		Cidr:                              cidr,
		Asns:                              append([]string(nil), asns...),
		Description:                       description,
		MaxLength:                         maxLength,
		PermitMoreSpecificAnnouncements:   permitMoreSpecific,
		State:                             ipamRoutingPolicyStateCreateComplete,
	}

	delta := b.newRoutingPolicyDeltaLocked(assocID, routingPolicyDeltaDoc{
		Action: routingPolicyActionCreate, Cidr: cidr, Asns: reg.Asns, MaxLength: maxLength,
		PermitMoreSpecificAnnouncements: permitMoreSpecific, Description: description,
	})
	reg.LatestDeltaID = delta.DeltaID
	b.ipamRoutingPolicyRegistrations.Put(reg)

	cp := *delta

	return &cp, nil
}

// ModifyIpamRoutingPolicyRegistration updates an existing routing policy registration's ASNs
// and optional attributes.
func (b *InMemoryBackend) ModifyIpamRoutingPolicyRegistration(
	assocID, cidr string, asns []string, description string, maxLength int32, permitMoreSpecific, _ bool,
) (*IpamRoutingPolicyRegistrationDelta, error) {
	if assocID == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	if len(asns) == 0 {
		return nil, fmt.Errorf("%w: Asns is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamRoutingPolicyRegistration")
	defer b.mu.Unlock()

	if !b.ipamInternetRegistryAssociations.Has(assocID) {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, assocID)
	}

	key := assocID + "|" + cidr

	reg, ok := b.ipamRoutingPolicyRegistrations.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s in association %s", ErrIpamRoutingPolicyRegistrationNotFound, cidr, assocID)
	}

	reg.Asns = append([]string(nil), asns...)
	if description != "" {
		reg.Description = description
	}

	if maxLength > 0 {
		reg.MaxLength = maxLength
	}

	reg.PermitMoreSpecificAnnouncements = permitMoreSpecific
	reg.State = ipamRoutingPolicyStateUpdateComplete

	delta := b.newRoutingPolicyDeltaLocked(assocID, routingPolicyDeltaDoc{
		Action: routingPolicyActionUpdate, Cidr: cidr, Asns: reg.Asns, MaxLength: reg.MaxLength,
		PermitMoreSpecificAnnouncements: reg.PermitMoreSpecificAnnouncements, Description: reg.Description,
	})
	reg.LatestDeltaID = delta.DeltaID

	cp := *delta

	return &cp, nil
}

// DeleteIpamRoutingPolicyRegistration removes a routing policy registration.
func (b *InMemoryBackend) DeleteIpamRoutingPolicyRegistration(
	assocID, cidr string, _ bool,
) (*IpamRoutingPolicyRegistrationDelta, error) {
	if assocID == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamRoutingPolicyRegistration")
	defer b.mu.Unlock()

	if !b.ipamInternetRegistryAssociations.Has(assocID) {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, assocID)
	}

	key := assocID + "|" + cidr
	if !b.ipamRoutingPolicyRegistrations.Has(key) {
		return nil, fmt.Errorf("%w: %s in association %s", ErrIpamRoutingPolicyRegistrationNotFound, cidr, assocID)
	}

	b.ipamRoutingPolicyRegistrations.Delete(key)

	delta := b.newRoutingPolicyDeltaLocked(
		assocID, routingPolicyDeltaDoc{Action: routingPolicyActionDelete, Cidr: cidr},
	)

	cp := *delta

	return &cp, nil
}

// routingPolicyBatchEntry is one element of BatchModifyIpamRoutingPolicyRegistrations'
// DeltaJson input array: {"action":"create|update|delete","cidr":"...","asns":[...],
// "maxLength":N,"permitMoreSpecificAnnouncements":bool,"description":"..."}. See
// routingPolicyDeltaDoc's doc comment: no AWS-published schema exists for this field, so this
// is gopherstack's own, applied consistently between input parsing and output round-trip.
type routingPolicyBatchEntry struct {
	Action                          string   `json:"action"`
	Cidr                            string   `json:"cidr"`
	Description                     string   `json:"description,omitempty"`
	Asns                            []string `json:"asns,omitempty"`
	MaxLength                       int32    `json:"maxLength,omitempty"`
	PermitMoreSpecificAnnouncements bool     `json:"permitMoreSpecificAnnouncements,omitempty"`
}

// validateRoutingPolicyBatchEntryLocked checks that a single batch entry's action is
// applicable given current state, without mutating anything. Must be called with b.mu held.
func (b *InMemoryBackend) validateRoutingPolicyBatchEntryLocked(assocID string, e routingPolicyBatchEntry) error {
	key := assocID + "|" + e.Cidr

	switch e.Action {
	case routingPolicyActionCreate:
		if e.Cidr == "" || len(e.Asns) == 0 {
			return fmt.Errorf("%w: batch create entry requires cidr and asns", ErrInvalidParameter)
		}

		if b.ipamRoutingPolicyRegistrations.Has(key) {
			return fmt.Errorf(
				"%w: a routing policy registration already exists for %s",
				ErrIpamRoutingPolicyRegistrationExists, e.Cidr,
			)
		}
	case routingPolicyActionUpdate, routingPolicyActionDelete:
		if !b.ipamRoutingPolicyRegistrations.Has(key) {
			return fmt.Errorf("%w: %s in association %s", ErrIpamRoutingPolicyRegistrationNotFound, e.Cidr, assocID)
		}
	default:
		return fmt.Errorf("%w: unknown batch action %q", ErrInvalidParameter, e.Action)
	}

	return nil
}

// applyRoutingPolicyBatchEntryLocked mutates state for a single, already-validated batch
// entry. Must be called with b.mu held.
func (b *InMemoryBackend) applyRoutingPolicyBatchEntryLocked(assocID string, e routingPolicyBatchEntry) {
	key := assocID + "|" + e.Cidr

	switch e.Action {
	case routingPolicyActionDelete:
		b.ipamRoutingPolicyRegistrations.Delete(key)
	case routingPolicyActionCreate:
		b.ipamRoutingPolicyRegistrations.Put(&IpamRoutingPolicyRegistration{
			IpamInternetRegistryAssociationID: assocID,
			Cidr:                              e.Cidr,
			Asns:                              append([]string(nil), e.Asns...),
			Description:                       e.Description,
			MaxLength:                         e.MaxLength,
			PermitMoreSpecificAnnouncements:   e.PermitMoreSpecificAnnouncements,
			State:                             ipamRoutingPolicyStateCreateComplete,
		})
	case routingPolicyActionUpdate:
		reg, _ := b.ipamRoutingPolicyRegistrations.Get(key)
		if len(e.Asns) > 0 {
			reg.Asns = append([]string(nil), e.Asns...)
		}

		if e.Description != "" {
			reg.Description = e.Description
		}

		if e.MaxLength > 0 {
			reg.MaxLength = e.MaxLength
		}

		reg.PermitMoreSpecificAnnouncements = e.PermitMoreSpecificAnnouncements
		reg.State = ipamRoutingPolicyStateUpdateComplete
	}
}

// BatchModifyIpamRoutingPolicyRegistrations applies a batch of create/update/delete routing
// policy registration changes described by deltaJSON, atomically (every entry validated
// before any mutation is applied).
func (b *InMemoryBackend) BatchModifyIpamRoutingPolicyRegistrations(
	assocID, deltaJSON string, _ bool,
) (*IpamRoutingPolicyRegistrationDelta, error) {
	if assocID == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	if deltaJSON == "" {
		return nil, fmt.Errorf("%w: DeltaJson is required", ErrInvalidParameter)
	}

	var entries []routingPolicyBatchEntry
	if err := json.Unmarshal([]byte(deltaJSON), &entries); err != nil {
		return nil, fmt.Errorf("%w: DeltaJson is not a valid batch document: %w", ErrInvalidParameter, err)
	}

	b.mu.Lock("BatchModifyIpamRoutingPolicyRegistrations")
	defer b.mu.Unlock()

	if !b.ipamInternetRegistryAssociations.Has(assocID) {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, assocID)
	}

	for _, e := range entries {
		if err := b.validateRoutingPolicyBatchEntryLocked(assocID, e); err != nil {
			return nil, err
		}
	}

	for _, e := range entries {
		b.applyRoutingPolicyBatchEntryLocked(assocID, e)
	}

	delta := &IpamRoutingPolicyRegistrationDelta{
		DeltaID:                           "ipam-rpr-delta-" + uuid.New().String()[:8],
		IpamInternetRegistryAssociationID: assocID,
		DeltaJSON:                         deltaJSON,
		State:                             ipamRoutingPolicyDeltaStatePublished,
		CreatedAt:                         time.Now().UTC(),
	}
	b.ipamRoutingPolicyRegistrationDeltas.Put(delta)

	for _, e := range entries {
		if e.Action == routingPolicyActionDelete {
			continue
		}

		if reg, ok := b.ipamRoutingPolicyRegistrations.Get(assocID + "|" + e.Cidr); ok {
			reg.LatestDeltaID = delta.DeltaID
		}
	}

	cp := *delta

	return &cp, nil
}

// GetIpamRoutingPolicyRegistrations returns routing policy registrations for an association,
// optionally filtered to a single CIDR.
func (b *InMemoryBackend) GetIpamRoutingPolicyRegistrations(
	assocID, cidr string,
) ([]*IpamRoutingPolicyRegistration, error) {
	if assocID == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamRoutingPolicyRegistrations")
	defer b.mu.RUnlock()

	if !b.ipamInternetRegistryAssociations.Has(assocID) {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, assocID)
	}

	out := make([]*IpamRoutingPolicyRegistration, 0, b.ipamRoutingPolicyRegistrations.Len())

	for _, reg := range b.ipamRoutingPolicyRegistrations.All() {
		if reg.IpamInternetRegistryAssociationID != assocID {
			continue
		}

		if cidr != "" && reg.Cidr != cidr {
			continue
		}

		cp := *reg
		cp.Asns = append([]string(nil), reg.Asns...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Cidr < out[j].Cidr })

	return out, nil
}

// GetIpamRoutingPolicyRegistrationDeltas returns the change history for an association,
// optionally filtered by delta ID and/or a [startTime, endTime] window, in the requested
// chronological order (ChronologicalOrderReverse = "reverse"; anything else is forward).
func (b *InMemoryBackend) GetIpamRoutingPolicyRegistrationDeltas(
	assocID, deltaID, chronologicalOrder string, startTime, endTime *time.Time,
) ([]*IpamRoutingPolicyRegistrationDelta, error) {
	if assocID == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamRoutingPolicyRegistrationDeltas")
	defer b.mu.RUnlock()

	if !b.ipamInternetRegistryAssociations.Has(assocID) {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, assocID)
	}

	out := make([]*IpamRoutingPolicyRegistrationDelta, 0, b.ipamRoutingPolicyRegistrationDeltas.Len())

	for _, delta := range b.ipamRoutingPolicyRegistrationDeltas.All() {
		if delta.IpamInternetRegistryAssociationID != assocID {
			continue
		}

		if deltaID != "" && delta.DeltaID != deltaID {
			continue
		}

		if startTime != nil && delta.CreatedAt.Before(*startTime) {
			continue
		}

		if endTime != nil && delta.CreatedAt.After(*endTime) {
			continue
		}

		cp := *delta
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		if chronologicalOrder == "reverse" {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}

		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})

	return out, nil
}

// GetIpamRouteOriginAuthorizations returns the Route Origin Authorizations currently
// considered published to the RPKI for an association, derived from its routing policy
// registrations (one entry per (ASN, CIDR) pair) -- never fabricated.
func (b *InMemoryBackend) GetIpamRouteOriginAuthorizations(
	assocID, cidr string,
) ([]*IpamRouteOriginAuthorization, error) {
	if assocID == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamRouteOriginAuthorizations")
	defer b.mu.RUnlock()

	if !b.ipamInternetRegistryAssociations.Has(assocID) {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, assocID)
	}

	var out []*IpamRouteOriginAuthorization

	for _, reg := range b.ipamRoutingPolicyRegistrations.All() {
		if reg.IpamInternetRegistryAssociationID != assocID {
			continue
		}

		if cidr != "" && reg.Cidr != cidr {
			continue
		}

		for _, asn := range reg.Asns {
			out = append(out, &IpamRouteOriginAuthorization{Asn: asn, Cidr: reg.Cidr, MaxLength: reg.MaxLength})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Cidr != out[j].Cidr {
			return out[i].Cidr < out[j].Cidr
		}

		return out[i].Asn < out[j].Asn
	})

	return out, nil
}

// GetIpamDiscoveredRoutes returns BGP routes discovered by IPAM resource discovery for a
// Region. This backend does not model BGP route discovery (no live-network monitoring
// pipeline exists to feed real data); it validates the resource discovery ID for a real
// error path and otherwise always returns an empty result -- recorded as a structural gap in
// PARITY.md, not a fabricated data source.
func (b *InMemoryBackend) GetIpamDiscoveredRoutes(resourceDiscoveryID string) error {
	if resourceDiscoveryID == "" {
		return fmt.Errorf("%w: IpamResourceDiscoveryId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamDiscoveredRoutes")
	defer b.mu.RUnlock()

	if !b.ipamResourceDiscoveries.Has(resourceDiscoveryID) {
		return fmt.Errorf("%w: %s", ErrIpamResourceDiscoveryNotFound, resourceDiscoveryID)
	}

	return nil
}

// GetIpamRouteProtectionFindings returns RPKI route protection findings for an IPAM. This
// backend does not model RPKI route validation against live BGP announcements; it validates
// the IPAM ID for a real error path and otherwise always returns an empty result -- recorded
// as a structural gap in PARITY.md, not a fabricated data source.
func (b *InMemoryBackend) GetIpamRouteProtectionFindings(ipamID string) error {
	if ipamID == "" {
		return fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamRouteProtectionFindings")
	defer b.mu.RUnlock()

	if !b.ipams.Has(ipamID) {
		return fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	return nil
}

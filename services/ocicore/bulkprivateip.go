package ocicore

import (
	"net/http"
	"strings"
)

const (
	opBulkCreatePrivateIPs = "BULK_CREATE_PRIVATE_IPS"
	opBulkDeletePrivateIPs = "BULK_DELETE_PRIVATE_IPS"
	opBulkDetachPrivateIPs = "BULK_DETACH_PRIVATE_IPS"
	opBulkUpdatePrivateIPs = "BULK_UPDATE_PRIVATE_IPS"

	entityPrivateIPWR = "privateip"
	wrActionDeleted   = "DELETED"

	maxBulkPrivateIPs = 32
)

type bulkCreateItem struct {
	TagsIn
	CidrPrefixLength *int   `json:"cidrPrefixLength"`
	DisplayName      string `json:"displayName"`
	HostnameLabel    string `json:"hostnameLabel"`
	IPAddress        string `json:"ipAddress"`
	Lifetime         string `json:"lifetime"`
	RouteTableID     string `json:"routeTableId"`
	Ipv4SubnetCidr   string `json:"ipv4SubnetCidrAtCreation"`
}

type bulkCreateIn struct {
	VnicID   string           `json:"vnicId"`
	VlanID   string           `json:"vlanId"`
	SubnetID string           `json:"subnetId"`
	Items    []bulkCreateItem `json:"bulkCreatePrivateIpItem"`
}

type bulkIDItem struct {
	PrivateIPID string `json:"privateIpId"`
}

type bulkIDsIn struct {
	VnicID   string       `json:"vnicId"`
	VlanID   string       `json:"vlanId"`
	SubnetID string       `json:"subnetId"`
	Items    []bulkIDItem `json:"bulkDeletePrivateIpItem"`
}

type bulkDetachIn struct {
	VnicID string       `json:"vnicId"`
	Items  []bulkIDItem `json:"bulkDetachPrivateIpItem"`
}

type bulkUpdateItem struct {
	TagsIn
	DisplayName   *string `json:"displayName"`
	HostnameLabel *string `json:"hostnameLabel"`
	Lifetime      *string `json:"lifetime"`
	RouteTableID  *string `json:"routeTableId"`
	PrivateIPID   string  `json:"privateIpId"`
}

type bulkUpdateIn struct {
	VnicID string           `json:"vnicId"`
	Items  []bulkUpdateItem `json:"bulkUpdatePrivateIpItem"`
}

func bulkCount(n int) *APIError {
	if n == 0 {
		return errMissing("at least one private IP item is required")
	}

	if n > maxBulkPrivateIPs {
		return errLimit("A bulk private IP request takes at most 32 items")
	}

	return nil
}

func (b *InMemoryBackend) bulkWorkRequest(op string, ids []string, action, compartment string) string {
	res := make([]WorkResource, 0, len(ids))

	for _, id := range ids {
		res = append(res, WorkResource{
			ActionType: action, EntityType: entityPrivateIPWR, Identifier: id, EntityURI: apiBase + "/privateIps/" + id,
		})
	}

	return b.newWorkRequest(op, compartment, res)
}

// bulkOnce replays the first work request for a repeated retry token.
func (b *InMemoryBackend) bulkOnce(scope, token string, run func() (string, *APIError)) (string, *APIError) {
	b.mu.Lock("bulkOnce")
	defer b.mu.Unlock()

	var (
		wr   string
		aerr *APIError
	)

	_ = b.retry.Once(scope, token, b.nowUTC(), func(v string) error {
		wr = v

		return nil
	}, func() (string, error) {
		wr, aerr = run()
		if aerr != nil {
			return "", aerr
		}

		return wr, nil
	})

	return wr, aerr
}

// BulkCreatePrivateIPs creates secondary private IPs in one request, all or none.
func (b *InMemoryBackend) BulkCreatePrivateIPs(token string, in bulkCreateIn) (string, *APIError) {
	if aerr := bulkCount(len(in.Items)); aerr != nil {
		return "", aerr
	}

	if in.VlanID != "" {
		return "", errParam("vlanId is not supported")
	}

	for _, it := range in.Items {
		if aerr := b.checkTags(it); aerr != nil {
			return "", aerr
		}

		if it.RouteTableID != "" || it.CidrPrefixLength != nil || it.Ipv4SubnetCidr != "" {
			return "", errParam("routeTableId, cidrPrefixLength and ipv4SubnetCidrAtCreation are not supported")
		}
	}

	return b.bulkOnce("BulkCreatePrivateIps", token, func() (string, *APIError) {
		return b.bulkCreateLocked(in)
	})
}

func (b *InMemoryBackend) bulkCreateLocked(in bulkCreateIn) (string, *APIError) {
	ids := make([]string, 0, len(in.Items))
	comp := ""

	for _, it := range in.Items {
		v, _, aerr := b.CreatePrivateIP(CreatePrivateIPIn{
			TagsIn: it.TagsIn, DisplayName: it.DisplayName, HostnameLabel: it.HostnameLabel, IPAddress: it.IPAddress,
			Lifetime: it.Lifetime, SubnetID: in.SubnetID, VnicID: in.VnicID,
		})
		if aerr != nil {
			for _, id := range ids {
				if p, ok := b.res[id].(*PrivateIP); ok {
					b.dropPrivateIP(p)
				}
			}

			return "", aerr
		}

		base := v.base()
		ids, comp = append(ids, base.ID), base.CompartmentID
	}

	return b.bulkWorkRequest(opBulkCreatePrivateIPs, ids, wrActionCreated, comp), nil
}

func (b *InMemoryBackend) secondaryIP(id string) (*PrivateIP, *APIError) {
	p, aerr := getLive[*PrivateIP](b, id)
	if aerr != nil {
		return nil, aerr
	}

	if p.IsPrimary {
		return nil, errConflict("The primary private IP " + id + " of a VNIC cannot be changed in bulk")
	}

	return p, nil
}

func (b *InMemoryBackend) secondaryIPs(items []bulkIDItem, vnicID, subnetID string) ([]*PrivateIP, *APIError) {
	if aerr := bulkCount(len(items)); aerr != nil {
		return nil, aerr
	}

	seen := map[string]bool{}
	out := make([]*PrivateIP, 0, len(items))

	for _, it := range items {
		p, aerr := b.secondaryIP(it.PrivateIPID)
		if aerr != nil {
			return nil, aerr
		}

		if seen[p.ID] {
			return nil, errParam("The private IP " + p.ID + " is listed twice")
		}

		seen[p.ID] = true

		if (vnicID != "" && p.VnicID != vnicID) || (subnetID != "" && p.SubnetID != subnetID) {
			return nil, errParam("The private IP " + p.ID + " does not belong to the given vnicId or subnetId")
		}

		out = append(out, p)
	}

	return out, nil
}

// BulkDeletePrivateIPs deletes secondary private IPs in one request, all or none.
func (b *InMemoryBackend) BulkDeletePrivateIPs(token string, in bulkIDsIn) (string, *APIError) {
	if in.VlanID != "" {
		return "", errParam("vlanId is not supported")
	}

	return b.bulkOnce("BulkDeletePrivateIps", token, func() (string, *APIError) {
		ps, aerr := b.secondaryIPs(in.Items, in.VnicID, in.SubnetID)
		if aerr != nil {
			return "", aerr
		}

		ids := make([]string, 0, len(ps))
		comp := ps[0].CompartmentID

		for _, p := range ps {
			if rt := firstID(liveOf(b, routesTo(p.ID))); rt != "" {
				return "", errConflict("The private IP " + p.ID + " is the target of a rule in route table " + rt)
			}

			ids = append(ids, p.ID)
		}

		for _, p := range ps {
			b.dropPrivateIP(p)
		}

		return b.bulkWorkRequest(opBulkDeletePrivateIPs, ids, wrActionDeleted, comp), nil
	})
}

// BulkDetachPrivateIPs unassigns reserved private IPs from a VNIC in one request, all or none.
func (b *InMemoryBackend) BulkDetachPrivateIPs(token string, in bulkDetachIn) (string, *APIError) {
	return b.bulkOnce("BulkDetachPrivateIps", token, func() (string, *APIError) {
		ps, aerr := b.secondaryIPs(in.Items, in.VnicID, "")
		if aerr != nil {
			return "", aerr
		}

		ids := make([]string, 0, len(ps))

		for _, p := range ps {
			if p.VnicID == "" {
				return "", errState("The private IP " + p.ID + " is not assigned to a VNIC")
			}

			if p.Lifetime != lifetimeReserved {
				return "", errParam("Only a RESERVED private IP can be unassigned")
			}

			ids = append(ids, p.ID)
		}

		for _, p := range ps {
			b.releasePublicOf(p)
			p.VnicID, p.IPState = "", statAvailable
			b.bump(p)
		}

		return b.bulkWorkRequest(opBulkDetachPrivateIPs, ids, wrActionUpdated, ps[0].CompartmentID), nil
	})
}

// BulkUpdatePrivateIPs updates secondary private IPs, optionally moving them to another VNIC, all or none.
func (b *InMemoryBackend) BulkUpdatePrivateIPs(token string, in bulkUpdateIn) (string, *APIError) {
	if aerr := bulkCount(len(in.Items)); aerr != nil {
		return "", aerr
	}

	for _, it := range in.Items {
		if aerr := b.checkTags(it); aerr != nil {
			return "", aerr
		}

		if it.RouteTableID != nil && *it.RouteTableID != "" {
			return "", errParam("routeTableId is not supported")
		}

		if it.DisplayName != nil {
			if aerr := validName(*it.DisplayName); aerr != nil {
				return "", aerr
			}
		}
	}

	return b.bulkOnce("BulkUpdatePrivateIps", token, func() (string, *APIError) {
		return b.bulkUpdateLocked(in)
	})
}

func (b *InMemoryBackend) bulkUpdateLocked(in bulkUpdateIn) (string, *APIError) {
	ps := make([]*PrivateIP, 0, len(in.Items))
	labels := map[string]string{}

	for _, it := range in.Items {
		p, aerr := b.checkBulkUpdate(it, in.VnicID, labels)
		if aerr != nil {
			return "", aerr
		}

		ps = append(ps, p)
	}

	ids := make([]string, 0, len(ps))

	for i, p := range ps {
		it := in.Items[i]

		if it.HostnameLabel != nil {
			p.HostnameLabel = *it.HostnameLabel
		}

		if it.Lifetime != nil {
			p.Lifetime = *it.Lifetime
		}

		if in.VnicID != "" && p.VnicID != in.VnicID {
			b.moveToVnic(p, in.VnicID)
		}

		b.updateCommon(p, UpdateIn{DisplayName: it.DisplayName, TagsIn: it.TagsIn})
		ids = append(ids, p.ID)
	}

	return b.bulkWorkRequest(opBulkUpdatePrivateIPs, ids, wrActionUpdated, ps[0].CompartmentID), nil
}

func (b *InMemoryBackend) checkBulkUpdate(
	it bulkUpdateItem,
	vnicID string,
	labels map[string]string,
) (*PrivateIP, *APIError) {
	p, aerr := b.secondaryIP(it.PrivateIPID)
	if aerr != nil {
		return nil, aerr
	}

	if _, dup := labels[p.ID]; dup {
		return nil, errParam("The private IP " + p.ID + " is listed twice")
	}

	labels[p.ID] = ""

	if it.HostnameLabel != nil {
		s, _ := get[*Subnet](b, p.SubnetID)
		if aerr = b.checkHostname(s, *it.HostnameLabel, p.ID); aerr != nil {
			return nil, aerr
		}

		key := p.SubnetID + "/" + strings.ToLower(*it.HostnameLabel)
		if *it.HostnameLabel != "" && labels[key] != "" {
			return nil, errConflict("The hostnameLabel " + *it.HostnameLabel + " is listed twice")
		}

		labels[key] = p.ID
	}

	if it.Lifetime != nil {
		if _, aerr = privateLifetime(*it.Lifetime, p.VnicID != "" || vnicID != ""); aerr != nil {
			return nil, aerr
		}
	}

	if vnicID != "" && p.VnicID != vnicID {
		v, verr := getLive[*Vnic](b, vnicID)
		if verr != nil {
			return nil, verr
		}

		if v.SubnetID != p.SubnetID {
			return nil, errParam("The VNIC is in a different subnet than the private IP")
		}
	}

	return p, nil
}

func (b *InMemoryBackend) moveToVnic(p *PrivateIP, vnicID string) {
	v, _ := get[*Vnic](b, vnicID)

	b.releasePublicOf(p)
	p.VnicID, p.IPState, p.AvailabilityDomain = v.ID, statAssigned, v.AvailabilityDomain
}

func bulkEntries() []entry {
	const base = "/privateIps/actions/"

	return []entry{
		{
			bulkOp((*InMemoryBackend).BulkCreatePrivateIPs),
			http.MethodPost,
			base + "bulkCreatePrivateIps",
			"BulkCreatePrivateIps",
		},
		{
			bulkOp((*InMemoryBackend).BulkDeletePrivateIPs),
			http.MethodPost,
			base + "bulkDeletePrivateIps",
			"BulkDeletePrivateIps",
		},
		{
			bulkOp((*InMemoryBackend).BulkDetachPrivateIPs),
			http.MethodPost,
			base + "bulkDetachPrivateIps",
			"BulkDetachPrivateIps",
		},
		{
			bulkOp((*InMemoryBackend).BulkUpdatePrivateIPs),
			http.MethodPost,
			base + "bulkUpdatePrivateIps",
			"BulkUpdatePrivateIps",
		},
	}
}

func bulkOp[In any](run func(*InMemoryBackend, string, In) (string, *APIError)) opFunc {
	return func(h *Handler, q *req) error {
		var in In
		if err := q.decode(&in); err != nil {
			return err
		}

		wr, aerr := run(h.Backend, q.r.Header.Get(headerRetry), in)
		if aerr != nil {
			return aerr
		}

		return q.acceptWorkRequest(wr)
	}
}

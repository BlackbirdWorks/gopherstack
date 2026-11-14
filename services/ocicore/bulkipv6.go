package ocicore

import (
	"net/http"
	"strings"
)

const (
	opBulkCreateIpv6s = "BULK_CREATE_IPV6S"
	opBulkDeleteIpv6s = "BULK_DELETE_IPV6S"
	opBulkDetachIpv6s = "BULK_DETACH_IPV6S"
	opBulkUpdateIpv6s = "BULK_UPDATE_IPV6S"

	entityIpv6WR = "ipv6"
)

type bulkIpv6CreateItem struct {
	TagsIn
	Hostname         *string `json:"hostname"`
	CidrPrefixLength *int    `json:"cidrPrefixLength"`
	DisplayName      string  `json:"displayName"`
	IPAddress        string  `json:"ipAddress"`
	Lifetime         string  `json:"lifetime"`
	RouteTableID     string  `json:"routeTableId"`
	Ipv6SubnetCidr   string  `json:"ipv6SubnetCidr"`
}

type bulkIpv6CreateIn struct {
	VnicID   string               `json:"vnicId"`
	SubnetID string               `json:"subnetId"`
	Items    []bulkIpv6CreateItem `json:"bulkCreateIpv6sItem"`
}

type bulkIpv6IDItem struct {
	Ipv6ID string `json:"ipv6Id"`
}

type bulkIpv6DeleteIn struct {
	VnicID   string           `json:"vnicId"`
	SubnetID string           `json:"subnetId"`
	Items    []bulkIpv6IDItem `json:"bulkDeleteIpv6sItem"`
}

type bulkIpv6DetachIn struct {
	VnicID string           `json:"vnicId"`
	Items  []bulkIpv6IDItem `json:"bulkDetachIpv6sItem"`
}

type bulkIpv6UpdateItem struct {
	TagsIn
	DisplayName  *string `json:"displayName"`
	Lifetime     *string `json:"lifetime"`
	RouteTableID *string `json:"routeTableId"`
	Hostname     *string `json:"hostname"`
	Ipv6ID       string  `json:"ipv6Id"`
}

type bulkIpv6UpdateIn struct {
	VnicID string               `json:"vnicId"`
	Items  []bulkIpv6UpdateItem `json:"bulkUpdateIpv6sItem"`
}

func (b *InMemoryBackend) bulkIpv6WorkRequest(op string, ips []*Ipv6, action string) string {
	ids := make([]string, 0, len(ips))
	for _, ip := range ips {
		ids = append(ids, ip.ID)
	}

	res := make([]WorkResource, 0, len(ids))
	for _, id := range ids {
		res = append(res, WorkResource{
			ActionType: action, EntityType: entityIpv6WR, Identifier: id, EntityURI: apiBase + "/ipv6/" + id,
		})
	}

	return b.newWorkRequest(op, ips[0].CompartmentID, res)
}

// BulkCreateIpv6s creates IPv6 addresses in one request, all or none.
func (b *InMemoryBackend) BulkCreateIpv6s(token string, in bulkIpv6CreateIn) (string, *APIError) {
	if aerr := bulkCount(len(in.Items)); aerr != nil {
		return "", aerr
	}

	for _, it := range in.Items {
		if aerr := b.checkTags(it); aerr != nil {
			return "", aerr
		}
	}

	return b.bulkOnce("BulkCreateIpv6s", token, func() (string, *APIError) {
		created := make([]*Ipv6, 0, len(in.Items))

		for _, it := range in.Items {
			ip, aerr := b.newIpv6(CreateIpv6In{
				TagsIn: it.TagsIn, CidrPrefixLength: it.CidrPrefixLength, Hostname: it.Hostname,
				DisplayName: it.DisplayName, IPAddress: it.IPAddress, VnicID: in.VnicID, SubnetID: in.SubnetID,
				Lifetime: it.Lifetime, RouteTableID: it.RouteTableID, Ipv6SubnetCidr: it.Ipv6SubnetCidr,
			})
			if aerr != nil {
				for _, c := range created {
					delete(b.res, c.ID)
					b.refreshVnic(c.VnicID)
				}

				return "", aerr
			}

			created = append(created, ip)
		}

		return b.bulkIpv6WorkRequest(opBulkCreateIpv6s, created, wrActionCreated), nil
	})
}

func (b *InMemoryBackend) bulkIpv6s(items []bulkIpv6IDItem, vnicID, subnetID string) ([]*Ipv6, *APIError) {
	if aerr := bulkCount(len(items)); aerr != nil {
		return nil, aerr
	}

	seen := map[string]bool{}
	out := make([]*Ipv6, 0, len(items))

	for _, it := range items {
		ip, aerr := getLive[*Ipv6](b, it.Ipv6ID)
		if aerr != nil {
			return nil, aerr
		}

		if seen[ip.ID] {
			return nil, errParam("The IPv6 " + ip.ID + " is listed twice")
		}

		seen[ip.ID] = true

		if (vnicID != "" && ip.VnicID != vnicID) || (subnetID != "" && ip.SubnetID != subnetID) {
			return nil, errParam("The IPv6 " + ip.ID + " does not belong to the given vnicId or subnetId")
		}

		out = append(out, ip)
	}

	return out, nil
}

// BulkDeleteIpv6s deletes IPv6 addresses in one request, all or none.
func (b *InMemoryBackend) BulkDeleteIpv6s(token string, in bulkIpv6DeleteIn) (string, *APIError) {
	return b.bulkOnce("BulkDeleteIpv6s", token, func() (string, *APIError) {
		ips, aerr := b.bulkIpv6s(in.Items, in.VnicID, in.SubnetID)
		if aerr != nil {
			return "", aerr
		}

		for _, ip := range ips {
			if aerr = b.deleteCheck(ip); aerr != nil {
				return "", aerr
			}
		}

		wr := b.bulkIpv6WorkRequest(opBulkDeleteIpv6s, ips, wrActionDeleted)

		for _, ip := range ips {
			b.terminate(ip)
		}

		return wr, nil
	})
}

// BulkDetachIpv6s unassigns reserved IPv6 addresses from a VNIC in one request, all or none.
func (b *InMemoryBackend) BulkDetachIpv6s(token string, in bulkIpv6DetachIn) (string, *APIError) {
	return b.bulkOnce("BulkDetachIpv6s", token, func() (string, *APIError) {
		ips, aerr := b.bulkIpv6s(in.Items, in.VnicID, "")
		if aerr != nil {
			return "", aerr
		}

		for _, ip := range ips {
			if ip.VnicID == "" {
				return "", errState("The IPv6 " + ip.ID + " is not assigned to a VNIC")
			}

			if ip.Lifetime == lifetimeEphemeral {
				return "", errParam("An ephemeral IPv6 cannot be unassigned: delete it instead")
			}
		}

		for _, ip := range ips {
			old := ip.VnicID
			ip.VnicID, ip.IPState = "", statAvailable
			b.bump(ip)
			b.refreshVnic(old)
		}

		return b.bulkIpv6WorkRequest(opBulkDetachIpv6s, ips, wrActionUpdated), nil
	})
}

// BulkUpdateIpv6s updates IPv6 addresses, optionally moving them to another VNIC, all or none.
func (b *InMemoryBackend) BulkUpdateIpv6s(token string, in bulkIpv6UpdateIn) (string, *APIError) {
	if aerr := bulkCount(len(in.Items)); aerr != nil {
		return "", aerr
	}

	for _, it := range in.Items {
		if aerr := b.checkTags(it); aerr != nil {
			return "", aerr
		}

		if it.DisplayName != nil {
			if aerr := validName(*it.DisplayName); aerr != nil {
				return "", aerr
			}
		}
	}

	return b.bulkOnce("BulkUpdateIpv6s", token, func() (string, *APIError) {
		return b.bulkUpdateIpv6Locked(in)
	})
}

func (b *InMemoryBackend) bulkUpdateIpv6Locked(in bulkIpv6UpdateIn) (string, *APIError) {
	ips := make([]*Ipv6, 0, len(in.Items))
	next := make([]Ipv6, 0, len(in.Items))
	oldVnics := make([]string, 0, len(in.Items))
	seen, labels := map[string]bool{}, map[string]bool{}

	for _, it := range in.Items {
		ip, aerr := getLive[*Ipv6](b, it.Ipv6ID)
		if aerr != nil {
			return "", aerr
		}

		if seen[ip.ID] {
			return "", errParam("The IPv6 " + ip.ID + " is listed twice")
		}

		seen[ip.ID] = true

		s, aerr := getLive[*Subnet](b, ip.SubnetID)
		if aerr != nil {
			return "", aerr
		}

		cp := *ip
		if aerr = b.applyIpv6Update(s, &cp, UpdateIpv6In{
			VnicID: vnicPtr(in.VnicID), Hostname: it.Hostname, RouteTableID: it.RouteTableID, Lifetime: it.Lifetime,
		}); aerr != nil {
			return "", aerr
		}

		if it.Hostname != nil && *it.Hostname != "" {
			key := ip.SubnetID + "/" + strings.ToLower(*it.Hostname)
			if labels[key] {
				return "", errConflict("The hostname " + *it.Hostname + " is listed twice")
			}

			labels[key] = true
		}

		ips, next, oldVnics = append(ips, ip), append(next, cp), append(oldVnics, ip.VnicID)
	}

	for i, ip := range ips {
		*ip = next[i]
		b.updateCommon(ip, UpdateIn{DisplayName: in.Items[i].DisplayName, TagsIn: in.Items[i].TagsIn})
		b.refreshVnic(oldVnics[i])
		b.refreshVnic(ip.VnicID)
	}

	return b.bulkIpv6WorkRequest(opBulkUpdateIpv6s, ips, wrActionUpdated), nil
}

func vnicPtr(id string) *string {
	if id == "" {
		return nil
	}

	return &id
}

func bulkIpv6Entries() []entry {
	const base = "/ipv6/actions/"

	return []entry{
		{bulkOp((*InMemoryBackend).BulkCreateIpv6s), http.MethodPost, base + "bulkCreateIpv6s", "BulkCreateIpv6s"},
		{bulkOp((*InMemoryBackend).BulkDeleteIpv6s), http.MethodPost, base + "bulkDeleteIpv6s", "BulkDeleteIpv6s"},
		{bulkOp((*InMemoryBackend).BulkDetachIpv6s), http.MethodPost, base + "bulkDetachIpv6s", "BulkDetachIpv6s"},
		{bulkOp((*InMemoryBackend).BulkUpdateIpv6s), http.MethodPost, base + "bulkUpdateIpv6s", "BulkUpdateIpv6s"},
	}
}

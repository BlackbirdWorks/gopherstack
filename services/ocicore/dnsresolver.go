package ocicore

import "net/http"

// VcnDNSResolverAssociation is the GetVcnDNSResolverAssociation response.
type VcnDNSResolverAssociation struct {
	VcnID          string `json:"vcnId"`
	LifecycleState string `json:"lifecycleState"`
	DNSResolverID  string `json:"dnsResolverId,omitempty"`
}

// SetDNSResolverLookup resolves the private resolver of a VCN from OCI DNS.
func (b *InMemoryBackend) SetDNSResolverLookup(fn func(vcnID string) (string, bool)) {
	b.mu.Lock("SetDNSResolverLookup")
	b.dnsResolver = fn
	b.mu.Unlock()
}

// GetVcnDNSResolverAssociation returns the association between a VCN and its private DNS resolver.
func (b *InMemoryBackend) GetVcnDNSResolverAssociation(id string) (VcnDNSResolverAssociation, *APIError) {
	b.mu.RLock("GetVcnDnsResolverAssociation")
	v, aerr := get[*Vcn](b, id)

	var state string
	if aerr == nil {
		state = v.LifecycleState
	}

	lookup := b.dnsResolver
	b.mu.RUnlock()

	if aerr != nil {
		return VcnDNSResolverAssociation{}, aerr
	}

	out := VcnDNSResolverAssociation{VcnID: id, LifecycleState: state}

	if lookup != nil {
		if rid, ok := lookup(id); ok {
			out.DNSResolverID = rid
		}
	}

	return out, nil
}

func dnsResolverEntries() []entry {
	return []entry{
		{(*Handler).getVcnDNSResolverAssociation, http.MethodGet,
			idPath("/vcns", "vcnId") + "/dnsResolverAssociation", "GetVcnDnsResolverAssociation"},
	}
}

func (h *Handler) getVcnDNSResolverAssociation(q *req) error {
	out, aerr := h.Backend.GetVcnDNSResolverAssociation(q.param("vcnId"))
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, out, "")
}

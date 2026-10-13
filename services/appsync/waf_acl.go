package appsync

import "context"

// WebACLResolver looks up the WAFv2 web ACL associated with an AppSync resource ARN.
type WebACLResolver interface {
	// WebACLARN returns the associated web ACL's ARN, or "" when none is associated.
	WebACLARN(ctx context.Context, resourceARN string) string
}

// SetWebACLResolver wires the WAFv2 association lookup used for wafWebAclArn.
func (b *InMemoryBackend) SetWebACLResolver(r WebACLResolver) {
	b.mu.Lock("SetWebACLResolver")
	defer b.mu.Unlock()

	b.webACLs = r
}

func (h *Handler) webACLARN(ctx context.Context, resourceARN string) string {
	bk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return ""
	}

	bk.mu.RLock("webACLARN")
	r := bk.webACLs
	bk.mu.RUnlock()

	if r == nil {
		return ""
	}

	return r.WebACLARN(withRegion(ctx, bk.region), resourceARN)
}

func (h *Handler) withWebACL(ctx context.Context, api *API) *API {
	if api == nil {
		return nil
	}

	cp := *api
	cp.WafWebACLArn = h.webACLARN(ctx, api.ARN)

	return &cp
}

func (h *Handler) withWebACLs(ctx context.Context, apis []*API) []*API {
	out := make([]*API, len(apis))
	for i, api := range apis {
		out[i] = h.withWebACL(ctx, api)
	}

	return out
}

// SetWebACLResolver wires r on the home backend and every region sibling built later.
func (h *Handler) SetWebACLResolver(r WebACLResolver) {
	for _, bk := range h.RegionBackends() {
		if mem, ok := bk.(*InMemoryBackend); ok {
			mem.SetWebACLResolver(r)
		}
	}
}

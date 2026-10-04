package appsync

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// withRegion returns ctx carrying region, so cross-service calls land in the API's region.
func withRegion(ctx context.Context, region string) context.Context {
	m := awsmeta.Get(ctx)
	if m == nil {
		return awsmeta.Set(ctx, &awsmeta.Metadata{Region: region})
	}

	cp := *m
	cp.Region = region

	return awsmeta.Set(ctx, &cp)
}

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackend(home.accountID, region, home.endpoint)
		nb.lambdaFn = home.lambdaFn
		nb.ddbBackend = home.ddbBackend
		nb.jwksProvider = home.jwksProvider
		nb.sigv4Secret = home.sigv4Secret

		p := NewHandler(nb)
		p.DefaultRegion = region
		p.AccountID = h.AccountID

		if ctx := h.workerCtx.Load(); ctx != nil {
			p.startJanitor(*ctx)
		}

		return p
	})
}

// RegionHandler returns the handler serving region: h itself for the home region, else its sibling.
func (h *Handler) RegionHandler(region string) *Handler {
	if p := h.peers.Get(region); p != nil {
		return p
	}

	return h
}

// RegionBackends returns the home backend followed by every sibling built so far.
func (h *Handler) RegionBackends() []StorageBackend {
	peers := h.peers.All()
	out := make([]StorageBackend, 0, 1+len(peers))
	out = append(out, h.Backend)

	for _, p := range peers {
		out = append(out, p.Backend)
	}

	return out
}

// graphQLOwner returns the handler whose region owns the GraphQL API in path, or nil.
func (h *Handler) graphQLOwner(path string) *Handler {
	segs := splitPath(path)

	const apiIDIdx, graphqlIdx = 2, 3

	if len(segs) <= graphqlIdx || segs[1] != pathSegAPIs || segs[graphqlIdx] != pathSegGraphQL {
		return nil
	}

	if _, err := h.Backend.GetGraphqlAPI(segs[apiIDIdx]); err == nil {
		return h
	}

	for _, p := range h.peers.All() {
		if _, err := p.Backend.GetGraphqlAPI(segs[apiIDIdx]); err == nil {
			return p
		}
	}

	return nil
}

// peerFor returns the sibling that should serve the request, or nil when h does.
func (h *Handler) peerFor(path, region string) *Handler {
	if owner := h.graphQLOwner(path); owner != nil {
		if owner == h {
			return nil
		}

		return owner
	}

	return h.peers.Get(region)
}

// BackendFor returns the backend serving region: the home backend, or the sibling built on first use.
func (h *Handler) BackendFor(region string) StorageBackend {
	return h.RegionHandler(region).Backend
}

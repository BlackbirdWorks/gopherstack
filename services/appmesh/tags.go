package appmesh

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()
	if !b.arnExists(arn) {
		return ErrResourceNotFound
	}
	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}
	maps.Copy(b.tags[arn], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()
	if !b.arnExists(arn) {
		return ErrResourceNotFound
	}
	for _, k := range keys {
		delete(b.tags[arn], k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(
	arn string,
	maxResults int32,
	nextToken string,
) ([]TagRef, string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	if !b.arnExists(arn) {
		return nil, "", ErrResourceNotFound
	}
	tagMap := b.tags[arn]
	keys := collections.SortedKeys(tagMap)
	items, next := paginateStrings(keys, nextToken, maxResults)
	refs := make([]TagRef, 0, len(items))
	for _, k := range items {
		refs = append(refs, TagRef{Key: k, Value: tagMap[k]})
	}

	return refs, next, nil
}

// arnExists checks whether the given ARN belongs to any known resource.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) arnExists(arn string) bool {
	return b.arnInMeshes(arn) ||
		b.arnInVirtualNodes(arn) ||
		b.arnInVirtualRouters(arn) ||
		b.arnInRoutes(arn) ||
		b.arnInVirtualServices(arn) ||
		b.arnInVirtualGateways(arn) ||
		b.arnInGatewayRoutes(arn)
}

func (b *InMemoryBackend) arnInMeshes(arn string) bool {
	found := false
	b.meshes.Range(func(m *Mesh) bool {
		if m.Meta.Arn == arn {
			found = true

			return false
		}

		return true
	})

	return found
}

func (b *InMemoryBackend) arnInVirtualNodes(arn string) bool {
	found := false
	b.virtualNodes.Range(func(vn *VirtualNode) bool {
		if vn.Meta.Arn == arn {
			found = true

			return false
		}

		return true
	})

	return found
}

func (b *InMemoryBackend) arnInVirtualRouters(arn string) bool {
	found := false
	b.virtualRouters.Range(func(vr *VirtualRouter) bool {
		if vr.Meta.Arn == arn {
			found = true

			return false
		}

		return true
	})

	return found
}

func (b *InMemoryBackend) arnInRoutes(arn string) bool {
	found := false
	b.routes.Range(func(r *Route) bool {
		if r.Meta.Arn == arn {
			found = true

			return false
		}

		return true
	})

	return found
}

func (b *InMemoryBackend) arnInVirtualServices(arn string) bool {
	found := false
	b.virtualSvcs.Range(func(vs *VirtualService) bool {
		if vs.Meta.Arn == arn {
			found = true

			return false
		}

		return true
	})

	return found
}

func (b *InMemoryBackend) arnInVirtualGateways(arn string) bool {
	found := false
	b.virtualGWs.Range(func(vg *VirtualGateway) bool {
		if vg.Meta.Arn == arn {
			found = true

			return false
		}

		return true
	})

	return found
}

func (b *InMemoryBackend) arnInGatewayRoutes(arn string) bool {
	found := false
	b.gatewayRoutes.Range(func(gr *GatewayRoute) bool {
		if gr.Meta.Arn == arn {
			found = true

			return false
		}

		return true
	})

	return found
}

package appmesh

import (
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// maxTagsPerResource is the real App Mesh API's per-resource tag limit (see
// the botocore service-2.json TagList shape: {"max": 50}). TagResource
// returns TooManyTagsException rather than silently applying tags beyond it.
const maxTagsPerResource = 50

func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()
	if !b.arnExists(arn) {
		return ErrResourceNotFound
	}
	merged := len(mergedTagCount(b.tags[arn], tags))
	if merged > maxTagsPerResource {
		return fmt.Errorf("%w: resource would have %d tags (max %d)", ErrTooManyTags, merged, maxTagsPerResource)
	}
	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}
	maps.Copy(b.tags[arn], tags)

	return nil
}

// mergedTagCount returns the tag set existing would have after merging in
// incoming, without mutating either map.
func mergedTagCount(existing, incoming map[string]string) map[string]string {
	merged := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(merged, existing)
	maps.Copy(merged, incoming)

	return merged
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

package mediastoredata

import (
	"context"
	"sort"
	"strings"
)

// defaultMaxResults is the page size applied when MaxResults is zero.
const defaultMaxResults = 1000

// ListItems returns items at the given folder path with optional pagination.
func (b *InMemoryBackend) ListItems(ctx context.Context, in ListItemsInput) *ListItemsOutput {
	b.mu.RLock("ListItems")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	tbl := b.stateRO(region)

	prefix := normalizePath(in.FolderPath)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var objects []*Object
	if tbl != nil {
		objects = tbl.All()
	}

	seen := make(map[string]bool)
	all := make([]*Item, 0, len(objects))

	// [store.Table.All] returns objects in unspecified order, matching the
	// old map[string]*Object iteration order this code always relied on --
	// the explicit sort.Slice by Name below (unchanged from before this
	// conversion) is what makes ListItems output order deterministic, not
	// the underlying map/table order. See store_setup.go.
	for _, obj := range objects {
		key := obj.Path
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		rest := strings.TrimPrefix(key, prefix)
		before, _, isNested := strings.Cut(rest, "/")

		if !isNested {
			if !seen[rest] {
				// Direct object.
				seen[rest] = true
				all = append(all, &Item{
					Name:          rest,
					Type:          "OBJECT",
					ETag:          obj.ETag,
					SHA256:        obj.SHA256,
					ContentType:   obj.ContentType,
					CacheControl:  obj.CacheControl,
					StorageClass:  obj.StorageClass,
					ContentLength: obj.ContentLength,
					LastModified:  obj.LastModified,
				})
			}
		} else if !seen[before] {
			// Folder – deduplicate.
			seen[before] = true
			all = append(all, &Item{
				Name: before,
				Type: "FOLDER",
			})
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	// Apply cursor.
	if in.NextToken != "" {
		cut := 0
		for cut < len(all) && all[cut].Name <= in.NextToken {
			cut++
		}
		all = all[cut:]
	}

	// Apply page limit.
	limit := in.MaxResults
	if limit <= 0 {
		limit = defaultMaxResults
	}

	out := &ListItemsOutput{}

	if len(all) > limit {
		out.Items = all[:limit]
		out.NextToken = all[limit-1].Name
	} else {
		out.Items = all
	}

	return out
}

package athena

import (
	"fmt"
	"maps"
	"sort"
	"strings"
)

// defaultListTagsForResourceMaxResults mirrors the default page size used by
// the other List* ops in this service (WorkGroups/DataCatalogs/PreparedStatements)
// when the caller does not supply MaxResults.
const defaultListTagsForResourceMaxResults = 50

// arnFieldCount is the number of colon-separated fields in a well-formed AWS
// ARN: "arn:partition:service:region:account:resource".
const arnFieldCount = 6

// resourceKindFromARN splits the resource portion of an Athena ARN
// ("arn:partition:athena:region:account:kind/id") into its kind ("workgroup",
// "datacatalog", "capacity-reservation", "notebook", ...) and identifier. Its
// third result reports false if resourceARN is not a well-formed Athena
// resource ARN.
func resourceKindFromARN(resourceARN string) (string, string, bool) {
	parts := strings.SplitN(resourceARN, ":", arnFieldCount)
	if len(parts) != arnFieldCount || parts[0] != "arn" {
		return "", "", false
	}

	return strings.Cut(parts[arnFieldCount-1], "/")
}

// resourceExistsForARN reports whether resourceARN refers to a currently
// existing taggable Athena resource. AWS documents TagResource/UntagResource/
// ListTagsForResource as applying to "Athena resources, such as workgroups,
// data catalogs, or capacity reservations" and returns InvalidRequestException
// for an ARN that does not resolve to a real resource; gopherstack previously
// let TagResource/UntagResource silently no-op on an unknown ARN and
// ListTagsForResource silently return an empty tag list, both of which mask
// the caller's mistake instead of surfacing it the way real Athena does.
//
// Callers must already hold b.mu (Lock or RLock) -- see pkgs/store's package
// doc: Table performs no locking of its own.
func (b *InMemoryBackend) resourceExistsForARN(resourceARN string) bool {
	kind, id, ok := resourceKindFromARN(resourceARN)
	if !ok {
		return false
	}

	switch kind {
	case "workgroup":
		return b.workGroups.Has(id)
	case "datacatalog":
		return b.dataCatalogs.Has(id)
	case "capacity-reservation":
		return b.capacityReservations.Has(id)
	case "notebook":
		return b.notebooks.Has(id)
	case "session":
		return b.sessions.Has(id)
	default:
		return false
	}
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingAthena).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every workgroup, data catalog, capacity reservation and session plus every tagged
// ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))
	seen := make(map[string]struct{}, len(b.resourceTags))

	for key, t := range b.resourceTags {
		if len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: maps.Clone(t)})
	}

	if withUntagged {
		for _, key := range b.resourceARNsLocked() {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				out = append(out, TaggedEntry{ARN: key})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	out := make([]string, 0, b.workGroups.Len()+b.dataCatalogs.Len()+b.capacityReservations.Len()+b.sessions.Len())

	for _, w := range b.workGroups.All() {
		out = append(out, b.workGroupARN(w.Name))
	}

	for _, c := range b.dataCatalogs.All() {
		out = append(out, b.dataCatalogARN(c.Name))
	}

	for _, c := range b.capacityReservations.All() {
		out = append(out, b.capacityReservationARN(c.Name))
	}

	for _, s := range b.sessions.All() {
		out = append(out, b.sessionARN(s.SessionID))
	}

	return out
}

// TagResource adds tags to a resource identified by ARN. AWS returns
// InvalidRequestException when the ARN does not resolve to an existing
// taggable resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExistsForARN(resourceARN) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	if _, ok := b.resourceTags[resourceARN]; !ok {
		b.resourceTags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN. AWS returns
// InvalidRequestException when the ARN does not resolve to an existing
// taggable resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExistsForARN(resourceARN) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	existing := b.resourceTags[resourceARN]
	for _, k := range keys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns a page of tags for a resource identified by
// ARN, honoring AWS's MaxResults/NextToken pagination inputs. AWS returns
// InvalidRequestException when the ARN does not resolve to an existing
// taggable resource.
func (b *InMemoryBackend) ListTagsForResource(
	resourceARN, nextToken string,
	maxResults int,
) ([]Tag, string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.resourceExistsForARN(resourceARN) {
		return nil, "", fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	existing := b.resourceTags[resourceARN]
	all := make([]Tag, 0, len(existing))

	for k, v := range existing {
		all = append(all, Tag{Key: k, Value: v})
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Key < all[j].Key
	})

	limit := defaultListTagsForResourceMaxResults
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	start := paginationStart(len(all), nextToken, func(i int) string { return all[i].Key })
	all = all[start:]

	outToken := ""
	if len(all) > limit {
		outToken = all[limit].Key
		all = all[:limit]
	}

	return all, outToken, nil
}

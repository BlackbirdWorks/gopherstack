package redshift

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	tagTypeCluster           = keyResourceCluster
	tagTypeParameterGroup    = "parametergroup"
	tagTypeSnapshot          = "snapshot"
	tagTypeEventSubscription = "eventsubscription"
	tagTypeIdcApplication    = "redshiftidcapplication"
)

// TaggedResource is one taggable resource and its tags, keyed by ARN.
type TaggedResource struct {
	Tags         map[string]string
	ResourceName string
	ResourceType string
}

// parseTagTarget maps a CreateTags/DeleteTags ResourceName (an ARN, or a bare
// cluster identifier) to its resource type and backend key.
func parseTagTarget(name string) (string, string) {
	if !strings.HasPrefix(name, "arn:") {
		return tagTypeCluster, name
	}

	const arnFields = 6

	parts := strings.SplitN(name, ":", arnFields)
	if len(parts) < arnFields {
		return tagTypeCluster, name
	}

	res := parts[arnFields-1]

	if rest, ok := strings.CutPrefix(res, tagTypeIdcApplication+"/"); ok {
		return tagTypeIdcApplication, rest
	}

	kind, rest, ok := strings.Cut(res, ":")
	if !ok {
		return tagTypeCluster, name
	}

	if kind == tagTypeSnapshot {
		if _, snap, found := strings.Cut(rest, "/"); found {
			rest = snap
		}
	}

	return kind, rest
}

// tagMapLocked returns the live tag map of a resource, allocating it on first use.
func (b *InMemoryBackend) tagMapLocked(resourceType, id string) (map[string]string, error) {
	var tagsPtr *map[string]string

	switch resourceType {
	case tagTypeParameterGroup:
		if pg, ok := b.parameterGroups.Get(id); ok {
			tagsPtr = &pg.Tags
		} else {
			return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, id)
		}
	case tagTypeSnapshot:
		if snap, ok := b.snapshots.Get(id); ok {
			tagsPtr = &snap.Tags
		} else {
			return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, id)
		}
	case tagTypeEventSubscription:
		if sub, ok := b.eventSubscriptions.Get(id); ok {
			tagsPtr = &sub.Tags
		} else {
			return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, id)
		}
	case tagTypeIdcApplication:
		app := b.idcApplicationByNameLocked(id)
		if app == nil {
			return nil, fmt.Errorf("%w: application %s not found", ErrIdcApplicationNotFound, id)
		}

		tagsPtr = &app.Tags
	default:
		return nil, fmt.Errorf("%w: resource type %q is not taggable", ErrInvalidParameter, resourceType)
	}

	if *tagsPtr == nil {
		*tagsPtr = map[string]string{}
	}

	return *tagsPtr, nil
}

func (b *InMemoryBackend) idcApplicationByNameLocked(name string) *IdcApplication {
	for _, app := range b.idcApplications.All() {
		if app.IdcApplicationName == name {
			return app
		}
	}

	return nil
}

// DescribeTags returns the tags of every cluster, keyed by cluster identifier.
func (b *InMemoryBackend) DescribeTags() map[string]map[string]string {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	all := b.clusters.All()
	result := make(map[string]map[string]string, len(all))

	for _, c := range all {
		result[c.ClusterIdentifier] = c.Tags.Clone()
	}

	return result
}

// DescribeAllTags returns every taggable resource that carries tags, sorted by ARN.
func (b *InMemoryBackend) DescribeAllTags() []TaggedResource {
	b.mu.RLock("DescribeAllTags")
	defer b.mu.RUnlock()

	var out []TaggedResource

	add := func(resourceArn, resourceType string, kv map[string]string) {
		if len(kv) > 0 {
			out = append(
				out,
				TaggedResource{ResourceName: resourceArn, ResourceType: resourceType, Tags: maps.Clone(kv)},
			)
		}
	}

	for _, c := range b.clusters.All() {
		add(b.arnLocked(tagTypeCluster, c.ClusterIdentifier), tagTypeCluster, c.Tags.Clone())
	}

	for _, pg := range b.parameterGroups.All() {
		add(b.arnLocked(tagTypeParameterGroup, pg.ParameterGroupName), tagTypeParameterGroup, pg.Tags)
	}

	for _, s := range b.snapshots.All() {
		add(s.SnapshotArn, tagTypeSnapshot, s.Tags)
	}

	for _, sub := range b.eventSubscriptions.All() {
		add(b.arnLocked(tagTypeEventSubscription, sub.CustSubscriptionID), tagTypeEventSubscription, sub.Tags)
	}

	for _, app := range b.idcApplications.All() {
		add(app.IdcApplicationArn, tagTypeIdcApplication, app.Tags)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ResourceName < out[j].ResourceName })

	return out
}

func (b *InMemoryBackend) arnLocked(resourceType, id string) string {
	return arn.Build("redshift", b.region, b.accountID, resourceType+":"+id)
}

// TagNewResource applies creation-time tags to a just-created resource of the given type.
func (b *InMemoryBackend) TagNewResource(resourceType, id string, kv map[string]string) error {
	if len(kv) == 0 {
		return nil
	}

	return b.CreateTags(b.arnLocked(resourceType, id), kv)
}

// CreateTags adds or updates tags on the resource named by an ARN or bare cluster identifier.
func (b *InMemoryBackend) CreateTags(resourceName string, kv map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	resourceType, id := parseTagTarget(resourceName)

	if resourceType == tagTypeCluster {
		c, exists := b.clusters.Get(id)
		if !exists {
			return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		c.Tags.Merge(kv)

		return nil
	}

	m, err := b.tagMapLocked(resourceType, id)
	if err != nil {
		return err
	}

	maps.Copy(m, kv)

	return nil
}

// DeleteTags removes tag keys from the resource named by an ARN or bare cluster identifier.
func (b *InMemoryBackend) DeleteTags(resourceName string, keys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	resourceType, id := parseTagTarget(resourceName)

	if resourceType == tagTypeCluster {
		c, exists := b.clusters.Get(id)
		if !exists {
			return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		c.Tags.DeleteKeys(keys)

		return nil
	}

	m, err := b.tagMapLocked(resourceType, id)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(m, k)
	}

	return nil
}

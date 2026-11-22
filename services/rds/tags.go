package rds

// AddTagsToResource adds or overwrites tags on the resource identified by arn.
func (b *InMemoryBackend) AddTagsToResource(arn string, tags []Tag) {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	current := b.tags[arn]
	// Build an index for O(1) key lookup.
	idx := make(map[string]int, len(current))
	for i, t := range current {
		idx[t.Key] = i
	}

	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			current[i].Value = t.Value
		} else {
			idx[t.Key] = len(current)
			current = append(current, t)
		}
	}

	b.tags[arn] = current
}

// RemoveTagsFromResource removes the named tags from the resource identified by arn.
func (b *InMemoryBackend) RemoveTagsFromResource(arn string, keys []string) {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}

	current := b.tags[arn]
	kept := make([]Tag, 0, len(current))

	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}

	b.tags[arn] = kept
}

// ListTagsForResource returns the tags for the resource identified by arn.
func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	src := b.tags[arn]
	cp := make([]Tag, len(src))
	copy(cp, src)

	return cp
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingRDS). RDS keeps tags for every taggable resource kind (DB
// instances, clusters, snapshots, parameter groups, and more) in one flat
// ARN-keyed map rather than inline per typed resource, so this is a direct
// walk of that map instead of one per-kind loop.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every instance, cluster, snapshot, subnet group, parameter group, option group,
// event subscription, global cluster, security group, proxy, endpoint and integration plus every tagged ARN of
// the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))
	seen := make(map[string]struct{}, len(b.tags))

	for key, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: tagListToMap(t)})
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
	out := b.resourceARNsPart1Locked()
	out = append(out, b.resourceARNsPart2Locked()...)

	return out
}

func (b *InMemoryBackend) resourceARNsPart1Locked() []string {
	out := make(
		[]string,
		0,
		b.instances.Len()+b.clusters.Len()+b.snapshots.Len()+b.clusterSnapshots.Len()+b.subnetGroups.Len()+
			b.parameterGroups.Len()+b.clusterParameterGroups.Len()+b.optionGroups.Len(),
	)

	for _, v := range b.instances.All() {
		out = append(out, v.DBInstanceArn)
	}

	for _, v := range b.clusters.All() {
		out = append(out, v.DBClusterArn)
	}

	for _, v := range b.snapshots.All() {
		out = append(out, v.DBSnapshotArn)
	}

	for _, v := range b.clusterSnapshots.All() {
		out = append(out, v.DBClusterSnapshotArn)
	}

	for _, v := range b.subnetGroups.All() {
		out = append(out, v.DBSubnetGroupArn)
	}

	for _, v := range b.parameterGroups.All() {
		out = append(out, v.DBParameterGroupArn)
	}

	for _, v := range b.clusterParameterGroups.All() {
		out = append(out, v.DBParameterGroupArn)
	}

	for _, v := range b.optionGroups.All() {
		out = append(out, v.OptionGroupArn)
	}

	return out
}

func (b *InMemoryBackend) resourceARNsPart2Locked() []string {
	out := make(
		[]string,
		0,
		b.eventSubscriptions.Len()+b.globalClusters.Len()+b.dbSecurityGroups.Len()+b.proxies.Len()+
			b.proxyEndpoints.Len()+b.clusterEndpoints.Len()+b.integrations.Len(),
	)

	for _, v := range b.eventSubscriptions.All() {
		out = append(out, v.EventSubscriptionArn)
	}

	for _, v := range b.globalClusters.All() {
		out = append(out, v.GlobalClusterArn)
	}

	for _, v := range b.dbSecurityGroups.All() {
		out = append(out, v.DBSecurityGroupArn)
	}

	for _, v := range b.proxies.All() {
		out = append(out, v.DBProxyARN)
	}

	for _, v := range b.proxyEndpoints.All() {
		out = append(out, v.DBProxyEndpointARN)
	}

	for _, v := range b.clusterEndpoints.All() {
		out = append(out, v.DBClusterEndpointArn)
	}

	for _, v := range b.integrations.All() {
		out = append(out, v.IntegrationArn)
	}

	return out
}

func tagListToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

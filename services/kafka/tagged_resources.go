package kafka

import "maps"

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every tagged cluster, configuration, replicator, VPC connection and channel in region.
func (b *InMemoryBackend) TaggedResources(region string) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	add := func(arnVal string, tags map[string]string) {
		if len(tags) > 0 && regionFromARN(arnVal, region) == region {
			out = append(out, TaggedEntry{ARN: arnVal, Tags: maps.Clone(tags)})
		}
	}

	for _, x := range b.clusters.All() {
		add(x.ClusterArn, x.Tags)
	}

	for _, x := range b.configurations.All() {
		add(x.Arn, x.Tags)
	}

	for _, x := range b.replicators.All() {
		add(x.ReplicatorArn, x.Tags)
	}

	for _, x := range b.vpcConnections.All() {
		add(x.VpcConnectionArn, x.Tags)
	}

	for _, x := range b.channels.All() {
		add(x.ChannelArn, x.Tags)
	}

	return out
}

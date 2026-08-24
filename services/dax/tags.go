package dax

import (
	"fmt"
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// listTagsPageSize is the number of tags returned per ListTags page.
const listTagsPageSize = 10

// TagResource adds tags to a DAX resource and returns the complete tag set.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) (map[string]string, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrInvalidARN)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceArn) {
		// TagResource's own awsAwsjson11_deserializeOpErrorTagResource switch
		// (aws-sdk-go-v2/service/dax@v1.32.4 deserializers.go) has no
		// TagNotFoundFault case -- only UntagResource types that fault.
		// arnExists only ever resolves cluster ARNs (see arnExists below), so
		// ClusterNotFoundFault, which TagResource's switch does type, is both
		// the wire-correct and semantically correct code here.
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, resourceArn)
	}

	existing := b.tags[resourceArn]
	merged := len(existing)

	for k := range tags {
		if _, alreadyExists := existing[k]; !alreadyExists {
			merged++
		}
	}

	if merged > maxTagsPerResource {
		return nil, fmt.Errorf("%w: resource would have %d tags (max %d)",
			ErrTagQuotaExceeded, merged, maxTagsPerResource)
	}

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	// Propagate to cluster.Tags if this is a cluster ARN.
	if cluster := b.clusterByARN(resourceArn); cluster != nil {
		maps.Copy(cluster.Tags, tags)
	}

	result := make(map[string]string, len(b.tags[resourceArn]))
	maps.Copy(result, b.tags[resourceArn])

	return result, nil
}

// UntagResource removes tags from a DAX resource and returns the remaining tags.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) (map[string]string, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrInvalidARN)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceArn) {
		return nil, fmt.Errorf("%w: %s", ErrTagNotFound, resourceArn)
	}

	if b.tags[resourceArn] != nil {
		for _, k := range tagKeys {
			delete(b.tags[resourceArn], k)
		}
	}

	if cluster := b.clusterByARN(resourceArn); cluster != nil {
		for _, k := range tagKeys {
			delete(cluster.Tags, k)
		}
	}

	result := make(map[string]string)
	if b.tags[resourceArn] != nil {
		maps.Copy(result, b.tags[resourceArn])
	}

	return result, nil
}

// ListTags returns tags for a DAX resource with optional pagination.
func (b *InMemoryBackend) ListTags(
	resourceArn string,
	nextToken string,
) (map[string]string, string, error) {
	if resourceArn == "" {
		return nil, "", fmt.Errorf("%w: ResourceName is required", ErrInvalidARN)
	}

	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	if !b.arnExists(resourceArn) {
		// ListTags's own awsAwsjson11_deserializeOpErrorListTags switch has no
		// TagNotFoundFault case either (see the identical note in TagResource
		// above).
		return nil, "", fmt.Errorf("%w: %s", ErrClusterNotFound, resourceArn)
	}

	allTags := b.tags[resourceArn]

	keys := collections.SortedKeys(allTags)

	startIdx := 0

	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				startIdx = i

				break
			}
		}
	}

	end := min(startIdx+listTagsPageSize, len(keys))

	page := keys[startIdx:end]
	result := make(map[string]string, len(page))

	for _, k := range page {
		result[k] = allTags[k]
	}

	var outToken string
	if end < len(keys) {
		outToken = keys[end]
	}

	return result, outToken, nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingDAX).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every DAX cluster ARN that currently has at least
// one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for arnStr, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: arnStr, Tags: maps.Clone(t)})
	}

	return out
}

// arnExists returns true if the ARN corresponds to an existing DAX resource.
// Real DAX only assigns ARNs to clusters -- ParameterGroup and SubnetGroup have
// no Arn field in the SDK types (types.ParameterGroup / types.SubnetGroup), so
// TagResource/UntagResource/ListTags only ever operate on cluster ARNs.
// Must be called with b.mu held.
func (b *InMemoryBackend) arnExists(arnStr string) bool {
	clusterPrefix := arn.Build("dax", b.Region, b.AccountID, "cache/")
	if name, ok := strings.CutPrefix(arnStr, clusterPrefix); ok {
		return b.clusters.Has(name)
	}

	return false
}

// clusterByARN returns the cluster matching the given ARN, or nil.
// Must be called with b.mu held.
func (b *InMemoryBackend) clusterByARN(arnStr string) *Cluster {
	prefix := arn.Build("dax", b.Region, b.AccountID, "cache/")
	if name, ok := strings.CutPrefix(arnStr, prefix); ok {
		c, _ := b.clusters.Get(name)

		return c
	}

	return nil
}

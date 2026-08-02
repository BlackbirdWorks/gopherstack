package networkmanager

import (
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file implements PARITY.md family X (the tagging trio, 3 ops) plus
// TaggedResources/TagResource/UntagResource, the cross-service entry points
// cli.go's wireTaggingNetworkManager (Resource Groups Tagging API) calls
// into -- following services/mgn/directconnect's identical
// wireTaggingCtxARNResources pattern for a multi-kind ARN dispatch.
//
// All 9 taggable NetworkManager resource kinds (GlobalNetwork/Site/Device/
// Link/Connection/ConnectPeer/CoreNetwork/Peering/Attachment) share the one
// generic /tags/{ResourceArn} path and are dispatched here by the
// resource-kind segment of the ARN (arn:{partition}:networkmanager::
// {account}:{kind}/{...} -- confirmed global, no region segment, PARITY.md).

// resourceKindFromARN extracts the resource-kind segment (e.g. "site",
// "device") from a NetworkManager ARN's resource part.
func resourceKindFromARN(resourceArn string) string {
	idx := strings.LastIndex(resourceArn, ":")
	if idx < 0 {
		return ""
	}

	resourcePart := resourceArn[idx+1:]

	kind, _, _ := strings.Cut(resourcePart, "/")

	return kind
}

// lastARNSegment returns the final "/"-separated segment of resourceArn --
// the resource's own ID, regardless of whether the kind nests under a
// GlobalNetworkId (site/device/link/connection) or not.
func lastARNSegment(resourceArn string) string {
	idx := strings.LastIndex(resourceArn, "/")
	if idx < 0 {
		return resourceArn
	}

	return resourceArn[idx+1:]
}

// tagResolver looks up one taggable resource kind's *tags.Tags by ID.
type tagResolver func(b *InMemoryBackend, id string) *tags.Tags

// tagResolvers maps each of the 9 taggable ARN resource-kind segments to the
// resolver that reaches its table -- a lookup table rather than a long
// switch, so tagsFor stays a flat dispatch instead of a high-complexity
// branch chain. A function (not a package-level var) so no caller can
// accidentally mutate shared state (gochecknoglobals) -- matches
// services/mgn's identical dataReplicationInitiationStepNames() idiom.
func tagResolvers() map[string]tagResolver {
	return map[string]tagResolver{
		"global-network": func(b *InMemoryBackend, id string) *tags.Tags {
			v, ok := b.globalNetworks.Get(id)
			if !ok {
				return nil
			}

			return v.Tags
		},
		"site": func(b *InMemoryBackend, id string) *tags.Tags {
			v, ok := b.sites.Get(id)
			if !ok {
				return nil
			}

			return v.Tags
		},
		"device": func(b *InMemoryBackend, id string) *tags.Tags {
			v, ok := b.devices.Get(id)
			if !ok {
				return nil
			}

			return v.Tags
		},
		"link": func(b *InMemoryBackend, id string) *tags.Tags {
			v, ok := b.links.Get(id)
			if !ok {
				return nil
			}

			return v.Tags
		},
		resourceTypeConnection: func(b *InMemoryBackend, id string) *tags.Tags {
			v, ok := b.connections.Get(id)
			if !ok {
				return nil
			}

			return v.Tags
		},
		resourceTypeConnectPeer: func(b *InMemoryBackend, id string) *tags.Tags {
			v, ok := b.connectPeers.Get(id)
			if !ok {
				return nil
			}

			return v.Tags
		},
		resourceTypeCoreNetwork: func(b *InMemoryBackend, id string) *tags.Tags {
			v, ok := b.coreNetworks.Get(id)
			if !ok {
				return nil
			}

			return v.Tags
		},
		"peering": func(b *InMemoryBackend, id string) *tags.Tags {
			v, ok := b.peerings.Get(id)
			if !ok {
				return nil
			}

			return v.Tags
		},
		resourceTypeAttachment: func(b *InMemoryBackend, id string) *tags.Tags {
			v, ok := b.attachments.Get(id)
			if !ok {
				return nil
			}

			return v.Tags
		},
	}
}

// tagsFor resolves resourceArn to its owning resource's *tags.Tags, or nil
// if the ARN does not name a known, currently-existing resource of one of
// the 9 taggable kinds.
func (b *InMemoryBackend) tagsFor(resourceArn string) *tags.Tags {
	resolver, ok := tagResolvers()[resourceKindFromARN(resourceArn)]
	if !ok {
		return nil
	}

	return resolver(b, lastARNSegment(resourceArn))
}

// TagResource adds/updates tags on resourceArn.
func (b *InMemoryBackend) TagResource(resourceArn string, tagMap map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t := b.tagsFor(resourceArn)
	if t == nil {
		return notFoundError(resourceTaggable, resourceArn)
	}

	t.Merge(tagMap)

	return nil
}

// UntagResource removes the given tag keys from resourceArn.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t := b.tagsFor(resourceArn)
	if t == nil {
		return notFoundError(resourceTaggable, resourceArn)
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns the tags currently on resourceArn.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.tagsFor(resourceArn)
	if t == nil {
		return nil, notFoundError(resourceTaggable, resourceArn)
	}

	return t.Clone(), nil
}

// TaggedResource is one entry in TaggedResources' cross-service tagging
// rollup.
type TaggedResource struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources enumerates every tagged resource across all 9 taggable
// kinds, for cli.go's ResourceGroupsTaggingAPI wiring
// (wireTaggingNetworkManager).
func (b *InMemoryBackend) TaggedResources() []TaggedResource {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedResource

	appendIfTagged := func(arn string, t *tags.Tags) {
		if t == nil || t.Len() == 0 {
			return
		}

		out = append(out, TaggedResource{ARN: arn, Tags: t.Clone()})
	}

	for _, v := range b.globalNetworks.Snapshot() {
		appendIfTagged(v.GlobalNetworkArn, v.Tags)
	}

	for _, v := range b.sites.Snapshot() {
		appendIfTagged(v.SiteArn, v.Tags)
	}

	for _, v := range b.devices.Snapshot() {
		appendIfTagged(v.DeviceArn, v.Tags)
	}

	for _, v := range b.links.Snapshot() {
		appendIfTagged(v.LinkArn, v.Tags)
	}

	for _, v := range b.connections.Snapshot() {
		appendIfTagged(v.ConnectionArn, v.Tags)
	}

	for _, v := range b.connectPeers.Snapshot() {
		appendIfTagged(b.connectPeerARN(v.ConnectPeerID), v.Tags)
	}

	for _, v := range b.coreNetworks.Snapshot() {
		appendIfTagged(v.CoreNetworkArn, v.Tags)
	}

	for _, v := range b.peerings.Snapshot() {
		appendIfTagged(b.peeringARN(v.PeeringID), v.Tags)
	}

	for _, v := range b.attachments.Snapshot() {
		appendIfTagged(b.attachmentARN(v.AttachmentID), v.Tags)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ARN < out[j].ARN })

	return out
}

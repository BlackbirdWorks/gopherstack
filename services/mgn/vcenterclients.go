package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// No CreateVcenterClient op exists anywhere in this SDK surface: a VcenterClient
// record is created only by the on-prem vCenter connector appliance registering
// itself with AWS, not exposed by any public API. SeedVcenterClient below is an
// EXPORTED, non-SDK, gopherstack-only convenience, never routed as an SDK operation.

// SeedVcenterClientOptions configures SeedVcenterClient. Every field is
// optional.
type SeedVcenterClientOptions struct {
	Tags             map[string]string
	SourceServerTags map[string]string
	VcenterClientID  string
	DatacenterName   string
	Hostname         string
	VcenterUUID      string
}

// SeedVcenterClient creates a new VcenterClient directly in this backend --
// see this file's package-level doc comment for why this exported, non-SDK
// helper exists.
func (b *InMemoryBackend) SeedVcenterClient(opts SeedVcenterClientOptions) *VcenterClient {
	b.mu.Lock("SeedVcenterClient")
	defer b.mu.Unlock()

	id := opts.VcenterClientID
	if id == "" {
		id = newVcenterClientID()
	}

	t := tags.New("mgn.vcenterclient." + id + ".tags")
	t.Merge(opts.Tags)

	v := &VcenterClient{
		VcenterClientID:  id,
		Arn:              b.vcenterClientARN(id),
		DatacenterName:   opts.DatacenterName,
		Hostname:         opts.Hostname,
		VcenterUUID:      opts.VcenterUUID,
		LastSeenDatetime: nowRFC3339(),
		Tags:             t,
		SourceServerTags: cloneStrMap(opts.SourceServerTags),
	}
	b.vcenterClients.Put(v)

	return v.clone()
}

// DescribeVcenterClients returns a page of VcenterClients.
func (b *InMemoryBackend) DescribeVcenterClients(token string, limit int) (page.Page[*VcenterClient], error) {
	b.mu.RLock("DescribeVcenterClients")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*VcenterClient]{}, err
	}

	all := b.vcenterClients.Snapshot()
	cloned := make([]*VcenterClient, len(all))

	for i, v := range all {
		cloned[i] = v.clone()
	}

	return page.New(cloned, token, limit, defaultPageLimit), nil
}

// DeleteVcenterClient deletes a VcenterClient.
func (b *InMemoryBackend) DeleteVcenterClient(id string) error {
	b.mu.Lock("DeleteVcenterClient")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	v, ok := b.vcenterClients.Get(id)
	if !ok {
		return notFoundError(resourceVcenterClient, id)
	}

	if v.Tags != nil {
		v.Tags.Close()
	}

	b.vcenterClients.Delete(id)

	return nil
}

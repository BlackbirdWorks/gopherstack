package medialive

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Network operations ---

// CreateNetwork creates a new Network.
func (b *InMemoryBackend) CreateNetwork(
	name string,
	ipPools []IPPool,
	routes []Route,
	tags map[string]string,
) (*Network, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	id := newID()
	pools := make([]IPPool, len(ipPools))
	copy(pools, ipPools)
	rs := make([]Route, len(routes))
	copy(rs, routes)

	n := &storedNetwork{
		Tags:                 copyTags(tags),
		ARN:                  b.networkARN(id),
		ID:                   id,
		Name:                 name,
		State:                networkStateActive,
		AssociatedClusterIDs: []string{},
		IPPools:              pools,
		Routes:               rs,
	}

	b.mu.Lock("CreateNetwork")
	defer b.mu.Unlock()

	b.networks.Put(n)

	return n.toNetwork(), nil
}

// DescribeNetwork returns a Network by ID.
func (b *InMemoryBackend) DescribeNetwork(networkID string) (*Network, error) {
	b.mu.RLock("DescribeNetwork")
	defer b.mu.RUnlock()

	n, ok := b.networks.Get(networkID)
	if !ok {
		return nil, fmt.Errorf("%w: network %s not found", ErrNotFound, networkID)
	}

	return n.toNetwork(), nil
}

// UpdateNetwork updates a Network's mutable fields.
func (b *InMemoryBackend) UpdateNetwork(
	networkID, name string,
	ipPools []IPPool,
	routes []Route,
) (*Network, error) {
	b.mu.Lock("UpdateNetwork")
	defer b.mu.Unlock()

	n, ok := b.networks.Get(networkID)
	if !ok {
		return nil, fmt.Errorf("%w: network %s not found", ErrNotFound, networkID)
	}

	if name != "" {
		n.Name = name
	}

	if ipPools != nil {
		pools := make([]IPPool, len(ipPools))
		copy(pools, ipPools)
		n.IPPools = pools
	}

	if routes != nil {
		rs := make([]Route, len(routes))
		copy(rs, routes)
		n.Routes = rs
	}

	return n.toNetwork(), nil
}

// DeleteNetwork deletes a Network.
func (b *InMemoryBackend) DeleteNetwork(networkID string) (*Network, error) {
	b.mu.Lock("DeleteNetwork")
	defer b.mu.Unlock()

	n, ok := b.networks.Get(networkID)
	if !ok {
		return nil, fmt.Errorf("%w: network %s not found", ErrNotFound, networkID)
	}

	n.State = networkStateDeleting
	out := n.toNetwork()
	b.networks.Delete(networkID)
	delete(b.tags, n.ARN)

	return out, nil
}

// ListNetworks returns a paginated list of Networks.
func (b *InMemoryBackend) ListNetworks(
	maxResults int,
	nextToken string,
) ([]*Network, string, error) {
	b.mu.RLock("ListNetworks")
	defer b.mu.RUnlock()

	all := b.networks.All()

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	out := make([]*Network, 0, len(pg.Data))
	for _, n := range pg.Data {
		out = append(out, n.toNetwork())
	}

	return out, pg.Next, nil
}

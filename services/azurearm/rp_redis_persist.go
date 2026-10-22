package azurearm

import "maps"

// redisSnapshot is the Microsoft.Cache provider's persisted ARM-side state; caches, keys and data are
// persisted by the Redis data plane.
type redisSnapshot struct {
	Caches map[string]*redisRecord `json:"caches,omitempty"`
}

func (p *RedisProvider) snapshot() *redisSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &redisSnapshot{Caches: make(map[string]*redisRecord, len(p.res))}
	for k, r := range p.res {
		c := *r
		c.Firewall, c.Linked = maps.Clone(r.Firewall), maps.Clone(r.Linked)
		snap.Caches[k] = &c
	}

	return snap
}

// restore replaces the provider's state; nil clears it. The data plane is not touched.
func (p *RedisProvider) restore(snap *redisSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.res = map[string]*redisRecord{}

	if snap == nil {
		return
	}

	for k, r := range snap.Caches {
		if r != nil {
			p.res[k] = r
		}
	}
}

func (h *Handler) redisProvider() (*RedisProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftCache)
	if !ok {
		return nil, false
	}

	rp, ok := p.(*RedisProvider)

	return rp, ok
}

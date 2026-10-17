package memorydb

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

// Engine mode constants. Only embedded starts a server; stub and docker keep
// synthetic endpoints, matching ElastiCache where docker is a pass-through.
const (
	EngineEmbedded = "embedded"
	EngineDocker   = "docker"
	EngineStub     = "stub"
)

const engineLabel = "memorydb"

// SetEngine selects the data-plane mode and the optional port allocator. It
// does not affect clusters that already exist.
func (b *InMemoryBackend) SetEngine(mode string, alloc *portalloc.Allocator) {
	b.mu.Lock("SetEngine")
	defer b.mu.Unlock()

	b.engineMode = mode
	b.allocator = alloc
}

// startEngineLocked starts the cluster's Redis server when embedded mode is on. Must hold b.mu.
func (b *InMemoryBackend) startEngineLocked(c *Cluster) error {
	if b.engineMode != EngineEmbedded || c.engine != nil {
		return nil
	}

	eng, err := redisengine.Start(b.allocator, engineLabel)
	if err != nil {
		return fmt.Errorf("start data plane for cluster %q: %w", c.Name, err)
	}

	c.engine = eng
	b.syncAuthLocked(c)

	return nil
}

// stopEngine closes the cluster's server and releases its port.
func stopEngine(c *Cluster) {
	c.engine.Close()
	c.engine = nil
	c.authUsers = nil
}

// startAllEnginesLocked (re)starts servers for every cluster, used after Restore. Must hold b.mu.
func (b *InMemoryBackend) startAllEnginesLocked() {
	for _, t := range b.clusters {
		for _, c := range t.All() {
			_ = b.startEngineLocked(c)
		}
	}
}

// stopAllEnginesLocked closes every cluster's server. Must hold b.mu.
func (b *InMemoryBackend) stopAllEnginesLocked() {
	for _, t := range b.clusters {
		for _, c := range t.All() {
			stopEngine(c)
		}
	}
}

// syncAllAuthLocked re-applies ACL passwords to every running server. Must hold b.mu.
func (b *InMemoryBackend) syncAllAuthLocked() {
	for _, t := range b.clusters {
		for _, c := range t.All() {
			b.syncAuthLocked(c)
		}
	}
}

// syncAuthLocked mirrors the cluster ACL's password users into the server's AUTH
// table. Only the first password per user is enforced, and access strings are not. Must hold b.mu.
func (b *InMemoryBackend) syncAuthLocked(c *Cluster) {
	if c.engine == nil || c.engine.Mini == nil {
		return
	}

	for _, name := range c.authUsers {
		c.engine.Mini.RequireUserAuth(name, "")
	}

	c.authUsers = nil

	region := c.Region
	if region == "" {
		region = b.defaultRegion
	}

	acl, ok := b.aclsStore(region).Get(c.ACLName)
	if !ok {
		return
	}

	for _, userName := range acl.UserNames {
		u, found := b.usersStore(region).Get(userName)
		if !found || len(u.Passwords) == 0 {
			continue
		}

		c.engine.Mini.RequireUserAuth(u.Name, u.Passwords[0])
		c.authUsers = append(c.authUsers, u.Name)
	}
}

// dataEndpoint returns the real server address for the cluster, or nil when none is running.
func (c *Cluster) dataEndpoint() *endpointObject {
	if c.engine == nil || c.engine.Mini == nil {
		return nil
	}

	return &endpointObject{Address: c.engine.Host, Port: int32(c.engine.Port)} //nolint:gosec // TCP port fits int32
}

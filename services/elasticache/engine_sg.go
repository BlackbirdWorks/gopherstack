package elasticache

import (
	"net"

	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

const defaultRedisPort = 6379

// SetSecurityGroupEnforcer makes clusters started from now on refuse clients their VPC security groups do
// not admit; nil leaves them open.
func (b *InMemoryBackend) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) { b.sgEnforcer.Store(e) }

// SetSecurityGroupEnforcer forwards to the backend when it supports enforcement.
func (h *Handler) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.SetSecurityGroupEnforcer(e)
	}
}

func (b *InMemoryBackend) startRedis() (*redisengine.Engine, error) {
	if b.sgEnforcer.Load() != nil {
		return redisengine.StartGated(b.allocator, "elasticache")
	}

	return redisengine.Start(b.allocator, "elasticache")
}

// armGateLocked points c's relay at its live security groups; a no-op for ungated engines. Must hold b.mu.
func (b *InMemoryBackend) armGateLocked(c *Cluster, svcPort int) {
	enf := b.sgEnforcer.Load()
	if c.gate == nil || enf == nil {
		return
	}

	region, id := c.Region, c.ClusterID

	c.gate.Set(func(remote net.Addr) bool {
		return enf.AdmitIngress(b.clusterGroups(region, id), sgfilter.ProtoTCP, svcPort, remote)
	})
}

func (b *InMemoryBackend) clusterGroups(region, id string) []string {
	b.mu.RLock("elasticacheClusterGroups")
	defer b.mu.RUnlock()

	c, ok := b.clustersStore(region).Get(id)
	if !ok {
		return nil
	}

	return append([]string(nil), c.SecurityGroupIDs...)
}

func closeEngine(c *Cluster) {
	if c.front != nil {
		c.front.Close()
		c.front = nil
	}

	if c.mini != nil {
		c.mini.Close()
		c.mini = nil
	}
}

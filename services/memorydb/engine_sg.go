package memorydb

import (
	"fmt"
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

// startRedis starts c's server; with enforcement on, clients reach it through a relay that checks the
// cluster's live security groups.
func (b *InMemoryBackend) startRedis(c *Cluster) (*redisengine.Engine, error) {
	enf := b.sgEnforcer.Load()
	if enf == nil {
		return redisengine.Start(b.allocator, engineLabel)
	}

	eng, err := redisengine.StartGated(b.allocator, engineLabel)
	if err != nil {
		return nil, fmt.Errorf("start gated redis: %w", err)
	}

	region, name := c.Region, c.Name
	port := defaultRedisPort

	if c.Port > 0 {
		port = int(c.Port)
	}

	eng.Gate.Set(func(remote net.Addr) bool {
		return enf.AdmitIngress(b.clusterGroups(region, name), sgfilter.ProtoTCP, port, remote)
	})

	return eng, nil
}

func (b *InMemoryBackend) clusterGroups(region, name string) []string {
	b.mu.RLock("memorydbClusterGroups")
	defer b.mu.RUnlock()

	c, ok := tableGet(b.clusters[region], name)
	if !ok {
		return nil
	}

	return append([]string(nil), c.SecurityGroupIDs...)
}

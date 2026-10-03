package rds

import (
	"fmt"
	"net"
	"strconv"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

// EnableHTTPEndpoint enables the HTTP endpoint for an Aurora Serverless cluster.
func (b *InMemoryBackend) EnableHTTPEndpoint(resourceARN string) error {
	b.mu.Lock("EnableHTTPEndpoint")
	defer b.mu.Unlock()
	for _, cluster := range b.clusters.All() {
		if cluster.DBClusterIdentifier == resourceARN ||
			b.rdsARN("cluster", cluster.DBClusterIdentifier) == resourceARN {
			cluster.HTTPEndpointEnabled = true

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrResourceNotFound, resourceARN)
}

// DisableHTTPEndpoint disables the HTTP endpoint for an Aurora Serverless cluster.
func (b *InMemoryBackend) DisableHTTPEndpoint(resourceARN string) error {
	b.mu.Lock("DisableHTTPEndpoint")
	defer b.mu.Unlock()
	for _, cluster := range b.clusters.All() {
		if cluster.DBClusterIdentifier == resourceARN ||
			b.rdsARN("cluster", cluster.DBClusterIdentifier) == resourceARN {
			cluster.HTTPEndpointEnabled = false

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrResourceNotFound, resourceARN)
}

// DataAPITarget resolves the real engine behind a docker-backed cluster for the RDS Data API;
// rdsdata.ErrNotRealCluster means the cluster has none and the SQLite engine serves it.
func (b *InMemoryBackend) DataAPITarget(resourceARN string) (rdsdata.RealTarget, error) {
	b.mu.RLock("DataAPITarget")
	defer b.mu.RUnlock()

	for _, c := range b.clusters.All() {
		if c.DBClusterIdentifier != resourceARN && b.rdsARN("cluster", c.DBClusterIdentifier) != resourceARN {
			continue
		}

		lb := b.clusterUnitLocked(c.DBClusterIdentifier)
		if lb == nil || engineKind(c.Engine) == "" {
			return rdsdata.RealTarget{}, rdsdata.ErrNotRealCluster
		}

		if !c.HTTPEndpointEnabled {
			return rdsdata.RealTarget{}, rdsdata.ErrHTTPEndpointNotEnabled
		}

		if !lb.ready {
			return rdsdata.RealTarget{}, rdsdata.ErrClusterNotReady
		}

		kind := engineMySQL
		if lb.kind == enginePostgres {
			kind = enginePostgres
		}

		return rdsdata.RealTarget{
			Kind:   kind,
			Addr:   net.JoinHostPort(b.engine.cfg.Host, strconv.Itoa(lb.port)),
			DBName: c.DatabaseName,
		}, nil
	}

	return rdsdata.RealTarget{}, rdsdata.ErrNotRealCluster
}

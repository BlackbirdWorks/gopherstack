package neptune

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const dataCaptureTimeout = 10 * time.Second

// captureClusterDataLocked exports the cluster's property graph and RDF store; both are empty when nothing is
// stored. Callers hold b.mu.
func (b *InMemoryBackend) captureClusterDataLocked(
	ctx context.Context, region, clusterID string,
) (string, string, error) {
	key := regionKey(region, clusterID)

	var pg, rdf string

	if g := b.graphs[key]; g != nil {
		nodes, rels := g.Counts()
		if nodes+rels > 0 {
			cctx, cancel := context.WithTimeout(ctx, dataCaptureTimeout)
			raw, err := g.Export(cctx)

			cancel()

			if err != nil {
				return "", "", fmt.Errorf("%w: property graph is busy: %w", ErrInvalidDBClusterStateFault, err)
			}

			pg = string(raw)
		}
	}

	if r := b.rdf[key]; r != nil && r.store.Len() > 0 {
		rdf = string(r.store.Export())
	}

	return pg, rdf, nil
}

// loadClusterDataLocked fills a freshly provisioned cluster with captured graph data. Callers hold b.mu.
func (b *InMemoryBackend) loadClusterDataLocked(ctx context.Context, key, pg, rdf string) {
	if pg != "" {
		if err := b.graphLocked(key).Import(ctx, []byte(pg)); err != nil {
			logger.Load(ctx).
				WarnContext(ctx, "neptune: restoring property graph data failed", "cluster", key, "error", err)
		}
	}

	if rdf != "" {
		if err := b.rdfLocked(key).store.Import([]byte(rdf)); err != nil {
			logger.Load(ctx).WarnContext(ctx, "neptune: restoring RDF data failed", "cluster", key, "error", err)
		}
	}
}

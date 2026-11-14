package neptune

import (
	"encoding/json/jsontext"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/neptune/streamlog"

	"context"
)

const (
	paramStreams      = "neptune_streams"
	paramStreamExpiry = "neptune_streams_expiry_days"
	hoursPerDay       = 24
)

// streamPolicyLocked reads neptune_streams and neptune_streams_expiry_days from the cluster's parameter group.
func (b *InMemoryBackend) streamPolicyLocked(region string, c *DBCluster) streamlog.Policy {
	params := describeParameters(
		neptuneClusterParameterCatalog(),
		b.clusterParameterOverrides[regionKey(region, c.DBClusterParameterGroupName)],
	)

	days, err := strconv.Atoi(paramValue(params, paramStreamExpiry, "7"))
	if err != nil || days < 1 {
		days = 7
	}

	return streamlog.Policy{
		Enabled:   paramValue(params, paramStreams, "0") == "1",
		Retention: time.Duration(days) * hoursPerDay * time.Hour,
	}
}

// applyStreamPolicyLocked pushes the cluster's current stream parameters to its logs; Neptune applies them at
// instance (re)start, so this runs on provisioning and reboot only.
func (b *InMemoryBackend) applyStreamPolicyLocked(region, clusterID string) {
	c, ok := b.clusterGet(region, clusterID)
	if !ok {
		return
	}

	key := regionKey(region, clusterID)
	pol := b.streamPolicyLocked(region, c)

	if g, found := b.graphs[key]; found {
		g.Stream().SetPolicy(pol)
	}

	if r, found := b.rdf[key]; found {
		r.store.Stream().SetPolicy(pol)
	}
}

type streamsDTO struct {
	PropertyGraph jsontext.Value `json:"propertyGraph,omitempty"`
	Sparql        jsontext.Value `json:"sparql,omitempty"`
}

func (b *InMemoryBackend) exportStreamsLocked(ctx context.Context) map[string]streamsDTO {
	out := map[string]streamsDTO{}

	for key, g := range b.graphs {
		raw, err := g.Stream().Export()
		if err != nil {
			logger.Load(ctx).
				WarnContext(ctx, "neptune: property graph stream export failed", "cluster", key, "error", err)

			continue
		}

		if raw != nil {
			d := out[key]
			d.PropertyGraph = raw
			out[key] = d
		}
	}

	for key, r := range b.rdf {
		raw, err := r.store.Stream().Export()
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "neptune: SPARQL stream export failed", "cluster", key, "error", err)

			continue
		}

		if raw != nil {
			d := out[key]
			d.Sparql = raw
			out[key] = d
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func (b *InMemoryBackend) importStreamsLocked(ctx context.Context, dumps map[string]streamsDTO) {
	for key, d := range dumps {
		if len(d.PropertyGraph) > 0 {
			if err := b.graphLocked(key).Stream().Import(d.PropertyGraph); err != nil {
				logger.Load(ctx).
					WarnContext(ctx, "neptune: property graph stream import failed", "cluster", key, "error", err)
			}
		}

		if len(d.Sparql) > 0 {
			if err := b.rdfLocked(key).store.Stream().Import(d.Sparql); err != nil {
				logger.Load(ctx).WarnContext(ctx, "neptune: SPARQL stream import failed", "cluster", key, "error", err)
			}
		}
	}
}

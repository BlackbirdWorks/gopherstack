package docdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

type gatedStopRuntime struct {
	docdb.EngineRuntime
	entered chan struct{}
	release chan struct{}
}

func (g *gatedStopRuntime) StopContainer(ctx context.Context, id string) error {
	close(g.entered)
	<-g.release

	return g.EngineRuntime.StopContainer(ctx, id)
}

func TestEngineStoppedPublishedAfterContainerStop(t *testing.T) {
	t.Parallel()

	rt := &gatedStopRuntime{
		EngineRuntime: &fakeEngineRuntime{},
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(docdb.EngineConfig{Runtime: rt, Probe: readyProbe, StartTimeout: time.Minute})
	t.Cleanup(b.Close)

	createEngineCluster(t, b, "5.0.0")
	waitClusterStatus(t, b, "available")

	out, err := b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	assert.Equal(t, "stopping", out.Status)

	<-rt.entered
	assert.Equal(t, "stopping", clusterStatus(b, "c1").Status)

	close(rt.release)
	waitClusterStatus(t, b, "stopped")
}

func TestEngineAvailablePublishedAfterEngineUpOnStart(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	gate := newGatedMongoProbe()
	b := engineBackend(t, rt, gate.probe)
	createEngineCluster(t, b, "5.0.0")
	close(gate.gate)
	waitClusterStatus(t, b, "available")

	_, err := b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	waitClusterStatus(t, b, "stopped")

	out, err := b.StartDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	assert.Equal(t, "starting", out.Status)
	waitClusterStatus(t, b, "available")
}

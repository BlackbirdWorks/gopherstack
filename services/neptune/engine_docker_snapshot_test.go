package neptune_test

import (
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func dockerEngineBackend(t *testing.T) *neptune.InMemoryBackend {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	er, ok := rt.(neptune.EngineRuntime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(neptune.EngineConfig{Runtime: &recordingEngineRuntime{EngineRuntime: er}})
	t.Cleanup(b.Close)

	return b
}

func awaitDockerCluster(t *testing.T, b *neptune.InMemoryBackend, id, want string) neptune.DBCluster {
	t.Helper()

	var c neptune.DBCluster

	require.Eventually(t, func() bool {
		cs, err := b.DescribeDBClusters(t.Context(), id, neptune.DBClusterFilters{})
		if err != nil || len(cs) != 1 {
			return false
		}

		c = cs[0]

		return c.Status == want
	}, dockerEngineWait, 250*time.Millisecond)

	return c
}

func containerScript(t *testing.T, c neptune.DBCluster, script string) []string {
	t.Helper()

	rs, err := gremlinClient(t, c).Submit(script)
	require.NoError(t, err)

	all, err := rs.All()
	require.NoError(t, err)

	return renderAll(all)
}

func containerVertexCount(t *testing.T, c neptune.DBCluster) string {
	t.Helper()

	return containerScript(t, c, "g.V().count()")[0]
}

func TestEngineDockerGremlinGraphSurvivesStopAndRestore(t *testing.T) {
	t.Parallel()

	b := dockerEngineBackend(t)
	createEngineCluster(t, b, "1.4.0.0")

	c := awaitDockerCluster(t, b, "c1", "available")
	id := c.DBClusterIdentifier

	containerScript(t, c, "g.addV('person').property('name','marko').property('age',29).iterate()")
	containerScript(t, c, "g.addV('person').property('name','vadas').iterate()")
	containerScript(t, c, "g.V().has('name','marko').addE('knows').to(__.V().has('name','vadas')).iterate()")
	assert.Equal(t, "2", containerVertexCount(t, c))

	_, err := b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	awaitDockerCluster(t, b, id, "stopped")

	_, err = b.StartDBCluster(t.Context(), "c1")
	require.NoError(t, err)

	started := awaitDockerCluster(t, b, id, "available")
	assert.Equal(t, "2", containerVertexCount(t, started), "a stopped cluster keeps its graph")

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := dockerEngineBackend(t)
	require.NoError(t, b2.Restore(t.Context(), snap))

	c2 := awaitDockerCluster(t, b2, id, "available")
	assert.Equal(t, "2", containerVertexCount(t, c2), "a restored cluster gets its graph back")

	assert.Equal(t, []string{"vadas"}, containerScript(t, c2, "g.V().has('name','marko').out('knows').values('name')"))
}

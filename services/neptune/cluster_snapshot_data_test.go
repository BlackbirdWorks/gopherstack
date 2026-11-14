package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func seedClusterData(t *testing.T, c neptune.DBCluster) {
	t.Helper()

	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> "snap" }`)

	res := sparqlDo(t, c, http.MethodPost, "/openCypher", formCT, "", "query=CREATE+(:Snap+{n:7})")
	require.Equal(t, http.StatusOK, res.status, res.body)
}

func assertClusterData(t *testing.T, c neptune.DBCluster, want bool) {
	t.Helper()

	ask := sparqlDo(t, c, http.MethodGet, "/sparql?"+url.Values{
		"query": {`ASK { <urn:s> <urn:p> "snap" }`},
	}.Encode(), "", "application/sparql-results+json", "")
	cnt := sparqlDo(t, c, http.MethodPost, "/openCypher", formCT, "", "query=MATCH+(n:Snap)+RETURN+count(n)+AS+c")

	if want {
		assert.Contains(t, ask.body, `"boolean":true`)
		assert.Contains(t, cnt.body, `"c":1`)

		return
	}

	assert.Contains(t, ask.body, `"boolean":false`)
	assert.Contains(t, cnt.body, `"c":0`)
}

func TestClusterDataFollowsSnapshotsAndClones(t *testing.T) {
	t.Parallel()

	b, c := newEmbeddedBackend(t)
	seedClusterData(t, c)

	_, err := b.CreateDBClusterSnapshot(t.Context(), "s1", "c1")
	require.NoError(t, err)

	_, err = b.CopyDBClusterSnapshot(t.Context(), "s1", "s2")
	require.NoError(t, err)

	sparqlUpdate(t, c, `DELETE WHERE { <urn:s> <urn:p> ?o }`)

	tests := []struct {
		restore func() error
		name    string
		target  string
	}{
		{
			name: "from snapshot", target: "r1",
			restore: func() error {
				_, rerr := b.RestoreDBClusterFromSnapshot(t.Context(), "s1", "r1", neptune.RestoreClusterOptions{})

				return rerr
			},
		},
		{
			name: "from copied snapshot", target: "r2",
			restore: func() error {
				_, rerr := b.RestoreDBClusterFromSnapshot(t.Context(), "s2", "r2", neptune.RestoreClusterOptions{})

				return rerr
			},
		},
	}

	for _, tt := range tests {
		require.NoError(t, tt.restore(), tt.name)

		restored := waitCluster(t, b, tt.target, "available")
		assertClusterData(t, restored, true)
	}

	assert.Contains(t, sparqlDo(t, c, http.MethodGet, "/sparql?"+url.Values{
		"query": {`ASK { <urn:s> <urn:p> "snap" }`},
	}.Encode(), "", "application/sparql-results+json", "").body, `"boolean":false`,
		"the source cluster kept its later change")

	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> "snap" }`)

	_, err = b.RestoreDBClusterToPointInTime(t.Context(), "c1", "clone", neptune.RestoreToPointInTimeOptions{
		UseLatestRestorableTime: true,
	})
	require.NoError(t, err)
	assertClusterData(t, waitCluster(t, b, "clone", "available"), true)
}

func TestClusterSnapshotDataPersists(t *testing.T) {
	t.Parallel()

	b, c := newEmbeddedBackend(t)
	seedClusterData(t, c)

	_, err := b.CreateDBClusterSnapshot(t.Context(), "s1", "c1")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())

	b2 := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b2.EnableEngine(neptune.EngineConfig{InProcessGremlin: true, OpenCypher: true})
	t.Cleanup(b2.Close)
	require.NoError(t, b2.Restore(t.Context(), snap))

	_, err = b2.RestoreDBClusterFromSnapshot(t.Context(), "s1", "r1", neptune.RestoreClusterOptions{})
	require.NoError(t, err)
	assertClusterData(t, waitCluster(t, b2, "r1", "available"), true)
}

package neptune_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func TestNeptunedataStorageInfo(t *testing.T) {
	t.Parallel()

	_, c := newStreamsBackend(t)
	cl := dataClient(clusterAddr(c))

	empty, err := cl.GetStorageInfo(t.Context(), &neptunedata.GetStorageInfoInput{})
	require.NoError(t, err)
	assert.Zero(t, aws.ToInt64(empty.TotalStorageBytesUsed))
	assert.Zero(t, aws.ToInt64(empty.Quads.RDF.SPOG.Count))

	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> "v" . <urn:s> <urn:q> <urn:o> }`)

	res := sparqlDo(t, c, "POST", "/openCypher", "application/x-www-form-urlencoded", "",
		"query=CREATE+(a:P+{n:1}),+(b:Q),+(a)-%5B:R+{w:2}%5D-%3E(b)")
	require.Equal(t, 200, res.status, res.body)

	out, err := cl.GetStorageInfo(t.Context(), &neptunedata.GetStorageInfoInput{})
	require.NoError(t, err)

	assert.Equal(t, int64(2), aws.ToInt64(out.Quads.RDF.SPOG.Count))
	assert.Equal(t, int64(2), aws.ToInt64(out.Quads.RDF.PZSOG.Count))
	assert.Equal(
		t,
		int64(5),
		aws.ToInt64(out.Quads.PG.GPSO.Count),
		"two labels, one node property, the edge, its property",
	)
	assert.Positive(t, aws.ToInt64(out.Quads.RDF.OSGP.SizeInBytes))
	assert.Positive(t, aws.ToInt64(out.Dictionaries.Total.Count))
	assert.Positive(t, aws.ToInt64(out.Dictionaries.Total.SizeInBytes))
	assert.Positive(t, aws.ToInt64(out.Streams.Count))
	assert.Positive(t, aws.ToInt64(out.Streams.SizeInBytes))
	assert.Zero(t, aws.ToInt64(out.UndoLogs.ListSize))
	assert.Greater(t, aws.ToInt64(out.TotalStorageBytesUsed), aws.ToInt64(out.Streams.SizeInBytes))
}

func TestNeptunedataIndexRebuild(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	cl := dataClient(clusterAddr(c))

	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> <urn:o> }`)

	start, err := cl.StartIndexRebuild(t.Context(), &neptunedata.StartIndexRebuildInput{
		Scope: types.ScopeQuads, Concurrency: aws.Int32(1000), WriteBufferSize: types.WriteBufferSizeHigh,
		ClientToken: aws.String("tok-1"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(start.RebuildId))
	assert.Equal(t, types.ScopeQuads, start.Scope)
	assert.Equal(t, types.WriteBufferSizeHigh, start.WriteBufferSize)
	assert.Positive(t, aws.ToInt32(start.Concurrency))
	assert.Less(t, aws.ToInt32(start.Concurrency), int32(1000), "capped to the writer's vCPUs")

	again, err := cl.StartIndexRebuild(
		t.Context(),
		&neptunedata.StartIndexRebuildInput{ClientToken: aws.String("tok-1")},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(start.RebuildId), aws.ToString(again.RebuildId), "client token is idempotent")

	require.Eventually(t, func() bool {
		st, gerr := cl.GetIndexRebuildStatus(
			t.Context(),
			&neptunedata.GetIndexRebuildStatusInput{RebuildId: start.RebuildId},
		)

		return gerr == nil && st.Status == types.RebuildStatusCompleted
	}, 10*time.Second, 5*time.Millisecond)

	st, err := cl.GetIndexRebuildStatus(
		t.Context(),
		&neptunedata.GetIndexRebuildStatusInput{RebuildId: start.RebuildId},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(100), aws.ToInt32(st.ProgressPercentage))
	assert.False(t, st.StartTime.IsZero())
	assert.False(t, st.EndTime.IsZero())
	assert.Equal(t, types.ScopeQuads, st.Scope)

	list, err := cl.ListIndexRebuilds(t.Context(), &neptunedata.ListIndexRebuildsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{aws.ToString(start.RebuildId)}, list.RebuildIds)

	_, err = cl.CancelIndexRebuild(t.Context(), &neptunedata.CancelIndexRebuildInput{RebuildId: start.RebuildId})
	require.Error(t, err)
	assert.Equal(t, "InvalidParameterException", errCode(t, err), "completed rebuilds cannot be cancelled")

	_, err = cl.GetIndexRebuildStatus(
		t.Context(),
		&neptunedata.GetIndexRebuildStatusInput{RebuildId: aws.String("nope")},
	)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

	_, err = cl.StartIndexRebuild(t.Context(), &neptunedata.StartIndexRebuildInput{Scope: "BOGUS"})
	require.Error(t, err)
	assert.Equal(t, "InvalidParameterException", errCode(t, err))

	res := sparqlDo(
		t,
		c,
		"GET",
		"/sparql?query=ASK+%7B%3Chttp%3A%2F%2Fx%3E+%3Fp+%3Fo%7D",
		"",
		"application/sparql-results+json",
		"",
	)
	assert.Equal(t, 200, res.status, "queries still work against the rebuilt indexes")
}

func TestNeptunedataIndexRebuildCancel(t *testing.T) {
	t.Parallel()

	b, c := newEmbeddedBackend(t)
	cl := dataClient(clusterAddr(c))

	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> <urn:o> }`)

	release := neptune.HoldIndexRebuilds(b, "us-east-1", "c1")

	start, err := cl.StartIndexRebuild(t.Context(), &neptunedata.StartIndexRebuildInput{})
	require.NoError(t, err)

	st, err := cl.GetIndexRebuildStatus(
		t.Context(),
		&neptunedata.GetIndexRebuildStatusInput{RebuildId: start.RebuildId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.RebuildStatusPreparing, st.Status)

	cancel, err := cl.CancelIndexRebuild(t.Context(), &neptunedata.CancelIndexRebuildInput{RebuildId: start.RebuildId})
	require.NoError(t, err)
	assert.Equal(t, types.RebuildStatusCancelling, cancel.Status)

	release()

	require.Eventually(t, func() bool {
		s, gerr := cl.GetIndexRebuildStatus(
			t.Context(),
			&neptunedata.GetIndexRebuildStatusInput{RebuildId: start.RebuildId},
		)

		return gerr == nil && s.Status == types.RebuildStatusCancelled
	}, 10*time.Second, 5*time.Millisecond)
}

func TestIndexRebuildPersistence(t *testing.T) {
	t.Parallel()

	b, c := newEmbeddedBackend(t)
	cl := dataClient(clusterAddr(c))

	start, err := cl.StartIndexRebuild(t.Context(), &neptunedata.StartIndexRebuildInput{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		s, gerr := cl.GetIndexRebuildStatus(
			t.Context(),
			&neptunedata.GetIndexRebuildStatusInput{RebuildId: start.RebuildId},
		)

		return gerr == nil && s.Status == types.RebuildStatusCompleted
	}, 10*time.Second, 5*time.Millisecond)

	snap := b.Snapshot(t.Context())

	b2 := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b2.EnableEngine(neptune.EngineConfig{InProcessGremlin: true, OpenCypher: true})
	t.Cleanup(b2.Close)
	require.NoError(t, b2.Restore(t.Context(), snap))

	c2 := waitCluster(t, b2, "c1", "available")

	list, err := dataClient(clusterAddr(c2)).ListIndexRebuilds(t.Context(), &neptunedata.ListIndexRebuildsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{aws.ToString(start.RebuildId)}, list.RebuildIds)
}

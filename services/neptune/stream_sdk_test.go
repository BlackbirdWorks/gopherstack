package neptune_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func newStreamsBackend(t *testing.T) (*neptune.InMemoryBackend, neptune.DBCluster) {
	t.Helper()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(neptune.EngineConfig{InProcessGremlin: true, OpenCypher: true})
	t.Cleanup(b.Close)

	_, err := b.CreateDBClusterParameterGroup(t.Context(), "streams-pg", "neptune1.3", "streams")
	require.NoError(t, err)

	_, err = b.ModifyDBClusterParameterGroup(t.Context(), "streams-pg", []neptune.ParameterInput{
		{ParameterName: "neptune_streams", ParameterValue: "1", ApplyMethod: "pending-reboot"},
	})
	require.NoError(t, err)

	_, err = b.CreateDBCluster(
		t.Context(),
		"c1",
		"streams-pg",
		0,
		neptune.DBClusterCreateOptions{EngineVersion: "1.3.0.0"},
	)
	require.NoError(t, err)

	return b, waitCluster(t, b, "c1", "available")
}

func errCode(t *testing.T, err error) string {
	t.Helper()

	var ae smithy.APIError

	require.ErrorAs(t, err, &ae)

	return ae.ErrorCode()
}

func TestSparqlStream(t *testing.T) {
	t.Parallel()

	_, c := newStreamsBackend(t)
	cl := dataClient(clusterAddr(c))

	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> <urn:o1> . <urn:s> <urn:p> <urn:o2> }`)
	sparqlUpdate(t, c, `DELETE DATA { <urn:s> <urn:p> <urn:o1> }`)

	const dg = "<http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph>"

	horizon, err := cl.GetSparqlStream(t.Context(), &neptunedata.GetSparqlStreamInput{
		IteratorType: types.IteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	assert.Equal(t, "NQUADS", aws.ToString(horizon.Format))
	assert.Equal(t, int32(3), aws.ToInt32(horizon.TotalRecords))
	require.Len(t, horizon.Records, 3)
	assert.Equal(t, map[string]string{"commitNum": "2", "opNum": "1"}, horizon.LastEventId)
	assert.Positive(t, aws.ToInt64(horizon.LastTrxTimestampInMillis))

	want := []struct {
		op, stmt string
		commit   string
		op1      string
		last     bool
	}{
		{"ADD", "<urn:s> <urn:p> <urn:o1> " + dg + " .\n", "1", "1", false},
		{"ADD", "<urn:s> <urn:p> <urn:o2> " + dg + " .\n", "1", "2", true},
		{"REMOVE", "<urn:s> <urn:p> <urn:o1> " + dg + " .\n", "2", "1", true},
	}

	for i, w := range want {
		r := horizon.Records[i]
		assert.Equal(t, w.op, aws.ToString(r.Op))
		assert.Equal(t, w.stmt, aws.ToString(r.Data.Stmt))
		assert.Equal(t, map[string]string{"commitNum": w.commit, "opNum": w.op1}, r.EventId)
		assert.Equal(t, w.last, aws.ToBool(r.IsLastOp))
		assert.Positive(t, aws.ToInt64(r.CommitTimestampInMillis))
	}

	tests := []struct {
		name      string
		in        neptunedata.GetSparqlStreamInput
		wantCodes []string
		wantOps   []string
	}{
		{
			name:    "at",
			in:      neptunedata.GetSparqlStreamInput{CommitNum: aws.Int64(1), OpNum: aws.Int64(2)},
			wantOps: []string{"ADD", "REMOVE"},
		},
		{
			name: "after",
			in: neptunedata.GetSparqlStreamInput{
				IteratorType: types.IteratorTypeAfterSequenceNumber, CommitNum: aws.Int64(1), OpNum: aws.Int64(2),
			},
			wantOps: []string{"REMOVE"},
		},
		{
			name: "after last",
			in: neptunedata.GetSparqlStreamInput{
				IteratorType: types.IteratorTypeAfterSequenceNumber,
				CommitNum:    aws.Int64(2),
			},
			wantOps: nil,
		},
		{
			name:    "latest",
			in:      neptunedata.GetSparqlStreamInput{IteratorType: types.IteratorTypeLatest},
			wantOps: []string{"REMOVE"},
		},
		{
			name:    "limit",
			in:      neptunedata.GetSparqlStreamInput{IteratorType: types.IteratorTypeTrimHorizon, Limit: aws.Int64(1)},
			wantOps: []string{"ADD"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, gerr := cl.GetSparqlStream(t.Context(), &tt.in)
			require.NoError(t, gerr)

			var ops []string
			for _, r := range out.Records {
				ops = append(ops, aws.ToString(r.Op))
			}

			assert.Equal(t, tt.wantOps, ops)
			assert.Equal(t, int32(len(tt.wantOps)), aws.ToInt32(out.TotalRecords))
		})
	}

	errTests := []struct {
		name string
		in   neptunedata.GetSparqlStreamInput
		code string
	}{
		{"past newest", neptunedata.GetSparqlStreamInput{CommitNum: aws.Int64(9)}, "StreamRecordsNotFoundException"},
		{
			"past newest op",
			neptunedata.GetSparqlStreamInput{CommitNum: aws.Int64(2), OpNum: aws.Int64(5)},
			"StreamRecordsNotFoundException",
		},
		{"no commit num", neptunedata.GetSparqlStreamInput{}, "MissingParameterException"},
		{
			"limit range",
			neptunedata.GetSparqlStreamInput{IteratorType: types.IteratorTypeLatest, Limit: aws.Int64(100001)},
			"InvalidParameterException",
		},
		{"bad iterator", neptunedata.GetSparqlStreamInput{IteratorType: "NOPE"}, "InvalidParameterException"},
	}

	for _, tt := range errTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, gerr := cl.GetSparqlStream(t.Context(), &tt.in)
			require.Error(t, gerr)
			assert.Equal(t, tt.code, errCode(t, gerr))
		})
	}
}

func TestPropertygraphStream(t *testing.T) {
	t.Parallel()

	_, c := newStreamsBackend(t)
	cl := dataClient(clusterAddr(c))

	res := sparqlDo(t, c, "POST", "/openCypher", "application/x-www-form-urlencoded", "",
		"query=CREATE+(a:P+{n:1}),+(b:Q),+(a)-%5B:R+{w:2}%5D-%3E(b)")
	require.Equal(t, 200, res.status, res.body)

	res = sparqlDo(t, c, "POST", "/openCypher", "application/x-www-form-urlencoded", "",
		"query=MATCH+(a:P)+SET+a.n+%3D+5")
	require.Equal(t, 200, res.status, res.body)

	g := gremlinG(t, c)
	require.NoError(t, <-g.AddV("G").Property("k", "v").Iterate())

	out, err := cl.GetPropertygraphStream(t.Context(), &neptunedata.GetPropertygraphStreamInput{
		IteratorType: types.IteratorTypeTrimHorizon, Limit: aws.Int64(1000),
	})
	require.NoError(t, err)
	assert.Equal(t, "PG_JSON", aws.ToString(out.Format))

	type rec struct{ op, typ, key, commit string }

	got := make([]rec, 0, len(out.Records))

	for _, r := range out.Records {
		got = append(
			got,
			rec{aws.ToString(r.Op), aws.ToString(r.Data.Type), aws.ToString(r.Data.Key), r.EventId["commitNum"]},
		)
	}

	assert.Equal(t, []rec{
		{"ADD", "vl", "label", "1"}, {"ADD", "vp", "n", "1"}, {"ADD", "vl", "label", "1"},
		{"ADD", "e", "label", "1"}, {"ADD", "ep", "w", "1"},
		{"REMOVE", "vp", "n", "2"}, {"ADD", "vp", "n", "2"},
		{"ADD", "vl", "label", "3"}, {"ADD", "vp", "k", "3"},
	}, got)

	edge := out.Records[3].Data
	assert.NotEmpty(t, aws.ToString(edge.From))
	assert.NotEmpty(t, aws.ToString(edge.To))
	assert.True(t, aws.ToBool(out.Records[4].IsLastOp))
	assert.False(t, aws.ToBool(out.Records[3].IsLastOp))
	assert.Equal(t, map[string]string{"commitNum": "3", "opNum": "2"}, out.LastEventId)

	prop, err := out.Records[5].Data.Value.MarshalSmithyDocument()
	require.NoError(t, err)
	assert.JSONEq(t, `{"value":1,"dataType":"Long"}`, string(prop))

	for _, path := range []string{"/propertygraph/stream", "/pg/stream", "/gremlin/stream"} {
		res = sparqlDo(t, c, "GET", path+"?iteratorType=LATEST", "", "", "")
		assert.Equal(t, 200, res.status, path)
	}
}

func TestStreamDisabled(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	cl := dataClient(clusterAddr(c))

	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> <urn:o> }`)

	_, err := cl.GetSparqlStream(
		t.Context(),
		&neptunedata.GetSparqlStreamInput{IteratorType: types.IteratorTypeTrimHorizon},
	)
	require.Error(t, err)
	assert.Equal(t, "UnsupportedOperationException", errCode(t, err))

	_, err = cl.GetPropertygraphStream(
		t.Context(),
		&neptunedata.GetPropertygraphStreamInput{IteratorType: types.IteratorTypeTrimHorizon},
	)
	require.Error(t, err)
	assert.Equal(t, "UnsupportedOperationException", errCode(t, err))
}

func TestStreamEnabledOnReboot(t *testing.T) {
	t.Parallel()

	b, c := newEmbeddedBackend(t)
	cl := dataClient(clusterAddr(c))

	_, err := b.CreateDBInstance(t.Context(), "i1", "c1", "db.r5.large", neptune.DBInstanceCreateOptions{})
	require.NoError(t, err)

	_, err = b.CreateDBClusterParameterGroup(t.Context(), "pg2", "neptune1.3", "d")
	require.NoError(t, err)
	_, err = b.ModifyDBClusterParameterGroup(t.Context(), "pg2", []neptune.ParameterInput{
		{ParameterName: "neptune_streams", ParameterValue: "1", ApplyMethod: "pending-reboot"},
	})
	require.NoError(t, err)
	_, err = b.ModifyDBCluster(t.Context(), "c1", "pg2", neptune.DBClusterModifyOptions{})
	require.NoError(t, err)

	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> <urn:o> }`)

	_, err = cl.GetSparqlStream(t.Context(), &neptunedata.GetSparqlStreamInput{IteratorType: types.IteratorTypeLatest})
	require.Error(t, err, "static parameter applies only after a reboot")

	_, err = b.RebootDBInstance(t.Context(), "i1")
	require.NoError(t, err)

	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> <urn:o2> }`)

	out, err := cl.GetSparqlStream(
		t.Context(),
		&neptunedata.GetSparqlStreamInput{IteratorType: types.IteratorTypeLatest},
	)
	require.NoError(t, err)
	require.Len(t, out.Records, 1)
	assert.Equal(t, map[string]string{"commitNum": "1", "opNum": "1"}, out.LastEventId)
}

func TestStreamGzipAndMethod(t *testing.T) {
	t.Parallel()

	_, c := newStreamsBackend(t)
	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> <urn:o> }`)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"http://"+clusterAddr(c)+"/sparql/stream?iteratorType=TRIM_HORIZON", nil)
	require.NoError(t, err)
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	zr, err := gzip.NewReader(bytes.NewReader(raw))
	require.NoError(t, err)

	body, err := io.ReadAll(zr)
	require.NoError(t, err)
	assert.Contains(t, string(body), `"format":"NQUADS"`)
	assert.Contains(t, string(body), `"lastTrxTimestamp"`)

	post := sparqlDo(t, c, http.MethodPost, "/sparql/stream", "", "", "")
	assert.Equal(t, http.StatusMethodNotAllowed, post.status)
}

func TestStreamPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b, c := newStreamsBackend(t)
	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> <urn:o> }`)
	require.NoError(t, <-gremlinG(t, c).AddV("Kept").Iterate())

	snap := b.Snapshot(t.Context())

	b2 := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b2.EnableEngine(neptune.EngineConfig{InProcessGremlin: true, OpenCypher: true})
	t.Cleanup(b2.Close)
	require.NoError(t, b2.Restore(t.Context(), snap))

	c2 := waitCluster(t, b2, "c1", "available")
	cl := dataClient(clusterAddr(c2))

	out, err := cl.GetSparqlStream(
		t.Context(),
		&neptunedata.GetSparqlStreamInput{IteratorType: types.IteratorTypeTrimHorizon},
	)
	require.NoError(t, err)
	require.Len(t, out.Records, 1)

	pg, err := cl.GetPropertygraphStream(
		t.Context(),
		&neptunedata.GetPropertygraphStreamInput{IteratorType: types.IteratorTypeTrimHorizon},
	)
	require.NoError(t, err)
	require.Len(t, pg.Records, 1)

	sparqlUpdate(t, c2, `INSERT DATA { <urn:s> <urn:p> <urn:o3> }`)

	out, err = cl.GetSparqlStream(
		t.Context(),
		&neptunedata.GetSparqlStreamInput{IteratorType: types.IteratorTypeLatest},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"commitNum": "2", "opNum": "1"}, out.LastEventId)
}

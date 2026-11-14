package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNeptunedataPropertygraphSummary(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	cl := dataClient(clusterAddr(c))

	_, err := cl.GetPropertygraphSummary(t.Context(), &neptunedata.GetPropertygraphSummaryInput{})
	require.Error(t, err)
	assert.Equal(t, "StatisticsNotAvailableException", errCode(t, err))

	res := sparqlDo(t, c, "POST", "/openCypher", "application/x-www-form-urlencoded", "",
		"query=CREATE+(a:P+{n:1,m:2}),+(b:P+{n:2}),+(c:Q),+(a)-%5B:R+{w:5}%5D-%3E(b),+(b)-%5B:S%5D-%3E(c)")
	require.Equal(t, 200, res.status, res.body)

	_, err = cl.ManagePropertygraphStatistics(t.Context(), &neptunedata.ManagePropertygraphStatisticsInput{
		Mode: types.StatisticsAutoGenerationModeRefresh,
	})
	require.NoError(t, err)

	basic, err := cl.GetPropertygraphSummary(t.Context(), &neptunedata.GetPropertygraphSummaryInput{})
	require.NoError(t, err)
	assert.Equal(t, int32(200), aws.ToInt32(basic.StatusCode))

	p := basic.Payload
	assert.Equal(t, "v1", aws.ToString(p.Version))
	assert.False(t, p.LastStatisticsComputationTime.IsZero())

	gs := p.GraphSummary
	assert.Equal(t, int64(3), aws.ToInt64(gs.NumNodes))
	assert.Equal(t, int64(2), aws.ToInt64(gs.NumEdges))
	assert.Equal(t, []string{"P", "Q"}, gs.NodeLabels)
	assert.Equal(t, []string{"R", "S"}, gs.EdgeLabels)
	assert.Equal(t, int64(2), aws.ToInt64(gs.NumNodeLabels))
	assert.Equal(t, []map[string]int64{{"n": 2}, {"m": 1}}, gs.NodeProperties)
	assert.Equal(t, []map[string]int64{{"w": 1}}, gs.EdgeProperties)
	assert.Equal(t, int64(3), aws.ToInt64(gs.TotalNodePropertyValues))
	assert.Equal(t, int64(1), aws.ToInt64(gs.TotalEdgePropertyValues))
	assert.Empty(t, gs.NodeStructures)
	assert.Empty(t, gs.EdgeStructures)

	det, err := cl.GetPropertygraphSummary(t.Context(), &neptunedata.GetPropertygraphSummaryInput{
		Mode: types.GraphSummaryTypeDetailed,
	})
	require.NoError(t, err)

	ds := det.Payload.GraphSummary
	require.Len(t, ds.NodeStructures, 3)
	assert.Empty(t, ds.NodeStructures[0].NodeProperties)
	assert.Equal(t, []string{"m", "n"}, ds.NodeStructures[1].NodeProperties)
	assert.Equal(t, []string{"R"}, ds.NodeStructures[1].DistinctOutgoingEdgeLabels)
	assert.Equal(t, []string{"S"}, ds.NodeStructures[2].DistinctOutgoingEdgeLabels)
	require.Len(t, ds.EdgeStructures, 2)
	assert.Equal(t, int64(1), aws.ToInt64(ds.EdgeStructures[0].Count))

	_, err = cl.GetPropertygraphSummary(t.Context(), &neptunedata.GetPropertygraphSummaryInput{Mode: "bogus"})
	require.Error(t, err)
	assert.Equal(t, "InvalidParameterException", errCode(t, err))
}

func TestNeptunedataRDFSummary(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	cl := dataClient(clusterAddr(c))

	sparqlUpdate(t, c, `INSERT DATA {
		<urn:a> a <urn:Cls> ; <urn:p> 1 , 2 .
		<urn:b> a <urn:Cls> ; <urn:p> 3 .
		<urn:c> <urn:q> <urn:a> }`)

	_, err := cl.GetRDFGraphSummary(t.Context(), &neptunedata.GetRDFGraphSummaryInput{})
	require.Error(t, err)
	assert.Equal(t, "StatisticsNotAvailableException", errCode(t, err))

	_, err = cl.ManageSparqlStatistics(t.Context(), &neptunedata.ManageSparqlStatisticsInput{
		Mode: types.StatisticsAutoGenerationModeRefresh,
	})
	require.NoError(t, err)

	out, err := cl.GetRDFGraphSummary(
		t.Context(),
		&neptunedata.GetRDFGraphSummaryInput{Mode: types.GraphSummaryTypeDetailed},
	)
	require.NoError(t, err)

	gs := out.Payload.GraphSummary
	assert.Equal(t, int64(3), aws.ToInt64(gs.NumDistinctSubjects))
	assert.Equal(t, int64(3), aws.ToInt64(gs.NumDistinctPredicates))
	assert.Equal(t, int64(6), aws.ToInt64(gs.NumQuads))
	assert.Equal(t, []string{"urn:Cls"}, gs.Classes)
	assert.Equal(t, int64(1), aws.ToInt64(gs.NumClasses))
	assert.Equal(t, int64(3), gs.Predicates[0]["urn:p"])
	require.Len(t, gs.SubjectStructures, 2)
	assert.Equal(t, int64(2), aws.ToInt64(gs.SubjectStructures[0].Count))

	basic, err := cl.GetRDFGraphSummary(t.Context(), &neptunedata.GetRDFGraphSummaryInput{})
	require.NoError(t, err)
	assert.Empty(t, basic.Payload.GraphSummary.SubjectStructures)

	res := sparqlDo(t, c, "GET", "/pg/statistics/summary?mode=basic", "", "", "")
	assert.Equal(t, 400, res.status, "no property-graph statistics were computed")

	res = sparqlDo(t, c, "GET", "/rdf/statistics/summary?x=1", "", "", "")
	assert.Equal(t, 400, res.status)
	assert.Contains(t, res.body, "unknown parameters: 'x'")

	res = sparqlDo(t, c, "POST", "/rdf/statistics/summary", "", "", "")
	assert.Equal(t, 405, res.status)
}

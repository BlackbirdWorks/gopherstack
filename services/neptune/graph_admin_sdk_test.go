package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dataClient(addr string) *neptunedata.Client {
	return neptunedata.New(neptunedata.Options{
		Region: "us-east-1", BaseEndpoint: aws.String("http://" + addr),
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
}

func TestNeptunedataEngineStatus(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)

	out, err := dataClient(clusterAddr(c)).GetEngineStatus(t.Context(), &neptunedata.GetEngineStatusInput{})
	require.NoError(t, err)

	assert.Equal(t, "healthy", aws.ToString(out.Status))
	assert.Equal(t, "writer", aws.ToString(out.Role))
	assert.Equal(t, c.EngineVersion, aws.ToString(out.DbEngineVersion))
	assert.Equal(t, "viaQueryHint", aws.ToString(out.DfeQueryEngine))
	assert.Equal(t, "sparql-1.1", aws.ToString(out.Sparql.Version))
	assert.NotEmpty(t, aws.ToString(out.Gremlin.Version))
	assert.Equal(t, "120000", out.Settings["clusterQueryTimeoutInMs"])
	assert.Equal(t, "disabled", out.LabMode["ObjectIndex"])
	assert.Contains(t, out.Features, "IAMAuthentication")
}

func TestNeptunedataStatistics(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	cl := dataClient(clusterAddr(c))

	sparqlUpdate(t, c, `INSERT DATA { <urn:a> <urn:p> 1, 2 . <urn:b> <urn:p> 3 ; <urn:q> 4 }`)

	type ops struct {
		get    func() (*types.Statistics, error)
		manage func(mode types.StatisticsAutoGenerationMode) (*string, error)
		del    func() error
	}

	sparqlOps := ops{
		get: func() (*types.Statistics, error) {
			o, err := cl.GetSparqlStatistics(t.Context(), &neptunedata.GetSparqlStatisticsInput{})
			if err != nil {
				return nil, err
			}

			return o.Payload, nil
		},
		manage: func(m types.StatisticsAutoGenerationMode) (*string, error) {
			o, err := cl.ManageSparqlStatistics(t.Context(), &neptunedata.ManageSparqlStatisticsInput{Mode: m})
			if err != nil {
				return nil, err
			}

			if o.Payload == nil {
				return nil, nil //nolint:nilnil // no payload for the toggles
			}

			return o.Payload.StatisticsId, nil
		},
		del: func() error {
			_, err := cl.DeleteSparqlStatistics(t.Context(), &neptunedata.DeleteSparqlStatisticsInput{})

			return err
		},
	}

	before, err := sparqlOps.get()
	require.NoError(t, err)
	assert.True(t, aws.ToBool(before.AutoCompute))
	assert.False(t, aws.ToBool(before.Active))
	assert.Equal(t, "-1", aws.ToString(before.StatisticsId))

	id, err := sparqlOps.manage(types.StatisticsAutoGenerationModeRefresh)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(id))

	after, err := sparqlOps.get()
	require.NoError(t, err)
	assert.True(t, aws.ToBool(after.Active))
	assert.Equal(t, aws.ToString(id), aws.ToString(after.StatisticsId))
	require.NotNil(t, after.SignatureInfo)
	assert.Equal(t, int32(2), aws.ToInt32(after.SignatureInfo.InstanceCount))
	assert.Equal(t, int32(2), aws.ToInt32(after.SignatureInfo.PredicateCount))
	assert.Equal(t, int32(2), aws.ToInt32(after.SignatureInfo.SignatureCount))
	assert.False(t, after.Date.IsZero())

	_, err = sparqlOps.manage(types.StatisticsAutoGenerationModeDisableAutocompute)
	require.NoError(t, err)

	toggled, err := sparqlOps.get()
	require.NoError(t, err)
	assert.False(t, aws.ToBool(toggled.AutoCompute))

	require.NoError(t, sparqlOps.del())

	gone, err := sparqlOps.get()
	require.NoError(t, err)
	assert.False(t, aws.ToBool(gone.Active))
	assert.Equal(t, "-1", aws.ToString(gone.StatisticsId))
}

func TestNeptunedataPropertygraphStatistics(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	cl := dataClient(clusterAddr(c))

	res := sparqlDo(t, c, "POST", "/openCypher", "application/x-www-form-urlencoded", "",
		"query=CREATE+(a:P+{n:1}),+(b:P+{n:2}),+(c:Q)%2C+(a)-%5B:R%5D-%3E(b)")
	require.Equal(t, 200, res.status, res.body)

	_, err := cl.ManagePropertygraphStatistics(t.Context(), &neptunedata.ManagePropertygraphStatisticsInput{
		Mode: types.StatisticsAutoGenerationModeRefresh,
	})
	require.NoError(t, err)

	out, err := cl.GetPropertygraphStatistics(t.Context(), &neptunedata.GetPropertygraphStatisticsInput{})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(out.Payload.Active))
	assert.Equal(t, int32(3), aws.ToInt32(out.Payload.SignatureInfo.InstanceCount))
	assert.Equal(t, int32(2), aws.ToInt32(out.Payload.SignatureInfo.SignatureCount))
	assert.Equal(t, int32(2), aws.ToInt32(out.Payload.SignatureInfo.PredicateCount))

	_, err = cl.ManagePropertygraphStatistics(t.Context(), &neptunedata.ManagePropertygraphStatisticsInput{
		Mode: types.StatisticsAutoGenerationMode("bogus"),
	})
	require.ErrorContains(t, err, "InvalidParameterException")
}

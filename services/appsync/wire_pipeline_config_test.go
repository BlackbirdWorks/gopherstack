package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// TestGetResolver_PipelineConfigDecodesAsObject drives GetResolver through
// the real aws-sdk-go-v2 appsync client for a PIPELINE-kind resolver. The
// real Resolver.PipelineConfig deserializes via
// awsRestjson1_deserializeDocumentPipelineConfig, which requires a JSON
// object with a "functions" member -- gopherstack previously emitted a bare
// array, which fails every real client's decode for any PIPELINE resolver.
func TestGetResolver_PipelineConfigDecodesAsObject(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := appsync.NewHandler(b)
	client := newTestAppsyncClient(t, h)

	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn1", DataSourceName: "ds"})
	require.NoError(t, err)

	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "getItem",
		Kind:           "PIPELINE",
		PipelineConfig: []string{fn.FunctionID},
	})
	require.NoError(t, err)

	out, err := client.GetResolver(t.Context(), &appsyncsdk.GetResolverInput{
		ApiId:     aws.String(api.APIID),
		TypeName:  aws.String("Query"),
		FieldName: aws.String("getItem"),
	})
	require.NoError(t, err, "real SDK client must decode GetResolver without error")
	require.NotNil(t, out.Resolver)
	require.NotNil(t, out.Resolver.PipelineConfig)
	assert.Equal(t, []string{fn.FunctionID}, out.Resolver.PipelineConfig.Functions)
}

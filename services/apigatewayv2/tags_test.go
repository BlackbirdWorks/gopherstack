package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// Test_StageTags proves Stage resources can be tagged/untagged via their own
// nested ARN ("arn:.../apis/{apiId}/stages/{stageName}"), matching real API
// Gateway v2 (Stage responses carry a Tags field). Before this fix
// TagResource/UntagResource/GetTags only recognised single-segment resources
// (apis, vpclinks, domainnames) so a stage ARN silently resolved to the wrong
// resource type and 500'd instead of tagging the stage.
func Test_StageTags(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name:         "api",
		ProtocolType: "HTTP",
	})
	require.NoError(t, err)

	_, err = b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)

	stageARN := "arn:aws:apigateway:us-east-1::/apis/" + api.APIID + "/stages/prod"

	require.NoError(t, b.TagResource(stageARN, map[string]string{"env": "prod", "team": "core"}))

	tags, err := b.GetTags(stageARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "team": "core"}, tags)

	stage, err := b.GetStage(api.APIID, "prod")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "team": "core"}, stage.Tags)

	require.NoError(t, b.UntagResource(stageARN, []string{"team"}))

	tags, err = b.GetTags(stageARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, tags)

	// Not-found cases surface the correct AWS error, not a 500.
	_, err = b.GetTags("arn:aws:apigateway:us-east-1::/apis/" + api.APIID + "/stages/missing")
	require.ErrorIs(t, err, apigatewayv2.ErrStageNotFound)

	_, err = b.GetTags("arn:aws:apigateway:us-east-1::/apis/nonexistent/stages/prod")
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}

package amp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/amp"
	"github.com/blackbirdworks/gopherstack/services/grafana"
)

func TestTagPathsStayWithTheirService(t *testing.T) {
	t.Parallel()

	url := sdktest.Serve(t,
		grafana.NewHandler(grafana.NewInMemoryBackend(t.Context(), testAccount, "us-east-1")),
		amp.NewHandler(amp.NewInMemoryBackend(testAccount, "us-east-1")),
	)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	ampClient := ampsdk.NewFromConfig(cfg, func(o *ampsdk.Options) { o.BaseEndpoint = aws.String(url) })
	grafanaClient := grafanasdk.NewFromConfig(cfg, func(o *grafanasdk.Options) { o.BaseEndpoint = aws.String(url) })

	ws, err := ampClient.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})
	require.NoError(t, err)

	_, err = ampClient.TagResource(
		t.Context(),
		&ampsdk.TagResourceInput{ResourceArn: ws.Arn, Tags: map[string]string{"a": "b"}},
	)
	require.NoError(t, err)

	out, err := ampClient.ListTagsForResource(t.Context(), &ampsdk.ListTagsForResourceInput{ResourceArn: ws.Arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "b"}, out.Tags)

	_, err = grafanaClient.ListTagsForResource(t.Context(), &grafanasdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:grafana:us-east-1:000000000000:/workspaces/g-0000"),
	})
	assert.NotEqual(t, "InternalServerException", apiCode(err))

	lw, err := grafanaClient.ListWorkspaces(t.Context(), &grafanasdk.ListWorkspacesInput{})
	require.NoError(t, err)
	assert.Empty(t, lw.Workspaces)

	aw, err := ampClient.ListWorkspaces(t.Context(), &ampsdk.ListWorkspacesInput{})
	require.NoError(t, err)
	assert.Len(t, aw.Workspaces, 1)
}

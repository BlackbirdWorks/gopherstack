package grafana_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDeleteWorkspaceAPIKey(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	out, err := client.CreateWorkspaceApiKey(t.Context(), &grafanasdk.CreateWorkspaceApiKeyInput{
		WorkspaceId:   aws.String(id),
		KeyName:       aws.String("ci-key"),
		KeyRole:       aws.String("EDITOR"),
		SecondsToLive: aws.Int32(3600),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(out.Key), "the plaintext key is returned exactly once at creation")
	require.Equal(t, "ci-key", aws.ToString(out.KeyName))
	require.Equal(t, id, aws.ToString(out.WorkspaceId))

	del, err := client.DeleteWorkspaceApiKey(t.Context(), &grafanasdk.DeleteWorkspaceApiKeyInput{
		WorkspaceId: aws.String(id),
		KeyName:     aws.String("ci-key"),
	})
	require.NoError(t, err)
	require.Equal(t, "ci-key", aws.ToString(del.KeyName))

	_, err = client.DeleteWorkspaceApiKey(t.Context(), &grafanasdk.DeleteWorkspaceApiKeyInput{
		WorkspaceId: aws.String(id),
		KeyName:     aws.String("ci-key"),
	})
	require.Error(t, err, "deleting an already-deleted key must 404")

	var nfe *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nfe)
}

func TestCreateWorkspaceAPIKey_SecondsToLiveBounds(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	// 31 days, exceeding the real API's 30-day maximum.
	const tooLong = 31 * 24 * 60 * 60

	_, err := client.CreateWorkspaceApiKey(t.Context(), &grafanasdk.CreateWorkspaceApiKeyInput{
		WorkspaceId:   aws.String(id),
		KeyName:       aws.String("too-long"),
		KeyRole:       aws.String("VIEWER"),
		SecondsToLive: aws.Int32(tooLong),
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)
}

package grafana_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/stretchr/testify/require"
)

func TestServiceAccountTokenLifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	sa, err := client.CreateWorkspaceServiceAccount(t.Context(), &grafanasdk.CreateWorkspaceServiceAccountInput{
		WorkspaceId: aws.String(id),
		Name:        aws.String("token-owner"),
		GrafanaRole: types.RoleAdmin,
	})
	require.NoError(t, err)

	tokenIn := &grafanasdk.CreateWorkspaceServiceAccountTokenInput{
		WorkspaceId:      aws.String(id),
		ServiceAccountId: sa.Id,
		Name:             aws.String("ci-token"),
		SecondsToLive:    aws.Int32(3600),
	}
	created, err := client.CreateWorkspaceServiceAccountToken(t.Context(), tokenIn)
	require.NoError(t, err)
	require.NotNil(t, created.ServiceAccountToken)
	require.NotEmpty(t, aws.ToString(created.ServiceAccountToken.Key),
		"the plaintext key is returned exactly once at creation")

	listIn := &grafanasdk.ListWorkspaceServiceAccountTokensInput{
		WorkspaceId:      aws.String(id),
		ServiceAccountId: sa.Id,
	}
	list, err := client.ListWorkspaceServiceAccountTokens(t.Context(), listIn)
	require.NoError(t, err)
	require.Len(t, list.ServiceAccountTokens, 1)
	require.Equal(t, "ci-token", aws.ToString(list.ServiceAccountTokens[0].Name))
	require.NotNil(t, list.ServiceAccountTokens[0].ExpiresAt)

	_, err = client.DeleteWorkspaceServiceAccountToken(t.Context(), &grafanasdk.DeleteWorkspaceServiceAccountTokenInput{
		WorkspaceId:      aws.String(id),
		ServiceAccountId: sa.Id,
		TokenId:          created.ServiceAccountToken.Id,
	})
	require.NoError(t, err)

	after, err := client.ListWorkspaceServiceAccountTokens(t.Context(), listIn)
	require.NoError(t, err)
	require.Empty(t, after.ServiceAccountTokens)
}

func TestDeleteWorkspaceServiceAccount_CascadesTokens(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	sa, err := client.CreateWorkspaceServiceAccount(t.Context(), &grafanasdk.CreateWorkspaceServiceAccountInput{
		WorkspaceId: aws.String(id),
		Name:        aws.String("owner"),
		GrafanaRole: types.RoleAdmin,
	})
	require.NoError(t, err)

	tokenIn := &grafanasdk.CreateWorkspaceServiceAccountTokenInput{
		WorkspaceId:      aws.String(id),
		ServiceAccountId: sa.Id,
		Name:             aws.String("tok"),
		SecondsToLive:    aws.Int32(3600),
	}
	tok, err := client.CreateWorkspaceServiceAccountToken(t.Context(), tokenIn)
	require.NoError(t, err)

	_, err = client.DeleteWorkspaceServiceAccount(t.Context(), &grafanasdk.DeleteWorkspaceServiceAccountInput{
		WorkspaceId:      aws.String(id),
		ServiceAccountId: sa.Id,
	})
	require.NoError(t, err)

	_, err = client.DeleteWorkspaceServiceAccountToken(t.Context(), &grafanasdk.DeleteWorkspaceServiceAccountTokenInput{
		WorkspaceId:      aws.String(id),
		ServiceAccountId: sa.Id,
		TokenId:          tok.ServiceAccountToken.Id,
	})
	require.Error(t, err, "the token must have cascaded away with its owning service account")

	var nfe *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nfe)
}

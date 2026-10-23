package integration_test

import (
	"errors"
	"testing"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func newSecretManagerClient(t *testing.T) *secretmanager.Client {
	t.Helper()

	client, err := secretmanager.NewClient(t.Context(), gcpGRPCOpts(t, "8086")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestIntegration_GCP_SecretManager_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newSecretManagerClient(t)
	ctx := t.Context()
	parent := "projects/" + gcpProject
	id := gcpName("it-secret")

	secret, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: parent, SecretId: id,
		Secret: &secretmanagerpb.Secret{
			Labels: map[string]string{"env": "it"},
			Replication: &secretmanagerpb.Replication{Replication: &secretmanagerpb.Replication_Automatic_{
				Automatic: &secretmanagerpb.Replication_Automatic{},
			}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, parent+"/secrets/"+id, secret.GetName())

	ver, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent: secret.GetName(), Payload: &secretmanagerpb.SecretPayload{Data: []byte("s3cr3t")},
	})
	require.NoError(t, err)
	assert.Equal(t, secret.GetName()+"/versions/1", ver.GetName())

	access, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: secret.GetName() + "/versions/latest",
	})
	require.NoError(t, err)
	assert.Equal(t, "s3cr3t", string(access.GetPayload().GetData()))

	upd, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret:     &secretmanagerpb.Secret{Name: secret.GetName(), Labels: map[string]string{"env": "it2"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "it2", upd.GetLabels()["env"])

	found := false
	it := client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: parent})

	for {
		s, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		found = found || s.GetName() == secret.GetName()
	}

	assert.True(t, found)

	require.NoError(t, client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: secret.GetName()}))
}

func TestIntegration_GCP_SecretManager_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newSecretManagerClient(t)

	_, err := client.AccessSecretVersion(t.Context(), &secretmanagerpb.AccessSecretVersionRequest{
		Name: "projects/" + gcpProject + "/secrets/" + gcpName("absent") + "/versions/latest",
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

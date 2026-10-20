package integration_test

import (
	"encoding/json"
	"testing"

	iam "cloud.google.com/go/iam/admin/apiv1"
	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func newIAMAdminClient(t *testing.T) *iam.IamClient {
	t.Helper()

	client, err := iam.NewIamClient(t.Context(), gcpGRPCOpts(t, "8125")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestIntegration_GCP_IAM_ServiceAccountAndKey(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newIAMAdminClient(t)
	ctx := t.Context()
	accountID := gcpName("it-sa")

	sa, err := client.CreateServiceAccount(ctx, &adminpb.CreateServiceAccountRequest{
		Name:           "projects/" + gcpProject,
		AccountId:      accountID,
		ServiceAccount: &adminpb.ServiceAccount{DisplayName: "IT account"},
	})
	require.NoError(t, err)
	assert.Equal(t, accountID+"@"+gcpProject+".iam.gserviceaccount.com", sa.GetEmail())

	patched, err := client.PatchServiceAccount(ctx, &adminpb.PatchServiceAccountRequest{
		ServiceAccount: &adminpb.ServiceAccount{Name: sa.GetName(), Description: "patched"},
		UpdateMask:     &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "patched", patched.GetDescription())

	key, err := client.CreateServiceAccountKey(ctx, &adminpb.CreateServiceAccountKeyRequest{
		Name:           sa.GetName(),
		PrivateKeyType: adminpb.ServiceAccountPrivateKeyType_TYPE_GOOGLE_CREDENTIALS_FILE,
	})
	require.NoError(t, err)

	var kf struct {
		Type        string `json:"type"`
		ClientEmail string `json:"client_email"`
	}

	require.NoError(t, json.Unmarshal(key.GetPrivateKeyData(), &kf))
	assert.Equal(t, "service_account", kf.Type)
	assert.Equal(t, sa.GetEmail(), kf.ClientEmail)

	keys, err := client.ListServiceAccountKeys(ctx, &adminpb.ListServiceAccountKeysRequest{Name: sa.GetName()})
	require.NoError(t, err)
	assert.NotEmpty(t, keys.GetKeys())

	require.NoError(t, client.DeleteServiceAccountKey(ctx,
		&adminpb.DeleteServiceAccountKeyRequest{Name: key.GetName()}))
	require.NoError(t, client.DeleteServiceAccount(ctx, &adminpb.DeleteServiceAccountRequest{Name: sa.GetName()}))
}

func TestIntegration_GCP_IAM_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newIAMAdminClient(t)

	email := gcpName("absent") + "@" + gcpProject + ".iam.gserviceaccount.com"

	_, err := client.GetServiceAccount(t.Context(), &adminpb.GetServiceAccountRequest{
		Name: "projects/" + gcpProject + "/serviceAccounts/" + email,
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

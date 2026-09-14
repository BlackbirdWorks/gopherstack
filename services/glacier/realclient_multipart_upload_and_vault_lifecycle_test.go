package glacier_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"
	glaciertypes "github.com/aws/aws-sdk-go-v2/service/glacier/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_MultipartUploadAndVaultLifecycle drives glacier's remaining typed-coverage-blind
// ops (gopherstack-n3zi) through the real aws-sdk-go-v2 client:
// AbortMultipartUpload, AbortVaultLock, AddTagsToVault,
// CompleteMultipartUpload, DeleteVaultAccessPolicy, DeleteVaultNotifications,
// GetDataRetrievalPolicy, GetVaultLock, ListProvisionedCapacity,
// PurchaseProvisionedCapacity, SetDataRetrievalPolicy, UploadMultipartPart.
func TestRealClient_MultipartUploadAndVaultLifecycle(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "multipart upload lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newWireTestClient(t)
			ctx := t.Context()
			vaultName := "s21-multipart-vault"

			accID := aws.String("-")

			_, err := client.CreateVault(ctx, &glaciersdk.CreateVaultInput{
				AccountId: accID, VaultName: aws.String(vaultName),
			})
			require.NoError(t, err)

			initOut, err := client.InitiateMultipartUpload(ctx, &glaciersdk.InitiateMultipartUploadInput{
				AccountId: accID, VaultName: aws.String(vaultName), PartSize: aws.String("1048576"),
			})
			require.NoError(t, err)
			uploadID := initOut.UploadId
			require.NotEmpty(t, aws.ToString(uploadID))

			partData := strings.Repeat("a", 1048576)

			uploadOut, err := client.UploadMultipartPart(ctx, &glaciersdk.UploadMultipartPartInput{
				AccountId: accID, VaultName: aws.String(vaultName), UploadId: uploadID,
				Range: aws.String("bytes 0-1048575/*"), Body: strings.NewReader(partData),
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(uploadOut.Checksum))

			completeOut, err := client.CompleteMultipartUpload(ctx, &glaciersdk.CompleteMultipartUploadInput{
				AccountId: accID, VaultName: aws.String(vaultName), UploadId: uploadID,
				ArchiveSize: aws.String("1048576"), Checksum: uploadOut.Checksum,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(completeOut.ArchiveId))
			assert.Equal(t, aws.ToString(uploadOut.Checksum), aws.ToString(completeOut.Checksum))
		}},
		{name: "abort multipart upload", run: func(t *testing.T) {
			t.Helper()

			client := newWireTestClient(t)
			ctx := t.Context()
			vaultName := "s21-abort-vault"
			accID := aws.String("-")

			_, err := client.CreateVault(ctx, &glaciersdk.CreateVaultInput{
				AccountId: accID, VaultName: aws.String(vaultName),
			})
			require.NoError(t, err)

			initOut, err := client.InitiateMultipartUpload(ctx, &glaciersdk.InitiateMultipartUploadInput{
				AccountId: accID, VaultName: aws.String(vaultName), PartSize: aws.String("1048576"),
			})
			require.NoError(t, err)

			_, err = client.AbortMultipartUpload(ctx, &glaciersdk.AbortMultipartUploadInput{
				AccountId: accID, VaultName: aws.String(vaultName), UploadId: initOut.UploadId,
			})
			require.NoError(t, err)

			_, err = client.AbortMultipartUpload(ctx, &glaciersdk.AbortMultipartUploadInput{
				AccountId: accID, VaultName: aws.String(vaultName), UploadId: initOut.UploadId,
			})
			require.Error(t, err, "aborting an already-aborted upload must fail")
		}},
		{name: "vault tags and access policy", run: func(t *testing.T) {
			t.Helper()

			client := newWireTestClient(t)
			ctx := t.Context()
			vaultName := "s21-tags-vault"

			_, err := client.CreateVault(ctx, &glaciersdk.CreateVaultInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.NoError(t, err)

			_, err = client.AddTagsToVault(ctx, &glaciersdk.AddTagsToVaultInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
				Tags: map[string]string{"env": "s21"},
			})
			require.NoError(t, err)

			listTagsOut, err := client.ListTagsForVault(ctx, &glaciersdk.ListTagsForVaultInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.NoError(t, err)
			assert.Equal(t, "s21", listTagsOut.Tags["env"])

			_, err = client.SetVaultAccessPolicy(ctx, &glaciersdk.SetVaultAccessPolicyInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
				Policy: &glaciertypes.VaultAccessPolicy{
					Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				},
			})
			require.NoError(t, err)

			_, err = client.DeleteVaultAccessPolicy(ctx, &glaciersdk.DeleteVaultAccessPolicyInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.NoError(t, err)

			_, err = client.GetVaultAccessPolicy(ctx, &glaciersdk.GetVaultAccessPolicyInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.Error(t, err, "policy was deleted, Get must fail")
		}},
		{name: "vault notifications", run: func(t *testing.T) {
			t.Helper()

			client := newWireTestClient(t)
			ctx := t.Context()
			vaultName := "s21-notifications-vault"

			_, err := client.CreateVault(ctx, &glaciersdk.CreateVaultInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.NoError(t, err)

			_, err = client.SetVaultNotifications(ctx, &glaciersdk.SetVaultNotificationsInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
				VaultNotificationConfig: &glaciertypes.VaultNotificationConfig{
					SNSTopic: aws.String("arn:aws:sns:us-east-1:000000000000:s21-topic"),
					Events:   []string{"ArchiveRetrievalCompleted"},
				},
			})
			require.NoError(t, err)

			_, err = client.DeleteVaultNotifications(ctx, &glaciersdk.DeleteVaultNotificationsInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.NoError(t, err)

			_, err = client.GetVaultNotifications(ctx, &glaciersdk.GetVaultNotificationsInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.Error(t, err, "notification config was deleted, Get must fail")
		}},
		{name: "vault lock", run: func(t *testing.T) {
			t.Helper()

			client := newWireTestClient(t)
			ctx := t.Context()
			vaultName := "s21-lock-vault"

			_, err := client.CreateVault(ctx, &glaciersdk.CreateVaultInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.NoError(t, err)

			_, err = client.InitiateVaultLock(ctx, &glaciersdk.InitiateVaultLockInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
				Policy: &glaciertypes.VaultLockPolicy{
					Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				},
			})
			require.NoError(t, err)

			getOut, err := client.GetVaultLock(ctx, &glaciersdk.GetVaultLockInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.NoError(t, err)
			assert.Equal(t, "InProgress", aws.ToString(getOut.State))
			assert.NotEmpty(t, aws.ToString(getOut.Policy))

			_, err = client.AbortVaultLock(ctx, &glaciersdk.AbortVaultLockInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.NoError(t, err)

			_, err = client.GetVaultLock(ctx, &glaciersdk.GetVaultLockInput{
				AccountId: aws.String("-"), VaultName: aws.String(vaultName),
			})
			require.Error(
				t,
				err,
				"lock was aborted, Get must 404 per api_op_GetVaultLock.go's doc comment",
			)
		}},
		{name: "data retrieval policy", run: func(t *testing.T) {
			t.Helper()

			client := newWireTestClient(t)
			ctx := t.Context()

			_, err := client.SetDataRetrievalPolicy(ctx, &glaciersdk.SetDataRetrievalPolicyInput{
				AccountId: aws.String("-"),
				Policy: &glaciertypes.DataRetrievalPolicy{
					Rules: []glaciertypes.DataRetrievalRule{
						{Strategy: aws.String("BytesPerHour"), BytesPerHour: aws.Int64(1024)},
					},
				},
			})
			require.NoError(t, err)

			getOut, err := client.GetDataRetrievalPolicy(ctx, &glaciersdk.GetDataRetrievalPolicyInput{
				AccountId: aws.String("-"),
			})
			require.NoError(t, err)
			require.NotNil(t, getOut.Policy)
			require.Len(t, getOut.Policy.Rules, 1)
			assert.Equal(t, "BytesPerHour", aws.ToString(getOut.Policy.Rules[0].Strategy))
			assert.Equal(t, int64(1024), aws.ToInt64(getOut.Policy.Rules[0].BytesPerHour))
		}},
		{name: "provisioned capacity", run: func(t *testing.T) {
			t.Helper()

			client := newWireTestClient(t)
			ctx := t.Context()

			purchaseOut, err := client.PurchaseProvisionedCapacity(
				ctx,
				&glaciersdk.PurchaseProvisionedCapacityInput{AccountId: aws.String("-")},
			)
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(purchaseOut.CapacityId))

			listOut, err := client.ListProvisionedCapacity(
				ctx,
				&glaciersdk.ListProvisionedCapacityInput{AccountId: aws.String("-")},
			)
			require.NoError(t, err)
			require.Len(t, listOut.ProvisionedCapacityList, 1)
			assert.Equal(
				t,
				aws.ToString(purchaseOut.CapacityId),
				aws.ToString(listOut.ProvisionedCapacityList[0].CapacityId),
			)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

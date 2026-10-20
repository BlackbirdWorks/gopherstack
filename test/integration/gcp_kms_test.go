package integration_test

import (
	"testing"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newKMSClient(t *testing.T) *kms.KeyManagementClient {
	t.Helper()

	client, err := kms.NewKeyManagementClient(t.Context(), gcpGRPCOpts(t, "8087")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestIntegration_GCP_KMS_EncryptDecrypt(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newKMSClient(t)
	ctx := t.Context()
	location := "projects/" + gcpProject + "/locations/global"

	ring, err := client.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
		Parent: location, KeyRingId: gcpName("it-ring"), KeyRing: &kmspb.KeyRing{},
	})
	require.NoError(t, err)

	got, err := client.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: ring.GetName()})
	require.NoError(t, err)
	assert.Equal(t, ring.GetName(), got.GetName())

	key, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: ring.GetName(), CryptoKeyId: "it-key",
		CryptoKey: &kmspb.CryptoKey{
			Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT,
			Labels:  map[string]string{"env": "it"},
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
				Algorithm: kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
			},
		},
	})
	require.NoError(t, err)

	enc, err := client.Encrypt(ctx, &kmspb.EncryptRequest{Name: key.GetName(), Plaintext: []byte("hello kms")})
	require.NoError(t, err)

	dec, err := client.Decrypt(ctx, &kmspb.DecryptRequest{Name: key.GetName(), Ciphertext: enc.GetCiphertext()})
	require.NoError(t, err)
	assert.Equal(t, "hello kms", string(dec.GetPlaintext()))

	versions := 0
	vit := client.ListCryptoKeyVersions(ctx, &kmspb.ListCryptoKeyVersionsRequest{Parent: key.GetName()})

	for {
		if _, nerr := vit.Next(); nerr != nil {
			break
		}

		versions++
	}

	assert.Equal(t, 1, versions)

	second, err := client.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{
		Parent: key.GetName(), CryptoKeyVersion: &kmspb.CryptoKeyVersion{},
	})
	require.NoError(t, err)

	destroyed, err := client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: second.GetName()})
	require.NoError(t, err)
	assert.Equal(t, kmspb.CryptoKeyVersion_DESTROY_SCHEDULED, destroyed.GetState())
}

func TestIntegration_GCP_KMS_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newKMSClient(t)

	_, err := client.GetKeyRing(t.Context(), &kmspb.GetKeyRingRequest{
		Name: "projects/" + gcpProject + "/locations/global/keyRings/" + gcpName("absent"),
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

package gcpspanner_test

import (
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/services/gcpkms"
	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

const (
	kmsRing  = "projects/" + project + "/locations/us-central1/keyRings/r"
	kmsSym   = kmsRing + "/cryptoKeys/sym"
	kmsSign  = kmsRing + "/cryptoKeys/sign"
	kmsNone  = kmsRing + "/cryptoKeys/none"
	kmsOther = "projects/" + project + "/locations/us-east1/keyRings/r/cryptoKeys/sym"
)

func newKMS(t *testing.T) *gcpkms.InMemoryBackend {
	t.Helper()

	b := gcpkms.NewInMemoryBackend()

	for _, l := range []string{"us-central1", "us-east1"} {
		_, err := b.CreateKeyRing(
			&kmspb.CreateKeyRingRequest{Parent: "projects/" + project + "/locations/" + l, KeyRingId: "r"},
		)
		require.NoError(t, err)

		_, err = b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
			Parent: "projects/" + project + "/locations/" + l + "/keyRings/r", CryptoKeyId: "sym",
			CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
		})
		require.NoError(t, err)
	}

	_, err := b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
		Parent: kmsRing, CryptoKeyId: "sign", CryptoKey: &kmspb.CryptoKey{
			Purpose:         kmspb.CryptoKey_ASYMMETRIC_SIGN,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256},
		},
	})
	require.NoError(t, err)

	return b
}

func TestDatabase_CustomerManagedEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  *databasepb.EncryptionConfig
		name string
		want codes.Code
	}{
		{name: "ok", cfg: &databasepb.EncryptionConfig{KmsKeyName: kmsSym}},
		{name: "key names", cfg: &databasepb.EncryptionConfig{KmsKeyNames: []string{kmsSym}}},
		{name: "missing", cfg: &databasepb.EncryptionConfig{KmsKeyName: kmsNone}, want: codes.InvalidArgument},
		{name: "other region", cfg: &databasepb.EncryptionConfig{KmsKeyName: kmsOther}, want: codes.InvalidArgument},
		{name: "wrong purpose", cfg: &databasepb.EncryptionConfig{KmsKeyName: kmsSign}, want: codes.InvalidArgument},
		{name: "empty", cfg: &databasepb.EncryptionConfig{}, want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) {
				b.UseNoRuntime()
				b.SetKeyChecker(newKMS(t))
			})
			e.createInstance(t, "enc")

			op, err := e.databases.CreateDatabase(t.Context(), &databasepb.CreateDatabaseRequest{
				Parent: parent + "/instances/enc", CreateStatement: "CREATE DATABASE `app`", EncryptionConfig: tt.cfg,
			})
			require.Equal(t, tt.want, status.Code(err))

			if tt.want != codes.OK {
				return
			}

			db, err := await(t, op)
			require.NoError(t, err)
			require.Len(t, db.GetEncryptionInfo(), 1)
			assert.Equal(
				t,
				databasepb.EncryptionInfo_CUSTOMER_MANAGED_ENCRYPTION,
				db.GetEncryptionInfo()[0].GetEncryptionType(),
			)
			assert.Equal(t, kmsSym+"/cryptoKeyVersions/1", db.GetEncryptionInfo()[0].GetKmsKeyVersion())
			assert.Nil(t, db.GetEncryptionInfo()[0].GetEncryptionStatus())
		})
	}
}

func TestDatabase_DisabledKeyReportsStatus(t *testing.T) {
	t.Parallel()

	kb := newKMS(t)
	e := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) {
		b.UseNoRuntime()
		b.SetKeyChecker(kb)
	})
	e.createInstance(t, "enc")

	op, err := e.databases.CreateDatabase(t.Context(), &databasepb.CreateDatabaseRequest{
		Parent: parent + "/instances/enc", CreateStatement: "CREATE DATABASE `app`",
		EncryptionConfig: &databasepb.EncryptionConfig{KmsKeyName: kmsSym},
	})
	require.NoError(t, err)

	_, err = await(t, op)
	require.NoError(t, err)

	_, err = kb.UpdateCryptoKeyVersion(&kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{
			Name:  kmsSym + "/cryptoKeyVersions/1",
			State: kmspb.CryptoKeyVersion_DISABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	})
	require.NoError(t, err)

	db, err := e.databases.GetDatabase(
		t.Context(),
		&databasepb.GetDatabaseRequest{Name: parent + "/instances/enc/databases/app"},
	)
	require.NoError(t, err)
	require.Len(t, db.GetEncryptionInfo(), 1)
	assert.EqualValues(t, codes.FailedPrecondition, db.GetEncryptionInfo()[0].GetEncryptionStatus().GetCode())
}

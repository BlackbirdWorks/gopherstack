package ocivault_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
	oci "github.com/blackbirdworks/gopherstack/services/ocivault"
)

const backupBucket = "vault-backups"

type backupEnv struct {
	*cryptoEnv
	os *ociobjectstorage.InMemoryBackend
}

func newBackupEnv(t *testing.T) *backupEnv {
	t.Helper()

	osb := ociobjectstorage.NewInMemoryBackend()
	_, err := osb.CreateBucket(osb.Namespace(),
		ociobjectstorage.CreateBucketInput{Name: backupBucket, CompartmentID: testCompartment}, "tester")
	require.NoError(t, err)

	e := newEnv(t, envOpts{})
	resp, err := e.vaults.CreateVault(t.Context(), keymanagement.CreateVaultRequest{
		CompartmentId: new(testCompartment), DisplayName: new("backup"),
		VaultType: keymanagement.CreateVaultDetailsVaultTypeVirtualPrivate,
	})
	require.NoError(t, err)
	e.clock.Advance(oci.TransitionDelay)

	ce := &cryptoEnv{m: e.mgmt(t, resp.Vault), c: e.crypto(t, resp.Vault), e: e, v: resp.Vault}
	ce.e.backend.SetBlobStore(osb)

	return &backupEnv{cryptoEnv: ce, os: osb}
}

func (be *backupEnv) bucketLoc(object string) keymanagement.BackupLocation {
	return keymanagement.BackupLocationBucket{
		Namespace: new(be.os.Namespace()), BucketName: new(backupBucket), ObjectName: new(object),
	}
}

func (be *backupEnv) parLoc(t *testing.T, object string) keymanagement.BackupLocation {
	t.Helper()

	par, err := be.os.CreatePAR(be.os.Namespace(), backupBucket, ociobjectstorage.CreatePARInput{
		Name: "p-" + object, AccessType: "ObjectReadWrite", ObjectName: object,
		TimeExpires: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)

	return keymanagement.BackupLocationUri{Uri: new("http://objectstorage.invalid" + par.AccessURI)}
}

func (be *backupEnv) loc(t *testing.T, kind, object string) keymanagement.BackupLocation {
	t.Helper()

	if kind == "par" {
		return be.parLoc(t, object)
	}

	return be.bucketLoc(object)
}

func (be *backupEnv) deleteVault(t *testing.T) {
	t.Helper()

	at := common.SDKTime{Time: be.e.clock.Now().Add(8 * 24 * time.Hour)}
	_, err := be.e.vaults.ScheduleVaultDeletion(t.Context(), keymanagement.ScheduleVaultDeletionRequest{
		VaultId: be.v.Id, TimeOfDeletion: &at,
	})
	require.NoError(t, err)
	be.e.settle()
	be.e.clock.Advance(9 * 24 * time.Hour)

	got, err := be.e.vaults.GetVault(t.Context(), keymanagement.GetVaultRequest{VaultId: be.v.Id})
	require.NoError(t, err)
	require.Equal(t, keymanagement.VaultLifecycleStateDeleted, got.LifecycleState)
}

func md5b64(b []byte) string {
	sum := md5.Sum(b)

	return base64.StdEncoding.EncodeToString(sum[:])
}

func TestBackupDeleteRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		backup  string
		restore string
	}{
		{"bucket then object store", "bucket", "store"},
		{"par then object store", "par", "store"},
		{"bucket then file", "bucket", "file"},
		{"par then file", "par", "file"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			be := newBackupEnv(t)
			ctx := t.Context()
			key := be.e.mkKey(t, be.m, "k", aes256(), "")

			enc, err := be.encrypt(t, key, "round trip", nil, "")
			require.NoError(t, err)

			vLoc, kLoc := be.loc(t, tt.backup, "vault.bk"), be.loc(t, tt.backup, "key.bk")

			bv, err := be.e.vaults.BackupVault(ctx, keymanagement.BackupVaultRequest{
				VaultId: be.v.Id, BackupLocation: vLoc,
			})
			require.NoError(t, err)
			assert.Equal(t, keymanagement.VaultLifecycleStateBackupInProgress, bv.LifecycleState)

			be.e.settle()

			bk, err := be.m.BackupKey(ctx, keymanagement.BackupKeyRequest{
				KeyId: key.Id, BackupLocation: kLoc,
			})
			require.NoError(t, err)
			assert.Equal(t, keymanagement.KeyLifecycleStateBackupInProgress, bk.LifecycleState)

			be.e.settle()

			vBlob, err := be.os.GetBlob(be.os.Namespace(), backupBucket, "vault.bk")
			require.NoError(t, err)

			kBlob, err := be.os.GetBlob(be.os.Namespace(), backupBucket, "key.bk")
			require.NoError(t, err)
			assert.NotContains(t, string(kBlob), "material", "key material must not appear in a backup in the clear")
			assert.NotContains(t, string(vBlob), *be.v.DisplayName)

			be.deleteVault(t)

			rv := restoreVault(t, be, tt.restore, vLoc, vBlob)
			assert.Equal(t, *be.v.Id, *rv.Id)
			require.NotNil(t, rv.RestoredFromVaultId)
			assert.Equal(t, *be.v.Id, *rv.RestoredFromVaultId)
			assert.Equal(t, keymanagement.VaultLifecycleStateRestoring, rv.LifecycleState)

			be.e.settle()

			rk := restoreKey(t, be, tt.restore, kLoc, kBlob)
			assert.Equal(t, *key.Id, *rk.Id)
			assert.Equal(t, *key.Id, *rk.RestoredFromKeyId)

			be.e.settle()

			dec, err := be.decrypt(t, key, *enc.Ciphertext, nil, "")
			require.NoError(t, err)
			assert.Equal(t, b64([]byte("round trip")), *dec.Plaintext)

			again, err := be.encrypt(t, key, "round trip", nil, "")
			require.NoError(t, err)
			assert.NotEmpty(t, *again.Ciphertext)
		})
	}
}

func restoreVault(
	t *testing.T, be *backupEnv, how string, loc keymanagement.BackupLocation, blob []byte,
) keymanagement.Vault {
	t.Helper()

	if how == "file" {
		r, err := be.e.vaults.RestoreVaultFromFile(t.Context(), keymanagement.RestoreVaultFromFileRequest{
			CompartmentId: new(testCompartment), RestoreVaultFromFileDetails: io.NopCloser(bytes.NewReader(blob)),
			ContentLength: new(int64(len(blob))), ContentMd5: new(md5b64(blob)),
		})
		require.NoError(t, err)
		assert.Equal(t, md5b64(blob), *r.OpcContentMd5)

		return r.Vault
	}

	r, err := be.e.vaults.RestoreVaultFromObjectStore(t.Context(), keymanagement.RestoreVaultFromObjectStoreRequest{
		CompartmentId:  new(testCompartment),
		BackupLocation: loc,
	})
	require.NoError(t, err)

	return r.Vault
}

func restoreKey(
	t *testing.T, be *backupEnv, how string, loc keymanagement.BackupLocation, blob []byte,
) keymanagement.Key {
	t.Helper()

	if how == "file" {
		r, err := be.m.RestoreKeyFromFile(t.Context(), keymanagement.RestoreKeyFromFileRequest{
			RestoreKeyFromFileDetails: io.NopCloser(bytes.NewReader(blob)),
			ContentLength:             new(int64(len(blob))), ContentMd5: new(md5b64(blob)),
		})
		require.NoError(t, err)

		return r.Key
	}

	r, err := be.m.RestoreKeyFromObjectStore(t.Context(), keymanagement.RestoreKeyFromObjectStoreRequest{
		BackupLocation: loc,
	})
	require.NoError(t, err)

	return r.Key
}

func TestRestoreRules(t *testing.T) {
	t.Parallel()

	be := newBackupEnv(t)
	ctx := t.Context()
	key := be.e.mkKey(t, be.m, "k", aes256(), "")

	_, err := be.e.vaults.BackupVault(ctx, keymanagement.BackupVaultRequest{
		VaultId: be.v.Id, BackupLocation: be.bucketLoc("v"),
	})
	require.NoError(t, err)
	be.e.settle()

	_, err = be.m.BackupKey(ctx, keymanagement.BackupKeyRequest{
		KeyId: key.Id, BackupLocation: be.bucketLoc("k"),
	})
	require.NoError(t, err)
	be.e.settle()

	vBlob, err := be.os.GetBlob(be.os.Namespace(), backupBucket, "v")
	require.NoError(t, err)

	kBlob, err := be.os.GetBlob(be.os.Namespace(), backupBucket, "k")
	require.NoError(t, err)

	tampered := bytes.Clone(kBlob)
	tampered[len(tampered)-1] ^= 0xff

	tests := []struct {
		call func() error
		name string
		code string
		http int
	}{
		{
			name: "vault still exists",
			call: func() error {
				_, e := be.e.vaults.RestoreVaultFromFile(ctx, keymanagement.RestoreVaultFromFileRequest{
					CompartmentId: new(
						testCompartment,
					),
					RestoreVaultFromFileDetails: io.NopCloser(bytes.NewReader(vBlob)),
				})

				return e
			},
			http: http.StatusConflict, code: "Conflict",
		},
		{
			name: "key still exists",
			call: func() error {
				_, e := be.m.RestoreKeyFromFile(ctx, keymanagement.RestoreKeyFromFileRequest{
					RestoreKeyFromFileDetails: io.NopCloser(bytes.NewReader(kBlob)),
				})

				return e
			},
			http: http.StatusConflict, code: "Conflict",
		},
		{
			name: "tampered file",
			call: func() error {
				_, e := be.m.RestoreKeyFromFile(ctx, keymanagement.RestoreKeyFromFileRequest{
					RestoreKeyFromFileDetails: io.NopCloser(bytes.NewReader(tampered)),
				})

				return e
			},
			http: http.StatusBadRequest, code: "InvalidParameter",
		},
		{
			name: "vault blob is not a key backup",
			call: func() error {
				_, e := be.m.RestoreKeyFromFile(ctx, keymanagement.RestoreKeyFromFileRequest{
					RestoreKeyFromFileDetails: io.NopCloser(bytes.NewReader(vBlob)),
				})

				return e
			},
			http: http.StatusBadRequest, code: "InvalidParameter",
		},
		{
			name: "md5 mismatch",
			call: func() error {
				_, e := be.m.RestoreKeyFromFile(ctx, keymanagement.RestoreKeyFromFileRequest{
					RestoreKeyFromFileDetails: io.NopCloser(
						bytes.NewReader(kBlob),
					),
					ContentMd5: new(md5b64([]byte("x"))),
				})

				return e
			},
			http: http.StatusBadRequest, code: "InvalidDigest",
		},
		{
			name: "missing bucket",
			call: func() error {
				_, e := be.m.BackupKey(ctx, keymanagement.BackupKeyRequest{
					KeyId: key.Id,
					BackupLocation: keymanagement.BackupLocationBucket{
						Namespace: new(be.os.Namespace()), BucketName: new("nope"), ObjectName: new("o"),
					},
				})

				return e
			},
			http: http.StatusBadRequest, code: "InvalidParameter",
		},
		{
			name: "restore object absent",
			call: func() error {
				_, e := be.m.RestoreKeyFromObjectStore(ctx, keymanagement.RestoreKeyFromObjectStoreRequest{
					BackupLocation: be.bucketLoc("absent"),
				})

				return e
			},
			http: http.StatusNotFound, code: "NotAuthorizedOrNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := svcErr(t, tt.call())
			assert.Equal(t, tt.http, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}

	got, err := be.m.GetKey(ctx, keymanagement.GetKeyRequest{KeyId: key.Id})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.KeyLifecycleStateEnabled, got.LifecycleState, "failed backup must not wedge the key")
}

func TestBackupSurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	be := newBackupEnv(t)
	key := be.e.mkKey(t, be.m, "k", aes256(), "")

	_, err := be.m.BackupKey(t.Context(), keymanagement.BackupKeyRequest{
		KeyId: key.Id, BackupLocation: be.bucketLoc("k"),
	})
	require.NoError(t, err)

	kBlob, err := be.os.GetBlob(be.os.Namespace(), backupBucket, "k")
	require.NoError(t, err)

	other := newEnv(t, envOpts{})
	require.NoError(t, other.backend.Restore(t.Context(), be.e.backend.Snapshot(t.Context())))
	other.clock.Advance(time.Hour)

	_, _, rerr := other.backend.RestoreKey("", kBlob)
	require.Error(t, rerr, "the key still exists in the restored state")
	assert.Contains(t, rerr.Error(), "already exists")

	fresh := newEnv(t, envOpts{})
	_, _, rerr = fresh.backend.RestoreKey("", kBlob)
	require.Error(t, rerr)
	assert.Contains(t, rerr.Error(), "corrupt", "a different emulator cannot open the backup")
}

func TestBackupRestrictions(t *testing.T) {
	t.Parallel()

	t.Run("default vault", func(t *testing.T) {
		t.Parallel()

		be := newBackupEnv(t)
		def := be.e.mkVault(t, "default")

		_, err := be.e.vaults.BackupVault(t.Context(), keymanagement.BackupVaultRequest{
			VaultId: def.Id, BackupLocation: be.bucketLoc("d"),
		})
		require.Error(t, err)
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
	})

	t.Run("software key", func(t *testing.T) {
		t.Parallel()

		be := newBackupEnv(t)
		key := be.e.mkKey(t, be.m, "sw", aes256(), keymanagement.CreateKeyDetailsProtectionModeSoftware)

		_, err := be.m.BackupKey(t.Context(), keymanagement.BackupKeyRequest{
			KeyId: key.Id, BackupLocation: be.bucketLoc("sw"),
		})
		require.Error(t, err)
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
	})
}

func TestKeyBackupExcludesDeletedVersions(t *testing.T) {
	t.Parallel()

	be := newBackupEnv(t)
	ctx := t.Context()
	key := be.e.mkKey(t, be.m, "k", aes256(), "")

	v2, err := be.m.CreateKeyVersion(ctx, keymanagement.CreateKeyVersionRequest{KeyId: key.Id})
	require.NoError(t, err)
	be.e.settle()

	list, err := be.m.ListKeyVersions(ctx, keymanagement.ListKeyVersionsRequest{KeyId: key.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 2)

	var old *string

	for _, it := range list.Items {
		if *it.Id != *v2.Id {
			old = it.Id
		}
	}

	_, err = be.m.ScheduleKeyVersionDeletion(ctx, keymanagement.ScheduleKeyVersionDeletionRequest{
		KeyId: key.Id, KeyVersionId: old,
	})
	require.NoError(t, err)
	be.e.settle()

	vLoc, kLoc := be.loc(t, "bucket", "vault.bk"), be.loc(t, "bucket", "key.bk")

	_, err = be.e.vaults.BackupVault(ctx, keymanagement.BackupVaultRequest{VaultId: be.v.Id, BackupLocation: vLoc})
	require.NoError(t, err)
	be.e.settle()

	_, err = be.m.BackupKey(ctx, keymanagement.BackupKeyRequest{KeyId: key.Id, BackupLocation: kLoc})
	require.NoError(t, err)
	be.e.settle()

	vBlob, err := be.os.GetBlob(be.os.Namespace(), backupBucket, "vault.bk")
	require.NoError(t, err)

	kBlob, err := be.os.GetBlob(be.os.Namespace(), backupBucket, "key.bk")
	require.NoError(t, err)

	be.deleteVault(t)
	restoreVault(t, be, "file", vLoc, vBlob)
	be.e.settle()
	restoreKey(t, be, "file", kLoc, kBlob)
	be.e.settle()

	got, err := be.m.ListKeyVersions(ctx, keymanagement.ListKeyVersionsRequest{KeyId: key.Id})
	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Equal(t, *v2.Id, *got.Items[0].Id)
}

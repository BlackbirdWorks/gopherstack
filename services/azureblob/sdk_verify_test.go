package azureblob_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

const otherKey = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+Pw=="

type verifyEnv struct {
	cred *azblob.SharedKeyCredential
	url  string
}

func newVerifyEnv(t *testing.T) *verifyEnv {
	t.Helper()

	h := azureblob.NewHandler(azureblob.NewInMemoryBackend())
	h.VerifySignatures = true
	h.AccountKeys = azureblob.ParseAccountKeys("acct2=" + otherKey)

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cred, err := azblob.NewSharedKeyCredential(testAccount, sdkAccountKey)
	require.NoError(t, err)

	return &verifyEnv{cred: cred, url: srv.URL + "/" + testAccount}
}

func TestVerify_SharedKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		account string
		key     string
		wantErr bool
	}{
		{name: "valid", account: testAccount, key: sdkAccountKey},
		{name: "wrong key", account: testAccount, key: otherKey, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newVerifyEnv(t)

			cred, err := azblob.NewSharedKeyCredential(tt.account, tt.key)
			require.NoError(t, err)

			cc, err := container.NewClientWithSharedKeyCredential(env.url+"/c1", cred, nil)
			require.NoError(t, err)

			_, err = cc.Create(t.Context(), nil)
			if tt.wantErr {
				requireStorageErr(t, err, bloberror.AuthenticationFailed, http.StatusForbidden)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestVerify_AnonymousFollowsPublicAccess(t *testing.T) {
	t.Parallel()

	env := newVerifyEnv(t)
	ctx := t.Context()

	cc, err := container.NewClientWithSharedKeyCredential(env.url+"/priv", env.cred, nil)
	require.NoError(t, err)

	_, err = cc.Create(ctx, nil)
	require.NoError(t, err)

	_, err = cc.NewBlockBlobClient("b").UploadBuffer(ctx, []byte("x"), nil)
	require.NoError(t, err)

	anon, err := blob.NewClientWithNoCredential(env.url+"/priv/b", nil)
	require.NoError(t, err)

	_, err = anon.GetProperties(ctx, nil)
	requireStatus(t, err, http.StatusNotFound)
}

func TestVerify_ServiceSAS(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		expiry   time.Time
		name     string
		blob     string
		wantCode bloberror.Code
		status   int
		perms    sas.BlobPermissions
		tamper   bool
		write    bool
	}{
		{name: "read ok", perms: sas.BlobPermissions{Read: true}, expiry: now.Add(time.Hour), blob: "b"},
		{
			name: "write with read sas", perms: sas.BlobPermissions{Read: true}, expiry: now.Add(time.Hour), blob: "b",
			write: true, wantCode: bloberror.AuthorizationPermissionMismatch, status: http.StatusForbidden,
		},
		{
			name: "write ok", perms: sas.BlobPermissions{Write: true, Create: true},
			expiry: now.Add(time.Hour), blob: "b", write: true,
		},
		{
			name: "expired", perms: sas.BlobPermissions{Read: true}, expiry: now.Add(-time.Hour), blob: "b",
			wantCode: bloberror.AuthenticationFailed, status: http.StatusForbidden,
		},
		{
			name: "other blob", perms: sas.BlobPermissions{Read: true},
			expiry: now.Add(time.Hour), blob: "other",
			wantCode: bloberror.AuthenticationFailed, status: http.StatusForbidden,
		},
		{
			name: "tampered", perms: sas.BlobPermissions{Read: true},
			expiry: now.Add(time.Hour), blob: "b", tamper: true,
			wantCode: bloberror.AuthenticationFailed, status: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newVerifyEnv(t)
			ctx := t.Context()

			cc, err := container.NewClientWithSharedKeyCredential(env.url+"/c1", env.cred, nil)
			require.NoError(t, err)

			_, err = cc.Create(ctx, nil)
			require.NoError(t, err)

			_, err = cc.NewBlockBlobClient("b").UploadBuffer(ctx, []byte("x"), nil)
			require.NoError(t, err)

			_, err = cc.NewBlockBlobClient("other").UploadBuffer(ctx, []byte("y"), nil)
			require.NoError(t, err)

			qp, err := sas.BlobSignatureValues{
				Protocol: sas.ProtocolHTTPSandHTTP, StartTime: now.Add(-time.Minute), ExpiryTime: tt.expiry,
				Permissions: tt.perms.String(), ContainerName: "c1", BlobName: "b",
			}.SignWithSharedKey(env.cred)
			require.NoError(t, err)

			enc := qp.Encode()
			if tt.tamper {
				enc = strings.Replace(enc, "sp=r&", "sp=rwd&", 1)
			}

			bc, err := blob.NewClientWithNoCredential(env.url+"/c1/"+tt.blob+"?"+enc, nil)
			require.NoError(t, err)

			if tt.write {
				bb, nerr := blockblob.NewClientWithNoCredential(env.url+"/c1/"+tt.blob+"?"+enc, nil)
				require.NoError(t, nerr)

				_, berr := bb.UploadBuffer(ctx, []byte("new"), nil)

				if tt.wantCode != "" {
					requireStorageErr(t, berr, tt.wantCode, tt.status)

					return
				}

				require.NoError(t, berr)

				return
			}

			_, err = bc.GetProperties(ctx, nil)
			if tt.wantCode != "" {
				requireStorageErr(t, err, tt.wantCode, tt.status)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestVerify_StoredPolicyAndAccountSAS(t *testing.T) {
	t.Parallel()

	env := newVerifyEnv(t)
	ctx := t.Context()
	now := time.Now().UTC()

	cc, err := container.NewClientWithSharedKeyCredential(env.url+"/c1", env.cred, nil)
	require.NoError(t, err)

	_, err = cc.Create(ctx, nil)
	require.NoError(t, err)

	_, err = cc.NewBlockBlobClient("b").UploadBuffer(ctx, []byte("x"), nil)
	require.NoError(t, err)

	_, err = cc.SetAccessPolicy(ctx, &container.SetAccessPolicyOptions{
		ContainerACL: []*container.SignedIdentifier{{
			ID: new("pol"),
			AccessPolicy: &container.AccessPolicy{
				Permission: new("r"), Start: new(now.Add(-time.Hour)), Expiry: new(now.Add(time.Hour)),
			},
		}},
	})
	require.NoError(t, err)

	pol, err := sas.BlobSignatureValues{Identifier: "pol", ContainerName: "c1"}.SignWithSharedKey(env.cred)
	require.NoError(t, err)

	read, err := blob.NewClientWithNoCredential(env.url+"/c1/b?"+pol.Encode(), nil)
	require.NoError(t, err)

	_, err = read.GetProperties(ctx, nil)
	require.NoError(t, err, "permissions and expiry come from the stored access policy")

	missing, err := sas.BlobSignatureValues{Identifier: "gone", ContainerName: "c1"}.SignWithSharedKey(env.cred)
	require.NoError(t, err)

	read, err = blob.NewClientWithNoCredential(env.url+"/c1/b?"+missing.Encode(), nil)
	require.NoError(t, err)

	_, err = read.GetProperties(ctx, nil)
	requireStorageErr(t, err, bloberror.AuthenticationFailed, http.StatusForbidden)

	acct, err := sas.AccountSignatureValues{
		Protocol: sas.ProtocolHTTPSandHTTP, ExpiryTime: now.Add(time.Hour),
		Permissions:   (&sas.AccountPermissions{Read: true, List: true}).String(),
		ResourceTypes: (&sas.AccountResourceTypes{Container: true, Object: true}).String(),
	}.SignWithSharedKey(env.cred)
	require.NoError(t, err)

	ac, err := azblob.NewClientWithNoCredential(env.url+"/?"+acct.Encode(), nil)
	require.NoError(t, err)

	_, err = ac.ServiceClient().NewContainerClient("c1").NewBlobClient("b").GetProperties(ctx, nil)
	require.NoError(t, err)

	_, err = ac.ServiceClient().NewContainerClient("c1").Delete(ctx, nil)
	requireStorageErr(t, err, bloberror.AuthorizationPermissionMismatch, http.StatusForbidden)
}

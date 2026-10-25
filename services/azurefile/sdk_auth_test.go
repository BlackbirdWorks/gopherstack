package azurefile_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
)

func verifying(h *azurefileHandler) { h.VerifyAuth = true }

func TestSDK_SharedKeyVerification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		verify  bool
		wantErr bool
	}{
		{name: "right key verified", verify: true, key: azureauth.DefaultAccountKey},
		{name: "wrong key rejected", verify: true, key: "d3Jvbmcta2V5LXdyb25nLWtleQ==", wantErr: true},
		{name: "wrong key tolerated when not verifying", key: "d3Jvbmcta2V5LXdyb25nLWtleQ=="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var opts []envOpt
			if tt.verify {
				opts = append(opts, verifying)
			}

			e := newEnv(t, opts...)

			cred, err := service.NewSharedKeyCredential(azureauth.DefaultAccountName, tt.key)
			require.NoError(t, err)

			svc, err := service.NewClientWithSharedKeyCredential(e.url+"/"+azureauth.DefaultAccountName+"/", cred, nil)
			require.NoError(t, err)

			sc := svc.NewShareClient("keyed")
			_, err = sc.Create(ctx(t), nil)

			if tt.wantErr {
				requireCode(t, err, fileerror.AuthenticationFailed)

				return
			}

			require.NoError(t, err)

			fc := sc.NewRootDirectoryClient().NewFileClient("f.txt")
			_, err = fc.Create(ctx(t), 4, nil)
			require.NoError(t, err)
			upload(t, fc, 0, "data")
			assert.Equal(t, "data", downloadAll(t, fc))
		})
	}
}

func TestSDK_ServiceSAS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, e *env, sc *share.Client)
		name  string
	}{
		{name: "share sas lists and writes", check: func(t *testing.T, _ *env, sc *share.Client) {
			t.Helper()

			url, err := sc.GetSASURL(
				sas.SharePermissions{Read: true, Create: true, Write: true, Delete: true, List: true},
				time.Now().Add(time.Hour),
				nil,
			)
			require.NoError(t, err)

			viaSAS, err := share.NewClientWithNoCredential(url, nil)
			require.NoError(t, err)

			fc := viaSAS.NewRootDirectoryClient().NewFileClient("sas.txt")
			_, err = fc.Create(ctx(t), 3, nil)
			require.NoError(t, err)
			upload(t, fc, 0, "sas")

			props, err := viaSAS.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.NotEmpty(t, props.ETag)
		}},
		{name: "file sas reads", check: func(t *testing.T, _ *env, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "f.txt", 3)
			upload(t, fc, 0, "xyz")

			url, err := fc.GetSASURL(sas.FilePermissions{Read: true}, time.Now().Add(time.Hour), nil)
			require.NoError(t, err)

			viaSAS, err := file.NewClientWithNoCredential(url, nil)
			require.NoError(t, err)
			assert.Equal(t, "xyz", downloadAll(t, viaSAS))
		}},
		{name: "tampered signature", check: func(t *testing.T, _ *env, sc *share.Client) {
			t.Helper()

			url, err := sc.GetSASURL(sas.SharePermissions{Read: true}, time.Now().Add(time.Hour), nil)
			require.NoError(t, err)

			viaSAS, err := share.NewClientWithNoCredential(strings.Replace(url, "sig=", "sig=AAAA", 1), nil)
			require.NoError(t, err)

			_, err = viaSAS.GetProperties(ctx(t), nil)
			requireCode(t, err, fileerror.AuthenticationFailed)
		}},
		{name: "expired", check: func(t *testing.T, _ *env, sc *share.Client) {
			t.Helper()

			url, err := sc.GetSASURL(sas.SharePermissions{Read: true}, time.Now().Add(-time.Hour), nil)
			require.NoError(t, err)

			viaSAS, err := share.NewClientWithNoCredential(url, nil)
			require.NoError(t, err)

			_, err = viaSAS.GetProperties(ctx(t), nil)
			requireCode(t, err, fileerror.AuthenticationFailed)
		}},
		{name: "sas for another share", check: func(t *testing.T, e *env, sc *share.Client) {
			t.Helper()

			_, err := e.svc.NewShareClient("othershare").Create(ctx(t), nil)
			require.NoError(t, err)

			url, err := sc.GetSASURL(sas.SharePermissions{Read: true}, time.Now().Add(time.Hour), nil)
			require.NoError(t, err)

			swapped := strings.Replace(url, "/sasshare?", "/othershare?", 1)
			require.NotEqual(t, url, swapped)

			viaSAS, err := share.NewClientWithNoCredential(swapped, nil)
			require.NoError(t, err)

			_, err = viaSAS.GetProperties(ctx(t), nil)
			requireCode(t, err, fileerror.AuthenticationFailed)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, verifying)
			sc := e.svc.NewShareClient("sasshare")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			tt.check(t, e, sc)
		})
	}
}

func TestSDK_AccountSAS(t *testing.T) {
	t.Parallel()

	e := newEnv(t, verifying)

	url, err := e.svc.GetSASURL(
		sas.AccountResourceTypes{Service: true, Container: true, Object: true},
		sas.AccountPermissions{Read: true, Create: true, Write: true, List: true, Delete: true},
		time.Now().Add(time.Hour), nil)
	require.NoError(t, err)

	viaSAS, err := service.NewClientWithNoCredential(url, nil)
	require.NoError(t, err)

	_, err = viaSAS.CreateShare(ctx(t), "acctsas", nil)
	require.NoError(t, err)

	assert.Equal(t, []string{"acctsas"}, listNames(t, e, nil))
}

func TestBearerAndAnonymous(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hdr  map[string]string
		name string
		want int
	}{
		{
			name: "bearer accepted",
			hdr:  map[string]string{"Authorization": "Bearer token", "x-ms-version": "2026-02-06"},
			want: http.StatusOK,
		},
		{name: "anonymous accepted", want: http.StatusOK},
		{
			name: "malformed shared key rejected",
			hdr:  map[string]string{"Authorization": "SharedKey broken"},
			want: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, verifying)
			resp := rawDo(t, http.MethodGet, e.url+"/devstoreaccount1?comp=list", "", tt.hdr)
			assert.Equal(t, tt.want, resp.StatusCode)
			assert.Equal(t, "2026-02-06", resp.Header.Get("X-Ms-Version"))
			assert.NotEmpty(t, resp.Header.Get("X-Ms-Request-Id"))
		})
	}
}

func TestHostRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_, err := e.svc.NewShareClient("pathstyle").Create(ctx(t), nil)
	require.NoError(t, err)

	host := azureauth.DefaultAccountName + ".file.core.windows.net"

	resp := rawDo(t, http.MethodGet, e.url+"/?comp=list", host, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = rawDo(t, http.MethodPut, e.url+"/hostshare?restype=share", host, nil)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	resp = rawDo(t, http.MethodGet, e.url+"/hostshare?restype=share", host, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, []string{"hostshare", "pathstyle"}, listNames(t, e, nil), "both styles share one backend")

	resp = rawDo(t, http.MethodGet, e.url+"/pathstyle?restype=share", "otheracct.file.core.windows.net", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "accounts are isolated by host")
	assert.Equal(t, "ShareNotFound", resp.Header.Get("X-Ms-Error-Code"))
}

func TestUnroutableRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{name: "no account", method: http.MethodGet, path: "/", want: http.StatusBadRequest},
		{name: "account level unknown", method: http.MethodGet, path: "/acct?foo=bar", want: http.StatusBadRequest},
		{
			name:   "unknown restype",
			method: http.MethodGet,
			path:   "/acct/share?restype=bogus",
			want:   http.StatusBadRequest,
		},
		{
			name:   "unknown share comp",
			method: http.MethodPost,
			path:   "/acct/share?restype=share&comp=nope",
			want:   http.StatusBadRequest,
		},
		{
			name:   "hard link unsupported",
			method: http.MethodPut,
			path:   "/acct/share/f?restype=hardlink",
			want:   http.StatusBadRequest,
		},
		{name: "file op on share root", method: http.MethodGet, path: "/acct/share", want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			resp := rawDo(t, tt.method, e.url+tt.path, "", nil)
			assert.Equal(t, tt.want, resp.StatusCode)
			assert.Equal(t, "InvalidUri", resp.Header.Get("X-Ms-Error-Code"))
		})
	}
}

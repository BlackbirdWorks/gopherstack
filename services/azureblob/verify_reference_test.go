package azureblob_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

//nolint:gochecknoglobals // shared test client
var testHTTP = httpclient.New(30 * time.Second)

type headerPolicy struct{ name, value string }

func (p headerPolicy) Do(req *policy.Request) (*http.Response, error) {
	req.Raw().Header.Set(p.name, p.value)

	return req.Next()
}

func newVerifyEnvWith(t *testing.T, mut func(*azureblob.Handler)) *verifyEnv {
	t.Helper()

	h := azureblob.NewHandler(azureblob.NewInMemoryBackend())
	h.VerifySignatures = true
	h.AccountKeys = azureblob.ParseAccountKeys("acct2=" + sdkAccountKey)

	if mut != nil {
		mut(h)
	}

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cred, err := azblob.NewSharedKeyCredential(testAccount, sdkAccountKey)
	require.NoError(t, err)

	return &verifyEnv{cred: cred, url: srv.URL + "/" + testAccount}
}

func TestVerify_SharedKeyRequestRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mut     func(*azureblob.Handler)
		path    string
		extra   []policy.Policy
		wantErr bool
	}{
		{name: "baseline", path: "/c1"},
		{
			name: "header account differs from addressed account", path: "/../acct2/c1",
			wantErr: true,
		},
		{
			name:    "date outside window",
			mut:     func(h *azureblob.Handler) { h.SetClock(func() time.Time { return time.Now().Add(time.Hour) }) },
			path:    "/c1",
			wantErr: true,
		},
		{
			name:  "date and x-ms-date both sent",
			path:  "/c1",
			extra: []policy.Policy{headerPolicy{"Date", "Mon, 02 Jan 2006 15:04:05 GMT"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newVerifyEnvWith(t, tt.mut)
			url := env.url + tt.path

			if strings.HasPrefix(tt.path, "/../") {
				url = strings.TrimSuffix(env.url, "/"+testAccount) + "/acct2/c1"
			}

			opts := &container.ClientOptions{}
			opts.PerCallPolicies = tt.extra

			cc, err := container.NewClientWithSharedKeyCredential(url, env.cred, opts)
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

func liteSignature(t *testing.T, method, resource, xmsDate string) string {
	t.Helper()

	raw, err := base64.StdEncoding.DecodeString(sdkAccountKey)
	require.NoError(t, err)

	mac := hmac.New(sha256.New, raw)
	mac.Write([]byte(method + "\n\n\n\nx-ms-date:" + xmsDate + "\nx-ms-version:2021-08-06\n" + resource))

	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func TestVerify_SharedKeyLiteSignsOnlyComp(t *testing.T) {
	t.Parallel()

	env := newVerifyEnvWith(t, nil)
	now := time.Now().UTC().Format(http.TimeFormat)

	do := func(method, target, resource string) int {
		req, err := http.NewRequestWithContext(t.Context(), method, target, nil)
		require.NoError(t, err)

		req.Header.Set("X-Ms-Date", now)
		req.Header.Set("X-Ms-Version", "2021-08-06")
		req.Header.Set("Authorization", "SharedKeyLite "+testAccount+":"+liteSignature(t, method, resource, now))

		resp, err := testHTTP.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		return resp.StatusCode
	}

	assert.Equal(t, http.StatusCreated, do(http.MethodPut, env.url+"/lite?restype=container",
		"/"+testAccount+"/"+testAccount+"/lite"))
	assert.Equal(t, http.StatusOK, do(http.MethodGet, env.url+"/lite?restype=container&comp=metadata&timeout=5",
		"/"+testAccount+"/"+testAccount+"/lite?comp=metadata"))
}

func TestVerify_RejectsUnknownAuthorizationScheme(t *testing.T) {
	t.Parallel()

	env := newVerifyEnvWith(t, nil)

	tests := []struct {
		name   string
		header string
		want   int
	}{
		{name: "junk scheme", header: "Foo bar", want: http.StatusForbidden},
		{name: "bearer passes", header: "Bearer token", want: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
				env.url+"/nope?restype=container", nil)
			require.NoError(t, err)

			req.Header.Set("Authorization", tt.header)

			resp, err := testHTTP.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, tt.want, resp.StatusCode)
		})
	}
}

func TestVerify_SASRules(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		vals     sas.BlobSignatureValues
		name     string
		wantCode bloberror.Code
		existing bool
		wantOK   bool
	}{
		{
			name:     "future version",
			vals:     sas.BlobSignatureValues{Version: "2099-01-01", Permissions: "cw"},
			wantCode: bloberror.AuthenticationFailed,
		},
		{
			name:     "garbage version",
			vals:     sas.BlobSignatureValues{Version: "zzz", Permissions: "cw"},
			wantCode: bloberror.AuthenticationFailed,
		},
		{
			name:     "encryption scope before 2020-12-06",
			vals:     sas.BlobSignatureValues{Version: "2020-10-02", EncryptionScope: "scope", Permissions: "cw"},
			wantCode: bloberror.AuthenticationFailed,
		},
		{name: "2020-12-06 ok", vals: sas.BlobSignatureValues{Version: "2020-12-06", Permissions: "cw"}, wantOK: true},
		{
			name:     "add only cannot put blob",
			vals:     sas.BlobSignatureValues{Permissions: "a"},
			wantCode: bloberror.AuthorizationPermissionMismatch,
		},
		{name: "create puts new blob", vals: sas.BlobSignatureValues{Permissions: "c"}, wantOK: true},
		{
			name: "create cannot overwrite", vals: sas.BlobSignatureValues{Permissions: "c"}, existing: true,
			wantCode: bloberror.AuthorizationPermissionMismatch,
		},
		{name: "write overwrites", vals: sas.BlobSignatureValues{Permissions: "w"}, existing: true, wantOK: true},
		{
			name:     "http only protocol",
			vals:     sas.BlobSignatureValues{Protocol: "http", Permissions: "cw"},
			wantCode: bloberror.AuthenticationFailed,
		},
		{
			name:     "https only over http",
			vals:     sas.BlobSignatureValues{Protocol: sas.Protocol("https"), Permissions: "cw"},
			wantCode: bloberror.AuthenticationFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newVerifyEnvWith(t, nil)
			ctx := t.Context()

			cc, err := container.NewClientWithSharedKeyCredential(env.url+"/c1", env.cred, nil)
			require.NoError(t, err)
			_, err = cc.Create(ctx, nil)
			require.NoError(t, err)

			if tt.existing {
				_, err = cc.NewBlockBlobClient("b").UploadBuffer(ctx, []byte("x"), nil)
				require.NoError(t, err)
			}

			tt.vals.ExpiryTime = now.Add(time.Hour)
			tt.vals.ContainerName, tt.vals.BlobName = "c1", "b"

			if tt.vals.Protocol == "" {
				tt.vals.Protocol = sas.ProtocolHTTPSandHTTP
			}

			qp, err := tt.vals.SignWithSharedKey(env.cred)
			require.NoError(t, err)

			bb, err := blockblob.NewClientWithNoCredential(env.url+"/c1/b?"+qp.Encode(), nil)
			require.NoError(t, err)

			_, err = bb.UploadBuffer(ctx, []byte("new"), nil)
			if tt.wantOK {
				require.NoError(t, err)

				return
			}

			requireStorageErr(t, err, tt.wantCode, http.StatusForbidden)
		})
	}
}

func TestVerify_StoredPolicyFieldConflict(t *testing.T) {
	t.Parallel()

	env := newVerifyEnvWith(t, nil)
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
			ID:           new("pol"),
			AccessPolicy: &container.AccessPolicy{Permission: new("r"), Expiry: new(now.Add(time.Hour))},
		}},
	})
	require.NoError(t, err)

	qp, err := sas.BlobSignatureValues{
		Identifier: "pol", ContainerName: "c1", Permissions: "r", ExpiryTime: now.Add(time.Hour),
	}.SignWithSharedKey(env.cred)
	require.NoError(t, err)

	bc, err := container.NewClientWithNoCredential(env.url+"/c1?"+qp.Encode(), nil)
	require.NoError(t, err)

	_, err = bc.NewBlobClient("b").GetProperties(ctx, nil)
	requireStatus(t, err, http.StatusBadRequest)
}

func TestVerify_AccountSASOperationTable(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		rt    *sas.AccountResourceTypes
		call  func(ctx context.Context, c *azblob.Client) error
		name  string
		perms sas.AccountPermissions
		want  int
	}{
		{
			name: "object type cannot list blobs", perms: sas.AccountPermissions{Read: true, List: true},
			rt: &sas.AccountResourceTypes{Object: true},
			call: func(ctx context.Context, c *azblob.Client) error {
				_, err := c.ServiceClient().NewContainerClient("c1").NewListBlobsFlatPager(nil).NextPage(ctx)

				return err
			},
			want: http.StatusForbidden,
		},
		{
			name: "container type cannot read a blob", perms: sas.AccountPermissions{Read: true, List: true},
			rt: &sas.AccountResourceTypes{Container: true},
			call: func(ctx context.Context, c *azblob.Client) error {
				_, err := c.ServiceClient().NewContainerClient("c1").NewBlobClient("ap").GetProperties(ctx, nil)

				return err
			},
			want: http.StatusForbidden,
		},
		{
			name:  "set container acl is shared key only",
			perms: sas.AccountPermissions{Read: true, Write: true, Create: true, Delete: true, List: true},
			call: func(ctx context.Context, c *azblob.Client) error {
				_, err := c.ServiceClient().NewContainerClient("c1").SetAccessPolicy(ctx, nil)

				return err
			},
			want: http.StatusForbidden,
		},
		{
			name:  "get container acl is shared key only",
			perms: sas.AccountPermissions{Read: true, Write: true, Create: true, Delete: true, List: true},
			call: func(ctx context.Context, c *azblob.Client) error {
				_, err := c.ServiceClient().NewContainerClient("c1").GetAccessPolicy(ctx, nil)

				return err
			},
			want: http.StatusForbidden,
		},
		{
			name:  "update cannot append",
			perms: sas.AccountPermissions{Update: true},
			call: func(ctx context.Context, c *azblob.Client) error {
				_, err := c.ServiceClient().NewContainerClient("c1").NewAppendBlobClient("ap").
					AppendBlock(ctx, body("x"), &appendblob.AppendBlockOptions{})

				return err
			},
			want: http.StatusForbidden,
		},
		{
			name:  "add appends",
			perms: sas.AccountPermissions{Add: true},
			call: func(ctx context.Context, c *azblob.Client) error {
				_, err := c.ServiceClient().NewContainerClient("c1").NewAppendBlobClient("ap").
					AppendBlock(ctx, body("x"), &appendblob.AppendBlockOptions{})

				return err
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newVerifyEnvWith(t, nil)
			ctx := t.Context()

			cc, err := container.NewClientWithSharedKeyCredential(env.url+"/c1", env.cred, nil)
			require.NoError(t, err)
			_, err = cc.Create(ctx, nil)
			require.NoError(t, err)
			_, err = cc.NewAppendBlobClient("ap").Create(ctx, nil)
			require.NoError(t, err)

			acct, err := sas.AccountSignatureValues{
				Protocol: sas.ProtocolHTTPSandHTTP, ExpiryTime: now.Add(time.Hour),
				Permissions:   tt.perms.String(),
				ResourceTypes: resourceTypesOf(tt.rt).String(),
			}.SignWithSharedKey(env.cred)
			require.NoError(t, err)

			ac, err := azblob.NewClientWithNoCredential(env.url+"/?"+acct.Encode(), nil)
			require.NoError(t, err)

			err = tt.call(ctx, ac)
			if tt.want == 0 {
				require.NoError(t, err)

				return
			}

			requireStatus(t, err, tt.want)
		})
	}
}

func TestVerify_AnonymousPublicBlobReads(t *testing.T) {
	t.Parallel()

	env := newVerifyEnvWith(t, nil)
	ctx := t.Context()

	cc, err := container.NewClientWithSharedKeyCredential(env.url+"/pub", env.cred, nil)
	require.NoError(t, err)
	_, err = cc.Create(ctx, &container.CreateOptions{Access: new(container.PublicAccessTypeBlob)})
	require.NoError(t, err)
	_, err = cc.NewBlockBlobClient("b").UploadBuffer(ctx, []byte("x"), nil)
	require.NoError(t, err)

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{name: "block list committed", query: "?comp=blocklist", want: http.StatusOK},
		{name: "block list explicit committed", query: "?comp=blocklist&blocklisttype=committed", want: http.StatusOK},
		{name: "block list uncommitted", query: "?comp=blocklist&blocklisttype=all", want: http.StatusNotFound},
		{name: "tags stay private", query: "?comp=tags", want: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, rerr := http.NewRequestWithContext(t.Context(), http.MethodGet, env.url+"/pub/b"+tt.query, nil)
			require.NoError(t, rerr)

			resp, derr := testHTTP.Do(req)
			require.NoError(t, derr)
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, tt.want, resp.StatusCode)
		})
	}
}

func TestVerify_CopySourceNeedsOwnAuthorization(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name      string
		srcPerms  string
		srcPublic bool
		wantOK    bool
	}{
		{name: "private source without sas", wantOK: false},
		{name: "source sas with read", srcPerms: "r", wantOK: true},
		{name: "source sas without read", srcPerms: "w", wantOK: false},
		{name: "public source", srcPublic: true, wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newVerifyEnvWith(t, nil)
			ctx := t.Context()

			opts := &container.CreateOptions{}
			if tt.srcPublic {
				opts.Access = new(container.PublicAccessTypeBlob)
			}

			src, err := container.NewClientWithSharedKeyCredential(env.url+"/src", env.cred, nil)
			require.NoError(t, err)
			_, err = src.Create(ctx, opts)
			require.NoError(t, err)
			_, err = src.NewBlockBlobClient("b").UploadBuffer(ctx, []byte("x"), nil)
			require.NoError(t, err)

			dst, err := container.NewClientWithSharedKeyCredential(env.url+"/dst", env.cred, nil)
			require.NoError(t, err)
			_, err = dst.Create(ctx, nil)
			require.NoError(t, err)

			srcURL := env.url + "/src/b"
			if tt.srcPerms != "" {
				qp, serr := sas.BlobSignatureValues{
					Protocol: sas.ProtocolHTTPSandHTTP, ExpiryTime: now.Add(time.Hour),
					Permissions: tt.srcPerms, ContainerName: "src", BlobName: "b",
				}.SignWithSharedKey(env.cred)
				require.NoError(t, serr)

				srcURL += "?" + qp.Encode()
			}

			dqp, err := sas.BlobSignatureValues{
				Protocol: sas.ProtocolHTTPSandHTTP, ExpiryTime: now.Add(time.Hour),
				Permissions: "cw", ContainerName: "dst", BlobName: "copy",
			}.SignWithSharedKey(env.cred)
			require.NoError(t, err)

			bb, err := blockblob.NewClientWithNoCredential(env.url+"/dst/copy?"+dqp.Encode(), nil)
			require.NoError(t, err)

			_, err = bb.StartCopyFromURL(ctx, srcURL, nil)
			if tt.wantOK {
				require.NoError(t, err)

				return
			}

			requireStorageErr(t, err, bloberror.CannotVerifyCopySource, http.StatusNotFound)
		})
	}
}

func TestCopyFromURL_DeclaredLengthOverCap(t *testing.T) {
	t.Parallel()

	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "300000000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("tiny"))
	}))
	t.Cleanup(src.Close)

	env := newVerifyEnvWith(t, nil)
	ctx := t.Context()

	dst, err := container.NewClientWithSharedKeyCredential(env.url+"/dst", env.cred, nil)
	require.NoError(t, err)
	_, err = dst.Create(ctx, nil)
	require.NoError(t, err)

	_, err = dst.NewBlockBlobClient("copy").StartCopyFromURL(ctx, src.URL+"/other/c/b", nil)
	requireStorageErr(t, err, bloberror.InvalidHeaderValue, http.StatusBadRequest)
}

func TestVerify_SASScopeAndWindow(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name   string
		vals   sas.BlobSignatureValues
		path   string
		wantOK bool
	}{
		{
			name: "container sas lists", vals: sas.BlobSignatureValues{Permissions: "rl"},
			path: "/c1?restype=container&comp=list", wantOK: true,
		},
		{
			name: "container sas reads a blob", vals: sas.BlobSignatureValues{Permissions: "rl"},
			path: "/c1/b", wantOK: true,
		},
		{name: "container sas other container", vals: sas.BlobSignatureValues{Permissions: "rl"}, path: "/c2/b"},
		{
			name: "blob sas cannot list", path: "/c1?restype=container&comp=list",
			vals: sas.BlobSignatureValues{Permissions: "rl", BlobName: "b"},
		},
		{
			name: "not yet valid", path: "/c1/b",
			vals: sas.BlobSignatureValues{Permissions: "r", StartTime: now.Add(time.Hour)},
		},
		{
			name: "ip outside range", path: "/c1/b",
			vals: sas.BlobSignatureValues{Permissions: "r", IPRange: sas.IPRange{Start: net.IPv4(10, 0, 0, 1)}},
		},
		{
			name: "ip inside range", path: "/c1/b", wantOK: true,
			vals: sas.BlobSignatureValues{
				Permissions: "r", IPRange: sas.IPRange{Start: net.IPv4(127, 0, 0, 0), End: net.IPv4(127, 0, 0, 5)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newVerifyEnvWith(t, nil)
			ctx := t.Context()

			for _, c := range []string{"c1", "c2"} {
				cc, err := container.NewClientWithSharedKeyCredential(env.url+"/"+c, env.cred, nil)
				require.NoError(t, err)
				_, err = cc.Create(ctx, nil)
				require.NoError(t, err)
				_, err = cc.NewBlockBlobClient("b").UploadBuffer(ctx, []byte("x"), nil)
				require.NoError(t, err)
			}

			vals := tt.vals
			vals.ExpiryTime, vals.ContainerName = now.Add(2*time.Hour), "c1"
			vals.Protocol = sas.ProtocolHTTPSandHTTP

			qp, err := vals.SignWithSharedKey(env.cred)
			require.NoError(t, err)

			sep := "?"
			if strings.Contains(tt.path, "?") {
				sep = "&"
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, env.url+tt.path+sep+qp.Encode(), nil)
			require.NoError(t, err)

			resp, err := testHTTP.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			if tt.wantOK {
				assert.Equal(t, http.StatusOK, resp.StatusCode)

				return
			}

			assert.Equal(t, http.StatusForbidden, resp.StatusCode)
		})
	}
}

func TestVerify_SnapshotSASDoesNotOpenTheBaseBlob(t *testing.T) {
	t.Parallel()

	env := newVerifyEnvWith(t, nil)
	ctx := t.Context()
	now := time.Now().UTC()

	cc, err := container.NewClientWithSharedKeyCredential(env.url+"/c1", env.cred, nil)
	require.NoError(t, err)
	_, err = cc.Create(ctx, nil)
	require.NoError(t, err)

	bb := cc.NewBlockBlobClient("b")
	_, err = bb.UploadBuffer(ctx, []byte("x"), nil)
	require.NoError(t, err)

	snap, err := bb.CreateSnapshot(ctx, nil)
	require.NoError(t, err)

	ts, err := time.Parse("2006-01-02T15:04:05.9999999Z", *snap.Snapshot)
	require.NoError(t, err)

	qp, err := sas.BlobSignatureValues{
		Protocol: sas.ProtocolHTTPSandHTTP, ExpiryTime: now.Add(time.Hour), Permissions: "r",
		ContainerName: "c1", BlobName: "b", SnapshotTime: ts,
	}.SignWithSharedKey(env.cred)
	require.NoError(t, err)

	get := func(target string) int {
		req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		require.NoError(t, rerr)

		resp, derr := testHTTP.Do(req)
		require.NoError(t, derr)
		require.NoError(t, resp.Body.Close())

		return resp.StatusCode
	}

	assert.Equal(t, http.StatusOK, get(env.url+"/c1/b?snapshot="+*snap.Snapshot+"&"+qp.Encode()))
	assert.Equal(t, http.StatusForbidden, get(env.url+"/c1/b?"+qp.Encode()))
}

func TestVerify_AnonymousWritesAndListingFollowAccessLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		access container.PublicAccessType
		method string
		path   string
		want   int
	}{
		{
			name: "anonymous put", access: container.PublicAccessTypeContainer, method: http.MethodPut,
			path: "/p/new", want: http.StatusUnauthorized,
		},
		{
			name: "container level lists", access: container.PublicAccessTypeContainer, method: http.MethodGet,
			path: "/p?restype=container&comp=list", want: http.StatusOK,
		},
		{
			name: "blob level cannot list", access: container.PublicAccessTypeBlob, method: http.MethodGet,
			path: "/p?restype=container&comp=list", want: http.StatusNotFound,
		},
		{
			name: "blob level reads blob", access: container.PublicAccessTypeBlob, method: http.MethodGet,
			path: "/p/b", want: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newVerifyEnvWith(t, nil)
			ctx := t.Context()

			cc, err := container.NewClientWithSharedKeyCredential(env.url+"/p", env.cred, nil)
			require.NoError(t, err)
			_, err = cc.Create(ctx, &container.CreateOptions{Access: &tt.access})
			require.NoError(t, err)
			_, err = cc.NewBlockBlobClient("b").UploadBuffer(ctx, []byte("x"), nil)
			require.NoError(t, err)

			req, err := http.NewRequestWithContext(ctx, tt.method, env.url+tt.path, nil)
			require.NoError(t, err)

			resp, err := testHTTP.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, tt.want, resp.StatusCode)
		})
	}
}

func resourceTypesOf(rt *sas.AccountResourceTypes) *sas.AccountResourceTypes {
	if rt != nil {
		return rt
	}

	return &sas.AccountResourceTypes{Container: true, Object: true}
}

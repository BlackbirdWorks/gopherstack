package azurefile_test

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fileViaSAS(t *testing.T, sc *share.Client, name string, p sas.FilePermissions) *file.Client {
	t.Helper()

	fc := sc.NewRootDirectoryClient().NewFileClient(name)
	u, err := fc.GetSASURL(p, time.Now().Add(time.Hour), nil)
	require.NoError(t, err)

	viaSAS, err := file.NewClientWithNoCredential(u, nil)
	require.NoError(t, err)

	return viaSAS
}

func TestSDK_SASPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, fc *file.Client) error
		name string
		want fileerror.Code
		perm sas.FilePermissions
	}{
		{
			name: "read cannot write", perm: sas.FilePermissions{Read: true}, want: "AuthorizationPermissionMismatch",
			run: func(t *testing.T, fc *file.Client) error {
				t.Helper()
				_, err := fc.UploadRange(ctx(t), 0, newReadSeekCloser("ab"), nil)

				return err
			},
		},
		{
			name: "write cannot read", perm: sas.FilePermissions{Write: true}, want: "AuthorizationPermissionMismatch",
			run: func(t *testing.T, fc *file.Client) error {
				t.Helper()
				_, err := fc.GetProperties(ctx(t), nil)

				return err
			},
		},
		{
			name: "read cannot delete", perm: sas.FilePermissions{Read: true}, want: "AuthorizationPermissionMismatch",
			run: func(t *testing.T, fc *file.Client) error {
				t.Helper()
				_, err := fc.Delete(ctx(t), nil)

				return err
			},
		},
		{
			name: "write can write", perm: sas.FilePermissions{Write: true},
			run: func(t *testing.T, fc *file.Client) error {
				t.Helper()
				_, err := fc.UploadRange(ctx(t), 0, newReadSeekCloser("ab"), nil)

				return err
			},
		},
		{
			name: "delete can delete", perm: sas.FilePermissions{Delete: true},
			run: func(t *testing.T, fc *file.Client) error {
				t.Helper()
				_, err := fc.Delete(ctx(t), nil)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, verifying)
			sc := e.svc.NewShareClient("perms")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)
			newFile(t, sc, "f.txt", 2)

			err = tt.run(t, fileViaSAS(t, sc, "f.txt", tt.perm))
			if tt.want == "" {
				require.NoError(t, err)

				return
			}

			requireCode(t, err, tt.want)
		})
	}
}

func TestSDK_StoredAccessPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy  *share.AccessPolicy
		name    string
		wantErr fileerror.Code
		remove  bool
		write   bool
	}{
		{name: "policy grants read", policy: &share.AccessPolicy{Permission: new("r")}},
		{
			name: "policy read blocks write", write: true, wantErr: "AuthorizationPermissionMismatch",
			policy: &share.AccessPolicy{Permission: new("r")},
		},
		{name: "policy write allows write", write: true, policy: &share.AccessPolicy{Permission: new("w")}},
		{
			name: "expired policy", wantErr: fileerror.AuthenticationFailed,
			policy: &share.AccessPolicy{Permission: new("r"), Expiry: new(time.Now().Add(-time.Hour))},
		},
		{name: "revoked policy", remove: true, wantErr: fileerror.AuthenticationFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, verifying)
			sc := e.svc.NewShareClient("policy")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)
			newFile(t, sc, "f.txt", 2)

			pol := tt.policy
			if pol == nil {
				pol = &share.AccessPolicy{Permission: new("r")}
			}

			if pol.Expiry == nil {
				pol.Expiry = new(time.Now().Add(time.Hour))
			}

			_, err = sc.SetAccessPolicy(ctx(t), &share.SetAccessPolicyOptions{
				ShareACL: []*share.SignedIdentifier{{ID: new("p1"), AccessPolicy: pol}},
			})
			require.NoError(t, err)

			qp, err := sas.SignatureValues{Identifier: "p1", ShareName: "policy", FilePath: "f.txt"}.
				SignWithSharedKey(e.cred)
			require.NoError(t, err)

			if tt.remove {
				_, err = sc.SetAccessPolicy(ctx(t), nil)
				require.NoError(t, err)
			}

			fc, err := file.NewClientWithNoCredential(
				sc.NewRootDirectoryClient().NewFileClient("f.txt").URL()+"?"+qp.Encode(),
				nil,
			)
			require.NoError(t, err)

			if tt.write {
				_, err = fc.UploadRange(ctx(t), 0, newReadSeekCloser("ab"), nil)
			} else {
				_, err = fc.GetProperties(ctx(t), nil)
			}

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			requireCode(t, err, tt.wantErr)
		})
	}
}

func TestSDK_SASOriginRestrictions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		proto   sas.Protocol
		wantErr fileerror.Code
		ip      sas.IPRange
	}{
		{name: "ip matches", ip: sas.IPRange{Start: net.ParseIP("127.0.0.1")}},
		{name: "ip range matches", ip: sas.IPRange{Start: net.ParseIP("127.0.0.0"), End: net.ParseIP("127.0.0.9")}},
		{
			name:    "ip mismatch",
			ip:      sas.IPRange{Start: net.ParseIP("10.1.1.1")},
			wantErr: "AuthorizationSourceIPMismatch",
		},
		{name: "https only over http", proto: sas.ProtocolHTTPS, wantErr: "AuthorizationProtocolMismatch"},
		{name: "https and http", proto: sas.ProtocolHTTPSandHTTP},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, verifying)
			sc := e.svc.NewShareClient("origin")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)
			newFile(t, sc, "f.txt", 2)

			perms := sas.FilePermissions{Read: true}

			qp, err := sas.SignatureValues{
				Permissions: perms.String(), ExpiryTime: time.Now().UTC().Add(time.Hour),
				ShareName: "origin", FilePath: "f.txt", IPRange: tt.ip, Protocol: tt.proto,
			}.SignWithSharedKey(e.cred)
			require.NoError(t, err)

			fc, err := file.NewClientWithNoCredential(
				sc.NewRootDirectoryClient().NewFileClient("f.txt").URL()+"?"+qp.Encode(),
				nil,
			)
			require.NoError(t, err)

			_, err = fc.GetProperties(ctx(t), nil)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			requireCode(t, err, tt.wantErr)
		})
	}
}

func TestSDK_AccountSASScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr fileerror.Code
		perms   sas.AccountPermissions
		types   sas.AccountResourceTypes
	}{
		{
			name: "container only cannot create files", wantErr: "AuthorizationResourceTypeMismatch",
			types: sas.AccountResourceTypes{Container: true}, perms: sas.AccountPermissions{Create: true, Write: true},
		},
		{
			name: "no list permission", wantErr: "AuthorizationPermissionMismatch",
			types: sas.AccountResourceTypes{Service: true}, perms: sas.AccountPermissions{Read: true},
		},
		{
			name:  "service list allowed",
			types: sas.AccountResourceTypes{Service: true}, perms: sas.AccountPermissions{List: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, verifying)
			u, err := e.svc.GetSASURL(tt.types, tt.perms, time.Now().Add(time.Hour), nil)
			require.NoError(t, err)

			viaSAS, err := service.NewClientWithNoCredential(u, nil)
			require.NoError(t, err)

			if tt.wantErr != "" && tt.types.Container {
				_, err = viaSAS.NewShareClient("x").NewRootDirectoryClient().NewFileClient("f").Create(ctx(t), 1, nil)
			} else {
				pager := viaSAS.NewListSharesPager(nil)
				_, err = pager.NextPage(ctx(t))
			}

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			requireCode(t, err, tt.wantErr)
		})
	}
}

func TestSDK_ShareQuota(t *testing.T) {
	t.Parallel()

	const gib = int64(1) << 30

	tests := []struct {
		run     func(t *testing.T, sc *share.Client)
		name    string
		wantErr bool
	}{
		{name: "create within quota", run: func(t *testing.T, sc *share.Client) {
			t.Helper()
			newFile(t, sc, "a.bin", gib)
		}},
		{name: "create beyond quota", wantErr: true, run: func(t *testing.T, sc *share.Client) {
			t.Helper()
			newFile(t, sc, "a.bin", gib)

			_, err := sc.NewRootDirectoryClient().NewFileClient("b.bin").Create(ctx(t), 1, nil)
			requireCode(t, err, fileerror.ShareSizeLimitReached)

			stats, err := sc.GetStatistics(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, gib, *stats.ShareUsageBytes)
		}},
		{name: "resize beyond quota", wantErr: true, run: func(t *testing.T, sc *share.Client) {
			t.Helper()
			fc := newFile(t, sc, "a.bin", 10)

			_, err := fc.Resize(ctx(t), gib+1, nil)
			requireCode(t, err, fileerror.ShareSizeLimitReached)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			sc := e.svc.NewShareClient("quota")
			_, err := sc.Create(ctx(t), &share.CreateOptions{Quota: new(int32(1))})
			require.NoError(t, err)

			tt.run(t, sc)
		})
	}
}

func TestSDK_CORS(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_, err := e.svc.SetProperties(ctx(t), &service.SetPropertiesOptions{CORS: []*service.CORSRule{
		{
			AllowedOrigins: new("https://app.example.com,http://*.contoso.com"),
			AllowedMethods: new("GET,PUT"),
			AllowedHeaders: new(
				"x-ms-meta-*,x-ms-version",
			),
			ExposedHeaders:  new("x-ms-*"),
			MaxAgeInSeconds: new(int32(200)),
		},
	}})
	require.NoError(t, err)

	base := e.url + "/devstoreaccount1/cors?restype=share"

	tests := []struct {
		hdr    map[string]string
		want   map[string]string
		name   string
		method string
		status int
	}{
		{
			name: "preflight allowed", method: http.MethodOptions, status: http.StatusOK,
			hdr: map[string]string{
				"Origin": "https://app.example.com", "Access-Control-Request-Method": "PUT",
				"Access-Control-Request-Headers": "x-ms-meta-foo,x-ms-version",
			},
			want: map[string]string{
				"Access-Control-Allow-Origin": "https://app.example.com", "Access-Control-Max-Age": "200",
				"Access-Control-Allow-Methods": "PUT", "Vary": "Origin",
			},
		},
		{
			name: "preflight wildcard subdomain", method: http.MethodOptions, status: http.StatusOK,
			hdr: map[string]string{"Origin": "http://a.contoso.com", "Access-Control-Request-Method": "GET"},
		},
		{
			name: "preflight origin mismatch", method: http.MethodOptions, status: http.StatusForbidden,
			hdr: map[string]string{"Origin": "https://evil.example.com", "Access-Control-Request-Method": "GET"},
		},
		{
			name: "preflight method mismatch", method: http.MethodOptions, status: http.StatusForbidden,
			hdr: map[string]string{"Origin": "https://app.example.com", "Access-Control-Request-Method": "DELETE"},
		},
		{
			name: "preflight header mismatch", method: http.MethodOptions, status: http.StatusForbidden,
			hdr: map[string]string{
				"Origin": "https://app.example.com", "Access-Control-Request-Method": "GET",
				"Access-Control-Request-Headers": "x-custom",
			},
		},
		{
			name: "preflight missing headers", method: http.MethodOptions, status: http.StatusBadRequest,
			hdr: map[string]string{"Origin": "https://app.example.com"},
		},
		{
			name: "actual request exposes", method: http.MethodGet, status: http.StatusNotFound,
			hdr: map[string]string{"Origin": "https://app.example.com"},
			want: map[string]string{
				"Access-Control-Allow-Origin": "https://app.example.com", "Access-Control-Expose-Headers": "x-ms-*",
				"Vary": "Origin",
			},
		},
		{
			name: "actual request unmatched origin", method: http.MethodGet, status: http.StatusNotFound,
			hdr: map[string]string{"Origin": "https://evil.example.com"}, want: map[string]string{"Vary": "Origin"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := rawDo(t, tt.method, base, "", tt.hdr)
			assert.Equal(t, tt.status, resp.StatusCode)

			for k, v := range tt.want {
				assert.Equal(t, v, resp.Header.Get(k), k)
			}
		})
	}
}

func TestSDK_CORSDisabled(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	resp := rawDo(t, http.MethodOptions, e.url+"/devstoreaccount1/s?restype=share", "", map[string]string{
		"Origin": "https://a.example.com", "Access-Control-Request-Method": "GET",
	})
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	resp = rawDo(
		t,
		http.MethodGet,
		e.url+"/devstoreaccount1?comp=list",
		"",
		map[string]string{"Origin": "https://a.example.com"},
	)
	assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Empty(t, resp.Header.Get("Vary"))
}

func TestSDK_AsyncCopy(t *testing.T) {
	t.Parallel()

	const big = int64(8 << 20)

	tests := []struct {
		run  func(t *testing.T, now *time.Time, src, dst *file.Client)
		name string
	}{
		{name: "pending then success", run: func(t *testing.T, now *time.Time, src, dst *file.Client) {
			t.Helper()

			copied, err := dst.StartCopyFromURL(ctx(t), src.URL(), nil)
			require.NoError(t, err)
			assert.Equal(t, file.CopyStatusTypePending, *copied.CopyStatus)

			props, err := dst.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, file.CopyStatusTypePending, *props.CopyStatus)

			*now = now.Add(time.Minute)

			props, err = dst.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, file.CopyStatusTypeSuccess, *props.CopyStatus)
			assert.Equal(t, big, *props.ContentLength)

			_, err = dst.AbortCopy(ctx(t), *copied.CopyID, nil)
			requireCode(t, err, fileerror.Code("NoPendingCopyOperation"))
		}},
		{name: "abort while pending", run: func(t *testing.T, _ *time.Time, src, dst *file.Client) {
			t.Helper()

			copied, err := dst.StartCopyFromURL(ctx(t), src.URL(), nil)
			require.NoError(t, err)

			_, err = dst.AbortCopy(ctx(t), "wrong-id", nil)
			requireCode(t, err, fileerror.Code("CopyIdMismatch"))

			_, err = dst.AbortCopy(ctx(t), *copied.CopyID, nil)
			require.NoError(t, err)

			props, err := dst.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, file.CopyStatusTypeAborted, *props.CopyStatus)
			assert.Zero(t, *props.ContentLength)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			e := newEnv(t, func(h *azurefileHandler) { h.Now = func() time.Time { return now } })
			sc := e.svc.NewShareClient("copyasync")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			src := newFile(t, sc, "src.bin", big)
			dst := sc.NewRootDirectoryClient().NewFileClient("dst.bin")

			tt.run(t, &now, src, dst)
		})
	}
}

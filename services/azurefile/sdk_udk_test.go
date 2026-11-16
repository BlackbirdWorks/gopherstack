package azurefile_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

type fakeToken struct{}

func (fakeToken) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "tok", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// hostRewrite sends https://<account>.file.core.windows.net requests to the test server, because the SDK derives
// the SAS account name from an https host label.
type hostRewrite struct {
	client *http.Client
	target string
}

func (h hostRewrite) Do(r *http.Request) (*http.Response, error) {
	c := r.Clone(r.Context())
	c.URL.Scheme, c.URL.Host = "http", h.target

	return h.client.Do(c)
}

func (e *env) transport() azcore.ClientOptions {
	return azcore.ClientOptions{Transport: hostRewrite{
		client: httpclient.New(30 * time.Second),
		target: strings.TrimPrefix(e.url, "http://"),
	}}
}

func hostURL(account string) string { return "https://" + account + ".file.core.windows.net/" }

func bearerService(t *testing.T, e *env, account string) *service.Client {
	t.Helper()

	svc, err := service.NewClient(hostURL(account), fakeToken{}, &service.ClientOptions{ClientOptions: e.transport()})
	require.NoError(t, err)

	return svc
}

func udkCredential(t *testing.T, svc *service.Client, start, expiry time.Time) *service.UserDelegationCredential {
	t.Helper()

	cred, err := svc.GetUserDelegationCredential(ctx(t), service.KeyInfo{
		Start: new(
			start.UTC().Format("2006-01-02T15:04:05Z"),
		),
		Expiry: new(expiry.UTC().Format("2006-01-02T15:04:05Z")),
	}, nil)
	require.NoError(t, err)

	return cred
}

func TestSDK_UserDelegationSAS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, e *env, sc *share.Client, cred *service.UserDelegationCredential)
		name  string
	}{
		{
			name: "file sas reads",
			check: func(t *testing.T, e *env, sc *share.Client, cred *service.UserDelegationCredential) {
				t.Helper()

				fc := newFile(t, sc, "f.txt", 3)
				upload(t, fc, 0, "xyz")

				q, err := sas.SignatureValues{
					Permissions: (&sas.FilePermissions{Read: true}).String(),
					ExpiryTime:  time.Now().UTC().Add(time.Hour),
					ShareName:   "udshare",
					FilePath:    "f.txt",
				}.SignWithUserDelegation(cred)
				require.NoError(t, err)

				viaSAS, err := file.NewClientWithNoCredential(
					hostURL(azureauth.DefaultAccountName)+"udshare/f.txt?"+q.Encode(),
					&file.ClientOptions{ClientOptions: e.transport()},
				)
				require.NoError(t, err)
				assert.Equal(t, "xyz", downloadAll(t, viaSAS))

				_, err = viaSAS.Delete(ctx(t), nil)
				requireCode(t, err, fileerror.AuthorizationPermissionMismatch)
			},
		},
		{
			name: "tampered signature",
			check: func(t *testing.T, e *env, sc *share.Client, cred *service.UserDelegationCredential) {
				t.Helper()

				newFile(t, sc, "f.txt", 3)
				q, err := sas.SignatureValues{
					Permissions: (&sas.FilePermissions{Read: true}).String(),
					ExpiryTime:  time.Now().UTC().Add(time.Hour),
					ShareName:   "udshare",
					FilePath:    "f.txt",
				}.SignWithUserDelegation(cred)
				require.NoError(t, err)

				viaSAS, err := file.NewClientWithNoCredential(
					hostURL(
						azureauth.DefaultAccountName,
					)+"udshare/f.txt?"+strings.Replace(
						q.Encode(),
						"sig=",
						"sig=AAAA",
						1,
					),
					&file.ClientOptions{ClientOptions: e.transport()},
				)
				require.NoError(t, err)
				_, err = viaSAS.GetProperties(ctx(t), nil)
				requireCode(t, err, fileerror.AuthenticationFailed)
			},
		},
		{
			name: "other file path",
			check: func(t *testing.T, e *env, sc *share.Client, cred *service.UserDelegationCredential) {
				t.Helper()

				newFile(t, sc, "other.txt", 3)
				q, err := sas.SignatureValues{
					Permissions: (&sas.FilePermissions{Read: true}).String(),
					ExpiryTime:  time.Now().UTC().Add(time.Hour),
					ShareName:   "udshare",
					FilePath:    "f.txt",
				}.SignWithUserDelegation(cred)
				require.NoError(t, err)

				viaSAS, err := file.NewClientWithNoCredential(
					hostURL(azureauth.DefaultAccountName)+"udshare/other.txt?"+q.Encode(),
					&file.ClientOptions{ClientOptions: e.transport()},
				)
				require.NoError(t, err)
				_, err = viaSAS.GetProperties(ctx(t), nil)
				requireCode(t, err, fileerror.AuthenticationFailed)
			},
		},
		{
			name: "share sas lists",
			check: func(t *testing.T, e *env, sc *share.Client, cred *service.UserDelegationCredential) {
				t.Helper()

				newFile(t, sc, "a.txt", 1)
				q, err := sas.SignatureValues{
					Permissions: (&sas.SharePermissions{Read: true, List: true}).String(),
					ExpiryTime:  time.Now().UTC().Add(time.Hour),
					ShareName:   "udshare",
				}.SignWithUserDelegation(cred)
				require.NoError(t, err)

				viaSAS, err := share.NewClientWithNoCredential(
					hostURL(azureauth.DefaultAccountName)+"udshare?"+q.Encode(),
					&share.ClientOptions{ClientOptions: e.transport()},
				)
				require.NoError(t, err)
				page, err := viaSAS.NewRootDirectoryClient().NewListFilesAndDirectoriesPager(nil).NextPage(ctx(t))
				require.NoError(t, err)
				assert.Len(t, page.Segment.Files, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, verifying)
			sc := e.svc.NewShareClient("udshare")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			cred := udkCredential(t, bearerService(t, e, azureauth.DefaultAccountName), time.Now().Add(-time.Minute),
				time.Now().Add(time.Hour))
			tt.check(t, e, sc, cred)
		})
	}
}

func TestSDK_UserDelegationKeyErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		start  time.Time
		expiry time.Time
		name   string
		code   fileerror.Code
		bearer bool
	}{
		{name: "shared key refused", bearer: false, start: time.Now(), expiry: time.Now().Add(time.Hour),
			code: fileerror.AuthenticationFailed},
		{name: "expiry beyond seven days", bearer: true, start: time.Now(), expiry: time.Now().Add(8 * 24 * time.Hour),
			code: "InvalidXmlNodeValue"},
		{name: "expiry before start", bearer: true, start: time.Now().Add(time.Hour), expiry: time.Now(),
			code: "InvalidXmlNodeValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			svc := e.svc
			if tt.bearer {
				svc = bearerService(t, e, azureauth.DefaultAccountName)
			}

			_, err := svc.GetUserDelegationCredential(ctx(t), service.KeyInfo{
				Start: new(
					tt.start.UTC().Format("2006-01-02T15:04:05Z"),
				),
				Expiry: new(tt.expiry.UTC().Format("2006-01-02T15:04:05Z")),
			}, nil)
			requireCode(t, err, tt.code)
		})
	}
}

func TestSDK_SignatureVerificationOtherAccounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "right key", key: azureauth.DefaultAccountKey},
		{name: "wrong key", key: "d3Jvbmcta2V5LXdyb25nLWtleQ==", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, verifying)
			cred, err := service.NewSharedKeyCredential("acctone", tt.key)
			require.NoError(t, err)

			svc, err := service.NewClientWithSharedKeyCredential(e.url+"/acctone/", cred, nil)
			require.NoError(t, err)

			sc := svc.NewShareClient("share1")
			_, err = sc.Create(ctx(t), nil)

			if tt.wantErr {
				requireCode(t, err, fileerror.AuthenticationFailed)

				return
			}

			require.NoError(t, err)

			url, err := sc.GetSASURL(sas.SharePermissions{Read: true, List: true}, time.Now().Add(time.Hour), nil)
			require.NoError(t, err)

			viaSAS, err := share.NewClientWithNoCredential(strings.Replace(url, "sig=", "sig=AAAA", 1), nil)
			require.NoError(t, err)

			_, err = viaSAS.NewRootDirectoryClient().NewListFilesAndDirectoriesPager(nil).NextPage(ctx(t))
			requireCode(t, err, fileerror.AuthenticationFailed)

			good, err := share.NewClientWithNoCredential(url, nil)
			require.NoError(t, err)
			_, err = good.NewRootDirectoryClient().NewListFilesAndDirectoriesPager(nil).NextPage(ctx(t))
			require.NoError(t, err)
		})
	}
}

func TestSDK_BearerTokenValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		validate func(string) (string, string, error)
		name     string
		wantOID  string
		code     fileerror.Code
	}{
		{name: "accepted token supplies oid", wantOID: "11111111-2222-3333-4444-555555555555",
			validate: func(string) (string, string, error) {
				return "11111111-2222-3333-4444-555555555555", "tenant", nil
			}},
		{name: "rejected token", code: fileerror.AuthenticationFailed,
			validate: func(string) (string, string, error) { return "", "", errBadToken }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, verifying, func(h *azurefileHandler) { h.ValidateToken = tt.validate })
			svc := bearerService(t, e, azureauth.DefaultAccountName)

			cred, err := svc.GetUserDelegationCredential(ctx(t), service.KeyInfo{
				Start:  new(time.Now().UTC().Format("2006-01-02T15:04:05Z")),
				Expiry: new(time.Now().UTC().Add(time.Hour).Format("2006-01-02T15:04:05Z")),
			}, nil)
			if tt.code != "" {
				requireCode(t, err, tt.code)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, cred)
		})
	}
}

var errBadToken = errors.New("bad token")

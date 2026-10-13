package sso_test

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ssosdk "github.com/aws/aws-sdk-go-v2/service/sso"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sso"
	"github.com/blackbirdworks/gopherstack/services/ssooidc"
)

var errDirectory = errors.New("directory down")

type fakeDirectory struct {
	err      error
	names    map[string]string
	issued   []sso.Entitlement
	subjects []ssooidc.Subject
	ents     []sso.Entitlement
}

func (d *fakeDirectory) Entitlements(ssooidc.Subject) ([]sso.Entitlement, error) {
	return d.ents, d.err
}

func (d *fakeDirectory) AccountInfo(id string) sso.AccountInfo {
	return sso.AccountInfo{Name: d.names[id], Email: d.names[id] + "@example.com"}
}

func (d *fakeDirectory) IssueCredentials(e sso.Entitlement, s ssooidc.Subject) (sso.Credentials, error) {
	d.issued = append(d.issued, e)
	d.subjects = append(d.subjects, s)

	return sso.Credentials{
		AccessKeyID: "ASIA" + e.PermissionSetName, SecretAccessKey: "secret", SessionToken: "token",
		Expiration: time.UnixMilli(1893456000123),
	}, nil
}

func entitlement(account, name string) sso.Entitlement {
	return sso.Entitlement{
		AccountID:         account,
		PermissionSetName: name,
		PermissionSetArn:  "arn:aws:sso:::permissionSet/ssoins-1/ps-" + name,
		InstanceArn:       "arn:aws:sso:::instance/ssoins-1",
	}
}

type fixture struct {
	oidc   *ssooidc.InMemoryBackend
	dir    *fakeDirectory
	client *ssosdk.Client
	token  string
}

func newFixture(t *testing.T, ents ...sso.Entitlement) fixture {
	t.Helper()

	backend := ssooidc.NewInMemoryBackend()
	dir := &fakeDirectory{
		ents:  ents,
		names: map[string]string{"111111111111": "prod", "222222222222": "dev", "333333333333": "ops"},
	}

	h := sso.NewHandler()
	h.SetTokenSource(backend)
	h.SetDirectory(dir)

	return fixture{oidc: backend, dir: dir, client: newClient(t, h), token: signIn(t, backend)}
}

func newClient(t *testing.T, h *sso.Handler) *ssosdk.Client {
	t.Helper()

	e := echo.New()
	reg := service.NewRegistry()
	require.NoError(t, reg.Register(h))
	e.Use(service.NewServiceRouter(reg).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return ssosdk.NewFromConfig(cfg, func(o *ssosdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

func signIn(t *testing.T, b *ssooidc.InMemoryBackend) string {
	t.Helper()

	reg, apiErr := b.RegisterClient(&ssooidc.RegisterClientInput{ClientName: "c", ClientType: "public"})
	require.Nil(t, apiErr)

	dev, apiErr := b.StartDeviceAuthorization(reg.ClientID, reg.ClientSecret, "https://example.awsapps.com/start")
	require.Nil(t, apiErr)
	require.NoError(
		t,
		b.ApproveDevice(dev.UserCode, ssooidc.Subject{UserName: "alice", UserID: "u-1", IdentityStoreID: "d-1"}),
	)

	tok, apiErr := b.CreateToken(&ssooidc.CreateTokenInput{
		ClientID: reg.ClientID, ClientSecret: reg.ClientSecret,
		GrantType: "urn:ietf:params:oauth:grant-type:device_code", DeviceCode: dev.DeviceCode,
	}, false)
	require.Nil(t, apiErr)

	return tok.AccessToken
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

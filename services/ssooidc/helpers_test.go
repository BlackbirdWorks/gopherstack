package ssooidc_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ssooidc"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	oidc "github.com/blackbirdworks/gopherstack/services/ssooidc"
)

const startURL = "https://d-1234567890.awsapps.com/start"

type fixedResolver map[string]oidc.Subject

func (r fixedResolver) ResolveSubject(name string) (oidc.Subject, error) {
	if name == "" {
		name = "alice"
	}

	if s, ok := r[name]; ok {
		return s, nil
	}

	return oidc.Subject{}, oidc.ErrUnknownUser
}

func newResolver() fixedResolver {
	return fixedResolver{
		"alice": {UserName: "alice", UserID: "u-alice", IdentityStoreID: "d-1"},
		"bob":   {UserName: "bob", UserID: "u-bob", IdentityStoreID: "d-1"},
	}
}

type fixture struct {
	handler *oidc.Handler
	client  *ssooidc.Client
	srv     *httptest.Server
}

func newFixture(t *testing.T) fixture {
	t.Helper()

	h := oidc.NewHandler(oidc.NewInMemoryBackend())
	h.SetSubjectResolver(newResolver())

	e := echo.New()
	reg := service.NewRegistry()
	require.NoError(t, reg.Register(h))
	e.Use(service.NewServiceRouter(reg).RouteHandler())
	e.GET(oidc.DevicePath, h.ServeDeviceVerification)
	e.POST(oidc.DevicePath, h.ServeDeviceVerification)
	e.GET(oidc.AuthorizePath, h.ServeAuthorize)

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := ssooidc.NewFromConfig(cfg, func(o *ssooidc.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	return fixture{handler: h, client: client, srv: srv}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func (f fixture) register(t *testing.T, grants ...string) (string, string) {
	t.Helper()

	out, err := f.client.RegisterClient(context.Background(), &ssooidc.RegisterClientInput{
		ClientName: aws.String("test-client"), ClientType: aws.String("public"),
		GrantTypes: grants, Scopes: []string{"sso:account:access"},
		RedirectUris: redirectsFor(grants),
	})
	require.NoError(t, err)

	return aws.ToString(out.ClientId), aws.ToString(out.ClientSecret)
}

func redirectsFor(grants []string) []string {
	if slices.Contains(grants, "authorization_code") {
		return []string{"http://127.0.0.1/oauth/callback"}
	}

	return nil
}

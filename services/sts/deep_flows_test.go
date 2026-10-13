package sts_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

type deepUserLookup struct{}

func (deepUserLookup) GetUserArnByAccessKeyID(key string) (string, error) {
	if key == "AKIAUSERKEY" {
		return "arn:aws:iam::000000000000:user/alice", nil
	}

	return "", sts.ErrSessionNotFound
}

func (deepUserLookup) GetUserIDByAccessKeyID(string) (string, error) { return "AIDAALICE", nil }

func newDeepSTSClient(t *testing.T, backend *sts.InMemoryBackend, key, secret, token string) *stssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(sts.NewHandler(backend)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, token)),
	)
	require.NoError(t, err)

	return stssdk.NewFromConfig(cfg, func(o *stssdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

func TestDeepFlows_CallerIdentityForIAMUserKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		wantArn string
		wantID  string
	}{
		{name: "iam_user", key: "AKIAUSERKEY", wantArn: "arn:aws:iam::000000000000:user/alice", wantID: "AIDAALICE"},
		{name: "unknown_long_term", key: "AKIAOTHER", wantArn: "arn:aws:iam::000000000000:root"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			b.SetUserLookup(deepUserLookup{})
			client := newDeepSTSClient(t, b, tt.key, "secret", "")

			out, err := client.GetCallerIdentity(t.Context(), &stssdk.GetCallerIdentityInput{})
			require.NoError(t, err)
			assert.Equal(t, tt.wantArn, aws.ToString(out.Arn))

			if tt.wantID != "" {
				assert.Equal(t, tt.wantID, aws.ToString(out.UserId))
			}
		})
	}
}

func TestDeepFlows_SessionCredentialsCannotMintTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(c *stssdk.Client) error
		name string
	}{
		{name: "get_session_token", call: func(c *stssdk.Client) error {
			_, err := c.GetSessionToken(t.Context(), &stssdk.GetSessionTokenInput{})

			return err
		}},
		{name: "get_federation_token", call: func(c *stssdk.Client) error {
			_, err := c.GetFederationToken(t.Context(), &stssdk.GetFederationTokenInput{Name: aws.String("fed")})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			root := newDeepSTSClient(t, b, "AKIAROOT", "secret", "")

			assumed, err := root.AssumeRole(t.Context(), &stssdk.AssumeRoleInput{
				RoleArn:         aws.String("arn:aws:iam::000000000000:role/r"),
				RoleSessionName: aws.String("sess"),
			})
			require.NoError(t, err)

			sessionClient := newDeepSTSClient(t, b,
				aws.ToString(assumed.Credentials.AccessKeyId),
				aws.ToString(assumed.Credentials.SecretAccessKey),
				aws.ToString(assumed.Credentials.SessionToken))

			var apiErr smithy.APIError
			require.ErrorAs(t, tt.call(sessionClient), &apiErr)
			assert.Equal(t, "AccessDenied", apiErr.ErrorCode())

			require.NoError(t, tt.call(root))
		})
	}
}

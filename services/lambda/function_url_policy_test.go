package lambda_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

const (
	urlPolicyAction     = "lambda:InvokeFunctionUrl"
	sameAcctUserARN     = "arn:aws:iam::000000000000:user/alice"
	otherAcctUserARN    = "arn:aws:iam::111111111111:user/bob"
	urlPolicyAllowAlice = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"lambda:InvokeFunctionUrl","Resource":"*"}]}`
)

type lambdaPolicyProvider struct{ bk *lambda.InMemoryBackend }

func (p lambdaPolicyProvider) GetResourcePolicy(_ context.Context, _ string) (string, error) {
	out, err := p.bk.GetPolicy(urlTestFn, "")
	if err != nil || out.Policy == nil {
		return "", nil //nolint:nilerr // no policy attached
	}

	return *out.Policy, nil
}

func signURLRequest(t *testing.T, req *http.Request, accessKey string) {
	t.Helper()

	sum := sha256.Sum256(nil)
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: "test"}
	require.NoError(t, v4.NewSigner().SignHTTP(
		t.Context(), creds, req, hex.EncodeToString(sum[:]), "lambda", "us-east-1", time.Now(),
	))
}

func (e *urlHostEnv) enforcedServe(t *testing.T, iamBackend *mockLambdaIAMBackend, req *http.Request) int {
	t.Helper()

	bk, ok := e.h.Backend.(*lambda.InMemoryBackend)
	require.True(t, ok)

	srv := echo.New()
	srv.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			r := c.Request()
			c.SetRequest(r.WithContext(awsmeta.Set(r.Context(), awsmeta.FromRequest(r, "us-east-1"))))

			return next(c)
		}
	})
	srv.Use(iam.EnforcementMiddleware(iamBackend, iam.EnforcementConfig{
		Global:            config.NewGlobalConfig("000000000000", "us-east-1", 0, 0, true, 0),
		ActionExtractors:  []iam.ActionExtractor{e.h},
		ResourceProviders: []iam.ResourcePolicyProvider{lambdaPolicyProvider{bk: bk}},
	}))
	srv.Any("/*", e.h.Handler())

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	return rec.Code
}

func urlPolicyIAM(userARN string, identityPolicies ...string) *mockLambdaIAMBackend {
	m := newMockLambdaIAMBackend()
	m.users["u"] = &iam.User{UserName: "u", Arn: userARN}
	m.keyMap["AKIAURLPOLICY"] = "u"
	m.policies["u"] = identityPolicies

	return m
}

func TestFunctionURLPolicy_AWSIAM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		userARN   string
		principal string
		authType  string
		identity  []string
		want      int
	}{
		{name: "identity_allow", userARN: sameAcctUserARN, identity: []string{urlPolicyAllowAlice}, want: 200},
		{name: "resource_allow", userARN: sameAcctUserARN, principal: sameAcctUserARN, want: 200},
		{name: "resource_account_allow", userARN: sameAcctUserARN, principal: "000000000000", want: 200},
		{
			name: "resource_account_root_allow", userARN: sameAcctUserARN,
			principal: "arn:aws:iam::000000000000:root", want: 200,
		},
		{name: "resource_wildcard_allow", userARN: sameAcctUserARN, principal: "*", authType: "AWS_IAM", want: 200},
		{name: "both_deny", userARN: sameAcctUserARN, want: 403},
		{
			name: "resource_other_principal", userARN: sameAcctUserARN,
			principal: "arn:aws:iam::000000000000:user/eve", want: 403,
		},
		{
			name: "resource_wrong_auth_type", userARN: sameAcctUserARN,
			principal: sameAcctUserARN, authType: "NONE", want: 403,
		},
		{name: "cross_account_resource_only", userARN: otherAcctUserARN, principal: otherAcctUserARN, want: 403},
		{
			name: "cross_account_identity_only", userARN: otherAcctUserARN,
			identity: []string{urlPolicyAllowAlice}, want: 403,
		},
		{
			name: "cross_account_both", userARN: otherAcctUserARN, identity: []string{urlPolicyAllowAlice},
			principal: otherAcctUserARN, want: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newURLHostEnv(t, "AWS_IAM", "", nil, `{"statusCode":200,"body":"ok"}`)

			if tt.principal != "" {
				authType := tt.authType
				if authType == "" || authType == "AWS_IAM" {
					authType = "AWS_IAM"
				}

				bk, ok := env.h.Backend.(*lambda.InMemoryBackend)
				require.True(t, ok)
				_, err := bk.AddPermission(urlTestFn, "", &lambda.AddPermissionInput{
					StatementID: "s1", Action: urlPolicyAction, Principal: tt.principal, FunctionURLAuthType: authType,
				})
				require.NoError(t, err)
			}

			req := env.request(t, http.MethodGet, env.host, "/", "")
			signURLRequest(t, req, "AKIAURLPOLICY")

			assert.Equal(t, tt.want, env.enforcedServe(t, urlPolicyIAM(tt.userARN, tt.identity...), req))
		})
	}
}

func TestFunctionURLPolicy_None(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		authType  string
		signed    bool
		enforced  bool
		want      int
	}{
		{name: "enforced_no_statement", enforced: true, want: 403},
		{name: "enforced_public_statement", enforced: true, principal: "*", authType: "NONE", want: 200},
		{
			name: "enforced_public_statement_signed", enforced: true, principal: "*",
			authType: "NONE", signed: true, want: 200,
		},
		{name: "enforced_named_principal", enforced: true, principal: sameAcctUserARN, authType: "NONE", want: 403},
		{name: "enforced_wrong_auth_type", enforced: true, principal: "*", authType: "AWS_IAM", want: 403},
		{name: "not_enforced_no_statement", want: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newURLHostEnv(t, "NONE", "", nil, `{"statusCode":200,"body":"ok"}`)

			if tt.principal != "" {
				bk, ok := env.h.Backend.(*lambda.InMemoryBackend)
				require.True(t, ok)
				_, err := bk.AddPermission(urlTestFn, "", &lambda.AddPermissionInput{
					StatementID: "s1", Action: urlPolicyAction,
					Principal: tt.principal, FunctionURLAuthType: tt.authType,
				})
				require.NoError(t, err)
			}

			req := env.request(t, http.MethodGet, env.host, "/", "")
			if tt.signed {
				signURLRequest(t, req, "AKIAURLPOLICY")
			}

			if !tt.enforced {
				assert.Equal(t, tt.want, env.do(t, req).Code)

				return
			}

			assert.Equal(t, tt.want, env.enforcedServe(t, urlPolicyIAM(sameAcctUserARN), req))
		})
	}
}

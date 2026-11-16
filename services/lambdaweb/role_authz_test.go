package lambdaweb_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

var errDenied = errors.New("denied")

// denyAuth refuses every action that starts with one of its prefixes.
type denyAuth struct {
	prefixes []string
	sources  []string
}

func (a *denyAuth) denies(action string) bool {
	for _, p := range a.prefixes {
		if strings.HasPrefix(action, p) {
			return true
		}
	}

	return false
}

func (a *denyAuth) AuthorizeRole(principal, _, action, _ string) error {
	if principal != roleauth.PrincipalLambda || a.denies(action) {
		return errDenied
	}

	return nil
}

func (a *denyAuth) AuthorizeServiceResource(principal, action, _, source string) error {
	a.sources = append(a.sources, source)

	if principal != roleauth.PrincipalLambda || a.denies(action) {
		return errDenied
	}

	return nil
}

type okKeys struct{}

func (okKeys) CheckKey(context.Context, string, string) error { return nil }

const testKeyARN = "arn:aws:kms:us-east-1:123456789012:key/mrk-1234"

func TestRoleAuthorizer_RevisionCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		denied  []string
		wantErr bool
	}{
		{name: "allowed"},
		{name: "kms key policy", denied: []string{"kms:"}, wantErr: true},
		{name: "s3 bucket policy", denied: []string{"s3:"}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			auth := &denyAuth{prefixes: tc.denied}
			env.backend.SetKeyChecker(okKeys{})
			env.backend.SetRoleAuthorizer(auth)

			rc := revisionConfig()
			rc.KmsKeyArn = aws.String(testKeyARN)

			_, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
				FunctionName: aws.String("app"), RevisionConfig: rc,
			})

			if tc.wantErr {
				assertAPIErrorCode(t, err, "AccessDeniedException")

				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, auth.sources)

			for _, src := range auth.sources {
				assert.Contains(t, src, "web-function/app/revision/", "aws:SourceArn is a revision ARN")
			}
		})
	}
}

func TestRoleAuthorizer_ExecutionRoleLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		denied   []string
		wantLogs bool
	}{
		{name: "role may write logs", wantLogs: true},
		{name: "role lacks logs permission", denied: []string{"logs:"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, logs := newLoggedEnv(t, logOpts{})
			d.backend.SetRoleAuthorizer(&denyAuth{prefixes: tc.denied})

			require.Equal(t, http.StatusOK, d.get(t).StatusCode)
			assert.Equal(t, tc.wantLogs, len(logs.snapshot("/aws/lambda/web/app")) > 0)
		})
	}
}

func TestVerifyRevisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate     func(codes *fakeCodes, env *testEnv)
		name       string
		wantFailed bool
	}{
		{name: "object still readable", mutate: func(*fakeCodes, *testEnv) {}},
		{name: "object version deleted", wantFailed: true, mutate: func(c *fakeCodes, _ *testEnv) {
			delete(c.objects, "code/app.zip@v1")
		}},
		{name: "bucket policy withdrawn", wantFailed: true, mutate: func(_ *fakeCodes, e *testEnv) {
			e.backend.SetRoleAuthorizer(&denyAuth{prefixes: []string{"s3:"}})
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			codes := newFakeCodes()
			env.backend.SetCodeStore(codes)

			created := env.createFunction(t, "app")
			env.settle()

			tc.mutate(codes, env)
			env.backend.VerifyRevisions(t.Context())

			rev, err := env.backend.GetWebFunctionRevision(testRegion, "app", aws.ToString(created.Revision.RevisionId))
			require.NoError(t, err)

			if !tc.wantFailed {
				assert.Equal(t, "Active", rev.State)

				return
			}

			assert.Equal(t, "Failed", rev.State)
			require.Len(t, rev.Errors, 1)
			assert.Equal(t, "CodeUnavailable", rev.Errors[0].ErrorCode)

			env.backend.VerifyRevisions(t.Context())

			again, err := env.backend.GetWebFunctionRevision(testRegion, "app", rev.ID)
			require.NoError(t, err)
			assert.Equal(t, "Failed", again.State, "Failed is permanent")
		})
	}
}

var _ lambdaweb.KeyChecker = okKeys{}

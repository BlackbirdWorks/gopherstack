package iam_test

import (
	"encoding/xml"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// changePasswordFixtureUser is one simulated IAM user with their own access
// key and login profile, signed against by their own real iamsdk.Client so
// requests carry that user's own SigV4 credential.
type changePasswordFixtureUser struct {
	client   *iamsdk.Client
	userName string
	password string
}

// newChangePasswordFixture starts a real iam.Handler behind an httptest
// server and creates two IAM users, each with their own access key and login
// profile, each with a real SDK client signing as that user.
func newChangePasswordFixture(
	t *testing.T,
) (*iam.InMemoryBackend, changePasswordFixtureUser, changePasswordFixtureUser, string) {
	t.Helper()

	b := iam.NewInMemoryBackend()
	h := iam.NewHandler(b)

	e := echo.New()
	e.POST("/", h.Handler())
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	alice := newChangePasswordUser(t, b, srv.URL, "alice", "AliceOldP@ss123!")
	bob := newChangePasswordUser(t, b, srv.URL, "bob", "BobOldP@ss12345!")

	return b, alice, bob, srv.URL
}

func newChangePasswordUser(
	t *testing.T, b *iam.InMemoryBackend, endpoint, userName, password string,
) changePasswordFixtureUser {
	t.Helper()

	_, err := b.CreateUser(userName, "/", "")
	require.NoError(t, err)

	ak, err := b.CreateAccessKey(userName)
	require.NoError(t, err)

	_, err = b.CreateLoginProfile(userName, password, false)
	require.NoError(t, err)

	return changePasswordFixtureUser{
		userName: userName,
		password: password,
		client:   newIAMClientAs(t, endpoint, ak.AccessKeyID, ak.SecretAccessKey),
	}
}

func newIAMClientAs(t *testing.T, endpoint, accessKeyID, secretKey string) *iamsdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
		),
	)
	require.NoError(t, err)

	return iamsdk.NewFromConfig(cfg, func(o *iamsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func awsErrorCode(t *testing.T, err error) string {
	t.Helper()

	apiErr, ok := errors.AsType[smithy.APIError](err)
	require.True(t, ok, "expected a real smithy.APIError, got %T: %v", err, err)

	return apiErr.ErrorCode()
}

func TestChangePassword_PerUserIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *iam.InMemoryBackend, alice, bob changePasswordFixtureUser, endpoint string)
		name string
	}{
		{
			name: "own old password succeeds",
			run: func(t *testing.T, b *iam.InMemoryBackend, alice, _ changePasswordFixtureUser, _ string) {
				t.Helper()

				_, err := alice.client.ChangePassword(t.Context(), &iamsdk.ChangePasswordInput{
					OldPassword: aws.String(alice.password),
					NewPassword: aws.String("AliceNewP@ss123!"),
				})
				require.NoError(t, err)

				lp, err := b.GetLoginProfile(alice.userName)
				require.NoError(t, err)
				assert.Equal(t, "AliceNewP@ss123!", lp.Password)
			},
		},
		{
			name: "other user's old password rejected",
			run: func(t *testing.T, b *iam.InMemoryBackend, alice, bob changePasswordFixtureUser, _ string) {
				t.Helper()

				_, err := alice.client.ChangePassword(t.Context(), &iamsdk.ChangePasswordInput{
					OldPassword: aws.String(bob.password),
					NewPassword: aws.String("AliceNewP@ss123!"),
				})
				require.Error(t, err)
				assert.Equal(t, "PasswordPolicyViolation", awsErrorCode(t, err))

				lp, err := b.GetLoginProfile(alice.userName)
				require.NoError(t, err)
				assert.Equal(t, alice.password, lp.Password, "alice's password must be unchanged")
			},
		},
		{
			name: "bob's password survives alice's change",
			run: func(t *testing.T, b *iam.InMemoryBackend, alice, bob changePasswordFixtureUser, _ string) {
				t.Helper()

				_, err := alice.client.ChangePassword(t.Context(), &iamsdk.ChangePasswordInput{
					OldPassword: aws.String(alice.password),
					NewPassword: aws.String("AliceNewP@ss123!"),
				})
				require.NoError(t, err)

				_, err = bob.client.ChangePassword(t.Context(), &iamsdk.ChangePasswordInput{
					OldPassword: aws.String(bob.password),
					NewPassword: aws.String("BobNewP@ss12345!"),
				})
				require.NoError(t, err, "bob's original password must still work after alice changed hers")

				lp, err := b.GetLoginProfile(bob.userName)
				require.NoError(t, err)
				assert.Equal(t, "BobNewP@ss12345!", lp.Password)
			},
		},
		{
			name: "unresolvable caller rejected, no global fallback",
			run: func(t *testing.T, b *iam.InMemoryBackend, alice, _ changePasswordFixtureUser, endpoint string) {
				t.Helper()

				stranger := newIAMClientAs(t, endpoint, "AKIAUNKNOWNCALLER0", "unregistered-secret")

				_, err := stranger.ChangePassword(t.Context(), &iamsdk.ChangePasswordInput{
					OldPassword: aws.String(alice.password),
					NewPassword: aws.String("StrangerNewP@ss1!"),
				})
				require.Error(t, err, "an access key with no known IAM user must not fall back to a shared password")
				assert.Equal(t, "NoSuchEntity", awsErrorCode(t, err))

				lp, err := b.GetLoginProfile(alice.userName)
				require.NoError(t, err)
				assert.Equal(t, alice.password, lp.Password,
					"alice's password must be unaffected by the unresolvable caller")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, alice, bob, endpoint := newChangePasswordFixture(t)
			tt.run(t, b, alice, bob, endpoint)
		})
	}
}

func TestGetLoginProfile_NeverLeaksPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
	}{
		{name: "simple password", password: "AliceOldP@ss123!"},
		{name: "password containing xml-like text", password: "P@ss<Password>leak</Password>1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			_, err := b.CreateUser("carol", "/", "")
			require.NoError(t, err)
			_, err = b.CreateLoginProfile("carol", tt.password, false)
			require.NoError(t, err)

			e := echo.New()
			req := iamRequest("GetLoginProfile", map[string]string{"UserName": "carol"})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))

			body := rec.Body.String()
			assert.Contains(t, body, "GetLoginProfileResponse")
			assert.NotContains(t, body, tt.password, "raw GetLoginProfile response body must never carry the password")

			var resp iam.GetLoginProfileResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "carol", resp.GetLoginProfileResult.LoginProfile.UserName)
		})
	}
}

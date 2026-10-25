package signin_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	signinsdk "github.com/aws/aws-sdk-go-v2/service/signin"
	"github.com/aws/aws-sdk-go-v2/service/signin/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/signin"
)

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	c := src.self(t)

	iam, err := c.CreateOAuth2TokenWithIAM(t.Context(), &signinsdk.CreateOAuth2TokenWithIAMInput{
		GrantType: aws.String("client_credentials"), Resource: aws.String("aws-mcp.amazonaws.com"),
	})
	require.NoError(t, err)

	human, err := c.CreateOAuth2Token(t.Context(), tokenInput("authorization_code", func(b *types.CreateOAuth2TokenRequestBody) {
		b.Code, b.RedirectUri, b.CodeVerifier = aws.String(src.authCode(t)), aws.String(testRedir), aws.String(verifier)
	}))
	require.NoError(t, err)

	_, err = c.PutConsoleAuthorizationConfiguration(t.Context(), &signinsdk.PutConsoleAuthorizationConfigurationInput{})
	require.NoError(t, err)

	st, err := c.PutResourcePermissionStatement(t.Context(), &signinsdk.PutResourcePermissionStatementInput{SourceIp: aws.String("10.0.0.1")})
	require.NoError(t, err)

	snap := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newFixtureWith(t, signin.NewInMemoryBackend(testAccount, testRegion))
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	d := dst.self(t)

	got, err := d.IntrospectOAuth2TokenWithIAM(t.Context(), &signinsdk.IntrospectOAuth2TokenWithIAMInput{Token: iam.AccessToken})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(got.Active))

	_, err = d.CreateOAuth2Token(t.Context(), tokenInput("refresh_token", func(b *types.CreateOAuth2TokenRequestBody) {
		b.RefreshToken = human.TokenOutput.RefreshToken
	}))
	require.NoError(t, err)

	cfg, err := d.GetConsoleAuthorizationConfiguration(t.Context(), &signinsdk.GetConsoleAuthorizationConfigurationInput{})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(cfg.ConsoleAuthorizationEnabled))

	list, err := d.ListResourcePermissionStatements(t.Context(), &signinsdk.ListResourcePermissionStatementsInput{})
	require.NoError(t, err)
	require.Len(t, list.PermissionStatements, 1)
	assert.Equal(t, aws.ToString(st.StatementId), aws.ToString(list.PermissionStatements[0].Sid))
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.self(t)

	_, err := c.PutConsoleAuthorizationConfiguration(t.Context(), &signinsdk.PutConsoleAuthorizationConfigurationInput{})
	require.NoError(t, err)

	f.backend.Reset()

	_, err = c.GetConsoleAuthorizationConfiguration(t.Context(), &signinsdk.GetConsoleAuthorizationConfigurationInput{})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

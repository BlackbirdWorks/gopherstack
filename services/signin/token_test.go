package signin_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	signinsdk "github.com/aws/aws-sdk-go-v2/service/signin"
	"github.com/aws/aws-sdk-go-v2/service/signin/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

func tokenInput(grant string, mut func(*types.CreateOAuth2TokenRequestBody)) *signinsdk.CreateOAuth2TokenInput {
	b := &types.CreateOAuth2TokenRequestBody{ClientId: aws.String(testClient), GrantType: aws.String(grant)}
	mut(b)

	return &signinsdk.CreateOAuth2TokenInput{TokenInput: b}
}

func TestAuthorizationCodeFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut      func(*types.CreateOAuth2TokenRequestBody, string)
		name     string
		wantCode string
		wait     time.Duration
	}{
		{name: "redeem", mut: func(b *types.CreateOAuth2TokenRequestBody, c string) {
			b.Code, b.RedirectUri, b.CodeVerifier = aws.String(c), aws.String(testRedir), aws.String(verifier)
		}},
		{
			name:     "bad verifier",
			wantCode: "ValidationException",
			mut: func(b *types.CreateOAuth2TokenRequestBody, c string) {
				b.Code, b.RedirectUri = aws.String(c), aws.String(testRedir)
				b.CodeVerifier = aws.String("wrong-verifier-wrong-verifier-wrong-verifier-123")
			},
		},
		{
			name:     "bad redirect",
			wantCode: "ValidationException",
			mut: func(b *types.CreateOAuth2TokenRequestBody, c string) {
				b.Code, b.RedirectUri, b.CodeVerifier = aws.String(
					c,
				), aws.String(
					"http://evil.example/cb",
				), aws.String(
					verifier,
				)
			},
		},
		{
			name:     "unknown code",
			wantCode: "ValidationException",
			mut: func(b *types.CreateOAuth2TokenRequestBody, _ string) {
				b.Code, b.RedirectUri, b.CodeVerifier = aws.String("nope"), aws.String(testRedir), aws.String(verifier)
			},
		},
		{
			name:     "expired",
			wantCode: "AccessDeniedException",
			wait:     11 * time.Minute,
			mut: func(b *types.CreateOAuth2TokenRequestBody, c string) {
				b.Code, b.RedirectUri, b.CodeVerifier = aws.String(c), aws.String(testRedir), aws.String(verifier)
			},
		},
		{
			name:     "missing code",
			wantCode: "ValidationException",
			mut: func(b *types.CreateOAuth2TokenRequestBody, _ string) {
				b.RedirectUri, b.CodeVerifier = aws.String(testRedir), aws.String(verifier)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			code := f.authCode(t)
			f.clock.advance(tt.wait)

			out, err := f.self(t).
				CreateOAuth2Token(t.Context(), tokenInput("authorization_code", func(b *types.CreateOAuth2TokenRequestBody) {
					tt.mut(b, code)
				}))

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			body := out.TokenOutput
			assert.Equal(t, "aws_sigv4", aws.ToString(body.TokenType))
			assert.EqualValues(t, 900, aws.ToInt32(body.ExpiresIn))
			assert.True(t, len(aws.ToString(body.RefreshToken)) > 4 && aws.ToString(body.RefreshToken)[:4] == "ASOR")
			assert.NotEmpty(t, aws.ToString(body.IdToken))
			assert.Regexp(t, `^ASIA[A-Z2-7]{16}$`, aws.ToString(body.AccessToken.AccessKeyId))
		})
	}
}

func TestCodeReplayRevokesSession(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.self(t)
	code := f.authCode(t)
	in := tokenInput("authorization_code", func(b *types.CreateOAuth2TokenRequestBody) {
		b.Code, b.RedirectUri, b.CodeVerifier = aws.String(code), aws.String(testRedir), aws.String(verifier)
	})

	first, err := c.CreateOAuth2Token(t.Context(), in)
	require.NoError(t, err)

	_, err = c.CreateOAuth2Token(t.Context(), in)
	require.Error(t, err)
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = c.CreateOAuth2Token(t.Context(), tokenInput("refresh_token", func(b *types.CreateOAuth2TokenRequestBody) {
		b.RefreshToken = first.TokenOutput.RefreshToken
	}))
	require.Error(t, err)
	assert.Equal(t, "AccessDeniedException", apiCode(err))
}

func TestRefreshFlow(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.self(t)
	first, err := c.CreateOAuth2Token(
		t.Context(),
		tokenInput("authorization_code", func(b *types.CreateOAuth2TokenRequestBody) {
			b.Code, b.RedirectUri, b.CodeVerifier = aws.String(
				f.authCode(t),
			), aws.String(
				testRedir,
			), aws.String(
				verifier,
			)
		}),
	)
	require.NoError(t, err)

	refresh := func() (*signinsdk.CreateOAuth2TokenOutput, error) {
		return c.CreateOAuth2Token(
			t.Context(),
			tokenInput("refresh_token", func(b *types.CreateOAuth2TokenRequestBody) {
				b.RefreshToken = first.TokenOutput.RefreshToken
			}),
		)
	}

	f.clock.advance(5 * time.Minute)

	same, err := refresh()
	require.NoError(t, err)
	assert.Equal(
		t,
		aws.ToString(first.TokenOutput.AccessToken.AccessKeyId),
		aws.ToString(same.TokenOutput.AccessToken.AccessKeyId),
	)
	assert.EqualValues(t, 600, aws.ToInt32(same.TokenOutput.ExpiresIn))
	assert.Nil(t, same.TokenOutput.IdToken)

	f.clock.advance(11 * time.Minute)

	fresh, err := refresh()
	require.NoError(t, err)
	assert.NotEqual(
		t,
		aws.ToString(first.TokenOutput.AccessToken.AccessKeyId),
		aws.ToString(fresh.TokenOutput.AccessToken.AccessKeyId),
	)
	assert.EqualValues(t, 900, aws.ToInt32(fresh.TokenOutput.ExpiresIn))

	f.clock.advance(13 * time.Hour)

	_, err = refresh()
	require.Error(t, err)
	assert.Equal(t, "AccessDeniedException", apiCode(err))

	_, err = c.CreateOAuth2Token(t.Context(), tokenInput("password", func(*types.CreateOAuth2TokenRequestBody) {}))
	assert.Equal(t, "ValidationException", apiCode(err))
}

func TestIssuedCredentialsResolve(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	out, err := f.self(t).
		CreateOAuth2Token(t.Context(), tokenInput("authorization_code", func(b *types.CreateOAuth2TokenRequestBody) {
			b.Code, b.RedirectUri, b.CodeVerifier = aws.String(
				f.authCode(t),
			), aws.String(
				testRedir,
			), aws.String(
				verifier,
			)
		}))
	require.NoError(t, err)

	key, tok := aws.ToString(
		out.TokenOutput.AccessToken.AccessKeyId,
	), aws.ToString(
		out.TokenOutput.AccessToken.SessionToken,
	)

	p, ok := f.backend.ResolvePrincipal(t.Context(), key, tok)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:iam::"+testAccount+":root", p.Arn)

	_, ok = f.backend.ResolvePrincipal(t.Context(), key, "wrong")
	assert.False(t, ok)

	f.clock.advance(16 * time.Minute)

	_, ok = f.backend.ResolvePrincipal(t.Context(), key, tok)
	assert.False(t, ok)
}

func TestIAMTokenLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.self(t)

	tok, err := c.CreateOAuth2TokenWithIAM(t.Context(), &signinsdk.CreateOAuth2TokenWithIAMInput{
		GrantType: aws.String("client_credentials"), Resource: aws.String("aws-mcp.amazonaws.com"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Bearer", aws.ToString(tok.TokenType))
	assert.EqualValues(t, 3600, aws.ToInt32(tok.ExpiresIn))

	introspect := func(cl *signinsdk.Client, token string) *signinsdk.IntrospectOAuth2TokenWithIAMOutput {
		out, ierr := cl.IntrospectOAuth2TokenWithIAM(t.Context(), &signinsdk.IntrospectOAuth2TokenWithIAMInput{
			Token: aws.String(token), TokenTypeHint: aws.String("access_token"),
		})
		require.NoError(t, ierr)

		return out
	}

	got := introspect(c, aws.ToString(tok.AccessToken))
	assert.True(t, aws.ToBool(got.Active))
	assert.Equal(t, "aws-mcp.amazonaws.com", aws.ToString(got.Aud))
	assert.Equal(t, "signin.amazonaws.com", aws.ToString(got.Iss))
	assert.Equal(t, testAccount, aws.ToString(got.AccountId))
	assert.Equal(t, "arn:aws:iam::"+testAccount+":root", aws.ToString(got.Sub))
	assert.Equal(t, "access_token", aws.ToString(got.TokenType))
	assert.Equal(t, f.clock.t.Add(time.Hour).Unix(), aws.ToInt64(got.Exp))
	assert.Regexp(t, `^arn:aws:signin:us-east-1:123456789012:session/`, aws.ToString(got.SigninSession))

	other := introspect(f.client(t, "999999999999"), aws.ToString(tok.AccessToken))
	assert.False(t, aws.ToBool(other.Active))
	assert.Nil(t, other.Sub)

	assert.False(t, aws.ToBool(introspect(c, aws.ToString(tok.AccessToken)+"x").Active))
	assert.False(t, aws.ToBool(introspect(c, "garbage").Active))

	f.clock.advance(time.Hour)
	assert.False(t, aws.ToBool(introspect(c, aws.ToString(tok.AccessToken)).Active))
}

func TestRefreshTokenIntrospectAndRevoke(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.self(t)
	out, err := c.CreateOAuth2Token(
		t.Context(),
		tokenInput("authorization_code", func(b *types.CreateOAuth2TokenRequestBody) {
			b.Code, b.RedirectUri, b.CodeVerifier = aws.String(
				f.authCode(t),
			), aws.String(
				testRedir,
			), aws.String(
				verifier,
			)
		}),
	)
	require.NoError(t, err)

	refresh := aws.ToString(out.TokenOutput.RefreshToken)
	check := func() *signinsdk.IntrospectOAuth2TokenWithIAMOutput {
		got, ierr := c.IntrospectOAuth2TokenWithIAM(
			t.Context(),
			&signinsdk.IntrospectOAuth2TokenWithIAMInput{Token: aws.String(refresh)},
		)
		require.NoError(t, ierr)

		return got
	}

	got := check()
	assert.True(t, aws.ToBool(got.Active))
	assert.Equal(t, "refresh_token", aws.ToString(got.TokenType))
	assert.Equal(t, testClient, aws.ToString(got.ClientId))
	assert.Nil(t, got.Aud)

	_, err = c.RevokeOAuth2TokenWithIAM(
		t.Context(),
		&signinsdk.RevokeOAuth2TokenWithIAMInput{Token: aws.String("not-a-refresh-token")},
	)
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client(t, "999999999999").
		RevokeOAuth2TokenWithIAM(t.Context(), &signinsdk.RevokeOAuth2TokenWithIAMInput{Token: aws.String(refresh)})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(check().Active))

	for range 2 {
		_, err = c.RevokeOAuth2TokenWithIAM(
			t.Context(),
			&signinsdk.RevokeOAuth2TokenWithIAMInput{Token: aws.String(refresh)},
		)
		require.NoError(t, err)
	}

	assert.False(t, aws.ToBool(check().Active))
}

func TestIAMTokenErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.self(t)

	_, err := c.CreateOAuth2TokenWithIAM(t.Context(), &signinsdk.CreateOAuth2TokenWithIAMInput{
		GrantType: aws.String("password"), Resource: aws.String("r"),
	})
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = c.IntrospectOAuth2TokenWithIAM(t.Context(), &signinsdk.IntrospectOAuth2TokenWithIAMInput{
		Token: aws.String("x"), TokenTypeHint: aws.String("bogus"),
	})
	assert.Equal(t, "ValidationException", apiCode(err))
}

func TestAnonymousCallerDenied(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{
		Account: testAccount, Principal: &awsmeta.Principal{Kind: awsmeta.PrincipalKindAnonymous},
	})

	_, err := f.backend.CreateOAuth2TokenWithIAM(ctx, "client_credentials", "r")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "anonymous")
}

func TestIAMTokenUsesCallerPrincipal(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{
		Account: testAccount, Region: testRegion,
		Principal: &awsmeta.Principal{
			Kind: awsmeta.PrincipalKindAssumedRole, Arn: "arn:aws:sts::123456789012:assumed-role/R/s",
			AccountID: testAccount, UserID: "AROAEXAMPLE:s",
		},
	})

	tok, err := f.backend.CreateOAuth2TokenWithIAM(ctx, "client_credentials", "aws-mcp.amazonaws.com")
	require.NoError(t, err)

	got, err := f.backend.IntrospectOAuth2Token(ctx, tok.AccessToken, "")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sts::123456789012:assumed-role/R/s", got.Sub)
	assert.Equal(t, "AROAEXAMPLE:s", got.UserID)
}

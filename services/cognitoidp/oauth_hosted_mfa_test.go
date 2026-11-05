package cognitoidp_test

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

var sixDigits = regexp.MustCompile(`\b\d{6}\b`)

// hostedMFAEnv is an OAuth environment whose pool requires MFA and whose delivered codes are captured.
type hostedMFAEnv struct {
	*oauthEnv
	sender *recordingSender
	offset *atomic.Int64
	secret string
}

func newHostedMFAEnv(t *testing.T) *hostedMFAEnv {
	t.Helper()

	env := newOAuthEnv(t)
	sender := &recordingSender{}
	offset := &atomic.Int64{}

	env.backend.SetMessageSender(sender)
	env.backend.SetClock(func() time.Time { return time.Now().Add(time.Duration(offset.Load())) })

	return &hostedMFAEnv{oauthEnv: env, sender: sender, offset: offset}
}

func (e *hostedMFAEnv) requireMFA(t *testing.T) {
	t.Helper()

	require.NoError(t, e.backend.SetUserPoolMfaConfig(e.poolID, "ON"))
}

func (e *hostedMFAEnv) lastEmailCode(t *testing.T) string {
	t.Helper()

	e.sender.mu.Lock()
	defer e.sender.mu.Unlock()

	require.NotEmpty(t, e.sender.emails, "no email was delivered")

	m := e.sender.emails[len(e.sender.emails)-1]

	return sixDigits.FindString(m.Text + m.HTML)
}

func (e *hostedMFAEnv) lastSMSCode(t *testing.T) string {
	t.Helper()

	e.sender.mu.Lock()
	defer e.sender.mu.Unlock()

	require.NotEmpty(t, e.sender.sms, "no SMS was delivered")

	return sixDigits.FindString(e.sender.sms[len(e.sender.sms)-1].Message)
}

// submit posts a challenge answer and returns the response and decoded page.
func (e *hostedMFAEnv) submit(t *testing.T, step loginStep, name, code string) (*http.Response, string) {
	t.Helper()

	resp, body := e.postForm(t, step.loginURL, url.Values{
		"_csrf": {step.csrf}, "session": {step.session}, "challenge": {name}, "code": {code},
	}, nil)

	return resp, string(body)
}

func (e *hostedMFAEnv) page(t *testing.T, step loginStep, name, code string) (loginStep, string) {
	t.Helper()

	resp, html := e.submit(t, step, name, code)
	require.Equal(t, http.StatusOK, resp.StatusCode, html)

	return loginStep{
		loginURL: step.loginURL,
		csrf:     between(html, `name="_csrf" value="`, `"`),
		session:  between(html, `name="session" value="`, `"`),
	}, html
}

func (e *hostedMFAEnv) finish(t *testing.T, resp *http.Response) {
	t.Helper()

	code := codeFrom(t, resp)
	require.NotEmpty(t, code)

	tokens := e.exchange(t, code)
	assert.NotEmpty(t, tokens["access_token"])
	assert.NotEmpty(t, tokens["id_token"])
	assert.NotEmpty(t, e.sessionCookie(t))
}

func TestHostedLoginMFASetupEnrolsAuthenticator(t *testing.T) {
	t.Parallel()

	env := newHostedMFAEnv(t)
	env.requireMFA(t)
	q := authorizeQuery(env.webID, "")

	step, name := env.startChallenge(t, q, oauthUser, oauthPassword)
	require.Equal(t, "MFA_SETUP", name)

	step, html := env.page(t, step, name, "000000")
	assert.Contains(t, html, "not valid")

	secret := between(html, `id="totp-secret">`, `</code>`)
	require.NotEmpty(t, secret)

	uri := between(html, `id="totp-uri">`, `</code>`)
	parsed, err := url.Parse(strings.ReplaceAll(uri, "&amp;", "&"))
	require.NoError(t, err)
	assert.Equal(t, "otpauth", parsed.Scheme)
	assert.Equal(t, "totp", parsed.Host)
	assert.Equal(t, secret, parsed.Query().Get("secret"))
	assert.Equal(t, "oauth-pool", parsed.Query().Get("issuer"))
	assert.Contains(t, parsed.Path, oauthUser)

	code, err := cognitoidp.GenerateTOTPCode(secret, time.Now())
	require.NoError(t, err)

	resp, body := env.submit(t, step, name, code)
	require.Equal(t, http.StatusFound, resp.StatusCode, body)
	env.finish(t, resp)

	next, nextName := env.startChallenge(t, authorizeQuery(env.webID, ""), oauthUser, oauthPassword)
	assert.Equal(t, "SOFTWARE_TOKEN_MFA", nextName, "the enrolled authenticator is now the user's MFA")

	code, err = cognitoidp.GenerateTOTPCode(secret, time.Now())
	require.NoError(t, err)

	resp, body = env.submit(t, next, nextName, code)
	require.Equal(t, http.StatusFound, resp.StatusCode, body)
}

func TestHostedLoginSetupSecretSurvivesRetry(t *testing.T) {
	t.Parallel()

	env := newHostedMFAEnv(t)
	env.requireMFA(t)
	step, name := env.startChallenge(t, authorizeQuery(env.webID, ""), oauthUser, oauthPassword)

	step, first := env.page(t, step, name, "111111")
	_, second := env.page(t, step, name, "222222")

	assert.Equal(t, between(first, `id="totp-secret">`, `</code>`), between(second, `id="totp-secret">`, `</code>`))
}

func TestHostedLoginCodeChallenges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, e *hostedMFAEnv)
		code      func(t *testing.T, e *hostedMFAEnv, secret string) string
		name      string
		challenge string
	}{
		{
			name: "software token", challenge: "SOFTWARE_TOKEN_MFA",
			setup: func(t *testing.T, e *hostedMFAEnv) {
				t.Helper()

				tokens := e.userTokens(t)
				secret, _, err := e.backend.AssociateSoftwareToken(tokens.AccessToken, "")
				require.NoError(t, err)

				code, err := cognitoidp.GenerateTOTPCode(secret, time.Now())
				require.NoError(t, err)
				_, err = e.backend.VerifySoftwareToken(tokens.AccessToken, "", code)
				require.NoError(t, err)

				e.secret = secret
			},
			code: func(t *testing.T, _ *hostedMFAEnv, secret string) string {
				t.Helper()

				code, err := cognitoidp.GenerateTOTPCode(secret, time.Now())
				require.NoError(t, err)

				return code
			},
		},
		{
			name: "sms", challenge: "SMS_MFA",
			setup: func(t *testing.T, e *hostedMFAEnv) {
				t.Helper()

				require.NoError(t, e.backend.AdminSetUserMFAPreference(e.poolID, oauthUser, true, false, "SMS_MFA"))
			},
			code: func(t *testing.T, e *hostedMFAEnv, _ string) string { t.Helper(); return e.lastSMSCode(t) },
		},
		{
			name: "email", challenge: "EMAIL_OTP",
			setup: func(t *testing.T, e *hostedMFAEnv) {
				t.Helper()

				require.NoError(t, e.backend.AdminSetUserMFAPreference(
					e.poolID, oauthUser, false, false, "EMAIL_OTP", "EMAIL_OTP"))
			},
			code: func(t *testing.T, e *hostedMFAEnv, _ string) string { t.Helper(); return e.lastEmailCode(t) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newHostedMFAEnv(t)
			tt.setup(t, env)
			env.requireMFA(t)

			step, name := env.startChallenge(t, authorizeQuery(env.webID, ""), oauthUser, oauthPassword)
			require.Equal(t, tt.challenge, name)

			code := tt.code(t, env, env.secret)
			require.Len(t, code, 6)

			wrong := "000000"
			if code == wrong {
				wrong = "111111"
			}

			step, html := env.page(t, step, name, wrong)
			assert.Contains(t, html, "not valid")

			resp, body := env.submit(t, step, name, code)
			require.Equal(t, http.StatusFound, resp.StatusCode, body)
			env.finish(t, resp)
		})
	}
}

func TestHostedLoginChallengeExpires(t *testing.T) {
	t.Parallel()

	env := newHostedMFAEnv(t)
	env.requireMFA(t)
	require.NoError(t, env.backend.AdminSetUserMFAPreference(env.poolID, oauthUser, true, false, "SMS_MFA"))

	step, name := env.startChallenge(t, authorizeQuery(env.webID, ""), oauthUser, oauthPassword)
	code := env.lastSMSCode(t)

	env.offset.Store(int64(10 * time.Minute))

	resp, body := env.submit(t, step, name, code)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, body, "sign in again")
	assert.Empty(t, resp.Header.Get("Location"))
}

func TestHostedLoginNewPasswordThenMFA(t *testing.T) {
	t.Parallel()

	env := newHostedMFAEnv(t)
	env.requireMFA(t)
	_, err := env.backend.AdminCreateUser(env.poolID, "temp-user", "Temp1234!", map[string]string{"email": "t@example.com"})
	require.NoError(t, err)

	step, name := env.startChallenge(t, authorizeQuery(env.webID, ""), "temp-user", "Temp1234!")
	require.Equal(t, "NEW_PASSWORD_REQUIRED", name)

	resp, body := env.postForm(t, step.loginURL, url.Values{
		"_csrf": {step.csrf}, "session": {step.session}, "challenge": {name}, "new_password": {"Perm5678!x"},
	}, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	html := string(body)
	require.Equal(t, "MFA_SETUP", between(html, `name="challenge" value="`, `"`), "MFA follows the password change")
	assert.NotEmpty(t, between(html, `id="totp-secret">`, `</code>`))
	assert.Empty(t, env.sessionCookie(t))
}

func TestHostedLoginMFASetupAbsentWhenMFAOff(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)
	resp := env.login(t, authorizeQuery(env.webID, ""))
	assert.NotEmpty(t, codeFrom(t, resp))
}

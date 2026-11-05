package cognitoidp

import (
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

// hostedLogin is the outcome of a hosted password check: a signed-in user, or a challenge to answer.
// Secret and SetupURI carry the TOTP enrolment details of an MFA_SETUP challenge.
type hostedLogin struct {
	Username      string
	ChallengeName string
	Session       string
	Secret        string
	SetupURI      string
}

type challengePage struct {
	Error    string
	CSRF     string
	Name     string
	Session  string
	Action   string
	Heading  string
	Hint     string
	Secret   string
	SetupURI string
}

//nolint:gochecknoglobals // parsed once
var challengeTemplate = template.Must(template.New("challenge").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>{{.Heading}}</title></head>
<body><h1>{{.Heading}}</h1>
{{if .Error}}<p role="alert">{{.Error}}</p>{{end}}
{{if .Hint}}<p>{{.Hint}}</p>{{end}}
{{if .Secret}}<p>Setup key: <code id="totp-secret">{{.Secret}}</code></p>
<p>Authenticator URI: <code id="totp-uri">{{.SetupURI}}</code></p>{{end}}
<form method="post" action="{{.Action}}">
<input type="hidden" name="_csrf" value="{{.CSRF}}">
<input type="hidden" name="session" value="{{.Session}}">
<input type="hidden" name="challenge" value="{{.Name}}">
{{if eq .Name "NEW_PASSWORD_REQUIRED"}}<label>New password
<input type="password" name="new_password" autocomplete="new-password"></label>
{{else}}<label>Code <input name="code" inputmode="numeric" autocomplete="one-time-code"></label>
{{end}}<button type="submit">{{if eq .Name "MFA_SETUP"}}Verify and sign in{{else}}Continue{{end}}</button>
</form></body></html>`))

func challengeCopy(name string) (string, string) {
	switch name {
	case challengeNewPasswordRequired:
		return "Set a new password", ""
	case challengeMFASetup:
		return "Set up an authenticator app", "Add the setup key to your authenticator app, then enter the code it shows."
	case challengeSoftwareTokenMFA:
		return "Enter your authenticator code", "Enter the code shown in your authenticator app."
	case challengeSMSMFA:
		return "Enter your text message code", "Enter the code we sent by text message."
	case challengeEmailOTP:
		return "Enter your email code", "Enter the code we sent to your email address."
	default:
		return "Verification required", ""
	}
}

// hostedChallengeLocked decides whether a verified user must answer a challenge before signing in.
func (b *InMemoryBackend) hostedChallengeLocked(pool *UserPool, clientID string, user *User) (hostedLogin, error) {
	if user.Status == UserStatusForceChangePassword {
		if tempPasswordExpired(pool, user) {
			return hostedLogin{}, fmt.Errorf("%w: temporary password has expired", ErrNotAuthorized)
		}

		res := b.newMFASession(pool, clientID, user.Username, challengeNewPasswordRequired)

		return hostedLogin{Username: user.Username, ChallengeName: challengeNewPasswordRequired, Session: res.MFASession}, nil
	}

	return b.hostedMFAChallengeLocked(pool, clientID, user)
}

// hostedMFAChallengeLocked starts the pool's MFA challenge for the user, if one applies.
func (b *InMemoryBackend) hostedMFAChallengeLocked(pool *UserPool, clientID string, user *User) (hostedLogin, error) {
	if !mfaGateApplies(pool, user) {
		return hostedLogin{Username: user.Username}, nil
	}

	name := mfaChallengeType(pool, user)
	switch name {
	case challengeSMSMFA, challengeSoftwareTokenMFA, challengeEmailOTP, challengeMFASetup:
	default:
		return hostedLogin{}, errHostedChallenge
	}

	res := b.newMFASession(pool, clientID, user.Username, name)
	next := hostedLogin{Username: user.Username, ChallengeName: name, Session: res.MFASession}

	if name == challengeMFASetup {
		secret, err := b.hostedSetupSecretLocked(pool, user)
		if err != nil {
			return hostedLogin{}, err
		}

		next.Secret, next.SetupURI = secret, totpSetupURI(pool, user, secret)
	}

	return next, nil
}

// hostedSetupSecretLocked keeps an unverified secret across re-renders so a scanned key stays valid.
func (b *InMemoryBackend) hostedSetupSecretLocked(_ *UserPool, user *User) (string, error) {
	if user.TOTPSecret != "" && !user.TOTPVerified {
		return user.TOTPSecret, nil
	}

	return associateTOTPLocked(user)
}

func totpSetupURI(pool *UserPool, user *User, secret string) string {
	issuer := pool.Name
	if issuer == "" {
		issuer = pool.ID
	}

	q := url.Values{"secret": {secret}, "issuer": {issuer}}

	return "otpauth://totp/" + url.PathEscape(issuer+":"+user.Username) + "?" + q.Encode()
}

// hostedSetupDetails returns the enrolment key of a pending MFA_SETUP session.
func (b *InMemoryBackend) hostedSetupDetails(session string) (string, string) {
	b.mu.RLock("HostedSetupDetails")
	defer b.mu.RUnlock()

	entry, ok := b.mfaSessions[session]
	if !ok || entry.ChallengeType != challengeMFASetup {
		return "", ""
	}

	pool, poolOK := b.pools.Get(entry.PoolID)
	user, userOK := b.users.Get(userKey(entry.PoolID, entry.Username))

	if !poolOK || !userOK || user.TOTPSecret == "" {
		return "", ""
	}

	return user.TOTPSecret, totpSetupURI(pool, user, user.TOTPSecret)
}

// hostedRetryable is a wrong answer the page may offer another attempt for.
func hostedRetryable(err error) bool {
	return isPasswordRejection(err) || errors.Is(err, ErrCodeMismatch)
}

// oauthRespondChallenge verifies a hosted challenge answer; the result is the signed-in user, or the
// next challenge (MFA after a new password).
func (b *InMemoryBackend) oauthRespondChallenge(clientID, session, name, answer string) (hostedLogin, error) {
	b.mu.Lock("OAuthRespondChallenge")
	defer b.mu.Unlock()

	entry, ok := b.mfaSessions[session]
	if !ok || !entry.ExpiresAt.After(b.now()) {
		delete(b.mfaSessions, session)

		return hostedLogin{}, fmt.Errorf("%w: session not found or expired", ErrNotAuthorized)
	}

	if entry.ClientID != clientID || entry.ChallengeType != name {
		delete(b.mfaSessions, session)

		return hostedLogin{}, fmt.Errorf("%w: session does not match this challenge", ErrNotAuthorized)
	}

	pool, ok := b.pools.Get(entry.PoolID)
	user, userOK := b.users.Get(userKey(entry.PoolID, entry.Username))

	if !ok || !userOK || !user.Enabled {
		delete(b.mfaSessions, session)

		return hostedLogin{}, fmt.Errorf("%w: user is not available", ErrNotAuthorized)
	}

	if err := b.applyHostedAnswerLocked(pool, user, entry, answer); err != nil {
		if !hostedRetryable(err) {
			delete(b.mfaSessions, session)
		}

		return hostedLogin{}, err
	}

	delete(b.mfaSessions, session)

	if name == challengeNewPasswordRequired {
		return b.hostedMFAChallengeLocked(pool, clientID, user)
	}

	return hostedLogin{Username: user.Username}, nil
}

func (b *InMemoryBackend) applyHostedAnswerLocked(
	pool *UserPool, user *User, entry *mfaSessionEntry, answer string,
) error {
	switch entry.ChallengeType {
	case challengeNewPasswordRequired:
		if answer == "" {
			return fmt.Errorf("%w: new password is required", ErrInvalidPassword)
		}

		if err := validatePassword(pool.PasswordPolicy, answer); err != nil {
			return err
		}

		return setPermanentPasswordLocked(pool, user, answer)

	case challengeMFASetup:
		return b.completeHostedMFASetupLocked(user, answer)

	default:
		return b.verifyMFAChallengeCode(entry, user, answer)
	}
}

// completeHostedMFASetupLocked enrols the TOTP secret shown on the setup page once the user proves they have it.
func (b *InMemoryBackend) completeHostedMFASetupLocked(user *User, code string) error {
	if user.TOTPSecret == "" || !verifyTOTPCode(user.TOTPSecret, code, b.now()) {
		return fmt.Errorf("%w: invalid software token code", ErrCodeMismatch)
	}

	user.TOTPVerified = true

	return b.applyMFAPreferenceLocked(user, false, true, challengeSoftwareTokenMFA)
}

func (h *Handler) answerHostedChallenge(c *echo.Context, req *authorizeRequest, q url.Values) error {
	form := c.Request().PostForm
	name, session := form.Get("challenge"), form.Get("session")

	answer := form.Get("code")
	if name == challengeNewPasswordRequired {
		answer = form.Get("new_password")
	}

	next, err := h.Backend.oauthRespondChallenge(req.client.ClientID, session, name, answer)

	switch {
	case hostedRetryable(err):
		retry := hostedLogin{ChallengeName: name, Session: session}
		retry.Secret, retry.SetupURI = h.Backend.hostedSetupDetails(session)

		return renderChallenge(c, q, http.StatusOK, retry, loginMessage(err))
	case err != nil:
		return renderLogin(c, q, http.StatusOK, "That did not work. Please sign in again.")
	case next.ChallengeName != "":
		return renderChallenge(c, q, http.StatusOK, next, "")
	}

	return h.finishLogin(c, req, next.Username)
}

func renderChallenge(c *echo.Context, q url.Values, status int, ch hostedLogin, msg string) error {
	token, err := newCSRFCookie(c)
	if err != nil {
		return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
	}

	beginHTML(c, status)

	heading, hint := challengeCopy(ch.ChallengeName)

	return challengeTemplate.Execute(c.Response(), challengePage{
		Error: msg, CSRF: token, Name: ch.ChallengeName, Session: ch.Session,
		Action:  loginPath(c.Request()) + "?" + q.Encode(),
		Heading: heading, Hint: hint, Secret: ch.Secret, SetupURI: ch.SetupURI,
	})
}

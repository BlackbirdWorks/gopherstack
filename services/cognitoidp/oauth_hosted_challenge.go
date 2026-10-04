package cognitoidp

import (
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"time"

	"github.com/labstack/echo/v5"
)

// hostedLogin is the outcome of a hosted password check: a signed-in user, or a challenge to answer.
type hostedLogin struct {
	Username      string
	ChallengeName string
	Session       string
}

//nolint:gochecknoglobals // parsed once
var challengeTemplate = template.Must(template.New("challenge").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Verify</title></head>
<body><h1>{{if eq .Name "NEW_PASSWORD_REQUIRED"}}Set a new password{{else}}Verification required{{end}}</h1>
{{if .Error}}<p role="alert">{{.Error}}</p>{{end}}
<form method="post" action="{{.Action}}">
<input type="hidden" name="_csrf" value="{{.CSRF}}">
<input type="hidden" name="session" value="{{.Session}}">
<input type="hidden" name="challenge" value="{{.Name}}">
{{if eq .Name "NEW_PASSWORD_REQUIRED"}}<label>New password
<input type="password" name="new_password" autocomplete="new-password"></label>
{{else}}<label>Code <input name="code" inputmode="numeric" autocomplete="one-time-code"></label>
{{end}}<button type="submit">Continue</button>
</form></body></html>`))

// hostedChallengeLocked decides whether a verified user must answer a challenge before signing in.
func (b *InMemoryBackend) hostedChallengeLocked(pool *UserPool, clientID string, user *User) (hostedLogin, error) {
	var name string

	switch {
	case user.Status == UserStatusForceChangePassword:
		if tempPasswordExpired(pool, user) {
			return hostedLogin{}, fmt.Errorf("%w: temporary password has expired", ErrNotAuthorized)
		}

		name = challengeNewPasswordRequired
	case pool.MfaConfiguration == "ON" || pool.MfaConfiguration == "OPTIONAL":
		name = mfaChallengeType(pool, user)
		if name != challengeSMSMFA && name != challengeSoftwareTokenMFA {
			return hostedLogin{}, errHostedChallenge
		}
	default:
		return hostedLogin{Username: user.Username}, nil
	}

	res := b.newMFASession(pool, clientID, user.Username, name)

	return hostedLogin{Username: user.Username, ChallengeName: name, Session: res.MFASession}, nil
}

// oauthRespondChallenge verifies a hosted challenge answer and returns the signed-in username.
func (b *InMemoryBackend) oauthRespondChallenge(clientID, session, name, answer string) (string, error) {
	b.mu.Lock("OAuthRespondChallenge")
	defer b.mu.Unlock()

	entry, ok := b.mfaSessions[session]
	if !ok || !entry.ExpiresAt.After(time.Now()) {
		delete(b.mfaSessions, session)

		return "", fmt.Errorf("%w: session not found or expired", ErrNotAuthorized)
	}

	if entry.ClientID != clientID || entry.ChallengeType != name {
		delete(b.mfaSessions, session)

		return "", fmt.Errorf("%w: session does not match this challenge", ErrNotAuthorized)
	}

	pool, ok := b.pools.Get(entry.PoolID)
	user, userOK := b.users.Get(userKey(entry.PoolID, entry.Username))

	if !ok || !userOK || !user.Enabled {
		delete(b.mfaSessions, session)

		return "", fmt.Errorf("%w: user is not available", ErrNotAuthorized)
	}

	if err := b.applyHostedAnswerLocked(pool, user, entry, answer); err != nil {
		if !errors.Is(err, ErrInvalidPassword) {
			delete(b.mfaSessions, session)
		}

		return "", err
	}

	delete(b.mfaSessions, session)

	return user.Username, nil
}

func (b *InMemoryBackend) applyHostedAnswerLocked(
	pool *UserPool, user *User, entry *mfaSessionEntry, answer string,
) error {
	if entry.ChallengeType == challengeNewPasswordRequired {
		if answer == "" {
			return fmt.Errorf("%w: new password is required", ErrInvalidPassword)
		}

		if err := validatePassword(pool.PasswordPolicy, answer); err != nil {
			return err
		}

		return setPermanentPasswordLocked(user, answer)
	}

	return verifyMFAChallengeCode(entry, user, answer)
}

func (h *Handler) answerHostedChallenge(c *echo.Context, req *authorizeRequest, q url.Values) error {
	form := c.Request().PostForm
	name, session := form.Get("challenge"), form.Get("session")

	answer := form.Get("code")
	if name == challengeNewPasswordRequired {
		answer = form.Get("new_password")
	}

	username, err := h.Backend.oauthRespondChallenge(req.client.ClientID, session, name, answer)

	switch {
	case errors.Is(err, ErrInvalidPassword):
		return renderChallenge(
			c,
			q,
			http.StatusOK,
			hostedLogin{ChallengeName: name, Session: session},
			loginMessage(err),
		)
	case err != nil:
		return renderLogin(c, q, http.StatusOK, "That did not work. Please sign in again.")
	}

	return h.finishLogin(c, req, username)
}

func renderChallenge(c *echo.Context, q url.Values, status int, ch hostedLogin, msg string) error {
	token, err := newCSRFCookie(c)
	if err != nil {
		return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
	}

	beginHTML(c, status)

	return challengeTemplate.Execute(c.Response(), map[string]string{
		"Error": msg, "CSRF": token, "Name": ch.ChallengeName, "Session": ch.Session,
		"Action": pathLogin + "?" + q.Encode(),
	})
}

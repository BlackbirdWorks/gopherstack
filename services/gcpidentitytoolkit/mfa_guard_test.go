package gcpidentitytoolkit_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type totpEnrolled struct {
	secret string
	id     string
}

func (e *env) enrollTotp(t *testing.T, idTok string) totpEnrolled {
	t.Helper()

	start := e.mfaPost(
		t, "mfaEnrollment:start", map[string]any{"idToken": idTok, "totpEnrollmentInfo": map[string]any{}},
	)
	require.Equal(t, http.StatusOK, start.status, start.body)

	info, _ := start.body["totpSessionInfo"].(map[string]any)
	secret, _ := info["sharedSecretKey"].(string)

	fin := e.mfaPost(t, "mfaEnrollment:finalize", map[string]any{
		"idToken": idTok,
		"totpVerificationInfo": map[string]any{
			"sessionInfo": info["sessionInfo"], "verificationCode": refTOTP(t, secret, e.clock.Now()),
		},
	})
	require.Equal(t, http.StatusOK, fin.status, fin.body)

	lk := e.client(t, "lookup", map[string]any{"idToken": fin.str("idToken")})
	users, _ := lk.body["users"].([]any)
	mfa, _ := users[0].(map[string]any)["mfaInfo"].([]any)
	id, _ := mfa[0].(map[string]any)["mfaEnrollmentId"].(string)

	return totpEnrolled{secret: secret, id: id}
}

func (e *env) totpFinalize(t *testing.T, pending, enrollID, code string) result {
	t.Helper()

	return e.mfaPost(t, "mfaSignIn:finalize", map[string]any{
		"mfaPendingCredential": pending, "mfaEnrollmentId": enrollID,
		"totpVerificationInfo": map[string]any{"verificationCode": code},
	})
}

func TestMfaTotpCodeCannotBeReplayed(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mfaConfig(t)

	en := e.enrollTotp(t, e.verifiedUser(t, "replay@example.com"))
	code := refTOTP(t, en.secret, e.clock.Now())

	pend := e.signIn(t, "replay@example.com").str("mfaPendingCredential")
	e.assertErr(t, e.totpFinalize(t, pend, en.id, code), http.StatusBadRequest, "INVALID_CODE")

	e.clock.Advance(30 * time.Second)

	next := refTOTP(t, en.secret, e.clock.Now())
	require.Equal(t, http.StatusOK, e.totpFinalize(t, pend, en.id, next).status)

	pend = e.signIn(t, "replay@example.com").str("mfaPendingCredential")
	e.assertErr(t, e.totpFinalize(t, pend, en.id, next), http.StatusBadRequest, "INVALID_CODE")
}

func TestMfaCodeAttemptsBounded(t *testing.T) {
	t.Parallel()

	type guessFns func() result

	tests := []struct {
		run  func(t *testing.T, e *env) (guessFns, guessFns)
		name string
	}{
		{name: "totp sign in", run: func(t *testing.T, e *env) (guessFns, guessFns) {
			t.Helper()

			en := e.enrollTotp(t, e.verifiedUser(t, "att@example.com"))
			pend := e.signIn(t, "att@example.com").str("mfaPendingCredential")
			e.clock.Advance(30 * time.Second)

			return func() result { return e.totpFinalize(t, pend, en.id, "000000") },
				func() result { return e.totpFinalize(t, pend, en.id, refTOTP(t, en.secret, e.clock.Now())) }
		}},
		{name: "sms enrollment", run: func(t *testing.T, e *env) (guessFns, guessFns) {
			t.Helper()

			idTok := e.verifiedUser(t, "att@example.com")
			start := e.mfaPost(t, "mfaEnrollment:start", map[string]any{
				"idToken":             idTok,
				"phoneEnrollmentInfo": map[string]any{"phoneNumber": "+15555550100", "recaptchaToken": "t"},
			})
			require.Equal(t, http.StatusOK, start.status, start.body)

			session := start.body["phoneSessionInfo"].(map[string]any)["sessionInfo"]
			fin := func(code string) result {
				return e.mfaPost(t, "mfaEnrollment:finalize", map[string]any{
					"idToken":               idTok,
					"phoneVerificationInfo": map[string]any{"sessionInfo": session, "code": code},
				})
			}

			return func() result { return fin("000000") }, func() result { return fin("123456") }
		}},
		{name: "phone sign in", run: func(t *testing.T, e *env) (guessFns, guessFns) {
			t.Helper()

			s := e.client(
				t, "sendVerificationCode", map[string]any{"phoneNumber": "+15555550100", "recaptchaToken": "t"},
			)
			require.Equal(t, http.StatusOK, s.status, s.body)

			session := s.str("sessionInfo")
			fin := func(code string) result {
				return e.client(t, "signInWithPhoneNumber", map[string]any{"sessionInfo": session, "code": code})
			}

			return func() result { return fin("000000") }, func() result { return fin("123456") }
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.mfaConfig(t)

			guess, valid := tt.run(t, e)

			for range 4 {
				e.assertErr(t, guess(), http.StatusBadRequest, "INVALID_CODE")
			}

			e.assertErr(t, guess(), http.StatusBadRequest, "TOO_MANY_ATTEMPTS_TRY_LATER")

			r := valid()
			assert.Equal(t, http.StatusBadRequest, r.status, "the correct code is refused once the session is burned")
			assert.NotEqual(t, "INVALID_CODE", r.errMsg())
		})
	}
}

func TestMfaRoutesFollowTheRealAPI(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mfaConfig(t)

	r := e.do(t, http.MethodPost, "/v2/projects/"+e.project+"/accounts/mfaEnrollment:start?key=k", "",
		map[string]any{"idToken": e.verifiedUser(t, "route@example.com"), "totpEnrollmentInfo": map[string]any{}})
	assert.NotEqual(t, http.StatusOK, r.status, "the real API has no project-scoped MFA path")
}

func TestMfaSessionsAreBounded(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mfaConfig(t)

	idTok := e.verifiedUser(t, "bound@example.com")

	var first any

	for i := range 12 {
		s := e.mfaPost(
			t, "mfaEnrollment:start", map[string]any{"idToken": idTok, "totpEnrollmentInfo": map[string]any{}},
		)
		require.Equal(t, http.StatusOK, s.status, s.body)

		e.clock.Advance(time.Second)

		if i == 0 {
			first = s.body["totpSessionInfo"].(map[string]any)["sessionInfo"]
		}
	}

	r := e.mfaPost(t, "mfaEnrollment:finalize", map[string]any{
		"idToken":              idTok,
		"totpVerificationInfo": map[string]any{"sessionInfo": first, "verificationCode": "000000"},
	})
	e.assertErr(t, r, http.StatusBadRequest, "INVALID_SESSION_INFO")
}

func TestMfaCredentialsNotListedAsOobCodes(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mfaConfig(t)

	e.enrollTotp(t, e.verifiedUser(t, "oob@example.com"))
	require.NotEmpty(t, e.signIn(t, "oob@example.com").str("mfaPendingCredential"))

	r := e.do(t, http.MethodGet, "/emulator/v1/projects/"+e.project+"/oobCodes", "", nil)
	codes, _ := r.body["oobCodes"].([]any)
	assert.Empty(t, codes)
}

func TestMfaAdminEnrollmentsCapped(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mfaConfig(t)

	infos := make([]any, 8)
	for i := range infos {
		infos[i] = map[string]any{"phoneInfo": "+1555555010" + string(rune('0'+i))}
	}

	r := e.do(t, http.MethodPost, "/v1/projects/"+e.project+"/accounts", adminAuth, map[string]any{
		"email": "cap@example.com", "password": testPassword, "emailVerified": true, "mfaInfo": infos,
	})
	require.Equal(t, http.StatusOK, r.status, r.body)

	lk := e.admin(t, "lookup", map[string]any{"email": []string{"cap@example.com"}})
	users, _ := lk.body["users"].([]any)
	mfa, _ := users[0].(map[string]any)["mfaInfo"].([]any)
	assert.Len(t, mfa, 5)
}

package gcpidentitytoolkit_test

import (
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPhone = "+15555550100"

func (e *env) verificationCodes(t *testing.T) []map[string]any {
	t.Helper()

	r := e.do(t, http.MethodGet, "/emulator/v1/projects/"+e.project+"/verificationCodes", "", nil)
	require.Equal(t, http.StatusOK, r.status, r.body)

	raw, _ := r.body["verificationCodes"].([]any)
	out := make([]map[string]any, 0, len(raw))

	for _, c := range raw {
		out = append(out, c.(map[string]any))
	}

	return out
}

func (e *env) sendPhoneCode(t *testing.T, phone string) result {
	t.Helper()

	return e.client(t, "sendVerificationCode", map[string]any{"phoneNumber": phone, "recaptchaToken": "tok"})
}

func TestSendVerificationCodeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
		err  string
	}{
		{name: "missing phone", body: map[string]any{"recaptchaToken": "t"}, err: "MISSING_PHONE_NUMBER"},
		{name: "bad format", body: map[string]any{"phoneNumber": "555", "recaptchaToken": "t"},
			err: "INVALID_PHONE_NUMBER : Invalid format."},
		{
			name: "no client identifier",
			body: map[string]any{"phoneNumber": testPhone},
			err:  "MISSING_CLIENT_IDENTIFIER",
		},
		{name: "ios pair", body: map[string]any{"phoneNumber": testPhone, "iosReceipt": "r", "iosSecret": "s"}},
		{name: "ios receipt alone", body: map[string]any{"phoneNumber": testPhone, "iosReceipt": "r"},
			err: "MISSING_CLIENT_IDENTIFIER"},
		{name: "play integrity", body: map[string]any{"phoneNumber": testPhone, "playIntegrityToken": "p"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			r := e.client(t, "sendVerificationCode", tt.body)

			if tt.err != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.err)

				return
			}

			require.Equal(t, http.StatusOK, r.status, r.body)
			assert.NotEmpty(t, r.str("sessionInfo"))
		})
	}
}

func TestPhoneSignInFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	send := e.sendPhoneCode(t, testPhone)
	require.Equal(t, http.StatusOK, send.status, send.body)

	codes := e.verificationCodes(t)
	require.Len(t, codes, 1)
	assert.Equal(t, testPhone, codes[0]["phoneNumber"])
	assert.Equal(t, send.str("sessionInfo"), codes[0]["sessionInfo"])
	assert.Empty(t, e.oobCodes(t), "phone sessions are not out-of-band codes")

	session, code := send.str("sessionInfo"), codes[0]["code"].(string)

	tests := []struct {
		body map[string]any
		name string
		err  string
	}{
		{name: "missing session", body: map[string]any{"code": code}, err: "MISSING_SESSION_INFO"},
		{name: "unknown session", body: map[string]any{"sessionInfo": "x", "code": code}, err: "INVALID_SESSION_INFO"},
		{name: "missing code", body: map[string]any{"sessionInfo": session}, err: "MISSING_CODE"},
		{name: "wrong code", body: map[string]any{"sessionInfo": session, "code": "abcdef"}, err: "INVALID_CODE"},
	}

	for _, tt := range tests {
		e.assertErr(t, e.client(t, "signInWithPhoneNumber", tt.body), http.StatusBadRequest, tt.err)
	}

	ok := e.client(t, "signInWithPhoneNumber", map[string]any{"sessionInfo": session, "code": code})
	require.Equal(t, http.StatusOK, ok.status, ok.body)
	assert.Equal(t, testPhone, ok.str("phoneNumber"))
	assert.NotEmpty(t, ok.str("idToken"))
	assert.NotEmpty(t, ok.str("refreshToken"))
	assert.Equal(t, true, ok.body["isNewUser"])

	lookup := e.client(t, "lookup", map[string]any{"idToken": ok.str("idToken")})
	require.Equal(t, http.StatusOK, lookup.status, lookup.body)

	users, _ := lookup.body["users"].([]any)
	require.Len(t, users, 1)
	assert.Equal(t, testPhone, users[0].(map[string]any)["phoneNumber"])

	reuse := e.client(t, "signInWithPhoneNumber", map[string]any{"sessionInfo": session, "code": code})
	e.assertErr(t, reuse, http.StatusBadRequest, "INVALID_SESSION_INFO")

	second := e.sendPhoneCode(t, testPhone)
	next := e.verificationCodes(t)[0]["code"].(string)
	again := e.client(t, "signInWithPhoneNumber",
		map[string]any{"sessionInfo": second.str("sessionInfo"), "code": next})
	require.Equal(t, http.StatusOK, again.status, again.body)
	assert.Equal(t, ok.str("localId"), again.str("localId"))
	assert.Nil(t, again.body["isNewUser"], "an existing user is not new")
}

func TestPhoneCodeExpires(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	send := e.sendPhoneCode(t, testPhone)
	code := e.verificationCodes(t)[0]["code"].(string)

	e.clock.Advance(6 * time.Minute)

	r := e.client(t, "signInWithPhoneNumber", map[string]any{"sessionInfo": send.str("sessionInfo"), "code": code})
	e.assertErr(t, r, http.StatusBadRequest, "SESSION_EXPIRED")
}

func TestPhoneTestNumbersUseFixedCode(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.setConfig(t, map[string]any{"signIn": map[string]any{"phoneNumber": map[string]any{
		"enabled": true, "testPhoneNumbers": map[string]string{testPhone: "123456"},
	}}})

	send := e.sendPhoneCode(t, testPhone)
	require.Equal(t, http.StatusOK, send.status, send.body)

	r := e.client(t, "signInWithPhoneNumber", map[string]any{"sessionInfo": send.str("sessionInfo"), "code": "123456"})
	require.Equal(t, http.StatusOK, r.status, r.body)
}

func TestPhoneSignInDisabled(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.setConfig(t, map[string]any{"signIn": map[string]any{"phoneNumber": map[string]any{"enabled": false}}})

	r := e.sendPhoneCode(t, testPhone)
	e.assertErr(
		t,
		r,
		http.StatusBadRequest,
		"OPERATION_NOT_ALLOWED : Phone number sign-in is disabled for this project.",
	)
}

func TestPhoneLinkAndTemporaryProof(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	signIn := func(phone string, extra map[string]any) result {
		send := e.sendPhoneCode(t, phone)
		require.Equal(t, http.StatusOK, send.status, send.body)

		var code string

		for _, c := range e.verificationCodes(t) {
			if c["sessionInfo"] == send.str("sessionInfo") {
				code = c["code"].(string)
			}
		}

		body := map[string]any{"sessionInfo": send.str("sessionInfo"), "code": code}
		maps.Copy(body, extra)

		return e.client(t, "signInWithPhoneNumber", body)
	}

	owner := signIn(testPhone, nil)
	require.Equal(t, http.StatusOK, owner.status, owner.body)

	pw := e.signUp(t, "link@example.com")

	clash := signIn(testPhone, map[string]any{"idToken": pw.str("idToken")})
	require.Equal(t, http.StatusOK, clash.status, clash.body)
	assert.Empty(t, clash.str("idToken"))
	assert.NotEmpty(t, clash.str("temporaryProof"))
	assert.Equal(t, "300", clash.str("temporaryProofExpiresIn"))
	assert.Equal(t, testPhone, clash.str("phoneNumber"))

	bad := e.client(t, "signInWithPhoneNumber",
		map[string]any{"temporaryProof": clash.str("temporaryProof"), "phoneNumber": "+15555550101"})
	e.assertErr(
		t,
		bad,
		http.StatusBadRequest,
		"INVALID_PHONE_NUMBER : The phone number does not match the temporary proof.",
	)

	viaProof := e.client(t, "signInWithPhoneNumber",
		map[string]any{"temporaryProof": clash.str("temporaryProof"), "phoneNumber": testPhone})
	require.Equal(t, http.StatusOK, viaProof.status, viaProof.body)
	assert.Equal(t, owner.str("localId"), viaProof.str("localId"))

	linked := signIn("+15555550102", map[string]any{"idToken": pw.str("idToken")})
	require.Equal(t, http.StatusOK, linked.status, linked.body)
	assert.Equal(t, pw.str("localId"), linked.str("localId"))
	assert.Nil(t, linked.body["isNewUser"])
}

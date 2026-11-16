package gcpidentitytoolkit_test

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"encoding/json/v2"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func refTOTP(t *testing.T, secretB32 string, at time.Time) string {
	t.Helper()

	key, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(secretB32)
	require.NoError(t, err)

	mac := hmac.New(sha1.New, key)

	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], uint64(at.Unix()/30))
	mac.Write(buf[:])

	sum := mac.Sum(nil)
	off := sum[len(sum)-1] & 0x0f

	return fmt.Sprintf("%06d", (binary.BigEndian.Uint32(sum[off:off+4])&0x7fffffff)%1_000_000)
}

func jwtClaims(t *testing.T, tok string) map[string]any {
	t.Helper()

	raw, err := base64.RawURLEncoding.DecodeString(strings.Split(tok, ".")[1])
	require.NoError(t, err)

	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out, jsonx.V1()))

	return out
}

func (e *env) mfaPost(t *testing.T, verb string, body any) result {
	t.Helper()

	return e.do(t, http.MethodPost, "/v2/accounts/"+verb+"?key=k", "", body)
}

func (e *env) mfaConfig(t *testing.T) {
	t.Helper()

	e.backend.SetDefaultProject(e.project)

	e.setConfig(t, map[string]any{"mfa": map[string]any{
		"state":            "ENABLED",
		"enabledProviders": []string{"PHONE_SMS"},
		"providerConfigs": []any{
			map[string]any{"state": "ENABLED", "totpProviderConfig": map[string]any{"adjacentIntervals": 1}},
		},
	}, "signIn": map[string]any{"phoneNumber": map[string]any{
		"enabled": true, "testPhoneNumbers": map[string]string{"+15555550100": "123456"},
	}}})
}

func (e *env) verifiedUser(t *testing.T, email string) string {
	t.Helper()

	r := e.do(t, http.MethodPost, "/v1/projects/"+e.project+"/accounts", adminAuth,
		map[string]any{"email": email, "password": testPassword, "emailVerified": true})
	require.Equal(t, http.StatusOK, r.status, r.body)

	return e.signIn(t, email).str("idToken")
}

func (e *env) signIn(t *testing.T, email string) result {
	t.Helper()

	return e.client(
		t,
		"signInWithPassword",
		map[string]any{"email": email, "password": testPassword, "returnSecureToken": true},
	)
}

func TestMfaTotpFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mfaConfig(t)

	idTok := e.verifiedUser(t, "totp@example.com")

	start := e.mfaPost(
		t,
		"mfaEnrollment:start",
		map[string]any{"idToken": idTok, "totpEnrollmentInfo": map[string]any{}},
	)
	require.Equal(t, http.StatusOK, start.status, start.body)

	info, _ := start.body["totpSessionInfo"].(map[string]any)
	secret, _ := info["sharedSecretKey"].(string)
	require.NotEmpty(t, secret)
	assert.Equal(t, "SHA1", info["hashingAlgorithm"])
	assert.EqualValues(t, 6, info["verificationCodeLength"])
	assert.EqualValues(t, 30, info["periodSec"])

	bad := e.mfaPost(t, "mfaEnrollment:finalize", map[string]any{
		"idToken": idTok, "displayName": "phone app",
		"totpVerificationInfo": map[string]any{"sessionInfo": info["sessionInfo"], "verificationCode": "000000"},
	})
	e.assertErr(t, bad, http.StatusBadRequest, "INVALID_CODE")

	fin := e.mfaPost(t, "mfaEnrollment:finalize", map[string]any{
		"idToken":     idTok,
		"displayName": "phone app",
		"totpVerificationInfo": map[string]any{
			"sessionInfo":      info["sessionInfo"],
			"verificationCode": refTOTP(t, secret, e.clock.Now()),
		},
	})
	require.Equal(t, http.StatusOK, fin.status, fin.body)
	assert.NotEmpty(t, fin.str("idToken"))

	lk := e.client(t, "lookup", map[string]any{"idToken": fin.str("idToken")})
	users, _ := lk.body["users"].([]any)
	require.Len(t, users, 1)

	mfa, _ := users[0].(map[string]any)["mfaInfo"].([]any)
	require.Len(t, mfa, 1)

	enrollment, _ := mfa[0].(map[string]any)
	assert.Equal(t, "phone app", enrollment["displayName"])
	assert.NotNil(t, enrollment["totpInfo"])

	first := e.signIn(t, "totp@example.com")
	require.Equal(t, http.StatusOK, first.status, first.body)
	assert.Empty(t, first.str("idToken"), "no tokens before the second factor")
	assert.NotEmpty(t, first.str("mfaPendingCredential"))

	pending := first.str("mfaPendingCredential")
	enrollID := enrollment["mfaEnrollmentId"]

	wrong := e.mfaPost(t, "mfaSignIn:finalize", map[string]any{
		"mfaPendingCredential": pending, "mfaEnrollmentId": enrollID,
		"totpVerificationInfo": map[string]any{"verificationCode": "111111"},
	})
	e.assertErr(t, wrong, http.StatusBadRequest, "INVALID_CODE")

	e.clock.Advance(30 * time.Second)

	ok := e.mfaPost(t, "mfaSignIn:finalize", map[string]any{
		"mfaPendingCredential": pending, "mfaEnrollmentId": enrollID,
		"totpVerificationInfo": map[string]any{"verificationCode": refTOTP(t, secret, e.clock.Now())},
	})
	require.Equal(t, http.StatusOK, ok.status, ok.body)

	fb, _ := jwtClaims(t, ok.str("idToken"))["firebase"].(map[string]any)
	assert.Equal(t, "totp", fb["sign_in_second_factor"])
	assert.Equal(t, enrollID, fb["second_factor_identifier"])
	assert.Equal(t, "password", fb["sign_in_provider"])

	replay := e.mfaPost(t, "mfaSignIn:finalize", map[string]any{
		"mfaPendingCredential": pending, "mfaEnrollmentId": enrollID,
		"totpVerificationInfo": map[string]any{"verificationCode": refTOTP(t, secret, e.clock.Now())},
	})
	e.assertErr(t, replay, http.StatusBadRequest, "INVALID_MFA_PENDING_CREDENTIAL")

	wd := e.mfaPost(
		t,
		"mfaEnrollment:withdraw",
		map[string]any{"idToken": ok.str("idToken"), "mfaEnrollmentId": enrollID},
	)
	require.Equal(t, http.StatusOK, wd.status, wd.body)

	assert.NotEmpty(t, e.signIn(t, "totp@example.com").str("idToken"), "no second factor left")
}

func TestMfaSMSFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mfaConfig(t)

	idTok := e.verifiedUser(t, "sms@example.com")
	phone := "+15555550100"

	start := e.mfaPost(t, "mfaEnrollment:start", map[string]any{
		"idToken": idTok, "phoneEnrollmentInfo": map[string]any{"phoneNumber": phone, "recaptchaToken": "tok"},
	})
	require.Equal(t, http.StatusOK, start.status, start.body)

	session := start.body["phoneSessionInfo"].(map[string]any)["sessionInfo"]

	codes := e.do(t, http.MethodGet, "/emulator/v1/projects/"+e.project+"/verificationCodes", "", nil)
	list, _ := codes.body["verificationCodes"].([]any)
	require.Len(t, list, 1, "the SMS is recorded and listed, not sent")
	assert.Equal(t, "123456", list[0].(map[string]any)["code"])
	assert.Contains(t, e.logs.String(), "would send SMS")

	fin := e.mfaPost(t, "mfaEnrollment:finalize", map[string]any{
		"idToken": idTok, "phoneVerificationInfo": map[string]any{"sessionInfo": session, "code": "123456"},
	})
	require.Equal(t, http.StatusOK, fin.status, fin.body)

	pend := e.signIn(t, "sms@example.com")
	infos, _ := pend.body["mfaInfo"].([]any)
	require.Len(t, infos, 1)
	assert.Equal(t, "+*******0100", infos[0].(map[string]any)["phoneInfo"])
	assert.Equal(t, phone, infos[0].(map[string]any)["unobfuscatedPhoneInfo"])

	enrollID := infos[0].(map[string]any)["mfaEnrollmentId"]
	pc := pend.str("mfaPendingCredential")

	ss := e.mfaPost(t, "mfaSignIn:start", map[string]any{
		"mfaPendingCredential": pc,
		"mfaEnrollmentId":      enrollID,
		"phoneSignInInfo":      map[string]any{"recaptchaToken": "tok"},
	})
	require.Equal(t, http.StatusOK, ss.status, ss.body)

	bad := e.mfaPost(t, "mfaSignIn:finalize", map[string]any{
		"mfaPendingCredential": pc,
		"mfaEnrollmentId":      enrollID,
		"phoneVerificationInfo": map[string]any{
			"sessionInfo": ss.body["phoneResponseInfo"].(map[string]any)["sessionInfo"],
			"code":        "999999",
		},
	})
	e.assertErr(t, bad, http.StatusBadRequest, "INVALID_CODE")

	ok := e.mfaPost(t, "mfaSignIn:finalize", map[string]any{
		"mfaPendingCredential": pc,
		"mfaEnrollmentId":      enrollID,
		"phoneVerificationInfo": map[string]any{
			"sessionInfo": ss.body["phoneResponseInfo"].(map[string]any)["sessionInfo"],
			"code":        "123456",
		},
	})
	require.Equal(t, http.StatusOK, ok.status, ok.body)

	fb, _ := jwtClaims(t, ok.str("idToken"))["firebase"].(map[string]any)
	assert.Equal(t, "phone", fb["sign_in_second_factor"])
}

func TestMfaEnrollmentErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		want   string
		cfg    bool
		verify bool
	}{
		{
			name: "mfa disabled", cfg: false, verify: true, want: "OPERATION_NOT_ALLOWED",
			body: map[string]any{"totpEnrollmentInfo": map[string]any{}},
		},
		{
			name: "unverified email", cfg: true, verify: false, want: "UNVERIFIED_EMAIL",
			body: map[string]any{"totpEnrollmentInfo": map[string]any{}},
		},
		{name: "no info", cfg: true, verify: true, want: "MISSING_MFA_ENROLLMENT_INFO", body: map[string]any{}},
		{
			name: "phone without identifier", cfg: true, verify: true, want: "MISSING_CLIENT_IDENTIFIER",
			body: map[string]any{"phoneEnrollmentInfo": map[string]any{"phoneNumber": "+15555550100"}},
		},
		{
			name: "bad phone", cfg: true, verify: true, want: "INVALID_PHONE_NUMBER",
			body: map[string]any{"phoneEnrollmentInfo": map[string]any{"phoneNumber": "nope", "recaptchaToken": "t"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			if tt.cfg {
				e.mfaConfig(t)
			}

			var idTok string
			if tt.verify {
				idTok = e.verifiedUser(t, "u@example.com")
			} else {
				idTok = e.signUp(t, "u@example.com").str("idToken")
			}

			tt.body["idToken"] = idTok
			r := e.mfaPost(t, "mfaEnrollment:start", tt.body)
			assert.Equal(t, http.StatusBadRequest, r.status, r.body)
			assert.True(t, strings.HasPrefix(r.errMsg(), tt.want), r.errMsg())
		})
	}
}

func TestMfaAdminEnrollmentAndRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mfaConfig(t)

	r := e.do(t, http.MethodPost, "/v1/projects/"+e.project+"/accounts", adminAuth, map[string]any{
		"email": "adm@example.com", "password": testPassword, "emailVerified": true,
		"mfaInfo": []any{map[string]any{"phoneInfo": "+15555550100", "displayName": "work"}},
	})
	require.Equal(t, http.StatusOK, r.status, r.body)

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	dst.project = e.project
	pend := dst.signIn(t, "adm@example.com")
	require.Equal(t, http.StatusOK, pend.status, pend.body)
	assert.NotEmpty(t, pend.str("mfaPendingCredential"))

	lk := dst.admin(t, "lookup", map[string]any{"email": []string{"adm@example.com"}})
	users, _ := lk.body["users"].([]any)
	require.Len(t, users, 1)

	mfa, _ := users[0].(map[string]any)["mfaInfo"].([]any)
	require.Len(t, mfa, 1)
	assert.Equal(t, "+15555550100", mfa[0].(map[string]any)["phoneInfo"])
}

func TestSMSRegionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		cfg   map[string]any
		phone string
		ok    bool
	}{
		{
			"allowlist hit",
			map[string]any{"allowlistOnly": map[string]any{"allowedRegions": []string{"US"}}},
			"+14155550100",
			true,
		},
		{
			"allowlist miss",
			map[string]any{"allowlistOnly": map[string]any{"allowedRegions": []string{"US"}}},
			"+447700900123",
			false,
		},
		{
			"canada is not us",
			map[string]any{"allowlistOnly": map[string]any{"allowedRegions": []string{"US"}}},
			"+16475550100",
			false,
		},
		{
			"disallowed",
			map[string]any{"allowByDefault": map[string]any{"disallowedRegions": []string{"GB"}}},
			"+447700900123",
			false,
		},
		{
			"default allows",
			map[string]any{"allowByDefault": map[string]any{"disallowedRegions": []string{"GB"}}},
			"+4915123456789",
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.setConfig(t, map[string]any{"smsRegionConfig": tt.cfg})

			r := e.client(t, "sendVerificationCode", map[string]any{"phoneNumber": tt.phone, "recaptchaToken": "t"})
			if tt.ok {
				assert.Equal(t, http.StatusOK, r.status, r.body)

				return
			}

			e.assertErr(t, r, http.StatusBadRequest,
				"OPERATION_NOT_ALLOWED : SMS unable to be sent until this region enabled by the app administrator.")
		})
	}
}

func TestSignUpQuota(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.setConfig(t, map[string]any{"quota": map[string]any{"signUpQuotaConfig": map[string]any{
		"quota":         "2",
		"startTime":     e.clock.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
		"quotaDuration": "86400s",
	}}})

	for _, email := range []string{"a@example.com", "b@example.com"} {
		assert.Equal(
			t,
			http.StatusOK,
			e.client(t, "signUp", map[string]any{"email": email, "password": testPassword}).status,
		)
	}

	third := e.client(t, "signUp", map[string]any{"email": "c@example.com", "password": testPassword})
	e.assertErr(t, third, http.StatusBadRequest, "TOO_MANY_ATTEMPTS_TRY_LATER")

	e.clock.Advance(61 * time.Minute)
	assert.Equal(
		t,
		http.StatusOK,
		e.client(t, "signUp", map[string]any{"email": "c@example.com", "password": testPassword}).status,
	)

	e.clock.Advance(25 * time.Hour)

	for _, email := range []string{"d@example.com", "e@example.com", "f@example.com"} {
		assert.Equal(
			t,
			http.StatusOK,
			e.client(t, "signUp", map[string]any{"email": email, "password": testPassword}).status,
			"outside the quota window nothing is metered",
		)
	}
}

func TestRecaptchaEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra map[string]any
		name  string
		state string
		want  int
	}{
		{name: "enforce without token", state: "ENFORCE", want: http.StatusBadRequest},
		{
			name:  "enforce with token",
			state: "ENFORCE",
			extra: map[string]any{"captchaResponse": "tok"},
			want:  http.StatusOK,
		},
		{name: "audit without token", state: "AUDIT", want: http.StatusOK},
		{name: "off without token", state: "OFF", want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.setConfig(t, map[string]any{"recaptchaConfig": map[string]any{"emailPasswordEnforcementState": tt.state}})

			body := map[string]any{"email": "r@example.com", "password": testPassword}
			maps.Copy(body, tt.extra)

			r := e.client(t, "signUp", body)
			assert.Equal(t, tt.want, r.status, r.body)

			if tt.want != http.StatusOK {
				assert.Equal(t, "MISSING_RECAPTCHA_TOKEN", r.errMsg())
			}
		})
	}
}

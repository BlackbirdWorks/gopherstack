package gcpidentitytoolkit_test

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/scrypt"
)

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func firebaseScrypt(t *testing.T, pw string, salt, sep, signer []byte, memCost, rounds int) []byte {
	t.Helper()

	dk, err := scrypt.Key([]byte(pw), append(append([]byte{}, salt...), sep...), 1<<memCost, rounds, 1, 32)
	require.NoError(t, err)

	block, err := aes.NewCipher(dk)
	require.NoError(t, err)

	out := make([]byte, len(signer))
	cipher.NewCTR(block, make([]byte, aes.BlockSize)).XORKeyStream(out, signer)

	return out
}

func TestImportHashes(t *testing.T) {
	t.Parallel()

	signer := []byte("signer-key-bytes")
	salt := []byte("0123456789abcdef")
	sep := []byte{0x07}

	bc, err := bcrypt.GenerateFromPassword([]byte("secret1"), bcrypt.MinCost)
	require.NoError(t, err)

	mac := hmac.New(sha256.New, signer)
	mac.Write([]byte("secret1"))
	mac.Write(salt)

	std, err := scrypt.Key([]byte("secret1"), salt, 1024, 8, 1, 32)
	require.NoError(t, err)

	tests := []struct {
		hash map[string]any
		user map[string]any
		name string
	}{
		{
			name: "bcrypt",
			hash: map[string]any{"hashAlgorithm": "BCRYPT"},
			user: map[string]any{"passwordHash": b64(bc)},
		},
		{
			name: "firebase scrypt",
			hash: map[string]any{
				"hashAlgorithm": "SCRYPT",
				"signerKey":     b64(signer),
				"saltSeparator": b64(sep),
				"rounds":        8,
				"memoryCost":    12,
			},
			user: map[string]any{
				"passwordHash": b64(firebaseScrypt(t, "secret1", salt, sep, signer, 12, 8)), "salt": b64(salt),
			},
		},
		{
			name: "hmac sha256",
			hash: map[string]any{"hashAlgorithm": "HMAC_SHA256", "signerKey": b64(signer)},
			user: map[string]any{"passwordHash": b64(mac.Sum(nil)), "salt": b64(salt)},
		},
		{
			name: "standard scrypt",
			hash: map[string]any{
				"hashAlgorithm":   "STANDARD_SCRYPT",
				"cpuMemCost":      1024,
				"blockSize":       8,
				"parallelization": 1,
				"dkLen":           32,
			},
			user: map[string]any{"passwordHash": b64(std), "salt": b64(salt)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)

			user := map[string]any{
				"localId":     "imp1",
				"email":       "Imp@example.com",
				"displayName": "Imported",
				"createdAt":   "1600000000000",
			}
			maps.Copy(user, tt.user)

			body := map[string]any{"users": []any{user}}
			maps.Copy(body, tt.hash)

			r := e.admin(t, "batchCreate", body)
			require.Equal(t, http.StatusOK, r.status, r.body)
			assert.NotContains(t, r.body, "error")

			in := e.client(t, "signInWithPassword", map[string]any{"email": "imp@example.com", "password": "secret1"})
			require.Equal(t, http.StatusOK, in.status, in.body)
			assert.Equal(t, "imp1", in.str("localId"))

			bad := e.client(
				t,
				"signInWithPassword",
				map[string]any{"email": "imp@example.com", "password": "nope-nope"},
			)
			assert.Equal(t, "INVALID_LOGIN_CREDENTIALS", bad.errMsg())

			again := e.client(
				t,
				"signInWithPassword",
				map[string]any{"email": "imp@example.com", "password": "secret1"},
			)
			assert.Equal(t, http.StatusOK, again.status, "sign-in after lazy re-hash")

			lk := e.admin(t, "lookup", map[string]any{"localId": []string{"imp1"}})
			u := lk.body["users"].([]any)[0].(map[string]any)
			assert.Equal(t, "1600000000000", u["createdAt"])
		})
	}
}

func TestImportErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		msg      string
		errMsg   string
		errIndex int
		status   int
	}{
		{name: "no users", body: map[string]any{}, status: 400, msg: "MISSING_USER_ACCOUNT"},
		{
			name:   "hash without algorithm",
			status: 200,
			errMsg: "Password hash requires an email and a hash algorithm",
			body: map[string]any{
				"users": []any{map[string]any{"localId": "a", "email": "a@example.com", "passwordHash": "AAAA"}},
			},
		},
		{
			name:   "unknown algorithm",
			status: 400,
			msg:    "INVALID_HASH_ALGORITHM : Unsupported or incomplete hash configuration.",
			body:   map[string]any{"hashAlgorithm": "ROT13", "users": []any{map[string]any{"localId": "a"}}},
		},
		{
			name: "duplicate uid", status: 200, errIndex: 1, errMsg: "DUPLICATE_LOCAL_ID",
			body: map[string]any{"users": []any{map[string]any{"localId": "a"}, map[string]any{"localId": "a"}}},
		},
		{
			name: "duplicate email", status: 200, errIndex: 1, errMsg: "EMAIL_EXISTS",
			body: map[string]any{"users": []any{
				map[string]any{
					"localId": "a",
					"email":   "x@example.com",
				},
				map[string]any{"localId": "b", "email": "X@example.com"},
			}},
		},
		{
			name:   "overwrite",
			status: 200,
			body: map[string]any{
				"allowOverwrite": true,
				"users":          []any{map[string]any{"localId": "a"}, map[string]any{"localId": "a"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			r := e.admin(t, "batchCreate", tt.body)
			assert.Equal(t, tt.status, r.status, r.body)

			if tt.msg != "" {
				assert.Equal(t, tt.msg, r.errMsg())

				return
			}

			errs, _ := r.body["error"].([]any)
			if tt.errMsg == "" {
				assert.Empty(t, errs)

				return
			}

			require.Len(t, errs, 1)
			assert.InDelta(t, tt.errIndex, errs[0].(map[string]any)["index"], 0)
			assert.Equal(t, tt.errMsg, errs[0].(map[string]any)["message"])
		})
	}
}

func TestBatchGetAndQuery(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for _, m := range []string{"a", "b", "c"} {
		e.signUp(t, m+"@example.com")
		e.clock.Advance(time.Second)
	}

	first := e.do(t, http.MethodGet, "/v1/projects/"+e.project+"/accounts:batchGet?maxResults=2", adminAuth, nil)
	require.Equal(t, http.StatusOK, first.status, first.body)
	assert.Len(t, first.body["users"], 2)
	require.NotEmpty(t, first.str("nextPageToken"))

	second := e.do(
		t,
		http.MethodGet,
		"/v1/projects/"+e.project+"/accounts:batchGet?maxResults=2&nextPageToken="+first.str("nextPageToken"),
		adminAuth,
		nil,
	)
	assert.Len(t, second.body["users"], 1)
	assert.Empty(t, second.str("nextPageToken"))

	q := e.admin(
		t,
		"query",
		map[string]any{"expression": []any{map[string]any{"email": "B@example.com"}}, "returnUserInfo": true},
	)
	require.Equal(t, http.StatusOK, q.status, q.body)
	assert.Equal(t, "1", q.str("recordsCount"))
	assert.Len(t, q.body["userInfo"], 1)

	count := e.admin(t, "query", map[string]any{"returnUserInfo": false})
	assert.Equal(t, "3", count.str("recordsCount"))
	assert.NotContains(t, count.body, "userInfo")

	desc := e.admin(t, "query", map[string]any{"sortBy": "USER_EMAIL", "order": "DESC", "limit": "1"})
	assert.Equal(t, "c@example.com", desc.body["userInfo"].([]any)[0].(map[string]any)["email"])

	bad := e.do(t, http.MethodGet, "/v1/projects/"+e.project+"/accounts:batchGet?maxResults=5000", adminAuth, nil)
	e.assertErr(t, bad, http.StatusBadRequest, "INVALID_MAX_RESULTS")
}

func TestBatchDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		left   string
		failed int
		force  bool
		prep   bool
	}{
		{name: "force", force: true, left: "0"},
		{name: "enabled user without force", failed: 1, left: "1"},
		{name: "disabled user without force", prep: true, left: "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			up := e.signUp(t, "a@example.com")

			if tt.prep {
				e.admin(t, "update", map[string]any{"localId": up.str("localId"), "disableUser": true})
			}

			r := e.admin(
				t,
				"batchDelete",
				map[string]any{"localIds": []string{up.str("localId"), "ghost"}, "force": tt.force},
			)
			require.Equal(t, http.StatusOK, r.status, r.body)

			errs, _ := r.body["errors"].([]any)
			assert.Len(t, errs, tt.failed)

			if tt.failed > 0 {
				assert.Equal(
					t,
					"NOT_DISABLED : Disable the account before batch deletion.",
					errs[0].(map[string]any)["message"],
				)
			}

			q := e.admin(t, "query", map[string]any{"returnUserInfo": false})
			assert.Equal(t, tt.left, q.str("recordsCount"))
		})
	}
}

func TestAdminCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
		msg  string
	}{
		{
			name: "full",
			body: map[string]any{
				"localId":       "u1",
				"email":         "u1@example.com",
				"password":      "secret1",
				"emailVerified": true,
				"phoneNumber":   "+15555550100",
				"displayName":   "U",
			},
		},
		{name: "empty", body: map[string]any{}},
		{name: "bad phone", body: map[string]any{"phoneNumber": "5551"}, msg: "INVALID_PHONE_NUMBER : Invalid format."},
		{
			name: "weak password",
			body: map[string]any{"email": "w@example.com", "password": "1"},
			msg:  "WEAK_PASSWORD : Password should be at least 6 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			r := e.do(t, http.MethodPost, "/v1/projects/"+e.project+"/accounts", adminAuth, tt.body)

			if tt.msg != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.msg)

				return
			}

			require.Equal(t, http.StatusOK, r.status, r.body)
			assert.NotEmpty(t, r.str("localId"))
			assert.NotContains(t, r.body, "idToken")

			if id, ok := tt.body["localId"]; ok {
				dup := e.do(t, http.MethodPost, "/v1/projects/"+e.project+"/accounts", adminAuth, tt.body)
				e.assertErr(t, dup, http.StatusBadRequest, "DUPLICATE_LOCAL_ID")

				lk := e.admin(t, "lookup", map[string]any{"phoneNumber": []string{"+15555550100"}})
				assert.Equal(t, id, lk.body["users"].([]any)[0].(map[string]any)["localId"])

				phone := e.do(
					t,
					http.MethodPost,
					"/v1/projects/"+e.project+"/accounts",
					adminAuth,
					map[string]any{"phoneNumber": "+15555550100"},
				)
				e.assertErr(t, phone, http.StatusBadRequest, "PHONE_NUMBER_EXISTS")
			}
		})
	}
}

func TestSessionCookie(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		duration string
		msg      string
	}{
		{name: "ok", duration: "3600"},
		{name: "too short", duration: "60", msg: "INVALID_DURATION"},
		{name: "too long", duration: "99999999", msg: "INVALID_DURATION"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			up := e.signUp(t, "s@example.com")

			r := e.do(t, http.MethodPost, "/v1/projects/"+e.project+":createSessionCookie", adminAuth,
				map[string]any{"idToken": up.str("idToken"), "validDuration": tt.duration})
			if tt.msg != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.msg)

				return
			}

			require.Equal(t, http.StatusOK, r.status, r.body)
			assert.Equal(t, 3, strings.Count(r.str("sessionCookie"), ".")+1)

			noAuth := e.do(
				t,
				http.MethodPost,
				"/v1/projects/"+e.project+":createSessionCookie",
				"",
				map[string]any{"idToken": up.str("idToken")},
			)
			assert.Equal(t, http.StatusUnauthorized, noAuth.status)
		})
	}
}

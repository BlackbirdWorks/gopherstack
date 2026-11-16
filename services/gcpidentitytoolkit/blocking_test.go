package gcpidentitytoolkit_test

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type blockingFn struct {
	reply  func(event string, claims map[string]any) (int, any)
	url    string
	events []map[string]any
	mu     sync.Mutex
}

func newBlockingFn(t *testing.T, reply func(event string, claims map[string]any) (int, any)) *blockingFn {
	t.Helper()

	f := &blockingFn{reply: reply}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)

		var body struct {
			Data struct {
				JWT string `json:"jwt"`
			} `json:"data"`
		}

		_ = json.Unmarshal(raw, &body, jsonx.V1())
		claims := jwtClaims(t, body.Data.JWT)

		f.mu.Lock()
		f.events = append(f.events, claims)
		f.mu.Unlock()

		event, _ := claims["event_type"].(string)
		code, out := f.reply(event, claims)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)

		data, _ := json.Marshal(out, jsonx.V1())
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)

	f.url = srv.URL

	return f
}

func (f *blockingFn) seen() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]map[string]any(nil), f.events...)
}

func (e *env) blockingConfig(t *testing.T, create, signIn string) {
	t.Helper()

	trig := map[string]any{}
	if create != "" {
		trig["beforeCreate"] = map[string]any{"functionUri": create}
	}

	if signIn != "" {
		trig["beforeSignIn"] = map[string]any{"functionUri": signIn}
	}

	e.setConfig(t, map[string]any{"blockingFunctions": map[string]any{"triggers": trig}})
}

func TestBlockingFunctionsAllowAndMutate(t *testing.T) {
	t.Parallel()

	fn := newBlockingFn(t, func(event string, _ map[string]any) (int, any) {
		if event == "providers/cloud.auth/eventTypes/user.beforeCreate:password" {
			return http.StatusOK, map[string]any{"userRecord": map[string]any{
				"displayName": "Renamed", "updateMask": "displayName",
			}}
		}

		return http.StatusOK, map[string]any{"userRecord": map[string]any{
			"sessionClaims": map[string]any{"tier": "gold"}, "updateMask": "sessionClaims",
		}}
	})

	e := newEnv(t)
	e.blockingConfig(t, fn.url, fn.url)

	r := e.client(
		t,
		"signUp",
		map[string]any{"email": "bf@example.com", "password": testPassword, "returnSecureToken": true},
	)
	require.Equal(t, http.StatusOK, r.status, r.body)

	claims := jwtClaims(t, r.str("idToken"))
	assert.Equal(t, "gold", claims["tier"])
	assert.Equal(t, "Renamed", claims["name"])

	events := fn.seen()
	require.Len(t, events, 2, "beforeCreate then beforeSignIn")
	assert.Equal(t, "password", events[0]["sign_in_method"])
	assert.Equal(t, fn.url, events[0]["aud"])

	rec, _ := events[0]["user_record"].(map[string]any)
	assert.Equal(t, "bf@example.com", rec["email"])
	assert.Equal(t, "https://securetoken.google.com/"+e.project, events[0]["iss"])
}

func TestBlockingFunctionsReject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		create     bool
		wantSecond int
	}{
		{"before create rejects and rolls back", true, http.StatusBadRequest},
		{"before sign in rejects", false, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deny := func(string, map[string]any) (int, any) {
				return http.StatusForbidden, map[string]any{
					"error": map[string]any{"status": "PERMISSION_DENIED", "message": "nope"},
				}
			}
			fn := newBlockingFn(t, deny)

			e := newEnv(t)

			if tt.create {
				e.blockingConfig(t, fn.url, "")
			} else {
				e.signUp(t, "rej@example.com")
				e.blockingConfig(t, "", fn.url)
			}

			var r result
			if tt.create {
				r = e.client(t, "signUp", map[string]any{"email": "rej@example.com", "password": testPassword})
			} else {
				r = e.signIn(t, "rej@example.com")
			}

			assert.Equal(t, http.StatusBadRequest, r.status, r.body)
			assert.Contains(t, r.errMsg(), "BLOCKING_FUNCTION_ERROR_RESPONSE")
			assert.Contains(t, r.errMsg(), "nope")

			if tt.create {
				e.setConfig(t, map[string]any{"blockingFunctions": nil})
				assert.Equal(t, http.StatusOK, e.client(
					t,
					"signUp",
					map[string]any{
						"email":    "rej@example.com",
						"password": testPassword,
					},
				).status, "the rejected account was removed")
			}
		})
	}
}

func TestBlockingFunctionDisablesUser(t *testing.T) {
	t.Parallel()

	fn := newBlockingFn(t, func(string, map[string]any) (int, any) {
		return http.StatusOK, map[string]any{"userRecord": map[string]any{"disabled": true, "updateMask": "disabled"}}
	})

	e := newEnv(t)
	e.signUp(t, "dis@example.com")
	e.blockingConfig(t, "", fn.url)

	r := e.signIn(t, "dis@example.com")
	assert.Equal(t, http.StatusBadRequest, r.status)
	assert.Contains(t, r.errMsg(), "USER_DISABLED")
}

func TestBlockingFunctionOnIdpSignIn(t *testing.T) {
	t.Parallel()

	fn := newBlockingFn(t, func(string, map[string]any) (int, any) { return http.StatusOK, map[string]any{} })
	e := newEnv(t)
	e.enableGoogle(t)
	e.setConfig(t, map[string]any{"blockingFunctions": map[string]any{
		"triggers":                  map[string]any{"beforeSignIn": map[string]any{"functionUri": fn.url}},
		"forwardInboundCredentials": map[string]any{"idToken": true},
	}})

	tok := fakeJWT(t, map[string]any{"sub": "g-77", "email": "idp@example.com", "email_verified": true})
	r := e.signInIdp(t, url.Values{"id_token": {tok}, "providerId": {"google.com"}}, nil)
	require.Equal(t, http.StatusOK, r.status, r.body)

	events := fn.seen()
	require.Len(t, events, 1)
	assert.Equal(t, "google.com", events[0]["sign_in_method"])
	assert.Equal(t, tok, events[0]["oauth_id_token"])
}

package gcpidentitytoolkit_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpidentitytoolkit"
)

func (e *env) setDefaultConfig(t *testing.T, patch map[string]any) {
	t.Helper()

	r := e.do(t, http.MethodPatch, "/v2/projects/"+gcpidentitytoolkit.DefaultProject+"/config", adminAuth, patch)
	require.Equal(t, http.StatusOK, r.status, r.body)
}

func TestClientPasswordPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cfg    map[string]any
		state  string
		minLen float64
	}{
		{name: "default", state: "OFF", minLen: 6},
		{
			name: "enforced",
			cfg: map[string]any{"passwordPolicyConfig": map[string]any{
				"passwordPolicyEnforcementState": "ENFORCE",
				"passwordPolicyVersions": []any{map[string]any{
					"schemaVersion":         1,
					"customStrengthOptions": map[string]any{"minPasswordLength": 10, "containsNumericCharacter": true},
				}},
			}},
			state: "ENFORCE", minLen: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			if tt.cfg != nil {
				e.setDefaultConfig(t, tt.cfg)
			}

			noKey := e.do(t, http.MethodGet, "/v2/passwordPolicy?tenantId=", "", nil)
			assert.Equal(t, http.StatusForbidden, noKey.status)

			r := e.do(t, http.MethodGet, "/v2/passwordPolicy?key=k", "", nil)
			require.Equal(t, http.StatusOK, r.status, r.body)
			assert.Equal(t, tt.state, r.str("enforcementState"))
			assert.NotEmpty(t, r.body["allowedNonAlphanumericCharacters"])

			opts, _ := r.body["customStrengthOptions"].(map[string]any)
			assert.InDelta(t, tt.minLen, opts["minPasswordLength"], 0)
		})
	}
}

func TestClientRecaptchaConfig(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	off := e.do(t, http.MethodGet, "/v2/recaptchaConfig?key=k&clientType=CLIENT_TYPE_WEB", "", nil)
	require.Equal(t, http.StatusOK, off.status, off.body)
	assert.Empty(t, off.str("recaptchaKey"))

	e.setDefaultConfig(t, map[string]any{"recaptchaConfig": map[string]any{
		"emailPasswordEnforcementState": "ENFORCE",
		"recaptchaKeys":                 []any{map[string]any{"key": "abc", "type": "WEB"}},
	}})

	on := e.do(t, http.MethodGet, "/v2/recaptchaConfig?key=k&clientType=CLIENT_TYPE_WEB", "", nil)
	require.Equal(t, http.StatusOK, on.status, on.body)
	assert.Equal(t, "projects/"+gcpidentitytoolkit.DefaultProject+"/keys/abc", on.str("recaptchaKey"))

	states, _ := on.body["recaptchaEnforcementState"].([]any)
	require.Len(t, states, 2)
	assert.Equal(t, "ENFORCE", states[0].(map[string]any)["enforcementState"])
	assert.Equal(t, "OFF", states[1].(map[string]any)["enforcementState"])
}

func TestPasswordNonAlphanumericSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pw   string
		ok   bool
	}{
		{name: "listed symbol", pw: "abcdef!", ok: true},
		{name: "space is not listed", pw: "abc def", ok: false},
		{name: "plus is not listed", pw: "abcdef+", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.setConfig(t, map[string]any{"passwordPolicyConfig": map[string]any{
				"passwordPolicyEnforcementState": "ENFORCE",
				"passwordPolicyVersions": []any{map[string]any{"customStrengthOptions": map[string]any{
					"containsNonAlphanumericCharacter": true,
				}}},
			}})

			r := e.client(t, "signUp", map[string]any{"email": "n@example.com", "password": tt.pw})
			if tt.ok {
				assert.Equal(t, http.StatusOK, r.status, r.body)

				return
			}

			e.assertErr(
				t,
				r,
				http.StatusBadRequest,
				"PASSWORD_DOES_NOT_MEET_REQUIREMENTS : Missing password requirements: "+
					"[Password must contain a non-alphanumeric character]",
			)
		})
	}
}

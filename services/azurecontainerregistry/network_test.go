package azurecontainerregistry_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

func TestNetworkRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		rules acr.NetworkRules
		want  int
	}{
		{name: "open by default", want: http.StatusOK},
		{name: "deny without rules", rules: acr.NetworkRules{DefaultDeny: true}, want: http.StatusForbidden},
		{name: "allow loopback range", rules: acr.NetworkRules{DefaultDeny: true, Allow: []string{"127.0.0.0/8"}},
			want: http.StatusOK},
		{name: "allow exact address", rules: acr.NetworkRules{DefaultDeny: true, Allow: []string{"127.0.0.1"}},
			want: http.StatusOK},
		{name: "other range denied", rules: acr.NetworkRules{DefaultDeny: true, Allow: []string{"10.0.0.0/8"}},
			want: http.StatusForbidden},
		{name: "rules ignored when default allow", rules: acr.NetworkRules{Allow: []string{"10.0.0.0/8"}},
			want: http.StatusOK},
		{name: "public disabled overrides allow",
			rules: acr.NetworkRules{PublicDisabled: true, DefaultDeny: true, Allow: []string{"127.0.0.1"}},
			want:  http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			creds := e.registry(t, "netreg", true)
			require.NoError(t, e.h.Backend.SetNetworkRules("netreg", tt.rules))

			c := e.client(t, "netreg", basic(creds.Username, creds.Password1))
			assert.Equal(t, tt.want, c.Do(http.MethodGet, "/v2/", nil, nil).Status)
			assert.Equal(t, tt.want == http.StatusOK,
				c.Do(http.MethodGet, "/oauth2/token?service=x", nil, nil).Status != http.StatusForbidden)

			if tt.want == http.StatusForbidden {
				got := c.Do(http.MethodGet, "/v2/", nil, nil)
				assert.Contains(t, string(got.Body), "DENIED")
			}
		})
	}
}

func TestNetworkRulesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		rule  string
		valid bool
	}{
		{name: "address", rule: "203.0.113.7", valid: true},
		{name: "cidr", rule: "203.0.113.0/24", valid: true},
		{name: "ipv6 refused", rule: "2001:db8::/32"},
		{name: "garbage", rule: "not-an-ip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			e.registry(t, "valnetreg", false)

			err := e.h.Backend.SetNetworkRules("valnetreg", acr.NetworkRules{Allow: []string{tt.rule}})
			if tt.valid {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, acr.ErrInvalidNetworkRule)
		})
	}
}

func TestNetworkRulesPersist(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.registry(t, "persistnet", false)
	require.NoError(t, e.h.Backend.SetNetworkRules("persistnet", acr.NetworkRules{DefaultDeny: true,
		Allow: []string{"10.0.0.0/8"}}))

	snap := e.h.Backend.Snapshot(t.Context())

	other := newEnv(t, "")
	require.NoError(t, other.h.Backend.Restore(t.Context(), snap))

	got := other.client(t, "persistnet", nil).Do(http.MethodGet, "/v2/", nil, nil)
	assert.Equal(t, http.StatusForbidden, got.Status)
}

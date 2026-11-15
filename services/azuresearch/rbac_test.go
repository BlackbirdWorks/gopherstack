package azuresearch_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresearch"
)

const (
	rbacResource = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Search/searchServices/svc1"
	rbacDocsRead = "Microsoft.Search/searchServices/indexes/documents/read"
)

var errBadToken = errors.New("bad token")

type fakeAuthority struct {
	grants map[string][]string
}

func (f fakeAuthority) Authenticate(token string) (string, error) {
	if _, ok := f.grants[token]; !ok {
		return "", errBadToken
	}

	return token, nil
}

func (f fakeAuthority) Allowed(principal, resource, action string, data bool) bool {
	if resource != rbacResource {
		return false
	}

	for _, g := range f.grants[principal] {
		if data != strings.HasPrefix(g, "data:") {
			continue
		}

		g = strings.TrimPrefix(g, "data:")

		if strings.HasSuffix(g, "*") && strings.HasPrefix(action, strings.TrimSuffix(g, "*")) || g == action {
			return true
		}
	}

	return false
}

func newRBACStack(t *testing.T, validate bool) *stack {
	t.Helper()

	auth := fakeAuthority{grants: map[string][]string{
		"reader":  {"data:" + rbacDocsRead},
		"writer":  {"data:Microsoft.Search/searchServices/indexes/documents/*"},
		"objects": {"Microsoft.Search/searchServices/*"},
		"nobody":  {},
	}}

	return newStackWith(t, "standard", runInline, func(h *azuresearch.Handler) {
		h.Settings.ValidateTokens = validate
		h.Authority = auth

		_, err := h.Backend.PutService(testService, azuresearch.ServiceConfig{
			SKU: "standard", HostingMode: "default", PublicNetworkAccess: "enabled", AuthMode: "aadOrApiKey",
			ReplicaCount: 1, PartitionCount: 1, ResourceID: rbacResource,
		})
		require.NoError(t, err)
	})
}

type bearerCase struct {
	body     any
	name     string
	token    string
	path     string
	method   string
	want     int
	validate bool
}

func bc(name, token, method, path string, body any, validate bool, want int) bearerCase {
	return bearerCase{
		body: body, name: name, token: token, path: path + "?" + apiVer, method: method, want: want, validate: validate,
	}
}

func TestBearerTokenRoles(t *testing.T) {
	t.Parallel()

	get, post := http.MethodGet, http.MethodPost
	batch := map[string]any{"value": []any{map[string]any{"@search.action": "upload", "id": "1"}}}
	empty := map[string]any{"value": []any{}}

	cases := []bearerCase{
		bc("unvalidated any token", "whatever", get, "/indexes", nil, false, 200),
		bc("invalid token", "bogus", get, "/indexes", nil, true, 401),
		bc("no role", "nobody", get, "/indexes", nil, true, 403),
		bc("reader cannot list indexes", "reader", get, "/indexes", nil, true, 403),
		bc("reader counts", "reader", get, "/indexes('hotels')/docs/$count", nil, true, 200),
		bc("reader cannot write", "reader", post, "/indexes('hotels')/docs/search.index", empty, true, 403),
		bc("writer writes", "writer", post, "/indexes('hotels')/docs/search.index", batch, true, 200),
		bc("writer cannot manage objects", "writer", get, "/indexes", nil, true, 403),
		bc("objects lists indexes", "objects", get, "/indexes", nil, true, 200),
		bc("objects cannot read docs", "objects", get, "/indexes('hotels')/docs/$count", nil, true, 403),
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newRBACStack(t, tc.validate)
			s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{
				"name": "hotels", "fields": []any{map[string]any{"name": "id", "type": "Edm.String", "key": true}},
			}, http.StatusCreated)

			r := s.bearer(t, tc.method, tc.path, tc.token, tc.body)
			assert.Equal(t, tc.want, r.status, string(r.raw))
		})
	}
}

func TestIPRulesEnforced(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		rules []string
		want  int
	}{
		{"no rules", nil, 200},
		{"loopback range", []string{"127.0.0.0/8"}, 200},
		{"loopback address", []string{"10.1.1.1", "127.0.0.1"}, 200},
		{"other range", []string{"10.0.0.0/8"}, 403},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newStackWith(t, "standard", runInline, func(h *azuresearch.Handler) {
				_, err := h.Backend.PutService(testService, azuresearch.ServiceConfig{
					SKU: "standard", HostingMode: "default", PublicNetworkAccess: "enabled", AuthMode: "apiKeyOnly",
					ReplicaCount: 1, PartitionCount: 1, IPRules: tc.rules,
				})
				require.NoError(t, err)
			})

			assert.Equal(t, tc.want, s.admin2(t, http.MethodGet, "/indexes?"+apiVer, nil).status)
		})
	}
}

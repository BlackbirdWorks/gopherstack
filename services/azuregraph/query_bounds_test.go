package azuregraph_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryOptionBoundsAndMalformedInput(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", 5000)
	manyTerms := strings.TrimSuffix(strings.Repeat("displayName eq 'a' or ", 130), " or ")
	nested := func(n int) string {
		return strings.Repeat("(", n) + "displayName eq 'a'" + strings.Repeat(")", n)
	}
	expandDepth := func(n int) string { return strings.Repeat("members(", n) + "$select=id" + strings.Repeat(")", n) }

	tests := []struct {
		name  string
		query string
		code  string
	}{
		{"filter too long", "$filter=" + url.QueryEscape("startswith(displayName,'"+long+"')"), "Request_BadRequest"},
		{"filter too many tokens", "$filter=" + url.QueryEscape(manyTerms), "Request_UnsupportedQuery"},
		{"filter too deep", "$filter=" + url.QueryEscape(nested(33)), "Request_UnsupportedQuery"},
		{"filter unbalanced open", "$filter=" + url.QueryEscape("(displayName eq 'a'"), "Request_UnsupportedQuery"},
		{"filter unbalanced close", "$filter=" + url.QueryEscape("displayName eq 'a')"), "Request_UnsupportedQuery"},
		{"filter open quote", "$filter=" + url.QueryEscape("displayName eq 'a"), "Request_UnsupportedQuery"},
		{"filter double operator", "$filter=" + url.QueryEscape("displayName eq eq 'a'"), "Request_UnsupportedQuery"},
		{"filter dangling and", "$filter=" + url.QueryEscape("displayName eq 'a' and"), "Request_UnsupportedQuery"},
		{"filter unknown property", "$filter=" + url.QueryEscape("nope eq 'a'"), "Request_UnsupportedQuery"},
		{"filter only parens", "$filter=" + url.QueryEscape("()"), "Request_UnsupportedQuery"},
		{"expand too deep", "$expand=" + url.QueryEscape(expandDepth(9)), "Request_BadRequest"},
		{"expand unbalanced open", "$expand=" + url.QueryEscape("members("), "Request_BadRequest"},
		{"expand unbalanced close", "$expand=" + url.QueryEscape("owners)"), "Request_BadRequest"},
		{"expand too long", "$expand=" + long, "Request_BadRequest"},
		{"expand unknown option", "$expand=" + url.QueryEscape("owners($top=1)"), "Request_UnsupportedQuery"},
		{"expand bad levels", "$expand=" + url.QueryEscape("manager($levels=0)"), "Request_BadRequest"},
		{"orderby too long", "$orderby=" + long, "Request_BadRequest"},
		{"orderby unknown", "$orderby=nope", "Request_UnsupportedQuery"},
		{"orderby list", "$orderby=" + url.QueryEscape("displayName,appId"), "Request_UnsupportedQuery"},
		{"search too long", "$search=" + url.QueryEscape(`"displayName:`+long+`"`), "Request_BadRequest"},
		{"select too long", "$select=" + long, "Request_BadRequest"},
		{"select unknown", "$select=nope", "Request_BadRequest"},
		{"top zero", "$top=0", "Request_BadRequest"},
		{"top over", "$top=1000", "Request_BadRequest"},
		{"top text", "$top=abc", "Request_BadRequest"},
		{"top negative", "$top=-1", "Request_BadRequest"},
		{"skiptoken too long", "$skiptoken=" + long, "Request_BadRequest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertErr(
				t,
				newEnv(t).req(t, http.MethodGet, "/v1.0/applications?"+tt.query, nil),
				http.StatusBadRequest,
				tt.code,
			)
		})
	}

	t.Run("limits themselves are accepted", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		for _, q := range []string{
			"$filter=" + url.QueryEscape(nested(32)),
			"$expand=" + url.QueryEscape(expandDepth(1)),
		} {
			r := e.req(t, http.MethodGet, "/v1.0/groups?"+q, nil)
			assert.Equal(t, http.StatusOK, r.status, r.raw)
		}
	})

	t.Run("search needs the header and a property", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/applications?$search="+url.QueryEscape(`"displayName:a"`), nil),
			http.StatusBadRequest, "Request_UnsupportedQuery")
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/applications?$search="+url.QueryEscape(`"nocolon"`), nil,
			"ConsistencyLevel", "eventual"), http.StatusBadRequest, "Request_BadRequest")
	})
}

func TestQueryBoundsOnEveryResource(t *testing.T) {
	t.Parallel()

	long := url.QueryEscape("startswith(displayName,'" + strings.Repeat("a", 5000) + "')")

	for _, path := range []string{
		"/v1.0/users", "/v1.0/groups", "/v1.0/servicePrincipals", "/v1.0/devices", "/v1.0/administrativeUnits",
		"/v1.0/roleManagement/directory/roleDefinitions", "/v1.0/oauth2PermissionGrants",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			r := newEnv(t).req(t, http.MethodGet, path+"?$filter="+long, nil)
			require.Equal(t, http.StatusBadRequest, r.status, r.raw)
		})
	}
}

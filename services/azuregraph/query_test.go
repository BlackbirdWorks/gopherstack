package azuregraph_test

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func values(r resp) []string {
	var out []string

	list, _ := r.body["value"].([]any)
	for _, v := range list {
		m, _ := v.(map[string]any)
		out = append(out, str(m, "displayName"))
	}

	return out
}

func TestFilters(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for _, n := range []string{"Alpha Web", "Alpha API", "Beta Web", "Gamma"} {
		e.mustCreate(t, "/v1.0/applications", map[string]any{"displayName": n, "tags": []string{"t-" + strings.Fields(n)[0]}})
	}

	const eventualHdr = "ConsistencyLevel"

	tests := []struct {
		name   string
		query  string
		hdr    []string
		want   []string
		status int
	}{
		{"eq", "$filter=displayName eq 'Gamma'", nil, []string{"Gamma"}, 200},
		{"eq case insensitive", "$filter=displayName eq 'gamma'", nil, []string{"Gamma"}, 200},
		{"startswith", "$filter=startswith(displayName,'alpha')", nil, []string{"Alpha Web", "Alpha API"}, 200},
		{"and", "$filter=startswith(displayName,'Alpha') and displayName eq 'Alpha API'", nil, []string{"Alpha API"}, 200},
		{"or", "$filter=displayName eq 'Gamma' or displayName eq 'Beta Web'", nil, []string{"Beta Web", "Gamma"}, 200},
		{"parens", "$filter=(displayName eq 'Gamma' or displayName eq 'Beta Web') and startswith(displayName,'B')", nil, []string{"Beta Web"}, 200},
		{"any", "$filter=tags/any(t:t eq 't-Beta')", nil, []string{"Beta Web"}, 200},
		{"escaped quote", "$filter=displayName eq 'it''s'", nil, nil, 200},
		{"null", "$filter=description eq null", nil, []string{"Alpha Web", "Alpha API", "Beta Web", "Gamma"}, 200},
		{"ne without header", "$filter=displayName ne 'Gamma'", nil, nil, 400},
		{"ne with header", "$filter=displayName ne 'Gamma'&$count=true", []string{eventualHdr, "eventual"}, []string{"Alpha Web", "Alpha API", "Beta Web"}, 200},
		{"endswith needs advanced", "$filter=endswith(displayName,'Web')", nil, nil, 400},
		{"endswith advanced", "$filter=endswith(displayName,'Web')&$count=true", []string{eventualHdr, "eventual"}, []string{"Alpha Web", "Beta Web"}, 200},
		{"not advanced", "$filter=not(displayName eq 'Gamma')&$count=true", []string{eventualHdr, "eventual"}, []string{"Alpha Web", "Alpha API", "Beta Web"}, 200},
		{"unknown property", "$filter=bogus eq 'x'", nil, nil, 400},
		{"syntax error", "$filter=displayName eq", nil, nil, 400},
		{"unterminated string", "$filter=displayName eq 'x", nil, nil, 400},
		{"orderby", "$orderby=displayName desc", nil, []string{"Gamma", "Beta Web", "Alpha Web", "Alpha API"}, 200},
		{"orderby unknown", "$orderby=bogus", nil, nil, 400},
		{"search", `$search="displayName:alpha"`, []string{eventualHdr, "eventual"}, []string{"Alpha Web", "Alpha API"}, 200},
		{"search or", `$search="displayName:gamma" OR "displayName:beta"`, []string{eventualHdr, "eventual"}, []string{"Beta Web", "Gamma"}, 200},
		{"search needs header", `$search="displayName:alpha"`, nil, nil, 400},
		{"expand unsupported", "$expand=owners", nil, nil, 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := e.req(t, http.MethodGet, "/v1.0/applications?"+strings.ReplaceAll(url.PathEscape(tt.query), "%3D", "="), nil, tt.hdr...)
			require.Equal(t, tt.status, r.status, r.raw)

			if tt.status == http.StatusOK {
				assert.ElementsMatch(t, tt.want, values(r))
			}
		})
	}
}

func TestQueryOptions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for i := range 7 {
		e.mustCreate(t, "/v1.0/applications", map[string]any{"displayName": "App " + strconv.Itoa(i)})
	}

	t.Run("paging follows nextLink", func(t *testing.T) {
		t.Parallel()

		next := "/v1.0/applications?$top=3&$filter=startswith(displayName,'App')"

		var got []string

		pages := 0

		for next != "" {
			r := e.req(t, http.MethodGet, next, nil)
			require.Equal(t, http.StatusOK, r.status, r.raw)

			got = append(got, values(r)...)
			pages++

			link := str(r.body, "@odata.nextLink")
			if link == "" {
				break
			}

			assert.Contains(t, link, "$skiptoken=")
			assert.Contains(t, link, "$top=3")

			parsed, err := url.Parse(link)
			require.NoError(t, err)

			next = parsed.RequestURI()
		}

		assert.Equal(t, 3, pages)
		assert.Len(t, got, 7)
		assert.Equal(t, "App 0", got[0])
		assert.Equal(t, "App 6", got[6])
	})

	t.Run("select", func(t *testing.T) {
		t.Parallel()

		r := e.req(t, http.MethodGet, "/v1.0/applications?$select=displayName,appId&$top=1", nil)
		require.Equal(t, http.StatusOK, r.status)

		item, _ := r.body["value"].([]any)[0].(map[string]any)
		assert.ElementsMatch(t, []string{"id", "displayName", "appId"}, keys(item))

		assertErr(t, e.req(t, http.MethodGet, "/v1.0/applications?$select=bogus", nil), http.StatusBadRequest, "Request_BadRequest")

		single := e.req(t, http.MethodGet, "/v1.0/applications/"+str(item, "id")+"?$select=displayName", nil)
		assert.Contains(t, keys(single.body), "displayName")
		assert.NotContains(t, keys(single.body), "web")
	})

	t.Run("count", func(t *testing.T) {
		t.Parallel()

		r := e.req(t, http.MethodGet, "/v1.0/applications?$count=true&$top=2", nil, "ConsistencyLevel", "eventual")
		require.Equal(t, http.StatusOK, r.status)
		assert.EqualValues(t, 7, r.body["@odata.count"])
		assert.Len(t, r.body["value"], 2)

		assertErr(t, e.req(t, http.MethodGet, "/v1.0/applications?$count=true", nil), http.StatusBadRequest, "Request_BadRequest")

		raw := e.req(t, http.MethodGet, "/v1.0/applications/$count", nil, "ConsistencyLevel", "eventual")
		assert.Equal(t, http.StatusOK, raw.status)
		assert.Equal(t, "7", raw.raw)
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/applications/$count", nil), http.StatusBadRequest, "Request_BadRequest")
	})

	t.Run("bad top", func(t *testing.T) {
		t.Parallel()

		for _, top := range []string{"0", "1000", "x", "-1"} {
			assertErr(t, e.req(t, http.MethodGet, "/v1.0/applications?$top="+top, nil), http.StatusBadRequest, "Request_BadRequest")
		}
	})

	t.Run("default page size", func(t *testing.T) {
		t.Parallel()

		big := newEnv(t)
		for i := range 101 {
			big.mustCreate(t, "/v1.0/groups", map[string]any{
				"displayName": "G" + strconv.Itoa(i), "mailEnabled": false, "mailNickname": "g" + strconv.Itoa(i), "securityEnabled": true,
			})
		}

		r := big.req(t, http.MethodGet, "/v1.0/groups", nil)
		assert.Len(t, r.body["value"], 100)
		assert.NotEmpty(t, str(r.body, "@odata.nextLink"))
	})

	t.Run("user filters", func(t *testing.T) {
		t.Parallel()

		u := newUser(t, e, "filterme"+upnDomain)

		r := e.req(t, http.MethodGet, "/v1.0/users?$filter=userPrincipalName%20eq%20'FILTERME"+upnDomain+"'", nil)
		require.Equal(t, http.StatusOK, r.status, r.raw)
		assert.Len(t, r.body["value"], 1)

		r = e.req(t, http.MethodGet, "/v1.0/users?$filter=startswith(userPrincipalName,'filter')", nil)
		assert.Len(t, r.body["value"], 1)
		assert.Equal(t, str(u, "id"), str(r.body["value"].([]any)[0].(map[string]any), "id"))
	})
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		if !strings.HasPrefix(k, "@odata.context") {
			out = append(out, k)
		}
	}

	return out
}

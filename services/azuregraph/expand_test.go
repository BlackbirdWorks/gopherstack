package azuregraph_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserSelectReturnsNonDefaultProperties(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	u := e.mustCreate(t, "/v1.0/users", map[string]any{
		"accountEnabled": true, "displayName": "Sel", "mailNickname": "sel", "userPrincipalName": "sel" + upnDomain,
		"department": "Eng", "usageLocation": "US", "passwordProfile": map[string]any{"password": "Sup3r-secret!"},
	})

	def := e.req(t, http.MethodGet, "/v1.0/users/"+str(u, "id"), nil)
	assert.NotContains(t, def.body, "department")
	assert.NotContains(t, def.body, "accountEnabled")

	sel := e.req(t, http.MethodGet, "/v1.0/users/"+str(u, "id")+"?$select=department,accountEnabled,usageLocation", nil)
	require.Equal(t, http.StatusOK, sel.status, sel.raw)
	assert.Equal(t, "Eng", str(sel.body, "department"))
	assert.Equal(t, "US", str(sel.body, "usageLocation"))
	assert.Equal(t, true, sel.body["accountEnabled"])
	assert.NotContains(t, sel.body, "displayName")

	list := e.req(t, http.MethodGet, "/v1.0/users?$select=department", nil)
	require.Equal(t, http.StatusOK, list.status, list.raw)
	assert.Contains(t, list.raw, `"department":"Eng"`)
}

func TestExpand(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	owner := newUser(t, e, "owner"+upnDomain)
	member := newUser(t, e, "member"+upnDomain)
	app := newApp(t, e, "Exp")
	g := newGroup(t, e, "ExpGroup")
	parent := newGroup(t, e, "ParentGroup")

	require.Equal(
		t,
		http.StatusNoContent,
		e.req(t, http.MethodPost, "/v1.0/applications/"+str(app, "id")+"/owners/$ref",
			map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/users/" + str(owner, "id")}).status,
	)
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPost, "/v1.0/groups/"+str(g, "id")+"/members/$ref",
		map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/users/" + str(member, "id")}).status)
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPost, "/v1.0/groups/"+str(parent, "id")+"/members/$ref",
		map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/groups/" + str(g, "id")}).status)

	appPath := "/v1.0/applications/" + str(app, "id")
	groupPath := "/v1.0/groups/" + str(g, "id")
	memberPath := "/v1.0/users/" + str(member, "id")

	tests := []struct {
		name  string
		path  string
		prop  string
		want  string
		wantN int
		code  int
	}{
		{"application owners", appPath + "?$expand=owners", "owners", str(owner, "id"), 1, 200},
		{
			"nested select", groupPath + "?$expand=members($select=id,displayName)", "members",
			str(member, "id"), 1, 200,
		},
		{"user memberOf", memberPath + "?$expand=memberOf", "memberOf", str(g, "id"), 1, 200},
		{
			"transitive", memberPath + "?$expand=transitiveMemberOf", "transitiveMemberOf",
			str(parent, "id"), 2, 200,
		},
		{"invalid navigation", memberPath + "?$expand=members", "", "", 0, 400},
		{"unknown navigation", groupPath + "?$expand=widgets", "", "", 0, 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := e.req(t, http.MethodGet, tt.path, nil)
			require.Equal(t, tt.code, r.status, r.raw)

			if tt.code != http.StatusOK {
				assertErr(t, r, tt.code, "Request_BadRequest")

				return
			}

			rel, _ := r.body[tt.prop].([]any)
			assert.Len(t, rel, tt.wantN)
			assert.Contains(t, r.raw, tt.want)
		})
	}

	t.Run("on a list", func(t *testing.T) {
		t.Parallel()

		r := e.req(t, http.MethodGet, "/v1.0/groups?$expand="+url.QueryEscape("members($select=id)"), nil)
		require.Equal(t, http.StatusOK, r.status, r.raw)
		assert.Contains(t, r.raw, str(member, "id"))
	})
}

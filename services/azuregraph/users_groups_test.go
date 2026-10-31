package azuregraph_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const upnDomain = "@gopherstack.onmicrosoft.com"

func TestUsers(t *testing.T) {
	t.Parallel()

	e := newEnv(t, func(s *azuregraphSettings) { s.ExtraDomains = "Example.com" })
	u := newUser(t, e, "alice"+upnDomain)

	assert.Equal(t, "Member", str(u, "userType"))
	assert.NotContains(t, u, "passwordProfile")
	assert.Equal(t, true, u["accountEnabled"])

	t.Run("get by id and upn", func(t *testing.T) {
		t.Parallel()

		for _, key := range []string{str(u, "id"), "alice" + upnDomain, "ALICE" + upnDomain} {
			r := e.req(t, http.MethodGet, "/v1.0/users/"+key, nil)
			require.Equal(t, http.StatusOK, r.status, key)
			assert.Equal(t, str(u, "id"), str(r.body, "id"))
			assert.NotContains(t, r.raw, "Sup3r-secret!")
		}

		assertErr(t, e.req(t, http.MethodGet, "/v1.0/users/nobody"+upnDomain, nil), http.StatusNotFound, "Request_ResourceNotFound")
	})

	t.Run("create validation", func(t *testing.T) {
		t.Parallel()

		ok := map[string]any{
			"accountEnabled": true, "displayName": "X", "mailNickname": "x", "userPrincipalName": "x" + upnDomain,
			"passwordProfile": map[string]any{"password": "Sup3r-secret!"},
		}

		with := func(k string, v any) map[string]any {
			m := map[string]any{}
			for kk, vv := range ok {
				m[kk] = vv
			}

			if v == nil {
				delete(m, k)
			} else {
				m[k] = v
			}

			return m
		}

		tests := []struct {
			name string
			body map[string]any
			msg  string
		}{
			{"missing upn", with("userPrincipalName", nil), "userPrincipalName"},
			{"missing nickname", with("mailNickname", nil), "mailNickname"},
			{"missing password profile", with("passwordProfile", nil), "passwordProfile"},
			{"missing accountEnabled", with("accountEnabled", nil), "accountEnabled"},
			{"unverified domain", with("userPrincipalName", "x@contoso.com"), "verified domain"},
			{"no at sign", with("userPrincipalName", "x"), "userPrincipalName"},
			{"weak password", with("passwordProfile", map[string]any{"password": "abc"}), "complexity"},
			{"duplicate upn", with("userPrincipalName", "alice"+upnDomain), "already exists"},
			{"unknown property", with("bogus", 1), "bogus"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				r := e.req(t, http.MethodPost, "/v1.0/users", tt.body)
				assertErr(t, r, http.StatusBadRequest, "Request_BadRequest")
				assert.Contains(t, r.raw, tt.msg)
			})
		}

		extra := with("userPrincipalName", "bob@example.com")
		extra["mailNickname"] = "bob"
		assert.Equal(t, http.StatusCreated, e.req(t, http.MethodPost, "/v1.0/users", extra).status, "extra verified domains are case-insensitive")
	})

	t.Run("patch and delete", func(t *testing.T) {
		t.Parallel()

		v := newUser(t, e, "carol"+upnDomain)
		path := "/v1.0/users/" + str(v, "id")

		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPatch, path, map[string]any{
			"jobTitle": "Engineer", "accountEnabled": false, "passwordProfile": map[string]any{"password": "An0ther-pass!"},
		}).status)

		got := e.req(t, http.MethodGet, path, nil).body
		assert.Equal(t, "Engineer", str(got, "jobTitle"))
		assert.Equal(t, false, got["accountEnabled"])

		assertErr(t, e.req(t, http.MethodPatch, path, map[string]any{"userPrincipalName": "carol@contoso.com"}), http.StatusBadRequest, "Request_BadRequest")
		assertErr(t, e.req(t, http.MethodPatch, path, map[string]any{"passwordProfile": map[string]any{"password": "x"}}), http.StatusBadRequest, "Request_BadRequest")

		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, path, nil).status)
		assertErr(t, e.req(t, http.MethodGet, path, nil), http.StatusNotFound, "Request_ResourceNotFound")
	})
}

func TestGroups(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	g := newGroup(t, e, "Engineering")
	gid := str(g, "id")

	assert.Equal(t, true, g["securityEnabled"])
	assert.Equal(t, []any{}, g["groupTypes"])

	t.Run("create validation", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			body map[string]any
			code int
		}{
			{"m365", map[string]any{"displayName": "Team", "mailEnabled": true, "mailNickname": "team", "securityEnabled": false, "groupTypes": []string{"Unified"}}, http.StatusCreated},
			{"missing nickname", map[string]any{"displayName": "x", "mailEnabled": false, "securityEnabled": true}, http.StatusBadRequest},
			{"neither flag", map[string]any{"displayName": "x", "mailEnabled": false, "mailNickname": "x", "securityEnabled": false}, http.StatusBadRequest},
			{"mail enabled non-unified", map[string]any{"displayName": "x", "mailEnabled": true, "mailNickname": "x", "securityEnabled": true}, http.StatusBadRequest},
			{"bad group type", map[string]any{"displayName": "x", "mailEnabled": false, "mailNickname": "x", "securityEnabled": true, "groupTypes": []string{"Nope"}}, http.StatusBadRequest},
			{"role assignable needs security", map[string]any{"displayName": "x", "mailEnabled": true, "mailNickname": "x", "securityEnabled": false, "groupTypes": []string{"Unified"}, "isAssignableToRole": true}, http.StatusBadRequest},
			{"role assignable", map[string]any{"displayName": "x", "mailEnabled": false, "mailNickname": "x", "securityEnabled": true, "isAssignableToRole": true}, http.StatusCreated},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				r := e.req(t, http.MethodPost, "/v1.0/groups", tt.body)
				assert.Equal(t, tt.code, r.status, r.raw)

				if tt.code == http.StatusCreated && tt.name == "m365" {
					assert.Equal(t, "team@gopherstack.onmicrosoft.com", str(r.body, "mail"))
					assert.Equal(t, "Public", str(r.body, "visibility"))
				}
			})
		}
	})

	t.Run("membership", func(t *testing.T) {
		t.Parallel()

		member := newUser(t, e, "dave"+upnDomain)
		mid := str(member, "id")
		ref := map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + mid}

		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPost, "/v1.0/groups/"+gid+"/members/$ref", ref).status)
		assertErr(t, e.req(t, http.MethodPost, "/v1.0/groups/"+gid+"/members/$ref", ref), http.StatusBadRequest, "Request_BadRequest")
		assert.Contains(t, e.req(t, http.MethodPost, "/v1.0/groups/"+gid+"/members/$ref", ref).raw, "already exist")
		assertErr(t, e.req(t, http.MethodPost, "/v1.0/groups/"+gid+"/members/$ref", map[string]any{}), http.StatusBadRequest, "Request_BadRequest")
		assertErr(t, e.req(t, http.MethodPost, "/v1.0/groups/"+gid+"/members/$ref", map[string]any{"@odata.id": "x/" + newID0()}), http.StatusNotFound, "Request_ResourceNotFound")
		assertErr(t, e.req(t, http.MethodPost, "/v1.0/groups/"+gid+"/members/$ref", map[string]any{"@odata.id": "x/" + gid}), http.StatusBadRequest, "Request_BadRequest")

		members := e.req(t, http.MethodGet, "/v1.0/groups/"+gid+"/members", nil)
		require.Len(t, members.body["value"], 1)

		first, _ := members.body["value"].([]any)[0].(map[string]any)
		assert.Equal(t, "#microsoft.graph.user", str(first, "@odata.type"))
		assert.Equal(t, mid, str(first, "id"))

		typedOnly := e.req(t, http.MethodGet, "/v1.0/groups/"+gid+"/members/microsoft.graph.group", nil)
		assert.Empty(t, typedOnly.body["value"])

		refs := e.req(t, http.MethodGet, "/v1.0/groups/"+gid+"/members/$ref", nil)
		assert.Contains(t, refs.raw, "directoryObjects/"+mid)

		of := e.req(t, http.MethodGet, "/v1.0/users/"+mid+"/memberOf", nil)
		require.Len(t, of.body["value"], 1)

		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/groups/"+gid+"/members/"+mid+"/$ref", nil).status)
		assertErr(t, e.req(t, http.MethodDelete, "/v1.0/groups/"+gid+"/members/"+mid+"/$ref", nil), http.StatusNotFound, "Request_ResourceNotFound")
	})

	t.Run("transitive membership", func(t *testing.T) {
		t.Parallel()

		outer, inner := newGroup(t, e, "Outer"), newGroup(t, e, "Inner")
		user := newUser(t, e, "erin"+upnDomain)
		link := func(parent, child map[string]any) {
			r := e.req(t, http.MethodPost, "/v1.0/groups/"+str(parent, "id")+"/members/$ref",
				map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + str(child, "id")})
			require.Equal(t, http.StatusNoContent, r.status, r.raw)
		}
		link(outer, inner)
		link(inner, user)
		link(inner, outer)

		direct := e.req(t, http.MethodGet, "/v1.0/groups/"+str(outer, "id")+"/members", nil)
		assert.Len(t, direct.body["value"], 1)

		all := e.req(t, http.MethodGet, "/v1.0/groups/"+str(outer, "id")+"/transitiveMembers", nil)
		assert.Len(t, all.body["value"], 2, "cycles terminate and the group itself is excluded")

		up := e.req(t, http.MethodGet, "/v1.0/users/"+str(user, "id")+"/transitiveMemberOf", nil)
		assert.Len(t, up.body["value"], 2)

		direct = e.req(t, http.MethodGet, "/v1.0/users/"+str(user, "id")+"/memberOf", nil)
		assert.Len(t, direct.body["value"], 1)
	})

	t.Run("owners", func(t *testing.T) {
		t.Parallel()

		owner := newUser(t, e, "frank"+upnDomain)
		app := newApp(t, e, "Owned")
		ref := map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/users/" + str(owner, "id")}

		for _, base := range []string{"/v1.0/groups/" + gid, "/v1.0/applications/" + str(app, "id")} {
			require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPost, base+"/owners/$ref", ref).status, base)
			assertErr(t, e.req(t, http.MethodPost, base+"/owners/$ref", ref), http.StatusBadRequest, "Request_BadRequest")

			list := e.req(t, http.MethodGet, base+"/owners", nil)
			require.Len(t, list.body["value"], 1, base)
			require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, base+"/owners/"+str(owner, "id")+"/$ref", nil).status)
			assert.Empty(t, e.req(t, http.MethodGet, base+"/owners", nil).body["value"])
		}

		assertErr(t, e.req(t, http.MethodPost, "/v1.0/groups/"+gid+"/owners/$ref",
			map[string]any{"@odata.id": "x/" + gid}), http.StatusBadRequest, "Request_BadRequest")
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/users/"+str(owner, "id")+"/owners", nil), http.StatusBadRequest, "BadRequest")
	})

	t.Run("bind on create", func(t *testing.T) {
		t.Parallel()

		u := newUser(t, e, "gina"+upnDomain)
		uref := "https://graph.microsoft.com/v1.0/users/" + str(u, "id")

		made := e.mustCreate(t, "/v1.0/groups", map[string]any{
			"displayName": "Bound", "mailEnabled": false, "mailNickname": "bound", "securityEnabled": true,
			"owners@odata.bind": []string{uref}, "members@odata.bind": []string{uref},
		})

		assert.Len(t, e.req(t, http.MethodGet, "/v1.0/groups/"+str(made, "id")+"/owners", nil).body["value"], 1)
		assert.Len(t, e.req(t, http.MethodGet, "/v1.0/groups/"+str(made, "id")+"/members", nil).body["value"], 1)

		assertErr(t, e.req(t, http.MethodPost, "/v1.0/groups", map[string]any{
			"displayName": "Bad", "mailEnabled": false, "mailNickname": "bad", "securityEnabled": true,
			"members@odata.bind": []string{"https://graph.microsoft.com/v1.0/users/" + newID0()},
		}), http.StatusNotFound, "Request_ResourceNotFound")
	})

	t.Run("patch and delete", func(t *testing.T) {
		t.Parallel()

		x := newGroup(t, e, "Temp")
		path := "/v1.0/groups/" + str(x, "id")

		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPatch, path, map[string]any{"description": "d"}).status)
		assertErr(t, e.req(t, http.MethodPatch, path, map[string]any{"isAssignableToRole": true}), http.StatusBadRequest, "Request_BadRequest")
		assertErr(t, e.req(t, http.MethodPatch, path, map[string]any{"mail": "x@y"}), http.StatusBadRequest, "Request_BadRequest")
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, path, nil).status)
		assertErr(t, e.req(t, http.MethodGet, path, nil), http.StatusNotFound, "Request_ResourceNotFound")
	})
}

func TestDirectoryObjects(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	u := newUser(t, e, "hal"+upnDomain)
	g := newGroup(t, e, "Halo")
	a := newApp(t, e, "HalApp")

	ids := []string{str(u, "id"), str(g, "id"), str(a, "id"), newID0()}

	tests := []struct {
		name  string
		types []string
		want  int
	}{
		{"all", nil, 3},
		{"users only", []string{"user"}, 1},
		{"user and group", []string{"user", "group"}, 2},
		{"directoryObject", []string{"directoryObject"}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{"ids": ids}
			if tt.types != nil {
				body["types"] = tt.types
			}

			r := e.req(t, http.MethodPost, "/v1.0/directoryObjects/getByIds", body)
			require.Equal(t, http.StatusOK, r.status, r.raw)
			assert.Len(t, r.body["value"], tt.want)

			for _, v := range r.body["value"].([]any) {
				assert.True(t, strings.HasPrefix(str(v.(map[string]any), "@odata.type"), "#microsoft.graph."))
			}
		})
	}

	assertErr(t, e.req(t, http.MethodPost, "/v1.0/directoryObjects/getByIds", map[string]any{"ids": []string{"x"}}), http.StatusBadRequest, "Request_BadRequest")
	assertErr(t, e.req(t, http.MethodPost, "/v1.0/directoryObjects/getByIds", map[string]any{}), http.StatusBadRequest, "Request_BadRequest")

	doomed := newGroup(t, e, "Doomed")
	one := e.req(t, http.MethodGet, "/v1.0/directoryObjects/"+str(doomed, "id"), nil)
	require.Equal(t, http.StatusOK, one.status)
	assert.Equal(t, "#microsoft.graph.group", str(one.body, "@odata.type"))

	assertErr(t, e.req(t, http.MethodGet, "/v1.0/directoryObjects/"+newID0(), nil), http.StatusNotFound, "Request_ResourceNotFound")
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/directoryObjects/"+str(doomed, "id"), nil).status)
	assertErr(t, e.req(t, http.MethodGet, "/v1.0/groups/"+str(doomed, "id"), nil), http.StatusNotFound, "Request_ResourceNotFound")
}

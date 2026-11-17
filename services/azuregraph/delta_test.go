package azuregraph_test

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func linkPath(t *testing.T, r resp, key string) string {
	t.Helper()

	link, _ := r.body[key].(string)
	require.NotEmpty(t, link, r.raw)

	u, err := url.Parse(link)
	require.NoError(t, err)

	return u.Path + "?" + u.RawQuery
}

func ids(r resp) []string { return values2(r, "id") }

func removedReason(r resp, id string) string {
	items, _ := r.body["value"].([]any)

	for _, it := range items {
		m, _ := it.(map[string]any)
		if str(m, "id") == id {
			rm, _ := m["@removed"].(map[string]any)

			return str(rm, "reason")
		}
	}

	return ""
}

func TestUsersDeltaRound(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	first := newUser(t, e, "d1"+upnDomain)

	r1 := e.req(t, http.MethodGet, "/v1.0/users/delta", nil)
	require.Equal(t, http.StatusOK, r1.status, r1.raw)
	assert.Equal(t, []string{str(first, "id")}, ids(r1))
	assert.Nil(t, r1.body["@odata.nextLink"])
	assert.Contains(t, str(r1.body, "@odata.deltaLink"), "$deltatoken=")
	assert.Contains(t, str(r1.body, "@odata.context"), "#users")

	link := linkPath(t, r1, "@odata.deltaLink")
	idle := e.req(t, http.MethodGet, link, nil)
	require.Equal(t, http.StatusOK, idle.status, idle.raw)
	assert.Empty(t, ids(idle), "nothing changed")

	second := newUser(t, e, "d2"+upnDomain)
	require.Equal(t, http.StatusNoContent,
		e.req(t, http.MethodPatch, "/v1.0/users/"+str(first, "id"), map[string]any{"jobTitle": "Boss"}).status)

	r2 := e.req(t, http.MethodGet, link, nil)
	require.Equal(t, http.StatusOK, r2.status, r2.raw)
	assert.ElementsMatch(t, []string{str(first, "id"), str(second, "id")}, ids(r2))

	link2 := linkPath(t, r2, "@odata.deltaLink")
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/users/"+str(second, "id"), nil).status)

	r3 := e.req(t, http.MethodGet, link2, nil)
	assert.Equal(t, []string{str(second, "id")}, ids(r3))
	assert.Equal(t, "changed", removedReason(r3, str(second, "id")), "soft deleted users can be restored")

	link3 := linkPath(t, r3, "@odata.deltaLink")
	idle3 := e.req(t, http.MethodGet, link3, nil)
	assert.Empty(t, ids(idle3), "a still soft-deleted user is not reported again")

	link3 = linkPath(t, idle3, "@odata.deltaLink")
	require.Equal(
		t,
		http.StatusNoContent,
		e.req(t, http.MethodDelete, "/v1.0/directory/deletedItems/"+str(second, "id"), nil).status,
	)

	r4 := e.req(t, http.MethodGet, link3, nil)
	assert.Equal(t, "deleted", removedReason(r4, str(second, "id")), r4.raw)

	link4 := linkPath(t, r4, "@odata.deltaLink")
	assert.Empty(t, ids(e.req(t, http.MethodGet, link4, nil)), "a removal is reported once per round")
	assert.Equal(
		t,
		[]string{str(second, "id")},
		ids(e.req(t, http.MethodGet, link3, nil)),
		"an old link replays its round",
	)
}

func TestUsersDeltaRestoreIsNew(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	u := newUser(t, e, "res"+upnDomain)
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/users/"+str(u, "id"), nil).status)

	r1 := e.req(t, http.MethodGet, "/v1.0/users/delta", nil)
	assert.Empty(t, ids(r1))

	require.Equal(
		t,
		http.StatusOK,
		e.req(t, http.MethodPost, "/v1.0/directory/deletedItems/"+str(u, "id")+"/restore", nil).status,
	)

	r2 := e.req(t, http.MethodGet, linkPath(t, r1, "@odata.deltaLink"), nil)
	require.Equal(t, []string{str(u, "id")}, ids(r2))
	assert.Equal(t, "res"+upnDomain, str(firstItem(r2), "userPrincipalName"))
}

func firstItem(r resp) map[string]any {
	items, _ := r.body["value"].([]any)
	if len(items) == 0 {
		return nil
	}

	m, _ := items[0].(map[string]any)

	return m
}

func TestDeltaSelectAndMinimal(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	u := newUser(t, e, "sel"+upnDomain)
	uid := str(u, "id")

	r1 := e.req(t, http.MethodGet, "/v1.0/users/delta?$select=displayName,jobTitle", nil)
	require.Equal(t, http.StatusOK, r1.status, r1.raw)
	assert.NotContains(t, firstItem(r1), "userPrincipalName")

	link := linkPath(t, r1, "@odata.deltaLink")

	patch := func(body map[string]any) {
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPatch, "/v1.0/users/"+uid, body).status)
	}

	patch(map[string]any{"city": "Oslo"})
	assert.Empty(t, ids(e.req(t, http.MethodGet, link, nil)), "an unselected property is not tracked")

	patch(map[string]any{"jobTitle": "Lead"})
	full := e.req(t, http.MethodGet, link, nil)
	require.Equal(t, []string{uid}, ids(full))
	assert.Contains(t, firstItem(full), "displayName", "default responses carry every tracked property")

	minimal := e.req(t, http.MethodGet, link, nil, "Prefer", "return=minimal")
	require.Equal(t, []string{uid}, ids(minimal))
	assert.Equal(t, "Lead", str(firstItem(minimal), "jobTitle"))
	assert.NotContains(t, firstItem(minimal), "displayName", "return=minimal sends changed properties only")
}

func TestDeltaFilterScopesObjects(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	a, b, c := newUser(t, e, "fa"+upnDomain), newUser(t, e, "fb"+upnDomain), newUser(t, e, "fc"+upnDomain)

	q := url.QueryEscape(fmt.Sprintf("id eq '%s' or id eq '%s'", str(a, "id"), str(b, "id")))
	r := e.req(t, http.MethodGet, "/v1.0/users/delta?$filter="+q, nil)
	require.Equal(t, http.StatusOK, r.status, r.raw)
	assert.ElementsMatch(t, []string{str(a, "id"), str(b, "id")}, ids(r))
	assert.NotContains(t, ids(r), str(c, "id"))

	require.Equal(t, http.StatusNoContent,
		e.req(t, http.MethodPatch, "/v1.0/users/"+str(c, "id"), map[string]any{"jobTitle": "x"}).status)
	assert.Empty(t, ids(e.req(t, http.MethodGet, linkPath(t, r, "@odata.deltaLink"), nil)))
}

func TestDeltaQueryValidation(t *testing.T) {
	t.Parallel()

	guid := "11111111-1111-1111-1111-111111111111"
	many := make([]string, 0, 51)

	for i := range 51 {
		many = append(many, fmt.Sprintf("id eq '00000000-0000-0000-0000-%012d'", i))
	}

	tests := []struct {
		name   string
		query  string
		code   string
		status int
	}{
		{"top", "$top=5", "Request_BadRequest", 400},
		{"orderby", "$orderby=displayName", "Request_BadRequest", 400},
		{"expand", "$expand=manager", "Request_BadRequest", 400},
		{"search", "$search=%22displayName:a%22", "Request_BadRequest", 400},
		{"count", "$count=true", "Request_BadRequest", 400},
		{"unknown select", "$select=nope", "Request_BadRequest", 400},
		{"manager select", "$select=manager", "Request_BadRequest", 400},
		{"other filter", "$filter=" + url.QueryEscape("displayName eq 'x'"), "Request_UnsupportedQuery", 400},
		{"non guid id", "$filter=" + url.QueryEscape("id eq 'abc'"), "Request_UnsupportedQuery", 400},
		{
			"and filter",
			"$filter=" + url.QueryEscape(fmt.Sprintf("id eq '%s' and id eq '%s'", guid, guid)),
			"Request_UnsupportedQuery",
			400,
		},
		{"too many ids", "$filter=" + url.QueryEscape(strings.Join(many, " or ")), "Request_BadRequest", 400},
		{"short token", "$deltatoken=abc", "Request_BadRequest", 400},
		{"odd chars", "$skiptoken=" + url.QueryEscape(strings.Repeat("!", 43)), "Request_BadRequest", 400},
		{"unknown delta token", "$deltatoken=" + strings.Repeat("A", 43), "syncStateNotFound", 410},
		{"unknown skip token", "$skiptoken=" + strings.Repeat("A", 43), "syncStateNotFound", 410},
		{
			"both tokens",
			"$skiptoken=" + strings.Repeat("A", 43) + "&$deltatoken=" + strings.Repeat("A", 43),
			"Request_BadRequest",
			400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			r := e.req(t, http.MethodGet, "/v1.0/users/delta?"+tt.query, nil)
			assertErr(t, r, tt.status, tt.code)

			if tt.status == http.StatusGone {
				assert.Contains(t, r.header.Get("Location"), "/users/delta?$deltatoken=")
			}
		})
	}

	t.Run("method", func(t *testing.T) {
		t.Parallel()

		assertErr(t, newEnv(t).req(t, http.MethodPost, "/v1.0/users/delta", map[string]any{}),
			http.StatusMethodNotAllowed, "Request_BadRequest")
	})
}

func TestDeltaTokenBinding(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	newUser(t, e, "bind"+upnDomain)

	r := e.req(t, http.MethodGet, "/v1.0/users/delta?$select=displayName", nil)
	require.Equal(t, http.StatusOK, r.status, r.raw)

	tok := strings.TrimPrefix(linkPath(t, r, "@odata.deltaLink"), "/v1.0/users/delta?")

	tests := []struct {
		name, path string
		status     int
	}{
		{"same query", "/v1.0/users/delta?" + tok, http.StatusOK},
		{"other collection", "/v1.0/groups/delta?" + tok, http.StatusBadRequest},
		{"changed select", "/v1.0/users/delta?$select=jobTitle&" + tok, http.StatusBadRequest},
		{"restated select", "/v1.0/users/delta?$select=displayName&" + tok, http.StatusOK},
		{
			"as skiptoken",
			"/v1.0/users/delta?" + strings.Replace(tok, "$deltatoken", "$skiptoken", 1),
			http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.status, e.req(t, http.MethodGet, tt.path, nil).status)
		})
	}
}

func TestDeltaTokenExpiry(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	r := e.req(t, http.MethodGet, "/v1.0/users/delta", nil)
	link := linkPath(t, r, "@odata.deltaLink")

	e.clock.advance(7*24*time.Hour - time.Minute)
	require.Equal(t, http.StatusOK, e.req(t, http.MethodGet, link, nil).status)

	e.clock.advance(8 * 24 * time.Hour)
	assertErr(t, e.req(t, http.MethodGet, link, nil), http.StatusGone, "syncStateNotFound")
	assert.Zero(t, e.h.Backend.DeltaSessionCount(), "expired sessions are swept")
}

func TestGroupsDeltaPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	want := make([]string, 0, 120)
	for i := range 120 {
		want = append(want, str(newGroup(t, e, fmt.Sprintf("pg%03d", i)), "id"))
	}

	p1 := e.req(t, http.MethodGet, "/v1.0/groups/delta", nil)
	require.Equal(t, http.StatusOK, p1.status, p1.raw)
	assert.Len(t, ids(p1), 100)
	assert.Nil(t, p1.body["@odata.deltaLink"])

	next := linkPath(t, p1, "@odata.nextLink")
	p2 := e.req(t, http.MethodGet, next, nil)
	require.Equal(t, http.StatusOK, p2.status, p2.raw)
	assert.Len(t, ids(p2), 20)
	assert.Nil(t, p2.body["@odata.nextLink"])
	assert.NotEmpty(t, str(p2.body, "@odata.deltaLink"))

	got := slices.Concat(ids(p1), ids(p2))
	assert.ElementsMatch(t, want, got)

	assertErr(t, e.req(t, http.MethodGet, next, nil), http.StatusGone, "syncStateNotFound")

	done := e.req(t, http.MethodGet, linkPath(t, p2, "@odata.deltaLink"), nil)
	assert.Empty(t, ids(done))
}

func TestGroupsDeltaMembers(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	g := newGroup(t, e, "mem")
	u := newUser(t, e, "mem"+upnDomain)
	gid, uid := str(g, "id"), str(u, "id")
	ref := map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + uid}

	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPost, "/v1.0/groups/"+gid+"/members/$ref", ref).status)

	r1 := e.req(t, http.MethodGet, "/v1.0/groups/delta?$select=displayName,members", nil)
	require.Equal(t, http.StatusOK, r1.status, r1.raw)

	ann, _ := firstItem(r1)["members@delta"].([]any)
	require.Len(t, ann, 1)
	assert.Equal(t, uid, str(ann[0].(map[string]any), "id"))
	assert.Equal(t, "#microsoft.graph.user", str(ann[0].(map[string]any), "@odata.type"))

	link := linkPath(t, r1, "@odata.deltaLink")
	assert.Empty(t, ids(e.req(t, http.MethodGet, link, nil)))

	require.Equal(
		t,
		http.StatusNoContent,
		e.req(t, http.MethodDelete, "/v1.0/groups/"+gid+"/members/"+uid+"/$ref", nil).status,
	)

	r2 := e.req(t, http.MethodGet, link, nil)
	require.Equal(t, []string{gid}, ids(r2), r2.raw)

	ann, _ = firstItem(r2)["members@delta"].([]any)
	require.Len(t, ann, 1)

	rm, _ := ann[0].(map[string]any)["@removed"].(map[string]any)
	assert.Equal(t, "deleted", str(rm, "reason"))
}

func TestDeltaSessionsAreBoundedUnderConcurrency(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	first := e.req(t, http.MethodGet, "/v1.0/users/delta", nil)
	oldest := linkPath(t, first, "@odata.deltaLink")

	var wg sync.WaitGroup

	for range 100 {
		wg.Go(func() {
			e.req(t, http.MethodGet, "/v1.0/users/delta", nil)
		})
	}

	wg.Wait()
	assert.Equal(t, 64, e.h.Backend.DeltaSessionCount())
	assertErr(t, e.req(t, http.MethodGet, oldest, nil), http.StatusGone, "syncStateNotFound")
}

func TestDeltaTokensDoNotSurviveRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	link := linkPath(t, e.req(t, http.MethodGet, "/v1.0/users/delta", nil), "@odata.deltaLink")

	fresh := newEnv(t)
	require.NoError(t, fresh.h.Restore(t.Context(), e.h.Snapshot(t.Context())))
	assertErr(t, fresh.req(t, http.MethodGet, link, nil), http.StatusGone, "syncStateNotFound")
}

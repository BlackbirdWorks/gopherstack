package cosmosdb_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

type userEnv struct {
	h   *cosmosdb.Handler
	now *atomic.Int64
}

func newUserEnv(t *testing.T) *userEnv {
	t.Helper()

	e := &userEnv{h: newTestHandler(t), now: &atomic.Int64{}}
	e.now.Store(authNow().Unix())
	e.h.ValidateAuth = true
	cosmosdb.SetHandlerClock(e.h, func() time.Time { return time.Unix(e.now.Load(), 0).UTC() })

	return e
}

func (e *userEnv) admin(t *testing.T, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	req := newReq(t, method, path, body)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	require.NoError(t, e.h.SignRequest(req))

	return serve(t, e.h, req)
}

func (e *userEnv) withToken(
	t *testing.T, token, method, path, body string, hdr map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := newReq(t, method, path, body)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	req.Header.Set("X-Ms-Date", time.Unix(e.now.Load(), 0).UTC().Format(http.TimeFormat))
	req.Header.Set("Authorization", url.QueryEscape(token))

	return serve(t, e.h, req)
}

func decodeMap(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

func pkHdr(v string) map[string]string {
	return map[string]string{"X-Ms-Documentdb-Partitionkey": `["` + v + `"]`}
}

func headersFor(spec string) map[string]string {
	switch {
	case strings.HasPrefix(spec, "pk:"):
		return pkHdr(strings.TrimPrefix(spec, "pk:"))
	case spec == "query":
		return map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"}
	case spec == "queryA":
		h := headersFor("query")
		h["X-Ms-Documentdb-Partitionkey"] = `["a"]`

		return h
	default:
		return nil
	}
}

func (e *userEnv) seed(t *testing.T) {
	t.Helper()

	steps := []struct {
		method, path, body, hdr string
	}{
		{http.MethodPost, "/dbs", `{"id":"d"}`, ""},
		{http.MethodPost, "/dbs/d/colls", `{"id":"c1","partitionKey":{"paths":["/pk"]}}`, ""},
		{http.MethodPost, "/dbs/d/colls", `{"id":"c2","partitionKey":{"paths":["/pk"]}}`, ""},
		{http.MethodPost, "/dbs/d/colls/c1/docs", `{"id":"1","pk":"a"}`, "pk:a"},
		{http.MethodPost, "/dbs/d/colls/c1/docs", `{"id":"2","pk":"b"}`, "pk:b"},
		{http.MethodPost, "/dbs/d/colls/c1/docs", `{"id":"del","pk":"a"}`, "pk:a"},
		{http.MethodPost, "/dbs/d/colls/c2/docs", `{"id":"1","pk":"a"}`, "pk:a"},
		{http.MethodPost, "/dbs/d/users", `{"id":"u"}`, ""},
	}
	for _, s := range steps {
		rec := e.admin(t, s.method, s.path, s.body, headersFor(s.hdr))
		require.Less(t, rec.Code, 300, "%s %s: %s", s.method, s.path, rec.Body.String())
	}
}

func (e *userEnv) permission(t *testing.T, id, mode, resource, pk string, hdr map[string]string) (string, int) {
	t.Helper()

	body := `{"id":"` + id + `","permissionMode":"` + mode + `","resource":"` + resource + `"`
	if pk != "" {
		body += `,"resourcePartitionKey":["` + pk + `"]`
	}

	rec := e.admin(t, http.MethodPost, "/dbs/d/users/u/permissions", body+"}", hdr)
	if rec.Code != http.StatusCreated {
		return "", rec.Code
	}

	tok, _ := decodeMap(t, rec)["_token"].(string)

	return tok, rec.Code
}

func TestResourceToken_ScopeMatrix(t *testing.T) {
	t.Parallel()

	e := newUserEnv(t)
	e.seed(t)

	perms := []struct{ id, mode, resource, pk string }{
		{"readC1", "Read", "dbs/d/colls/c1", ""},
		{"allC1", "All", "dbs/d/colls/c1", ""},
		{"pkA", "Read", "dbs/d/colls/c1", "a"},
		{"allPkA", "All", "dbs/d/colls/c1", "a"},
		{"docRead", "Read", "dbs/d/colls/c1/docs/1", ""},
		{"docAll", "All", "dbs/d/colls/c1/docs/1", ""},
		{"docPkA", "Read", "dbs/d/colls/c1/docs/1", "a"},
		{"docPkB", "Read", "dbs/d/colls/c1/docs/1", "b"},
	}
	tokens := make(map[string]string, len(perms))

	for _, p := range perms {
		tok, code := e.permission(t, p.id, p.mode, p.resource, p.pk, nil)
		require.Equal(t, http.StatusCreated, code, p.id)
		assert.True(t, strings.HasPrefix(tok, "type=resource&ver=1&sig="), p.id)

		tokens[p.id] = tok
	}

	const q = `{"query":"SELECT * FROM c"}`

	cases := []struct {
		name, perm, method, path, body, hdr string
		want                                int
	}{
		{"read doc read-only", "readC1", http.MethodGet, "/dbs/d/colls/c1/docs/1", "", "pk:a", 200},
		{"read other pk container perm", "readC1", http.MethodGet, "/dbs/d/colls/c1/docs/2", "", "pk:b", 200},
		{"read doc all", "allC1", http.MethodGet, "/dbs/d/colls/c1/docs/1", "", "pk:a", 200},
		{"read doc own pk", "pkA", http.MethodGet, "/dbs/d/colls/c1/docs/1", "", "pk:a", 200},
		{"read doc other pk denied", "pkA", http.MethodGet, "/dbs/d/colls/c1/docs/2", "", "pk:b", 403},
		{"read doc no pk header denied", "pkA", http.MethodGet, "/dbs/d/colls/c1/docs/1", "", "", 403},
		{"doc permission for its own key", "docPkA", http.MethodGet, "/dbs/d/colls/c1/docs/1", "", "pk:a", 200},
		{"doc permission for another key", "docPkB", http.MethodGet, "/dbs/d/colls/c1/docs/1", "", "pk:a", 403},
		{"doc permission without a key header", "docPkA", http.MethodGet, "/dbs/d/colls/c1/docs/1", "", "", 403},
		{"doc permission authorizes its key only", "docPkB", http.MethodGet, "/dbs/d/colls/c1/docs/1", "", "pk:b", 404},
		{"read granted doc", "docRead", http.MethodGet, "/dbs/d/colls/c1/docs/1", "", "pk:a", 200},
		{"read other doc denied", "docRead", http.MethodGet, "/dbs/d/colls/c1/docs/2", "", "pk:b", 403},
		{"cross container denied", "readC1", http.MethodGet, "/dbs/d/colls/c2/docs/1", "", "pk:a", 403},
		{"cross container all denied", "allC1", http.MethodGet, "/dbs/d/colls/c2", "", "", 403},
		{"get container", "readC1", http.MethodGet, "/dbs/d/colls/c1", "", "", 200},
		{"doc perm cannot read container", "docRead", http.MethodGet, "/dbs/d/colls/c1", "", "", 403},
		{"pk ranges read", "readC1", http.MethodGet, "/dbs/d/colls/c1/pkranges", "", "", 200},
		{
			"read perm cannot create",
			"readC1",
			http.MethodPost,
			"/dbs/d/colls/c1/docs",
			`{"id":"n1","pk":"a"}`,
			"pk:a",
			403,
		},
		{"create with all perm", "allC1", http.MethodPost, "/dbs/d/colls/c1/docs", `{"id":"n2","pk":"a"}`, "pk:a", 201},
		{"create in own pk", "allPkA", http.MethodPost, "/dbs/d/colls/c1/docs", `{"id":"n3","pk":"a"}`, "pk:a", 201},
		{
			"create in other pk denied",
			"allPkA",
			http.MethodPost,
			"/dbs/d/colls/c1/docs",
			`{"id":"n4","pk":"b"}`,
			"pk:b",
			403,
		},
		{
			"replace granted doc",
			"docAll",
			http.MethodPut,
			"/dbs/d/colls/c1/docs/1",
			`{"id":"1","pk":"a","v":1}`,
			"pk:a",
			200,
		},
		{
			"read doc perm cannot replace",
			"docRead",
			http.MethodPut,
			"/dbs/d/colls/c1/docs/1",
			`{"id":"1","pk":"a"}`,
			"pk:a",
			403,
		},
		{
			"doc perm cannot create",
			"docAll",
			http.MethodPost,
			"/dbs/d/colls/c1/docs",
			`{"id":"n5","pk":"a"}`,
			"pk:a",
			403,
		},
		{"delete with all perm", "allC1", http.MethodDelete, "/dbs/d/colls/c1/docs/del", "", "pk:a", 204},
		{"delete container denied", "allC1", http.MethodDelete, "/dbs/d/colls/c2", "", "", 403},
		{"query read perm", "readC1", http.MethodPost, "/dbs/d/colls/c1/docs", q, "query", 200},
		{"query own pk", "pkA", http.MethodPost, "/dbs/d/colls/c1/docs", q, "queryA", 200},
		{"cross partition query denied", "pkA", http.MethodPost, "/dbs/d/colls/c1/docs", q, "query", 403},
		{"account root allowed", "docRead", http.MethodGet, "/", "", "", 200},
		{"list databases denied", "allC1", http.MethodGet, "/dbs", "", "", 403},
		{"get database denied", "allC1", http.MethodGet, "/dbs/d", "", "", 403},
		{
			"create container denied",
			"allC1",
			http.MethodPost,
			"/dbs/d/colls",
			`{"id":"x","partitionKey":{"paths":["/pk"]}}`,
			"",
			403,
		},
		{"list users denied", "allC1", http.MethodGet, "/dbs/d/users", "", "", 403},
		{"list permissions denied", "allC1", http.MethodGet, "/dbs/d/users/u/permissions", "", "", 403},
		{"table api denied", "allC1", http.MethodGet, "/Tables", "", "", 403},
		{"offers denied", "allC1", http.MethodGet, "/offers", "", "", 403},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := e.withToken(t, tokens[tc.perm], tc.method, tc.path, tc.body, headersFor(tc.hdr))
			require.Equal(t, tc.want, rec.Code, rec.Body.String())

			if tc.want == http.StatusForbidden {
				assert.Contains(t, rec.Body.String(), "Insufficient permissions provided in the authorization header")
			}
		})
	}
}

func TestResourceToken_Expiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		header  string
		advance time.Duration
		want    int
	}{
		{"default valid before an hour", "", 3599 * time.Second, 200},
		{"default expired at an hour", "", 3600 * time.Second, 403},
		{"custom valid", "600", 599 * time.Second, 200},
		{"custom expired", "600", 600 * time.Second, 403},
		{"max lifetime valid", "86400", 86399 * time.Second, 200},
		{"max lifetime expired", "86400", 24 * time.Hour, 403},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newUserEnv(t)
			e.seed(t)

			hdr := map[string]string{}
			if tc.header != "" {
				hdr["X-Ms-Documentdb-Expiry-Seconds"] = tc.header
			}

			tok, code := e.permission(t, "p", "Read", "dbs/d/colls/c1", "", hdr)
			require.Equal(t, http.StatusCreated, code)

			e.now.Add(int64(tc.advance / time.Second))

			rec := e.withToken(t, tok, http.MethodGet, "/dbs/d/colls/c1", "", nil)
			require.Equal(t, tc.want, rec.Code, rec.Body.String())

			if tc.want == http.StatusForbidden {
				assert.Contains(t, rec.Body.String(), "The authorization token is not valid at the current time")
			}
		})
	}
}

func TestPermission_ExpiryHeaderValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, value string
		want        int
	}{
		{"zero", "0", 400},
		{"negative", "-5", 400},
		{"below the minimum", "599", 400},
		{"too long", "86401", 400},
		{"not a number", "abc", 400},
		{"minimum", "600", 201},
		{"former documented maximum", "18001", 201},
		{"maximum", "86400", 201},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newUserEnv(t)
			e.seed(t)

			_, code := e.permission(t, "p", "Read", "dbs/d/colls/c1", "",
				map[string]string{"X-Ms-Documentdb-Expiry-Seconds": tc.value})
			assert.Equal(t, tc.want, code)
		})
	}
}

func TestPermission_ExpiryMaxSetting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, value, wantMsg string
		max, want            int
	}{
		{"default accepts 86400", "86400", "", 0, 201},
		{"default rejects 86401", "86401", "between 600 and 86400", 0, 400},
		{"configured accepts its max", "18000", "", 18000, 201},
		{"configured rejects above max", "18001", "between 600 and 18000", 18000, 400},
		{"configured rejects 86400", "86400", "between 600 and 18000", 18000, 400},
		{"configured keeps minimum", "599", "between 600 and 18000", 18000, 400},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newUserEnv(t)
			e.h.ResourceTokenMaxSeconds = tc.max
			e.seed(t)

			rec := e.admin(t, http.MethodPost, "/dbs/d/users/u/permissions",
				`{"id":"p","permissionMode":"Read","resource":"dbs/d/colls/c1"}`,
				map[string]string{"X-Ms-Documentdb-Expiry-Seconds": tc.value})
			require.Equal(t, tc.want, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tc.wantMsg)
		})
	}
}

func TestResourceToken_Rejected(t *testing.T) {
	t.Parallel()

	e := newUserEnv(t)
	e.seed(t)

	good, _ := e.permission(t, "p", "Read", "dbs/d/colls/c1", "", nil)
	other, _ := e.permission(t, "all", "All", "dbs/d/colls/c1", "", nil)

	flip := func(s string) string {
		i := strings.Index(s, "sig=") + len("sig=") + 2
		c := byte('A')
		if s[i] == 'A' {
			c = 'B'
		}

		return s[:i] + string(c) + s[i+1:]
	}
	payloadOf := func(s string) string { return s[strings.LastIndex(s, ";")+1:] }
	macOf := func(s string) string { return s[:strings.LastIndex(s, ";")] }

	foreign := newUserEnv(t)
	foreign.h.MasterKey = "Zm9yZ2VkLWtleS1mb3ItdGVzdGluZy1vbmx5LTEyMzQ1Ng=="
	foreign.seed(t)
	forged, _ := foreign.permission(t, "p", "All", "dbs/d/colls/c1", "", nil)

	tests := []struct {
		name  string
		token string
	}{
		{"flipped signature", flip(good)},
		{"payload swapped to wider permission", macOf(good) + ";" + payloadOf(other)},
		{"signed with another key", forged},
		{"missing payload", macOf(good)},
		{"garbage", "type=resource&ver=1&sig=not-a-token"},
		{"empty payload", macOf(good) + ";"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := e.withToken(t, tc.token, http.MethodGet, "/dbs/d/colls/c1", "", nil)
			assert.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
		})
	}
}

func TestResourceToken_RevokedByPermissionChange(t *testing.T) {
	t.Parallel()

	e := newUserEnv(t)
	e.seed(t)

	tok, _ := e.permission(t, "p", "All", "dbs/d/colls/c1", "", nil)
	create := `{"id":"r1","pk":"a"}`

	require.Equal(t, 201, e.withToken(t, tok, http.MethodPost, "/dbs/d/colls/c1/docs", create, pkHdr("a")).Code)

	rec := e.admin(t, http.MethodPut, "/dbs/d/users/u/permissions/p",
		`{"id":"p","permissionMode":"Read","resource":"dbs/d/colls/c1"}`, nil)
	require.Equal(t, 200, rec.Code)

	assert.Equal(t, 403, e.withToken(t, tok, http.MethodPost, "/dbs/d/colls/c1/docs", `{"id":"r2","pk":"a"}`,
		pkHdr("a")).Code)

	require.Equal(t, 204, e.admin(t, http.MethodDelete, "/dbs/d/users/u/permissions/p", "", nil).Code)
	assert.Equal(t, 401, e.withToken(t, tok, http.MethodGet, "/dbs/d/colls/c1", "", nil).Code)
}

func TestResourceToken_NotAcceptedWithoutValidation(t *testing.T) {
	t.Parallel()

	e := newUserEnv(t)
	e.seed(t)

	tok, _ := e.permission(t, "p", "Read", "dbs/d/colls/c1", "", nil)
	e.h.ValidateAuth = false

	assert.Equal(t, 200, e.withToken(t, tok, http.MethodGet, "/dbs/d", "", nil).Code)
}

func TestUsersPermissions_CRUD(t *testing.T) {
	t.Parallel()

	e := newUserEnv(t)
	e.seed(t)

	contRID, _ := decodeMap(t, e.admin(t, http.MethodGet, "/dbs/d/colls/c1", "", nil))["_rid"].(string)
	require.NotEmpty(t, contRID)

	steps := []struct {
		name, method, path, body string
		want                     int
	}{
		{"create user", http.MethodPost, "/dbs/d/users", `{"id":"bob"}`, 201},
		{"duplicate user", http.MethodPost, "/dbs/d/users", `{"id":"bob"}`, 409},
		{"empty user id", http.MethodPost, "/dbs/d/users", `{"id":""}`, 400},
		{"user in missing db", http.MethodPost, "/dbs/zzz/users", `{"id":"x"}`, 404},
		{"get user", http.MethodGet, "/dbs/d/users/bob", "", 200},
		{"get missing user", http.MethodGet, "/dbs/d/users/nobody", "", 404},
		{"list users", http.MethodGet, "/dbs/d/users", "", 200},
		{"replace user rename", http.MethodPut, "/dbs/d/users/bob", `{"id":"robert"}`, 200},
		{"renamed user readable", http.MethodGet, "/dbs/d/users/robert", "", 200},
		{"old name gone", http.MethodGet, "/dbs/d/users/bob", "", 404},
		{"rename onto existing", http.MethodPut, "/dbs/d/users/robert", `{"id":"u"}`, 409},
		{"bad mode", http.MethodPost, "/dbs/d/users/robert/permissions",
			`{"id":"p","permissionMode":"Write","resource":"dbs/d/colls/c1"}`, 400},
		{"database resource", http.MethodPost, "/dbs/d/users/robert/permissions",
			`{"id":"p","permissionMode":"All","resource":"dbs/d"}`, 400},
		{"missing container", http.MethodPost, "/dbs/d/users/robert/permissions",
			`{"id":"p","permissionMode":"All","resource":"dbs/d/colls/nope"}`, 400},
		{"missing document", http.MethodPost, "/dbs/d/users/robert/permissions",
			`{"id":"p","permissionMode":"All","resource":"dbs/d/colls/c1/docs/nope"}`, 400},
		{
			"pk on document resource",
			http.MethodPost,
			"/dbs/d/users/robert/permissions",
			`{"id":"pdoc","permissionMode":"All","resource":"dbs/d/colls/c1/docs/1","resourcePartitionKey":["a"]}`,
			201,
		},
		{
			"pk on document resource is deleted again",
			http.MethodDelete,
			"/dbs/d/users/robert/permissions/pdoc",
			"",
			204,
		},
		{"malformed pk", http.MethodPost, "/dbs/d/users/robert/permissions",
			`{"id":"p","permissionMode":"All","resource":"dbs/d/colls/c1","resourcePartitionKey":[[1]]}`, 400},
		{"permission for missing user", http.MethodPost, "/dbs/d/users/nobody/permissions",
			`{"id":"p","permissionMode":"All","resource":"dbs/d/colls/c1"}`, 404},
		{"create by id link", http.MethodPost, "/dbs/d/users/robert/permissions",
			`{"id":"p","permissionMode":"All","resource":"dbs/d/colls/c1"}`, 201},
		{"duplicate permission", http.MethodPost, "/dbs/d/users/robert/permissions",
			`{"id":"p","permissionMode":"All","resource":"dbs/d/colls/c1"}`, 409},
		{
			"create by rid link",
			http.MethodPost,
			"/dbs/d/users/robert/permissions",
			`{"id":"byrid","permissionMode":"Read","resource":"dbs/` + ridOf(
				"dbs/d",
			) + `/colls/` + contRID + `/"}`,
			201,
		},
		{"get permission", http.MethodGet, "/dbs/d/users/robert/permissions/p", "", 200},
		{"list permissions", http.MethodGet, "/dbs/d/users/robert/permissions", "", 200},
		{"replace permission", http.MethodPut, "/dbs/d/users/robert/permissions/p",
			`{"id":"p","permissionMode":"Read","resource":"dbs/d/colls/c2"}`, 200},
		{"replace missing permission", http.MethodPut, "/dbs/d/users/robert/permissions/zz",
			`{"id":"zz","permissionMode":"Read","resource":"dbs/d/colls/c2"}`, 404},
		{"delete permission", http.MethodDelete, "/dbs/d/users/robert/permissions/p", "", 204},
		{"deleted permission gone", http.MethodGet, "/dbs/d/users/robert/permissions/p", "", 404},
		{"delete user", http.MethodDelete, "/dbs/d/users/robert", "", 204},
		{"deleted user permissions gone", http.MethodGet, "/dbs/d/users/robert/permissions", "", 404},
		{"unsupported verb", http.MethodPatch, "/dbs/d/users", "", 405},
	}

	for _, s := range steps {
		rec := e.admin(t, s.method, s.path, s.body, nil)
		require.Equal(t, s.want, rec.Code, "%s: %s", s.name, rec.Body.String())
	}
}

func ridOf(path string) string { return cosmosdb.FakeRID(path) }

func TestUsersPermissions_ListAndQueryShape(t *testing.T) {
	t.Parallel()

	e := newUserEnv(t)
	e.seed(t)

	_, code := e.permission(t, "p1", "All", "dbs/d/colls/c1", "a", nil)
	require.Equal(t, 201, code)

	list := decodeMap(t, e.admin(t, http.MethodGet, "/dbs/d/users/u/permissions", "", nil))
	items, _ := list["Permissions"].([]any)
	require.Len(t, items, 1)

	item, _ := items[0].(map[string]any)
	assert.Equal(t, "All", item["permissionMode"])
	assert.Equal(t, []any{"a"}, item["resourcePartitionKey"])
	assert.NotEmpty(t, item["_token"])

	qr := e.admin(t, http.MethodPost, "/dbs/d/users/u/permissions", `{"query":"SELECT * FROM c WHERE c.id = 'p1'"}`,
		map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"})
	require.Equal(t, 200, qr.Code, qr.Body.String())
	assert.Len(t, decodeMap(t, qr)["Permissions"], 1)

	users := decodeMap(t, e.admin(t, http.MethodGet, "/dbs/d/users", "", nil))
	assert.Len(t, users["Users"], 1)
}

func TestUsersPermissions_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	e := newUserEnv(t)
	e.seed(t)

	tok, code := e.permission(t, "p", "Read", "dbs/d/colls/c1", "a", nil)
	require.Equal(t, 201, code)

	snap := e.h.Snapshot(t.Context())

	restored := newUserEnv(t)
	require.NoError(t, restored.h.Restore(t.Context(), snap))

	got := decodeMap(t, restored.admin(t, http.MethodGet, "/dbs/d/users/u/permissions/p", "", nil))
	assert.Equal(t, "Read", got["permissionMode"])
	assert.Equal(t, []any{"a"}, got["resourcePartitionKey"])

	assert.Equal(t, 200, restored.withToken(t, tok, http.MethodGet, "/dbs/d/colls/c1/docs/1", "", pkHdr("a")).Code)
	assert.Equal(t, 403, restored.withToken(t, tok, http.MethodGet, "/dbs/d/colls/c1/docs/2", "", pkHdr("b")).Code)
}

func TestResourceToken_ScriptPermissions(t *testing.T) {
	t.Parallel()

	e := newUserEnv(t)
	e.seed(t)

	const sprocBody = `{"id":"s","body":"function(){ getContext().getResponse().setBody(1); }"}`

	for _, step := range []struct{ path, body string }{
		{"/dbs/d/colls/c1/sprocs", sprocBody},
		{"/dbs/d/colls/c1/sprocs", `{"id":"other","body":"function(){}"}`},
		{"/dbs/d/colls/c1/udfs", `{"id":"u1","body":"function(x){return x;}"}`},
		{"/dbs/d/colls/c1/triggers", `{"id":"t1","body":"function(){}","triggerOperation":"All","triggerType":"Pre"}`},
	} {
		rec := e.admin(t, http.MethodPost, step.path, step.body, nil)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	}

	tokens := map[string]string{}

	for id, p := range map[string]struct{ mode, resource string }{
		"sprocAll":  {"All", "dbs/d/colls/c1/sprocs/s"},
		"sprocRead": {"Read", "dbs/d/colls/c1/sprocs/s"},
		"udfRead":   {"Read", "dbs/d/colls/c1/udfs/u1"},
		"trigAll":   {"All", "dbs/d/colls/c1/triggers/t1"},
	} {
		tok, code := e.permission(t, id, p.mode, p.resource, "", nil)
		require.Equal(t, http.StatusCreated, code, id)

		tokens[id] = tok
	}

	_, code := e.permission(t, "missing", "All", "dbs/d/colls/c1/sprocs/none", "", nil)
	assert.Equal(t, http.StatusBadRequest, code)

	cases := []struct {
		name, perm, method, path, body string
		want                           int
	}{
		{"execute with an All permission", "sprocAll", http.MethodPost, "/dbs/d/colls/c1/sprocs/s", "[]", 200},
		{"a Read permission cannot execute", "sprocRead", http.MethodPost, "/dbs/d/colls/c1/sprocs/s", "[]", 403},
		{"read the granted procedure", "sprocRead", http.MethodGet, "/dbs/d/colls/c1/sprocs/s", "", 200},
		{"another procedure is denied", "sprocAll", http.MethodGet, "/dbs/d/colls/c1/sprocs/other", "", 403},
		{"the procedure list is denied", "sprocAll", http.MethodGet, "/dbs/d/colls/c1/sprocs", "", 403},
		{
			"a procedure permission does not reach documents",
			"sprocAll",
			http.MethodGet,
			"/dbs/d/colls/c1/docs/1",
			"",
			403,
		},
		{
			"a procedure permission is not a UDF permission",
			"sprocAll",
			http.MethodGet,
			"/dbs/d/colls/c1/udfs/u1",
			"",
			403,
		},
		{"read the granted UDF", "udfRead", http.MethodGet, "/dbs/d/colls/c1/udfs/u1", "", 200},
		{"a Read permission cannot delete", "udfRead", http.MethodDelete, "/dbs/d/colls/c1/udfs/u1", "", 403},
		{"delete the granted trigger", "trigAll", http.MethodDelete, "/dbs/d/colls/c1/triggers/t1", "", 204},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := e.withToken(t, tokens[tc.perm], tc.method, tc.path, tc.body, pkHdr("a"))
			assert.Equal(t, tc.want, rec.Code, rec.Body.String())
		})
	}
}

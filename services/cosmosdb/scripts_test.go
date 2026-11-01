package cosmosdb_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

const pkA = `["a"]`

func scriptJSON(t *testing.T, fields map[string]string) string {
	t.Helper()

	raw, err := json.Marshal(fields)
	require.NoError(t, err)

	return string(raw)
}

func (s *liveServer) putScript(kind, id, body string, extra map[string]string) rawResponse {
	s.t.Helper()

	fields := map[string]string{"id": id, "body": body}
	maps.Copy(fields, extra)

	return s.do(http.MethodPost, "/dbs/d/colls/c/"+kind, nil, scriptJSON(s.t, fields))
}

func (s *liveServer) runSproc(id, args string, extra map[string]string) rawResponse {
	s.t.Helper()

	h := map[string]string{"X-Ms-Documentdb-Partitionkey": pkA}
	maps.Copy(h, extra)

	return s.do(http.MethodPost, "/dbs/d/colls/c/sprocs/"+id, h, args)
}

func TestScripts_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra    map[string]string
		replaced map[string]string
		kind     string
		listKey  string
	}{
		{kind: "sprocs", listKey: "StoredProcedures"},
		{
			kind: "triggers", listKey: "Triggers",
			extra:    map[string]string{"triggerType": "Pre", "triggerOperation": "Create"},
			replaced: map[string]string{"triggerType": "Post", "triggerOperation": "All"},
		},
		{kind: "udfs", listKey: "UserDefinedFunctions"},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			path := "/dbs/d/colls/c/" + tt.kind

			created := s.putScript(tt.kind, "f1", "function f1(){}", tt.extra)
			require.Equal(t, http.StatusCreated, created.status, string(created.body))
			assert.Equal(t, "f1", created.object(t)["id"])
			assert.NotEmpty(t, created.header.Get("ETag"))

			assert.Equal(t, http.StatusConflict, s.putScript(tt.kind, "f1", "function f1(){}", tt.extra).status)
			assert.Equal(t, http.StatusBadRequest, s.putScript(tt.kind, "bad", "function (", tt.extra).status)
			assert.Equal(t, http.StatusBadRequest, s.putScript(tt.kind, "", "function f(){}", tt.extra).status)

			require.Equal(t, http.StatusCreated, s.putScript(tt.kind, "f0", "function f0(){}", tt.extra).status)

			list := s.do(http.MethodGet, path, nil, "")
			require.Equal(t, http.StatusOK, list.status)

			items, _ := list.object(t)[tt.listKey].([]any)
			require.Len(t, items, 2)
			assert.Equal(t, "f0", items[0].(map[string]any)["id"])

			fields := map[string]string{"id": "f1", "body": "function f1(){ return 1; }"}
			maps.Copy(fields, tt.replaced)

			put := s.do(http.MethodPut, path+"/f1", nil, scriptJSON(t, fields))
			require.Equal(t, http.StatusOK, put.status, string(put.body))
			assert.Equal(t, "function f1(){ return 1; }", put.object(t)["body"])

			withExtra := func(id string) string {
				f := map[string]string{"id": id, "body": "function f(){}"}
				maps.Copy(f, tt.extra)

				return scriptJSON(t, f)
			}

			assert.Equal(t, http.StatusBadRequest, s.do(http.MethodPut, path+"/f1", nil, withExtra("other")).status)
			assert.Equal(t, http.StatusNotFound, s.do(http.MethodPut, path+"/nope", nil, withExtra("nope")).status)

			got := s.do(http.MethodGet, path+"/f1", nil, "")
			require.Equal(t, http.StatusOK, got.status)
			assert.Equal(t, "function f1(){ return 1; }", got.object(t)["body"])

			assert.Equal(t, http.StatusNoContent, s.do(http.MethodDelete, path+"/f1", nil, "").status)
			assert.Equal(t, http.StatusNotFound, s.do(http.MethodGet, path+"/f1", nil, "").status)
			assert.Equal(t, http.StatusNotFound, s.do(http.MethodDelete, path+"/f1", nil, "").status)
		})
	}
}

func TestScripts_TriggerDefinitionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra map[string]string
		name  string
	}{
		{name: "missing type", extra: map[string]string{"triggerOperation": "Create"}},
		{name: "bad type", extra: map[string]string{"triggerType": "During", "triggerOperation": "Create"}},
		{name: "bad operation", extra: map[string]string{"triggerType": "Pre", "triggerOperation": "Upsert"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			assert.Equal(t, http.StatusBadRequest, s.putScript("triggers", "t", "function t(){}", tt.extra).status)
		})
	}
}

func TestSproc_Execute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		args       string
		wantResult string
		wantIDs    []string
		absentIDs  []string
		wantStatus int
	}{
		{
			name: "creates documents and returns a count",
			body: `function s(n) {
				var coll = getContext().getCollection();
				var made = 0;
				for (var i = 0; i < n; i++) {
					coll.createDocument(coll.getSelfLink(), {id: "s" + i, pk: "a", i: i}, function (err, doc) {
						if (err) throw err;
						made++;
					});
				}
				getContext().getResponse().setBody({made: made});
			}`,
			args: `[3]`, wantStatus: 200, wantResult: `{"made":3}`, wantIDs: []string{"s0", "s1", "s2"},
		},
		{
			name: "failing sproc rolls back its writes",
			body: `function s() {
				var coll = getContext().getCollection();
				coll.createDocument(coll.getSelfLink(), {id: "w1", pk: "a"}, function (err) { if (err) throw err; });
				coll.createDocument(coll.getSelfLink(), {id: "w2", pk: "a"}, function (err) { if (err) throw err; });
				coll.deleteDocument("dbs/d/colls/c/docs/seed", function (err) { if (err) throw err; });
				throw new Error("boom");
			}`,
			wantStatus: 400, wantIDs: []string{"seed"}, absentIDs: []string{"w1", "w2"},
		},
		{
			name: "failed operation inside a callback rolls back",
			body: `function s() {
				var coll = getContext().getCollection();
				coll.createDocument(coll.getSelfLink(), {id: "w1", pk: "a"}, function (err) { if (err) throw err; });
				coll.createDocument(coll.getSelfLink(), {id: "seed", pk: "a"}, function (err) { if (err) throw err; });
			}`,
			wantStatus: 400, wantIDs: []string{"seed"}, absentIDs: []string{"w1"},
		},
		{
			name: "operation error reaches the callback with a status",
			body: `function s() {
				var coll = getContext().getCollection();
				coll.createDocument(coll.getSelfLink(), {id: "seed", pk: "a"}, function (err) {
					getContext().getResponse().setBody({status: err.number});
				});
			}`,
			wantStatus: 200, wantResult: `{"status":409}`,
		},
		{
			name: "read replace and delete",
			body: `function s() {
				var coll = getContext().getCollection();
				var out = {};
				coll.readDocument("dbs/d/colls/c/docs/seed", function (err, doc) { out.v = doc.v; });
				coll.replaceDocument("dbs/d/colls/c/docs/seed", {id: "seed", pk: "a", v: 8},
					function (err, doc) { out.now = doc.v; });
				coll.upsertDocument(coll.getSelfLink(), {id: "u1", pk: "a"}, function (err) { if (err) throw err; });
				coll.deleteDocument("dbs/d/colls/c/docs/u1", function (err) { if (err) throw err; });
				getContext().getResponse().setBody(out);
			}`,
			wantStatus: 200, wantResult: `{"v":1,"now":8}`, absentIDs: []string{"u1"},
		},
		{
			name: "queryDocuments is partition scoped",
			body: `function s() {
				var coll = getContext().getCollection();
				coll.queryDocuments(coll.getSelfLink(), "SELECT VALUE COUNT(1) FROM c", function (err, feed) {
					getContext().getResponse().setBody(feed[0]);
				});
			}`,
			wantStatus: 200, wantResult: `2`,
		},
		{
			name: "query with parameters",
			body: `function s(v) {
				var coll = getContext().getCollection();
				var q = {query: "SELECT c.id FROM c WHERE c.v = @v", parameters: [{name: "@v", value: v}]};
				coll.queryDocuments(coll.getSelfLink(), q,
					function (err, feed) { getContext().getResponse().setBody(feed); });
			}`,
			args: `[1]`, wantStatus: 200, wantResult: `[{"id":"seed"}]`,
		},
		{
			name: "writes outside the partition are rejected",
			body: `function s() {
				var coll = getContext().getCollection();
				coll.createDocument(coll.getSelfLink(), {id: "x", pk: "b"}, function (err) {
					getContext().getResponse().setBody({status: err.number});
				});
			}`,
			wantStatus: 200, wantResult: `{"status":400}`, absentIDs: []string{"x"},
		},
		{
			name: "no host access",
			body: `function s() {
				getContext().getResponse().setBody(
					[typeof require, typeof process, typeof fetch, typeof XMLHttpRequest, typeof setTimeout]);
			}`,
			wantStatus: 200, wantResult: `["undefined","undefined","undefined","undefined","undefined"]`,
		},
		{name: "syntax ok but throws on call", body: `function s(){ undefinedFunction(); }`, wantStatus: 400},
		{name: "returns nothing", body: `function s(){}`, wantStatus: 200},
		{name: "bad arguments", body: `function s(){}`, args: `{"a":1}`, wantStatus: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			require.Equal(t, http.StatusCreated, s.create(`{"id":"seed","pk":"a","v":1}`).status)
			require.Equal(t, http.StatusCreated, s.create(`{"id":"seed2","pk":"a","v":2}`).status)
			require.Equal(t, http.StatusCreated, s.putScript("sprocs", "s", tt.body, nil).status)

			resp := s.runSproc("s", tt.args, nil)
			require.Equal(t, tt.wantStatus, resp.status, string(resp.body))

			if tt.wantResult != "" {
				assert.JSONEq(t, tt.wantResult, string(resp.body))
			}

			for _, id := range tt.wantIDs {
				assert.Equal(t, http.StatusOK, s.get(id).status, id)
			}

			for _, id := range tt.absentIDs {
				assert.Equal(t, http.StatusNotFound, s.get(id).status, id)
			}
		})
	}
}

func TestSproc_ExecuteErrors(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	require.Equal(t, http.StatusCreated, s.putScript("sprocs", "s", "function s(){}", nil).status)

	assert.Equal(t, http.StatusNotFound, s.runSproc("nope", "[]", nil).status)
	assert.Equal(t, http.StatusNotFound, s.do(http.MethodPost, "/dbs/d/colls/missing/sprocs/s",
		map[string]string{"X-Ms-Documentdb-Partitionkey": pkA}, "[]").status)
	assert.Equal(t, http.StatusBadRequest, s.do(http.MethodPost, "/dbs/d/colls/c/sprocs/s", nil, "[]").status,
		"partition key header is required")
}

func TestSproc_TimeoutRollsBack(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	cosmosdb.SetScriptTimeout(s.backend, 100*time.Millisecond)

	require.Equal(t, http.StatusCreated, s.putScript("sprocs", "spin", `function spin() {
		var coll = getContext().getCollection();
		coll.createDocument(coll.getSelfLink(), {id: "t1", pk: "a"}, function (err) { if (err) throw err; });
		while (true) {}
	}`, nil).status)

	resp := s.runSproc("spin", "[]", nil)
	assert.Equal(t, http.StatusRequestTimeout, resp.status, string(resp.body))
	assert.Equal(t, http.StatusNotFound, s.get("t1").status)

	require.Equal(
		t,
		http.StatusCreated,
		s.putScript("sprocs", "ok", `function ok(){ getContext().getResponse().setBody(1); }`, nil).status,
	)
	assert.Equal(t, http.StatusOK, s.runSproc("ok", "[]", nil).status, "the store keeps serving after a timeout")
}

func TestSproc_Logging(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	require.Equal(
		t,
		http.StatusCreated,
		s.putScript("sprocs", "log", `function log(){ console.log("hello", 42); }`, nil).status,
	)

	on := s.runSproc("log", "[]", map[string]string{"X-Ms-Documentdb-Script-Enable-Logging": "true"})
	require.Equal(t, http.StatusOK, on.status)

	logs, err := url.QueryUnescape(on.header.Get("X-Ms-Documentdb-Script-Log-Results"))
	require.NoError(t, err)
	assert.Equal(t, "hello 42", logs)

	off := s.runSproc("log", "[]", nil)
	assert.Empty(t, off.header.Get("X-Ms-Documentdb-Script-Log-Results"))
}

func TestTriggers(t *testing.T) {
	t.Parallel()

	const (
		stamp = `function stamp() {
			var req = getContext().getRequest();
			var doc = req.getBody();
			doc.stamped = true;
			req.setBody(doc);
		}`
		audit = `function audit() {
			var coll = getContext().getCollection();
			var doc = getContext().getResponse().getBody();
			coll.createDocument(coll.getSelfLink(), {id: "audit-" + doc.id, pk: "a"}, function (err) { if (err) throw err; });
			doc.audited = true;
			getContext().getResponse().setBody(doc);
		}`
		failPost = `function failPost() { throw new Error("nope"); }`
	)

	tests := []struct {
		headers    map[string]string
		check      func(t *testing.T, s *liveServer, r rawResponse)
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{
			name: "pre trigger edits the created document", method: http.MethodPost, path: "/dbs/d/colls/c/docs",
			body: `{"id":"n1","pk":"a"}`, headers: map[string]string{"X-Ms-Documentdb-Pre-Trigger-Include": "stamp"},
			wantStatus: 201,
			check: func(t *testing.T, s *liveServer, _ rawResponse) {
				t.Helper()

				assert.JSONEq(t, `{"id":"n1","pk":"a","stamped":true}`, stripSystem(t, s.get("n1").body))
			},
		},
		{
			name: "post trigger writes in the same transaction and shapes the response", method: http.MethodPost,
			path: "/dbs/d/colls/c/docs", body: `{"id":"n1","pk":"a"}`,
			headers:    map[string]string{"X-Ms-Documentdb-Post-Trigger-Include": "audit"},
			wantStatus: 201,
			check: func(t *testing.T, s *liveServer, r rawResponse) {
				t.Helper()

				assert.Equal(t, true, r.object(t)["audited"])
				assert.Equal(t, http.StatusOK, s.get("audit-n1").status)
			},
		},
		{
			name:       "failing post trigger rolls the write back",
			method:     http.MethodPost,
			path:       "/dbs/d/colls/c/docs",
			body:       `{"id":"n1","pk":"a"}`,
			headers:    map[string]string{"X-Ms-Documentdb-Post-Trigger-Include": "failPost"},
			wantStatus: 400,
			check: func(t *testing.T, s *liveServer, _ rawResponse) {
				t.Helper()

				assert.Equal(t, http.StatusNotFound, s.get("n1").status)
			},
		},
		{
			name: "pre and post together", method: http.MethodPost, path: "/dbs/d/colls/c/docs",
			body: `{"id":"n1","pk":"a"}`,
			headers: map[string]string{
				"X-Ms-Documentdb-Pre-Trigger-Include": "stamp", "X-Ms-Documentdb-Post-Trigger-Include": "audit",
			},
			wantStatus: 201,
			check: func(t *testing.T, _ *liveServer, r rawResponse) {
				t.Helper()

				assert.Equal(t, true, r.object(t)["stamped"])
				assert.Equal(t, true, r.object(t)["audited"])
			},
		},
		{
			name: "unknown trigger", method: http.MethodPost, path: "/dbs/d/colls/c/docs", body: `{"id":"n1","pk":"a"}`,
			headers: map[string]string{"X-Ms-Documentdb-Pre-Trigger-Include": "ghost"}, wantStatus: 400,
		},
		{
			name:       "trigger of the wrong type",
			method:     http.MethodPost,
			path:       "/dbs/d/colls/c/docs",
			body:       `{"id":"n1","pk":"a"}`,
			headers:    map[string]string{"X-Ms-Documentdb-Pre-Trigger-Include": "audit"},
			wantStatus: 400,
		},
		{
			name:   "trigger for another operation",
			method: http.MethodPut,
			path:   "/dbs/d/colls/c/docs/seed",
			body:   `{"id":"seed","pk":"a"}`,
			headers: map[string]string{
				"X-Ms-Documentdb-Pre-Trigger-Include": "stamp",
				"X-Ms-Documentdb-Partitionkey":        pkA,
			},
			wantStatus: 400,
		},
		{
			name:   "replace fires a Replace trigger",
			method: http.MethodPut,
			path:   "/dbs/d/colls/c/docs/seed",
			body:   `{"id":"seed","pk":"a"}`,
			headers: map[string]string{
				"X-Ms-Documentdb-Pre-Trigger-Include": "stampAll",
				"X-Ms-Documentdb-Partitionkey":        pkA,
			},
			wantStatus: 200,
			check: func(t *testing.T, s *liveServer, _ rawResponse) {
				t.Helper()

				assert.JSONEq(t, `{"id":"seed","pk":"a","stamped":true}`, stripSystem(t, s.get("seed").body))
			},
		},
		{
			name: "upsert of an existing document is a Replace", method: http.MethodPost, path: "/dbs/d/colls/c/docs",
			body: `{"id":"seed","pk":"a"}`,
			headers: map[string]string{
				"X-Ms-Documentdb-Pre-Trigger-Include": "stampAll", "X-Ms-Documentdb-Is-Upsert": "true",
			},
			wantStatus: 200,
		},
		{
			name:   "delete fires a post trigger",
			method: http.MethodDelete,
			path:   "/dbs/d/colls/c/docs/seed",
			headers: map[string]string{
				"X-Ms-Documentdb-Post-Trigger-Include": "onDelete",
				"X-Ms-Documentdb-Partitionkey":         pkA,
			},
			wantStatus: 204,
			check: func(t *testing.T, s *liveServer, _ rawResponse) {
				t.Helper()

				assert.Equal(t, http.StatusNotFound, s.get("seed").status)
				assert.Equal(t, http.StatusOK, s.get("deleted-log").status)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			require.Equal(t, http.StatusCreated, s.create(`{"id":"seed","pk":"a"}`).status)

			create := func(id, body string, extra map[string]string) {
				require.Equal(t, http.StatusCreated, s.putScript("triggers", id, body, extra).status)
			}
			create("stamp", stamp, map[string]string{"triggerType": "Pre", "triggerOperation": "Create"})
			create("stampAll", stamp, map[string]string{"triggerType": "Pre", "triggerOperation": "All"})
			create("audit", audit, map[string]string{"triggerType": "Post", "triggerOperation": "Create"})
			create("failPost", failPost, map[string]string{"triggerType": "Post", "triggerOperation": "All"})
			create("onDelete", `function onDelete() {
				var coll = getContext().getCollection();
				coll.createDocument(coll.getSelfLink(), {id: "deleted-log", pk: "a"}, function (err) { if (err) throw err; });
			}`, map[string]string{"triggerType": "Post", "triggerOperation": "Delete"})

			resp := s.do(tt.method, tt.path, tt.headers, tt.body)
			require.Equal(t, tt.wantStatus, resp.status, string(resp.body))

			if tt.check != nil {
				tt.check(t, s, resp)
			}
		})
	}
}

func TestUDF_InQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		want       string
		wantStatus int
	}{
		{name: "projection", query: "SELECT VALUE udf.double(c.v) FROM c ORDER BY c.v", want: `[2,4,6]`},
		{name: "filter", query: "SELECT VALUE c.id FROM c WHERE udf.double(c.v) > 2 ORDER BY c.id", want: `["b","c"]`},
		{
			name:  "string result and two args",
			query: "SELECT VALUE udf.tag(c.id, c.v) FROM c WHERE c.v = 1",
			want:  `["a-1"]`,
		},
		{
			name:  "udf result in an object",
			query: "SELECT c.id, udf.double(c.v) AS d FROM c WHERE c.v = 3",
			want:  `[{"id":"c","d":6}]`,
		},
		{name: "unknown udf", query: "SELECT VALUE udf.ghost(c.v) FROM c", wantStatus: 400},
		{name: "throwing udf", query: "SELECT VALUE udf.boom(c.v) FROM c", wantStatus: 400},
		{name: "a plain udf alias is untouched", query: "SELECT VALUE udf.v FROM c udf WHERE udf.v = 1", want: `[1]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)

			for _, d := range []string{`{"id":"a","pk":"a","v":1}`, `{"id":"b","pk":"a","v":2}`, `{"id":"c","pk":"a","v":3}`} {
				require.Equal(t, http.StatusCreated, s.create(d).status)
			}

			require.Equal(
				t,
				http.StatusCreated,
				s.putScript("udfs", "double", `function double(x){ return x * 2; }`, nil).status,
			)
			require.Equal(
				t,
				http.StatusCreated,
				s.putScript("udfs", "tag", `function tag(id, v){ return id + "-" + v; }`, nil).status,
			)
			require.Equal(
				t,
				http.StatusCreated,
				s.putScript("udfs", "boom", `function boom(){ throw new Error("x"); }`, nil).status,
			)

			body, err := json.Marshal(map[string]any{"query": tt.query})
			require.NoError(t, err)

			resp := s.do(http.MethodPost, "/dbs/d/colls/c/docs", map[string]string{
				"X-Ms-Documentdb-Isquery": "True", "Content-Type": "application/query+json",
			}, string(body))

			want := tt.wantStatus
			if want == 0 {
				want = http.StatusOK
			}

			require.Equal(t, want, resp.status, string(resp.body))

			if tt.want != "" {
				docs, marshalErr := json.Marshal(resp.object(t)["Documents"])
				require.NoError(t, marshalErr)
				assert.JSONEq(t, tt.want, string(docs))
			}
		})
	}
}

func TestContainer_ConflictResolutionPolicy(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)

	def := s.do(http.MethodGet, "/dbs/d/colls/c", nil, "").object(t)
	assert.Equal(t, "LastWriterWins", def["conflictResolutionPolicy"].(map[string]any)["mode"])

	created := s.do(http.MethodPost, "/dbs/d/colls", nil,
		`{"id":"lww","partitionKey":{"paths":["/pk"],"kind":"Hash"},`+
			`"conflictResolutionPolicy":{"mode":"LastWriterWins","conflictResolutionPath":"/v"}}`)
	require.Equal(t, http.StatusCreated, created.status)

	got := s.do(http.MethodGet, "/dbs/d/colls/lww", nil, "").object(t)
	assert.Equal(t, "/v", got["conflictResolutionPolicy"].(map[string]any)["conflictResolutionPath"])

	custom := s.do(http.MethodPut, "/dbs/d/colls/lww", nil,
		`{"id":"lww","partitionKey":{"paths":["/pk"],"kind":"Hash"},`+
			`"conflictResolutionPolicy":{"mode":"Custom","conflictResolutionProcedure":"dbs/d/colls/lww/sprocs/resolve"}}`)
	assert.Equal(t, http.StatusBadRequest, custom.status, "the policy can only be set when the container is created")

	same := s.do(http.MethodPut, "/dbs/d/colls/lww", nil,
		`{"id":"lww","partitionKey":{"paths":["/pk"],"kind":"Hash"},`+
			`"conflictResolutionPolicy":{"mode":"LastWriterWins","conflictResolutionPath":"/v"}}`)
	assert.Equal(t, http.StatusOK, same.status, "replacing with the same policy is allowed")

	bad := s.do(http.MethodPost, "/dbs/d/colls", nil,
		`{"id":"bad","partitionKey":{"paths":["/pk"],"kind":"Hash"},"conflictResolutionPolicy":{"mode":"Newest"}}`)
	assert.Equal(t, http.StatusBadRequest, bad.status)
}

func TestPersistence_ScriptsAndChangeFeedSurviveRestore(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	require.Equal(t, http.StatusCreated, s.create(`{"id":"a1","pk":"a"}`).status)
	require.Equal(t, http.StatusCreated, s.create(`{"id":"a2","pk":"a"}`).status)
	require.Equal(
		t,
		http.StatusCreated,
		s.putScript("sprocs", "s", `function s(){ getContext().getResponse().setBody(7); }`, nil).status,
	)
	require.Equal(t, http.StatusCreated, s.putScript("udfs", "u", `function u(x){ return x + 1; }`, nil).status)
	require.Equal(t, http.StatusCreated, s.putScript("triggers", "t", `function t(){}`,
		map[string]string{"triggerType": "Pre", "triggerOperation": "All"}).status)

	snapshot := s.h.Snapshot(t.Context())
	require.NotEmpty(t, snapshot)

	restored := newLiveServer(t)
	require.NoError(t, restored.h.Restore(t.Context(), snapshot))

	assert.Equal(t, "7", string(restored.runSproc("s", "[]", nil).body))

	feed := restored.do(http.MethodGet, "/dbs/d/colls/c/docs", map[string]string{"A-Im": "Incremental feed"}, "")
	assert.Equal(t, []string{"a1", "a2"}, feedIDs(t, feed))
	assert.Equal(t, `"2"`, feed.header.Get("ETag"))

	require.Equal(t, http.StatusCreated, restored.create(`{"id":"a3","pk":"a"}`).status)

	delta := restored.do(http.MethodGet, "/dbs/d/colls/c/docs",
		map[string]string{"A-Im": "Incremental feed", "If-None-Match": feed.header.Get("ETag")}, "")
	assert.Equal(t, []string{"a3"}, feedIDs(t, delta))

	for _, kind := range []string{"sprocs", "udfs", "triggers"} {
		assert.Equal(t, http.StatusOK, restored.do(http.MethodGet, "/dbs/d/colls/c/"+kind+"/"+map[string]string{
			"sprocs": "s", "udfs": "u", "triggers": "t",
		}[kind], nil, "").status, kind)
	}
}

func TestSproc_DoesNotBlockOtherContainers(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	cosmosdb.SetScriptTimeout(s.backend, 1500*time.Millisecond)

	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls", nil,
		`{"id":"other","partitionKey":{"paths":["/pk"],"kind":"Hash"}}`).status)
	require.Equal(
		t,
		http.StatusCreated,
		s.putScript("sprocs", "spin", `function spin(){ while (true) {} }`, nil).status,
	)

	var done atomic.Bool

	go func() {
		defer done.Store(true)

		s.runSproc("spin", "[]", nil)
	}()

	var maxGap time.Duration

	last := time.Now()

	for i := 0; !done.Load(); i++ {
		resp := s.do(http.MethodPost, "/dbs/d/colls/other/docs", map[string]string{
			"X-Ms-Documentdb-Partitionkey": pkA,
		}, fmt.Sprintf(`{"id":"w%d","pk":"a"}`, i))
		require.Equal(t, http.StatusCreated, resp.status, string(resp.body))

		now := time.Now()
		maxGap = max(maxGap, now.Sub(last))
		last = now
	}

	assert.Less(t, maxGap, 500*time.Millisecond, "a running stored procedure must not stall writes to other containers")
}

func TestSproc_ConcurrentWriteSameContainerIsSerialized(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	require.Equal(t, http.StatusCreated, s.putScript("sprocs", "inc", `function inc(){
		var coll = getContext().getCollection();
		coll.queryDocuments(coll.getSelfLink(), "SELECT * FROM c", function (err, docs) {
			if (err) throw err;
			coll.createDocument(coll.getSelfLink(), {id: "n" + docs.length, pk: "a"}, function (e) { if (e) throw e; });
		});
	}`, nil).status)

	const runs = 8

	var wg sync.WaitGroup

	for range runs {
		wg.Go(func() {
			assert.Equal(t, http.StatusOK, s.runSproc("inc", "[]", nil).status)
		})
	}

	wg.Wait()

	for i := range runs {
		assert.Equal(t, http.StatusOK, s.get(fmt.Sprintf("n%d", i)).status, "each run saw the previous run's write")
	}
}

func TestSproc_ConcurrentAcrossContainersIsRaceFree(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls", nil,
		`{"id":"c2","partitionKey":{"paths":["/pk"],"kind":"Hash"}}`).status)

	body := scriptJSON(t, map[string]string{
		"id": "noop", "body": `function noop(){ getContext().getResponse().setBody(1); }`,
	})
	for _, coll := range []string{"c", "c2"} {
		require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls/"+coll+"/sprocs", nil, body).status)
	}

	headers := map[string]string{"X-Ms-Documentdb-Partitionkey": pkA}

	var wg sync.WaitGroup

	for i := range 32 {
		coll := []string{"c", "c2"}[i%2]

		wg.Go(func() {
			res := s.do(http.MethodPost, "/dbs/d/colls/"+coll+"/sprocs/noop", headers, "[]")
			assert.Equal(t, http.StatusOK, res.status)
		})
		wg.Go(func() {
			s.do(http.MethodGet, "/dbs/d/colls/"+coll+"/docs/missing", headers, "")
		})
	}

	wg.Wait()
}

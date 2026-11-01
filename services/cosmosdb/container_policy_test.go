package cosmosdb_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

const (
	collDocsPath = "/dbs/d/colls/c/docs"
	pkHeader     = "X-Ms-Documentdb-Partitionkey"
)

// newPolicyHandler returns a handler holding database d and container c created from containerJSON.
func newPolicyHandler(t *testing.T, containerJSON string) *cosmosdb.Handler {
	t.Helper()

	h := newTestHandler(t)
	require.Equal(t, http.StatusCreated, doRequest(t, h, http.MethodPost, "/dbs", nil, []byte(`{"id":"d"}`)).Code)

	rec := doRequest(t, h, http.MethodPost, "/dbs/d/colls", nil, []byte(containerJSON))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	return h
}

const uniqueContainer = `{"id":"c","partitionKey":{"paths":["/pk"],"kind":"Hash"},` +
	`"uniqueKeyPolicy":{"uniqueKeys":[{"paths":["/email"]},{"paths":["/a","/b"]}]}}`

func TestUniqueKeyPolicy_EnforcedOnEveryWritePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers    map[string]string
		name       string
		method     string
		path       string
		body       string
		wantMsg    string
		setup      []string
		wantStatus int
	}{
		{
			name:       "create duplicate email in same partition",
			setup:      []string{`{"id":"1","pk":"x","email":"a@b","a":1,"b":0}`},
			method:     http.MethodPost,
			path:       collDocsPath,
			body:       `{"id":"2","pk":"x","email":"a@b","a":2,"b":0}`,
			wantStatus: http.StatusConflict,
			wantMsg:    "Unique index constraint violation.",
		},
		{
			name:       "create same email in another partition",
			setup:      []string{`{"id":"1","pk":"x","email":"a@b","a":1,"b":0}`},
			method:     http.MethodPost,
			path:       collDocsPath,
			body:       `{"id":"2","pk":"y","email":"a@b"}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "composite key needs every path equal",
			setup:      []string{`{"id":"1","pk":"x","email":"e1","a":1,"b":2}`},
			method:     http.MethodPost,
			path:       collDocsPath,
			body:       `{"id":"2","pk":"x","email":"e2","a":1,"b":3}`,
			wantStatus: http.StatusCreated,
		},
		{
			name: "composite key duplicate", setup: []string{`{"id":"1","pk":"x","email":"e1","a":1,"b":2}`},
			method: http.MethodPost, path: collDocsPath, body: `{"id":"2","pk":"x","email":"e2","a":1,"b":2}`,
			wantStatus: http.StatusConflict, wantMsg: "Unique index constraint violation.",
		},
		{
			name: "two documents missing the path collide", setup: []string{`{"id":"1","pk":"x"}`},
			method: http.MethodPost, path: collDocsPath, body: `{"id":"2","pk":"x"}`,
			wantStatus: http.StatusConflict, wantMsg: "Unique index constraint violation.",
		},
		{
			name:       "upsert of the same document does not conflict with itself",
			setup:      []string{`{"id":"1","pk":"x","email":"a@b","a":1,"b":0}`},
			method:     http.MethodPost,
			path:       collDocsPath,
			body:       `{"id":"1","pk":"x","email":"a@b","v":2,"a":1,"b":0}`,
			headers:    map[string]string{"X-Ms-Documentdb-Is-Upsert": "true"},
			wantStatus: http.StatusCreated,
		},
		{
			name: "upsert colliding with another document",
			setup: []string{
				`{"id":"1","pk":"x","email":"a@b","a":1,"b":0}`,
				`{"id":"2","pk":"x","email":"c@d","a":2,"b":0}`,
			},
			method:     http.MethodPost,
			path:       collDocsPath,
			body:       `{"id":"2","pk":"x","email":"a@b","a":2,"b":0}`,
			headers:    map[string]string{"X-Ms-Documentdb-Is-Upsert": "true"},
			wantStatus: http.StatusConflict,
			wantMsg:    "Unique index constraint violation.",
		},
		{
			name: "replace colliding with another document",
			setup: []string{
				`{"id":"1","pk":"x","email":"a@b","a":1,"b":0}`,
				`{"id":"2","pk":"x","email":"c@d","a":2,"b":0}`,
			},
			method:     http.MethodPut,
			path:       collDocsPath + "/2",
			body:       `{"id":"2","pk":"x","email":"a@b","a":2,"b":0}`,
			headers:    map[string]string{pkHeader: `["x"]`},
			wantStatus: http.StatusConflict,
			wantMsg:    "Unique index constraint violation.",
		},
		{
			name: "patch colliding with another document",
			setup: []string{
				`{"id":"1","pk":"x","email":"a@b","a":1,"b":0}`,
				`{"id":"2","pk":"x","email":"c@d","a":2,"b":0}`,
			},
			method: http.MethodPatch,
			path:   collDocsPath + "/2",
			body:   `{"operations":[{"op":"set","path":"/email","value":"a@b"}]}`,
			headers: map[string]string{
				pkHeader:       `["x"]`,
				"Content-Type": "application/json-patch+json",
			},
			wantStatus: http.StatusConflict,
			wantMsg:    "Unique index constraint violation.",
		},
		{
			name: "replace to a free value succeeds",
			setup: []string{
				`{"id":"1","pk":"x","email":"a@b","a":1,"b":0}`,
				`{"id":"2","pk":"x","email":"c@d","a":2,"b":0}`,
			},
			method:     http.MethodPut,
			path:       collDocsPath + "/2",
			body:       `{"id":"2","pk":"x","email":"free@x","a":2,"b":0}`,
			headers:    map[string]string{pkHeader: `["x"]`},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newPolicyHandler(t, uniqueContainer)

			for _, doc := range tt.setup {
				require.Equal(
					t,
					http.StatusCreated,
					doRequest(t, h, http.MethodPost, collDocsPath, nil, []byte(doc)).Code,
				)
			}

			rec := doRequest(t, h, tt.method, tt.path, tt.headers, []byte(tt.body))
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantMsg != "" {
				assert.Equal(t, tt.wantMsg, decodeBody(t, rec)["message"])
				assert.Equal(t, "Conflict", decodeBody(t, rec)["code"])
			}
		})
	}
}

func TestUniqueKeyPolicy_BatchFailsAtomically(t *testing.T) {
	t.Parallel()

	h := newPolicyHandler(t, uniqueContainer)
	require.Equal(
		t,
		http.StatusCreated,
		doRequest(
			t,
			h,
			http.MethodPost,
			collDocsPath,
			nil,
			[]byte(`{"id":"1","pk":"x","email":"a@b","a":1,"b":0}`),
		).Code,
	)

	body := `[{"operationType":"Create","resourceBody":{"id":"2","pk":"x","email":"new@x","a":2,"b":0}},` +
		`{"operationType":"Create","resourceBody":{"id":"3","pk":"x","email":"a@b","a":3,"b":0}}]`
	rec := doRequest(t, h, http.MethodPost, collDocsPath,
		map[string]string{pkHeader: `["x"]`, "X-Ms-Cosmos-Is-Batch-Request": "true"}, []byte(body))
	assert.Equal(t, http.StatusMultiStatus, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"statusCode":409`)

	rec = doRequest(t, h, http.MethodGet, collDocsPath+"/2", map[string]string{pkHeader: `["x"]`}, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestContainerPolicies_EchoedAndValidated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(t *testing.T, body map[string]any)
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "defaults are echoed",
			body:       `{"id":"x","partitionKey":{"paths":["/pk"]}}`,
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, map[string]any{"uniqueKeys": []any{}}, body["uniqueKeyPolicy"])
				assert.Equal(t, map[string]any{"type": "Geography"}, body["geospatialConfig"])
				assert.NotContains(t, body, "computedProperties")
			},
		},
		{
			name: "settings are stored and echoed",
			body: `{"id":"x","partitionKey":{"paths":["/pk"]},"geospatialConfig":{"type":"Geometry"},` +
				`"vectorEmbeddingPolicy":{"vectorEmbeddings":[{"path":"/v","dataType":"float32",` +
				`"dimensions":3,"distanceFunction":"cosine"}]},` +
				`"computedProperties":[{"name":"lowerName","query":"SELECT VALUE LOWER(c.name) FROM c"}]}`,
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, map[string]any{"type": "Geometry"}, body["geospatialConfig"])
				assert.Contains(t, body, "vectorEmbeddingPolicy")
				assert.Len(t, body["computedProperties"], 1)
			},
		},
		{
			name: "bad geospatial type", wantStatus: http.StatusBadRequest,
			body: `{"id":"x","partitionKey":{"paths":["/pk"]},"geospatialConfig":{"type":"Flat"}}`,
		},
		{
			name: "unique key path needs a slash", wantStatus: http.StatusBadRequest,
			body: `{"id":"x","partitionKey":{"paths":["/pk"]},"uniqueKeyPolicy":{"uniqueKeys":[{"paths":["email"]}]}}`,
		},
		{
			name:       "computed property must be select value",
			wantStatus: http.StatusBadRequest,
			body: `{"id":"x","partitionKey":{"paths":["/pk"]},` +
				`"computedProperties":[{"name":"n","query":"SELECT c.a FROM c"}]}`,
		},
		{
			name:       "computed property name cannot be id",
			wantStatus: http.StatusBadRequest,
			body: `{"id":"x","partitionKey":{"paths":["/pk"]},` +
				`"computedProperties":[{"name":"id","query":"SELECT VALUE 1 FROM c"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.Equal(
				t,
				http.StatusCreated,
				doRequest(t, h, http.MethodPost, "/dbs", nil, []byte(`{"id":"d"}`)).Code,
			)

			rec := doRequest(t, h, http.MethodPost, "/dbs/d/colls", nil, []byte(tt.body))
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.check != nil {
				tt.check(t, decodeBody(t, rec))
			}
		})
	}
}

func TestContainerReplace_UniqueKeyImmutableAndETagChanges(t *testing.T) {
	t.Parallel()

	h := newPolicyHandler(t, uniqueContainer)

	first := doRequest(t, h, http.MethodGet, "/dbs/d/colls/c", nil, nil)
	require.Equal(t, http.StatusOK, first.Code)

	tests := []struct {
		name       string
		unique     string
		wantStatus int
	}{
		{
			name:       "same policy",
			unique:     `{"uniqueKeys":[{"paths":["/email"]},{"paths":["/a","/b"]}]}`,
			wantStatus: http.StatusOK,
		},
		{name: "changed policy", unique: `{"uniqueKeys":[{"paths":["/other"]}]}`, wantStatus: http.StatusBadRequest},
		{name: "dropped policy", unique: `{"uniqueKeys":[]}`, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		body := `{"id":"c","partitionKey":{"paths":["/pk"],"kind":"Hash"},"uniqueKeyPolicy":` + tt.unique + `}`
		rec := doRequest(t, h, http.MethodPut, "/dbs/d/colls/c", nil, []byte(body))
		require.Equal(t, tt.wantStatus, rec.Code, tt.name+": "+rec.Body.String())

		if tt.wantStatus == http.StatusOK {
			assert.NotEqual(t, first.Header().Get("ETag"), rec.Header().Get("ETag"))
			assert.Equal(t, rec.Header().Get("ETag"), decodeBody(t, rec)["_etag"])
		}
	}

	again := doRequest(t, h, http.MethodGet, "/dbs/d/colls/c", nil, nil)
	assert.NotEqual(t, first.Header().Get("ETag"), again.Header().Get("ETag"))
}

func TestComputedProperties_UsableInQueries(t *testing.T) {
	t.Parallel()

	h := newPolicyHandler(t, `{"id":"c","partitionKey":{"paths":["/pk"],"kind":"Hash"},`+
		`"computedProperties":[{"name":"cp_lower","query":"SELECT VALUE LOWER(c.name) FROM c"},`+
		`{"name":"cp_len","query":"SELECT VALUE LENGTH(c.name) FROM c"}],`+
		`"indexingPolicy":{"indexingMode":"consistent","includedPaths":[{"path":"/*"},{"path":"/cp_len/?"}],`+
		`"excludedPaths":[]}}`)

	for _, d := range []string{`{"id":"1","pk":"x","name":"Alice"}`, `{"id":"2","pk":"x","name":"BOB"}`} {
		require.Equal(t, http.StatusCreated, doRequest(t, h, http.MethodPost, collDocsPath, nil, []byte(d)).Code)
	}

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "filter", query: `SELECT VALUE c.id FROM c WHERE c.cp_lower = 'bob'`, want: `["2"]`},
		{name: "project", query: `SELECT VALUE c.cp_len FROM c ORDER BY c.id`, want: `[5,3]`},
		{name: "order by", query: `SELECT VALUE c.id FROM c ORDER BY c.cp_len`, want: `["2","1"]`},
		{name: "computed string is not an array", query: `SELECT VALUE ARRAY_LENGTH(c.cp_lower) FROM c`, want: `[]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, collDocsPath,
				map[string]string{"X-Ms-Documentdb-Isquery": "True", "Content-Type": "application/query+json"},
				[]byte(`{"query":"`+strings.ReplaceAll(tt.query, `"`, `\"`)+`"}`))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.JSONEq(t, tt.want, string(mustJSON(t, decodeBody(t, rec)["Documents"])))
		})
	}

	rec := doRequest(t, h, http.MethodGet, collDocsPath+"/1", map[string]string{pkHeader: `["x"]`}, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, decodeBody(t, rec), "cp_lower")
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()

	out, err := json.Marshal(v)
	require.NoError(t, err)

	return out
}

func TestContainerPolicies_SurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := cosmosdb.NewInMemoryBackend()
	_, err := src.CreateDatabase("d")
	require.NoError(t, err)

	spec := cosmosdb.ContainerSpec{ID: "c", PartitionKeyPath: "/pk",
		UniqueKeyPolicy:    []byte(`{"uniqueKeys":[{"paths":["/email"]}]}`),
		ComputedProperties: []byte(`[{"name":"n","query":"SELECT VALUE 1 FROM c"}]`),
		GeospatialConfig:   []byte(`{"type":"Geometry"}`)}

	created, err := src.CreateContainer("d", spec)
	require.NoError(t, err)

	dst := cosmosdb.NewInMemoryBackend()
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	got, err := dst.GetContainer("d", "c")
	require.NoError(t, err)
	assert.JSONEq(t, string(spec.UniqueKeyPolicy), string(got.UniqueKeyPolicy))
	assert.JSONEq(t, string(spec.ComputedProperties), string(got.ComputedProperties))
	assert.JSONEq(t, string(spec.GeospatialConfig), string(got.GeospatialConfig))
	assert.True(t, created.Modified.Equal(got.Modified))

	_, err = dst.CreateDocument("d", "c", map[string]any{"id": "1", "pk": "x", "email": "e"}, false)
	require.NoError(t, err)
	_, err = dst.CreateDocument("d", "c", map[string]any{"id": "2", "pk": "x", "email": "e"}, false)
	require.ErrorIs(t, err, cosmosdb.ErrUniqueKeyViolation)
}

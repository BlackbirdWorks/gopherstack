package cosmosdb_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func computedBackend(t *testing.T, indexing string, props string) *cosmosdb.InMemoryBackend {
	t.Helper()

	b := cosmosdb.NewInMemoryBackend()
	_, err := b.CreateDatabase("d")
	require.NoError(t, err)

	spec := cosmosdb.ContainerSpec{
		ID: "c", PartitionKeyPath: "/pk", ComputedProperties: json.RawMessage(props),
	}
	if indexing != "" {
		spec.IndexingPolicy = json.RawMessage(indexing)
	}

	_, err = b.CreateContainer("d", spec)
	require.NoError(t, err)

	for i, name := range []string{"Bob", "alice", "Carol"} {
		_, err = b.CreateDocument("d", "c", map[string]any{"id": fmt.Sprintf("d%d", i), "pk": "k", "name": name}, false)
		require.NoError(t, err)
	}

	return b
}

const lowerName = `[{"name":"cp_lower","query":"SELECT VALUE LOWER(c.name) FROM c"}]`

func TestComputedProperties_IndexSelection(t *testing.T) {
	t.Parallel()

	const (
		defaultPolicy = ""
		indexed       = `{"indexingMode":"consistent",` +
			`"includedPaths":[{"path":"/*"},{"path":"/cp_lower/?"}],"excludedPaths":[]}`
		composite = `{"indexingMode":"consistent","includedPaths":[{"path":"/*"}],"excludedPaths":[],` +
			`"compositeIndexes":[[{"path":"/cp_lower","order":"ascending"},{"path":"/name","order":"ascending"}]]}`
	)

	tests := []struct {
		name     string
		indexing string
		query    string
		wantErr  string
		want     []any
	}{
		{
			name: "filter on an unindexed computed property is evaluated at run time", indexing: defaultPolicy,
			query: `SELECT VALUE c.id FROM c WHERE c.cp_lower = "alice"`, want: []any{"d1"},
		},
		{
			name: "the wildcard path does not index a computed property", indexing: defaultPolicy,
			query: `SELECT VALUE c.id FROM c ORDER BY c.cp_lower`, wantErr: "order-by item is excluded",
		},
		{
			name: "an explicit path indexes it for ORDER BY", indexing: indexed,
			query: `SELECT VALUE c.id FROM c ORDER BY c.cp_lower`, want: []any{"d1", "d0", "d2"},
		},
		{
			name: "a composite index can include it", indexing: composite,
			query: `SELECT VALUE c.id FROM c ORDER BY c.cp_lower, c.name`, want: []any{"d1", "d0", "d2"},
		},
		{
			name:     "group by a computed property",
			indexing: defaultPolicy,
			query:    `SELECT VALUE COUNT(1) FROM c GROUP BY c.cp_lower`,
			want:     []any{json.Number("1"), json.Number("1"), json.Number("1")},
		},
		{
			name: "select star leaves computed properties out", indexing: defaultPolicy,
			query: `SELECT VALUE IS_DEFINED(c.cp_lower) FROM c WHERE c.id = "d0"`, want: []any{true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := computedBackend(t, tt.indexing, lowerName)

			res, err := b.RunQueryOpts("d", "c", tt.query, nil, "", cosmosdb.QueryOptions{})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, res.Rows)
		})
	}
}

func TestComputedProperties_ShadowAPersistedProperty(t *testing.T) {
	t.Parallel()

	b := computedBackend(t, "", `[{"name":"name","query":"SELECT VALUE UPPER(c.name) FROM c"}]`)

	byName, err := b.RunQuery("d", "c", `SELECT VALUE c.name FROM c WHERE c.id = "d1"`, nil, "")
	require.NoError(t, err)
	assert.Equal(t, []any{"ALICE"}, byName.Rows, "queries use the computed property over the persisted one")

	star, err := b.RunQuery("d", "c", `SELECT * FROM c WHERE c.id = "d1"`, nil, "")
	require.NoError(t, err)
	require.Len(t, star.Rows, 1)
	assert.Equal(t, "alice", star.Rows[0].(map[string]any)["name"], "a wildcard projection returns the persisted value")
}

func TestComputedProperties_DefinitionRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		props string
		want  int
	}{
		{name: "valid", props: lowerName, want: http.StatusCreated},
		{
			name:  "root alias forms",
			props: `[{"name":"a","query":"SELECT VALUE c.x FROM root c"}]`,
			want:  http.StatusCreated,
		},
		{
			name:  "reserved name",
			props: `[{"name":"id","query":"SELECT VALUE c.x FROM c"}]`,
			want:  http.StatusBadRequest,
		},
		{
			name:  "system property name",
			props: `[{"name":"_ts","query":"SELECT VALUE c.x FROM c"}]`,
			want:  http.StatusBadRequest,
		},
		{
			name:  "duplicate names",
			props: `[{"name":"a","query":"SELECT VALUE 1 FROM c"},{"name":"a","query":"SELECT VALUE 2 FROM c"}]`,
			want:  http.StatusBadRequest,
		},
		{name: "no VALUE", props: `[{"name":"a","query":"SELECT c.x FROM c"}]`, want: http.StatusBadRequest},
		{
			name:  "where clause",
			props: `[{"name":"a","query":"SELECT VALUE c.x FROM c WHERE c.y = 1"}]`,
			want:  http.StatusBadRequest,
		},
		{
			name:  "join",
			props: `[{"name":"a","query":"SELECT VALUE t FROM c JOIN t IN c.tags"}]`,
			want:  http.StatusBadRequest,
		},
		{
			name:  "nondeterministic",
			props: `[{"name":"a","query":"SELECT VALUE RAND() FROM c"}]`,
			want:  http.StatusBadRequest,
		},
		{
			name:  "current time",
			props: `[{"name":"a","query":"SELECT VALUE GetCurrentTimestamp() FROM c"}]`,
			want:  http.StatusBadRequest,
		},
		{
			name:  "aggregate",
			props: `[{"name":"a","query":"SELECT VALUE COUNT(1) FROM c"}]`,
			want:  http.StatusBadRequest,
		},
		{
			name:  "spatial function",
			props: `[{"name":"a","query":"SELECT VALUE ST_ISVALID(c.loc) FROM c"}]`,
			want:  http.StatusBadRequest,
		},
		{
			name:  "scalar subquery",
			props: `[{"name":"a","query":"SELECT VALUE (SELECT VALUE 1) FROM c"}]`,
			want:  http.StatusBadRequest,
		},
		{name: "not a query", props: `[{"name":"a","query":"SELECT"}]`, want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			resp := s.do(http.MethodPost, "/dbs/d/colls", nil,
				`{"id":"cp","partitionKey":{"paths":["/pk"],"kind":"Hash"},"computedProperties":`+tt.props+`}`)
			assert.Equal(t, tt.want, resp.status, string(resp.body))
		})
	}

	t.Run("more than twenty", func(t *testing.T) {
		t.Parallel()

		props := make([]map[string]string, 0, 21)
		for i := range 21 {
			props = append(props, map[string]string{"name": fmt.Sprintf("p%d", i), "query": "SELECT VALUE 1 FROM c"})
		}

		raw, err := json.Marshal(props)
		require.NoError(t, err)

		s := newLiveServer(t)
		resp := s.do(http.MethodPost, "/dbs/d/colls", nil,
			`{"id":"cp","partitionKey":{"paths":["/pk"],"kind":"Hash"},"computedProperties":`+string(raw)+`}`)
		assert.Equal(t, http.StatusBadRequest, resp.status)
	})

	t.Run("a spatial index on a computed property", func(t *testing.T) {
		t.Parallel()

		s := newLiveServer(t)
		resp := s.do(http.MethodPost, "/dbs/d/colls", nil, `{"id":"cp","partitionKey":{"paths":["/pk"],"kind":"Hash"},`+
			`"computedProperties":[{"name":"cp_loc","query":"SELECT VALUE c.loc FROM c"}],`+
			`"indexingPolicy":{"indexingMode":"consistent","includedPaths":[{"path":"/*"}],"excludedPaths":[],`+
			`"spatialIndexes":[{"path":"/cp_loc/*","types":["Point"]}]}}`)
		assert.Equal(t, http.StatusBadRequest, resp.status)
	})
}

package cosmosdb_test

import (
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	msgComposite = "The order by query does not have a corresponding composite index that it can be served from."
	msgScan      = "An invalid query has been specified with filters against path(s) excluded from indexing. " +
		"Consider adding allow scan header in the request."
)

func policyContainer(policy string) string {
	return `{"id":"c","partitionKey":{"paths":["/pk"],"kind":"Hash"},"indexingPolicy":` + policy + `}`
}

func TestIndexingPolicy_QueryEnforcement(t *testing.T) {
	t.Parallel()

	const (
		composite = `{"indexingMode":"consistent","automatic":true,"includedPaths":[{"path":"/*"}],"excludedPaths":[],` +
			`"compositeIndexes":[[{"path":"/name","order":"ascending"},{"path":"/age","order":"descending"}]]}`
		excluded = `{"indexingMode":"consistent","automatic":true,"includedPaths":[{"path":"/*"}],` +
			`"excludedPaths":[{"path":"/secret/*"},{"path":"/\"_etag\"/?"}]}`
		partial = `{"indexingMode":"consistent","automatic":true,"includedPaths":[{"path":"/name/?"}],` +
			`"excludedPaths":[{"path":"/*"}]}`
		none = `{"indexingMode":"none","automatic":false}`
	)

	tests := []struct {
		name    string
		policy  string
		query   string
		headers map[string]string
		wantMsg string
	}{
		{
			name:    "default policy multi order by needs composite",
			policy:  ``,
			query:   `SELECT * FROM c ORDER BY c.name, c.age`,
			wantMsg: msgComposite,
		},
		{name: "single order by needs no composite", policy: ``, query: `SELECT * FROM c ORDER BY c.name`},
		{name: "matching composite", policy: composite, query: `SELECT * FROM c ORDER BY c.name ASC, c.age DESC`},
		{name: "fully opposite composite", policy: composite, query: `SELECT * FROM c ORDER BY c.name DESC, c.age ASC`},
		{
			name:    "mixed directions",
			policy:  composite,
			query:   `SELECT * FROM c ORDER BY c.name ASC, c.age ASC`,
			wantMsg: msgComposite,
		},
		{
			name:    "wrong path order",
			policy:  composite,
			query:   `SELECT * FROM c ORDER BY c.age DESC, c.name ASC`,
			wantMsg: msgComposite,
		},
		{
			name:    "excluded path filter needs scan",
			policy:  excluded,
			query:   `SELECT * FROM c WHERE c.secret.token = 'x'`,
			wantMsg: msgScan,
		},
		{
			name: "scan header allows it", policy: excluded, query: `SELECT * FROM c WHERE c.secret.token = 'x'`,
			headers: map[string]string{"X-Ms-Documentdb-Query-Enable-Scan": "true"},
		},
		{
			name: "sdk scan header allows it", policy: excluded, query: `SELECT * FROM c WHERE c.secret.token = 'x'`,
			headers: map[string]string{"X-Ms-Documentdb-Force-Query-Scan": "true"},
		},
		{name: "indexed filter is fine", policy: excluded, query: `SELECT * FROM c WHERE c.name = 'x'`},
		{
			name: "and with an indexed side is served", policy: excluded,
			query: `SELECT * FROM c WHERE c.name = 'x' AND c.secret.token = 'y'`,
		},
		{
			name: "or with an excluded side needs scan", policy: excluded,
			query: `SELECT * FROM c WHERE c.name = 'x' OR c.secret.token = 'y'`, wantMsg: msgScan,
		},
		{name: "included path only", policy: partial, query: `SELECT * FROM c WHERE c.name = 'x'`},
		{name: "everything else excluded", policy: partial, query: `SELECT * FROM c WHERE c.age = 3`, wantMsg: msgScan},
		{name: "no filter never needs scan", policy: none, query: `SELECT * FROM c`},
		{
			name:    "none mode filter needs scan",
			policy:  none,
			query:   `SELECT * FROM c WHERE c.name = 'x'`,
			wantMsg: msgScan,
		},
		{name: "none mode id filter is fine", policy: none, query: `SELECT * FROM c WHERE c.id = '1'`},
		{
			name: "none mode with scan", policy: none, query: `SELECT * FROM c WHERE c.name = 'x'`,
			headers: map[string]string{"X-Ms-Documentdb-Query-Enable-Scan": "true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			container := `{"id":"c","partitionKey":{"paths":["/pk"],"kind":"Hash"}}`
			if tt.policy != "" {
				container = policyContainer(tt.policy)
			}

			h := newPolicyHandler(t, container)
			headers := map[string]string{"X-Ms-Documentdb-Isquery": "True", "Content-Type": "application/query+json"}

			maps.Copy(headers, tt.headers)

			rec := doRequest(t, h, http.MethodPost, collDocsPath, headers,
				[]byte(`{"query":"`+strings.ReplaceAll(tt.query, `"`, `\"`)+`"}`))

			if tt.wantMsg == "" {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Equal(t, "BadRequest", decodeBody(t, rec)["code"])
			assert.Equal(t, tt.wantMsg, decodeBody(t, rec)["message"])
		})
	}
}

package azuresearch_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresearch"
)

type fakeTables struct {
	filter string
	ents   []azuresearch.TableEntity
}

func (f *fakeTables) ListEntities(table, filter string) ([]azuresearch.TableEntity, error) {
	if table != "hotels" {
		return nil, errNoDoc
	}

	f.filter = filter

	return f.ents, nil
}

func TestIndexerTableSource(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	tables := &fakeTables{ents: []azuresearch.TableEntity{
		{
			PartitionKey: "p", RowKey: "1_a", ETag: "e1", Timestamp: time.Unix(1700000000, 0),
			Properties: map[string]any{"content": "alpha entity", "name": "first"},
		},
		{
			PartitionKey: "p", RowKey: "2", ETag: "e2",
			Properties: map[string]any{"content": "beta entity", "name": "second"},
		},
	}}
	s.b.SetTableSource(tables)

	s.must(t, http.MethodPost, "/indexes?"+apiVer, docsIndex(), http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('ts')?"+apiVer, map[string]any{
		"name": "ts", "type": "azuretable",
		"credentials": map[string]any{"connectionString": "DefaultEndpointsProtocol=https;AccountName=acct"},
		"container":   map[string]any{"name": "hotels", "query": "PartitionKey eq 'p'"},
		"dataDeletionDetectionPolicy": map[string]any{
			"@odata.type":          "#Microsoft.Azure.Search.SoftDeleteColumnDeletionDetectionPolicy",
			"softDeleteColumnName": "IsDeleted", "softDeleteMarkerValue": "true",
		},
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, map[string]any{
		"name": "tx", "dataSourceName": "ts", "targetIndexName": "docs",
		"fieldMappings": []any{map[string]any{"sourceFieldName": "Key", "targetFieldName": "id"}},
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers('tx')/search.run?"+apiVer, nil, http.StatusAccepted)

	assert.Equal(t, "PartitionKey eq 'p'", tables.filter)
	assert.Equal(t, []string{"first", "second"}, s.searchIDs(t, "entity"))

	got := s.must(t, http.MethodGet, "/indexes('docs')/docs('p1_a')?"+apiVer, nil, http.StatusOK)
	assert.Equal(t, "first", got.body["name"], "the key is PartitionKey+RowKey")

	tables.ents[0].ETag = "e1b"
	tables.ents[0].Properties["IsDeleted"] = true

	s.must(t, http.MethodPost, "/indexers('tx')/search.run?"+apiVer, nil, http.StatusAccepted)
	assert.Equal(t, []string{"second"}, s.searchIDs(t, "entity"))
}

func TestIndexerTableSourceUnavailable(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	s.must(t, http.MethodPost, "/indexes?"+apiVer, docsIndex(), http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('ts')?"+apiVer, map[string]any{
		"name": "ts", "type": "azuretable",
		"credentials": map[string]any{"connectionString": "AccountName=acct"},
		"container":   map[string]any{"name": "hotels"},
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, map[string]any{
		"name": "tx", "dataSourceName": "ts", "targetIndexName": "docs",
	}, http.StatusCreated)

	s.must(t, http.MethodPost, "/indexers('tx')/search.run?"+apiVer, nil, http.StatusAccepted)

	last, _ := s.status(t, "tx")["lastResult"].(map[string]any)
	require.NotNil(t, last)
	assert.Equal(t, "transientFailure", last["status"])
}

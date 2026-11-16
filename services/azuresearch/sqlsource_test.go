package azuresearch_test

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSQL struct {
	last [3]string
	rows [][]string
	mu   sync.Mutex
}

func (f *fakeSQL) Query(_ context.Context, server, db, text string) ([]string, [][]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.last = [3]string{server, db, text}

	return []string{"HotelId", "City"}, f.rows, nil
}

func TestAzureSQLDataSource(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	src := &fakeSQL{rows: [][]string{{"1", "Seattle"}, {"2", "Paris"}}}
	s.b.SetSQLSource(src)

	s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{"name": "docs", "fields": []any{
		map[string]any{"name": "id", "type": "Edm.String", "key": true},
		map[string]any{"name": "city", "type": "Edm.String", "searchable": true},
	}}, http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('sql')?"+apiVer, map[string]any{
		"name": "sql", "type": "azuresql", "container": map[string]any{"name": "dbo.Hotels"},
		"credentials": map[string]any{
			"connectionString": "Server=tcp:srv1.database.windows.net,1433;Database=db1;User ID=u;Password=p;",
		},
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, map[string]any{
		"name": "ix", "dataSourceName": "sql", "targetIndexName": "docs",
		"fieldMappings": []any{
			map[string]any{"sourceFieldName": "HotelId", "targetFieldName": "id"},
			map[string]any{"sourceFieldName": "City", "targetFieldName": "city"},
		},
	}, http.StatusCreated)

	run := func() map[string]any {
		s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

		last, _ := s.status(t, "ix")["lastResult"].(map[string]any)

		return last
	}

	first := run()
	assert.Equal(t, "success", first["status"], "%v", first)
	assert.EqualValues(t, 2, first["itemsProcessed"])
	assert.Equal(t, [3]string{"srv1", "db1", "SELECT * FROM [dbo].[Hotels]"}, src.last)

	r := s.admin2(t, http.MethodGet, "/indexes('docs')/docs?"+apiVer+"&search=Paris&$select=id", nil)
	assert.Equal(t, []string{"2"}, keysByField(r, "id"))

	assert.EqualValues(t, 0, run()["itemsProcessed"], "unchanged rows are skipped")

	src.mu.Lock()
	src.rows[1][1] = "Lyon"
	src.mu.Unlock()

	assert.EqualValues(t, 1, run()["itemsProcessed"], "a changed row is reindexed")

	r = s.admin2(t, http.MethodGet, "/indexes('docs')/docs?"+apiVer+"&search=Lyon&$select=id", nil)
	assert.Equal(t, []string{"2"}, keysByField(r, "id"))
}

func TestAzureSQLDataSourceRejectsBadTable(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.b.SetSQLSource(&fakeSQL{})

	s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{"name": "docs", "fields": []any{
		map[string]any{"name": "id", "type": "Edm.String", "key": true},
	}}, http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('sql')?"+apiVer, map[string]any{
		"name": "sql", "type": "azuresql", "container": map[string]any{"name": "t; DROP TABLE x"},
		"credentials": map[string]any{"connectionString": "Server=tcp:srv1.database.windows.net;Database=d;"},
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, map[string]any{
		"name": "ix", "dataSourceName": "sql", "targetIndexName": "docs",
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

	last, _ := s.status(t, "ix")["lastResult"].(map[string]any)
	assert.Equal(t, "transientFailure", last["status"])
	require.NotEmpty(t, last["errorMessage"])
}

func TestADLSGen2DataSourceReadsFolder(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	blobs := newFakeBlobs()
	blobs.put("folder/a.txt", "inside folder")
	blobs.put("other/b.txt", "outside folder")
	s.b.SetBlobSource(blobs)

	s.must(t, http.MethodPost, "/indexes?"+apiVer, docsIndex(), http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('lake')?"+apiVer, map[string]any{
		"name": "lake", "type": "adlsgen2", "container": map[string]any{"name": "docs", "query": "folder"},
		"credentials": map[string]any{
			"connectionString": "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=a2V5;",
		},
	}, http.StatusCreated)
	s.must(
		t,
		http.MethodPost,
		"/indexers?"+apiVer,
		blobIndexer(map[string]any{"dataSourceName": "lake"}),
		http.StatusCreated,
	)
	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

	assert.Equal(t, []string{"folder/a.txt"}, s.searchIDs(t, "folder"))
}

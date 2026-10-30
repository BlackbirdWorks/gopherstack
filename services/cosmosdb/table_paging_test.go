package cosmosdb_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	nextPKHeader = "X-Ms-Continuation-Nextpartitionkey"
	nextRKHeader = "X-Ms-Continuation-Nextrowkey"
)

func TestTableQuery_ContinuationPages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantNext  string
		entities  int
		wantFirst int
	}{
		{name: "top splits the result", entities: 5, query: "?$top=2", wantFirst: 2, wantNext: "r0002"},
		{name: "a short result has no continuation", entities: 2, query: "?$top=5", wantFirst: 2},
		{name: "a page holds at most 1000", entities: 1002, query: "", wantFirst: 1000, wantNext: "r1000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatchTable(t)

			for i := range tt.entities {
				body := fmt.Sprintf(`{"PartitionKey":"p","RowKey":"r%04d"}`, i)
				require.Equal(t, http.StatusCreated, doRequest(t, h, http.MethodPost, "/t", nil, []byte(body)).Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/t()"+tt.query, nil, nil)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Len(t, decodeBody(t, rec)["value"], tt.wantFirst)

			if tt.wantNext == "" {
				assert.Empty(t, rec.Header().Get(nextPKHeader))

				return
			}

			assert.Equal(t, "p", rec.Header().Get(nextPKHeader))
			next := rec.Header().Get(nextRKHeader)
			assert.Equal(t, tt.wantNext, next)

			rest := doRequest(t, h, http.MethodGet,
				"/t()?NextPartitionKey=p&NextRowKey="+next, nil, nil)
			require.Equal(t, http.StatusOK, rest.Code)
			assert.Len(t, decodeBody(t, rest)["value"], tt.entities-tt.wantFirst)
		})
	}
}

func TestTableBatch_QueryPartCarriesContinuation(t *testing.T) {
	t.Parallel()

	h := newBatchTable(t)

	for _, rk := range []string{"a", "b", "c"} {
		body := `{"PartitionKey":"p","RowKey":"` + rk + `"}`
		require.Equal(t, http.StatusCreated, doRequest(t, h, http.MethodPost, "/t", nil, []byte(body)).Code)
	}

	rec := postBatch(t, h, buildRetrieveBatch("t()?$top=2", false))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "x-ms-continuation-NextPartitionKey: p")
	assert.Contains(t, rec.Body.String(), "x-ms-continuation-NextRowKey: c")
}

func TestTableBatch_SecondChangesetRejected(t *testing.T) {
	t.Parallel()

	h := newBatchTable(t)
	one := buildBatch(batchOp{method: "POST", path: "t", body: `{"PartitionKey":"p","RowKey":"a"}`})
	two := strings.Replace(
		one,
		"--"+batchBoundary+"--\r\n",
		"--"+batchBoundary+"\r\nContent-Type: multipart/mixed; boundary=cs2\r\n\r\n--cs2--\r\n--"+batchBoundary+"--\r\n",
		1,
	)

	rec := postBatch(t, h, two)
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, http.StatusNotFound, getEntityStatus(t, h, "a"), "nothing from the batch is applied")
}

func TestTableAPI_ListTablesPagesAndSelect(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"ta", "tb", "tc"} {
		body := `{"TableName":"` + name + `"}`
		require.Equal(t, http.StatusCreated, doRequest(t, h, http.MethodPost, "/Tables", nil, []byte(body)).Code)
	}

	first := doRequest(t, h, http.MethodGet, "/Tables?$top=2", nil, nil)
	require.Equal(t, http.StatusOK, first.Code)
	assert.Len(t, decodeBody(t, first)["value"], 2)
	assert.Equal(t, "tc", first.Header().Get("X-Ms-Continuation-Nexttablename"))

	rest := doRequest(t, h, http.MethodGet, "/Tables?NextTableName=tc", nil, nil)
	assert.Len(t, decodeBody(t, rest)["value"], 1)
	assert.Empty(t, rest.Header().Get("X-Ms-Continuation-Nexttablename"))

	require.Equal(t, http.StatusCreated, doRequest(t, h, http.MethodPost, "/ta", nil,
		[]byte(`{"PartitionKey":"p","RowKey":"r","A":"x","B":"y"}`)).Code)

	sel := doRequest(t, h, http.MethodGet, "/ta()?$select=A", nil, nil)
	require.Equal(t, http.StatusOK, sel.Code)

	row, ok := decodeBody(t, sel)["value"].([]any)[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "x", row["A"])
	assert.NotContains(t, row, "PartitionKey")
	assert.NotContains(t, row, "RowKey")
	assert.NotContains(t, row, "B")
}

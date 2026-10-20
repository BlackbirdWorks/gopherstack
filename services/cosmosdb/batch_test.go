package cosmosdb_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_TransactionalBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build      func(*azcosmos.TransactionalBatch)
		wantBody   map[string]string
		name       string
		wantCodes  []int32
		wantAbsent []string
		wantOK     bool
	}{
		{
			name: "all operation kinds commit",
			build: func(b *azcosmos.TransactionalBatch) {
				b.CreateItem([]byte(`{"id":"n1","pk":"a","v":1}`), nil)
				b.UpsertItem([]byte(`{"id":"n2","pk":"a","v":2}`), nil)
				b.UpsertItem([]byte(`{"id":"seed","pk":"a","v":9}`), nil)
				b.ReadItem("seed", nil)
				b.ReplaceItem("seed", []byte(`{"id":"seed","pk":"a","v":3}`), nil)
				var ops azcosmos.PatchOperations
				ops.AppendIncrement("/v", 10)
				b.PatchItem("seed", ops, nil)
				b.DeleteItem("gone", nil)
			},
			wantOK:    true,
			wantCodes: []int32{201, 201, 200, 200, 200, 200, 204},
			wantBody:  map[string]string{"seed": `{"id":"seed","pk":"a","v":13}`, "n1": `{"id":"n1","pk":"a","v":1}`},
			wantAbsent: []string{
				"gone",
			},
		},
		{
			name: "duplicate create rolls everything back",
			build: func(b *azcosmos.TransactionalBatch) {
				b.CreateItem([]byte(`{"id":"n1","pk":"a"}`), nil)
				b.DeleteItem("gone", nil)
				b.CreateItem([]byte(`{"id":"seed","pk":"a"}`), nil)
				b.CreateItem([]byte(`{"id":"n3","pk":"a"}`), nil)
			},
			wantCodes:  []int32{424, 424, 409, 424},
			wantAbsent: []string{"n1", "n3"},
			wantBody:   map[string]string{"gone": `{"id":"gone","pk":"a"}`},
		},
		{
			name: "missing read fails the batch",
			build: func(b *azcosmos.TransactionalBatch) {
				b.CreateItem([]byte(`{"id":"n1","pk":"a"}`), nil)
				b.ReadItem("nope", nil)
			},
			wantCodes:  []int32{424, 404},
			wantAbsent: []string{"n1"},
		},
		{
			name: "operation in another partition fails",
			build: func(b *azcosmos.TransactionalBatch) {
				b.CreateItem([]byte(`{"id":"n1","pk":"b"}`), nil)
			},
			wantCodes:  []int32{400},
			wantAbsent: []string{"n1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			c := s.container()
			pk := azcosmos.NewPartitionKeyString("a")

			for _, seed := range []string{`{"id":"seed","pk":"a","v":0}`, `{"id":"gone","pk":"a"}`} {
				require.Equal(t, http.StatusCreated, s.create(seed).status)
			}

			batch := c.NewTransactionalBatch(pk)
			tt.build(&batch)

			resp, err := c.ExecuteTransactionalBatch(t.Context(), batch, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantOK, resp.Success)

			codes := make([]int32, len(resp.OperationResults))
			for i, r := range resp.OperationResults {
				codes[i] = r.StatusCode
			}

			assert.Equal(t, tt.wantCodes, codes)

			for _, id := range tt.wantAbsent {
				assert.Equal(t, http.StatusNotFound, s.get(id).status, id)
			}

			for id, want := range tt.wantBody {
				got := s.get(id)
				require.Equal(t, http.StatusOK, got.status, id)
				assert.JSONEq(t, want, stripSystem(t, got.body))
			}
		})
	}
}

func TestSDK_TransactionalBatch_ReadReturnsBody(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	c := s.container()

	require.Equal(t, http.StatusCreated, s.create(`{"id":"seed","pk":"a","v":5}`).status)

	batch := c.NewTransactionalBatch(azcosmos.NewPartitionKeyString("a"))
	batch.ReadItem("seed", nil)

	resp, err := c.ExecuteTransactionalBatch(t.Context(), batch, nil)
	require.NoError(t, err)
	require.True(t, resp.Success)
	require.Len(t, resp.OperationResults, 1)
	assert.JSONEq(t, `{"id":"seed","pk":"a","v":5}`, stripSystem(t, resp.OperationResults[0].ResourceBody))
	assert.NotEmpty(t, resp.OperationResults[0].ETag)
}

func TestRaw_BatchLimits(t *testing.T) {
	t.Parallel()

	op := func(i int) string {
		return fmt.Sprintf(`{"operationType":"Create","resourceBody":{"id":"b%d","pk":"a"}}`, i)
	}

	build := func(n int) string {
		ops := make([]string, n)
		for i := range ops {
			ops[i] = op(i)
		}

		return "[" + strings.Join(ops, ",") + "]"
	}

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "hundred operations", body: build(100), wantStatus: http.StatusOK},
		{name: "over the limit", body: build(101), wantStatus: http.StatusBadRequest},
		{name: "empty", body: "[]", wantStatus: http.StatusBadRequest},
		{name: "unknown operation type", body: `[{"operationType":"Merge"}]`, wantStatus: http.StatusBadRequest},
		{name: "not an array", body: `{"a":1}`, wantStatus: http.StatusBadRequest},
		{name: "create without body", body: `[{"operationType":"Create"}]`, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			resp := s.do(http.MethodPost, "/dbs/d/colls/c/docs", map[string]string{
				"X-Ms-Cosmos-Is-Batch-Request": "True",
				"X-Ms-Cosmos-Batch-Atomic":     "True",
				"X-Ms-Documentdb-Partitionkey": `["a"]`,
			}, tt.body)
			assert.Equal(t, tt.wantStatus, resp.status, string(resp.body))
		})
	}
}

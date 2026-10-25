package cosmosdb_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

const (
	batchBoundary     = "batch_1"
	changesetBoundary = "changeset_1"
)

// batchOp is one embedded request of a changeset.
type batchOp struct {
	method  string
	path    string
	body    string
	headers []string
}

func buildBatch(ops ...batchOp) string {
	var sb strings.Builder

	sb.WriteString(
		"--" + batchBoundary + "\r\nContent-Type: multipart/mixed; boundary=" + changesetBoundary + "\r\n\r\n",
	)

	for i, op := range ops {
		sb.WriteString(
			"--" + changesetBoundary + "\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n",
		)
		fmt.Fprintf(&sb, "Content-ID: %d\r\n\r\n", i+1)
		sb.WriteString(op.method + " http://localhost/" + op.path + " HTTP/1.1\r\n")
		sb.WriteString("Content-Type: application/json\r\n")

		for _, h := range op.headers {
			sb.WriteString(h + "\r\n")
		}

		sb.WriteString("\r\n" + op.body + "\r\n")
	}

	sb.WriteString("--" + changesetBoundary + "--\r\n--" + batchBoundary + "--\r\n")

	return sb.String()
}

func postBatch(t *testing.T, h *cosmosdb.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	return doRequest(t, h, http.MethodPost, "/$batch",
		map[string]string{"Content-Type": "multipart/mixed; boundary=" + batchBoundary}, []byte(body))
}

func newBatchTable(t *testing.T) *cosmosdb.Handler {
	t.Helper()

	h := newTestHandler(t)
	require.Equal(
		t,
		http.StatusCreated,
		doRequest(t, h, http.MethodPost, "/Tables", nil, []byte(`{"TableName":"t"}`)).Code,
	)

	return h
}

func entityKey(rk string) string { return fmt.Sprintf("t(PartitionKey='p',RowKey='%s')", rk) }

func getEntityStatus(t *testing.T, h *cosmosdb.Handler, rk string) int {
	t.Helper()

	return doRequest(t, h, http.MethodGet, "/"+entityKey(rk), nil, nil).Code
}

var batchPartStatus = regexp.MustCompile(`HTTP/1\.1 (\d{3}) `)

func partStatuses(body string) []string {
	matches := batchPartStatus.FindAllStringSubmatch(body, -1)
	out := make([]string, 0, len(matches))

	for _, m := range matches {
		out = append(out, m[1])
	}

	return out
}

func TestTableBatch_CommitsAllOperations(t *testing.T) {
	t.Parallel()

	h := newBatchTable(t)
	require.Equal(
		t,
		http.StatusCreated,
		doRequest(t, h, http.MethodPost, "/t", nil, []byte(`{"PartitionKey":"p","RowKey":"upd","V":"old"}`)).Code,
	)
	require.Equal(
		t,
		http.StatusCreated,
		doRequest(t, h, http.MethodPost, "/t", nil, []byte(`{"PartitionKey":"p","RowKey":"del","V":"old"}`)).Code,
	)

	rec := postBatch(t, h, buildBatch(
		batchOp{method: "POST", path: "t", body: `{"PartitionKey":"p","RowKey":"ins","V":"new"}`},
		batchOp{
			method:  "PUT",
			path:    entityKey("upd"),
			headers: []string{"If-Match: *"},
			body:    `{"PartitionKey":"p","RowKey":"upd","V":"replaced"}`,
		},
		batchOp{
			method: "MERGE",
			path:   entityKey("upsert"),
			body:   `{"PartitionKey":"p","RowKey":"upsert","V":"merged"}`,
		},
		batchOp{method: "DELETE", path: entityKey("del"), headers: []string{"If-Match: *"}},
	))

	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.True(t, strings.HasPrefix(rec.Header().Get("Content-Type"), "multipart/mixed; boundary=batchresponse_"))
	assert.Equal(t, []string{"201", "204", "204", "204"}, partStatuses(rec.Body.String()))
	assert.Contains(t, rec.Body.String(), "Content-ID: 1")
	assert.Contains(t, rec.Body.String(), "Content-ID: 4")

	assert.Equal(t, http.StatusOK, getEntityStatus(t, h, "ins"))
	assert.Equal(t, http.StatusOK, getEntityStatus(t, h, "upsert"))
	assert.Equal(t, http.StatusNotFound, getEntityStatus(t, h, "del"))

	got := decodeBody(t, doRequest(t, h, http.MethodGet, "/"+entityKey("upd"), nil, nil))
	assert.Equal(t, "replaced", got["V"])
}

func TestTableBatch_FailureRollsBackAndReportsIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wantCode   string
		wantIndex  string
		ops        []batchOp
	}{
		{
			name: "insert conflict on third operation",
			ops: []batchOp{
				{method: "POST", path: "t", body: `{"PartitionKey":"p","RowKey":"a"}`},
				{method: "POST", path: "t", body: `{"PartitionKey":"p","RowKey":"b"}`},
				{method: "POST", path: "t", body: `{"PartitionKey":"p","RowKey":"existing"}`},
			},
			wantStatus: "409", wantCode: "EntityAlreadyExists", wantIndex: "2:",
		},
		{
			name: "etag mismatch on second operation",
			ops: []batchOp{
				{method: "POST", path: "t", body: `{"PartitionKey":"p","RowKey":"a"}`},
				{
					method: "PUT", path: entityKey("existing"), headers: []string{`If-Match: W/"datetime'nope'"`},
					body: `{"PartitionKey":"p","RowKey":"existing"}`,
				},
			},
			wantStatus: "412", wantCode: "UpdateConditionNotSatisfied", wantIndex: "1:",
		},
		{
			name: "delete of a missing entity",
			ops: []batchOp{
				{method: "POST", path: "t", body: `{"PartitionKey":"p","RowKey":"a"}`},
				{method: "DELETE", path: entityKey("ghost"), headers: []string{"If-Match: *"}},
			},
			wantStatus: "404", wantCode: "ResourceNotFound", wantIndex: "1:",
		},
		{
			name: "different partitions",
			ops: []batchOp{
				{method: "POST", path: "t", body: `{"PartitionKey":"p","RowKey":"a"}`},
				{method: "POST", path: "t", body: `{"PartitionKey":"q","RowKey":"b"}`},
			},
			wantStatus: "400", wantCode: "CommandsInBatchActOnDifferentPartitions", wantIndex: "1:",
		},
		{
			name: "same entity twice",
			ops: []batchOp{
				{method: "POST", path: "t", body: `{"PartitionKey":"p","RowKey":"a"}`},
				{method: "PUT", path: entityKey("a"), body: `{"PartitionKey":"p","RowKey":"a"}`},
			},
			wantStatus: "400", wantCode: "InvalidDuplicateRow", wantIndex: "1:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatchTable(t)
			require.Equal(t, http.StatusCreated,
				doRequest(t, h, http.MethodPost, "/t", nil, []byte(`{"PartitionKey":"p","RowKey":"existing"}`)).Code)

			rec := postBatch(t, h, buildBatch(tt.ops...))
			require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

			body := rec.Body.String()
			assert.Equal(t, []string{tt.wantStatus}, partStatuses(body))
			assert.Contains(t, body, `"code":"`+tt.wantCode+`"`)
			assert.Contains(t, body, `"value":"`+tt.wantIndex)

			assert.Equal(t, http.StatusNotFound, getEntityStatus(t, h, "a"), "no operation may commit")
			assert.Equal(t, http.StatusNotFound, getEntityStatus(t, h, "b"))
		})
	}
}

func TestTableBatch_RejectsBadRequests(t *testing.T) {
	t.Parallel()

	tooMany := make([]batchOp, 0, 101)
	for i := range 101 {
		tooMany = append(
			tooMany,
			batchOp{method: "POST", path: "t", body: fmt.Sprintf(`{"PartitionKey":"p","RowKey":"r%d"}`, i)},
		)
	}

	tests := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
	}{
		{name: "not multipart", contentType: "application/json", body: `{}`, wantStatus: http.StatusBadRequest},
		{
			name:        "empty body",
			contentType: "multipart/mixed; boundary=" + batchBoundary,
			body:        "",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "too many operations",
			contentType: "multipart/mixed; boundary=" + batchBoundary,
			body:        buildBatch(tooMany...),
			wantStatus:  http.StatusBadRequest,
		},
		{
			name: "delete without if-match", contentType: "multipart/mixed; boundary=" + batchBoundary,
			body:       buildBatch(batchOp{method: "DELETE", path: entityKey("a")}),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatchTable(t)
			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/$batch",
				map[string]string{"Content-Type": tt.contentType},
				[]byte(tt.body),
			)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestTableBatch_MissingTableReports404(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := postBatch(t, h, buildBatch(batchOp{method: "POST", path: "nope", body: `{"PartitionKey":"p","RowKey":"a"}`}))
	require.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, []string{"404"}, partStatuses(rec.Body.String()))
	assert.Contains(t, rec.Body.String(), `"code":"TableNotFound"`)
}

func TestTableBatch_AztablesSubmitTransaction(t *testing.T) {
	t.Parallel()

	h := cosmosdb.NewHandler(cosmosdb.NewInMemoryBackend())
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	svc, err := aztables.NewServiceClientWithNoCredential(fmt.Sprintf("http://127.0.0.1:%d", h.Port),
		&aztables.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}})
	require.NoError(t, err)

	_, err = svc.CreateTable(t.Context(), "sdk", nil)
	require.NoError(t, err)

	client := svc.NewClient("sdk")

	entity := func(rk, v string) []byte {
		return []byte(fmt.Sprintf(`{"PartitionKey":"p","RowKey":%q,"V":%q}`, rk, v))
	}

	_, err = client.SubmitTransaction(t.Context(), []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeAdd, Entity: entity("a", "1")},
		{ActionType: aztables.TransactionTypeAdd, Entity: entity("b", "1")},
		{ActionType: aztables.TransactionTypeInsertReplace, Entity: entity("c", "1")},
	}, nil)
	require.NoError(t, err)

	got, err := client.GetEntity(t.Context(), "p", "b", nil)
	require.NoError(t, err)
	assert.Contains(t, string(got.Value), `"V":"1"`)

	_, err = client.SubmitTransaction(t.Context(), []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeAdd, Entity: entity("d", "1")},
		{ActionType: aztables.TransactionTypeAdd, Entity: entity("a", "dup")},
	}, nil)
	require.Error(t, err)

	_, err = client.GetEntity(t.Context(), "p", "d", nil)
	require.Error(t, err, "a failed transaction must not commit its earlier operations")

	_, err = client.SubmitTransaction(t.Context(), []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeDelete, Entity: entity("a", "")},
		{ActionType: aztables.TransactionTypeUpdateMerge, Entity: entity("b", "2")},
	}, nil)
	require.NoError(t, err)

	_, err = client.GetEntity(t.Context(), "p", "a", nil)
	require.Error(t, err)
}

func buildRetrieveBatch(path string, extra bool) string {
	var sb strings.Builder

	sb.WriteString("--" + batchBoundary + "\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\r\n")
	sb.WriteString("GET http://localhost/" + path + " HTTP/1.1\r\nAccept: application/json;odata=minimalmetadata\r\n\r\n")

	if extra {
		sb.WriteString("--" + batchBoundary + "\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\r\n")
		sb.WriteString("GET http://localhost/" + path + " HTTP/1.1\r\n\r\n")
	}

	sb.WriteString("--" + batchBoundary + "--\r\n")

	return sb.String()
}

func TestTableBatch_SingleRetrieve(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       func() string
		wantStatus int
		wantParts  []string
		wantBody   string
	}{
		{
			name:       "hit",
			body:       func() string { return buildRetrieveBatch(entityKey("a"), false) },
			wantStatus: http.StatusAccepted, wantParts: []string{"200"}, wantBody: `"V":"x"`,
		},
		{
			name:       "miss",
			body:       func() string { return buildRetrieveBatch(entityKey("zz"), false) },
			wantStatus: http.StatusAccepted, wantParts: []string{"404"}, wantBody: `"code":"ResourceNotFound"`,
		},
		{
			name:       "second operation outside a changeset",
			body:       func() string { return buildRetrieveBatch(entityKey("a"), true) },
			wantStatus: http.StatusBadRequest, wantBody: "only operation",
		},
		{
			name: "inside a changeset",
			body: func() string {
				return buildBatch(batchOp{method: "GET", path: entityKey("a")})
			},
			wantStatus: http.StatusBadRequest, wantBody: "only operation",
		},
		{
			name: "payload over 4 MiB",
			body: func() string {
				return buildBatch(batchOp{
					method: "POST", path: "t",
					body: `{"PartitionKey":"p","RowKey":"big","V":"` + strings.Repeat("a", 4<<20) + `"}`,
				})
			},
			wantStatus: http.StatusBadRequest, wantBody: "4 MiB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatchTable(t)
			require.Equal(t, http.StatusCreated,
				doRequest(t, h, http.MethodPost, "/t", nil, []byte(`{"PartitionKey":"p","RowKey":"a","V":"x"}`)).Code)

			rec := postBatch(t, h, tt.body())
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			if tt.wantParts != nil {
				assert.Equal(t, tt.wantParts, partStatuses(rec.Body.String()))
				assert.NotContains(t, rec.Body.String(), "changesetresponse_")
			}
		})
	}
}

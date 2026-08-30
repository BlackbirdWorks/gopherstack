package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestHandler_CancelTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		transactionID string
		setupFn       func(b *lakeformation.InMemoryBackend)
		body          string
		wantStatus    int
	}{
		{
			name:          "cancel_new_transaction",
			transactionID: "txn-0001",
			body:          `{"TransactionId":"txn-0001"}`,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "cancel_already_committed",
			transactionID: "txn-0002",
			setupFn: func(b *lakeformation.InMemoryBackend) {
				_, err := b.CommitTransaction("txn-0002")
				require.NoError(t, err)
			},
			body:       `{"TransactionId":"txn-0002"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_transaction_id",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doLFRequest(t, h, "/CancelTransaction", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CommitTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn     func(b *lakeformation.InMemoryBackend)
		name        string
		body        string
		wantStatus2 string
		wantStatus  int
	}{
		{
			name:        "commit_new_transaction",
			body:        `{"TransactionId":"txn-1001"}`,
			wantStatus:  http.StatusOK,
			wantStatus2: "COMMITTED",
		},
		{
			name: "commit_already_cancelled",
			setupFn: func(b *lakeformation.InMemoryBackend) {
				require.NoError(t, b.CancelTransaction("txn-1002"))
			},
			body:       `{"TransactionId":"txn-1002"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_transaction_id",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doLFRequest(t, h, "/CommitTransaction", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantStatus2 != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantStatus2, resp["TransactionStatus"])
			}
		})
	}
}

func TestHandler_TransactionLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("cancel_then_commit_fails", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler()

		rec := doLFRequest(t, h, "/CancelTransaction", `{"TransactionId":"txn-lifecycle-1"}`)
		assert.Equal(t, http.StatusOK, rec.Code)

		rec = doLFRequest(t, h, "/CommitTransaction", `{"TransactionId":"txn-lifecycle-1"}`)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("commit_then_cancel_fails", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler()

		rec := doLFRequest(t, h, "/CommitTransaction", `{"TransactionId":"txn-lifecycle-2"}`)
		assert.Equal(t, http.StatusOK, rec.Code)

		rec = doLFRequest(t, h, "/CancelTransaction", `{"TransactionId":"txn-lifecycle-2"}`)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestCancelTransaction_AlreadyCommitted(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Commit first.
	rec := postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "tx-1"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, b.TransactionCount())

	// Cancel should fail.
	rec2 := postJSON(t, h, "/CancelTransaction", map[string]any{"TransactionId": "tx-1"})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestCommitTransaction_AlreadyCancelled(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Cancel first.
	rec := postJSON(t, h, "/CancelTransaction", map[string]any{"TransactionId": "tx-2"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Commit should fail.
	rec2 := postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "tx-2"})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestCommitTransaction_ReturnsStatus(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "tx-3"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "COMMITTED", out["TransactionStatus"])
}

func TestStartTransaction_ReturnsID(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartTransaction", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotEmpty(t, out["TransactionId"])
	assert.Equal(t, 1, b.TransactionCount())
}

func TestStartTransaction_MultipleUnique(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec1 := postJSON(t, h, "/StartTransaction", nil)
	rec2 := postJSON(t, h, "/StartTransaction", nil)

	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out1, out2 map[string]any
	require.NoError(t, jsonDecode(rec1.Body, &out1))
	require.NoError(t, jsonDecode(rec2.Body, &out2))
	assert.NotEqual(t, out1["TransactionId"], out2["TransactionId"])
	assert.Equal(t, 2, b.TransactionCount())
}

func TestDescribeTransaction_Active(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartTransaction", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txID := out["TransactionId"].(string)

	rec2 := postJSON(t, h, "/DescribeTransaction", map[string]any{"TransactionId": txID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var desc map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &desc))
	assert.NotNil(t, desc["TransactionDescription"])
}

func TestDescribeTransaction_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DescribeTransaction", map[string]any{"TransactionId": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDescribeTransaction_MissingID(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DescribeTransaction", map[string]any{"TransactionId": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListTransactions(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/StartTransaction", nil)
	postJSON(t, h, "/StartTransaction", nil)

	rec := postJSON(t, h, "/ListTransactions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 2)
}

// --- RemoveLFTagsFromResource tests ---

func TestListTransactions_StatusFilter_Active(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Start two transactions
	rec1 := postJSON(t, h, "/StartTransaction", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, jsonDecode(rec1.Body, &out1))
	txID1 := out1["TransactionId"].(string)

	// Commit one of them
	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": txID1})
	postJSON(t, h, "/StartTransaction", nil) // second stays ACTIVE

	// Filter for only ACTIVE
	rec := postJSON(t, h, "/ListTransactions", map[string]any{"StatusFilter": "ACTIVE"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 1, "only ACTIVE transaction should be returned")
	assert.Equal(t, "ACTIVE", txns[0].(map[string]any)["TransactionStatus"])
}

func TestListTransactions_StatusFilter_Committed(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "txn-commit-1"})
	require.Equal(t, http.StatusOK, rec.Code)

	postJSON(t, h, "/StartTransaction", nil) // active transaction

	rec2 := postJSON(t, h, "/ListTransactions", map[string]any{"StatusFilter": "COMMITTED"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 1)
}

func TestListTransactions_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/StartTransaction", nil)
	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "committed-1"})

	rec := postJSON(t, h, "/ListTransactions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 2)
}

// --- DeleteObjectsOnCancel state check ---

func TestDeleteObjectsOnCancel_RequiresAborted(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartTransaction", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var txOut map[string]any
	require.NoError(t, jsonDecode(rec.Body, &txOut))
	txID := txOut["TransactionId"].(string)

	// Transaction is ACTIVE — should fail
	rec2 := postJSON(t, h, "/DeleteObjectsOnCancel", map[string]any{
		"TransactionId": txID,
		"DatabaseName":  "db",
		"TableName":     "t",
		"Objects":       []map[string]any{{"Uri": "s3://bucket/key"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code, "must be ABORTED before DeleteObjectsOnCancel")
}

func TestDeleteObjectsOnCancel_AfterCancel(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CancelTransaction", map[string]any{"TransactionId": "txn-aborted"})

	rec := postJSON(t, h, "/DeleteObjectsOnCancel", map[string]any{
		"TransactionId": "txn-aborted",
		"DatabaseName":  "db",
		"TableName":     "t",
		"Objects":       []map[string]any{{"Uri": "s3://bucket/key"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code, "should succeed when transaction is ABORTED")
}

func TestDeleteObjectsOnCancel_MissingID(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteObjectsOnCancel", map[string]any{"TransactionId": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// DatabaseName, TableName, and Objects are all `required` on the real
// DeleteObjectsOnCancelInput (api_op_DeleteObjectsOnCancel.go, lakeformation@v1.50.4) but
// were accepted and silently dropped -- gopherstack-4shm class bug, never validated, never
// forwarded to the backend.
func TestDeleteObjectsOnCancel_RequiresDatabaseTableObjects(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CancelTransaction", map[string]any{"TransactionId": "txn-missing-fields"})

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing_database_name",
			body: map[string]any{
				"TransactionId": "txn-missing-fields",
				"TableName":     "t",
				"Objects":       []map[string]any{{"Uri": "s3://bucket/key"}},
			},
		},
		{
			name: "missing_table_name",
			body: map[string]any{
				"TransactionId": "txn-missing-fields",
				"DatabaseName":  "db",
				"Objects":       []map[string]any{{"Uri": "s3://bucket/key"}},
			},
		},
		{
			name: "missing_objects",
			body: map[string]any{
				"TransactionId": "txn-missing-fields",
				"DatabaseName":  "db",
				"TableName":     "t",
			},
		},
		{
			name: "empty_objects",
			body: map[string]any{
				"TransactionId": "txn-missing-fields",
				"DatabaseName":  "db",
				"TableName":     "t",
				"Objects":       []map[string]any{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postJSON(t, h, "/DeleteObjectsOnCancel", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, "expected rejection: %s", rec.Body.String())
		})
	}
}

func TestDeleteObjectsOnCancel_AllRequiredFieldsPresent(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CancelTransaction", map[string]any{"TransactionId": "txn-full"})

	rec := postJSON(t, h, "/DeleteObjectsOnCancel", map[string]any{
		"TransactionId": "txn-full",
		"DatabaseName":  "db",
		"TableName":     "t",
		"Objects":       []map[string]any{{"Uri": "s3://bucket/key"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code, "should succeed with all required fields: %s", rec.Body.String())
}

// --- UpdateDataCellsFilter validation ---

func TestExtendTransaction_ActiveSucceeds(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	startRec := postJSON(t, h, "/StartTransaction", nil)
	require.Equal(t, http.StatusOK, startRec.Code)

	var txOut map[string]any
	require.NoError(t, jsonDecode(startRec.Body, &txOut))
	txID := txOut["TransactionId"].(string)

	rec := postJSON(t, h, "/ExtendTransaction", map[string]any{"TransactionId": txID})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestExtendTransaction_CommittedFails(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "committed-tx"})

	rec := postJSON(t, h, "/ExtendTransaction", map[string]any{"TransactionId": "committed-tx"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- GetTableObjects ---

func TestStartTransaction_ReadOnly_RejectsWrites(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartTransaction", map[string]any{"TransactionType": "READ_ONLY"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txID := out["TransactionId"].(string)

	// Write to READ_ONLY transaction must fail
	rec2 := postJSON(t, h, "/UpdateTableObjects", map[string]any{
		"TransactionId": txID,
		"DatabaseName":  "db1",
		"TableName":     "t1",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestStartTransaction_ReadAndWrite_AllowsWrites(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartTransaction", map[string]any{"TransactionType": "READ_AND_WRITE"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txID := out["TransactionId"].(string)

	rec2 := postJSON(t, h, "/UpdateTableObjects", map[string]any{
		"TransactionId": txID,
		"DatabaseName":  "db1",
		"TableName":     "t1",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// --- #11: Transaction timestamps ---

func TestDescribeTransaction_HasTimestamps(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec1 := postJSON(t, h, "/StartTransaction", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var startOut map[string]any
	require.NoError(t, jsonDecode(rec1.Body, &startOut))
	txID := startOut["TransactionId"].(string)

	rec2 := postJSON(t, h, "/DescribeTransaction", map[string]any{"TransactionId": txID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var descOut map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &descOut))

	desc := descOut["TransactionDescription"].(map[string]any)
	assert.NotEmpty(t, desc["TransactionStartTime"], "TransactionStartTime should be set")
}

func TestCommitTransaction_SetsEndTime(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec1 := postJSON(t, h, "/StartTransaction", nil)
	var startOut map[string]any
	require.NoError(t, jsonDecode(rec1.Body, &startOut))
	txID := startOut["TransactionId"].(string)

	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": txID})

	rec3 := postJSON(t, h, "/DescribeTransaction", map[string]any{"TransactionId": txID})
	var descOut map[string]any
	require.NoError(t, jsonDecode(rec3.Body, &descOut))

	desc := descOut["TransactionDescription"].(map[string]any)
	assert.NotEmpty(t, desc["TransactionEndTime"], "TransactionEndTime should be set after commit")
}

// --- #13: ListTransactions StatusFilter enum ---

func TestListTransactions_COMPLETED_Filter(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// committed transaction
	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "tx-commit"})
	// aborted transaction
	postJSON(t, h, "/CancelTransaction", map[string]any{"TransactionId": "tx-abort"})
	// active transaction
	postJSON(t, h, "/StartTransaction", nil)

	rec := postJSON(t, h, "/ListTransactions", map[string]any{"StatusFilter": "COMPLETED"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 2, "COMPLETED should return both COMMITTED and ABORTED")

	for _, tx := range txns {
		status := tx.(map[string]any)["TransactionStatus"].(string)
		assert.True(t, status == "COMMITTED" || status == "ABORTED", "unexpected status: %s", status)
	}
}

func TestListTransactions_ALL_Filter(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "tx1"})
	postJSON(t, h, "/StartTransaction", nil)

	rec := postJSON(t, h, "/ListTransactions", map[string]any{"StatusFilter": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 2, "ALL should return every transaction")
}

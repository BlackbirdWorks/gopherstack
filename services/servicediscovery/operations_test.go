package servicediscovery_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GetOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
		createNS   bool
	}{
		{
			name:       "success",
			createNS:   true,
			wantStatus: http.StatusOK,
			wantBody:   "Operation",
		},
		{
			name:       "not_found",
			body:       map[string]any{"OperationId": "op-does-not-exist"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "OperationNotFound",
		},
		{
			name:       "missing_operation_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			switch {
			case tt.createNS:
				createRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "op-ns"})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				opID := createResp["OperationId"].(string)

				rec = doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
			case tt.bodyRaw != nil:
				rec = doSDRawRequest(t, h, "GetOperation", tt.bodyRaw)
			default:
				rec = doSDRequest(t, h, "GetOperation", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ListOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ops-ns"})

	rec := doSDRequest(t, h, "ListOperations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Operations")
}

// TestOperationCountIncrementsOnRegisterDeregister verifies that
// register and deregister both create new operations.
func TestOperationCountIncrementsOnRegisterDeregister(t *testing.T) {
	t.Parallel()

	b, h := newBackendAndHandler(t)

	nsID := createNamespaceHelper(t, h, "ns-opcount")
	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-opcount", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	opsBefore := servicediscovery.OperationCount(b)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "op-inst", "Attributes": map[string]string{},
	})
	assert.Equal(t, opsBefore+1, servicediscovery.OperationCount(b))

	doSDRequest(t, h, "DeregisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "op-inst",
	})
	assert.Equal(t, opsBefore+2, servicediscovery.OperationCount(b))
}

// TestListOperations_Pagination verifies pagination on ListOperations.
func TestListOperations_Pagination(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")
	servicediscovery.SetDeterministicIDs(b)
	h := servicediscovery.NewHandler(b)

	for i := range 4 {
		doSDRequest(t, h, "CreateHttpNamespace", map[string]any{
			"Name": fmt.Sprintf("ns-ops-%02d", i),
		})
	}

	tests := []struct {
		req           map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			req:           map[string]any{},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			req:           map[string]any{"MaxResults": 2},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, "ListOperations", tt.req)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			ops := out["Operations"].([]any)
			assert.Len(t, ops, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestListOperations_SummaryShape verifies that ListOperations returns the
// lightweight OperationSummary shape (Id + Status only) -- NOT the full
// Operation shape GetOperation returns. Real Cloud Map's ListOperationsOutput
// is "Operations []types.OperationSummary", which has only Id and Status
// (api_op_ListOperations.go); Type/CreateDate/UpdateDate/Targets/ErrorCode/
// ErrorMessage are GetOperation-only fields.
func TestListOperations_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-summary-shape"})

	rec := doSDRequest(t, h, "ListOperations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	ops := out["Operations"].([]any)
	require.Len(t, ops, 1)

	op := ops[0].(map[string]any)
	assert.Equal(t, []string{"Id", "Status"}, sortedKeys(op))
}

// sortedKeys returns the sorted keys of a map[string]any, for exact-shape assertions.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// TestListOperations_NamespaceAndServiceIDFilters verifies the NAMESPACE_ID
// and SERVICE_ID filters on ListOperations, which match against an
// operation's Targets map.
func TestListOperations_NamespaceAndServiceIDFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsID := createNamespaceHelper(t, h, "ns-op-filter")

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-op-filter", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "op-filter-inst", "Attributes": map[string]string{},
	})

	nsFilterRec := doSDRequest(t, h, "ListOperations", map[string]any{
		"Filters": []map[string]any{{"Name": "NAMESPACE_ID", "Values": []string{nsID}}},
	})
	require.Equal(t, http.StatusOK, nsFilterRec.Code)

	var nsOut map[string]any
	require.NoError(t, json.Unmarshal(nsFilterRec.Body.Bytes(), &nsOut))
	assert.Len(t, nsOut["Operations"].([]any), 1, "only CREATE_NAMESPACE targets this namespace")

	svcFilterRec := doSDRequest(t, h, "ListOperations", map[string]any{
		"Filters": []map[string]any{{"Name": "SERVICE_ID", "Values": []string{svcID}}},
	})
	require.Equal(t, http.StatusOK, svcFilterRec.Code)

	var svcOut map[string]any
	require.NoError(t, json.Unmarshal(svcFilterRec.Body.Bytes(), &svcOut))
	assert.Len(t, svcOut["Operations"].([]any), 1, "only REGISTER_INSTANCE targets this service")
}

// TestListOperations_TypeFilterIN verifies the TYPE filter's documented IN
// condition (multiple type values, any match returned).
func TestListOperations_TypeFilterIN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsID := createNamespaceHelper(t, h, "ns-type-filter")
	doSDRequest(t, h, "DeleteNamespace", map[string]any{"Id": nsID})

	rec := doSDRequest(t, h, "ListOperations", map[string]any{
		"Filters": []map[string]any{
			{"Name": "TYPE", "Values": []string{"CREATE_NAMESPACE", "DELETE_NAMESPACE"}, "Condition": "IN"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out["Operations"].([]any), 2)
}

// TestListOperations_UpdateDateBetweenFilter verifies the UPDATE_DATE filter's
// documented BETWEEN condition: a [start, end] epoch-seconds range that must
// bracket UpdateDate.
func TestListOperations_UpdateDateBetweenFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	before := time.Now().Add(-time.Hour).Unix()
	doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-date-filter"})
	after := time.Now().Add(time.Hour).Unix()

	inRangeRec := doSDRequest(t, h, "ListOperations", map[string]any{
		"Filters": []map[string]any{
			{
				"Name":      "UPDATE_DATE",
				"Condition": "BETWEEN",
				"Values":    []string{strconv.FormatInt(before, 10), strconv.FormatInt(after, 10)},
			},
		},
	})
	require.Equal(t, http.StatusOK, inRangeRec.Code)

	var inRangeOut map[string]any
	require.NoError(t, json.Unmarshal(inRangeRec.Body.Bytes(), &inRangeOut))
	assert.Len(t, inRangeOut["Operations"].([]any), 1)

	outOfRangeRec := doSDRequest(t, h, "ListOperations", map[string]any{
		"Filters": []map[string]any{
			{
				"Name":      "UPDATE_DATE",
				"Condition": "BETWEEN",
				"Values": []string{
					strconv.FormatInt(after, 10),
					strconv.FormatInt(after+3600, 10),
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, outOfRangeRec.Code)

	var outOfRangeOut map[string]any
	require.NoError(t, json.Unmarshal(outOfRangeRec.Body.Bytes(), &outOfRangeOut))
	assert.Empty(t, outOfRangeOut["Operations"].([]any))
}

// TestListOperations_StatusFilter verifies the STATUS filter on ListOperations.
func TestListOperations_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-status"})

	tests := []struct {
		filterStatus string
		name         string
		wantLen      int
	}{
		{name: "filter_SUCCESS_returns_one", filterStatus: "SUCCESS", wantLen: 1},
		{name: "filter_PENDING_returns_none", filterStatus: "PENDING", wantLen: 0},
		{name: "no_filter_returns_all", filterStatus: "", wantLen: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := map[string]any{}
			if tt.filterStatus != "" {
				req["Filters"] = []map[string]any{
					{"Name": "STATUS", "Values": []string{tt.filterStatus}},
				}
			}

			rec := doSDRequest(t, h, "ListOperations", req)
			require.Equal(t, http.StatusOK, rec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			ops := out["Operations"].([]any)
			assert.Len(t, ops, tt.wantLen, "filterStatus=%s", tt.filterStatus)
		})
	}
}

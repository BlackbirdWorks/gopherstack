package servicediscovery_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

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

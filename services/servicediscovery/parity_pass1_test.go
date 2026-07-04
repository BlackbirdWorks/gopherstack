package servicediscovery_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// TestParity_UpdateService_ReturnsOperationId verifies UpdateService returns an OperationId,
// not a Service body, matching real AWS Cloud Map behavior.
func TestParity_UpdateService_ReturnsOperationId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCode  int
		wantOpID  bool
		wantNoSvc bool
	}{
		{
			name:      "success_returns_operation_id",
			wantCode:  http.StatusOK,
			wantOpID:  true,
			wantNoSvc: true,
		},
		{
			name:     "not_found_returns_error",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.name {
			case "success_returns_operation_id":
				createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-parity"})
				require.Equal(t, http.StatusOK, createRec.Code)
				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
				svcID := created["Service"].(map[string]any)["Id"].(string)

				rec := doSDRequest(t, h, "UpdateService", map[string]any{
					"Id":      svcID,
					"Service": map[string]any{"Description": "updated"},
				})
				require.Equal(t, tt.wantCode, rec.Code)

				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				if tt.wantOpID {
					assert.NotEmpty(t, out["OperationId"], "should contain OperationId")
				}
				if tt.wantNoSvc {
					assert.Nil(t, out["Service"], "should NOT contain Service body")
				}
			case "not_found_returns_error":
				rec := doSDRequest(t, h, "UpdateService", map[string]any{
					"Id":      "nonexistent",
					"Service": map[string]any{"Description": "x"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

// TestParity_Namespace_ServiceCount verifies GetNamespace and ListNamespaces include
// an accurate ServiceCount field (number of services in the namespace).
func TestParity_Namespace_ServiceCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		servicesBefore int
		wantCount      int
	}{
		{name: "empty_namespace_count_zero", servicesBefore: 0, wantCount: 0},
		{name: "one_service_count_one", servicesBefore: 1, wantCount: 1},
		{name: "three_services_count_three", servicesBefore: 3, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createNsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-parity"})
			require.Equal(t, http.StatusOK, createNsRec.Code)
			var nsOp map[string]string
			require.NoError(t, json.Unmarshal(createNsRec.Body.Bytes(), &nsOp))
			opID := nsOp["OperationId"]

			opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
			require.Equal(t, http.StatusOK, opRec.Code)
			var opOut map[string]any
			require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opOut))
			nsID := opOut["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

			for i := range tt.servicesBefore {
				rec := doSDRequest(t, h, "CreateService", map[string]any{
					"Name":        fmt.Sprintf("svc-%d", i),
					"NamespaceId": nsID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			// Verify via GetNamespace
			getRec := doSDRequest(t, h, "GetNamespace", map[string]any{"Id": nsID})
			require.Equal(t, http.StatusOK, getRec.Code)
			var getOut map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
			ns := getOut["Namespace"].(map[string]any)
			assert.Equal(t, tt.wantCount, int(ns["ServiceCount"].(float64)), "GetNamespace ServiceCount")

			// Verify via ListNamespaces
			listRec := doSDRequest(t, h, "ListNamespaces", map[string]any{})
			require.Equal(t, http.StatusOK, listRec.Code)
			var listOut map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
			nsList := listOut["Namespaces"].([]any)
			require.Len(t, nsList, 1)
			gotListCount := int(nsList[0].(map[string]any)["ServiceCount"].(float64))
			assert.Equal(t, tt.wantCount, gotListCount, "ListNamespaces ServiceCount")
		})
	}
}

// TestParity_Service_InstanceCount verifies GetService and ListServices include
// an accurate InstanceCount field.
func TestParity_Service_InstanceCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		instancesBefore int
		wantCount       int
	}{
		{name: "empty_service_count_zero", instancesBefore: 0, wantCount: 0},
		{name: "two_instances_count_two", instancesBefore: 2, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createSvcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-count"})
			require.Equal(t, http.StatusOK, createSvcRec.Code)
			var svcOut map[string]any
			require.NoError(t, json.Unmarshal(createSvcRec.Body.Bytes(), &svcOut))
			svcID := svcOut["Service"].(map[string]any)["Id"].(string)

			for i := range tt.instancesBefore {
				rec := doSDRequest(t, h, "RegisterInstance", map[string]any{
					"ServiceId":  svcID,
					"InstanceId": fmt.Sprintf("inst-%d", i),
					"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			// Verify via GetService
			getRec := doSDRequest(t, h, "GetService", map[string]any{"Id": svcID})
			require.Equal(t, http.StatusOK, getRec.Code)
			var getOut map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
			svc := getOut["Service"].(map[string]any)
			assert.Equal(t, tt.wantCount, int(svc["InstanceCount"].(float64)), "GetService InstanceCount")

			// Verify via ListServices
			listRec := doSDRequest(t, h, "ListServices", map[string]any{})
			require.Equal(t, http.StatusOK, listRec.Code)
			var listOut map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
			svcs := listOut["Services"].([]any)
			require.Len(t, svcs, 1)
			gotListCount := int(svcs[0].(map[string]any)["InstanceCount"].(float64))
			assert.Equal(t, tt.wantCount, gotListCount, "ListServices InstanceCount")
		})
	}
}

// TestParity_ListNamespaces_Pagination verifies NextToken/MaxResults pagination on ListNamespaces.
func TestParity_ListNamespaces_Pagination(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")
	servicediscovery.SetDeterministicIDs(b)
	h := servicediscovery.NewHandler(b)

	for i := range 4 {
		rec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{
			"Name": fmt.Sprintf("ns-%02d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
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

			rec := doSDRequest(t, h, "ListNamespaces", tt.req)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			nsList := out["Namespaces"].([]any)
			assert.Len(t, nsList, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListNamespaces_TypeFilter verifies the TYPE filter works on ListNamespaces.
func TestParity_ListNamespaces_TypeFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "http-ns"})
	doSDRequest(t, h, "CreatePrivateDnsNamespace", map[string]any{"Name": "private-ns", "Vpc": "vpc-1"})
	doSDRequest(t, h, "CreatePublicDnsNamespace", map[string]any{"Name": "public-ns"})

	tests := []struct {
		filterType string
		name       string
		wantLen    int
	}{
		{name: "filter_HTTP", filterType: "HTTP", wantLen: 1},
		{name: "filter_DNS_PRIVATE", filterType: "DNS_PRIVATE", wantLen: 1},
		{name: "filter_DNS_PUBLIC", filterType: "DNS_PUBLIC", wantLen: 1},
		{name: "no_filter_all", filterType: "", wantLen: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := map[string]any{}
			if tt.filterType != "" {
				req["Filters"] = []map[string]any{
					{"Name": "TYPE", "Values": []string{tt.filterType}},
				}
			}

			rec := doSDRequest(t, h, "ListNamespaces", req)
			require.Equal(t, http.StatusOK, rec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			nsList := out["Namespaces"].([]any)
			assert.Len(t, nsList, tt.wantLen, "filterType=%s", tt.filterType)
		})
	}
}

// TestParity_ListServices_Pagination verifies NextToken/MaxResults pagination on ListServices.
func TestParity_ListServices_Pagination(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")
	servicediscovery.SetDeterministicIDs(b)
	h := servicediscovery.NewHandler(b)

	for i := range 4 {
		rec := doSDRequest(t, h, "CreateService", map[string]any{
			"Name": fmt.Sprintf("svc-%02d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
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

			rec := doSDRequest(t, h, "ListServices", tt.req)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			svcs := out["Services"].([]any)
			assert.Len(t, svcs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListOperations_Pagination verifies pagination on ListOperations.
func TestParity_ListOperations_Pagination(t *testing.T) {
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

// TestParity_ListOperations_StatusFilter verifies the STATUS filter on ListOperations.
func TestParity_ListOperations_StatusFilter(t *testing.T) {
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

// TestParity_ListInstances_Pagination verifies NextToken/MaxResults pagination on ListInstances.
func TestParity_ListInstances_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createSvcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-inst-page"})
	require.Equal(t, http.StatusOK, createSvcRec.Code)
	var svcOut map[string]any
	require.NoError(t, json.Unmarshal(createSvcRec.Body.Bytes(), &svcOut))
	svcID := svcOut["Service"].(map[string]any)["Id"].(string)

	for i := range 4 {
		rec := doSDRequest(t, h, "RegisterInstance", map[string]any{
			"ServiceId":  svcID,
			"InstanceId": fmt.Sprintf("inst-%02d", i),
			"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		req           map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			req:           map[string]any{"ServiceId": svcID},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			req:           map[string]any{"ServiceId": svcID, "MaxResults": 2},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, "ListInstances", tt.req)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			insts := out["Instances"].([]any)
			assert.Len(t, insts, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_UpdateService_CreatesOperation verifies UpdateService creates a retrievable operation.
func TestParity_UpdateService_CreatesOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-op-check"})
	require.Equal(t, http.StatusOK, createRec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	svcID := created["Service"].(map[string]any)["Id"].(string)

	updateRec := doSDRequest(t, h, "UpdateService", map[string]any{
		"Id":      svcID,
		"Service": map[string]any{"Description": "desc-v2"},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut map[string]string
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateOut))
	opID := updateOut["OperationId"]
	require.NotEmpty(t, opID)

	getOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
	require.Equal(t, http.StatusOK, getOpRec.Code)
	var opOut map[string]any
	require.NoError(t, json.Unmarshal(getOpRec.Body.Bytes(), &opOut))
	op := opOut["Operation"].(map[string]any)
	assert.Equal(t, "UPDATE_SERVICE", op["Type"])
	assert.Equal(t, "SUCCESS", op["Status"])
}

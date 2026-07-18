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

func TestHandler_CreateHTTPNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"Name": "my-namespace"},
			wantStatus: http.StatusOK,
			wantBody:   "OperationId",
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doSDRawRequest(t, h, "CreateHttpNamespace", tt.bodyRaw)
			} else {
				rec = doSDRequest(t, h, "CreateHttpNamespace", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_CreatePrivateDNSNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"Name": "private.local", "Vpc": "vpc-12345"},
			wantStatus: http.StatusOK,
			wantBody:   "OperationId",
		},
		{
			name:       "missing_name",
			body:       map[string]any{"Vpc": "vpc-12345"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doSDRawRequest(t, h, "CreatePrivateDnsNamespace", tt.bodyRaw)
			} else {
				rec = doSDRequest(t, h, "CreatePrivateDnsNamespace", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_CreatePublicDNSNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"Name": "public.example.com"},
			wantStatus: http.StatusOK,
			wantBody:   "OperationId",
		},
		{
			name:       "missing_name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doSDRawRequest(t, h, "CreatePublicDnsNamespace", tt.bodyRaw)
			} else {
				rec = doSDRequest(t, h, "CreatePublicDnsNamespace", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_CreateNamespace_DuplicateName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "my-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "my-ns"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "NamespaceAlreadyExists")
}

func TestHandler_GetNamespace(t *testing.T) {
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
			wantBody:   "Namespace",
		},
		{
			name:       "not_found",
			body:       map[string]any{"Id": "ns-does-not-exist"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "NamespaceNotFound",
		},
		{
			name:       "missing_id",
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
				createRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "my-ns"})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

				opID := createResp["OperationId"].(string)

				opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
				require.Equal(t, http.StatusOK, opRec.Code)

				var opResp map[string]any
				require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))

				operation := opResp["Operation"].(map[string]any)
				targets := operation["Targets"].(map[string]any)
				nsID := targets["NAMESPACE"].(string)

				rec = doSDRequest(t, h, "GetNamespace", map[string]any{"Id": nsID})
			case tt.bodyRaw != nil:
				rec = doSDRawRequest(t, h, "GetNamespace", tt.bodyRaw)
			default:
				rec = doSDRequest(t, h, "GetNamespace", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DeleteNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       any
		bodyRaw    []byte
		wantStatus int
		createNS   bool
	}{
		{
			name:       "success",
			createNS:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       map[string]any{"Id": "ns-does-not-exist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_id",
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
				createRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "delete-ns"})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				opID := createResp["OperationId"].(string)

				opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
				require.Equal(t, http.StatusOK, opRec.Code)

				var opResp map[string]any
				require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
				operation := opResp["Operation"].(map[string]any)
				targets := operation["Targets"].(map[string]any)
				nsID := targets["NAMESPACE"].(string)

				rec = doSDRequest(t, h, "DeleteNamespace", map[string]any{"Id": nsID})
			case tt.bodyRaw != nil:
				rec = doSDRawRequest(t, h, "DeleteNamespace", tt.bodyRaw)
			default:
				rec = doSDRequest(t, h, "DeleteNamespace", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListNamespaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-alpha"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSDRequest(t, h, "ListNamespaces", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Namespaces")
	assert.Contains(t, rec.Body.String(), "ns-alpha")
}

// TestHandler_NamespaceTagsInGetResponse verifies that Tags and CreateDate
// are included in GetNamespace responses.
func TestHandler_NamespaceTagsInGetResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create namespace with tags.
	createRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{
		"Name": "tagged-ns",
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	require.Equal(t, 200, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	opID := createResp["OperationId"].(string)
	opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})

	var opResp map[string]any
	require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
	nsID := opResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	// Get namespace and check tags are present.
	getRec := doSDRequest(t, h, "GetNamespace", map[string]any{"Id": nsID})
	require.Equal(t, 200, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	ns := getResp["Namespace"].(map[string]any)
	assert.NotNil(t, ns["Tags"], "Tags must be present in GetNamespace response")
	assert.NotZero(t, ns["CreateDate"], "CreateDate must be present in GetNamespace response")

	tags := ns["Tags"].([]any)
	assert.Len(t, tags, 2, "expected 2 tags")
	first := tags[0].(map[string]any)
	assert.Equal(t, "env", first["Key"])
	assert.Equal(t, "prod", first["Value"])
}

// TestHandler_ListNamespacesReturnsTags verifies ListNamespaces includes tags.
func TestHandler_ListNamespacesReturnsTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSDRequest(t, h, "CreateHttpNamespace", map[string]any{
		"Name": "ns-list-tags",
		"Tags": []map[string]any{{"Key": "k", "Value": "v"}},
	})

	rec := doSDRequest(t, h, "ListNamespaces", map[string]any{})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	nsList := resp["Namespaces"].([]any)
	require.Len(t, nsList, 1)
	tags := nsList[0].(map[string]any)["Tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "k", tags[0].(map[string]any)["Key"])
}

// TestHandler_CreatePrivateDNSNamespaceNoVpc verifies that CreatePrivateDnsNamespace
// succeeds without Vpc (implementation is lenient - AWS validates Vpc server-side).
func TestHandler_CreatePrivateDNSNamespaceNoVpc(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "CreatePrivateDnsNamespace", map[string]any{
		"Name": "private-ns",
	})
	// Implementation accepts missing Vpc (lenient mock); returns 200.
	assert.Equal(t, 200, rec.Code)
}

// TestNamespace_ServiceCount verifies GetNamespace and ListNamespaces include
// an accurate ServiceCount field (number of services in the namespace).
func TestNamespace_ServiceCount(t *testing.T) {
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

// TestListNamespaces_Pagination verifies NextToken/MaxResults pagination on ListNamespaces.
func TestListNamespaces_Pagination(t *testing.T) {
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

// TestListNamespaces_TypeFilter verifies the TYPE filter works on ListNamespaces.
func TestListNamespaces_TypeFilter(t *testing.T) {
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

// TestHandler_UpdateHttpNamespace tests UpdateHttpNamespace.
func TestHandler_UpdateHttpNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantKey  string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK, wantKey: "OperationId"},
		{name: "missing_id", wantCode: http.StatusBadRequest},
		{name: "not_found", wantCode: http.StatusBadRequest},
		{name: "wrong_type", wantCode: http.StatusBadRequest},
		{name: "invalid_json", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.name {
			case "success":
				createRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "http-ns"})
				require.Equal(t, http.StatusOK, createRec.Code)
				var nsOut map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &nsOut))
				opID := nsOut["OperationId"].(string)
				opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
				var opOut map[string]any
				require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opOut))
				nsID := opOut["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

				rec := doSDRequest(t, h, "UpdateHttpNamespace", map[string]any{
					"Id":        nsID,
					"Namespace": map[string]any{"Description": "updated description"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)
				assert.Contains(t, rec.Body.String(), tt.wantKey)

			case "missing_id":
				rec := doSDRequest(t, h, "UpdateHttpNamespace", map[string]any{
					"Namespace": map[string]any{"Description": "desc"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "not_found":
				rec := doSDRequest(t, h, "UpdateHttpNamespace", map[string]any{
					"Id":        "does-not-exist",
					"Namespace": map[string]any{"Description": "desc"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "wrong_type":
				// Create a private DNS namespace and try to update it as HTTP namespace.
				createRec := doSDRequest(t, h, "CreatePrivateDnsNamespace", map[string]any{"Name": "private-ns"})
				require.Equal(t, http.StatusOK, createRec.Code)
				var nsOut map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &nsOut))
				opID := nsOut["OperationId"].(string)
				opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
				var opOut map[string]any
				require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opOut))
				privateNsID := opOut["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

				rec := doSDRequest(t, h, "UpdateHttpNamespace", map[string]any{
					"Id":        privateNsID,
					"Namespace": map[string]any{"Description": "desc"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "invalid_json":
				rec := doSDRawRequest(t, h, "UpdateHttpNamespace", []byte("{bad json"))
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

// TestHandler_UpdatePrivateDnsNamespace tests UpdatePrivateDnsNamespace.
func TestHandler_UpdatePrivateDnsNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantKey  string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK, wantKey: "OperationId"},
		{name: "missing_id", wantCode: http.StatusBadRequest},
		{name: "not_found", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.name {
			case "success":
				createRec := doSDRequest(t, h, "CreatePrivateDnsNamespace", map[string]any{"Name": "private-ns"})
				var out map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
				opID := out["OperationId"].(string)
				opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
				var opOut map[string]any
				require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opOut))
				nsID := opOut["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)
				rec := doSDRequest(t, h, "UpdatePrivateDnsNamespace", map[string]any{
					"Id":        nsID,
					"Namespace": map[string]any{"Description": "updated"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)
				assert.Contains(t, rec.Body.String(), tt.wantKey)

			case "missing_id":
				rec := doSDRequest(t, h, "UpdatePrivateDnsNamespace", map[string]any{
					"Namespace": map[string]any{"Description": "updated"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "not_found":
				rec := doSDRequest(t, h, "UpdatePrivateDnsNamespace", map[string]any{
					"Id":        "no-such-ns",
					"Namespace": map[string]any{"Description": "updated"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

// TestHandler_UpdatePublicDnsNamespace tests UpdatePublicDnsNamespace.
func TestHandler_UpdatePublicDnsNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantKey  string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK, wantKey: "OperationId"},
		{name: "missing_id", wantCode: http.StatusBadRequest},
		{name: "not_found", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.name {
			case "success":
				createRec := doSDRequest(t, h, "CreatePublicDnsNamespace", map[string]any{"Name": "public-ns"})
				var out map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
				opID := out["OperationId"].(string)
				opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
				var opOut map[string]any
				require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opOut))
				nsID := opOut["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)
				rec := doSDRequest(t, h, "UpdatePublicDnsNamespace", map[string]any{
					"Id":        nsID,
					"Namespace": map[string]any{"Description": "updated"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)
				assert.Contains(t, rec.Body.String(), tt.wantKey)

			case "missing_id":
				rec := doSDRequest(t, h, "UpdatePublicDnsNamespace", map[string]any{
					"Namespace": map[string]any{"Description": "updated"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "not_found":
				rec := doSDRequest(t, h, "UpdatePublicDnsNamespace", map[string]any{
					"Id":        "no-such-ns",
					"Namespace": map[string]any{"Description": "updated"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

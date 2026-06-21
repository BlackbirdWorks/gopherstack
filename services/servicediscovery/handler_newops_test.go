package servicediscovery_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestHandler_UpdateService tests UpdateService.
func TestHandler_UpdateService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantKey  string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK, wantKey: "Service"},
		{name: "missing_id", wantCode: http.StatusBadRequest},
		{name: "not_found", wantCode: http.StatusBadRequest},
		{name: "invalid_json", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.name {
			case "success":
				createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "my-svc"})
				var out map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
				svcID := out["Service"].(map[string]any)["Id"].(string)
				rec := doSDRequest(t, h, "UpdateService", map[string]any{
					"Id":      svcID,
					"Service": map[string]any{"Description": "updated description"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)
				assert.Contains(t, rec.Body.String(), tt.wantKey)

			case "missing_id":
				rec := doSDRequest(t, h, "UpdateService", map[string]any{
					"Service": map[string]any{"Description": "updated"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "not_found":
				rec := doSDRequest(t, h, "UpdateService", map[string]any{
					"Id":      "no-such-svc",
					"Service": map[string]any{"Description": "updated"},
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "invalid_json":
				rec := doSDRawRequest(t, h, "UpdateService", []byte("{bad"))
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

// TestHandler_ServiceAttributes tests GetServiceAttributes, UpdateServiceAttributes, DeleteServiceAttributes.
func TestHandler_ServiceAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "update_and_get", wantCode: http.StatusOK},
		{name: "get_not_found", wantCode: http.StatusBadRequest},
		{name: "get_missing_service_id", wantCode: http.StatusBadRequest},
		{name: "update_missing_arn", wantCode: http.StatusBadRequest},
		{name: "update_service_not_found", wantCode: http.StatusBadRequest},
		{name: "delete_and_verify", wantCode: http.StatusOK},
		{name: "delete_missing_id", wantCode: http.StatusBadRequest},
		{name: "delete_not_found_service", wantCode: http.StatusBadRequest},
		{name: "get_before_update", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Helper to create a service and return its ID and ARN.
			createSvc := func() (string, string) {
				t.Helper()
				rec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "attrs-svc"})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				svc := out["Service"].(map[string]any)

				return svc["Id"].(string), svc["Arn"].(string)
			}

			switch tt.name {
			case "update_and_get":
				svcID, svcARN := createSvc()
				updateRec := doSDRequest(t, h, "UpdateServiceAttributes", map[string]any{
					"ServiceArn": svcARN,
					"Attributes": map[string]string{"env": "prod", "version": "1.0"},
				})
				assert.Equal(t, http.StatusOK, updateRec.Code, "update should succeed: %s", updateRec.Body.String())

				getRec := doSDRequest(t, h, "GetServiceAttributes", map[string]any{"ServiceId": svcID})
				assert.Equal(t, http.StatusOK, getRec.Code)

				var getOut map[string]any
				require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
				sa := getOut["ServiceAttributes"].(map[string]any)
				attrs := sa["Attributes"].(map[string]any)
				assert.Equal(t, "prod", attrs["env"])
				assert.Equal(t, "1.0", attrs["version"])
				assert.Equal(t, svcARN, sa["Arn"])

			case "get_not_found":
				_, svcARN := createSvc()
				// Update then get a different ID
				doSDRequest(t, h, "UpdateServiceAttributes", map[string]any{
					"ServiceArn": svcARN,
					"Attributes": map[string]string{"env": "prod"},
				})
				// No attributes for another service ID
				getRec := doSDRequest(t, h, "GetServiceAttributes", map[string]any{"ServiceId": "no-such-svc"})
				assert.Equal(t, tt.wantCode, getRec.Code)

			case "get_missing_service_id":
				getRec := doSDRequest(t, h, "GetServiceAttributes", map[string]any{})
				assert.Equal(t, tt.wantCode, getRec.Code)

			case "update_missing_arn":
				updateRec := doSDRequest(t, h, "UpdateServiceAttributes", map[string]any{
					"Attributes": map[string]string{"env": "prod"},
				})
				assert.Equal(t, tt.wantCode, updateRec.Code)

			case "update_service_not_found":
				updateRec := doSDRequest(t, h, "UpdateServiceAttributes", map[string]any{
					"ServiceArn": "arn:aws:servicediscovery:us-east-1:000000000000:service/svc-99999999",
					"Attributes": map[string]string{"env": "prod"},
				})
				assert.Equal(t, tt.wantCode, updateRec.Code)

			case "delete_and_verify":
				svcID, svcARN := createSvc()
				doSDRequest(t, h, "UpdateServiceAttributes", map[string]any{
					"ServiceArn": svcARN,
					"Attributes": map[string]string{"env": "prod"},
				})
				deleteRec := doSDRequest(t, h, "DeleteServiceAttributes", map[string]any{"ServiceId": svcID})
				assert.Equal(t, http.StatusOK, deleteRec.Code)
				// After delete, GetServiceAttributes should 404
				getRec := doSDRequest(t, h, "GetServiceAttributes", map[string]any{"ServiceId": svcID})
				assert.Equal(t, http.StatusBadRequest, getRec.Code)

			case "delete_missing_id":
				deleteRec := doSDRequest(t, h, "DeleteServiceAttributes", map[string]any{})
				assert.Equal(t, tt.wantCode, deleteRec.Code)

			case "delete_not_found_service":
				deleteRec := doSDRequest(t, h, "DeleteServiceAttributes", map[string]any{"ServiceId": "no-such-svc"})
				assert.Equal(t, tt.wantCode, deleteRec.Code)

			case "get_before_update":
				svcID, _ := createSvc()
				// GetServiceAttributes before any UpdateServiceAttributes should fail
				getRec := doSDRequest(t, h, "GetServiceAttributes", map[string]any{"ServiceId": svcID})
				assert.Equal(t, tt.wantCode, getRec.Code)
			}
		})
	}
}

// TestHandler_UpdateInstanceCustomHealthStatus tests UpdateInstanceCustomHealthStatus.
func TestHandler_UpdateInstanceCustomHealthStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "set_healthy", wantCode: http.StatusOK},
		{name: "set_unhealthy", wantCode: http.StatusOK},
		{name: "invalid_status", wantCode: http.StatusBadRequest},
		{name: "missing_service_id", wantCode: http.StatusBadRequest},
		{name: "missing_instance_id", wantCode: http.StatusBadRequest},
		{name: "missing_status", wantCode: http.StatusBadRequest},
		{name: "instance_not_found", wantCode: http.StatusBadRequest},
		{name: "invalid_json", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Helper to create a service with an instance, returning the svcID and instanceID.
			createSvcWithInstance := func() (string, string) {
				t.Helper()
				createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "health-svc"})
				require.Equal(t, http.StatusOK, createRec.Code)
				var out map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
				svcID := out["Service"].(map[string]any)["Id"].(string)
				regRec := doSDRequest(t, h, "RegisterInstance", map[string]any{
					"ServiceId":  svcID,
					"InstanceId": "inst-001",
					"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "1.2.3.4"},
				})
				require.Equal(t, http.StatusOK, regRec.Code)

				return svcID, "inst-001"
			}

			switch tt.name {
			case "set_healthy":
				svcID, instID := createSvcWithInstance()
				rec := doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
					"ServiceId":  svcID,
					"InstanceId": instID,
					"Status":     "HEALTHY",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "set_unhealthy":
				svcID, instID := createSvcWithInstance()
				rec := doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
					"ServiceId":  svcID,
					"InstanceId": instID,
					"Status":     "UNHEALTHY",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "invalid_status":
				svcID, instID := createSvcWithInstance()
				rec := doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
					"ServiceId":  svcID,
					"InstanceId": instID,
					"Status":     "UNKNOWN",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "missing_service_id":
				rec := doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
					"InstanceId": "inst-001",
					"Status":     "HEALTHY",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "missing_instance_id":
				rec := doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
					"ServiceId": "svc-00000001",
					"Status":    "HEALTHY",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "missing_status":
				rec := doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
					"ServiceId":  "svc-00000001",
					"InstanceId": "inst-001",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "instance_not_found":
				rec := doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
					"ServiceId":  "svc-00000001",
					"InstanceId": "no-such-inst",
					"Status":     "HEALTHY",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "invalid_json":
				rec := doSDRawRequest(t, h, "UpdateInstanceCustomHealthStatus", []byte("{bad"))
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

// TestHandler_DiscoverInstancesRevision tests DiscoverInstancesRevision.
func TestHandler_DiscoverInstancesRevision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantKey  string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK, wantKey: "InstancesRevision"},
		{name: "revision_increments", wantCode: http.StatusOK, wantKey: "InstancesRevision"},
		{name: "namespace_not_found", wantCode: http.StatusBadRequest},
		{name: "service_not_found", wantCode: http.StatusBadRequest},
		{name: "missing_namespace_name", wantCode: http.StatusBadRequest},
		{name: "missing_service_name", wantCode: http.StatusBadRequest},
		{name: "invalid_json", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Helper: create namespace+service and return names.
			setup := func() (string, string) {
				t.Helper()
				createNsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "rev-ns"})
				require.Equal(t, http.StatusOK, createNsRec.Code)
				var nsOut map[string]any
				require.NoError(t, json.Unmarshal(createNsRec.Body.Bytes(), &nsOut))
				opID := nsOut["OperationId"].(string)
				opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
				var opOut map[string]any
				require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opOut))
				nsID := opOut["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

				createSvcRec := doSDRequest(t, h, "CreateService", map[string]any{
					"Name":        "rev-svc",
					"NamespaceId": nsID,
				})
				require.Equal(t, http.StatusOK, createSvcRec.Code)

				return "rev-ns", "rev-svc"
			}

			switch tt.name {
			case "success":
				nsName, svcName := setup()
				rec := doSDRequest(t, h, "DiscoverInstancesRevision", map[string]any{
					"NamespaceName": nsName,
					"ServiceName":   svcName,
				})
				assert.Equal(t, tt.wantCode, rec.Code)
				assert.Contains(t, rec.Body.String(), tt.wantKey)

			case "revision_increments":
				nsName, svcName := setup()
				nsOut := doSDRequest(t, h, "GetOperation", map[string]any{})

				// Get initial revision
				rec1 := doSDRequest(t, h, "DiscoverInstancesRevision", map[string]any{
					"NamespaceName": nsName,
					"ServiceName":   svcName,
				})
				assert.Equal(t, http.StatusOK, rec1.Code)
				_ = nsOut

				var out1 map[string]any
				require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
				rev1 := out1["InstancesRevision"].(float64)

				// Register an instance to bump revision
				createSvcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "bump-svc"})
				var svcOut map[string]any
				require.NoError(t, json.Unmarshal(createSvcRec.Body.Bytes(), &svcOut))
				svcID := svcOut["Service"].(map[string]any)["Id"].(string)

				doSDRequest(t, h, "RegisterInstance", map[string]any{
					"ServiceId":  svcID,
					"InstanceId": "inst-bump",
					"Attributes": map[string]string{},
				})

				// Revision should now be higher
				rec2 := doSDRequest(t, h, "DiscoverInstancesRevision", map[string]any{
					"NamespaceName": nsName,
					"ServiceName":   svcName,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
				var out2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
				rev2 := out2["InstancesRevision"].(float64)
				assert.Greater(t, rev2, rev1)

			case "namespace_not_found":
				rec := doSDRequest(t, h, "DiscoverInstancesRevision", map[string]any{
					"NamespaceName": "no-such-ns",
					"ServiceName":   "svc",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "service_not_found":
				nsName, _ := setup()
				rec := doSDRequest(t, h, "DiscoverInstancesRevision", map[string]any{
					"NamespaceName": nsName,
					"ServiceName":   "no-such-svc",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "missing_namespace_name":
				rec := doSDRequest(t, h, "DiscoverInstancesRevision", map[string]any{
					"ServiceName": "svc",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "missing_service_name":
				rec := doSDRequest(t, h, "DiscoverInstancesRevision", map[string]any{
					"NamespaceName": "ns",
				})
				assert.Equal(t, tt.wantCode, rec.Code)

			case "invalid_json":
				rec := doSDRawRequest(t, h, "DiscoverInstancesRevision", []byte("{bad"))
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

// TestHandler_NewOps_GetSupportedOperations verifies all 9 new ops are in GetSupportedOperations.
func TestHandler_NewOps_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"DeleteServiceAttributes",
		"DiscoverInstancesRevision",
		"GetServiceAttributes",
		"UpdateHttpNamespace",
		"UpdateInstanceCustomHealthStatus",
		"UpdatePrivateDnsNamespace",
		"UpdatePublicDnsNamespace",
		"UpdateService",
		"UpdateServiceAttributes",
	} {
		assert.Contains(t, ops, op, "expected %s in GetSupportedOperations", op)
	}
}

// TestHandler_NewOps_PersistenceRoundTrip verifies that new state (service attributes, health statuses)
// is preserved through Snapshot/Restore.
func TestHandler_NewOps_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a service and set its attributes.
	createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "persist-svc"})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	svc := createOut["Service"].(map[string]any)
	svcID := svc["Id"].(string)
	svcARN := svc["Arn"].(string)

	updateRec := doSDRequest(t, h, "UpdateServiceAttributes", map[string]any{
		"ServiceArn": svcARN,
		"Attributes": map[string]string{"stage": "test"},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Register an instance and set custom health status.
	regRec := doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "inst-persist",
		"Attributes": map[string]string{},
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	healthRec := doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "inst-persist",
		"Status":     "UNHEALTHY",
	})
	require.Equal(t, http.StatusOK, healthRec.Code)

	// Snapshot and restore.
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Verify service attributes restored.
	getRec := doSDRequest(t, h2, "GetServiceAttributes", map[string]any{"ServiceId": svcID})
	assert.Equal(t, http.StatusOK, getRec.Code)
	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	attrs := getOut["ServiceAttributes"].(map[string]any)["Attributes"].(map[string]any)
	assert.Equal(t, "test", attrs["stage"])
}

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

func TestHandler_RegisterInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       any
		bodyRaw    []byte
		wantStatus int
		createSvc  bool
	}{
		{
			name:       "success",
			createSvc:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_service_id",
			body:       map[string]any{"InstanceId": "i-001"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_instance_id",
			body:       map[string]any{"ServiceId": "svc-00000001"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "service_not_found",
			body:       map[string]any{"ServiceId": "svc-does-not-exist", "InstanceId": "i-001"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			switch {
			case tt.createSvc:
				createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "reg-svc"})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				svcData := createResp["Service"].(map[string]any)
				svcID := svcData["Id"].(string)

				rec = doSDRequest(t, h, "RegisterInstance", map[string]any{
					"ServiceId":  svcID,
					"InstanceId": "i-001",
					"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
				})
			case tt.bodyRaw != nil:
				rec = doSDRawRequest(t, h, "RegisterInstance", tt.bodyRaw)
			default:
				rec = doSDRequest(t, h, "RegisterInstance", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_RegisterInstance_AttributeQuota verifies the RegisterInstance
// custom-attribute quota documented on RegisterInstanceInput.Attributes: "You
// can add up to 30 custom attributes. For each key-value pair, the maximum
// length of the attribute name is 255 characters, and the maximum length of
// the attribute value is 1,024 characters. The total size of all provided
// attributes (sum of all keys and values) must not exceed 5,000 characters".
func TestHandler_RegisterInstance_AttributeQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs      func() map[string]string
		name       string
		wantStatus int
	}{
		{
			name:       "30_attributes_ok",
			wantStatus: http.StatusOK,
			attrs: func() map[string]string {
				m := make(map[string]string, 30)
				for i := range 30 {
					m[fmt.Sprintf("k%02d", i)] = "v"
				}

				return m
			},
		},
		{
			name:       "31_attributes_rejected",
			wantStatus: http.StatusBadRequest,
			attrs: func() map[string]string {
				m := make(map[string]string, 31)
				for i := range 31 {
					m[fmt.Sprintf("k%02d", i)] = "v"
				}

				return m
			},
		},
		{
			name:       "key_too_long_rejected",
			wantStatus: http.StatusBadRequest,
			attrs: func() map[string]string {
				return map[string]string{fmt.Sprintf("%0256s", "k"): "v"}
			},
		},
		{
			name:       "value_too_long_rejected",
			wantStatus: http.StatusBadRequest,
			attrs: func() map[string]string {
				return map[string]string{"k": fmt.Sprintf("%01025s", "v")}
			},
		},
		{
			name:       "total_size_exceeded_rejected",
			wantStatus: http.StatusBadRequest,
			attrs: func() map[string]string {
				// 6 attributes * (2-char key + 1000-char value) = 6012 > 5000.
				m := make(map[string]string, 6)
				for i := range 6 {
					m[fmt.Sprintf("k%d", i)] = fmt.Sprintf("%01000s", "v")
				}

				return m
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "attr-quota-svc"})
			require.Equal(t, http.StatusOK, createRec.Code)
			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			svcID := createResp["Service"].(map[string]any)["Id"].(string)

			rec := doSDRequest(t, h, "RegisterInstance", map[string]any{
				"ServiceId": svcID, "InstanceId": "i-quota", "Attributes": tt.attrs(),
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// TestHandler_RegisterInstance_InitHealthStatus verifies that the
// AWS_INIT_HEALTH_STATUS attribute seeds an instance's initial custom health
// status when the service has HealthCheckCustomConfig, per
// RegisterInstanceInput.Attributes' doc comment: "If the service configuration
// includes HealthCheckCustomConfig, you can optionally use
// AWS_INIT_HEALTH_STATUS to specify the initial status of the custom health
// check, HEALTHY or UNHEALTHY. If you don't specify a value ..., the initial
// status is HEALTHY".
func TestHandler_RegisterInstance_InitHealthStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs        map[string]string
		name         string
		wantStatus   string
		withCustomHC bool
	}{
		{
			name:         "unhealthy_init_status_honored",
			withCustomHC: true,
			attrs:        map[string]string{"AWS_INIT_HEALTH_STATUS": "UNHEALTHY"},
			wantStatus:   "UNHEALTHY",
		},
		{
			name:         "healthy_init_status_honored",
			withCustomHC: true,
			attrs:        map[string]string{"AWS_INIT_HEALTH_STATUS": "HEALTHY"},
			wantStatus:   "HEALTHY",
		},
		{
			name:         "unspecified_defaults_to_healthy",
			withCustomHC: true,
			attrs:        map[string]string{},
			wantStatus:   "HEALTHY",
		},
		{
			name:         "ignored_without_custom_health_check",
			withCustomHC: false,
			attrs:        map[string]string{"AWS_INIT_HEALTH_STATUS": "UNHEALTHY"},
			wantStatus:   "HEALTHY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{"Name": "init-health-svc"}
			if tt.withCustomHC {
				body["HealthCheckCustomConfig"] = map[string]any{"FailureThreshold": 1}
			}

			createRec := doSDRequest(t, h, "CreateService", body)
			require.Equal(t, http.StatusOK, createRec.Code)
			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			svcID := createResp["Service"].(map[string]any)["Id"].(string)

			regRec := doSDRequest(t, h, "RegisterInstance", map[string]any{
				"ServiceId": svcID, "InstanceId": "i-init-health", "Attributes": tt.attrs,
			})
			require.Equal(t, http.StatusOK, regRec.Code)

			statusRec := doSDRequest(t, h, "GetInstancesHealthStatus", map[string]any{"ServiceId": svcID})
			require.Equal(t, http.StatusOK, statusRec.Code)
			var statusResp map[string]any
			require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &statusResp))
			statuses := statusResp["Status"].(map[string]any)
			assert.Equal(t, tt.wantStatus, statuses["i-init-health"])
		})
	}
}

func TestHandler_GetInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "inst-svc"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	svcData := createResp["Service"].(map[string]any)
	svcID := svcData["Id"].(string)

	regRec := doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-001",
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	tests := []struct {
		body       any
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"ServiceId": svcID, "InstanceId": "i-001"},
			wantStatus: http.StatusOK,
			wantBody:   "Instance",
		},
		{
			name:       "not_found",
			body:       map[string]any{"ServiceId": svcID, "InstanceId": "i-does-not-exist"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "InstanceNotFound",
		},
		{
			name:       "missing_service_id",
			body:       map[string]any{"InstanceId": "i-001"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_instance_id",
			body:       map[string]any{"ServiceId": svcID},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, "GetInstance", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ListInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "list-inst-svc"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	svcData := createResp["Service"].(map[string]any)
	svcID := svcData["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "i-001",
	})

	tests := []struct {
		body       any
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"ServiceId": svcID},
			wantStatus: http.StatusOK,
			wantBody:   "Instances",
		},
		{
			name:       "service_not_found",
			body:       map[string]any{"ServiceId": "svc-does-not-exist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_service_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, "ListInstances", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DeregisterInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "dereg-svc"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	svcData := createResp["Service"].(map[string]any)
	svcID := svcData["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "i-001",
	})

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"ServiceId": svcID, "InstanceId": "i-001"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       map[string]any{"ServiceId": svcID, "InstanceId": "i-does-not-exist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_service_id",
			body:       map[string]any{"InstanceId": "i-001"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_instance_id",
			body:       map[string]any{"ServiceId": svcID},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, "DeregisterInstance", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetInstancesHealthStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *servicediscovery.Handler) string
		body       func(serviceID string) map[string]any
		name       string
		wantFields []string
		wantCode   int
	}{
		{
			name: "returns_healthy_for_all_instances",
			setup: func(t *testing.T, h *servicediscovery.Handler) string {
				t.Helper()
				// Create namespace and service
				nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{
					"Name":             "ns-health",
					"CreatorRequestId": "req-health",
				})
				require.Equal(t, http.StatusOK, nsRec.Code)

				// Create service
				svcRec := doSDRequest(t, h, "CreateService", map[string]any{
					"Name":             "svc-health",
					"CreatorRequestId": "req-svc-health",
				})
				require.Equal(t, http.StatusOK, svcRec.Code)

				var svcOut map[string]any
				require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcOut))
				svc := svcOut["Service"].(map[string]any)

				return svc["Id"].(string)
			},
			body: func(serviceID string) map[string]any {
				return map[string]any{"ServiceId": serviceID}
			},
			wantCode:   http.StatusOK,
			wantFields: []string{"Status"},
		},
		{
			name: "missing_service_id_returns_error",
			setup: func(t *testing.T, _ *servicediscovery.Handler) string {
				t.Helper()

				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serviceID := tt.setup(t, h)
			rec := doSDRequest(t, h, "GetInstancesHealthStatus", tt.body(serviceID))
			require.Equal(t, tt.wantCode, rec.Code)

			if len(tt.wantFields) > 0 {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				for _, field := range tt.wantFields {
					assert.Contains(t, out, field)
				}
			}
		})
	}
}

// TestInMemoryBackend_UpdateInstanceCustomHealthStatus_RequiresCustomHealthCheck
// verifies that UpdateInstanceCustomHealthStatus rejects services that were not
// configured with HealthCheckCustomConfig, matching real Cloud Map: "You can use
// UpdateInstanceCustomHealthStatus to change the status only for custom health checks.".
func TestInMemoryBackend_UpdateInstanceCustomHealthStatus_RequiresCustomHealthCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		newService func(id, name, namespaceID string) *servicediscovery.Service
		name       string
	}{
		{
			name:       "no custom health check configured",
			newService: servicediscovery.NewServiceForTest,
			wantErr:    servicediscovery.ErrCustomHealthNotFound,
		},
		{
			name:       "custom health check configured",
			newService: servicediscovery.NewServiceWithCustomHealthForTest,
			wantErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")

			svc := tt.newService("svc-0000001", "svc-one", "")
			servicediscovery.AddServiceInternal(b, svc)

			inst := servicediscovery.NewInstanceForTest("i-1", svc.ID, nil)
			servicediscovery.AddInstanceInternal(b, inst)

			err := b.UpdateInstanceCustomHealthStatus(svc.ID, "i-1", "UNHEALTHY")

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestHandler_RegisterInstanceRealOperationID verifies that RegisterInstance
// returns a real, non-hardcoded operation ID that can be retrieved.
func TestHandler_RegisterInstanceRealOperationID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create namespace and service first.
	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-reg"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-reg", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	// Register instance.
	regRec := doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-001",
		"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
	})
	require.Equal(t, 200, regRec.Code)

	var regResp map[string]any
	require.NoError(t, json.Unmarshal(regRec.Body.Bytes(), &regResp))

	opID := regResp["OperationId"].(string)
	assert.NotEqual(t, "op-register", opID, "must not be hardcoded op ID")
	assert.NotEmpty(t, opID)

	// Verify the operation can be retrieved.
	opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
	require.Equal(t, 200, opRec.Code)

	var opResp map[string]any
	require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
	assert.Equal(t, "REGISTER_INSTANCE", opResp["Operation"].(map[string]any)["Type"])
}

// TestHandler_DeregisterInstanceRealOperationID verifies that DeregisterInstance
// returns a real operation ID for polling.
func TestHandler_DeregisterInstanceRealOperationID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup.
	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-dereg"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-dereg", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-002",
		"Attributes": map[string]string{},
	})

	// Deregister.
	deregRec := doSDRequest(t, h, "DeregisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-002",
	})
	require.Equal(t, 200, deregRec.Code)

	var deregResp map[string]any
	require.NoError(t, json.Unmarshal(deregRec.Body.Bytes(), &deregResp))

	opID := deregResp["OperationId"].(string)
	assert.NotEqual(t, "op-deregister", opID)
	assert.NotEmpty(t, opID)

	opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
	require.Equal(t, 200, opRec.Code)

	var opResp map[string]any
	require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
	assert.Equal(t, "DEREGISTER_INSTANCE", opResp["Operation"].(map[string]any)["Type"])
}

// TestHandler_GetInstancesHealthStatusStoredValue verifies that UpdateInstanceCustomHealthStatus
// changes the value returned by GetInstancesHealthStatus.
func TestHandler_GetInstancesHealthStatusStoredValue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup service and instances.
	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-health"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name":                    "svc-health",
		"NamespaceId":             nsID,
		"HealthCheckCustomConfig": map[string]any{"FailureThreshold": 1},
	})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "inst-healthy", "Attributes": map[string]string{},
	})
	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "inst-unhealthy", "Attributes": map[string]string{},
	})

	// Mark one instance unhealthy.
	doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "inst-unhealthy",
		"Status":     "UNHEALTHY",
	})

	// Check statuses.
	statusRec := doSDRequest(t, h, "GetInstancesHealthStatus", map[string]any{
		"ServiceId": svcID,
	})
	require.Equal(t, 200, statusRec.Code)

	var statusResp map[string]any
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &statusResp))

	statuses := statusResp["Status"].(map[string]any)
	assert.Equal(t, "HEALTHY", statuses["inst-healthy"])
	assert.Equal(t, "UNHEALTHY", statuses["inst-unhealthy"])
}

// TestHandler_GetInstancesHealthStatusFilterByIDs verifies the Instances filter.
func TestHandler_GetInstancesHealthStatusFilterByIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-filter-health"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-filter", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	for _, iid := range []string{"a", "b", "c"} {
		doSDRequest(t, h, "RegisterInstance", map[string]any{
			"ServiceId": svcID, "InstanceId": iid, "Attributes": map[string]string{},
		})
	}

	// Filter to only b and c.
	rec := doSDRequest(t, h, "GetInstancesHealthStatus", map[string]any{
		"ServiceId": svcID,
		"Instances": []string{"b", "c"},
	})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	statuses := resp["Status"].(map[string]any)
	assert.Len(t, statuses, 2)
	assert.Contains(t, statuses, "b")
	assert.Contains(t, statuses, "c")
	assert.NotContains(t, statuses, "a")
}

// TestHandler_GetInstancesHealthStatusServiceNotFound verifies error on unknown service.
func TestHandler_GetInstancesHealthStatusServiceNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "GetInstancesHealthStatus", map[string]any{
		"ServiceId": "svc-does-not-exist",
	})
	assert.Equal(t, 400, rec.Code)
}

// TestListInstances_Pagination verifies NextToken/MaxResults pagination on ListInstances.
func TestListInstances_Pagination(t *testing.T) {
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

			// Helper to create a service (with a custom health check, required for
			// UpdateInstanceCustomHealthStatus per AWS) and register an instance,
			// returning the svcID and instanceID.
			createSvcWithInstance := func() (string, string) {
				t.Helper()
				createRec := doSDRequest(t, h, "CreateService", map[string]any{
					"Name":                    "health-svc",
					"HealthCheckCustomConfig": map[string]any{"FailureThreshold": 1},
				})
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

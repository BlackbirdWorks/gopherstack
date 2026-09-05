package servicediscovery_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

func TestHandler_DiscoverInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "example.local"})
	require.Equal(t, http.StatusOK, nsRec.Code)

	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	opID := nsResp["OperationId"].(string)

	opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
	require.Equal(t, http.StatusOK, opRec.Code)

	var opResp map[string]any
	require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
	operation := opResp["Operation"].(map[string]any)
	targets := operation["Targets"].(map[string]any)
	nsID := targets["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name":        "my-service",
		"NamespaceId": nsID,
	})
	require.Equal(t, http.StatusOK, svcRec.Code)

	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcData := svcResp["Service"].(map[string]any)
	svcID := svcData["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-001",
		"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
	})

	tests := []struct {
		body       any
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success_with_results",
			body: map[string]any{
				"NamespaceName": "example.local",
				"ServiceName":   "my-service",
			},
			wantStatus: http.StatusOK,
			wantBody:   "Instances",
		},
		{
			name: "no_results_unknown_ns",
			body: map[string]any{
				"NamespaceName": "does-not-exist",
				"ServiceName":   "my-service",
			},
			wantStatus: http.StatusOK,
			wantBody:   "Instances",
		},
		{
			name:       "missing_namespace_name",
			body:       map[string]any{"ServiceName": "my-service"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_service_name",
			body:       map[string]any{"NamespaceName": "example.local"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, "DiscoverInstances", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestInMemoryBackend_DiscoverInstances_HealthyOrElseAll verifies the
// HEALTHY_OR_ELSE_ALL HealthStatus filter: it returns only healthy instances
// unless none are healthy, in which case it "fails open" and returns every
// registered instance -- matching real Cloud Map DiscoverInstances semantics.
func TestInMemoryBackend_DiscoverInstances_HealthyOrElseAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		unhealthyIDs  []string
		wantInstances []string
	}{
		{
			name:          "some healthy returns only healthy",
			unhealthyIDs:  []string{"i-2"},
			wantInstances: []string{"i-1"},
		},
		{
			name:          "none healthy fails open to all",
			unhealthyIDs:  []string{"i-1", "i-2"},
			wantInstances: []string{"i-1", "i-2"},
		},
		{
			name:          "all healthy returns all",
			unhealthyIDs:  nil,
			wantInstances: []string{"i-1", "i-2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")

			_, err := b.CreateHTTPNamespace("ns-fail-open", "", nil)
			require.NoError(t, err)
			nsID := b.ListNamespaces(servicediscovery.ListNamespacesFilter{})[0].ID

			svc, err := b.CreateService(
				"svc-fail-open", nsID, "", "", nil, nil,
				&servicediscovery.HealthCheckCustomConfig{FailureThreshold: 1}, nil,
			)
			require.NoError(t, err)

			_, err = b.RegisterInstance(svc.ID, "i-1", nil)
			require.NoError(t, err)
			_, err = b.RegisterInstance(svc.ID, "i-2", nil)
			require.NoError(t, err)

			for _, id := range tt.unhealthyIDs {
				require.NoError(t, b.UpdateInstanceCustomHealthStatus(svc.ID, id, "UNHEALTHY"))
			}

			discovered, _, err := b.DiscoverInstances("ns-fail-open", "svc-fail-open", "HEALTHY_OR_ELSE_ALL", nil, nil)
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(discovered))
			for _, d := range discovered {
				gotIDs = append(gotIDs, d.InstanceID)
			}

			assert.ElementsMatch(t, tt.wantInstances, gotIDs)
		})
	}
}

// TestHandler_DiscoverInstancesHealthStatusFilter verifies that DiscoverInstances
// filters by HealthStatus.
func TestHandler_DiscoverInstancesHealthStatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-discover-health"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name":                    "svc-discover-health",
		"NamespaceId":             nsID,
		"HealthCheckCustomConfig": map[string]any{"FailureThreshold": 1},
	})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "healthy-i", "Attributes": map[string]string{},
	})
	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "unhealthy-i", "Attributes": map[string]string{},
	})

	doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "unhealthy-i",
		"Status":     "UNHEALTHY",
	})

	// Discover with HEALTHY filter.
	recHealthy := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName": "ns-discover-health",
		"ServiceName":   "svc-discover-health",
		"HealthStatus":  "HEALTHY",
	})
	require.Equal(t, 200, recHealthy.Code)

	var respH map[string]any
	require.NoError(t, json.Unmarshal(recHealthy.Body.Bytes(), &respH))
	assert.Len(t, respH["Instances"].([]any), 1)
	assert.Equal(t, "healthy-i", respH["Instances"].([]any)[0].(map[string]any)["InstanceId"])

	// Discover with ALL filter - should return both.
	recAll := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName": "ns-discover-health",
		"ServiceName":   "svc-discover-health",
		"HealthStatus":  "ALL",
	})
	var respAll map[string]any
	require.NoError(t, json.Unmarshal(recAll.Body.Bytes(), &respAll))
	assert.Len(t, respAll["Instances"].([]any), 2)
}

// TestHandler_DiscoverInstancesQueryParameters verifies attribute filtering.
func TestHandler_DiscoverInstancesQueryParameters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-qp"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-qp", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "i-web",
		"Attributes": map[string]string{"tier": "web", "color": "blue"},
	})
	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "i-db",
		"Attributes": map[string]string{"tier": "db", "color": "green"},
	})

	rec := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName":   "ns-qp",
		"ServiceName":     "svc-qp",
		"QueryParameters": map[string]string{"tier": "web"},
	})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	insts := resp["Instances"].([]any)
	assert.Len(t, insts, 1)
	assert.Equal(t, "i-web", insts[0].(map[string]any)["InstanceId"])
}

// TestHandler_DiscoverInstancesOptionalParameters verifies OptionalParameters'
// documented "opportunistic" semantics: "If there are instances that match
// both the filters specified in both the QueryParameters parameter and this
// parameter, all of these instances are returned. Otherwise, the filters are
// ignored, and only instances that match the filters that are specified in
// the QueryParameters parameter are returned." (DiscoverInstancesInput
// .OptionalParameters doc comment).
func TestHandler_DiscoverInstancesOptionalParameters(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *servicediscovery.Handler {
		t.Helper()

		h := newTestHandler(t)

		nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-optparams"})
		var nsResp map[string]any
		require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
		nsOpID := nsResp["OperationId"].(string)
		nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
		var nsOpResp map[string]any
		require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
		nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

		svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-optparams", "NamespaceId": nsID})
		var svcResp map[string]any
		require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
		svcID := svcResp["Service"].(map[string]any)["Id"].(string)

		doSDRequest(t, h, "RegisterInstance", map[string]any{
			"ServiceId": svcID, "InstanceId": "i-blue-web",
			"Attributes": map[string]string{"color": "blue", "tier": "web"},
		})
		doSDRequest(t, h, "RegisterInstance", map[string]any{
			"ServiceId": svcID, "InstanceId": "i-green-web",
			"Attributes": map[string]string{"color": "green", "tier": "web"},
		})

		return h
	}

	t.Run("optional_match_narrows_results", func(t *testing.T) {
		t.Parallel()

		h := setup(t)

		rec := doSDRequest(t, h, "DiscoverInstances", map[string]any{
			"NamespaceName":      "ns-optparams",
			"ServiceName":        "svc-optparams",
			"QueryParameters":    map[string]string{"tier": "web"},
			"OptionalParameters": map[string]string{"color": "blue"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		insts := resp["Instances"].([]any)
		require.Len(t, insts, 1, "only the instance matching both QueryParameters and OptionalParameters")
		assert.Equal(t, "i-blue-web", insts[0].(map[string]any)["InstanceId"])
	})

	t.Run("no_optional_match_falls_back_to_query_parameters_only", func(t *testing.T) {
		t.Parallel()

		h := setup(t)

		rec := doSDRequest(t, h, "DiscoverInstances", map[string]any{
			"NamespaceName":      "ns-optparams",
			"ServiceName":        "svc-optparams",
			"QueryParameters":    map[string]string{"tier": "web"},
			"OptionalParameters": map[string]string{"color": "purple"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		insts := resp["Instances"].([]any)
		assert.Len(t, insts, 2, "no instance matches OptionalParameters, so it's ignored")
	})
}

// TestHandler_DiscoverInstancesUnhealthyFilter verifies UNHEALTHY filter.
func TestHandler_DiscoverInstancesUnhealthyFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsID := createNamespaceHelper(t, h, "ns-unhealthy-disc")
	svcRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name":                    "svc-unhealthy-disc",
		"NamespaceId":             nsID,
		"HealthCheckCustomConfig": map[string]any{"FailureThreshold": 1},
	})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "u1", "Attributes": map[string]string{},
	})

	doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
		"ServiceId": svcID, "InstanceId": "u1", "Status": "UNHEALTHY",
	})

	rec := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName": "ns-unhealthy-disc",
		"ServiceName":   "svc-unhealthy-disc",
		"HealthStatus":  "UNHEALTHY",
	})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["Instances"].([]any), 1)
}

// TestHandler_DiscoverInstancesHealthStatusIgnoredWithoutHealthCheck verifies
// the DiscoverInstancesInput.HealthStatus doc comment's documented override:
// "This parameter is ignored for services that don't have a health check
// configured, and all instances are returned." A service created with
// neither HealthCheckConfig nor HealthCheckCustomConfig can never have an
// UNHEALTHY instance, so an UNHEALTHY filter must still return every
// instance rather than narrowing to none.
func TestHandler_DiscoverInstancesHealthStatusIgnoredWithoutHealthCheck(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsID := createNamespaceHelper(t, h, "ns-no-healthcheck")
	svcRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name":        "svc-no-healthcheck",
		"NamespaceId": nsID,
	})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "i1", "Attributes": map[string]string{},
	})

	rec := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName": "ns-no-healthcheck",
		"ServiceName":   "svc-no-healthcheck",
		"HealthStatus":  "UNHEALTHY",
	})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["Instances"].([]any), 1, "HealthStatus must be ignored when the service has no health check")
}

// TestHandler_DiscoverInstancesEmptyWhenNamespaceNotFound verifies graceful empty result.
func TestHandler_DiscoverInstancesEmptyWhenNamespaceNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName": "nonexistent",
		"ServiceName":   "nonexistent",
	})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Instances"].([]any))
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

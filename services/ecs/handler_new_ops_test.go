package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// ----- CapacityProvider tests -----

func TestECS_CreateCapacityProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantName string
		wantCode int
	}{
		{
			name:     "creates capacity provider",
			input:    map[string]any{"name": "my-cp"},
			wantCode: http.StatusOK,
			wantName: "my-cp",
		},
		{
			name:     "missing name returns 400",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with tags",
			input: map[string]any{
				"name": "tagged-cp",
				"tags": []map[string]any{{"key": "env", "value": "test"}},
			},
			wantCode: http.StatusOK,
			wantName: "tagged-cp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "CreateCapacityProvider", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cp, ok := resp["capacityProvider"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantName, cp["name"])
			assert.Equal(t, "ACTIVE", cp["status"])
			assert.NotEmpty(t, cp["capacityProviderArn"])
		})
	}
}

func TestECS_CreateCapacityProvider_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "my-cp"})
	rec := doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "my-cp"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AlreadyExists")
}

func TestECS_DeleteCapacityProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cpName   string
		deleteBy string
		wantCode int
	}{
		{
			name:     "delete by name",
			cpName:   "my-cp",
			deleteBy: "my-cp",
			wantCode: http.StatusOK,
		},
		{
			name:     "delete non-existent returns 400",
			cpName:   "",
			deleteBy: "non-existent",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.cpName != "" {
				doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": tt.cpName})
			}

			rec := doECSRequest(
				t,
				h,
				"DeleteCapacityProvider",
				map[string]any{"capacityProvider": tt.deleteBy},
			)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cp, ok := resp["capacityProvider"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.cpName, cp["name"])
		})
	}
}

func TestECS_DescribeCapacityProviders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input        map[string]any
		name         string
		seedNames    []string
		wantCount    int
		wantFailures int
		wantCode     int
	}{
		{
			name:      "describe all when no filter",
			seedNames: []string{"cp-a", "cp-b"},
			input:     map[string]any{},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name:      "describe specific by name",
			seedNames: []string{"cp-a", "cp-b"},
			input:     map[string]any{"capacityProviders": []string{"cp-a"}},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
		{
			name:      "empty backend returns empty list",
			seedNames: nil,
			input:     map[string]any{},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			// AWS reports unknown capacity providers as a Failure entry (reason
			// MISSING), not as a whole-request error — matches DescribeClusters,
			// DescribeContainerInstances, etc.
			name:         "unknown capacity provider returns failure not error",
			input:        map[string]any{"capacityProviders": []string{"unknown-cp"}},
			wantCode:     http.StatusOK,
			wantCount:    0,
			wantFailures: 1,
		},
		{
			name:         "mix of known and unknown returns partial success",
			seedNames:    []string{"cp-a"},
			input:        map[string]any{"capacityProviders": []string{"cp-a", "unknown-cp"}},
			wantCode:     http.StatusOK,
			wantCount:    1,
			wantFailures: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, n := range tt.seedNames {
				doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": n})
			}

			rec := doECSRequest(t, h, "DescribeCapacityProviders", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			providers, ok := resp["capacityProviders"].([]any)
			require.True(t, ok)
			assert.Len(t, providers, tt.wantCount)

			failures, ok := resp["failures"].([]any)
			require.True(t, ok)
			assert.Len(t, failures, tt.wantFailures)
		})
	}
}

func TestECS_DescribeCapacityProviders_SortedByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, n := range []string{"cp-z", "cp-a", "cp-m"} {
		doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": n})
	}

	rec := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	providers, ok := resp["capacityProviders"].([]any)
	require.True(t, ok)
	require.Len(t, providers, 3)

	assert.Equal(t, "cp-a", providers[0].(map[string]any)["name"])
	assert.Equal(t, "cp-m", providers[1].(map[string]any)["name"])
	assert.Equal(t, "cp-z", providers[2].(map[string]any)["name"])
}

func TestECS_DescribeCapacityProviders_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "my-cp"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	arn := createResp["capacityProvider"].(map[string]any)["capacityProviderArn"].(string)

	rec = doECSRequest(
		t,
		h,
		"DescribeCapacityProviders",
		map[string]any{"capacityProviders": []string{arn}},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	providers, ok := resp["capacityProviders"].([]any)
	require.True(t, ok)
	require.Len(t, providers, 1)
	assert.Equal(t, "my-cp", providers[0].(map[string]any)["name"])
}

// ----- DeleteAccountSetting tests -----

func TestECS_DeleteAccountSetting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		seedKey  string
		seedName string
		wantCode int
	}{
		{
			name:     "delete existing setting",
			seedName: "serviceLongArnFormat",
			seedKey:  ":serviceLongArnFormat",
			input:    map[string]any{"name": "serviceLongArnFormat"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing name returns 400",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "non-existent setting returns 400",
			input:    map[string]any{"name": "nonExistentSetting"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

			if tt.seedName != "" {
				backend.AddAccountSettingInternal(tt.seedKey, &ecs.AccountSetting{
					Name:  tt.seedName,
					Value: "enabled",
				})
			}

			h := ecs.NewHandler(backend)
			rec := doECSRequest(t, h, "DeleteAccountSetting", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			setting, ok := resp["setting"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.seedName, setting["name"])
		})
	}
}

// ----- DeleteAttributes tests -----

func TestECS_DeleteAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     map[string]any
		name      string
		seedAttrs []ecs.Attribute
		wantCount int
		wantCode  int
	}{
		{
			name: "delete existing attributes",
			seedAttrs: []ecs.Attribute{
				{Name: "my-attr", Value: "my-value", TargetID: "container-1"},
			},
			input: map[string]any{
				"attributes": []map[string]any{
					{"name": "my-attr", "targetId": "container-1"},
				},
			},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
		{
			name: "delete non-existent attribute returns empty list",
			input: map[string]any{
				"attributes": []map[string]any{
					{"name": "nonexistent", "targetId": "x"},
				},
			},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name: "empty attributes list",
			input: map[string]any{
				"attributes": []map[string]any{},
			},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

			for i := range tt.seedAttrs {
				backend.AddAttributeInternal("default", &tt.seedAttrs[i])
			}

			h := ecs.NewHandler(backend)
			rec := doECSRequest(t, h, "DeleteAttributes", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			attrs, ok := resp["attributes"].([]any)
			require.True(t, ok)
			assert.Len(t, attrs, tt.wantCount)
		})
	}
}

// ----- DeleteTaskDefinitions tests -----

func TestECS_DeleteTaskDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input            map[string]any
		setupFn          func(h *ecs.Handler) string
		name             string
		wantDeletedCount int
		wantFailureCount int
		wantCode         int
	}{
		{
			name: "delete INACTIVE task definition succeeds",
			setupFn: func(h *ecs.Handler) string {
				rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
					"family":               "my-family",
					"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))
				arn := r["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

				doECSRequest(
					t,
					h,
					"DeregisterTaskDefinition",
					map[string]any{"taskDefinition": arn},
				)

				return arn
			},
			wantCode:         http.StatusOK,
			wantDeletedCount: 1,
			wantFailureCount: 0,
		},
		{
			name: "delete ACTIVE task definition returns failure",
			setupFn: func(h *ecs.Handler) string {
				rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
					"family":               "active-family",
					"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))

				return r["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)
			},
			wantCode:         http.StatusOK,
			wantDeletedCount: 0,
			wantFailureCount: 1,
		},
		{
			name:     "missing task definitions returns 400",
			input:    map[string]any{"taskDefinitions": []string{}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "non-existent ARN becomes failure",
			setupFn: func(_ *ecs.Handler) string {
				return "nonexistent-arn"
			},
			wantCode:         http.StatusOK,
			wantDeletedCount: 0,
			wantFailureCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var input map[string]any

			if tt.setupFn != nil {
				arn := tt.setupFn(h)
				input = map[string]any{"taskDefinitions": []string{arn}}
			} else {
				input = tt.input
			}

			rec := doECSRequest(t, h, "DeleteTaskDefinitions", input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			deleted := resp["taskDefinitions"].([]any)
			failures := resp["failures"].([]any)

			assert.Len(t, deleted, tt.wantDeletedCount)
			assert.Len(t, failures, tt.wantFailureCount)
		})
	}
}

// ----- DescribeServiceDeployments tests -----

func TestECS_DescribeServiceDeployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input            map[string]any
		name             string
		seedDeployments  []ecs.ServiceDeployment
		wantDeployCount  int
		wantFailureCount int
		wantCode         int
	}{
		{
			name:             "describe non-existent returns failure",
			input:            map[string]any{"serviceDeploymentArns": []string{"nonexistent-arn"}},
			wantCode:         http.StatusOK,
			wantDeployCount:  0,
			wantFailureCount: 1,
		},
		{
			name: "describe existing deployment",
			seedDeployments: []ecs.ServiceDeployment{
				{
					ServiceDeploymentArn: "arn:aws:ecs:us-east-1:000000000000:service-deployment/cluster/svc/deploy-1",
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/cluster",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/cluster/svc",
					Status:               "SUCCESSFUL",
				},
			},
			input: map[string]any{
				"serviceDeploymentArns": []string{
					"arn:aws:ecs:us-east-1:000000000000:service-deployment/cluster/svc/deploy-1",
				},
			},
			wantCode:         http.StatusOK,
			wantDeployCount:  1,
			wantFailureCount: 0,
		},
		{
			name: "mixed found and not found",
			seedDeployments: []ecs.ServiceDeployment{
				{
					ServiceDeploymentArn: "arn:aws:ecs:us-east-1:000000000000:service-deployment/cluster/svc/deploy-2",
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/cluster",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/cluster/svc",
					Status:               "IN_PROGRESS",
				},
			},
			input: map[string]any{
				"serviceDeploymentArns": []string{
					"arn:aws:ecs:us-east-1:000000000000:service-deployment/cluster/svc/deploy-2",
					"nonexistent",
				},
			},
			wantCode:         http.StatusOK,
			wantDeployCount:  1,
			wantFailureCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

			for i := range tt.seedDeployments {
				backend.AddServiceDeploymentInternal(&tt.seedDeployments[i])
			}

			h := ecs.NewHandler(backend)
			rec := doECSRequest(t, h, "DescribeServiceDeployments", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			deployments := resp["serviceDeployments"].([]any)
			failures := resp["failures"].([]any)

			assert.Len(t, deployments, tt.wantDeployCount)
			assert.Len(t, failures, tt.wantFailureCount)
		})
	}
}

// ----- ExpressGatewayService tests -----

func TestECS_CreateExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "creates express gateway service",
			input: map[string]any{
				"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
				"serviceName":           "my-express-svc",
			},
			wantCode: http.StatusOK,
			wantName: "my-express-svc",
		},
		{
			name: "missing executionRoleArn returns 400",
			input: map[string]any{
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing infrastructureRoleArn returns 400",
			input: map[string]any{
				"executionRoleArn": "arn:aws:iam::000000000000:role/exec-role",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with tags",
			input: map[string]any{
				"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
				"serviceName":           "tagged-svc",
				"tags":                  []map[string]any{{"key": "env", "value": "prod"}},
			},
			wantCode: http.StatusOK,
			wantName: "tagged-svc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "CreateExpressGatewayService", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantName, svc["serviceName"])
			assert.Equal(t, "ACTIVE", svc["status"])
			assert.NotEmpty(t, svc["serviceArn"])
		})
	}
}

func TestECS_CreateExpressGatewayService_DuplicateARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	input := map[string]any{
		"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
		"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
		"serviceName":           "my-svc",
	}

	doECSRequest(t, h, "CreateExpressGatewayService", input)
	rec := doECSRequest(t, h, "CreateExpressGatewayService", input)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AlreadyExists")
}

func TestECS_DeleteExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createFn func(h *ecs.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "delete existing service",
			createFn: func(h *ecs.Handler) string {
				rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
					"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
					"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
					"serviceName":           "svc-to-delete",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))

				return r["service"].(map[string]any)["serviceArn"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "delete non-existent returns 400",
			createFn: func(_ *ecs.Handler) string {
				return "arn:nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty serviceArn returns 400",
			createFn: func(_ *ecs.Handler) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serviceArn := tt.createFn(h)

			rec := doECSRequest(
				t,
				h,
				"DeleteExpressGatewayService",
				map[string]any{"serviceArn": serviceArn},
			)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, serviceArn, svc["serviceArn"])
		})
	}
}

func TestECS_DescribeExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createFn func(h *ecs.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "describe existing service",
			createFn: func(h *ecs.Handler) string {
				rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
					"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
					"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
					"serviceName":           "my-express",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))

				return r["service"].(map[string]any)["serviceArn"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "describe non-existent returns 400",
			createFn: func(_ *ecs.Handler) string {
				return "arn:nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serviceArn := tt.createFn(h)

			rec := doECSRequest(
				t,
				h,
				"DescribeExpressGatewayService",
				map[string]any{"serviceArn": serviceArn},
			)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, serviceArn, svc["serviceArn"])
			assert.Equal(t, "ACTIVE", svc["status"])
		})
	}
}

func TestECS_ExpressGatewayService_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
		"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
		"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
		"serviceName":           "rt-svc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	arn := createResp["service"].(map[string]any)["serviceArn"].(string)

	// Describe
	rec = doECSRequest(t, h, "DescribeExpressGatewayService", map[string]any{"serviceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doECSRequest(t, h, "DeleteExpressGatewayService", map[string]any{"serviceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete returns 400
	rec = doECSRequest(t, h, "DescribeExpressGatewayService", map[string]any{"serviceArn": arn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_NewOps_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"CreateCapacityProvider",
		"DeleteCapacityProvider",
		"DescribeCapacityProviders",
		"DeleteAccountSetting",
		"DeleteAttributes",
		"DeleteTaskDefinitions",
		"DescribeServiceDeployments",
		"CreateExpressGatewayService",
		"DeleteExpressGatewayService",
		"DescribeExpressGatewayService",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op, "expected %s in supported operations", op)
	}
}

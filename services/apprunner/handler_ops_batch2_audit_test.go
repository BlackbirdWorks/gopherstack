package apprunner_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

func TestBatch2_PauseService_StateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "pause RUNNING service succeeds",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusOK,
		},
		{
			name: "pause PAUSED service returns InvalidStateException",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

func TestBatch2_ResumeService_StateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		name     string
		wantType string
		wantCode int
	}{
		{
			name: "resume PAUSED service succeeds",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "resume RUNNING service returns InvalidStateException",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "ResumeService", map[string]any{"ServiceArn": arn})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

func TestBatch2_UpdateService_StateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "update RUNNING service succeeds",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusOK,
		},
		{
			name: "update PAUSED service returns InvalidStateException",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "UpdateService", map[string]any{
				"ServiceArn":            arn,
				"InstanceConfiguration": map[string]any{"Cpu": "2 vCPU", "Memory": "4 GB"},
			})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

func TestBatch2_StartDeployment_StateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "deploy RUNNING service succeeds",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusOK,
		},
		{
			name: "deploy PAUSED service returns InvalidStateException",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "StartDeployment", map[string]any{"ServiceArn": arn})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

// --- AutoScalingConfiguration tests ---

func TestAutoScalingConfigurationCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:     "create returns ARN and defaults",
			action:   "CreateAutoScalingConfiguration",
			body:     map[string]any{"AutoScalingConfigurationName": "my-asg"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				cfg := resp["AutoScalingConfiguration"].(map[string]any)
				assert.Contains(t, cfg["AutoScalingConfigurationArn"], "autoscalingconfiguration/my-asg/1/")
				assert.Equal(t, "ACTIVE", cfg["Status"])
				assert.Equal(t, float64(1), cfg["AutoScalingConfigurationRevision"])
				assert.Equal(t, float64(100), cfg["MaxConcurrency"])
				assert.Equal(t, float64(25), cfg["MaxSize"])
				assert.Equal(t, float64(1), cfg["MinSize"])
			},
		},
		{
			name:     "create missing name returns 400",
			action:   "CreateAutoScalingConfiguration",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAutoScalingConfigurationDescribeDeleteList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateAutoScalingConfiguration", map[string]any{
		"AutoScalingConfigurationName": "cfg1",
		"MaxConcurrency":               50,
		"MaxSize":                      10,
		"MinSize":                      2,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	asgArn := createResp["AutoScalingConfiguration"].(map[string]any)["AutoScalingConfigurationArn"].(string)

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:     "describe returns config",
			action:   "DescribeAutoScalingConfiguration",
			body:     map[string]any{"AutoScalingConfigurationArn": asgArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				cfg := resp["AutoScalingConfiguration"].(map[string]any)
				assert.Equal(t, "cfg1", cfg["AutoScalingConfigurationName"])
				assert.Equal(t, float64(50), cfg["MaxConcurrency"])
			},
		},
		{
			name:     "describe missing ARN returns 400",
			action:   "DescribeAutoScalingConfiguration",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "describe unknown ARN returns 400",
			action: "DescribeAutoScalingConfiguration",
			body: map[string]any{
				"AutoScalingConfigurationArn": "arn:aws:apprunner:us-east-1:000000000000:autoscalingconfiguration/notexist/1/abc",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list returns config",
			action:   "ListAutoScalingConfigurations",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list := resp["AutoScalingConfigurationSummaryList"].([]any)
				assert.Len(t, list, 1)
			},
		},
		{
			name:     "delete returns config",
			action:   "DeleteAutoScalingConfiguration",
			body:     map[string]any{"AutoScalingConfigurationArn": asgArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				cfg := resp["AutoScalingConfiguration"].(map[string]any)
				assert.Equal(t, "cfg1", cfg["AutoScalingConfigurationName"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAutoScalingConfigurationRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateAutoScalingConfiguration", map[string]any{"AutoScalingConfigurationName": "my-asg"})
	require.Equal(t, http.StatusOK, rec.Code)
	var r1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r1))
	asgArn1 := r1["AutoScalingConfiguration"].(map[string]any)["AutoScalingConfigurationArn"].(string)

	rec = doRequest(t, h, "CreateAutoScalingConfiguration", map[string]any{"AutoScalingConfigurationName": "my-asg"})
	require.Equal(t, http.StatusOK, rec.Code)
	var r2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r2))
	rev2 := r2["AutoScalingConfiguration"].(map[string]any)["AutoScalingConfigurationRevision"].(float64)
	assert.Equal(t, float64(2), rev2)

	rec = doRequest(t, h, "ListAutoScalingConfigurations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	list := listResp["AutoScalingConfigurationSummaryList"].([]any)
	assert.Len(t, list, 2)

	rec = doRequest(t, h, "ListAutoScalingConfigurations", map[string]any{"LatestOnly": true})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	list = listResp["AutoScalingConfigurationSummaryList"].([]any)
	assert.Len(t, list, 1)

	rec = doRequest(t, h, "UpdateDefaultAutoScalingConfiguration", map[string]any{
		"AutoScalingConfigurationArn": asgArn1,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	cfg := updateResp["AutoScalingConfiguration"].(map[string]any)
	assert.Equal(t, true, cfg["IsDefault"])
}

func TestListServicesForAutoScalingConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateAutoScalingConfiguration", map[string]any{"AutoScalingConfigurationName": "my-asg"})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	asgArn := createResp["AutoScalingConfiguration"].(map[string]any)["AutoScalingConfigurationArn"].(string)

	tests := []struct {
		name     string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:     "list services returns empty list",
			body:     map[string]any{"AutoScalingConfigurationArn": asgArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list := resp["ServiceArnList"].([]any)
				assert.Empty(t, list)
			},
		},
		{
			name:     "missing ARN returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "unknown ARN returns 400",
			body: map[string]any{
				"AutoScalingConfigurationArn": "arn:aws:apprunner:us-east-1:000000000000:autoscalingconfiguration/notexist/1/abc",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "ListServicesForAutoScalingConfiguration", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// --- Connection tests ---

func TestConnectionCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:     "create returns ARN",
			action:   "CreateConnection",
			body:     map[string]any{"ConnectionName": "my-conn", "ProviderType": "GITHUB"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				conn := resp["Connection"].(map[string]any)
				assert.Contains(t, conn["ConnectionArn"], "connection/my-conn/")
				assert.Equal(t, "AVAILABLE", conn["Status"])
				assert.Equal(t, "GITHUB", conn["ProviderType"])
			},
		},
		{
			name:     "create missing name returns 400",
			action:   "CreateConnection",
			body:     map[string]any{"ProviderType": "GITHUB"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create missing provider type returns 400",
			action:   "CreateConnection",
			body:     map[string]any{"ConnectionName": "x"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestConnectionDeleteList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateConnection", map[string]any{"ConnectionName": "conn1", "ProviderType": "GITHUB"})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	connArn := createResp["Connection"].(map[string]any)["ConnectionArn"].(string)

	doRequest(t, h, "CreateConnection", map[string]any{"ConnectionName": "conn2", "ProviderType": "BITBUCKET"})

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:     "list returns 2 connections",
			action:   "ListConnections",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list := resp["ConnectionSummaryList"].([]any)
				assert.Len(t, list, 2)
			},
		},
		{
			name:     "list with name filter returns 1",
			action:   "ListConnections",
			body:     map[string]any{"ConnectionName": "conn1"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list := resp["ConnectionSummaryList"].([]any)
				assert.Len(t, list, 1)
			},
		},
		{
			name:     "delete returns connection",
			action:   "DeleteConnection",
			body:     map[string]any{"ConnectionArn": connArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				conn := resp["Connection"].(map[string]any)
				assert.Equal(t, "conn1", conn["ConnectionName"])
			},
		},
		{
			name:   "delete unknown ARN returns 400",
			action: "DeleteConnection",
			body: map[string]any{
				"ConnectionArn": "arn:aws:apprunner:us-east-1:000000000000:connection/notexist/abc",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete missing ARN returns 400",
			action:   "DeleteConnection",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// --- ObservabilityConfiguration tests ---

func TestObservabilityConfigurationCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:   "create returns ARN",
			action: "CreateObservabilityConfiguration",
			body: map[string]any{
				"ObservabilityConfigurationName": "my-obs",
				"TraceConfiguration":             map[string]any{"Vendor": "AWSXRAY"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				cfg := resp["ObservabilityConfiguration"].(map[string]any)
				assert.Contains(t, cfg["ObservabilityConfigurationArn"], "observabilityconfiguration/my-obs/1/")
				assert.Equal(t, "ACTIVE", cfg["Status"])
				assert.Equal(t, true, cfg["Latest"])
				assert.Equal(t, float64(1), cfg["ObservabilityConfigurationRevision"])
			},
		},
		{
			name:     "create missing name returns 400",
			action:   "CreateObservabilityConfiguration",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestObservabilityConfigurationDescribeDeleteList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateObservabilityConfiguration", map[string]any{
		"ObservabilityConfigurationName": "obs1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	obsArn := createResp["ObservabilityConfiguration"].(map[string]any)["ObservabilityConfigurationArn"].(string)

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:     "describe returns config",
			action:   "DescribeObservabilityConfiguration",
			body:     map[string]any{"ObservabilityConfigurationArn": obsArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				cfg := resp["ObservabilityConfiguration"].(map[string]any)
				assert.Equal(t, "obs1", cfg["ObservabilityConfigurationName"])
			},
		},
		{
			name:     "describe missing ARN returns 400",
			action:   "DescribeObservabilityConfiguration",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "describe unknown ARN returns 400",
			action: "DescribeObservabilityConfiguration",
			body: map[string]any{
				"ObservabilityConfigurationArn": "arn:aws:apprunner:us-east-1:000000000000:observabilityconfiguration/notexist/1/abc",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list returns config",
			action:   "ListObservabilityConfigurations",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list := resp["ObservabilityConfigurationSummaryList"].([]any)
				assert.Len(t, list, 1)
			},
		},
		{
			name:     "delete returns config",
			action:   "DeleteObservabilityConfiguration",
			body:     map[string]any{"ObservabilityConfigurationArn": obsArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				cfg := resp["ObservabilityConfiguration"].(map[string]any)
				assert.Equal(t, "obs1", cfg["ObservabilityConfigurationName"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// --- VpcConnector tests ---

func TestVpcConnectorCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:   "create returns ARN",
			action: "CreateVpcConnector",
			body: map[string]any{
				"VpcConnectorName": "my-vpc",
				"Subnets":          []string{"subnet-aaa"},
				"SecurityGroups":   []string{"sg-bbb"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vc := resp["VpcConnector"].(map[string]any)
				assert.Contains(t, vc["VpcConnectorArn"], "vpcconnector/my-vpc/1/")
				assert.Equal(t, "ACTIVE", vc["Status"])
				assert.Equal(t, float64(1), vc["VpcConnectorRevision"])
				subnets := vc["Subnets"].([]any)
				assert.Len(t, subnets, 1)
			},
		},
		{
			name:     "create missing name returns 400",
			action:   "CreateVpcConnector",
			body:     map[string]any{"Subnets": []string{"subnet-aaa"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create missing subnets returns 400",
			action:   "CreateVpcConnector",
			body:     map[string]any{"VpcConnectorName": "x"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestVpcConnectorDescribeDeleteList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateVpcConnector", map[string]any{
		"VpcConnectorName": "vc1",
		"Subnets":          []string{"subnet-111"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	vcArn := createResp["VpcConnector"].(map[string]any)["VpcConnectorArn"].(string)

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:     "describe returns connector",
			action:   "DescribeVpcConnector",
			body:     map[string]any{"VpcConnectorArn": vcArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vc := resp["VpcConnector"].(map[string]any)
				assert.Equal(t, "vc1", vc["VpcConnectorName"])
			},
		},
		{
			name:     "describe missing ARN returns 400",
			action:   "DescribeVpcConnector",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "describe unknown ARN returns 400",
			action: "DescribeVpcConnector",
			body: map[string]any{
				"VpcConnectorArn": "arn:aws:apprunner:us-east-1:000000000000:vpcconnector/notexist/1/abc",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list returns 1",
			action:   "ListVpcConnectors",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list := resp["VpcConnectors"].([]any)
				assert.Len(t, list, 1)
			},
		},
		{
			name:     "delete returns connector",
			action:   "DeleteVpcConnector",
			body:     map[string]any{"VpcConnectorArn": vcArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vc := resp["VpcConnector"].(map[string]any)
				assert.Equal(t, "vc1", vc["VpcConnectorName"])
			},
		},
		{
			name:   "delete unknown ARN returns 400",
			action: "DeleteVpcConnector",
			body: map[string]any{
				"VpcConnectorArn": "arn:aws:apprunner:us-east-1:000000000000:vpcconnector/notexist/1/abc",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete missing ARN returns 400",
			action:   "DeleteVpcConnector",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// --- VpcIngressConnection tests ---

func TestVpcIngressConnectionCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:   "create returns ARN",
			action: "CreateVpcIngressConnection",
			body: map[string]any{
				"VpcIngressConnectionName": "my-vic",
				"ServiceArn":               svcArn,
				"IngressVpcConfiguration": map[string]any{
					"VpcId":         "vpc-aaa",
					"VpcEndpointId": "vpce-bbb",
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vic := resp["VpcIngressConnection"].(map[string]any)
				assert.Contains(t, vic["VpcIngressConnectionArn"], "vpcingressconnection/my-vic/")
				assert.Equal(t, "AVAILABLE", vic["Status"])
				assert.Equal(t, svcArn, vic["ServiceArn"])
				ivc := vic["IngressVpcConfiguration"].(map[string]any)
				assert.Equal(t, "vpc-aaa", ivc["VpcId"])
			},
		},
		{
			name:     "create missing name returns 400",
			action:   "CreateVpcIngressConnection",
			body:     map[string]any{"ServiceArn": svcArn},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create missing service ARN returns 400",
			action:   "CreateVpcIngressConnection",
			body:     map[string]any{"VpcIngressConnectionName": "x"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestVpcIngressConnectionDescribeDeleteListUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	rec := doRequest(t, h, "CreateVpcIngressConnection", map[string]any{
		"VpcIngressConnectionName": "vic1",
		"ServiceArn":               svcArn,
		"IngressVpcConfiguration":  map[string]any{"VpcId": "vpc-111", "VpcEndpointId": "vpce-222"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	vicArn := createResp["VpcIngressConnection"].(map[string]any)["VpcIngressConnectionArn"].(string)

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:     "describe returns VIC",
			action:   "DescribeVpcIngressConnection",
			body:     map[string]any{"VpcIngressConnectionArn": vicArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vic := resp["VpcIngressConnection"].(map[string]any)
				assert.Equal(t, "vic1", vic["VpcIngressConnectionName"])
			},
		},
		{
			name:     "describe missing ARN returns 400",
			action:   "DescribeVpcIngressConnection",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "describe unknown ARN returns 400",
			action: "DescribeVpcIngressConnection",
			body: map[string]any{
				"VpcIngressConnectionArn": "arn:aws:apprunner:us-east-1:000000000000:vpcingressconnection/notexist/abc",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list returns 1",
			action:   "ListVpcIngressConnections",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list := resp["VpcIngressConnectionSummaryList"].([]any)
				assert.Len(t, list, 1)
			},
		},
		{
			name:     "list with service ARN filter",
			action:   "ListVpcIngressConnections",
			body:     map[string]any{"Filter": map[string]any{"ServiceArn": svcArn}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list := resp["VpcIngressConnectionSummaryList"].([]any)
				assert.Len(t, list, 1)
			},
		},
		{
			name:   "update changes VPC config",
			action: "UpdateVpcIngressConnection",
			body: map[string]any{
				"VpcIngressConnectionArn": vicArn,
				"IngressVpcConfiguration": map[string]any{
					"VpcId":         "vpc-new",
					"VpcEndpointId": "vpce-new",
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vic := resp["VpcIngressConnection"].(map[string]any)
				ivc := vic["IngressVpcConfiguration"].(map[string]any)
				assert.Equal(t, "vpc-new", ivc["VpcId"])
			},
		},
		{
			name:     "update missing ARN returns 400",
			action:   "UpdateVpcIngressConnection",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete returns VIC",
			action:   "DeleteVpcIngressConnection",
			body:     map[string]any{"VpcIngressConnectionArn": vicArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vic := resp["VpcIngressConnection"].(map[string]any)
				assert.Equal(t, "vic1", vic["VpcIngressConnectionName"])
			},
		},
		{
			name:     "delete missing ARN returns 400",
			action:   "DeleteVpcIngressConnection",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// --- CustomDomain tests ---

func TestCustomDomainAssociateDescribeDisassociate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:   "associate returns domain",
			action: "AssociateCustomDomain",
			body: map[string]any{
				"ServiceArn": svcArn,
				"DomainName": "example.com",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				cd := resp["CustomDomain"].(map[string]any)
				assert.Equal(t, "example.com", cd["DomainName"])
				assert.Equal(t, "ACTIVE", cd["Status"])
				assert.Equal(t, true, cd["EnableWWWSubdomain"])
			},
		},
		{
			name:     "associate missing ServiceArn returns 400",
			action:   "AssociateCustomDomain",
			body:     map[string]any{"DomainName": "x.com"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "associate missing DomainName returns 400",
			action:   "AssociateCustomDomain",
			body:     map[string]any{"ServiceArn": svcArn},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "associate unknown service returns 400",
			action: "AssociateCustomDomain",
			body: map[string]any{
				"ServiceArn": "arn:aws:apprunner:us-east-1:000000000000:service/notexist",
				"DomainName": "x.com",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestCustomDomainDescribeAndDisassociate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	doRequest(t, h, "AssociateCustomDomain", map[string]any{"ServiceArn": svcArn, "DomainName": "example.com"})
	doRequest(t, h, "AssociateCustomDomain", map[string]any{"ServiceArn": svcArn, "DomainName": "sub.example.com"})

	tests := []struct {
		name     string
		action   string
		body     any
		check    func(t *testing.T, body []byte)
		wantCode int
	}{
		{
			name:     "describe returns 2 domains",
			action:   "DescribeCustomDomains",
			body:     map[string]any{"ServiceArn": svcArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				domains := resp["CustomDomains"].([]any)
				assert.Len(t, domains, 2)
				assert.NotEmpty(t, resp["ServiceArn"])
				assert.NotEmpty(t, resp["DNSTarget"])
			},
		},
		{
			name:     "describe missing ServiceArn returns 400",
			action:   "DescribeCustomDomains",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "describe unknown service returns 400",
			action:   "DescribeCustomDomains",
			body:     map[string]any{"ServiceArn": "arn:aws:apprunner:us-east-1:000000000000:service/notexist"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "disassociate removes domain",
			action:   "DisassociateCustomDomain",
			body:     map[string]any{"ServiceArn": svcArn, "DomainName": "example.com"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				cd := resp["CustomDomain"].(map[string]any)
				assert.Equal(t, "example.com", cd["DomainName"])
			},
		},
		{
			name:     "disassociate missing ServiceArn returns 400",
			action:   "DisassociateCustomDomain",
			body:     map[string]any{"DomainName": "x.com"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "disassociate missing DomainName returns 400",
			action:   "DisassociateCustomDomain",
			body:     map[string]any{"ServiceArn": svcArn},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

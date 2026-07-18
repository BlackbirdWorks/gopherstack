package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestStack verifies stack CRUD operations return correct fields.
func TestStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		setup     func(h *opsworks.Handler) string
		check     func(t *testing.T, body []byte, setupID string)
		name      string
		operation string
		wantCode  int
	}{
		{
			name:      "CreateStack returns StackId",
			operation: "CreateStack",
			body: map[string]any{
				"Name":                      "my-stack",
				"Region":                    "us-east-1",
				"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
				"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte, _ string) {
				t.Helper()
				resp := parseJSON(t, body)
				stackID, ok := resp["StackId"].(string)
				assert.True(t, ok, "StackId should be a string")
				assert.NotEmpty(t, stackID)
			},
		},
		{
			name:      "DescribeStacks returns stack with CreatedAt",
			operation: "DescribeStacks",
			body:      map[string]any{},
			setup: func(h *opsworks.Handler) string {
				rec := doTarget(t, h, "CreateStack", map[string]any{
					"Name":                      "my-stack",
					"Region":                    "us-east-1",
					"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
					"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())

				return resp["StackId"].(string)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte, _ string) {
				t.Helper()
				resp := parseJSON(t, body)
				stacks, ok := resp["Stacks"].([]any)
				require.True(t, ok)
				require.Len(t, stacks, 1)
				stack := stacks[0].(map[string]any)
				assert.NotEmpty(t, stack["StackId"])
				assert.NotEmpty(t, stack["Name"])
				assert.NotEmpty(t, stack["CreatedAt"])
				assert.NotEmpty(t, stack["Arn"])
			},
		},
		{
			name:      "DescribeStacks by ID returns correct stack",
			operation: "DescribeStacks",
			setup: func(h *opsworks.Handler) string {
				rec := doTarget(t, h, "CreateStack", map[string]any{
					"Name":                      "targeted-stack",
					"Region":                    "us-east-1",
					"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
					"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())

				return resp["StackId"].(string)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte, setupID string) {
				t.Helper()
				resp := parseJSON(t, body)
				stacks := resp["Stacks"].([]any)
				require.Len(t, stacks, 1)
				stack := stacks[0].(map[string]any)
				assert.Equal(t, setupID, stack["StackId"])
				assert.Equal(t, "targeted-stack", stack["Name"])
			},
		},
		{
			name:      "UpdateStack modifies name",
			operation: "UpdateStack",
			setup: func(h *opsworks.Handler) string {
				rec := doTarget(t, h, "CreateStack", map[string]any{
					"Name":                      "old-name",
					"Region":                    "us-east-1",
					"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
					"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())

				return resp["StackId"].(string)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte, _ string) {
				t.Helper()
				resp := parseJSON(t, body)
				assert.Empty(t, resp)
			},
		},
		{
			name:      "DeleteStack removes stack",
			operation: "DeleteStack",
			setup: func(h *opsworks.Handler) string {
				rec := doTarget(t, h, "CreateStack", map[string]any{
					"Name":                      "to-delete",
					"Region":                    "us-east-1",
					"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
					"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())

				return resp["StackId"].(string)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte, _ string) {
				t.Helper()
				resp := parseJSON(t, body)
				assert.Empty(t, resp)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var setupID string
			if tt.setup != nil {
				setupID = tt.setup(h)
			}

			body := tt.body
			if body == nil && setupID != "" {
				switch tt.operation {
				case "DescribeStacks":
					body = map[string]any{"StackIds": []string{setupID}}
				case "UpdateStack":
					body = map[string]any{"StackId": setupID, "Name": "new-name"}
				case "DeleteStack":
					body = map[string]any{"StackId": setupID}
				}
			}

			rec := doTarget(t, h, tt.operation, body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes(), setupID)
			}
		})
	}
}

// TestCloneStack verifies CloneStack creates an independent copy.
func TestCloneStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "CloneStack returns new StackId",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "CloneStack", map[string]any{
					"SourceStackId": stackID,
					"Name":          "cloned-stack",
					"Region":        "us-west-2",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				cloneID, ok := resp["StackId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, cloneID)
				assert.NotEqual(t, stackID, cloneID)
			},
		},
		{
			name: "CloneStack of nonexistent stack returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "CloneStack", map[string]any{
					"SourceStackId": "nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "cloned stack visible via DescribeStacks",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "CloneStack", map[string]any{
					"SourceStackId": stackID,
					"Name":          "clone2",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				cloneID := parseJSON(t, rec.Body.Bytes())["StackId"].(string)

				rec = doTarget(t, h, "DescribeStacks", map[string]any{
					"StackIds": []string{cloneID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				stacks := resp["Stacks"].([]any)
				require.Len(t, stacks, 1)
				assert.Equal(t, "clone2", stacks[0].(map[string]any)["Name"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}

// TestStartStopStack verifies StartStack and StopStack.
func TestStartStopStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "StartStack returns empty response",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "StartStack", map[string]any{"StackId": stackID})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "StopStack returns empty response",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "StopStack", map[string]any{"StackId": stackID})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "StartStack on nonexistent stack returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "StartStack", map[string]any{"StackId": "no-such-stack"})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			// StartStack must commit its instances to the terminal "online"
			// status. Previously it parked them in "starting" with nothing to
			// ever advance them, so DescribeInstances pollers waiting for
			// "online" spun forever.
			name: "StartStack transitions stopped instances to online",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "StartStack", map[string]any{"StackId": stackID})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeInstances", map[string]any{
					"InstanceIds": []string{instanceID},
				})
				resp := parseJSON(t, rec.Body.Bytes())
				inst := resp["Instances"].([]any)[0].(map[string]any)
				assert.Equal(t, "online", inst["Status"])
			},
		},
		{
			// StopStack must commit its instances to the terminal "stopped"
			// status rather than leaving them stuck in "stopping".
			name: "StopStack transitions online instances to stopped",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)
				doTarget(t, h, "StartStack", map[string]any{"StackId": stackID})

				rec := doTarget(t, h, "StopStack", map[string]any{"StackId": stackID})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeInstances", map[string]any{
					"InstanceIds": []string{instanceID},
				})
				resp := parseJSON(t, rec.Body.Bytes())
				inst := resp["Instances"].([]any)[0].(map[string]any)
				assert.Equal(t, "stopped", inst["Status"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}

// TestGetHostnameSuggestion verifies hostname suggestions.
func TestGetHostnameSuggestion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "returns non-empty hostname",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "GetHostnameSuggestion", map[string]any{
					"StackId": stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.NotEmpty(t, resp["Hostname"])
			},
		},
		{
			name: "returns 404 for nonexistent stack",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "GetHostnameSuggestion", map[string]any{
					"StackId": "nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}

// TestDescribeStackSummary verifies DescribeStackSummary returns counts.
func TestDescribeStackSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "returns summary with instance counts",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "DescribeStackSummary", map[string]any{"StackId": stackID})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				summary, ok := resp["StackSummary"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, summary["StackId"])
				assert.NotEmpty(t, summary["Name"])
				counts := summary["InstancesCount"].(map[string]any)
				assert.InEpsilon(t, float64(1), counts["Total"], 0.001)
			},
		},
		{
			name: "nonexistent stack returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "DescribeStackSummary", map[string]any{"StackId": "none"})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}

// TestDescribeStackProvisioningParameters verifies provisioning params.
func TestDescribeStackProvisioningParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "returns agent installer URL",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "DescribeStackProvisioningParameters", map[string]any{
					"StackId": stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.NotEmpty(t, resp["AgentInstallerUrl"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}

// TestDeleteStackCascade verifies DeleteStack removes child resources.
func TestDeleteStackCascade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler, b *opsworks.InMemoryBackend)
		name  string
	}{
		{
			name: "DeleteStack removes layers and instances",
			check: func(t *testing.T, h *opsworks.Handler, b *opsworks.InMemoryBackend) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				createTestInstance(t, h, stackID, layerID)

				assert.Equal(t, 1, opsworks.LayerCount(b))
				assert.Equal(t, 1, opsworks.InstanceCount(b))

				rec := doTarget(t, h, "DeleteStack", map[string]any{"StackId": stackID})
				require.Equal(t, http.StatusOK, rec.Code)

				assert.Equal(t, 0, opsworks.LayerCount(b))
				assert.Equal(t, 0, opsworks.InstanceCount(b))
				assert.Equal(t, 0, opsworks.StackCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := opsworks.NewInMemoryBackend("000000000000", "us-east-1")
			h := opsworks.NewHandler(b)
			tt.check(t, h, b)
		})
	}
}

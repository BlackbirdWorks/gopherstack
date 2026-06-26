package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// helpers shared across new-ops tests

func createTestStack(t *testing.T, h *opsworks.Handler) string {
	t.Helper()
	rec := doTarget(t, h, "CreateStack", map[string]any{
		"Name":                      "test-stack",
		"Region":                    "us-east-1",
		"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
		"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return parseJSON(t, rec.Body.Bytes())["StackId"].(string)
}

func createTestLayer(t *testing.T, h *opsworks.Handler, stackID string) string {
	t.Helper()
	rec := doTarget(t, h, "CreateLayer", map[string]any{
		"StackId":   stackID,
		"Type":      "custom",
		"Name":      "test-layer",
		"Shortname": "tl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return parseJSON(t, rec.Body.Bytes())["LayerId"].(string)
}

func createTestInstance(t *testing.T, h *opsworks.Handler, stackID, layerID string) string {
	t.Helper()
	rec := doTarget(t, h, "CreateInstance", map[string]any{
		"StackId":      stackID,
		"LayerIds":     []string{layerID},
		"InstanceType": "t2.micro",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return parseJSON(t, rec.Body.Bytes())["InstanceId"].(string)
}

// TestParity_CloneStack verifies CloneStack creates an independent copy.
func TestParity_CloneStack(t *testing.T) {
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

// TestParity_StartStopStack verifies StartStack and StopStack.
func TestParity_StartStopStack(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}

// TestParity_GetHostnameSuggestion verifies hostname suggestions.
func TestParity_GetHostnameSuggestion(t *testing.T) {
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

// TestParity_DescribeStackSummary verifies DescribeStackSummary returns counts.
func TestParity_DescribeStackSummary(t *testing.T) {
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

// TestParity_DescribeStackProvisioningParameters verifies provisioning params.
func TestParity_DescribeStackProvisioningParameters(t *testing.T) {
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

// TestParity_DeleteStackCascade verifies DeleteStack removes child resources.
func TestParity_DeleteStackCascade(t *testing.T) {
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

// TestParity_DescribeCommands_CorrectFieldName verifies CommandIds (not CommandIDs) works.
func TestParity_DescribeCommands_CorrectFieldName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "DescribeCommands with CommandIds field filters correctly",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "CreateApp", map[string]any{
					"StackId": stackID,
					"Name":    "app",
					"Type":    "other",
				})
				appID := parseJSON(t, rec.Body.Bytes())["AppId"].(string)

				rec = doTarget(t, h, "CreateDeployment", map[string]any{
					"StackId": stackID,
					"AppId":   appID,
					"Command": map[string]any{"Name": "deploy"},
				})
				deploymentID := parseJSON(t, rec.Body.Bytes())["DeploymentId"].(string)

				// Get commands for the deployment to find a command ID.
				rec = doTarget(t, h, "DescribeCommands", map[string]any{
					"DeploymentId": deploymentID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				commands := parseJSON(t, rec.Body.Bytes())["Commands"].([]any)
				require.Len(t, commands, 1)
				cmdID := commands[0].(map[string]any)["CommandId"].(string)

				// Now filter by CommandIds (correct AWS field name).
				rec = doTarget(t, h, "DescribeCommands", map[string]any{
					"CommandIds": []string{cmdID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				filtered := parseJSON(t, rec.Body.Bytes())["Commands"].([]any)
				require.Len(t, filtered, 1)
				assert.Equal(t, cmdID, filtered[0].(map[string]any)["CommandId"])
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

// TestParity_RegisterDeregisterInstance verifies instance registration lifecycle.
func TestParity_RegisterDeregisterInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "RegisterInstance returns InstanceId",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterInstance", map[string]any{
					"StackId":  stackID,
					"Hostname": "my-server",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.NotEmpty(t, resp["InstanceId"])
			},
		},
		{
			name: "DeregisterInstance removes instance",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterInstance", map[string]any{
					"StackId":  stackID,
					"Hostname": "to-deregister",
				})
				instanceID := parseJSON(t, rec.Body.Bytes())["InstanceId"].(string)

				rec = doTarget(t, h, "DeregisterInstance", map[string]any{
					"InstanceId": instanceID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeInstances", map[string]any{
					"InstanceIds": []string{instanceID},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "RegisterInstance on nonexistent stack returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "RegisterInstance", map[string]any{
					"StackId":  "nonexistent",
					"Hostname": "host",
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

// TestParity_AssignUnassignInstance verifies AssignInstance/UnassignInstance.
func TestParity_AssignUnassignInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "AssignInstance assigns to layer",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, "")

				rec := doTarget(t, h, "AssignInstance", map[string]any{
					"InstanceId": instanceID,
					"LayerIds":   []string{layerID},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "UnassignInstance returns OK",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "UnassignInstance", map[string]any{
					"InstanceId": instanceID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
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

// TestParity_UserProfiles verifies user profile CRUD.
func TestParity_UserProfiles(t *testing.T) {
	t.Parallel()

	const testArn = "arn:aws:iam::000000000000:user/test-user"

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "CreateUserProfile returns IAM ARN",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "CreateUserProfile", map[string]any{
					"IamUserArn":  testArn,
					"SshUsername": "testuser",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Equal(t, testArn, resp["IamUserArn"])
			},
		},
		{
			name: "DescribeUserProfiles returns created profile",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				doTarget(t, h, "CreateUserProfile", map[string]any{
					"IamUserArn":  testArn,
					"SshUsername": "testuser",
				})
				rec := doTarget(t, h, "DescribeUserProfiles", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				profiles := resp["UserProfiles"].([]any)
				require.Len(t, profiles, 1)
				p := profiles[0].(map[string]any)
				assert.Equal(t, testArn, p["IamUserArn"])
				assert.Equal(t, "testuser", p["SshUsername"])
			},
		},
		{
			name: "UpdateUserProfile changes SSH username",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				doTarget(t, h, "CreateUserProfile", map[string]any{
					"IamUserArn":  testArn,
					"SshUsername": "oldname",
				})
				rec := doTarget(t, h, "UpdateUserProfile", map[string]any{
					"IamUserArn":  testArn,
					"SshUsername": "newname",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeUserProfiles", map[string]any{
					"IamUserArns": []string{testArn},
				})
				profiles := parseJSON(t, rec.Body.Bytes())["UserProfiles"].([]any)
				assert.Equal(t, "newname", profiles[0].(map[string]any)["SshUsername"])
			},
		},
		{
			name: "DeleteUserProfile removes profile",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				doTarget(t, h, "CreateUserProfile", map[string]any{
					"IamUserArn":  testArn,
					"SshUsername": "user",
				})
				rec := doTarget(t, h, "DeleteUserProfile", map[string]any{
					"IamUserArn": testArn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeUserProfiles", map[string]any{
					"IamUserArns": []string{testArn},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "DescribeMyUserProfile returns profile",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "DescribeMyUserProfile", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				profile := resp["UserProfile"].(map[string]any)
				assert.NotEmpty(t, profile["IamUserArn"])
			},
		},
		{
			name: "UpdateMyUserProfile returns OK",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "UpdateMyUserProfile", map[string]any{
					"SshPublicKey": "ssh-rsa AAAA test",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
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

// TestParity_ElasticLoadBalancers verifies ELB attach/detach/describe.
func TestParity_ElasticLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "AttachElasticLoadBalancer returns OK",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				rec := doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "my-elb",
					"LayerId":                 layerID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "DescribeElasticLoadBalancers returns attached ELB",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "my-elb",
					"LayerId":                 layerID,
				})

				rec := doTarget(t, h, "DescribeElasticLoadBalancers", map[string]any{
					"StackId": stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				elbs := resp["ElasticLoadBalancers"].([]any)
				require.Len(t, elbs, 1)
				elb := elbs[0].(map[string]any)
				assert.Equal(t, "my-elb", elb["ElasticLoadBalancerName"])
				assert.NotEmpty(t, elb["DnsName"])
			},
		},
		{
			name: "DetachElasticLoadBalancer removes ELB",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "my-elb",
					"LayerId":                 layerID,
				})
				rec := doTarget(t, h, "DetachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "my-elb",
					"LayerId":                 layerID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeElasticLoadBalancers", map[string]any{})
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Empty(t, resp["ElasticLoadBalancers"].([]any))
			},
		},
		{
			name: "AttachElasticLoadBalancer to nonexistent layer returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "elb",
					"LayerId":                 "nonexistent",
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

// TestParity_ElasticIps verifies elastic IP lifecycle operations.
func TestParity_ElasticIps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "RegisterElasticIp returns the IP",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "RegisterElasticIp", map[string]any{
					"ElasticIp": "1.2.3.4",
					"Region":    "us-east-1",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Equal(t, "1.2.3.4", resp["ElasticIp"])
			},
		},
		{
			name: "AssociateElasticIp links IP to instance",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "2.3.4.5"})
				rec := doTarget(t, h, "AssociateElasticIp", map[string]any{
					"ElasticIp":  "2.3.4.5",
					"InstanceId": instanceID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeElasticIps", map[string]any{
					"InstanceId": instanceID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				eips := parseJSON(t, rec.Body.Bytes())["ElasticIps"].([]any)
				require.Len(t, eips, 1)
				assert.Equal(t, "2.3.4.5", eips[0].(map[string]any)["Ip"])
			},
		},
		{
			name: "DisassociateElasticIp clears instance link",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "3.4.5.6"})
				doTarget(t, h, "AssociateElasticIp", map[string]any{
					"ElasticIp":  "3.4.5.6",
					"InstanceId": instanceID,
				})
				rec := doTarget(t, h, "DisassociateElasticIp", map[string]any{"ElasticIp": "3.4.5.6"})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "UpdateElasticIp changes name",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "4.5.6.7"})
				rec := doTarget(t, h, "UpdateElasticIp", map[string]any{
					"ElasticIp": "4.5.6.7",
					"Name":      "my-eip",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "DeregisterElasticIp removes registration",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "5.6.7.8"})
				rec := doTarget(t, h, "DeregisterElasticIp", map[string]any{"ElasticIp": "5.6.7.8"})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeElasticIps", map[string]any{
					"Ips": []string{"5.6.7.8"},
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

// TestParity_Volumes verifies volume registration lifecycle.
func TestParity_Volumes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "RegisterVolume returns VolumeId",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-1234",
					"StackId":     stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.NotEmpty(t, resp["VolumeId"])
			},
		},
		{
			name: "DescribeVolumes returns registered volume",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-5678",
					"StackId":     stackID,
				})
				volumeID := parseJSON(t, rec.Body.Bytes())["VolumeId"].(string)

				rec = doTarget(t, h, "DescribeVolumes", map[string]any{
					"VolumeIds": []string{volumeID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				vols := parseJSON(t, rec.Body.Bytes())["Volumes"].([]any)
				require.Len(t, vols, 1)
				vol := vols[0].(map[string]any)
				assert.Equal(t, "vol-5678", vol["Ec2VolumeId"])
			},
		},
		{
			name: "AssignVolume and UpdateVolume",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-abc",
					"StackId":     stackID,
				})
				volumeID := parseJSON(t, rec.Body.Bytes())["VolumeId"].(string)

				rec = doTarget(t, h, "AssignVolume", map[string]any{
					"VolumeId":   volumeID,
					"InstanceId": instanceID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "UpdateVolume", map[string]any{
					"VolumeId":   volumeID,
					"Name":       "my-volume",
					"MountPoint": "/data",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "UnassignVolume returns OK",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-def",
					"StackId":     stackID,
				})
				volumeID := parseJSON(t, rec.Body.Bytes())["VolumeId"].(string)
				doTarget(t, h, "AssignVolume", map[string]any{
					"VolumeId": volumeID, "InstanceId": instanceID,
				})

				rec = doTarget(t, h, "UnassignVolume", map[string]any{"VolumeId": volumeID})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "DeregisterVolume removes volume",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-ghi",
					"StackId":     stackID,
				})
				volumeID := parseJSON(t, rec.Body.Bytes())["VolumeId"].(string)

				rec = doTarget(t, h, "DeregisterVolume", map[string]any{"VolumeId": volumeID})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeVolumes", map[string]any{
					"VolumeIds": []string{volumeID},
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

// TestParity_RdsDbInstances verifies RDS DB instance registration.
func TestParity_RdsDbInstances(t *testing.T) {
	t.Parallel()

	const testArn = "arn:aws:rds:us-east-1:000000000000:db:mydb"

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "RegisterRdsDbInstance and DescribeRdsDbInstances",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterRdsDbInstance", map[string]any{
					"StackId":          stackID,
					"RdsDbInstanceArn": testArn,
					"DbUser":           "admin",
					"DbPassword":       "secret",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeRdsDbInstances", map[string]any{
					"StackId": stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				rdbs := parseJSON(t, rec.Body.Bytes())["RdsDbInstances"].([]any)
				require.Len(t, rdbs, 1)
				rdb := rdbs[0].(map[string]any)
				assert.Equal(t, testArn, rdb["RdsDbInstanceArn"])
				assert.Equal(t, "admin", rdb["DbUser"])
			},
		},
		{
			name: "UpdateRdsDbInstance changes DbUser",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				doTarget(t, h, "RegisterRdsDbInstance", map[string]any{
					"StackId":          stackID,
					"RdsDbInstanceArn": testArn,
					"DbUser":           "olduser",
				})
				rec := doTarget(t, h, "UpdateRdsDbInstance", map[string]any{
					"RdsDbInstanceArn": testArn,
					"DbUser":           "newuser",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeRdsDbInstances", map[string]any{
					"RdsDbInstanceArns": []string{testArn},
				})
				rdbs := parseJSON(t, rec.Body.Bytes())["RdsDbInstances"].([]any)
				assert.Equal(t, "newuser", rdbs[0].(map[string]any)["DbUser"])
			},
		},
		{
			name: "DeregisterRdsDbInstance removes instance",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				doTarget(t, h, "RegisterRdsDbInstance", map[string]any{
					"StackId":          stackID,
					"RdsDbInstanceArn": testArn,
					"DbUser":           "user",
				})
				rec := doTarget(t, h, "DeregisterRdsDbInstance", map[string]any{
					"RdsDbInstanceArn": testArn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeRdsDbInstances", map[string]any{
					"RdsDbInstanceArns": []string{testArn},
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

// TestParity_EcsClusters verifies ECS cluster registration.
func TestParity_EcsClusters(t *testing.T) {
	t.Parallel()

	const testClusterArn = "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster"

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "RegisterEcsCluster and DescribeEcsClusters",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterEcsCluster", map[string]any{
					"EcsClusterArn": testClusterArn,
					"StackId":       stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Equal(t, testClusterArn, resp["EcsClusterArn"])

				rec = doTarget(t, h, "DescribeEcsClusters", map[string]any{
					"StackId": stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				clusters := parseJSON(t, rec.Body.Bytes())["EcsClusters"].([]any)
				require.Len(t, clusters, 1)
				c := clusters[0].(map[string]any)
				assert.Equal(t, testClusterArn, c["EcsClusterArn"])
				assert.Equal(t, "my-cluster", c["EcsClusterName"])
			},
		},
		{
			name: "DeregisterEcsCluster removes cluster",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				doTarget(t, h, "RegisterEcsCluster", map[string]any{
					"EcsClusterArn": testClusterArn,
					"StackId":       stackID,
				})
				rec := doTarget(t, h, "DeregisterEcsCluster", map[string]any{
					"EcsClusterArn": testClusterArn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeEcsClusters", map[string]any{
					"EcsClusterArns": []string{testClusterArn},
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

// TestParity_Permissions verifies SetPermission and DescribePermissions.
func TestParity_Permissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "SetPermission and DescribePermissions round-trip",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				const arn = "arn:aws:iam::000000000000:user/dev"
				rec := doTarget(t, h, "SetPermission", map[string]any{
					"StackId":    stackID,
					"IamUserArn": arn,
					"Level":      "deploy",
					"AllowSsh":   true,
					"AllowSudo":  false,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribePermissions", map[string]any{
					"StackId":    stackID,
					"IamUserArn": arn,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				perms := parseJSON(t, rec.Body.Bytes())["Permissions"].([]any)
				require.Len(t, perms, 1)
				p := perms[0].(map[string]any)
				assert.Equal(t, "deploy", p["Level"])
				assert.Equal(t, true, p["AllowSsh"])
				assert.Equal(t, false, p["AllowSudo"])
			},
		},
		{
			name: "SetPermission on nonexistent stack returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "SetPermission", map[string]any{
					"StackId":    "none",
					"IamUserArn": "arn:aws:iam::000000000000:user/x",
					"Level":      "manage",
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

// TestParity_AutoScaling verifies time-based and load-based auto-scaling ops.
func TestParity_AutoScaling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "SetTimeBasedAutoScaling and DescribeTimeBasedAutoScaling",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "SetTimeBasedAutoScaling", map[string]any{
					"InstanceId": instanceID,
					"AutoScalingSchedule": map[string]any{
						"Monday": map[string]string{"1": "on"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeTimeBasedAutoScaling", map[string]any{
					"InstanceIds": []string{instanceID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				configs := parseJSON(t, rec.Body.Bytes())["TimeBasedAutoScalingConfigurations"].([]any)
				require.Len(t, configs, 1)
				assert.Equal(t, instanceID, configs[0].(map[string]any)["InstanceId"])
			},
		},
		{
			name: "SetLoadBasedAutoScaling and DescribeLoadBasedAutoScaling",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)

				rec := doTarget(t, h, "SetLoadBasedAutoScaling", map[string]any{
					"LayerId": layerID,
					"Enable":  true,
					"UpScaling": map[string]any{
						"CpuThreshold":  80.0,
						"InstanceCount": 2,
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeLoadBasedAutoScaling", map[string]any{
					"LayerIds": []string{layerID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				configs := parseJSON(t, rec.Body.Bytes())["LoadBasedAutoScalingConfigurations"].([]any)
				require.Len(t, configs, 1)
				c := configs[0].(map[string]any)
				assert.Equal(t, layerID, c["LayerId"])
				assert.Equal(t, true, c["Enable"])
			},
		},
		{
			name: "SetTimeBasedAutoScaling on nonexistent instance returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "SetTimeBasedAutoScaling", map[string]any{
					"InstanceId": "nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "SetLoadBasedAutoScaling on nonexistent layer returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "SetLoadBasedAutoScaling", map[string]any{
					"LayerId": "nonexistent",
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

// TestParity_GrantAccess verifies GrantAccess returns temporary credentials.
func TestParity_GrantAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "GrantAccess returns temporary credentials",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "GrantAccess", map[string]any{
					"InstanceId":        instanceID,
					"ValidForInMinutes": 60,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				creds := resp["TemporaryCredential"].(map[string]any)
				assert.NotEmpty(t, creds["Username"])
				assert.NotEmpty(t, creds["Password"])
				assert.InEpsilon(t, float64(60), creds["ValidForInMinutes"], 0.001)
			},
		},
		{
			name: "GrantAccess on nonexistent instance returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "GrantAccess", map[string]any{
					"InstanceId": "nonexistent",
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

// TestParity_DescribeReadOps verifies describe-only operations.
func TestParity_DescribeReadOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		body      map[string]any
		checkKey  string
	}{
		{
			name:      "DescribeServiceErrors returns empty list",
			operation: "DescribeServiceErrors",
			body:      map[string]any{},
			checkKey:  "ServiceErrors",
		},
		{
			name:      "DescribeRaidArrays returns empty list",
			operation: "DescribeRaidArrays",
			body:      map[string]any{},
			checkKey:  "RaidArrays",
		},
		{
			name:      "DescribeAgentVersions returns versions",
			operation: "DescribeAgentVersions",
			body:      map[string]any{},
			checkKey:  "AgentVersions",
		},
		{
			name:      "DescribeOperatingSystems returns OS list",
			operation: "DescribeOperatingSystems",
			body:      map[string]any{},
			checkKey:  "OperatingSystems",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doTarget(t, h, tt.operation, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseJSON(t, rec.Body.Bytes())
			_, ok := resp[tt.checkKey]
			assert.True(t, ok, "response should contain key %q", tt.checkKey)
		})
	}
}

// TestParity_DescribeAgentVersions verifies non-empty static list.
func TestParity_DescribeAgentVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTarget(t, h, "DescribeAgentVersions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseJSON(t, rec.Body.Bytes())
	versions := resp["AgentVersions"].([]any)
	assert.NotEmpty(t, versions)
	v := versions[0].(map[string]any)
	assert.NotEmpty(t, v["Version"])
}

// TestParity_DescribeOperatingSystems verifies non-empty static list.
func TestParity_DescribeOperatingSystems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTarget(t, h, "DescribeOperatingSystems", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseJSON(t, rec.Body.Bytes())
	oses := resp["OperatingSystems"].([]any)
	assert.NotEmpty(t, oses)
	os := oses[0].(map[string]any)
	assert.NotEmpty(t, os["Id"])
	assert.NotEmpty(t, os["Name"])
}

// TestParity_ListTagsPagination verifies ListTags respects MaxResults/NextToken.
func TestParity_ListTagsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "MaxResults limits returned tags",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				stackArn := "arn:aws:opsworks:us-east-1:000000000000:stack/" + stackID

				// Add 5 tags.
				doTarget(t, h, "TagResource", map[string]any{
					"ResourceArn": stackArn,
					"Tags": map[string]string{
						"a": "1", "b": "2", "c": "3", "d": "4", "e": "5",
					},
				})

				// Request only 2.
				rec := doTarget(t, h, "ListTags", map[string]any{
					"ResourceArn": stackArn,
					"MaxResults":  2,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				tags := resp["Tags"].(map[string]any)
				assert.Len(t, tags, 2)
				assert.NotEmpty(t, resp["NextToken"])
			},
		},
		{
			name: "NextToken continues pagination",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				stackArn := "arn:aws:opsworks:us-east-1:000000000000:stack/" + stackID

				doTarget(t, h, "TagResource", map[string]any{
					"ResourceArn": stackArn,
					"Tags": map[string]string{
						"a": "1", "b": "2", "c": "3",
					},
				})

				rec := doTarget(t, h, "ListTags", map[string]any{
					"ResourceArn": stackArn,
					"MaxResults":  2,
				})
				resp := parseJSON(t, rec.Body.Bytes())
				nextToken := resp["NextToken"].(string)

				rec = doTarget(t, h, "ListTags", map[string]any{
					"ResourceArn": stackArn,
					"NextToken":   nextToken,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp2 := parseJSON(t, rec.Body.Bytes())
				tags2 := resp2["Tags"].(map[string]any)
				assert.Len(t, tags2, 1)
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

// TestParity_CreateInstanceHostnameUnique verifies hostnames are unique.
func TestParity_CreateInstanceHostnameUnique(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	stackID := createTestStack(t, h)
	layerID := createTestLayer(t, h, stackID)

	seen := make(map[string]bool)
	for range 10 {
		rec := doTarget(t, h, "CreateInstance", map[string]any{
			"StackId":      stackID,
			"LayerIds":     []string{layerID},
			"InstanceType": "t2.micro",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		instanceID := parseJSON(t, rec.Body.Bytes())["InstanceId"].(string)

		rec = doTarget(t, h, "DescribeInstances", map[string]any{
			"InstanceIds": []string{instanceID},
		})
		inst := parseJSON(t, rec.Body.Bytes())["Instances"].([]any)[0].(map[string]any)
		hostname := inst["Hostname"].(string)
		assert.False(t, seen[hostname], "duplicate hostname: %s", hostname)
		seen[hostname] = true
	}
}

// TestParity_DeploymentCompletedAt verifies CompletedAt differs from CreatedAt.
func TestParity_DeploymentCompletedAt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	stackID := createTestStack(t, h)
	rec := doTarget(t, h, "CreateApp", map[string]any{
		"StackId": stackID, "Name": "app", "Type": "other",
	})
	appID := parseJSON(t, rec.Body.Bytes())["AppId"].(string)

	rec = doTarget(t, h, "CreateDeployment", map[string]any{
		"StackId": stackID,
		"AppId":   appID,
		"Command": map[string]any{"Name": "deploy"},
	})
	deploymentID := parseJSON(t, rec.Body.Bytes())["DeploymentId"].(string)

	rec = doTarget(t, h, "DescribeDeployments", map[string]any{
		"DeploymentIds": []string{deploymentID},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	d := parseJSON(t, rec.Body.Bytes())["Deployments"].([]any)[0].(map[string]any)
	assert.NotEmpty(t, d["CompletedAt"])
	assert.NotEmpty(t, d["CreatedAt"])
	// CompletedAt should differ from CreatedAt (not set to creation instant).
	assert.NotEqual(t, d["CreatedAt"], d["CompletedAt"])
}

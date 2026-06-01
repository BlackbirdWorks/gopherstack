package opsworks_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

func newTestHandler(t *testing.T) *opsworks.Handler {
	t.Helper()
	backend := opsworks.NewInMemoryBackend("000000000000", "us-east-1")

	return opsworks.NewHandler(backend)
}

func doTarget(t *testing.T, h *opsworks.Handler, operation string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "OpsWorks_20130218."+operation)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func parseJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var result map[string]any
	require.NoError(t, json.Unmarshal(body, &result))

	return result
}

// TestAudit1_Stack verifies stack CRUD operations return correct fields.
func TestAudit1_Stack(t *testing.T) {
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

// TestAudit1_Layer verifies layer CRUD operations return correct fields.
func TestAudit1_Layer(t *testing.T) {
	t.Parallel()

	createStack := func(h *opsworks.Handler) string {
		rec := doTarget(t, h, "CreateStack", map[string]any{
			"Name":                      "stack",
			"Region":                    "us-east-1",
			"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
			"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		resp := parseJSON(t, rec.Body.Bytes())

		return resp["StackId"].(string)
	}

	tests := []struct {
		check     func(t *testing.T, h *opsworks.Handler, stackID string)
		name      string
		operation string
	}{
		{
			name:      "CreateLayer returns LayerId",
			operation: "CreateLayer",
			check: func(t *testing.T, h *opsworks.Handler, stackID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateLayer", map[string]any{
					"StackId":   stackID,
					"Type":      "custom",
					"Name":      "my-layer",
					"Shortname": "ml",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				layerID, ok := resp["LayerId"].(string)
				assert.True(t, ok)
				assert.NotEmpty(t, layerID)
			},
		},
		{
			name:      "DescribeLayers returns layer with CreatedAt and Arn",
			operation: "DescribeLayers",
			check: func(t *testing.T, h *opsworks.Handler, stackID string) {
				t.Helper()
				doTarget(t, h, "CreateLayer", map[string]any{
					"StackId":   stackID,
					"Type":      "custom",
					"Name":      "my-layer",
					"Shortname": "ml",
				})
				rec := doTarget(t, h, "DescribeLayers", map[string]any{"StackId": stackID})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				layers, ok := resp["Layers"].([]any)
				require.True(t, ok)
				require.Len(t, layers, 1)
				layer := layers[0].(map[string]any)
				assert.NotEmpty(t, layer["LayerId"])
				assert.NotEmpty(t, layer["Arn"])
				assert.NotEmpty(t, layer["CreatedAt"])
				assert.Equal(t, "custom", layer["Type"])
				assert.Equal(t, "my-layer", layer["Name"])
			},
		},
		{
			name:      "UpdateLayer modifies name",
			operation: "UpdateLayer",
			check: func(t *testing.T, h *opsworks.Handler, stackID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateLayer", map[string]any{
					"StackId":   stackID,
					"Type":      "custom",
					"Name":      "old-name",
					"Shortname": "on",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				layerID := parseJSON(t, rec.Body.Bytes())["LayerId"].(string)

				rec = doTarget(t, h, "UpdateLayer", map[string]any{
					"LayerId": layerID,
					"Name":    "new-name",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name:      "DeleteLayer removes layer",
			operation: "DeleteLayer",
			check: func(t *testing.T, h *opsworks.Handler, stackID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateLayer", map[string]any{
					"StackId":   stackID,
					"Type":      "custom",
					"Name":      "to-delete",
					"Shortname": "td",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				layerID := parseJSON(t, rec.Body.Bytes())["LayerId"].(string)

				rec = doTarget(t, h, "DeleteLayer", map[string]any{"LayerId": layerID})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeLayers", map[string]any{"StackId": stackID})
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Empty(t, resp["Layers"].([]any))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			stackID := createStack(h)
			tt.check(t, h, stackID)
		})
	}
}

// TestAudit1_Instance verifies instance CRUD and lifecycle operations.
func TestAudit1_Instance(t *testing.T) {
	t.Parallel()

	setup := func(h *opsworks.Handler) (stackID, layerID string) {
		rec := doTarget(t, h, "CreateStack", map[string]any{
			"Name":                      "stack",
			"Region":                    "us-east-1",
			"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
			"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		stackID = parseJSON(t, rec.Body.Bytes())["StackId"].(string)

		rec = doTarget(t, h, "CreateLayer", map[string]any{
			"StackId":   stackID,
			"Type":      "custom",
			"Name":      "layer",
			"Shortname": "l",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		layerID = parseJSON(t, rec.Body.Bytes())["LayerId"].(string)

		return stackID, layerID
	}

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler, stackID, layerID string)
		name  string
	}{
		{
			name: "CreateInstance returns InstanceId",
			check: func(t *testing.T, h *opsworks.Handler, stackID, layerID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateInstance", map[string]any{
					"StackId":      stackID,
					"LayerIds":     []string{layerID},
					"InstanceType": "t2.micro",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.NotEmpty(t, resp["InstanceId"])
			},
		},
		{
			name: "DescribeInstances returns instance with Status and Arn",
			check: func(t *testing.T, h *opsworks.Handler, stackID, layerID string) {
				t.Helper()
				doTarget(t, h, "CreateInstance", map[string]any{
					"StackId":      stackID,
					"LayerIds":     []string{layerID},
					"InstanceType": "t2.micro",
				})
				rec := doTarget(t, h, "DescribeInstances", map[string]any{"StackId": stackID})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				instances, ok := resp["Instances"].([]any)
				require.True(t, ok)
				require.Len(t, instances, 1)
				inst := instances[0].(map[string]any)
				assert.NotEmpty(t, inst["InstanceId"])
				assert.NotEmpty(t, inst["Arn"])
				assert.NotEmpty(t, inst["Status"])
				assert.NotEmpty(t, inst["CreatedAt"])
				assert.Equal(t, "t2.micro", inst["InstanceType"])
			},
		},
		{
			name: "StartInstance transitions status",
			check: func(t *testing.T, h *opsworks.Handler, stackID, layerID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateInstance", map[string]any{
					"StackId":      stackID,
					"LayerIds":     []string{layerID},
					"InstanceType": "t2.micro",
				})
				instanceID := parseJSON(t, rec.Body.Bytes())["InstanceId"].(string)

				rec = doTarget(t, h, "StartInstance", map[string]any{"InstanceId": instanceID})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeInstances", map[string]any{
					"InstanceIds": []string{instanceID},
				})
				resp := parseJSON(t, rec.Body.Bytes())
				inst := resp["Instances"].([]any)[0].(map[string]any)
				assert.Equal(t, "starting", inst["Status"])
			},
		},
		{
			name: "StopInstance transitions status",
			check: func(t *testing.T, h *opsworks.Handler, stackID, layerID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateInstance", map[string]any{
					"StackId":      stackID,
					"LayerIds":     []string{layerID},
					"InstanceType": "t2.micro",
				})
				instanceID := parseJSON(t, rec.Body.Bytes())["InstanceId"].(string)

				rec = doTarget(t, h, "StopInstance", map[string]any{"InstanceId": instanceID})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeInstances", map[string]any{
					"InstanceIds": []string{instanceID},
				})
				resp := parseJSON(t, rec.Body.Bytes())
				inst := resp["Instances"].([]any)[0].(map[string]any)
				assert.Equal(t, "stopping", inst["Status"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			stackID, layerID := setup(h)
			tt.check(t, h, stackID, layerID)
		})
	}
}

// TestAudit1_App verifies app CRUD operations return correct fields.
func TestAudit1_App(t *testing.T) {
	t.Parallel()

	createStack := func(h *opsworks.Handler) string {
		rec := doTarget(t, h, "CreateStack", map[string]any{
			"Name":                      "stack",
			"Region":                    "us-east-1",
			"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
			"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		return parseJSON(t, rec.Body.Bytes())["StackId"].(string)
	}

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler, stackID string)
		name  string
	}{
		{
			name: "CreateApp returns AppId",
			check: func(t *testing.T, h *opsworks.Handler, stackID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateApp", map[string]any{
					"StackId": stackID,
					"Name":    "my-app",
					"Type":    "other",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.NotEmpty(t, resp["AppId"])
			},
		},
		{
			name: "DescribeApps returns app with CreatedAt and Arn",
			check: func(t *testing.T, h *opsworks.Handler, stackID string) {
				t.Helper()
				doTarget(t, h, "CreateApp", map[string]any{
					"StackId": stackID,
					"Name":    "my-app",
					"Type":    "other",
				})
				rec := doTarget(t, h, "DescribeApps", map[string]any{"StackId": stackID})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				apps, ok := resp["Apps"].([]any)
				require.True(t, ok)
				require.Len(t, apps, 1)
				app := apps[0].(map[string]any)
				assert.NotEmpty(t, app["AppId"])
				assert.NotEmpty(t, app["Arn"])
				assert.NotEmpty(t, app["CreatedAt"])
				assert.Equal(t, "my-app", app["Name"])
				assert.Equal(t, "other", app["Type"])
			},
		},
		{
			name: "DeleteApp removes app",
			check: func(t *testing.T, h *opsworks.Handler, stackID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateApp", map[string]any{
					"StackId": stackID,
					"Name":    "to-delete",
					"Type":    "other",
				})
				appID := parseJSON(t, rec.Body.Bytes())["AppId"].(string)

				rec = doTarget(t, h, "DeleteApp", map[string]any{"AppId": appID})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeApps", map[string]any{"StackId": stackID})
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Empty(t, resp["Apps"].([]any))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			stackID := createStack(h)
			tt.check(t, h, stackID)
		})
	}
}

// TestAudit1_Deployment verifies deployment and command operations.
func TestAudit1_Deployment(t *testing.T) {
	t.Parallel()

	createStackAndApp := func(h *opsworks.Handler) (stackID, appID string) {
		rec := doTarget(t, h, "CreateStack", map[string]any{
			"Name":                      "stack",
			"Region":                    "us-east-1",
			"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
			"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		stackID = parseJSON(t, rec.Body.Bytes())["StackId"].(string)

		rec = doTarget(t, h, "CreateApp", map[string]any{
			"StackId": stackID,
			"Name":    "app",
			"Type":    "other",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		appID = parseJSON(t, rec.Body.Bytes())["AppId"].(string)

		return stackID, appID
	}

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler, stackID, appID string)
		name  string
	}{
		{
			name: "CreateDeployment returns DeploymentId",
			check: func(t *testing.T, h *opsworks.Handler, stackID, appID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateDeployment", map[string]any{
					"StackId": stackID,
					"AppId":   appID,
					"Command": map[string]any{"Name": "deploy"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.NotEmpty(t, resp["DeploymentId"])
			},
		},
		{
			name: "DescribeDeployments returns deployment with status and timestamps",
			check: func(t *testing.T, h *opsworks.Handler, stackID, appID string) {
				t.Helper()
				doTarget(t, h, "CreateDeployment", map[string]any{
					"StackId": stackID,
					"AppId":   appID,
					"Command": map[string]any{"Name": "deploy"},
				})
				rec := doTarget(t, h, "DescribeDeployments", map[string]any{"StackId": stackID})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				deployments, ok := resp["Deployments"].([]any)
				require.True(t, ok)
				require.Len(t, deployments, 1)
				d := deployments[0].(map[string]any)
				assert.NotEmpty(t, d["DeploymentId"])
				assert.NotEmpty(t, d["Status"])
				assert.NotEmpty(t, d["CreatedAt"])
				assert.NotEmpty(t, d["CompletedAt"])
				assert.NotEmpty(t, d["Command"])
			},
		},
		{
			name: "DescribeCommands returns commands for deployment",
			check: func(t *testing.T, h *opsworks.Handler, stackID, appID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateDeployment", map[string]any{
					"StackId": stackID,
					"AppId":   appID,
					"Command": map[string]any{"Name": "deploy"},
				})
				deploymentID := parseJSON(t, rec.Body.Bytes())["DeploymentId"].(string)

				rec = doTarget(t, h, "DescribeCommands", map[string]any{
					"DeploymentId": deploymentID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				commands, ok := resp["Commands"].([]any)
				require.True(t, ok)
				require.NotEmpty(t, commands)
				cmd := commands[0].(map[string]any)
				assert.NotEmpty(t, cmd["CommandId"])
				assert.NotEmpty(t, cmd["Status"])
				assert.NotEmpty(t, cmd["CreatedAt"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			stackID, appID := createStackAndApp(h)
			tt.check(t, h, stackID, appID)
		})
	}
}

// TestAudit1_Tags verifies tag operations on OpsWorks resources.
func TestAudit1_Tags(t *testing.T) {
	t.Parallel()

	createStack := func(h *opsworks.Handler) (stackID, arn string) {
		rec := doTarget(t, h, "CreateStack", map[string]any{
			"Name":                      "tagged-stack",
			"Region":                    "us-east-1",
			"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
			"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		stackID = parseJSON(t, rec.Body.Bytes())["StackId"].(string)
		arn = "arn:aws:opsworks:us-east-1:000000000000:stack/" + stackID

		return stackID, arn
	}

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler, arn string)
		name  string
	}{
		{
			name: "TagResource and ListTags round-trip",
			check: func(t *testing.T, h *opsworks.Handler, arn string) {
				t.Helper()
				rec := doTarget(t, h, "TagResource", map[string]any{
					"ResourceArn": arn,
					"Tags":        map[string]string{"env": "prod", "team": "ops"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "ListTags", map[string]any{"ResourceArn": arn})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				tags, ok := resp["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tags["env"])
				assert.Equal(t, "ops", tags["team"])
			},
		},
		{
			name: "UntagResource removes specified keys",
			check: func(t *testing.T, h *opsworks.Handler, arn string) {
				t.Helper()
				doTarget(t, h, "TagResource", map[string]any{
					"ResourceArn": arn,
					"Tags":        map[string]string{"env": "prod", "team": "ops"},
				})

				rec := doTarget(t, h, "UntagResource", map[string]any{
					"ResourceArn": arn,
					"TagKeys":     []string{"env"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "ListTags", map[string]any{"ResourceArn": arn})
				resp := parseJSON(t, rec.Body.Bytes())
				tags := resp["Tags"].(map[string]any)
				assert.NotContains(t, tags, "env")
				assert.Contains(t, tags, "team")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, arn := createStack(h)
			tt.check(t, h, arn)
		})
	}
}

// TestAudit1_ErrorHandling verifies error responses for invalid requests.
func TestAudit1_ErrorHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		operation string
		wantCode  int
	}{
		{
			name:      "DescribeStacks with nonexistent ID returns 404",
			operation: "DescribeStacks",
			body:      map[string]any{"StackIds": []string{"nonexistent-id"}},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "DescribeLayers with nonexistent ID returns 404",
			operation: "DescribeLayers",
			body:      map[string]any{"LayerIds": []string{"nonexistent-id"}},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "DescribeInstances with nonexistent ID returns 404",
			operation: "DescribeInstances",
			body:      map[string]any{"InstanceIds": []string{"nonexistent-id"}},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "DeleteStack with nonexistent ID returns 404",
			operation: "DeleteStack",
			body:      map[string]any{"StackId": "nonexistent-id"},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "DeleteLayer with nonexistent ID returns 404",
			operation: "DeleteLayer",
			body:      map[string]any{"LayerId": "nonexistent-id"},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "StartInstance with nonexistent ID returns 404",
			operation: "StartInstance",
			body:      map[string]any{"InstanceId": "nonexistent-id"},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "CreateStack with empty name returns 400",
			operation: "CreateStack",
			body: map[string]any{
				"Name":           "",
				"ServiceRoleArn": "arn:aws:iam::000000000000:role/test",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTarget(t, h, tt.operation, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestApp verifies app CRUD operations return correct fields.
func TestApp(t *testing.T) {
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

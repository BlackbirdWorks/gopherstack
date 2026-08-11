package workspaces_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientBranding(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)
	resourceID := "d-branding-test"

	// Import branding
	rec := doTargetRequest(t, h, "ImportClientBranding", map[string]any{
		"ResourceId": resourceID,
		"DeviceTypeWindows": map[string]any{
			"Logo":        "base64data",
			"SupportLink": "https://support.example.com",
		},
		"DeviceTypeOsx": map[string]any{
			"Logo": "maclogo",
		},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("import branding: expected 200, got %d: %s", rec.Code, rec.Body)
	}

	// Describe branding
	rec2 := doTargetRequest(t, h, "DescribeClientBranding", map[string]any{
		"ResourceId": resourceID,
	})
	if rec2.Code != http.StatusOK {
		t.Fatalf("describe branding: expected 200, got %d", rec2.Code)
	}

	var descOut map[string]any
	decodeJSON(t, rec2.Body.Bytes(), &descOut)

	if descOut["DeviceTypeWindows"] == nil {
		t.Fatal("expected DeviceTypeWindows branding")
	}

	// Delete branding for one platform
	rec3 := doTargetRequest(t, h, "DeleteClientBranding", map[string]any{
		"ResourceId": resourceID,
		"Platforms":  []string{"DeviceTypeWindows"},
	})
	if rec3.Code != http.StatusOK {
		t.Fatalf("delete branding: expected 200, got %d", rec3.Code)
	}

	// Verify Windows branding is gone
	rec4 := doTargetRequest(t, h, "DescribeClientBranding", map[string]any{
		"ResourceId": resourceID,
	})
	var descOut2 map[string]any
	decodeJSON(t, rec4.Body.Bytes(), &descOut2)

	if descOut2["DeviceTypeWindows"] != nil {
		t.Fatal("expected DeviceTypeWindows branding to be deleted")
	}

	if descOut2["DeviceTypeOsx"] == nil {
		t.Fatal("expected DeviceTypeOsx branding to remain")
	}
}

func TestClientProperties(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name             string
		resourceID       string
		reconnectEnabled string
	}{
		{name: "enabled", resourceID: "d-001", reconnectEnabled: "ENABLED"},
		{name: "disabled", resourceID: "d-002", reconnectEnabled: "DISABLED"},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Modify
			rec := doTargetRequest(t, h, "ModifyClientProperties", map[string]any{
				"ResourceId": tc.resourceID,
				"ClientProperties": map[string]any{
					"ReconnectEnabled": tc.reconnectEnabled,
				},
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("modify: expected 200, got %d", rec.Code)
			}

			// Describe
			rec2 := doTargetRequest(t, h, "DescribeClientProperties", map[string]any{
				"ResourceIds": []string{tc.resourceID},
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("describe: expected 200, got %d", rec2.Code)
			}

			var descOut struct {
				ClientPropertiesList []struct {
					ClientProperties map[string]any `json:"ClientProperties"`
					ResourceId       string         `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
				} `json:"ClientPropertiesList"`
			}
			decodeJSON(t, rec2.Body.Bytes(), &descOut)

			if len(descOut.ClientPropertiesList) != 1 {
				t.Fatalf("expected 1 item, got %d", len(descOut.ClientPropertiesList))
			}

			got, _ := descOut.ClientPropertiesList[0].ClientProperties["ReconnectEnabled"].(string)
			if got != tc.reconnectEnabled {
				t.Fatalf("expected %s, got %s", tc.reconnectEnabled, got)
			}
		})
	}
}

// TestModifyClientProperties_NewFields covers gopherstack-gt9o: real
// types.ClientProperties (aws-sdk-go-v2/service/workspaces@v1.73.1
// types/types.go:263) gained ClientExperiencePolicy, and LogUploadEnabled was
// already unthreaded too. ClientExperiencePolicy is a bare *string in the SDK
// (no @enum trait, unlike LogUploadEnabled/ReconnectEnabled's generated enum
// types) so any value must be accepted, not just the documented examples.
func TestModifyClientProperties_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		clientProperties map[string]any
		name             string
	}{
		{
			name: "documented enum value",
			clientProperties: map[string]any{
				"ClientExperiencePolicy": "FORCE_UI_2026",
				"LogUploadEnabled":       "ENABLED",
				"ReconnectEnabled":       "ENABLED",
			},
		},
		{
			name: "undocumented value still accepted",
			clientProperties: map[string]any{
				"ClientExperiencePolicy": "SOME_FUTURE_VALUE",
				"LogUploadEnabled":       "DISABLED",
				"ReconnectEnabled":       "DISABLED",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandlerWithBackend(t)
			resourceID := "d-new-fields-test"

			rec := doTargetRequest(t, h, "ModifyClientProperties", map[string]any{
				"ResourceId":       resourceID,
				"ClientProperties": tc.clientProperties,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			descRec := doTargetRequest(t, h, "DescribeClientProperties", map[string]any{
				"ResourceIds": []string{resourceID},
			})
			require.Equal(t, http.StatusOK, descRec.Code)

			var descOut struct {
				ClientPropertiesList []struct {
					ClientProperties map[string]any `json:"ClientProperties"`
				} `json:"ClientPropertiesList"`
			}
			decodeJSON(t, descRec.Body.Bytes(), &descOut)
			require.Len(t, descOut.ClientPropertiesList, 1)

			got := descOut.ClientPropertiesList[0].ClientProperties
			assert.Equal(t, tc.clientProperties["ClientExperiencePolicy"], got["ClientExperiencePolicy"])
			assert.Equal(t, tc.clientProperties["LogUploadEnabled"], got["LogUploadEnabled"])
			assert.Equal(t, tc.clientProperties["ReconnectEnabled"], got["ReconnectEnabled"])
		})
	}
}

// TestModifyClientProperties_MergeSemantics verifies ModifyClientProperties is
// a partial update: omitted fields on a later call must not clear values set
// by an earlier call, matching the real API's per-field-optional request shape.
func TestModifyClientProperties_MergeSemantics(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)
	resourceID := "d-merge-test"

	rec := doTargetRequest(t, h, "ModifyClientProperties", map[string]any{
		"ResourceId": resourceID,
		"ClientProperties": map[string]any{
			"ClientExperiencePolicy": "FORCE_CLASSIC",
			"ReconnectEnabled":       "ENABLED",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doTargetRequest(t, h, "ModifyClientProperties", map[string]any{
		"ResourceId": resourceID,
		"ClientProperties": map[string]any{
			"ReconnectEnabled": "DISABLED",
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	descRec := doTargetRequest(t, h, "DescribeClientProperties", map[string]any{
		"ResourceIds": []string{resourceID},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		ClientPropertiesList []struct {
			ClientProperties map[string]any `json:"ClientProperties"`
		} `json:"ClientPropertiesList"`
	}
	decodeJSON(t, descRec.Body.Bytes(), &descOut)
	require.Len(t, descOut.ClientPropertiesList, 1)

	got := descOut.ClientPropertiesList[0].ClientProperties
	assert.Equal(
		t,
		"FORCE_CLASSIC",
		got["ClientExperiencePolicy"],
		"unrelated earlier field must survive a partial update",
	)
	assert.Equal(t, "DISABLED", got["ReconnectEnabled"])
}

// TestDescribeClientProperties_UnsetFieldsAbsentFromWire asserts on the raw
// response body: a resource that never had ClientExperiencePolicy or
// LogUploadEnabled set must omit those keys entirely, not serialize an empty
// string that would parse identically to "field absent" and mask the
// difference between the two on a real client.
func TestDescribeClientProperties_UnsetFieldsAbsentFromWire(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)
	resourceID := "d-absent-fields-test"

	rec := doTargetRequest(t, h, "ModifyClientProperties", map[string]any{
		"ResourceId": resourceID,
		"ClientProperties": map[string]any{
			"ReconnectEnabled": "ENABLED",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doTargetRequest(t, h, "DescribeClientProperties", map[string]any{
		"ResourceIds": []string{resourceID},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	body := descRec.Body.String()
	assert.NotContains(t, body, "ClientExperiencePolicy")
	assert.NotContains(t, body, "LogUploadEnabled")
	assert.Contains(t, body, "ReconnectEnabled")
}

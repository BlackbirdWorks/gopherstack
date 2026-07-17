package workspaces_test

import (
	"net/http"
	"testing"
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

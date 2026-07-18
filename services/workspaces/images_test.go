package workspaces_test

import (
	"net/http"
	"testing"
)

// TestWorkspaceImageCRUD exercises image creation via Copy/Create/Import operations.
func TestWorkspaceImageCRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	tests := []struct {
		body  any
		check func(t *testing.T, body []byte)
		name  string
		op    string
	}{
		{
			name: "CopyWorkspaceImage",
			op:   "CopyWorkspaceImage",
			body: map[string]any{
				"Name":          "copied-image",
				"SourceImageId": "wsi-source",
				"SourceRegion":  "us-west-2",
				"Description":   "A copied image",
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var out map[string]string
				decodeJSON(t, body, &out)
				if out["ImageId"] == "" {
					t.Fatal("expected ImageId")
				}
			},
		},
		{
			name: "CreateWorkspaceImage",
			op:   "CreateWorkspaceImage",
			body: map[string]any{
				"Name":        "new-image",
				"Description": "from workspace",
				"WorkspaceId": "ws-00000001",
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var out map[string]string
				decodeJSON(t, body, &out)
				if out["ImageId"] == "" {
					t.Fatal("expected ImageId")
				}
				if out["State"] != "AVAILABLE" {
					t.Fatalf("expected AVAILABLE state, got %s", out["State"])
				}
			},
		},
		{
			name: "ImportWorkspaceImage",
			op:   "ImportWorkspaceImage",
			body: map[string]any{
				"Ec2ImageId":       "ami-12345678",
				"ImageName":        "imported",
				"ImageDescription": "ec2 import",
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var out map[string]string
				decodeJSON(t, body, &out)
				if out["ImageId"] == "" {
					t.Fatal("expected ImageId")
				}
			},
		},
		{
			name: "ImportCustomWorkspaceImage",
			op:   "ImportCustomWorkspaceImage",
			body: map[string]any{
				"ImageName":        "custom-img",
				"ImageDescription": "custom",
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var out map[string]string
				decodeJSON(t, body, &out)
				if out["ImageId"] == "" {
					t.Fatal("expected ImageId")
				}
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doTargetRequest(t, h, tc.op, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body)
			}

			tc.check(t, rec.Body.Bytes())
		})
	}
}

// TestWorkspaceImageDescribeAndPermissions exercises describe/permission/update
// flows for a previously created image.
func TestWorkspaceImageDescribeAndPermissions(
	t *testing.T,
) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)

	// Create an image
	rec := doTargetRequest(t, h, "CopyWorkspaceImage", map[string]any{
		"Name":          "perm-test",
		"SourceImageId": "wsi-src",
		"SourceRegion":  "us-east-1",
	})
	var createOut map[string]string
	decodeJSON(t, rec.Body.Bytes(), &createOut)
	imageID := createOut["ImageId"]

	// Describe images
	rec2 := doTargetRequest(t, h, "DescribeWorkspaceImages", map[string]any{
		"ImageIds": []string{imageID},
	})
	if rec2.Code != http.StatusOK {
		t.Fatalf("describe images: expected 200, got %d", rec2.Code)
	}

	var descOut struct {
		Images []map[string]any `json:"Images"`
	}
	decodeJSON(t, rec2.Body.Bytes(), &descOut)

	if len(descOut.Images) != 1 {
		t.Fatalf("expected 1 image, got %d", len(descOut.Images))
	}

	// Update permission
	rec3 := doTargetRequest(t, h, "UpdateWorkspaceImagePermission", map[string]any{
		"ImageId":         imageID,
		"SharedAccountId": "999988887777",
		"AllowCopyImage":  true,
	})
	if rec3.Code != http.StatusOK {
		t.Fatalf("update permission: expected 200, got %d", rec3.Code)
	}

	// Describe permissions
	rec4 := doTargetRequest(t, h, "DescribeWorkspaceImagePermissions", map[string]any{
		"ImageId": imageID,
	})
	if rec4.Code != http.StatusOK {
		t.Fatalf("describe perms: expected 200, got %d", rec4.Code)
	}

	var permsOut struct {
		ImageId          string           `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
		ImagePermissions []map[string]any `json:"ImagePermissions"`
	}
	decodeJSON(t, rec4.Body.Bytes(), &permsOut)

	if len(permsOut.ImagePermissions) != 1 {
		t.Fatalf("expected 1 permission, got %d", len(permsOut.ImagePermissions))
	}

	// DescribeCustomWorkspaceImageImport
	rec5 := doTargetRequest(t, h, "DescribeCustomWorkspaceImageImport", map[string]any{
		"ImageId": imageID,
	})
	if rec5.Code != http.StatusOK {
		t.Fatalf("describe custom import: expected 200, got %d", rec5.Code)
	}

	// CreateUpdatedWorkspaceImage
	rec6 := doTargetRequest(t, h, "CreateUpdatedWorkspaceImage", map[string]any{
		"SourceImageId": imageID,
		"Name":          "updated",
		"Description":   "updated version",
	})
	if rec6.Code != http.StatusOK {
		t.Fatalf("create updated image: expected 200, got %d", rec6.Code)
	}

	// Delete image
	rec7 := doTargetRequest(t, h, "DeleteWorkspaceImage", map[string]any{
		"ImageId": imageID,
	})
	if rec7.Code != http.StatusOK {
		t.Fatalf("delete image: expected 200, got %d", rec7.Code)
	}
}

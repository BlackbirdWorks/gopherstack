package workspaces_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplicationAssociations(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{"DirectoryId": "d-test"})

	// Create a workspace first
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    "alice",
				"DirectoryId": "d-test",
				"BundleId":    "wsb-test",
			},
		},
	})
	var wsOut struct {
		PendingRequests []map[string]string `json:"PendingRequests"`
	}
	decodeJSON(t, rec.Body.Bytes(), &wsOut)

	wsID := wsOut.PendingRequests[0]["WorkspaceId"]
	appID := "app-12345"

	// DescribeImageAssociations/DescribeBundleAssociations now validate that
	// ImageId/BundleId reference real resources (previously a stub that
	// ignored the input entirely), so exercise them against a real image and
	// a real Amazon-owned bundle rather than made-up IDs.
	imgRec := doTargetRequest(t, h, "CreateWorkspaceImage", map[string]any{
		"Name":        "img-for-assoc-test",
		"Description": "test",
		"WorkspaceId": wsID,
	})
	var imgOut struct {
		ImageID string `json:"ImageId"`
	}
	decodeJSON(t, imgRec.Body.Bytes(), &imgOut)

	tests := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{
			name: "AssociateWorkspaceApplication",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "AssociateWorkspaceApplication", map[string]any{
					"WorkspaceId":   wsID,
					"ApplicationId": appID,
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
		{
			name: "DescribeWorkspaceAssociations",
			fn: func(t *testing.T) {
				t.Helper()
				// AssociatedResourceTypes is a real required field (smithy
				// `required` on DescribeWorkspaceAssociationsInput).
				r := doTargetRequest(t, h, "DescribeWorkspaceAssociations", map[string]any{
					"WorkspaceId":             wsID,
					"AssociatedResourceTypes": []string{"APPLICATION"},
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d: %s", r.Code, r.Body)
				}
			},
		},
		{
			name: "DescribeApplicationAssociations",
			fn: func(t *testing.T) {
				t.Helper()
				// AssociatedResourceTypes is a real required field (smithy
				// `required` on DescribeApplicationAssociationsInput).
				r := doTargetRequest(t, h, "DescribeApplicationAssociations", map[string]any{
					"ApplicationId":           appID,
					"AssociatedResourceTypes": []string{"WORKSPACE"},
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d: %s", r.Code, r.Body)
				}
			},
		},
		{
			name: "DeployWorkspaceApplications",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DeployWorkspaceApplications", map[string]any{
					"WorkspaceId": wsID,
					"Force":       false,
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
		{
			name: "DescribeApplications",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DescribeApplications", map[string]any{})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
		{
			name: "DescribeImageAssociations",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DescribeImageAssociations", map[string]any{
					"ImageId":                 imgOut.ImageID,
					"AssociatedResourceTypes": []string{"APPLICATION"},
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d: %s", r.Code, r.Body)
				}
			},
		},
		{
			name: "DescribeBundleAssociations",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DescribeBundleAssociations", map[string]any{
					"BundleId":                "wsb-bh8rsxt14",
					"AssociatedResourceTypes": []string{"APPLICATION"},
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d: %s", r.Code, r.Body)
				}
			},
		},
		{
			name: "DisassociateWorkspaceApplication",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DisassociateWorkspaceApplication", map[string]any{
					"WorkspaceId":   wsID,
					"ApplicationId": appID,
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t)
		})
	}
}

// TestDescribeImageAssociations_Validation and
// TestDescribeBundleAssociations_Validation verify the required-field and
// existence validation these ops previously skipped entirely (a stub that
// ignored ImageId/BundleId and always returned an empty 200).
func TestDescribeImageAssociations_Validation(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)

	t.Run("unknown ImageId is not found", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "DescribeImageAssociations", map[string]any{
			"ImageId":                 "wsi-does-not-exist",
			"AssociatedResourceTypes": []string{"APPLICATION"},
		})
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body)
		}
	})

	t.Run("missing AssociatedResourceTypes is rejected", func(t *testing.T) {
		t.Parallel()

		imgRec := doTargetRequest(t, h, "CopyWorkspaceImage", map[string]any{
			"Name":          "img-for-validation",
			"SourceImageId": "wsi-src",
			"SourceRegion":  "us-east-1",
		})
		var imgOut map[string]string
		decodeJSON(t, imgRec.Body.Bytes(), &imgOut)

		rec := doTargetRequest(t, h, "DescribeImageAssociations", map[string]any{
			"ImageId": imgOut["ImageId"],
		})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body)
		}
	})
}

func TestDescribeBundleAssociations_Validation(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)

	t.Run("unknown BundleId is not found", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "DescribeBundleAssociations", map[string]any{
			"BundleId":                "wsb-does-not-exist",
			"AssociatedResourceTypes": []string{"APPLICATION"},
		})
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body)
		}
	})

	t.Run("missing AssociatedResourceTypes is rejected", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "DescribeBundleAssociations", map[string]any{
			"BundleId": "wsb-bh8rsxt14",
		})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body)
		}
	})

	t.Run("Amazon-owned bundle is a valid target", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "DescribeBundleAssociations", map[string]any{
			"BundleId":                "wsb-bh8rsxt14",
			"AssociatedResourceTypes": []string{"APPLICATION"},
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body)
		}

		var out struct {
			Associations []any `json:"Associations"`
		}
		decodeJSON(t, rec.Body.Bytes(), &out)
		if out.Associations == nil {
			t.Fatal("expected non-nil (possibly empty) Associations array")
		}
	})
}

// TestWorkspaceApplicationAssociations_Validation verifies the
// Associate/Disassociate/Deploy/Describe*Associations family validates a
// nonexistent WorkspaceId (previously accepted unconditionally and reported
// success) and the required AssociatedResourceTypes field, and that the wire
// shape uses the real "State" key/enum rather than the previous
// "AssociationStatus": "INSTALLED" (neither exists on the real
// WorkspaceResourceAssociation type). ApplicationId is deliberately NOT
// existence-checked: this backend never populates the read-only applications
// catalog (real AWS has no CreateApplication API), so requiring a match would
// make the whole family permanently unusable.
func TestWorkspaceApplicationAssociations_Validation(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{"DirectoryId": "d-appval"})
	recCreate := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{"UserName": "bob", "DirectoryId": "d-appval", "BundleId": "wsb-test"},
		},
	})
	require.Equal(t, http.StatusOK, recCreate.Code)

	var wsOut struct {
		PendingRequests []map[string]string `json:"PendingRequests"`
	}
	decodeJSON(t, recCreate.Body.Bytes(), &wsOut)
	require.Len(t, wsOut.PendingRequests, 1)
	wsID := wsOut.PendingRequests[0]["WorkspaceId"]

	t.Run("associate rejects unknown workspace", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "AssociateWorkspaceApplication", map[string]any{
			"WorkspaceId":   "ws-does-not-exist",
			"ApplicationId": "app-1",
		})
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body)
	})

	t.Run("disassociate rejects unknown workspace", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "DisassociateWorkspaceApplication", map[string]any{
			"WorkspaceId":   "ws-does-not-exist",
			"ApplicationId": "app-1",
		})
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body)
	})

	t.Run("deploy rejects unknown workspace", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "DeployWorkspaceApplications", map[string]any{
			"WorkspaceId": "ws-does-not-exist",
		})
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body)
	})

	t.Run("describe workspace associations rejects unknown workspace", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "DescribeWorkspaceAssociations", map[string]any{
			"WorkspaceId":             "ws-does-not-exist",
			"AssociatedResourceTypes": []string{"APPLICATION"},
		})
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body)
	})

	t.Run("describe workspace associations requires resource types", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "DescribeWorkspaceAssociations", map[string]any{
			"WorkspaceId": wsID,
		})
		require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body)
	})

	t.Run("describe application associations requires resource types", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "DescribeApplicationAssociations", map[string]any{
			"ApplicationId": "app-1",
		})
		require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body)
	})

	t.Run("associate reports the real State enum, not a fabricated one", func(t *testing.T) {
		t.Parallel()

		rec := doTargetRequest(t, h, "AssociateWorkspaceApplication", map[string]any{
			"WorkspaceId":   wsID,
			"ApplicationId": "app-real-shape",
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)

		var out struct {
			Association struct {
				State             string `json:"State"`
				AssociationStatus string `json:"AssociationStatus"`
			} `json:"Association"`
		}
		decodeJSON(t, rec.Body.Bytes(), &out)
		assert.Equal(t, "COMPLETED", out.Association.State)
		assert.Empty(
			t, out.Association.AssociationStatus,
			"AssociationStatus is not a field on the real WorkspaceResourceAssociation type",
		)
	})
}

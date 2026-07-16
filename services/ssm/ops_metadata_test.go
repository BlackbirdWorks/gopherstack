package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestOpsMetadata_List(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{ResourceID: "res-1"})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListOpsMetadata", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "OpsMetadataList")
}
func TestGetOpsSummary(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "GetOpsSummary", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Entities")
}

// TestStubOps_DeleteOpsMetadata exercises the opsmetadata delete stub.
func TestStubOps_DeleteOpsMetadata(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	meta, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"OpsMetadataArn": meta.OpsMetadataArn})
	rec := doRequest(t, h, "DeleteOpsMetadata", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_GetOpsMetadata exercises that stub.
func TestStubOps_GetOpsMetadata(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	meta, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-abcdef0123456789",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"OpsMetadataArn": meta.OpsMetadataArn})
	rec := doRequest(t, h, "GetOpsMetadata", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_UpdateOpsMetadata exercises that stub.
func TestStubOps_UpdateOpsMetadata(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	meta, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-update0123456789",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"OpsMetadataArn": meta.OpsMetadataArn})
	rec := doRequest(t, h, "UpdateOpsMetadata", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}
func TestCreateOpsMetadata_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "with_resource_id",
			body:       `{"ResourceId":"my-app-resource"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "with_metadata",
			body:       `{"ResourceId":"res-1","Metadata":{"env":{"Value":"prod"}}}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			rec := doRequest(t, h, "CreateOpsMetadata", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateOpsMetadataOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.OpsMetadataArn)
			assert.Contains(t, resp.OpsMetadataArn, "arn:aws:ssm:")
			assert.Equal(t, 1, backend.OpsMetadataCount())
		})
	}
}
func TestCreateOpsMetadata_ValidationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing_resource_id",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateOpsMetadata", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
}
func TestResetClearsAllMaps(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandler(t)

	doRequest(t, h, "CreateActivation", `{"IamRole":"arn:aws:iam::123:role/r"}`)
	doRequest(t, h, "CreateOpsItem", `{"Title":"t","Source":"s"}`)
	doRequest(t, h, "CreateMaintenanceWindow", `{"Name":"w","Schedule":"rate(1 day)","Duration":2,"Cutoff":0}`)
	doRequest(t, h, "CreatePatchBaseline", `{"Name":"b"}`)
	doRequest(t, h, "CreateOpsMetadata", `{"ResourceId":"res"}`)

	require.Equal(t, 1, backend.ActivationCount())
	require.Equal(t, 1, backend.OpsItemCount())
	require.Equal(t, 1, backend.MaintenanceWindowCount())
	require.Equal(t, 1, backend.PatchBaselineCount())
	require.Equal(t, 1, backend.OpsMetadataCount())

	backend.Reset()

	assert.Equal(t, 0, backend.ActivationCount())
	assert.Equal(t, 0, backend.OpsItemCount())
	assert.Equal(t, 0, backend.MaintenanceWindowCount())
	assert.Equal(t, 0, backend.PatchBaselineCount())
	assert.Equal(t, 0, backend.OpsMetadataCount())
}
func TestInvalidJSON(t *testing.T) {
	t.Parallel()
	ops := []string{
		"CancelCommand", "CancelMaintenanceWindowExecution",
		"CreateActivation", "CreateAssociation", "CreateAssociationBatch",
		"CreateMaintenanceWindow", "CreateOpsItem", "CreateOpsMetadata",
		"CreatePatchBaseline", "AssociateOpsItemRelatedItem",
	}
	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t)
			rec := doRequest(t, h, op, "invalid json{{{")
			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		})
	}
}

// TestRefinement2_CreateOpsMetadata_DuplicateResourceID verifies duplicate check.
func TestCreateOpsMetadata_DuplicateResourceID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		firstBody  string
		secondBody string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "duplicate_resource_id",
			firstBody:  `{"ResourceId":"my-resource"}`,
			secondBody: `{"ResourceId":"my-resource"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "OpsMetadataAlreadyExistsException",
		},
		{
			name:       "different_resource_ids_succeed",
			firstBody:  `{"ResourceId":"resource-1"}`,
			secondBody: `{"ResourceId":"resource-2"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			first := doRequest(t, h, "CreateOpsMetadata", tt.firstBody)
			require.Equal(t, http.StatusOK, first.Code)

			second := doRequest(t, h, "CreateOpsMetadata", tt.secondBody)
			assert.Equal(t, tt.wantStatus, second.Code)
			if tt.wantErr != "" {
				assert.Contains(t, second.Body.String(), tt.wantErr)
			}
		})
	}
}

// TestRefinement2_SeedHelpers verifies seed helpers work correctly.
func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	t.Run("activation", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		b.AddActivationInternal(ssm.Activation{
			ActivationID:   "act-test-1",
			ActivationCode: "ABCDEFGHIJ1234567890",
			IamRole:        "test-role",
		})
		assert.Equal(t, 1, b.ActivationCount())
	})

	t.Run("association", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		b.AddAssociationInternal(ssm.Association{
			AssociationID: "assoc-test-1",
			Name:          "MyDoc",
		})
		assert.Equal(t, 1, b.AssociationCount())
	})

	t.Run("maintenance_window", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		b.AddMaintenanceWindowInternal(ssm.MaintenanceWindow{
			WindowID: "mw-test-1",
			Name:     "TestWindow",
		})
		assert.Equal(t, 1, b.MaintenanceWindowCount())
	})

	t.Run("ops_item", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		b.AddOpsItemInternal(ssm.OpsItem{
			OpsItemID: "oi-test-1",
			Title:     "Test Item",
			Source:    "test",
		})
		assert.Equal(t, 1, b.OpsItemCount())
	})

	t.Run("ops_metadata", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		b.AddOpsMetadataInternal(ssm.OpsMetadata{
			OpsMetadataArn: "arn:aws:ssm:us-east-1:123456789012:opsmetadata/test",
			ResourceID:     "test-resource",
		})
		assert.Equal(t, 1, b.OpsMetadataCount())
	})

	t.Run("patch_baseline", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		b.AddPatchBaselineInternal(ssm.PatchBaseline{
			BaselineID: "pb-test-1",
			Name:       "TestBaseline",
		})
		assert.Equal(t, 1, b.PatchBaselineCount())
	})

	t.Run("ops_item_related_item_count", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		b.AddOpsItemInternal(ssm.OpsItem{
			OpsItemID: "oi-test-rel",
			Title:     "Item With Related",
			Source:    "test",
		})
		assert.Equal(t, 0, b.OpsItemRelatedItemCount("oi-test-rel"))
	})
}

// TestRefinement2_PersistenceWithNewFields verifies Snapshot/Restore preserves new fields.
func TestPersistenceWithNewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
	}{
		{name: "ops_metadata_resource_id_preserved", resourceID: "my-app-resource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := ssm.NewInMemoryBackend()

			_, err := b1.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
				ResourceID: tt.resourceID,
			})
			require.NoError(t, err)

			// Duplicate should fail
			_, err = b1.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
				ResourceID: tt.resourceID,
			})
			require.Error(t, err)

			snap := b1.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := ssm.NewInMemoryBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			// After restore, duplicate should still fail
			_, err = b2.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
				ResourceID: tt.resourceID,
			})
			require.Error(t, err)
		})
	}
}

// TestRefinement2_ResetClearsNewFields verifies Reset clears new fields.
func TestResetClearsNewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_resource_id_map_and_misc_tags"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()

			_, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{ResourceID: "res-1"})
			require.NoError(t, err)

			err = b.AddTagsToResource(context.TODO(), &ssm.AddTagsToResourceInput{
				ResourceType: "Activation",
				ResourceID:   "act-1",
				Tags:         []ssm.Tag{{Key: "K", Value: "V"}},
			})
			require.NoError(t, err)

			b.Reset()

			// After reset, creating the same resource ID should succeed
			_, err = b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{ResourceID: "res-1"})
			require.NoError(t, err)

			_ = tt.name
		})
	}
}
func TestBackendOps_GetOpsMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	createOut, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-meta-test",
	})
	require.NoError(t, err)

	out, err := b.GetOpsMetadata(context.TODO(), &ssm.GetOpsMetadataInput{OpsMetadataArn: createOut.OpsMetadataArn})
	require.NoError(t, err)
	assert.Equal(t, createOut.OpsMetadataArn, out.OpsMetadataArn)
}
func TestBackendOps_UpdateOpsMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	createOut, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-update-meta",
	})
	require.NoError(t, err)

	out, err := b.UpdateOpsMetadata(context.TODO(), &ssm.UpdateOpsMetadataInput{
		OpsMetadataArn: createOut.OpsMetadataArn,
		Metadata: map[string]ssm.MetadataValue{
			"env": {Value: "prod"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, createOut.OpsMetadataArn, out.OpsMetadataArn)
}
func TestBackendOps_DeleteOpsMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	createOut, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-delete-meta",
	})
	require.NoError(t, err)

	out, err := b.DeleteOpsMetadata(
		context.TODO(),
		&ssm.DeleteOpsMetadataInput{OpsMetadataArn: createOut.OpsMetadataArn},
	)
	require.NoError(t, err)
	assert.NotNil(t, out)

	// Should be gone.
	_, err = b.GetOpsMetadata(context.TODO(), &ssm.GetOpsMetadataInput{OpsMetadataArn: createOut.OpsMetadataArn})
	require.Error(t, err)
}
func TestFull_OpsMetadata_CreateUpdateDelete(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Create
	code, out := mustPost(t, h, "CreateOpsMetadata", map[string]any{
		"ResourceId": "i-abc123",
		"Metadata": map[string]any{
			"env": map[string]any{"Value": "prod"},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	arn := out["OpsMetadataArn"].(string)
	assert.NotEmpty(t, arn)

	// Get
	code, out = mustPost(t, h, "GetOpsMetadata", map[string]any{"OpsMetadataArn": arn})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["Metadata"])

	// Update
	code, _ = mustPost(t, h, "UpdateOpsMetadata", map[string]any{
		"OpsMetadataArn": arn,
		"MetadataToUpdate": map[string]any{
			"version": map[string]any{"Value": "2"},
		},
	})
	assert.Equal(t, http.StatusOK, code)

	// Delete
	code, _ = mustPost(t, h, "DeleteOpsMetadata", map[string]any{"OpsMetadataArn": arn})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetOpsMetadata", map[string]any{"OpsMetadataArn": arn})
	assert.Equal(t, http.StatusBadRequest, code)
}

// TestClassifySSMErrorExtended covers the extended error classification paths.
func TestClassifySSMErrorExtended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *ssm.InMemoryBackend)
		name        string
		action      string
		body        string
		wantErrType string
		wantCode    int
	}{
		{
			name:        "activation_not_found_via_delete",
			action:      "DeleteActivation",
			body:        `{"ActivationId":"nonexistent"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "ActivationNotFound",
		},
		{
			name:        "association_not_found_via_delete",
			action:      "DeleteAssociation",
			body:        `{"AssociationId":"nonexistent"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "AssociationDoesNotExist",
		},
		{
			name:        "ops_item_not_found",
			action:      "GetOpsItem",
			body:        `{"OpsItemId":"oi-nonexistent"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "OpsItemNotFoundException",
		},
		{
			name:        "ops_metadata_not_found",
			action:      "GetOpsMetadata",
			body:        `{"OpsMetadataArn":"arn:aws:ssm:us-east-1:123:opsmetadata/nonexistent"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "OpsMetadataNotFoundException",
		},
		{
			name:        "patch_baseline_not_found",
			action:      "GetPatchBaseline",
			body:        `{"BaselineId":"pb-nonexistent"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "DoesNotExistException",
		},
		{
			name:        "ops_metadata_already_exists",
			action:      "CreateOpsMetadata",
			body:        `{"ResourceId":"my-resource"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "OpsMetadataAlreadyExistsException",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
					ResourceID: "my-resource",
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantErrType, resp["__type"])
		})
	}
}
func TestOpsMetadata_FullCRUD(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create.
	meta, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
		Metadata: map[string]ssm.MetadataValue{
			"env": {Value: "production"},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, meta.OpsMetadataArn)

	// List.
	rec := doRequest(t, h, "ListOpsMetadata", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "OpsMetadataList")

	// Get.
	body, _ := json.Marshal(map[string]any{"OpsMetadataArn": meta.OpsMetadataArn})
	rec = doRequest(t, h, "GetOpsMetadata", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "production")

	// Update.
	body, _ = json.Marshal(map[string]any{
		"OpsMetadataArn": meta.OpsMetadataArn,
		"Metadata": map[string]any{
			"version": map[string]any{"Value": "2"},
		},
	})
	rec = doRequest(t, h, "UpdateOpsMetadata", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// Get updated — both keys should be present.
	body, _ = json.Marshal(map[string]any{"OpsMetadataArn": meta.OpsMetadataArn})
	rec = doRequest(t, h, "GetOpsMetadata", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "production")
	assert.Contains(t, rec.Body.String(), "version")

	// Delete.
	body, _ = json.Marshal(map[string]any{"OpsMetadataArn": meta.OpsMetadataArn})
	rec = doRequest(t, h, "DeleteOpsMetadata", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
}

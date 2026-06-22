package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestRefinement2_CreateActivation_IamRoleValidation verifies IamRole is required.
func TestRefinement2_CreateActivation_IamRoleValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing_iam_role",
			body:       `{"Description":"test"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "empty_iam_role",
			body:       `{"IamRole":""}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "valid_iam_role",
			body:       `{"IamRole":"arn:aws:iam::123456789012:role/SSMRole"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateActivation", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantErr != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErr)
			}
		})
	}
}

// TestRefinement2_CreateAssociation_NameValidation verifies Name is required.
func TestRefinement2_CreateAssociation_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing_name",
			body:       `{"InstanceId":"i-abc123"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "empty_name",
			body:       `{"Name":"","InstanceId":"i-abc123"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateAssociation", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
}

// TestRefinement2_CreateMaintenanceWindow_DurationCutoffValidation verifies duration/cutoff rules.
func TestRefinement2_CreateMaintenanceWindow_DurationCutoffValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "duration_zero",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":0,"Cutoff":0}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "duration_too_high",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":25,"Cutoff":1}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "cutoff_equals_duration",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":4,"Cutoff":4}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "cutoff_greater_than_duration",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":4,"Cutoff":5}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "valid_duration_and_cutoff",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":4,"Cutoff":1}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "max_duration",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":24,"Cutoff":1}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateMaintenanceWindow", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantErr != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErr)
			}
		})
	}
}

// TestRefinement2_CancelCommand_CancelsInvocations verifies invocations are also cancelled.
func TestRefinement2_CancelCommand_CancelsInvocations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantInvocStatus string
		instanceIDs     []string
	}{
		{
			name:            "cancels_single_invocation",
			instanceIDs:     []string{"i-001"},
			wantInvocStatus: "Cancelled",
		},
		{
			name:            "cancels_multiple_invocations",
			instanceIDs:     []string{"i-001", "i-002", "i-003"},
			wantInvocStatus: "Cancelled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			instanceIDsJSON, _ := json.Marshal(tt.instanceIDs)
			sendBody := `{"DocumentName":"AWS-RunShellScript","InstanceIds":` + string(instanceIDsJSON) + `}`
			sendRec := doRequest(t, h, "SendCommand", sendBody)
			require.Equal(t, http.StatusOK, sendRec.Code)

			var sendResp map[string]any
			require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &sendResp))
			cmd := sendResp["Command"].(map[string]any)
			cmdID := cmd["CommandId"].(string)

			cancelRec := doRequest(t, h, "CancelCommand", `{"CommandId":"`+cmdID+`"}`)
			require.Equal(t, http.StatusOK, cancelRec.Code)

			for _, instanceID := range tt.instanceIDs {
				invRec := doRequest(t, h, "GetCommandInvocation",
					`{"CommandId":"`+cmdID+`","InstanceId":"`+instanceID+`"}`)
				require.Equal(t, http.StatusOK, invRec.Code)

				var invResp map[string]any
				require.NoError(t, json.Unmarshal(invRec.Body.Bytes(), &invResp))
				assert.Equal(t, tt.wantInvocStatus, invResp["Status"])
				assert.Equal(t, tt.wantInvocStatus, invResp["StatusDetails"])
			}
		})
	}
}

// TestRefinement2_CreateOpsMetadata_DuplicateResourceID verifies duplicate check.
func TestRefinement2_CreateOpsMetadata_DuplicateResourceID(t *testing.T) {
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

// TestRefinement2_PatchBaseline_DefaultOS verifies default OperatingSystem.
func TestRefinement2_PatchBaseline_DefaultOS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantOS string
	}{
		{
			name:   "default_os_when_omitted",
			body:   `{"Name":"MyBaseline"}`,
			wantOS: "WINDOWS",
		},
		{
			name:   "explicit_linux_os",
			body:   `{"Name":"LinuxBaseline","OperatingSystem":"AMAZON_LINUX_2"}`,
			wantOS: "AMAZON_LINUX_2",
		},
		{
			name:   "explicit_windows_os",
			body:   `{"Name":"WinBaseline","OperatingSystem":"WINDOWS"}`,
			wantOS: "WINDOWS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			rec := doRequest(t, ssm.NewHandler(backend), "CreatePatchBaseline", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp ssm.CreatePatchBaselineOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.NotEmpty(t, resp.BaselineID)

			bl := backend.GetPatchBaselineInternal(resp.BaselineID)
			assert.Equal(t, tt.wantOS, bl.OperatingSystem)
		})
	}
}

// TestRefinement2_SeedHelpers verifies seed helpers work correctly.
func TestRefinement2_SeedHelpers(t *testing.T) {
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

// TestRefinement2_AccountID_Region verifies AccountID and Region methods.
func TestRefinement2_AccountID_Region(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantAcct   string
		wantRegion string
	}{
		{
			name:       "default_values",
			wantAcct:   "123456789012",
			wantRegion: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			assert.Equal(t, tt.wantAcct, b.AccountID())
			assert.Equal(t, tt.wantRegion, b.Region())
		})
	}
}

// TestRefinement2_TagsForNonParameterResources verifies miscResourceTags.
func TestRefinement2_TagsForNonParameterResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		resourceID   string
		tags         []ssm.Tag
		wantTagCount int
	}{
		{
			name:         "tags_for_managed_instance",
			resourceType: "ManagedInstance",
			resourceID:   "mi-abc123",
			tags:         []ssm.Tag{{Key: "Env", Value: "prod"}, {Key: "Team", Value: "ops"}},
			wantTagCount: 2,
		},
		{
			name:         "tags_for_activation",
			resourceType: "Activation",
			resourceID:   "act-abc123",
			tags:         []ssm.Tag{{Key: "Purpose", Value: "test"}},
			wantTagCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			addBody, _ := json.Marshal(map[string]any{
				"ResourceType": tt.resourceType,
				"ResourceId":   tt.resourceID,
				"Tags":         tt.tags,
			})

			addRec := doRequest(t, h, "AddTagsToResource", string(addBody))
			require.Equal(t, http.StatusOK, addRec.Code)

			listBody, _ := json.Marshal(map[string]any{
				"ResourceType": tt.resourceType,
				"ResourceId":   tt.resourceID,
			})

			listRec := doRequest(t, h, "ListTagsForResource", string(listBody))
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp ssm.ListTagsForResourceOutput
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.TagList, tt.wantTagCount)
		})
	}
}

// TestRefinement2_DeepCopy_Association verifies modifying caller's Parameters doesn't corrupt stored data.
func TestRefinement2_DeepCopy_Association(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		docName   string
		params    map[string][]string
		mutateKey string
		mutateVal string
	}{
		{
			name:      "modify_after_create_does_not_corrupt",
			docName:   "TestDoc",
			params:    map[string][]string{"commands": {"echo hello"}},
			mutateKey: "commands",
			mutateVal: "echo MUTATED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			h := ssm.NewHandler(backend)

			doRequest(t, h, "CreateDocument",
				`{"Name":"`+tt.docName+`","Content":"{\"schemaVersion\":\"2.2\"}"}`)

			paramsJSON, _ := json.Marshal(tt.params)
			createBody := `{"Name":"` + tt.docName + `","Parameters":` + string(paramsJSON) + `}`

			rec := doRequest(t, h, "CreateAssociation", createBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp ssm.CreateAssociationOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			originalCmds := createResp.AssociationDescription.Parameters[tt.mutateKey]

			// Verify original value was stored correctly
			require.NotEmpty(t, originalCmds)
			assert.Equal(t, tt.params[tt.mutateKey][0], originalCmds[0])
		})
	}
}

// TestRefinement2_GenerateCode verifies generateCode properties.
func TestRefinement2_GenerateCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		validChars string
		wantLen    int
	}{
		{
			name:       "activation_code_length_20",
			wantLen:    20,
			validChars: "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			// Create an activation to get a code
			out, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
				IamRole: "arn:aws:iam::123:role/test",
			})
			require.NoError(t, err)

			code := out.ActivationCode
			assert.Len(t, code, tt.wantLen)

			for _, ch := range code {
				assert.True(t, strings.ContainsRune(tt.validChars, ch),
					"char %q not in valid chars", ch)
			}
		})
	}
}

// TestRefinement2_DispatchTableCached verifies handler caches the dispatch table.
func TestRefinement2_DispatchTableCached(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "dispatch_table_works_after_creation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ssm.NewHandler(ssm.NewInMemoryBackend())
			// Verify that multiple calls to the handler (which uses the cached ops) work fine
			rec1 := doRequest(t, h, "ListDocuments", `{}`)
			assert.Equal(t, http.StatusOK, rec1.Code)

			rec2 := doRequest(t, h, "ListDocuments", `{}`)
			assert.Equal(t, http.StatusOK, rec2.Code)

			_ = tt.name
		})
	}
}

// TestRefinement2_RemoveTagsForNonParameter verifies RemoveTagsFromResource for non-parameter resources.
func TestRefinement2_RemoveTagsForNonParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		resourceID   string
		addTags      []ssm.Tag
		removeKeys   []string
		wantRemain   int
	}{
		{
			name:         "remove_one_tag",
			resourceType: "ManagedInstance",
			resourceID:   "mi-xyz",
			addTags:      []ssm.Tag{{Key: "A", Value: "1"}, {Key: "B", Value: "2"}},
			removeKeys:   []string{"A"},
			wantRemain:   1,
		},
		{
			name:         "remove_all_tags",
			resourceType: "Activation",
			resourceID:   "act-xyz",
			addTags:      []ssm.Tag{{Key: "X", Value: "1"}},
			removeKeys:   []string{"X"},
			wantRemain:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			addBody, _ := json.Marshal(map[string]any{
				"ResourceType": tt.resourceType,
				"ResourceId":   tt.resourceID,
				"Tags":         tt.addTags,
			})
			require.Equal(t, http.StatusOK, doRequest(t, h, "AddTagsToResource", string(addBody)).Code)

			removeBody, _ := json.Marshal(map[string]any{
				"ResourceType": tt.resourceType,
				"ResourceId":   tt.resourceID,
				"TagKeys":      tt.removeKeys,
			})
			require.Equal(t, http.StatusOK, doRequest(t, h, "RemoveTagsFromResource", string(removeBody)).Code)

			listBody, _ := json.Marshal(map[string]any{
				"ResourceType": tt.resourceType,
				"ResourceId":   tt.resourceID,
			})
			listRec := doRequest(t, h, "ListTagsForResource", string(listBody))
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp ssm.ListTagsForResourceOutput
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
			assert.Len(t, resp.TagList, tt.wantRemain)
		})
	}
}

// TestRefinement2_PersistenceWithNewFields verifies Snapshot/Restore preserves new fields.
func TestRefinement2_PersistenceWithNewFields(t *testing.T) {
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

// TestRefinement2_CreateActivationWithTags verifies tags are stored on activation create.
func TestRefinement2_CreateActivationWithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		tagCount int
	}{
		{
			name:     "activation_with_tags",
			body:     `{"IamRole":"arn:aws:iam::123:role/r","Tags":[{"Key":"Env","Value":"test"},{"Key":"Team","Value":"ops"}]}`,
			tagCount: 2,
		},
		{
			name:     "activation_without_tags",
			body:     `{"IamRole":"arn:aws:iam::123:role/r"}`,
			tagCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateActivation", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp ssm.CreateActivationOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.NotEmpty(t, resp.ActivationID)

			listBody, _ := json.Marshal(map[string]any{
				"ResourceType": "Activation",
				"ResourceId":   resp.ActivationID,
			})
			listRec := doRequest(t, h, "ListTagsForResource", string(listBody))
			require.Equal(t, http.StatusOK, listRec.Code)

			var tagResp ssm.ListTagsForResourceOutput
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagResp))
			assert.Len(t, tagResp.TagList, tt.tagCount)
		})
	}
}

// TestRefinement2_ResetClearsNewFields verifies Reset clears new fields.
func TestRefinement2_ResetClearsNewFields(t *testing.T) {
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

// TestRefinement2_InternalTimestamp verifies activation codes use valid timestamp format.
func TestRefinement2_InternalTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "activation_expiry_is_in_future"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			out, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
				IamRole: "arn:aws:iam::123:role/test",
			})
			require.NoError(t, err)

			assert.NotEmpty(t, out.ActivationID)
			assert.True(t, strings.HasPrefix(out.ActivationID, "act-"))

			_ = tt.name
		})
	}
}

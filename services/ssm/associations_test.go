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

func TestAssociationOps(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create association
	assoc, err := b.CreateAssociation(context.TODO(), &ssm.CreateAssociationInput{
		Name:       "AWS-RunShellScript",
		InstanceID: "i-001",
	})
	require.NoError(t, err)

	assocID := assoc.AssociationDescription.AssociationID

	// ListAssociationVersions
	rec := doRequest(t, h, "ListAssociationVersions", `{"AssociationId":"`+assocID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "AssociationVersions")

	// DescribeAssociationExecutions
	rec = doRequest(t, h, "DescribeAssociationExecutions", `{"AssociationId":"`+assocID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeEffectiveInstanceAssociations
	rec = doRequest(t, h, "DescribeEffectiveInstanceAssociations", `{"InstanceId":"i-001"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Associations")

	// DescribeInstanceAssociationsStatus
	rec = doRequest(t, h, "DescribeInstanceAssociationsStatus", `{"InstanceId":"i-001"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}
func TestUpdateAssociationStatus_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		instanceID string
		assocName  string
	}{
		{
			name:       "no_matching_association_returns_error",
			instanceID: "i-does-not-exist",
			assocName:  "AWS-RunShellScript",
		},
		{
			name:       "empty_ids_return_error",
			instanceID: "",
			assocName:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.UpdateAssociationStatus(context.TODO(), &ssm.UpdateAssociationStatusInput{
				InstanceID: tt.instanceID,
				Name:       tt.assocName,
				AssociationStatus: ssm.AssociationStatusValue{
					Name: "Success",
				},
			})
			require.ErrorIs(t, err, ssm.ErrAssociationNotFound,
				"no-match UpdateAssociationStatus must return ErrAssociationNotFound")
		})
	}
}
func TestUpdateAssociationStatus_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusName string
	}{
		{
			name:       "status_updated_to_success",
			statusName: "Success",
		},
		{
			name:       "status_updated_to_failed",
			statusName: "Failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)

			assocOut, err := b.CreateAssociation(context.TODO(), &ssm.CreateAssociationInput{
				Name:       "AWS-RunShellScript",
				InstanceID: "i-abc123",
			})
			require.NoError(t, err)

			out, err := b.UpdateAssociationStatus(context.TODO(), &ssm.UpdateAssociationStatusInput{
				InstanceID: "i-abc123",
				Name:       "AWS-RunShellScript",
				AssociationStatus: ssm.AssociationStatusValue{
					Name: tt.statusName,
				},
			})
			require.NoError(t, err)
			require.NotNil(t, out)
			require.NotNil(t, out.AssociationDescription.Overview)
			assert.Equal(t, tt.statusName, out.AssociationDescription.Overview.Status)
			assert.Equal(t, assocOut.AssociationDescription.AssociationID, out.AssociationDescription.AssociationID)
		})
	}
}
func TestUpdateAssociationStatus_Handler_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "no_matching_association_returns_400",
			body: `{"InstanceId":"i-ghost","Name":"AWS-RunShellScript","AssociationStatus":{"Name":"Success"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "UpdateAssociationStatus", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "AssociationDoesNotExist")
		})
	}
}
func TestAssociationExecutions_Stable(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.TODO()

	create, err := b.CreateAssociation(ctx, &ssm.CreateAssociationInput{
		Name:       "AWS-RunShellScript",
		InstanceID: "i-0123456789abcdef0",
	})
	require.NoError(t, err)
	assocID := create.AssociationDescription.AssociationID

	first, err := b.DescribeAssociationExecutions(ctx, &ssm.DescribeAssociationExecutionsInput{
		AssociationID: assocID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, first.AssociationExecutions)
	execID := first.AssociationExecutions[0].ExecutionID
	require.NotEmpty(t, execID)

	// A second call must return the SAME execution ID (not a fresh UUID).
	second, err := b.DescribeAssociationExecutions(ctx, &ssm.DescribeAssociationExecutionsInput{
		AssociationID: assocID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, second.AssociationExecutions)
	assert.Equal(t, execID, second.AssociationExecutions[0].ExecutionID,
		"association execution ID must be stable across calls")

	// Targets for that execution must be stable and reference the instance.
	targetsA, err := b.DescribeAssociationExecutionTargets(
		ctx,
		&ssm.DescribeAssociationExecutionTargetsInput{
			AssociationID: assocID,
			ExecutionID:   execID,
		},
	)
	require.NoError(t, err)
	require.Len(t, targetsA.AssociationExecutionTargets, 1)
	assert.Equal(t, "i-0123456789abcdef0", targetsA.AssociationExecutionTargets[0].ResourceID)
	assert.Equal(t, execID, targetsA.AssociationExecutionTargets[0].ExecutionID)

	targetsB, err := b.DescribeAssociationExecutionTargets(
		ctx,
		&ssm.DescribeAssociationExecutionTargetsInput{
			AssociationID: assocID,
			ExecutionID:   execID,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, targetsA.AssociationExecutionTargets, targetsB.AssociationExecutionTargets,
		"association execution targets must be stable across calls")

	// StartAssociationsOnce appends a new stable execution record.
	_, err = b.StartAssociationsOnce(ctx, &ssm.StartAssociationsOnceInput{
		AssociationIDs: []string{assocID},
	})
	require.NoError(t, err)
	assert.Equal(t, 2, b.AssociationExecutionCount(assocID),
		"a one-time run must append a new execution record")
}

// TestStubOps_DeleteAssociation exercises the association-backed delete stub.
func TestStubOps_DeleteAssociation(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create an association so we have a valid ID.
	assoc, err := b.CreateAssociation(context.TODO(), &ssm.CreateAssociationInput{
		Name:       "AWS-RunShellScript",
		InstanceID: "i-1234567890abcdef0",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(
		map[string]any{"AssociationId": assoc.AssociationDescription.AssociationID},
	)
	rec := doRequest(t, h, "DeleteAssociation", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_DescribeAssociation exercises that stub.
func TestStubOps_DescribeAssociation(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	assoc, err := b.CreateAssociation(context.TODO(), &ssm.CreateAssociationInput{
		Name:       "AWS-RunShellScript",
		InstanceID: "i-1234567890abcdef0",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(
		map[string]any{"AssociationId": assoc.AssociationDescription.AssociationID},
	)
	rec := doRequest(t, h, "DescribeAssociation", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_UpdateAssociation exercises the update stub.
func TestStubOps_UpdateAssociation(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	assoc, err := b.CreateAssociation(context.TODO(), &ssm.CreateAssociationInput{
		Name:       "AWS-RunShellScript",
		InstanceID: "i-1234567890abcdef0",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(
		map[string]any{"AssociationId": assoc.AssociationDescription.AssociationID},
	)
	rec := doRequest(t, h, "UpdateAssociation", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}
func TestCreateAssociation_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "associates_existing_document", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)

			doRequest(
				t,
				h,
				"CreateDocument",
				`{"Name":"MyDoc","Content":"{\"schemaVersion\":\"2.2\"}","DocumentType":"Command"}`,
			)

			rec := doRequest(t, h, "CreateAssociation", `{"Name":"MyDoc","InstanceId":"i-abc123"}`)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateAssociationOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.AssociationDescription.AssociationID)
			assert.Equal(t, "MyDoc", resp.AssociationDescription.Name)
			assert.Equal(t, 1, backend.AssociationCount())
		})
	}
}
func TestCreateAssociationBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantStatus     int
		wantSuccessful int
		wantFailed     int
		setupDoc       bool
	}{
		{
			name:           "all_succeed",
			setupDoc:       true,
			body:           `{"Entries":[{"Name":"MyDoc","InstanceId":"i-1"},{"Name":"MyDoc","InstanceId":"i-2"}]}`,
			wantStatus:     http.StatusOK,
			wantSuccessful: 2,
			wantFailed:     0,
		},
		{
			name:           "partial_failure",
			setupDoc:       true,
			body:           `{"Entries":[{"Name":"MyDoc","InstanceId":"i-1"},{"Name":"NoSuchDoc","InstanceId":"i-2"}]}`,
			wantStatus:     http.StatusOK,
			wantSuccessful: 1,
			wantFailed:     1,
		},
		{
			name:           "all_fail",
			setupDoc:       false,
			body:           `{"Entries":[{"Name":"BadDoc1"},{"Name":"BadDoc2"}]}`,
			wantStatus:     http.StatusOK,
			wantSuccessful: 0,
			wantFailed:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)

			if tt.setupDoc {
				doRequest(
					t,
					h,
					"CreateDocument",
					`{"Name":"MyDoc","Content":"{\"schemaVersion\":\"2.2\"}","DocumentType":"Command"}`,
				)
			}

			rec := doRequest(t, h, "CreateAssociationBatch", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateAssociationBatchOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.Successful, tt.wantSuccessful)
			assert.Len(t, resp.Failed, tt.wantFailed)
			assert.Equal(t, tt.wantSuccessful, backend.AssociationCount())
		})
	}
}

// TestCreateAssociation_NameValidation verifies Name is required.
func TestCreateAssociation_NameValidation(t *testing.T) {
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
func TestFull_Association_CreateDescribeUpdate(t *testing.T) {
	t.Parallel()
	h := newHandler()

	// Need a document
	postJSON(t, h, "CreateDocument", map[string]any{
		"Name":    "AssocDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	// Create association
	code, out := postJSON(t, h, "CreateAssociation", map[string]any{
		"Name":       "AssocDoc",
		"InstanceId": "i-assoc001",
		"Parameters": map[string]any{"cmd": []string{"echo test"}},
	})
	assert.Equal(t, http.StatusOK, code)
	assocID := out["AssociationDescription"].(map[string]any)["AssociationId"].(string)
	assert.NotEmpty(t, assocID)

	// Describe
	code, out = postJSON(t, h, "DescribeAssociation", map[string]any{"AssociationId": assocID})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["AssociationDescription"])

	// Update
	code, _ = postJSON(t, h, "UpdateAssociation", map[string]any{
		"AssociationId":   assocID,
		"AssociationName": "UpdatedAssoc",
	})
	assert.Equal(t, http.StatusOK, code)
}

// TestCopyAssocTargets exercises the copyAssocTargets nil/non-nil paths.
func TestCopyAssocTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		targets []ssm.AssociationTarget
	}{
		{
			name:    "nil_targets",
			targets: nil,
		},
		{
			name: "non_nil_targets",
			targets: []ssm.AssociationTarget{
				{Key: "tag:Env", Values: []string{"prod"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			// CreateAssociationBatch exercises copyAssocTargets internally
			out, err := b.CreateAssociationBatch(context.TODO(), &ssm.CreateAssociationBatchInput{
				Entries: []ssm.CreateAssociationBatchRequestEntry{
					{
						Name:    "AWS-RunShellScript",
						Targets: tt.targets,
					},
				},
			})
			require.NoError(t, err)
			assert.Len(t, out.Successful, 1)
		})
	}
}

// TestDeleteAssociation_NotFound exercises the error path.
func TestDeleteAssociation_NotFound(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, err := b.DeleteAssociation(context.TODO(), &ssm.DeleteAssociationInput{AssociationID: "nonexistent"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrAssociationNotFound)
}

// TestUpdateAssociation covers update branches including nil vs non-nil targets.
func TestUpdateAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		update  ssm.UpdateAssociationInput
		wantErr bool
	}{
		{
			name: "not_found",
			update: ssm.UpdateAssociationInput{
				AssociationID: "does-not-exist",
			},
			wantErr: true,
		},
		{
			name: "update_name_and_version",
			update: ssm.UpdateAssociationInput{
				AssociationName: "updated-name",
				DocumentVersion: "$LATEST",
			},
			wantErr: false,
		},
		{
			name: "update_with_targets",
			update: ssm.UpdateAssociationInput{
				Targets: []ssm.AssociationTarget{
					{Key: "tag:Env", Values: []string{"staging"}},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()

			if !tt.wantErr {
				// Create an association first
				out, err := b.CreateAssociation(context.TODO(), &ssm.CreateAssociationInput{
					Name: "AWS-RunShellScript",
				})
				require.NoError(t, err)
				tt.update.AssociationID = out.AssociationDescription.AssociationID
			}

			_, err := b.UpdateAssociation(context.TODO(), &tt.update)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrAssociationNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestDeleteAssociation_TableDriven verifies success and not-found for DeleteAssociation.
func TestDeleteAssociation_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		wantStatus int
		setupFirst bool
	}{
		{
			name:       "deletes_existing_association",
			setupFirst: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_association_returns_error",
			setupFirst: false,
			wantStatus: http.StatusBadRequest,
			wantErrMsg: "AssociationDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			assocID := "assoc-nonexistent"
			if tt.setupFirst {
				assoc, err := b.CreateAssociation(context.Background(), &ssm.CreateAssociationInput{
					Name:       "AWS-RunShellScript",
					InstanceID: "i-1234567890abcdef0",
				})
				require.NoError(t, err)
				assocID = assoc.AssociationDescription.AssociationID
			}

			body, _ := json.Marshal(map[string]any{"AssociationId": assocID})
			rec := doRequest(t, h, "DeleteAssociation", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrMsg != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrMsg)
			}
		})
	}
}
func TestDescribeAssociationExecutionTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input         ssm.DescribeAssociationExecutionTargetsInput
		name          string
		assocInstance string
		targets       []ssm.AssociationTarget
		wantCount     int
	}{
		{
			name:      "empty_assoc_id_returns_empty",
			input:     ssm.DescribeAssociationExecutionTargetsInput{AssociationID: ""},
			wantCount: 0,
		},
		{
			name:      "unknown_assoc_returns_empty",
			input:     ssm.DescribeAssociationExecutionTargetsInput{AssociationID: "assoc-does-not-exist"},
			wantCount: 0,
		},
		{
			name:          "instance_only_returns_one_target",
			assocInstance: "i-1234567890abcdef0",
			wantCount:     1,
		},
		{
			name:      "explicit_targets_returned",
			targets:   []ssm.AssociationTarget{{Key: "InstanceIds", Values: []string{"i-aaa", "i-bbb"}}},
			wantCount: 2,
		},
		{
			name:          "instance_plus_targets",
			assocInstance: "i-base",
			targets:       []ssm.AssociationTarget{{Key: "InstanceIds", Values: []string{"i-extra"}}},
			wantCount:     2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			ctx := context.Background()

			var assocID string
			if tc.assocInstance != "" || len(tc.targets) > 0 {
				out, err := b.CreateAssociation(ctx, &ssm.CreateAssociationInput{
					Name:       "AWS-RunShellScript",
					InstanceID: tc.assocInstance,
					Targets:    tc.targets,
				})
				require.NoError(t, err)
				assocID = out.AssociationDescription.AssociationID
			}

			input := tc.input
			if assocID != "" {
				input.AssociationID = assocID
			}

			out, err := b.DescribeAssociationExecutionTargets(ctx, &input)
			require.NoError(t, err)
			assert.Len(t, out.AssociationExecutionTargets, tc.wantCount)

			// All returned targets must carry the association ID.
			for _, tgt := range out.AssociationExecutionTargets {
				assert.Equal(t, assocID, tgt.AssociationID)
				assert.NotEmpty(t, tgt.ExecutionID)
				assert.NotEmpty(t, tgt.Status)
			}
		})
	}
}

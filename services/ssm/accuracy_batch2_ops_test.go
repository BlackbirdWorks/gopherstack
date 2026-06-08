package ssm_test

// accuracy_batch2_ops_test.go — AWS-accuracy audit for SSM batch-2 ops (go-p80r).
//
// Covers four concrete deviations from AWS behaviour found in backend_batch2.go:
//
//  1. GetAutomationExecution: non-existent ID returned empty struct; AWS returns
//     AutomationExecutionNotFoundException.
//
//  2. DeleteResourceDataSync: non-existent SyncName silently succeeded; AWS returns
//     ResourceDataSyncNotFoundException.
//
//  3. LabelParameterVersion: labelling a non-existent parameter silently stored
//     orphan label state; AWS returns ParameterNotFound.
//
//  4. UpdateAssociationStatus: no matching InstanceID+Name returned an empty success
//     body; AWS returns AssociationDoesNotExist.

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// ---------------------------------------------------------------------------
// Issue #1 — GetAutomationExecution with non-existent ID must return error
// ---------------------------------------------------------------------------

func TestBatch2Ops_GetAutomationExecution_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		execID string
	}{
		{
			name:   "backend_empty_id_returns_error",
			execID: "",
		},
		{
			name:   "backend_unknown_id_returns_error",
			execID: "auto-does-not-exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.GetAutomationExecution(context.TODO(), &ssm.GetAutomationExecutionInput{
				AutomationExecutionID: tt.execID,
			})
			require.ErrorIs(t, err, ssm.ErrAutomationExecutionNotFound,
				"non-existent execution must return ErrAutomationExecutionNotFound")
		})
	}
}

func TestBatch2Ops_GetAutomationExecution_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		docID string
	}{
		{
			name:  "started_execution_is_returned",
			docID: "AWS-RunPatchBaseline",
		},
		{
			name:  "started_execution_with_version",
			docID: "AWS-ApplyPatchBaseline",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			out, err := b.StartAutomationExecution(context.TODO(), &ssm.StartAutomationExecutionInput{
				DocumentName: tt.docID,
			})
			require.NoError(t, err)
			require.NotEmpty(t, out.AutomationExecutionID)

			got, err := b.GetAutomationExecution(context.TODO(), &ssm.GetAutomationExecutionInput{
				AutomationExecutionID: out.AutomationExecutionID,
			})
			require.NoError(t, err)
			require.NotNil(t, got.AutomationExecution)
			assert.Equal(t, out.AutomationExecutionID, got.AutomationExecution.AutomationExecutionID)
			assert.Equal(t, tt.docID, got.AutomationExecution.DocumentName)
		})
	}
}

func TestBatch2Ops_GetAutomationExecution_Handler_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "empty_body_returns_error",
			body: `{}`,
			want: http.StatusInternalServerError,
		},
		{
			name: "unknown_id_returns_error",
			body: `{"AutomationExecutionId":"auto-ghost"}`,
			want: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "GetAutomationExecution", tt.body)
			assert.Equal(t, tt.want, rec.Code)
			assert.Contains(t, rec.Body.String(), "AutomationExecutionNotFoundException")
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #2 — DeleteResourceDataSync with non-existent SyncName must return error
// ---------------------------------------------------------------------------

func TestBatch2Ops_DeleteResourceDataSync_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		syncName string
	}{
		{
			name:     "unknown_sync_name_returns_error",
			syncName: "ghost-sync",
		},
		{
			name:     "another_unknown_sync_returns_error",
			syncName: "never-created",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.DeleteResourceDataSync(context.TODO(), &ssm.DeleteResourceDataSyncInput{
				SyncName: tt.syncName,
			})
			require.ErrorIs(t, err, ssm.ErrResourceDataSyncNotFound,
				"non-existent sync must return ErrResourceDataSyncNotFound")
		})
	}
}

func TestBatch2Ops_DeleteResourceDataSync_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		syncName string
		syncType string
	}{
		{
			name:     "created_sync_can_be_deleted",
			syncName: "my-s3-sync",
			syncType: "SyncToDestination",
		},
		{
			name:     "default_sync_type",
			syncName: "another-sync",
			syncType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)

			_, err := b.CreateResourceDataSync(context.TODO(), &ssm.CreateResourceDataSyncInput{
				SyncName: tt.syncName,
				SyncType: tt.syncType,
			})
			require.NoError(t, err)

			_, err = b.DeleteResourceDataSync(context.TODO(), &ssm.DeleteResourceDataSyncInput{
				SyncName: tt.syncName,
			})
			require.NoError(t, err)

			// Second delete must fail.
			_, err = b.DeleteResourceDataSync(context.TODO(), &ssm.DeleteResourceDataSyncInput{
				SyncName: tt.syncName,
			})
			require.ErrorIs(t, err, ssm.ErrResourceDataSyncNotFound,
				"second delete of removed sync must return ErrResourceDataSyncNotFound")
		})
	}
}

func TestBatch2Ops_DeleteResourceDataSync_Handler_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "non_existent_sync_returns_500",
			body: `{"SyncName":"ghost-sync"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "DeleteResourceDataSync", tt.body)
			assert.Equal(t, http.StatusInternalServerError, rec.Code)
			assert.Contains(t, rec.Body.String(), "ResourceDataSyncNotFoundException")
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #3 — LabelParameterVersion on non-existent parameter must return error
// ---------------------------------------------------------------------------

func TestBatch2Ops_LabelParameterVersion_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		paramName string
	}{
		{
			name:      "non_existent_parameter_returns_error",
			paramName: "/does/not/exist",
		},
		{
			name:      "another_non_existent_parameter",
			paramName: "ghost-param",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.LabelParameterVersion(context.TODO(), &ssm.LabelParameterVersionInput{
				Name:   tt.paramName,
				Labels: []string{"test-label"},
			})
			require.ErrorIs(t, err, ssm.ErrParameterNotFound,
				"labelling non-existent parameter must return ErrParameterNotFound")
		})
	}
}

func TestBatch2Ops_LabelParameterVersion_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		labels []string
	}{
		{
			name:   "single_label_applied",
			labels: []string{"my-label"},
		},
		{
			name:   "multiple_labels_applied",
			labels: []string{"alpha", "beta"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)

			_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
				Name:  "/test/label-param",
				Value: "myvalue",
				Type:  "String",
			})
			require.NoError(t, err)

			out, err := b.LabelParameterVersion(context.TODO(), &ssm.LabelParameterVersionInput{
				Name:   "/test/label-param",
				Labels: tt.labels,
			})
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.labels, out.AddedLabels)
			assert.Empty(t, out.InvalidLabels)
		})
	}
}

func TestBatch2Ops_LabelParameterVersion_Handler_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "non_existent_param_returns_400",
			body: `{"Name":"/no/such/param","Labels":["lbl"]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "LabelParameterVersion", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "ParameterNotFound")
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #4 — UpdateAssociationStatus with no matching association must return error
// ---------------------------------------------------------------------------

func TestBatch2Ops_UpdateAssociationStatus_NotFound(t *testing.T) {
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

func TestBatch2Ops_UpdateAssociationStatus_RoundTrip(t *testing.T) {
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

func TestBatch2Ops_UpdateAssociationStatus_Handler_NotFound(t *testing.T) {
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

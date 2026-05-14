package sagemaker_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCreateNotebookInstance_Validation validates that the SageMaker mock
// matches the AWS contract: NotebookInstanceName, InstanceType, and RoleArn
// are all required parameters.
func TestCreateNotebookInstance_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "all_fields_present_succeeds",
			body: map[string]any{
				"NotebookInstanceName": "ok-nb",
				"InstanceType":         "ml.t2.medium",
				"RoleArn":              "arn:aws:iam::000000000000:role/r",
			},
			wantErr: false,
		},
		{
			name: "missing_role_arn_rejected",
			body: map[string]any{
				"NotebookInstanceName": "no-role",
				"InstanceType":         "ml.t2.medium",
			},
			wantErr: true,
		},
		{
			name: "missing_instance_type_rejected",
			body: map[string]any{
				"NotebookInstanceName": "no-it",
				"RoleArn":              "arn:aws:iam::000000000000:role/r",
			},
			wantErr: true,
		},
		{
			name: "missing_name_rejected",
			body: map[string]any{
				"InstanceType": "ml.t2.medium",
				"RoleArn":      "arn:aws:iam::000000000000:role/r",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateNotebookInstance", tt.body)

			if tt.wantErr {
				assert.NotEqual(t, http.StatusOK, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

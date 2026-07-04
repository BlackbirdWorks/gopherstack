package sagemaker_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_CreateModelRequiresExecutionRoleArn verifies that CreateModel rejects
// requests with a missing ExecutionRoleArn. Real AWS requires this field on all
// CreateModel calls; the emulator previously created models with an empty role ARN.
func TestParity_CreateModelRequiresExecutionRoleArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "absent_role_arn_rejected",
			body: map[string]any{
				"ModelName": "my-model",
				"PrimaryContainer": map[string]any{
					"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_role_arn_rejected",
			body: map[string]any{
				"ModelName":        "my-model",
				"ExecutionRoleArn": "",
				"PrimaryContainer": map[string]any{
					"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid_role_arn_accepted",
			body: map[string]any{
				"ModelName":        "my-model",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
				"PrimaryContainer": map[string]any{
					"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateModel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateModel status for case %q", tt.name)
		})
	}
}

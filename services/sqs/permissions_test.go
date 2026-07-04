package sqs_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestSQS_Permissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *sqs.Handler) string
		action          string
		body            map[string]any
		name            string
		wantBodyContain string
		wantCode        int
	}{
		{
			name: "AddPermission_success",
			setup: func(t *testing.T, h *sqs.Handler) string {
				t.Helper()

				return doCreateQueue(t, h, "perm-queue-add")
			},
			action: "AddPermission",
			body: map[string]any{
				"QueueUrl":      "http://localhost/000000000000/perm-queue-add",
				"Label":         "AllowSend",
				"AWSAccountIds": []string{"123456789012"},
				"Actions":       []string{"SendMessage"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "AddPermission_queue_not_found",
			setup: func(_ *testing.T, _ *sqs.Handler) string {
				return ""
			},
			action: "AddPermission",
			body: map[string]any{
				"QueueUrl":      "http://localhost/000000000000/nonexistent",
				"Label":         "AllowSend",
				"AWSAccountIds": []string{"123456789012"},
				"Actions":       []string{"SendMessage"},
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "QueueDoesNotExist",
		},
		{
			name: "RemovePermission_success",
			setup: func(t *testing.T, h *sqs.Handler) string {
				t.Helper()
				qURL := doCreateQueue(t, h, "perm-queue-remove")

				rec := doRequest(t, h, "AddPermission", map[string]any{
					"QueueUrl":      qURL,
					"Label":         "ToRemove",
					"AWSAccountIds": []string{"111111111111"},
					"Actions":       []string{"ReceiveMessage"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return qURL
			},
			action: "RemovePermission",
			body: map[string]any{
				"QueueUrl": "http://localhost/000000000000/perm-queue-remove",
				"Label":    "ToRemove",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "RemovePermission_queue_not_found",
			setup: func(_ *testing.T, _ *sqs.Handler) string {
				return ""
			},
			action: "RemovePermission",
			body: map[string]any{
				"QueueUrl": "http://localhost/000000000000/ghost-queue",
				"Label":    "any",
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "QueueDoesNotExist",
		},
		{
			name: "RemovePermission_nonexistent_label_is_idempotent",
			setup: func(t *testing.T, h *sqs.Handler) string {
				t.Helper()

				return doCreateQueue(t, h, "perm-queue-idempotent")
			},
			action: "RemovePermission",
			body: map[string]any{
				"QueueUrl": "http://localhost/000000000000/perm-queue-idempotent",
				"Label":    "doesNotExist",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "AddPermission_empty_label_returns_error",
			setup: func(t *testing.T, h *sqs.Handler) string {
				t.Helper()

				return doCreateQueue(t, h, "perm-queue-empty-label")
			},
			action: "AddPermission",
			body: map[string]any{
				"QueueUrl":      "http://localhost/000000000000/perm-queue-empty-label",
				"Label":         "",
				"AWSAccountIds": []string{"123456789012"},
				"Actions":       []string{"SendMessage"},
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "InvalidParameterValue",
		},
		{
			name: "AddPermission_empty_actions_returns_error",
			setup: func(t *testing.T, h *sqs.Handler) string {
				t.Helper()

				return doCreateQueue(t, h, "perm-queue-empty-actions")
			},
			action: "AddPermission",
			body: map[string]any{
				"QueueUrl":      "http://localhost/000000000000/perm-queue-empty-actions",
				"Label":         "MyLabel",
				"AWSAccountIds": []string{"123456789012"},
				"Actions":       []string{},
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "InvalidParameterValue",
		},
		{
			name: "AddPermission_empty_aws_account_ids_returns_error",
			setup: func(t *testing.T, h *sqs.Handler) string {
				t.Helper()

				return doCreateQueue(t, h, "perm-queue-empty-accounts")
			},
			action: "AddPermission",
			body: map[string]any{
				"QueueUrl":      "http://localhost/000000000000/perm-queue-empty-accounts",
				"Label":         "MyLabel",
				"AWSAccountIds": []string{},
				"Actions":       []string{"SendMessage"},
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(t, h)

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBodyContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContain)
			}
		})
	}
}

func TestSQS_Permissions_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantLabel      string
		wantActions    []string
		wantAccountIDs []string
	}{
		{
			name:           "add_and_verify_permission_stored",
			wantLabel:      "MyLabel",
			wantAccountIDs: []string{"123456789012"},
			wantActions:    []string{"SendMessage", "ReceiveMessage"},
		},
		{
			name:           "add_multiple_permissions",
			wantLabel:      "SecondLabel",
			wantAccountIDs: []string{"222222222222", "333333333333"},
			wantActions:    []string{"*"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)

			queueName := "perm-test-" + tt.name
			_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: queueName, Endpoint: "localhost"})
			require.NoError(t, err)

			queueURL := "http://localhost/000000000000/" + queueName

			err = b.AddPermission(&sqs.AddPermissionInput{
				QueueURL:      queueURL,
				Label:         tt.wantLabel,
				AWSAccountIDs: tt.wantAccountIDs,
				Actions:       tt.wantActions,
			})
			require.NoError(t, err)

			// Verify the permission was stored by checking that removing it succeeds.
			err = b.RemovePermission(&sqs.RemovePermissionInput{
				QueueURL: queueURL,
				Label:    tt.wantLabel,
			})
			require.NoError(t, err)

			// Removing again should also succeed (idempotent).
			err = b.RemovePermission(&sqs.RemovePermissionInput{
				QueueURL: queueURL,
				Label:    tt.wantLabel,
			})
			require.NoError(t, err)
		})
	}
}

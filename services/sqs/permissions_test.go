package sqs_test

import (
	"encoding/json"
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

func TestAddPermission_UpdatesPolicyAttribute(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-policy")

	require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "AllowSend",
		AWSAccountIDs: []string{"123456789012"},
		Actions:       []string{"SendMessage"},
	}))

	attrs := b2getAttrs(t, b, qURL, "Policy")
	policyJSON, ok := attrs["Policy"]
	require.True(t, ok, "Policy attribute should be set after AddPermission")
	assert.NotEmpty(t, policyJSON)

	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(policyJSON), &doc))
	assert.Equal(t, "2012-10-17", doc["Version"])

	stmts := doc["Statement"].([]any)
	require.Len(t, stmts, 1)

	stmt := stmts[0].(map[string]any)
	assert.Equal(t, "AllowSend", stmt["Sid"])
	assert.Equal(t, "Allow", stmt["Effect"])
}

func TestAddPermission_WildcardAction(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-wildcard")

	require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "AllowAll",
		AWSAccountIDs: []string{"*"},
		Actions:       []string{"*"},
	}))

	attrs := b2getAttrs(t, b, qURL, "Policy")
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(attrs["Policy"]), &doc))
	stmts := doc["Statement"].([]any)
	stmt := stmts[0].(map[string]any)
	actions := stmt["Action"].([]any)
	assert.Contains(t, actions, "sqs:*")
}

func TestRemovePermission_ClearsPolicyAttribute(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-remove")

	require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "ToRemove",
		AWSAccountIDs: []string{"111111111111"},
		Actions:       []string{"ReceiveMessage"},
	}))

	require.NoError(t, b.RemovePermission(&sqs.RemovePermissionInput{
		QueueURL: qURL,
		Label:    "ToRemove",
	}))

	attrs := b2getAttrs(t, b, qURL, "Policy")
	_, hasPolicy := attrs["Policy"]
	assert.False(t, hasPolicy, "Policy attribute should be removed when all permissions are deleted")
}

func TestAddPermission_MultipleStatements(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-multi")

	require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "Stmt1",
		AWSAccountIDs: []string{"111111111111"},
		Actions:       []string{"SendMessage"},
	}))
	require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "Stmt2",
		AWSAccountIDs: []string{"222222222222"},
		Actions:       []string{"ReceiveMessage"},
	}))

	attrs := b2getAttrs(t, b, qURL, "Policy")
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(attrs["Policy"]), &doc))
	stmts := doc["Statement"].([]any)
	assert.Len(t, stmts, 2)
}

func TestRemovePermission_PartialRemoval(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-partial")

	for _, label := range []string{"Stmt1", "Stmt2"} {
		require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
			QueueURL:      qURL,
			Label:         label,
			AWSAccountIDs: []string{"123456789012"},
			Actions:       []string{"SendMessage"},
		}))
	}

	require.NoError(t, b.RemovePermission(&sqs.RemovePermissionInput{
		QueueURL: qURL,
		Label:    "Stmt1",
	}))

	attrs := b2getAttrs(t, b, qURL, "Policy")
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(attrs["Policy"]), &doc))
	stmts := doc["Statement"].([]any)
	assert.Len(t, stmts, 1)
	stmt := stmts[0].(map[string]any)
	assert.Equal(t, "Stmt2", stmt["Sid"])
}

func TestAddPermission_EmptyLabel_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-empty-label")
	err := b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "",
		AWSAccountIDs: []string{"123456789012"},
		Actions:       []string{"SendMessage"},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidPermissionLabel)
}

func TestAddPermission_EmptyActions_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-empty-actions")
	err := b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "L",
		AWSAccountIDs: []string{"123456789012"},
		Actions:       []string{},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidPermissionActions)
}

func TestAddPermission_EmptyAccountIDs_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-empty-accounts")
	err := b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "L",
		AWSAccountIDs: []string{},
		Actions:       []string{"SendMessage"},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidPermissionAccountIDs)
}

func TestAddPermission_QueueNotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	err := b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      "http://localhost/000000000000/ghost",
		Label:         "L",
		AWSAccountIDs: []string{"123456789012"},
		Actions:       []string{"SendMessage"},
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestRemovePermission_Idempotent(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-idempotent")

	// Remove non-existent label — should not error (AWS is idempotent)
	require.NoError(t, b.RemovePermission(&sqs.RemovePermissionInput{
		QueueURL: qURL,
		Label:    "NoSuchLabel",
	}))
}

// TestSQS_AddPermission_Validation tests input validation for AddPermission.
func TestSQS_AddPermission_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		input     sqs.AddPermissionInput
	}{
		{
			name: "empty_label_returns_error",
			input: sqs.AddPermissionInput{
				QueueURL:      "http://localhost/000000000000/q",
				Label:         "",
				AWSAccountIDs: []string{"123456789012"},
				Actions:       []string{"SendMessage"},
			},
			wantErrIs: sqs.ErrInvalidPermissionLabel,
		},
		{
			name: "empty_actions_returns_error",
			input: sqs.AddPermissionInput{
				QueueURL:      "http://localhost/000000000000/q",
				Label:         "MyLabel",
				AWSAccountIDs: []string{"123456789012"},
				Actions:       []string{},
			},
			wantErrIs: sqs.ErrInvalidPermissionActions,
		},
		{
			name: "empty_aws_account_ids_returns_error",
			input: sqs.AddPermissionInput{
				QueueURL:      "http://localhost/000000000000/q",
				Label:         "MyLabel",
				AWSAccountIDs: []string{},
				Actions:       []string{"SendMessage"},
			},
			wantErrIs: sqs.ErrInvalidPermissionAccountIDs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)

			_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "q", Endpoint: "localhost"})
			require.NoError(t, err)

			err = b.AddPermission(&tt.input)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErrIs)
		})
	}
}

package sns_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSNS_TopicPermissions validates the AddPermission operation lifecycle.
func TestSNS_TopicPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(b *sns.InMemoryBackend) string
		name     string
		label    string
		accounts []string
		actions  []string
	}{
		{
			name: "add_permission_success",
			setup: func(b *sns.InMemoryBackend) string {
				t, _ := b.CreateTopic("perm-topic", nil)

				return t.TopicArn
			},
			label:    "allow-publish",
			accounts: []string{"123456789012"},
			actions:  []string{"Publish"},
		},
		{
			name: "add_permission_duplicate_label",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("perm-topic-dup", nil)

				err := b.AddPermission(tp.TopicArn, "my-label", []string{"123"}, []string{"Publish"})
				require.NoError(t, err)

				return tp.TopicArn
			},
			label:    "my-label",
			accounts: []string{"999"},
			actions:  []string{"Subscribe"},
			wantErr:  sns.ErrPermissionLabelExists,
		},
		{
			name:     "add_permission_topic_not_found",
			setup:    func(_ *sns.InMemoryBackend) string { return "arn:aws:sns:us-east-1:000000000000:missing" },
			label:    "some-label",
			accounts: []string{"123"},
			actions:  []string{"Publish"},
			wantErr:  sns.ErrTopicNotFound,
		},
		{
			name: "add_permission_empty_label",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("t", nil)

				return tp.TopicArn
			},
			label:    "",
			accounts: []string{"123"},
			actions:  []string{"Publish"},
			wantErr:  sns.ErrInvalidParameter,
		},
		{
			name: "add_permission_label_too_long",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("t2", nil)

				return tp.TopicArn
			},
			label:    strings.Repeat("a", 81),
			accounts: []string{"123"},
			actions:  []string{"Publish"},
			wantErr:  sns.ErrInvalidParameter,
		},
		{
			name: "add_permission_label_invalid_chars",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("t3", nil)

				return tp.TopicArn
			},
			label:    "invalid label!",
			accounts: []string{"123"},
			actions:  []string{"Publish"},
			wantErr:  sns.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			topicArn := tt.setup(b)

			err := b.AddPermission(topicArn, tt.label, tt.accounts, tt.actions)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

// TestSNS_TopicPermissionsHandler validates AddPermission HTTP handler.
func TestSNS_TopicPermissionsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       url.Values
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "add_permission_success",
			form: url.Values{
				"Action":                {"AddPermission"},
				"Version":               {"2010-03-31"},
				"TopicArn":              {"arn:aws:sns:us-east-1:000000000000:perm-test"},
				"Label":                 {"allow-sub"},
				"AWSAccountId.member.1": {"123456789012"},
				"ActionName.member.1":   {"Publish"},
			},
			wantStatus: http.StatusOK,
			wantBody:   "AddPermissionResponse",
		},
		{
			name: "add_permission_missing_topic_arn",
			form: url.Values{
				"Action":  {"AddPermission"},
				"Version": {"2010-03-31"},
				"Label":   {"x"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "add_permission_missing_label",
			form: url.Values{
				"Action":   {"AddPermission"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:perm-test"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			_, err := b.CreateTopic("perm-test", nil)
			require.NoError(t, err)

			rec := snsPost(t, h, tt.form)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestSNS_RemovePermission validates the RemovePermission operation.
func TestSNS_RemovePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *sns.InMemoryBackend) string
		name    string
		label   string
	}{
		{
			name: "remove_existing_label",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("rm-perm-topic", nil)

				err := b.AddPermission(tp.TopicArn, "my-label", []string{"123"}, []string{"Publish"})
				require.NoError(t, err)

				return tp.TopicArn
			},
			label: "my-label",
		},
		{
			name: "remove_missing_label",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("rm-perm-topic2", nil)

				return tp.TopicArn
			},
			label:   "nonexistent",
			wantErr: sns.ErrPermissionLabelNotFound,
		},
		{
			name:    "remove_topic_not_found",
			setup:   func(_ *sns.InMemoryBackend) string { return "arn:aws:sns:us-east-1:000000000000:missing" },
			label:   "x",
			wantErr: sns.ErrTopicNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			topicArn := tt.setup(b)

			err := b.RemovePermission(topicArn, tt.label)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

// TestSNS_RemovePermissionHandler validates the RemovePermission HTTP handler.
func TestSNS_RemovePermissionHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       url.Values
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			form: url.Values{
				"Action":   {"RemovePermission"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:perm-test"},
				"Label":    {"my-label"},
			},
			wantStatus: http.StatusOK,
			wantBody:   "RemovePermissionResponse",
		},
		{
			name: "missing_label",
			form: url.Values{
				"Action":   {"RemovePermission"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:perm-test"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_topic_arn",
			form: url.Values{
				"Action":  {"RemovePermission"},
				"Version": {"2010-03-31"},
				"Label":   {"my-label"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			tp, err := b.CreateTopic("perm-test", nil)
			require.NoError(t, err)

			err = b.AddPermission(tp.TopicArn, "my-label", []string{"123"}, []string{"Publish"})
			require.NoError(t, err)

			rec := snsPost(t, h, tt.form)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

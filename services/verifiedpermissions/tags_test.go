package verifiedpermissions_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

func TestBackend_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		tags    map[string]string
		name    string
		wantErr bool
	}{
		{
			name: "tag existing policy store",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
				require.NoError(t, err)

				return ps.Arn
			},
			tags:    map[string]string{"env": "prod", "team": "platform"},
			wantErr: false,
		},
		{
			name: "tag non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.InMemoryBackend) string {
				return "arn:aws:verifiedpermissions::123456789012:policy-store/nonexistent"
			},
			tags:    map[string]string{"key": "value"},
			wantErr: true,
		},
		{
			name: "add tags to already tagged store",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", map[string]string{"existing": "tag"}, "OFF", "", "")
				require.NoError(t, err)

				return ps.Arn
			},
			tags:    map[string]string{"new": "tag"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(t, b)

			err := b.TagResource(arn, tt.tags)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			tags, err := b.ListTagsForResource(arn)
			require.NoError(t, err)

			for k, v := range tt.tags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestBackend_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		name    string
		tagKeys []string
		wantErr bool
	}{
		{
			name: "untag existing resource",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore(
					"desc",
					map[string]string{"env": "prod", "team": "platform"},
					"OFF",
					"",
					"",
				)
				require.NoError(t, err)

				return ps.Arn
			},
			tagKeys: []string{"env"},
			wantErr: false,
		},
		{
			name: "untag non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.InMemoryBackend) string {
				return "arn:aws:verifiedpermissions::123456789012:policy-store/nonexistent"
			},
			tagKeys: []string{"key"},
			wantErr: true,
		},
		{
			name: "untag with empty keys removes nothing",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", map[string]string{"env": "prod"}, "OFF", "", "")
				require.NoError(t, err)

				return ps.Arn
			},
			tagKeys: []string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(t, b)

			err := b.UntagResource(arn, tt.tagKeys)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if len(tt.tagKeys) > 0 {
				tags, tagsErr := b.ListTagsForResource(arn)
				require.NoError(t, tagsErr)

				for _, k := range tt.tagKeys {
					_, exists := tags[k]
					assert.False(t, exists, "tag %q should have been removed", k)
				}
			}
		})
	}
}

func TestBackend_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.InMemoryBackend) string
		wantTags map[string]string
		name     string
		wantErr  bool
	}{
		{
			name: "list tags for existing resource with tags",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", map[string]string{"env": "prod"}, "OFF", "", "")
				require.NoError(t, err)

				return ps.Arn
			},
			wantTags: map[string]string{"env": "prod"},
			wantErr:  false,
		},
		{
			name: "list tags for resource without tags",
			setup: func(t *testing.T, b *verifiedpermissions.InMemoryBackend) string {
				t.Helper()

				ps, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
				require.NoError(t, err)

				return ps.Arn
			},
			wantTags: map[string]string{},
			wantErr:  false,
		},
		{
			name: "list tags for non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.InMemoryBackend) string {
				return "arn:aws:verifiedpermissions::123456789012:policy-store/nonexistent"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(t, b)

			tags, err := b.ListTagsForResource(arn)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestBackend_ARNIndexTagOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn      func(b *verifiedpermissions.InMemoryBackend, psArn string) error
		name    string
		wantErr bool
	}{
		{
			name: "tag resource with valid ARN",
			fn: func(b *verifiedpermissions.InMemoryBackend, arn string) error {
				return b.TagResource(arn, map[string]string{"k": "v"})
			},
			wantErr: false,
		},
		{
			name: "untag resource with valid ARN",
			fn: func(b *verifiedpermissions.InMemoryBackend, arn string) error {
				return b.UntagResource(arn, []string{"k"})
			},
			wantErr: false,
		},
		{
			name: "tag resource with invalid ARN",
			fn: func(b *verifiedpermissions.InMemoryBackend, _ string) error {
				return b.TagResource("arn:bad", map[string]string{"k": "v"})
			},
			wantErr: true,
		},
		{
			name: "list tags with valid ARN",
			fn: func(b *verifiedpermissions.InMemoryBackend, arn string) error {
				_, err := b.ListTagsForResource(arn)

				return err
			},
			wantErr: false,
		},
		{
			name: "list tags with invalid ARN",
			fn: func(b *verifiedpermissions.InMemoryBackend, _ string) error {
				_, err := b.ListTagsForResource("arn:bad")

				return err
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ps := seedPolicyStore(t, b, "tag store")

			err := tt.fn(b, ps.Arn)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackend_DeleteOps_NoOrphanedResourceTags lives in whitebox_test.go: it
// needs direct access to the unexported resourceTags map.

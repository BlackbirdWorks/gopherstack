package timestreamwrite_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

func TestInMemoryBackend_Tags(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateDatabase("my-db", "", nil)
	require.NoError(t, err)

	arn := "arn:aws:timestream:us-east-1:000000000000:database/my-db"

	err = b.TagResource(arn, map[string]string{"env": "test", "team": "infra"})
	require.NoError(t, err)

	tags := b.ListTagsForResource(arn)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "infra", tags["team"])

	err = b.UntagResource(arn, []string{"team"})
	require.NoError(t, err)

	tags = b.ListTagsForResource(arn)
	assert.Equal(t, "test", tags["env"])
	_, hasTeam := tags["team"]
	assert.False(t, hasTeam)
}

// TestInMemoryBackend_TagCountExport verifies TagCount export.
func TestInMemoryBackend_TagCountExport(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("db1", "", nil)
	require.NoError(t, err)

	err = b.TagResource("arn:aws:timestream:us-east-1:000000000000:database/db1", map[string]string{"k": "v"})
	require.NoError(t, err)
	assert.Equal(t, 1, timestreamwrite.TagCount(b))
}

// TestInMemoryBackend_TagResource_RequiresExistingResource verifies that
// TagResource rejects ARNs that do not belong to any known database or table.
func TestInMemoryBackend_TagResource_RequiresExistingResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*timestreamwrite.InMemoryBackend)
		arn     string
		wantErr bool
	}{
		{
			name:    "unknown ARN returns not found",
			arn:     "arn:aws:timestream:us-east-1:000000000000:database/no-such-db",
			wantErr: true,
		},
		{
			name: "known database ARN succeeds",
			setup: func(b *timestreamwrite.InMemoryBackend) {
				_, _ = b.CreateDatabase("known-db", "", nil)
			},
			arn:     "arn:aws:timestream:us-east-1:000000000000:database/known-db",
			wantErr: false,
		},
		{
			name: "known table ARN succeeds",
			setup: func(b *timestreamwrite.InMemoryBackend) {
				_, _ = b.CreateDatabase("tbl-db", "", nil)
				_, _ = b.CreateTable("tbl-db", "tbl", nil, nil)
			},
			arn:     "arn:aws:timestream:us-east-1:000000000000:database/tbl-db/table/tbl",
			wantErr: false,
		},
		{
			name: "scheduled-query ARN accepted for cross-service tags",
			arn:  "arn:aws:timestream:us-east-1:000000000000:scheduled-query/my-query",
			// scheduled-query ARNs are accepted unconditionally so the TimestreamWrite
			// handler can serve as the unified tag store for all Timestream resources.
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := timestreamwrite.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.TagResource(tt.arn, map[string]string{"k": "v"})

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, timestreamwrite.ErrResourceNotFound)

				return
			}

			require.NoError(t, err)
		})
	}
}

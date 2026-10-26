package azurefile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newShareBackend(t *testing.T) *InMemoryBackend {
	t.Helper()

	b := NewInMemoryBackend()

	b.mu.Lock("test")
	b.acct("Acct").Shares["share1"] = b.newShare("share1", time.Now())
	b.mu.Unlock()

	return b
}

func TestShareTreeRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		steps []ShareTree
		want  ShareTree
	}{
		{
			name: "create",
			steps: []ShareTree{{
				Dirs:  []string{"d", "d/e"},
				Files: map[string][]byte{"a.txt": []byte("A"), "d/b.txt": []byte("B"), "d/e/c.txt": nil},
			}},
			want: ShareTree{
				Dirs:  []string{"d", "d/e"},
				Files: map[string][]byte{"a.txt": []byte("A"), "d/b.txt": []byte("B"), "d/e/c.txt": {}},
			},
		},
		{
			name: "update-and-remove",
			steps: []ShareTree{
				{Dirs: []string{"d"}, Files: map[string][]byte{"a.txt": []byte("A"), "d/b.txt": []byte("B")}},
				{Files: map[string][]byte{"a.txt": []byte("A2"), "new.txt": []byte("N")}},
			},
			want: ShareTree{Files: map[string][]byte{"a.txt": []byte("A2"), "new.txt": []byte("N")}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newShareBackend(t)

			for _, step := range tc.steps {
				require.NoError(t, b.ApplyShare("acct", "SHARE1", step))
			}

			got, err := b.ExportShare("ACCT", "share1")
			require.NoError(t, err)
			assert.Equal(t, tc.want.Dirs, got.Dirs)
			assert.Equal(t, len(tc.want.Files), len(got.Files))

			for p, data := range tc.want.Files {
				assert.Equal(t, string(data), string(got.Files[p]), p)
			}
		})
	}
}

func TestShareTreeUnchangedFileKeepsETag(t *testing.T) {
	t.Parallel()

	b := newShareBackend(t)
	tree := ShareTree{Files: map[string][]byte{"a.txt": []byte("A")}}

	require.NoError(t, b.ApplyShare("acct", "share1", tree))

	etag := func() string {
		b.mu.RLock("test")
		defer b.mu.RUnlock()

		sh, _ := b.liveShare("acct", "share1")

		return sh.Root.child("a.txt").ETag
	}

	before := etag()

	require.NoError(t, b.ApplyShare("acct", "share1", tree))
	assert.Equal(t, before, etag())
}

func TestShareTreeMissingShare(t *testing.T) {
	t.Parallel()

	b := newShareBackend(t)

	_, err := b.ExportShare("acct", "nope")
	require.ErrorIs(t, err, ErrShareNotFound)
	require.ErrorIs(t, b.ApplyShare("other", "share1", ShareTree{}), ErrShareNotFound)
}

package rolesanywhere_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

// ---- Tags ----

func TestTagResource_Roundtrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resARN := "arn:aws:rolesanywhere:us-east-1:000000000000:trust-anchor/some-id"

	tags := []rolesanywhere.TagEntry{
		{Key: "env", Value: "prod"},
		{Key: "team", Value: "security"},
	}

	require.NoError(t, b.TagResource(context.Background(), resARN, tags))

	got, err := b.ListTagsForResource(context.Background(), resARN)
	require.NoError(t, err)
	assert.Len(t, got, 2)

	found := make(map[string]string)
	for _, t := range got {
		found[t.Key] = t.Value
	}

	assert.Equal(t, "prod", found["env"])
	assert.Equal(t, "security", found["team"])
}

func TestUntagResource_RemovesTags(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resARN := "arn:aws:rolesanywhere:us-east-1:000000000000:trust-anchor/untag-id"

	_ = b.TagResource(
		context.Background(),
		resARN,
		[]rolesanywhere.TagEntry{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}},
	)
	_ = b.UntagResource(context.Background(), resARN, []string{"a"})

	got, _ := b.ListTagsForResource(context.Background(), resARN)
	assert.Len(t, got, 1)
	assert.Equal(t, "b", got[0].Key)
}

func TestTagResource_Upsert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantValues map[string]string
		name       string
		initial    []rolesanywhere.TagEntry
		updates    []rolesanywhere.TagEntry
	}{
		{
			name: "update existing key preserves other keys",
			initial: []rolesanywhere.TagEntry{
				{Key: "env", Value: "dev"},
				{Key: "team", Value: "ops"},
			},
			updates: []rolesanywhere.TagEntry{{Key: "env", Value: "prod"}},
			wantValues: map[string]string{
				"env":  "prod",
				"team": "ops",
			},
		},
		{
			name:    "add new key",
			initial: []rolesanywhere.TagEntry{{Key: "a", Value: "1"}},
			updates: []rolesanywhere.TagEntry{{Key: "b", Value: "2"}},
			wantValues: map[string]string{
				"a": "1",
				"b": "2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			arn := "arn:aws:test::" + tt.name
			require.NoError(t, b.TagResource(context.Background(), arn, tt.initial))
			require.NoError(t, b.TagResource(context.Background(), arn, tt.updates))

			got, err := b.ListTagsForResource(context.Background(), arn)
			require.NoError(t, err)

			found := make(map[string]string)
			for _, tg := range got {
				found[tg.Key] = tg.Value
			}
			assert.Equal(t, tt.wantValues, found)
		})
	}
}

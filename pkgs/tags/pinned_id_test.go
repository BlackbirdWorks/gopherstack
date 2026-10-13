package tags_test

import (
	"net/url"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type kvTag struct{ Key, Value string }

func TestPinnedIDFromMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in       map[string]string
		wantRest map[string]string
		name     string
		wantID   string
	}{
		{name: "absent", in: map[string]string{"a": "b"}, wantRest: map[string]string{"a": "b"}},
		{
			name:     "present",
			in:       map[string]string{"a": "b", tags.PinnedIDKey: "x"},
			wantID:   "x",
			wantRest: map[string]string{"a": "b"},
		},
		{name: "nil", in: nil, wantRest: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			id, rest := tags.PinnedIDFromMap(tt.in)
			assert.Equal(t, tt.wantID, id)
			assert.Equal(t, tt.wantRest, rest)
		})
	}
}

func TestPinnedIDFromList(t *testing.T) {
	t.Parallel()

	kv := func(t kvTag) (string, string) { return t.Key, t.Value }

	tests := []struct {
		name     string
		in       []kvTag
		wantID   string
		wantRest []kvTag
	}{
		{name: "absent", in: []kvTag{{"a", "b"}}, wantRest: []kvTag{{"a", "b"}}},
		{
			name: "present", in: []kvTag{{"a", "b"}, {tags.PinnedIDKey, "x"}},
			wantID: "x", wantRest: []kvTag{{"a", "b"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			id, rest := tags.PinnedIDFromList(tt.in, kv)
			assert.Equal(t, tt.wantID, id)
			assert.Equal(t, tt.wantRest, rest)
		})
	}
}

func TestPinnedIDFromTagSpecs(t *testing.T) {
	t.Parallel()

	vals := url.Values{}
	vals.Set("TagSpecification.1.ResourceType", "instance")
	vals.Set("TagSpecification.1.Tag.1.Key", "Name")
	vals.Set("TagSpecification.1.Tag.1.Value", "n")
	vals.Set("TagSpecification.2.ResourceType", "vpc")
	vals.Set("TagSpecification.2.Tag.1.Key", tags.PinnedIDKey)
	vals.Set("TagSpecification.2.Tag.1.Value", "vpc-1")

	assert.Equal(t, "vpc-1", tags.PinnedIDFromTagSpecs(vals, "vpc"))
	assert.Empty(t, tags.PinnedIDFromTagSpecs(vals, "instance"))
	assert.Empty(t, tags.PinnedIDFromTagSpecs(vals, "subnet"))
}

func TestValidatePinnedID(t *testing.T) {
	t.Parallel()

	re := regexp.MustCompile(`^vpc-[0-9a-f]{8}$`)

	require.NoError(t, tags.ValidatePinnedID("vpc-0123abcd", re, "vpc-<8 hex>"))
	err := tags.ValidatePinnedID("bad", re, "vpc-<8 hex>")
	require.ErrorIs(t, err, tags.ErrInvalidPinnedID)
	assert.Contains(t, err.Error(), "vpc-<8 hex>")
}

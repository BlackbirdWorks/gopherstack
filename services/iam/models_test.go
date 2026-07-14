package iam_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagsToXML_Sorted(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("sorted-user", "/", "")
	require.NoError(t, b.TagUser("sorted-user", map[string]string{
		"z-last":  "val-z",
		"a-first": "val-a",
		"m-mid":   "val-m",
	}))

	u, err := b.GetUser("sorted-user")
	require.NoError(t, err)
	assert.Equal(t, "val-a", u.Tags["a-first"])
	assert.Equal(t, "val-m", u.Tags["m-mid"])
	assert.Equal(t, "val-z", u.Tags["z-last"])
}

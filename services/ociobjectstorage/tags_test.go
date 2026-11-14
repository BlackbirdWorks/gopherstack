package ociobjectstorage_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
	oci "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

func TestBucketTagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		defined map[string]map[string]any
		name    string
		code    string
	}{
		{name: "known tag", defined: map[string]map[string]any{"ops": {"cost": "x"}}},
		{
			name:    "unknown namespace",
			defined: map[string]map[string]any{"nope": {"cost": "x"}},
			code:    "NotAuthorizedOrNotFound",
		},
		{
			name:    "unknown key",
			defined: map[string]map[string]any{"ops": {"nope": "x"}},
			code:    "NotAuthorizedOrNotFound",
		},
	}

	for _, tt := range tests {
		for _, op := range []string{"create", "update"} {
			t.Run(tt.name+" "+op, func(t *testing.T) {
				t.Parallel()

				b := oci.NewInMemoryBackend()
				b.SetTagLookup(func(ns, key string) (ocitags.Def, bool) {
					return ocitags.Def{}, strings.EqualFold(ns, "ops") && strings.EqualFold(key, "cost")
				})

				in := oci.CreateBucketInput{Name: "bk", CompartmentID: "ocid1.compartment.oc1..c"}
				if op == "create" {
					in.DefinedTags = tt.defined
				}

				_, err := b.CreateBucket(oci.DefaultNamespace, in, "")
				if op == "create" {
					assertTagResult(t, err, tt.code)

					return
				}

				require.NoError(t, err)

				_, err = b.UpdateBucket(
					oci.DefaultNamespace,
					"bk",
					oci.UpdateBucketInput{DefinedTags: tt.defined},
					oci.Preconds{},
				)
				assertTagResult(t, err, tt.code)
			})
		}
	}
}

func assertTagResult(t *testing.T, err error, code string) {
	t.Helper()

	if code == "" {
		require.NoError(t, err)

		return
	}

	require.Error(t, err)
	assert.Contains(t, err.Error(), code)
}

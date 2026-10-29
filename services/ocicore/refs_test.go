package ocicore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func TestLookupSubnetAndVcn(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	v := e.mkVcn(t, "10.0.0.0/16", "refs")
	s := e.mkSubnet(t, v, "10.0.1.0/24")

	got, ok := e.backend.LookupSubnet(*s.Id)
	require.True(t, ok)
	assert.Equal(t, *v.Id, got.VcnID)
	assert.Equal(t, "10.0.1.0/24", got.CidrBlock)

	tests := []struct {
		name string
		fn   func() bool
		want bool
	}{
		{"known subnet", func() bool { _, ok := e.backend.LookupSubnet(*s.Id); return ok }, true},
		{"unknown subnet", func() bool { _, ok := e.backend.LookupSubnet("ocid1.subnet.oc1.iad.nope"); return ok }, false},
		{"vcn is not a subnet", func() bool { _, ok := e.backend.LookupSubnet(*v.Id); return ok }, false},
		{"known vcn", func() bool { return e.backend.VcnExists(*v.Id) }, true},
		{"subnet is not a vcn", func() bool { return e.backend.VcnExists(*s.Id) }, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.fn())
		})
	}
}

func TestLookupCatalogue(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	imgs := oci.CatalogImages()
	require.NotEmpty(t, imgs)

	tests := []struct {
		name  string
		check func(t *testing.T)
	}{
		{"flexible shape", func(t *testing.T) {
			t.Helper()

			s, ok := oci.LookupShape("VM.Standard.E4.Flex")
			require.True(t, ok)
			assert.True(t, s.Flexible)
			assert.Equal(t, "x86_64", s.Arch)
			assert.InDelta(t, 16, s.DefaultMemPerOcpu, 0)
		}},
		{"fixed shape", func(t *testing.T) {
			t.Helper()

			s, ok := oci.LookupShape("VM.Standard2.1")
			require.True(t, ok)
			assert.False(t, s.Flexible)
		}},
		{"unknown shape", func(t *testing.T) {
			t.Helper()

			_, ok := oci.LookupShape("VM.Nope")
			assert.False(t, ok)
		}},
		{"shape list", func(t *testing.T) {
			t.Helper()
			assert.Len(t, oci.CatalogShapes(), 9)
		}},
		{"platform image", func(t *testing.T) {
			t.Helper()

			got, ok := e.backend.LookupImage(imgs[0].ID)
			require.True(t, ok)
			assert.Equal(t, imgs[0], got)
			assert.Equal(t, "Oracle Linux", got.OS)
		}},
		{"unknown image", func(t *testing.T) {
			t.Helper()

			_, ok := e.backend.LookupImage("ocid1.image.oc1.iad.nope")
			assert.False(t, ok)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t)
		})
	}
}

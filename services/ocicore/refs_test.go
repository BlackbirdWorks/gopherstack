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

	subnetKnown := func(id string) bool {
		_, found := e.backend.LookupSubnet(id)

		return found
	}

	tests := []struct {
		fn   func() bool
		name string
		want bool
	}{
		{func() bool { return subnetKnown(*s.Id) }, "known subnet", true},
		{func() bool { return subnetKnown("ocid1.subnet.oc1.iad.nope") }, "unknown subnet", false},
		{func() bool { return subnetKnown(*v.Id) }, "vcn is not a subnet", false},
		{func() bool { return e.backend.VcnExists(*v.Id) }, "known vcn", true},
		{func() bool { return e.backend.VcnExists(*s.Id) }, "subnet is not a vcn", false},
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
		check func(t *testing.T)
		name  string
	}{
		{name: "flexible shape", check: func(t *testing.T) {
			t.Helper()

			s, ok := oci.LookupShape("VM.Standard.E4.Flex")
			require.True(t, ok)
			assert.True(t, s.Flexible)
			assert.Equal(t, "x86_64", s.Arch)
			assert.InDelta(t, 16, s.DefaultMemPerOcpu, 0)
		}},
		{name: "fixed shape", check: func(t *testing.T) {
			t.Helper()

			s, ok := oci.LookupShape("VM.Standard2.1")
			require.True(t, ok)
			assert.False(t, s.Flexible)
		}},
		{name: "unknown shape", check: func(t *testing.T) {
			t.Helper()

			_, ok := oci.LookupShape("VM.Nope")
			assert.False(t, ok)
		}},
		{name: "shape list", check: func(t *testing.T) {
			t.Helper()
			assert.Len(t, oci.CatalogShapes(), 9)
		}},
		{name: "platform image", check: func(t *testing.T) {
			t.Helper()

			got, ok := e.backend.LookupImage(imgs[0].ID)
			require.True(t, ok)
			assert.Equal(t, imgs[0], got)
			assert.Equal(t, "Oracle Linux", got.OS)
		}},
		{name: "unknown image", check: func(t *testing.T) {
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

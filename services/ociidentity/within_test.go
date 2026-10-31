package ociidentity_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestCompartmentWithin(t *testing.T) {
	t.Parallel()

	b := oci.NewInMemoryBackend()

	parent, _, err := b.CreateCompartment(
		oci.CreateCompartmentIn{CompartmentID: oci.TenancyID, Name: "parent", Description: "d"},
	)
	require.NoError(t, err)

	child, _, err := b.CreateCompartment(
		oci.CreateCompartmentIn{CompartmentID: parent.ID, Name: "child", Description: "d"},
	)
	require.NoError(t, err)

	other, _, err := b.CreateCompartment(
		oci.CreateCompartmentIn{CompartmentID: oci.TenancyID, Name: "other", Description: "d"},
	)
	require.NoError(t, err)

	tests := []struct {
		name     string
		id       string
		ancestor string
		want     bool
	}{
		{"self", parent.ID, parent.ID, true},
		{"child of parent", child.ID, parent.ID, true},
		{"parent of child", parent.ID, child.ID, false},
		{"siblings", other.ID, parent.ID, false},
		{"anything in tenancy", child.ID, oci.TenancyID, true},
		{"unknown", "ocid1.compartment.oc1..nope", parent.ID, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, b.CompartmentWithin(tt.id, tt.ancestor))
		})
	}
}

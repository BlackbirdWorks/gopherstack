package ociobjectstorage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHoldsInCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	assert.Empty(t, e.backend.HoldsInCompartment(testCompartment))

	e.mkBucket(t, "zeta", nil)
	e.mkBucket(t, "alpha", nil)

	assert.Equal(t, "Object Storage bucket alpha", e.backend.HoldsInCompartment(testCompartment))
	assert.Empty(t, e.backend.HoldsInCompartment("ocid1.compartment.oc1..other"))
}

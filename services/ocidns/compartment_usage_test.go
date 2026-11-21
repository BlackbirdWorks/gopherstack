package ocidns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHoldsInCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	assert.Empty(t, e.backend.HoldsInCompartment(testCompartment))

	e.createZone(t, "example.com", nil)

	assert.Equal(t, "DNS zone example.com", e.backend.HoldsInCompartment(testCompartment))
	assert.Empty(t, e.backend.HoldsInCompartment("ocid1.compartment.oc1..other"))
}

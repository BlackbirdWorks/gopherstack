package ocistreaming_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHoldsInCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	assert.Empty(t, e.backend.HoldsInCompartment(testCompartment))

	e.mkStream(t, "s", 1)

	assert.True(t, strings.HasPrefix(e.backend.HoldsInCompartment(testCompartment), "Streaming "))
	assert.Empty(t, e.backend.HoldsInCompartment("ocid1.compartment.oc1..other"))
}

package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestInMemoryBackend_ErrValidation_Sentinel verifies that ErrValidation is a distinct non-nil error.
func TestInMemoryBackend_ErrValidation_Sentinel(t *testing.T) {
	t.Parallel()

	assert.Error(t, iotwireless.ErrValidation)
}

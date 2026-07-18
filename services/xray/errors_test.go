package xray_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// TestErrValidation verifies ErrValidation sentinel is exported.
func TestErrValidation(t *testing.T) {
	t.Parallel()

	assert.Error(t, xray.ErrValidation)
}

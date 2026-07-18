package support_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/support"
)

// TestSupport_ErrValidationSentinel verifies ErrValidation is exported.
func TestSupport_ErrValidationSentinel(t *testing.T) {
	t.Parallel()

	assert.Error(t, support.ErrValidation)
}

// TestSupport_HandleError_ErrValidation verifies ErrValidation -> 400.
func TestSupport_HandleError_ErrValidation(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	// Missing attachmentId triggers ErrValidation.
	rec := doSupportRequest(t, h, "DescribeAttachment", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestSupport_HandleError_ErrUnknown verifies unknown actions -> 400.
func TestSupport_HandleError_ErrUnknown(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	rec := doSupportRequest(t, h, "NonExistentAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

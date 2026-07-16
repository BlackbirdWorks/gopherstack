package iam_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIAM_ServiceLastAccessedDetails(t *testing.T) {
	t.Parallel()

	_, be := newTestHandler(t)

	// GetServiceLastAccessedDetails — exercises the backend function
	_, _, err := be.GetServiceLastAccessedDetails("arn:aws:iam::123456789012:user/test-user")
	// May return empty or error — just exercises the code path
	assert.NoError(t, err)
}

package transfer_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_ListSecurityPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListSecurityPolicies", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DescribeSecurityPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "DescribeSecurityPolicy", map[string]any{
		"SecurityPolicyName": "TransferSecurityPolicy-2020-06",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

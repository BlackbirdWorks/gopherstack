package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- UpdateSPICECapacityConfiguration validation ----

func TestQuickSight_UpdateSPICECapacityConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/spice-capacity-configuration"), map[string]any{
		"PurchaseMode": "AUTO_PURCHASE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, accountPath("/spice-capacity-configuration"), map[string]any{
		"PurchaseMode": "MANUAL",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Invalid purchase mode -> validation error.
	invalidRec := doRequest(t, h, http.MethodPost, accountPath("/spice-capacity-configuration"), map[string]any{
		"PurchaseMode": "BOGUS",
	})
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)
	assert.Equal(t, "InvalidParameterValueException", parseBody(t, invalidRec)["Code"])

	// Missing purchase mode is treated as a no-op (still 200 OK), matching the
	// real operation's response shape carrying no data to validate against.
	missingRec := doRequest(t, h, http.MethodPost, accountPath("/spice-capacity-configuration"), map[string]any{})
	assert.Equal(t, http.StatusOK, missingRec.Code)
}

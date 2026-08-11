package quicksight_test

import (
	"net/http"
	"testing"

	sdktypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Identity propagation config CRUD-ish round-trip and errors ----

func TestQuickSight_IdentityPropagationConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Empty initially.
	emptyRec := doRequest(t, h, http.MethodGet, accountPath("/identity-propagation-config"), nil)
	require.Equal(t, http.StatusOK, emptyRec.Code)
	assert.Empty(t, parseBody(t, emptyRec)["Services"])

	// Invalid service name -> validation error.
	invalidRec := doRequest(
		t,
		h,
		http.MethodPut,
		accountPath("/identity-propagation-config/BOGUS"),
		map[string]any{
			"AuthorizedTargets": []string{"arn:aws:sso::000000000000:application/foo"},
		},
	)
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)

	updateRec := doRequest(
		t,
		h,
		http.MethodPut,
		accountPath("/identity-propagation-config/REDSHIFT"),
		map[string]any{
			"AuthorizedTargets": []string{"arn:aws:sso::000000000000:application/foo"},
		},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)

	listRec := doRequest(t, h, http.MethodGet, accountPath("/identity-propagation-config"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	services := parseBody(t, listRec)["Services"].([]any)
	require.Len(t, services, 1)
	entry := services[0].(map[string]any)
	assert.Equal(t, "REDSHIFT", entry["Service"])
	targets := entry["AuthorizedTargets"].([]any)
	assert.Equal(t, "arn:aws:sso::000000000000:application/foo", targets[0])

	// Re-update (upsert) replaces the authorized targets.
	updateRec2 := doRequest(
		t,
		h,
		http.MethodPut,
		accountPath("/identity-propagation-config/REDSHIFT"),
		map[string]any{
			"AuthorizedTargets": []string{"arn:aws:sso::000000000000:application/bar"},
		},
	)
	require.Equal(t, http.StatusOK, updateRec2.Code)
	listRec2 := doRequest(t, h, http.MethodGet, accountPath("/identity-propagation-config"), nil)
	entry2 := parseBody(t, listRec2)["Services"].([]any)[0].(map[string]any)
	assert.Equal(
		t,
		"arn:aws:sso::000000000000:application/bar",
		entry2["AuthorizedTargets"].([]any)[0],
	)

	deleteRec := doRequest(
		t,
		h,
		http.MethodDelete,
		accountPath("/identity-propagation-config/REDSHIFT"),
		nil,
	)
	require.Equal(t, http.StatusOK, deleteRec.Code)

	deleteMissingRec := doRequest(
		t,
		h,
		http.MethodDelete,
		accountPath("/identity-propagation-config/REDSHIFT"),
		nil,
	)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)

	finalListRec := doRequest(
		t,
		h,
		http.MethodGet,
		accountPath("/identity-propagation-config"),
		nil,
	)
	assert.Empty(t, parseBody(t, finalListRec)["Services"])
}

// Anti-drift: every value the pinned SDK's types.ServiceType enum knows
// about must be accepted, so a hand-maintained allowlist can't fall behind
// again.
func TestQuickSight_IdentityPropagationConfig_EverySDKServiceTypeAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, svc := range sdktypes.ServiceType("").Values() {
		t.Run(string(svc), func(t *testing.T) {
			t.Parallel()

			rec := doRequest(
				t,
				h,
				http.MethodPut,
				accountPath("/identity-propagation-config/"+string(svc)),
				map[string]any{
					"AuthorizedTargets": []string{"arn:aws:sso::000000000000:application/foo"},
				},
			)
			assert.Equal(t, http.StatusOK, rec.Code, "SDK ServiceType %s must be accepted", svc)
		})
	}
}

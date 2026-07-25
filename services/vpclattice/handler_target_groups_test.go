package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// TestTargetGroup_CRUD tests target groups.
func TestTargetGroup_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	tests := []struct {
		body     map[string]any
		check    func(t *testing.T, resp map[string]any)
		name     string
		wantCode int
	}{
		{
			name:     "create missing name returns 400",
			body:     map[string]any{"type": "INSTANCE"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "create instance target group returns 201",
			body: map[string]any{
				"name": "tg-inst",
				"type": "INSTANCE",
				"config": map[string]any{
					"protocol":      "HTTP",
					"port":          8080,
					"vpcIdentifier": "vpc-abc",
				},
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Contains(t, resp["arn"], ":targetgroup/tg-")
				assert.Equal(t, "ACTIVE", resp["status"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h2 := newTestHandler(t)
			rec := doRequest(t, h2, http.MethodPost, "/targetgroups", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}

	// full CRUD test
	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg-full",
		"type": "IP",
		"config": map[string]any{
			"protocol":      "HTTPS",
			"port":          443,
			"vpcIdentifier": "vpc-xyz",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tg := parseBody(t, rec)
	tgID, _ := tg["id"].(string)
	require.NotEmpty(t, tgID)
	assert.Equal(t, 1, vpclattice.TargetGroupCount(h.Backend.(*vpclattice.InMemoryBackend)))

	// get
	rec = doRequest(t, h, http.MethodGet, "/targetgroups/"+tgID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// list
	rec = doRequest(t, h, http.MethodGet, "/targetgroups", nil)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// update health check
	rec = doRequest(t, h, http.MethodPatch, "/targetgroups/"+tgID, map[string]any{
		"healthCheck": map[string]any{
			"enabled":  true,
			"protocol": "HTTP",
			"path":     "/health",
			"port":     8080,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/targetgroups/"+tgID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, vpclattice.TargetGroupCount(h.Backend.(*vpclattice.InMemoryBackend)))
}

// TestTargetGroupDelete_ConflictWhileReferencedByRule verifies that
// DeleteTargetGroup is rejected with 409 while a listener rule (including a
// listener's default action, which becomes its default rule) still
// forwards to the target group, matching real AWS's DeleteTargetGroup doc
// comment ("You can't delete a target group if it is used in a listener
// rule").
func TestTargetGroupDelete_ConflictWhileReferencedByRule(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	svcRec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-tg-conflict"})
	require.Equal(t, http.StatusCreated, svcRec.Code)
	svcID, _ := parseBody(t, svcRec)["id"].(string)

	tgRec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg-in-use",
		"type": "IP",
		"config": map[string]any{
			"protocol":      "HTTP",
			"port":          80,
			"vpcIdentifier": "vpc-1",
		},
	})
	require.Equal(t, http.StatusCreated, tgRec.Code)
	tgID, _ := parseBody(t, tgRec)["id"].(string)

	lRec := doRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners", map[string]any{
		"name":     "l1",
		"protocol": "HTTP",
		"defaultAction": map[string]any{
			"forward": map[string]any{
				"targetGroups": []any{
					map[string]any{"targetGroupIdentifier": tgID, "weight": 100},
				},
			},
		},
	})
	require.Equal(t, http.StatusCreated, lRec.Code)

	rec := doRequest(t, h, http.MethodDelete, "/targetgroups/"+tgID, nil)
	assert.Equal(t, http.StatusConflict, rec.Code, "delete must be rejected while a rule forwards to the target group")

	rec = doRequest(t, h, http.MethodGet, "/targetgroups/"+tgID, nil)
	assert.Equal(t, http.StatusOK, rec.Code, "target group must still exist after the rejected delete")

	// Once the listener (and its default rule) is gone, the target group is
	// no longer in use and can be deleted.
	listListenersRec := doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners", nil)
	require.Equal(t, http.StatusOK, listListenersRec.Code)
	listeners, _ := parseBody(t, listListenersRec)["items"].([]any)
	require.Len(t, listeners, 1)
	listenerID, _ := listeners[0].(map[string]any)["id"].(string)

	require.Equal(t, http.StatusNoContent,
		doRequest(t, h, http.MethodDelete, "/services/"+svcID+"/listeners/"+listenerID, nil).Code)

	rec = doRequest(t, h, http.MethodDelete, "/targetgroups/"+tgID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestTargetGroupSummaryWireShape verifies ListTargetGroups summary entries
// use "vpcIdentifier" (not "vpcId") and include lastUpdatedAt, matching the
// real TargetGroupSummary shape. The emulator previously emitted "vpcId",
// which real SDK clients (populating VpcIdentifier) would never see, and
// omitted lastUpdatedAt entirely.
func TestTargetGroupSummaryWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg-summary-shape",
		"type": "IP",
		"config": map[string]any{
			"protocol":                    "HTTP",
			"port":                        80,
			"vpcIdentifier":               "vpc-summary",
			"ipAddressType":               "IPV4",
			"lambdaEventStructureVersion": "V1",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/targetgroups", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, _ := parseBody(t, listRec)["items"].([]any)
	require.Len(t, items, 1)

	summary, _ := items[0].(map[string]any)
	assert.Equal(t, "vpc-summary", summary["vpcIdentifier"], "summary must use vpcIdentifier wire key")
	assert.Nil(t, summary["vpcId"], "summary must not use the vpcId wire key")
	assert.NotEmpty(t, summary["lastUpdatedAt"])
	assert.Equal(t, "IPV4", summary["ipAddressType"])
	assert.Equal(t, "V1", summary["lambdaEventStructureVersion"])
}

// TestTargetGroupConfigRoundTripsIPAddressType verifies that
// ipAddressType/lambdaEventStructureVersion set on CreateTargetGroup are
// echoed back in GetTargetGroup's config, matching real AWS's
// GetTargetGroupOutput.Config shape. The emulator captured these fields but
// never serialized them back to clients.
func TestTargetGroupConfigRoundTripsIPAddressType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg-config-roundtrip",
		"type": "IP",
		"config": map[string]any{
			"protocol":                    "HTTP",
			"port":                        80,
			"vpcIdentifier":               "vpc-rt",
			"ipAddressType":               "IPV6",
			"lambdaEventStructureVersion": "V2",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/targetgroups/"+tgID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	config, ok := parseBody(t, getRec)["config"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "IPV6", config["ipAddressType"])
	assert.Equal(t, "V2", config["lambdaEventStructureVersion"])
}

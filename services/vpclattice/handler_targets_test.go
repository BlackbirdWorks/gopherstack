package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTargets tests register/deregister/list targets.
func TestTargets(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name":   "tg-targets",
		"type":   "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	// register
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/registertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.2", "port": 80},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	assert.Empty(t, unsuccessful)

	// list
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/listtargets", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 2)

	// deregister one
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/targetgroups/"+tgID+"/deregistertargets",
		map[string]any{
			"targets": []any{
				map[string]any{"id": "10.0.0.1", "port": 80},
			},
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// list after deregister
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/listtargets", map[string]any{})
	list = parseBody(t, rec)
	items, _ = list["items"].([]any)
	assert.Len(t, items, 1)
}

// TestListTargets_BodyFilter verifies that target filters in the POST body are applied.
func TestListTargets_BodyFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filters   []map[string]any
		wantIDs   []string
		wantCount int
	}{
		{
			name:      "no filter returns all targets",
			wantCount: 3,
		},
		{
			name:      "filter by ID returns matching target",
			filters:   []map[string]any{{"id": "10.0.0.1"}},
			wantCount: 1,
			wantIDs:   []string{"10.0.0.1"},
		},
		{
			name:      "filter by ID+port returns exact match",
			filters:   []map[string]any{{"id": "10.0.0.1", "port": float64(80)}},
			wantCount: 1,
			wantIDs:   []string{"10.0.0.1"},
		},
		{
			name:      "filter by ID with wrong port returns nothing",
			filters:   []map[string]any{{"id": "10.0.0.1", "port": float64(9999)}},
			wantCount: 0,
		},
		{
			name:      "multiple filters return union of matches",
			filters:   []map[string]any{{"id": "10.0.0.1"}, {"id": "10.0.0.2"}},
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			recTG := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
				"name": "tg-filter-test",
				"type": "IP",
				"config": map[string]any{
					"protocol":      "HTTP",
					"port":          80,
					"vpcIdentifier": "vpc-1",
				},
			})
			require.Equal(t, http.StatusCreated, recTG.Code)
			tgID, _ := parseBody(t, recTG)["id"].(string)

			rec := doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/registertargets", map[string]any{
				"targets": []map[string]any{
					{"id": "10.0.0.1", "port": 80},
					{"id": "10.0.0.2", "port": 80},
					{"id": "10.0.0.3", "port": 80},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var listBody map[string]any
			if tc.filters != nil {
				listBody = map[string]any{"targets": tc.filters}
			}

			recList := doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/listtargets", listBody)
			require.Equal(t, http.StatusOK, recList.Code)
			resp := parseBody(t, recList)
			items, _ := resp["items"].([]any)
			assert.Len(t, items, tc.wantCount)

			for _, wantID := range tc.wantIDs {
				found := false
				for _, item := range items {
					m, _ := item.(map[string]any)
					if m["id"] == wantID {
						found = true

						break
					}
				}
				assert.True(t, found, "expected target %s in results", wantID)
			}
		})
	}
}

// TestRegisterDeregisterTargetsSuccessfulField verifies that
// RegisterTargets/DeregisterTargets responses include the "successful" list
// of targets, matching the real API's RegisterTargetsOutput/
// DeregisterTargetsOutput shape (Successful []Target, Unsuccessful
// []TargetFailure). The emulator previously omitted "successful" entirely,
// so SDK clients reading resp.Successful always saw an empty slice even on a
// fully successful call.
func TestRegisterDeregisterTargetsSuccessfulField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name":   "tg-successful-field",
		"type":   "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	// register two targets, one of which will later fail to deregister
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/registertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.2", "port": 80},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	successful, _ := resp["successful"].([]any)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	require.Len(t, successful, 2, "RegisterTargets must report both targets as successful")
	assert.Empty(t, unsuccessful)

	first, _ := successful[0].(map[string]any)
	assert.Equal(t, "10.0.0.1", first["id"])
	assert.InEpsilon(t, float64(80), first["port"], 0)

	// register a duplicate -> should fail, and NOT appear in successful
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/registertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.3", "port": 80},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseBody(t, rec)
	successful, _ = resp["successful"].([]any)
	unsuccessful, _ = resp["unsuccessful"].([]any)
	require.Len(t, successful, 1)
	require.Len(t, unsuccessful, 1)
	successOne, _ := successful[0].(map[string]any)
	assert.Equal(t, "10.0.0.3", successOne["id"])

	// deregister: one present, one absent
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/deregistertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.99", "port": 80},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseBody(t, rec)
	successful, _ = resp["successful"].([]any)
	unsuccessful, _ = resp["unsuccessful"].([]any)
	require.Len(t, successful, 1, "DeregisterTargets must report the removed target as successful")
	require.Len(t, unsuccessful, 1)
	successOne, _ = successful[0].(map[string]any)
	assert.Equal(t, "10.0.0.1", successOne["id"])
}

// TestTargetFailureUsesFailureCodeFailureMessageKeys verifies that target
// failure entries (from RegisterTargets/DeregisterTargets) use the wire keys
// "failureCode"/"failureMessage", matching the real TargetFailure shape. The
// emulator previously emitted "code"/"message", which real SDK clients
// (expecting FailureCode/FailureMessage) would never populate.
func TestTargetFailureUsesFailureCodeFailureMessageKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name":   "tg-failure-keys",
		"type":   "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	// deregister a target that was never registered -> guaranteed failure
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/deregistertargets", map[string]any{
		"targets": []any{map[string]any{"id": "10.0.0.1", "port": 80}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	require.Len(t, unsuccessful, 1)

	failure, _ := unsuccessful[0].(map[string]any)
	assert.NotEmpty(t, failure["failureCode"], "TargetFailure must use failureCode, not code")
	assert.NotEmpty(t, failure["failureMessage"], "TargetFailure must use failureMessage, not message")
	assert.Nil(t, failure["code"])
	assert.Nil(t, failure["message"])
}

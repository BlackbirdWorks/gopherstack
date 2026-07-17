package elbv2_test

import (
	"encoding/xml"
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestRegisterAndDeregisterTargets tests target registration.
func TestRegisterAndDeregisterTargets(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "targets-tg")

	// Register targets
	rec := doELBv2(t, h, url.Values{
		"Action":              {"RegisterTargets"},
		"Version":             {"2015-12-01"},
		"TargetGroupArn":      {tgArn},
		"Targets.member.1.Id": {"i-0123456789abcdef0"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe target health
	rec2 := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var healthResp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &healthResp))
	require.Len(t, healthResp.Result.TargetHealthDescriptions.Members, 1)
	assert.Equal(t, "i-0123456789abcdef0", healthResp.Result.TargetHealthDescriptions.Members[0].Target.ID)

	// Deregister targets
	rec3 := doELBv2(t, h, url.Values{
		"Action":              {"DeregisterTargets"},
		"Version":             {"2015-12-01"},
		"TargetGroupArn":      {tgArn},
		"Targets.member.1.Id": {"i-0123456789abcdef0"},
	})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestRegisterTargetsMissingARN tests missing ARN for register targets.
func TestRegisterTargetsMissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"RegisterTargets"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDeregisterTargetsMissingARN tests missing ARN for deregister targets.
func TestDeregisterTargetsMissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DeregisterTargets"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeTargetHealthMissingARN tests missing ARN for describe target health.
func TestDescribeTargetHealthMissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeTargetHealth"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRegisterTargetsDedupByPort verifies targets are de-duped by (ID, Port).
func TestRegisterTargetsDedupByPort(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "dedup-port-tg")

	// Register target on port 8080.
	rec1 := doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-abc"},
		"Targets.member.1.Port": {"8080"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	// Register same target on port 8080 again (duplicate).
	rec2 := doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-abc"},
		"Targets.member.1.Port": {"8080"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Register same target on different port 8081 (should be allowed).
	rec3 := doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-abc"},
		"Targets.member.1.Port": {"8081"},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	// Should have 2 entries: (i-abc:8080) and (i-abc:8081).
	healthRec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, healthRec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID   string `xml:"Id"`
						Port int32  `xml:"Port"`
					} `xml:"Target"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(healthRec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.TargetHealthDescriptions.Members, 2)
}

// TestDeregisterTargetsPortAware verifies that DeregisterTargets matches by ID+Port,
// so a target registered on multiple ports can be deregistered independently.
func TestDeregisterTargetsPortAware(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "port-aware-tg")

	// Register same instance on two ports.
	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-abc"},
		"Targets.member.1.Port": {"8080"},
		"Targets.member.2.Id":   {"i-abc"},
		"Targets.member.2.Port": {"8081"},
	})

	// Deregister only port 8080.
	doELBv2(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-abc"},
		"Targets.member.1.Port": {"8080"},
	})

	// Port 8080 must be draining; port 8081 must not be affected.
	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct { //nolint:govet // field order is chosen for readability
					Target struct {
						ID   string `xml:"Id"`
						Port int    `xml:"Port"`
					} `xml:"Target"`
					TargetHealth struct {
						State string `xml:"State"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 2)

	states := map[int]string{}
	for _, m := range resp.Result.TargetHealthDescriptions.Members {
		states[m.Target.Port] = m.TargetHealth.State
	}
	assert.Equal(t, "draining", states[8080], "deregistered port should be draining")
	assert.NotEqual(t, "draining", states[8081], "non-deregistered port must not be draining")
}

// TestDescribeTargetHealthFilter verifies that DescribeTargetHealth filters by requested targets.
func TestDescribeTargetHealthFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "health-filter-tg")

	// Register three targets.
	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-aaa"},
		"Targets.member.1.Port": {"80"},
		"Targets.member.2.Id":   {"i-bbb"},
		"Targets.member.2.Port": {"80"},
		"Targets.member.3.Id":   {"i-ccc"},
		"Targets.member.3.Port": {"80"},
	})

	// Request health for only i-aaa and i-ccc.
	rec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTargetHealth"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-aaa"},
		"Targets.member.1.Port": {"80"},
		"Targets.member.2.Id":   {"i-ccc"},
		"Targets.member.2.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 2)

	ids := []string{
		resp.Result.TargetHealthDescriptions.Members[0].Target.ID,
		resp.Result.TargetHealthDescriptions.Members[1].Target.ID,
	}
	assert.Contains(t, ids, "i-aaa")
	assert.Contains(t, ids, "i-ccc")
	assert.NotContains(t, ids, "i-bbb")
}

// TestDescribeTargetHealthUnregisteredTargets verifies that querying health for specific targets that are
// not registered returns state "unused" with reason "Target.NotRegistered", matching real AWS behaviour.
func TestDescribeTargetHealthUnregisteredTargets(t *testing.T) {
	t.Parallel()

	type targetHealthResult struct {
		State  string `xml:"State"`
		Reason string `xml:"Reason"`
	}
	type memberResult struct {
		TargetHealth targetHealthResult `xml:"TargetHealth"`
		Target       struct {
			ID   string `xml:"Id"`
			Port int32  `xml:"Port"`
		} `xml:"Target"`
	}
	type respType struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []memberResult `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}

	tests := []struct {
		requestTargets   url.Values
		name             string
		wantUnregistered []string // IDs expected with state=unused, reason=Target.NotRegistered
		wantRegistered   []string // IDs expected with a non-unused state
		wantLen          int
	}{
		{
			name: "single_unregistered_target",
			requestTargets: url.Values{
				"Targets.member.1.Id":   {"i-unregistered"},
				"Targets.member.1.Port": {"80"},
			},
			wantLen:          1,
			wantUnregistered: []string{"i-unregistered"},
		},
		{
			name: "mixed_registered_and_unregistered",
			requestTargets: url.Values{
				"Targets.member.1.Id":   {"i-registered"},
				"Targets.member.1.Port": {"80"},
				"Targets.member.2.Id":   {"i-ghost"},
				"Targets.member.2.Port": {"80"},
			},
			wantLen:          2,
			wantRegistered:   []string{"i-registered"},
			wantUnregistered: []string{"i-ghost"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tgArn := mustCreateTG(t, h, "unreg-tg")

			// Register only "i-registered" for the mixed test case.
			if len(tc.wantRegistered) > 0 {
				doELBv2(t, h, url.Values{
					"Action":                {"RegisterTargets"},
					"Version":               {"2015-12-01"},
					"TargetGroupArn":        {tgArn},
					"Targets.member.1.Id":   {"i-registered"},
					"Targets.member.1.Port": {"80"},
				})
			}

			vals := url.Values{
				"Action":         {"DescribeTargetHealth"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {tgArn},
			}
			maps.Copy(vals, tc.requestTargets)

			rec := doELBv2(t, h, vals)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp respType
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.TargetHealthDescriptions.Members, tc.wantLen)

			byID := make(map[string]memberResult, len(resp.Result.TargetHealthDescriptions.Members))
			for _, m := range resp.Result.TargetHealthDescriptions.Members {
				byID[m.Target.ID] = m
			}

			for _, id := range tc.wantUnregistered {
				m, ok := byID[id]
				require.True(t, ok, "expected %q in response", id)
				assert.Equal(t, "unused", m.TargetHealth.State, "target %q should be unused", id)
				assert.Equal(t, "Target.NotRegistered", m.TargetHealth.Reason, "target %q reason mismatch", id)
			}

			for _, id := range tc.wantRegistered {
				m, ok := byID[id]
				require.True(t, ok, "expected %q in response", id)
				assert.NotEqual(t, "unused", m.TargetHealth.State, "registered target %q should not be unused", id)
			}
		})
	}
}

func TestRegisterTargets_InitialHealthState(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "hc-initial-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-00000001"},
		"Targets.member.1.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
	th := resp.Result.TargetHealthDescriptions.Members[0]
	assert.Equal(t, "i-00000001", th.Target.ID)
	assert.Equal(t, "initial", th.TargetHealth.State)
	assert.Equal(t, "Elb.InitialHealthChecking", th.TargetHealth.Reason)
}

func TestTargetHealth_SetHealthy(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	h := elbv2.NewHandler(b)

	tgArn := b1CreateTG(t, h, "hc-set-healthy")

	rec := doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-healthy-01"},
		"Targets.member.1.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, b.SetTargetHealthState(tgArn, "i-healthy-01", 80, "healthy", ""))

	rec2 := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	th := resp.Result.TargetHealthDescriptions.Members[0].TargetHealth
	assert.Equal(t, "healthy", th.State)
	assert.Empty(t, th.Reason)
}

func TestTargetHealth_SetUnhealthy(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	h := elbv2.NewHandler(b)

	tgArn := b1CreateTG(t, h, "hc-set-unhealthy")

	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-unhealthy-01"},
		"Targets.member.1.Port": {"80"},
	})

	require.NoError(t, b.SetTargetHealthState(tgArn, "i-unhealthy-01", 80, "unhealthy", "Target.ResponseCodeMismatch"))

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	th := resp.Result.TargetHealthDescriptions.Members[0].TargetHealth
	assert.Equal(t, "unhealthy", th.State)
	assert.Equal(t, "Target.ResponseCodeMismatch", th.Reason)
}

func TestTargetHealth_MultipleDifferentStates(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	h := elbv2.NewHandler(b)

	tgArn := b1CreateTG(t, h, "hc-multi-state")

	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-initial-01"},
		"Targets.member.1.Port": {"80"},
		"Targets.member.2.Id":   {"i-healthy-02"},
		"Targets.member.2.Port": {"80"},
	})

	require.NoError(t, b.SetTargetHealthState(tgArn, "i-healthy-02", 80, "healthy", ""))

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
					TargetHealth struct {
						State string `xml:"State"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 2)

	states := map[string]string{}
	for _, m := range resp.Result.TargetHealthDescriptions.Members {
		states[m.Target.ID] = m.TargetHealth.State
	}
	assert.Equal(t, "initial", states["i-initial-01"])
	assert.Equal(t, "healthy", states["i-healthy-02"])
}

func TestRegisterTargets_Dedup(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "register-dedup")

	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-dedup-01"},
		"Targets.member.1.Port": {"80"},
	})
	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-dedup-01"},
		"Targets.member.1.Port": {"80"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct{} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
}

func TestDeregisterTargets_Success(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "dereg-tg")

	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-to-dereg"},
		"Targets.member.1.Port": {"80"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-to-dereg"},
		"Targets.member.1.Port": {"80"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// AWS: deregistered targets enter draining state and remain visible until
	// deregistration_delay expires. Check the target is in draining state.
	rec2 := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
	th := resp.Result.TargetHealthDescriptions.Members[0].TargetHealth
	assert.Equal(t, "draining", th.State)
	assert.Equal(t, "Target.DeregistrationInProgress", th.Reason)
}

func TestDescribeTargetHealth_FilterByTarget(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "hc-filter")

	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-target-a"},
		"Targets.member.1.Port": {"80"},
		"Targets.member.2.Id":   {"i-target-b"},
		"Targets.member.2.Port": {"80"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTargetHealth"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-target-a"},
		"Targets.member.1.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
	assert.Equal(t, "i-target-a", resp.Result.TargetHealthDescriptions.Members[0].Target.ID)
}

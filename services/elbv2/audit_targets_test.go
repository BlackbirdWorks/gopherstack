package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestAuditELBv2_DeregisterTargets_DrainingState verifies that after DeregisterTargets,
// the target enters draining state with reason Target.DeregistrationInProgress.
func TestAuditELBv2_DeregisterTargets_DrainingState(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	tgArn := auditCreateTG(t, h, "drain-state-tg")

	auditDo(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-drain-01"},
		"Targets.member.1.Port": {"80"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-drain-01"},
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
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	auditDo(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	}).into(&resp)

	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
	th := resp.Result.TargetHealthDescriptions.Members[0].TargetHealth
	assert.Equal(t, "draining", th.State)
	assert.Equal(t, "Target.DeregistrationInProgress", th.Reason)
}

// TestAuditELBv2_DeregisterTargets_NonRegisteredIsNoop verifies that deregistering
// a target that was never registered is a no-op (does not affect other targets).
func TestAuditELBv2_DeregisterTargets_NonRegisteredIsNoop(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	tgArn := auditCreateTG(t, h, "noop-drain-tg")

	auditDo(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-real"},
		"Targets.member.1.Port": {"80"},
	})

	// Deregister a target that was never registered.
	rec := doELBv2(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-never-registered"},
		"Targets.member.1.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "deregistering unknown target must not error")

	// The real target must be unaffected.
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
	auditDo(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	}).into(&resp)

	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
	assert.Equal(t, "i-real", resp.Result.TargetHealthDescriptions.Members[0].Target.ID)
	assert.NotEqual(
		t,
		"draining",
		resp.Result.TargetHealthDescriptions.Members[0].TargetHealth.State,
	)
}

// TestAuditELBv2_DeregisterTargets_ZeroDelay_EventuallyRemoved verifies that when
// deregistration_delay.timeout_seconds=0, targets are removed from the TG promptly
// by the background reconciler.
func TestAuditELBv2_DeregisterTargets_ZeroDelay_EventuallyRemoved(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	tgArn := auditCreateTG(t, h, "zero-drain-tg")

	// Set deregistration delay to 0 so the drain completes immediately.
	auditDo(t, h, url.Values{
		"Action":                    {"ModifyTargetGroupAttributes"},
		"Version":                   {"2015-12-01"},
		"TargetGroupArn":            {tgArn},
		"Attributes.member.1.Key":   {"deregistration_delay.timeout_seconds"},
		"Attributes.member.1.Value": {"0"},
	})

	auditDo(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-zero-drain"},
		"Targets.member.1.Port": {"80"},
	})

	auditDo(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-zero-drain"},
		"Targets.member.1.Port": {"80"},
	})

	// Wait for the background reconciler to remove the drained target.
	// The reconciler fires every ~40ms; with delay=0 the drain has already expired.
	require.Eventually(t, func() bool {
		var resp struct {
			Result struct {
				TargetHealthDescriptions struct {
					Members []struct{} `xml:"member"`
				} `xml:"TargetHealthDescriptions"`
			} `xml:"DescribeTargetHealthResult"`
		}
		rec := doELBv2(t, h, url.Values{
			"Action":         {"DescribeTargetHealth"},
			"Version":        {"2015-12-01"},
			"TargetGroupArn": {tgArn},
		})
		if rec.Code != http.StatusOK {
			return false
		}
		_ = xml.Unmarshal(rec.Body.Bytes(), &resp)

		return len(resp.Result.TargetHealthDescriptions.Members) == 0
	}, 500*time.Millisecond, 20*time.Millisecond, "target must be removed after drain delay expires")
}

// TestAuditELBv2_DeregisterTargets_MultiPort_OnlyDrainsTarget verifies that
// deregistering one port leaves the other port unaffected.
func TestAuditELBv2_DeregisterTargets_MultiPort_OnlyDrainsTarget(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	tgArn := auditCreateTG(t, h, "multi-port-drain-tg")

	auditDo(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-mp"},
		"Targets.member.1.Port": {"8080"},
		"Targets.member.2.Id":   {"i-mp"},
		"Targets.member.2.Port": {"8081"},
	})

	auditDo(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-mp"},
		"Targets.member.1.Port": {"8080"},
	})

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					TargetHealth struct {
						State string `xml:"State"`
					} `xml:"TargetHealth"`
					Target struct {
						Port int `xml:"Port"`
					} `xml:"Target"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	auditDo(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	}).into(&resp)

	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 2)
	states := map[int]string{}
	for _, m := range resp.Result.TargetHealthDescriptions.Members {
		states[m.Target.Port] = m.TargetHealth.State
	}
	assert.Equal(t, "draining", states[8080])
	assert.NotEqual(t, "draining", states[8081])
}

// TestAuditELBv2_TargetHealth_AllStates verifies that all documented health states
// can be set and retrieved: initial, healthy, unhealthy, draining, unused.
func TestAuditELBv2_TargetHealth_AllStates(t *testing.T) {
	t.Parallel()

	b := auditBackend(t)
	h := elbv2.NewHandler(b)

	tgArn := auditCreateTG(t, h, "all-states-tg")

	// Register multiple targets.
	auditDo(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-initial"},
		"Targets.member.1.Port": {"80"},
		"Targets.member.2.Id":   {"i-healthy"},
		"Targets.member.2.Port": {"80"},
		"Targets.member.3.Id":   {"i-unhealthy"},
		"Targets.member.3.Port": {"80"},
		"Targets.member.4.Id":   {"i-draining"},
		"Targets.member.4.Port": {"80"},
	})

	require.NoError(t, b.SetTargetHealthState(tgArn, "i-healthy", 80, "healthy", ""))
	require.NoError(
		t,
		b.SetTargetHealthState(tgArn, "i-unhealthy", 80, "unhealthy", "Target.FailedHealthChecks"),
	)

	// Deregister to trigger draining.
	auditDo(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-draining"},
		"Targets.member.1.Port": {"80"},
	})

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
	auditDo(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	}).into(&resp)

	states := map[string]string{}
	reasons := map[string]string{}
	for _, m := range resp.Result.TargetHealthDescriptions.Members {
		states[m.Target.ID] = m.TargetHealth.State
		reasons[m.Target.ID] = m.TargetHealth.Reason
	}

	assert.Equal(t, "initial", states["i-initial"])
	assert.Equal(t, "Elb.InitialHealthChecking", reasons["i-initial"])
	assert.Equal(t, "healthy", states["i-healthy"])
	assert.Empty(t, reasons["i-healthy"])
	assert.Equal(t, "unhealthy", states["i-unhealthy"])
	assert.Equal(t, "Target.FailedHealthChecks", reasons["i-unhealthy"])
	assert.Equal(t, "draining", states["i-draining"])
	assert.Equal(t, "Target.DeregistrationInProgress", reasons["i-draining"])

	// Query a non-registered target → unused state.
	var unusedResp struct {
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
	auditDo(t, h, url.Values{
		"Action":                {"DescribeTargetHealth"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-not-registered"},
		"Targets.member.1.Port": {"80"},
	}).into(&unusedResp)

	require.Len(t, unusedResp.Result.TargetHealthDescriptions.Members, 1)
	uth := unusedResp.Result.TargetHealthDescriptions.Members[0].TargetHealth
	assert.Equal(t, "unused", uth.State)
	assert.Equal(t, "Target.NotRegistered", uth.Reason)
}

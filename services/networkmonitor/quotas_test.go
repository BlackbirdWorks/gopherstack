package networkmonitor_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

// TestCreateMonitorQuota_MonitorsPerRegion verifies that CreateMonitor
// enforces the real Network Synthetic Monitor "Number of monitors per
// account per AWS region" quota (default 100; see
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html#nw-monitor-quotas).
// The 101st monitor in a Region must be rejected with ServiceQuotaExceeded,
// and a Region at capacity must not block a different Region.
func TestCreateMonitorQuota_MonitorsPerRegion(t *testing.T) {
	t.Parallel()

	const limit = 100

	b := newTestBackend(t)
	ctx := context.Background()

	for i := range limit {
		_, err := b.CreateMonitor(ctx, fmt.Sprintf("mon-%03d", i), nil, nil, nil)
		require.NoError(t, err, "monitor %d should be within quota", i)
	}

	_, err := b.CreateMonitor(ctx, "one-too-many", nil, nil, nil)
	require.Error(t, err, "the 101st monitor must be rejected")
	require.ErrorIs(t, err, networkmonitor.ErrServiceQuotaExceeded)

	westCtx := networkmonitor.WithRegion("us-west-2")

	_, err = b.CreateMonitor(westCtx, "mon-in-other-region", nil, nil, nil)
	require.NoError(t, err, "a different Region must not share the exhausted Region's quota")
}

// TestCreateProbeQuota_ProbesPerMonitor verifies the "Number of probes per
// monitor" quota (default 24): the 25th probe added to a single monitor,
// whether via CreateProbe or nested in CreateMonitor, must be rejected.
func TestCreateProbeQuota_ProbesPerMonitor(t *testing.T) {
	t.Parallel()

	const limit = 24

	t.Run("via CreateProbe", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		ctx := context.Background()

		_, err := b.CreateMonitor(ctx, "probe-quota-mon", nil, nil, nil)
		require.NoError(t, err)

		for i := range limit {
			_, createErr := b.CreateProbe(ctx, "probe-quota-mon", &networkmonitor.ProbeInputForTest{
				Destination: "10.0.0.1",
				Protocol:    "ICMP",
				// A distinct sourceArn per probe keeps this test isolated
				// from the per-subnet quota (see the sibling test below).
				SourceArn: fmt.Sprintf("arn:aws:ec2:us-east-1:000000000000:subnet/subnet-%02d", i),
			}, nil)
			require.NoError(t, createErr, "probe %d should be within the per-monitor quota", i)
		}

		_, err = b.CreateProbe(ctx, "probe-quota-mon", &networkmonitor.ProbeInputForTest{
			Destination: "10.0.0.1",
			Protocol:    "ICMP",
			SourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-99",
		}, nil)
		require.Error(t, err, "the 25th probe must be rejected")
		require.ErrorIs(t, err, networkmonitor.ErrServiceQuotaExceeded)
	})

	t.Run("via CreateMonitor nested probes", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		ctx := context.Background()

		probes := make([]networkmonitor.ProbeInputForTest, limit+1)
		for i := range probes {
			probes[i] = networkmonitor.ProbeInputForTest{
				Destination: "10.0.0.1",
				Protocol:    "ICMP",
				SourceArn:   fmt.Sprintf("arn:aws:ec2:us-east-1:000000000000:subnet/subnet-%02d", i),
			}
		}

		_, err := networkmonitor.CreateMonitorWithProbesForTest(ctx, b, "nested-quota-mon", probes)
		require.Error(t, err, "creating a monitor with 25 nested probes must be rejected")
		require.ErrorIs(t, err, networkmonitor.ErrServiceQuotaExceeded)
	})
}

// TestCreateProbeQuota_ProbesPerSubnet verifies the "Number of probes per
// subnet for each monitor" quota (default 4): the 5th probe sharing a
// sourceArn within one monitor must be rejected, but a different sourceArn
// (or a different monitor) must not be affected.
func TestCreateProbeQuota_ProbesPerSubnet(t *testing.T) {
	t.Parallel()

	const (
		limit        = 4
		sharedSubnet = "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-shared"
		otherSubnet  = "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-other"
	)

	b := newTestBackend(t)
	ctx := context.Background()

	_, err := b.CreateMonitor(ctx, "subnet-quota-mon", nil, nil, nil)
	require.NoError(t, err)

	for i := range limit {
		_, createErr := b.CreateProbe(ctx, "subnet-quota-mon", &networkmonitor.ProbeInputForTest{
			Destination: "10.0.0.1",
			Protocol:    "ICMP",
			SourceArn:   sharedSubnet,
		}, nil)
		require.NoError(t, createErr, "probe %d on the shared subnet should be within quota", i)
	}

	_, err = b.CreateProbe(ctx, "subnet-quota-mon", &networkmonitor.ProbeInputForTest{
		Destination: "10.0.0.1",
		Protocol:    "ICMP",
		SourceArn:   sharedSubnet,
	}, nil)
	require.Error(t, err, "the 5th probe on the same subnet must be rejected")
	require.ErrorIs(t, err, networkmonitor.ErrServiceQuotaExceeded)

	_, err = b.CreateProbe(ctx, "subnet-quota-mon", &networkmonitor.ProbeInputForTest{
		Destination: "10.0.0.1",
		Protocol:    "ICMP",
		SourceArn:   otherSubnet,
	}, nil)
	require.NoError(t, err, "a different subnet within the same monitor must not be blocked")
}

// TestHandlerCreateProbe_ServiceQuotaExceededWireStatus verifies the wire
// contract for a quota rejection: HTTP 402 with the
// "ServiceQuotaExceededException" error type header, matching the real
// networkmonitor API's documented status for this exception (see
// https://docs.aws.amazon.com/networkmonitor/latest/APIReference/API_CreateProbe.html#API_CreateProbe_Errors).
func TestHandlerCreateProbe_ServiceQuotaExceededWireStatus(t *testing.T) {
	t.Parallel()

	const limit = 4

	h := newTestHandler(t)
	createMonitorP(t, h, "wire-quota-mon")

	for i := range limit {
		rr := doNMRequest(t, h, http.MethodPost, "/monitors/wire-quota-mon/probes", map[string]any{
			"probe": map[string]any{
				"destination": "10.0.0.1",
				"protocol":    "ICMP",
				"sourceArn":   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-shared",
			},
		})
		require.Equal(t, http.StatusOK, rr.Code, "probe %d: %s", i, rr.Body.String())
	}

	rr := doNMRequest(t, h, http.MethodPost, "/monitors/wire-quota-mon/probes", map[string]any{
		"probe": map[string]any{
			"destination": "10.0.0.1",
			"protocol":    "ICMP",
			"sourceArn":   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-shared",
		},
	})

	require.Equal(t, http.StatusPaymentRequired, rr.Code, "body: %s", rr.Body.String())
	require.Equal(t, "ServiceQuotaExceededException", rr.Header().Get("X-Amzn-Errortype"))
}

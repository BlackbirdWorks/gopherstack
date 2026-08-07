package networkmonitor

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func createMonitorWithProbes(
	ctx context.Context, b *InMemoryBackend, name string, probes []probeInput,
) (*Monitor, error) {
	converted := make([]createMonitorProbeInput, len(probes))

	for i, p := range probes {
		converted[i] = createMonitorProbeInput(p)
	}

	return b.CreateMonitor(ctx, name, nil, converted, nil)
}

// TestCreateProbeQuota_ProbesPerMonitor_ViaNestedProbes covers the same
// per-monitor probe quota as TestCreateProbeQuota_ProbesPerMonitor (in
// quotas_test.go), but via CreateMonitor's nested probes parameter.
func TestCreateProbeQuota_ProbesPerMonitor_ViaNestedProbes(t *testing.T) {
	t.Parallel()

	const limit = 24

	b := NewInMemoryBackend("us-east-1", "000000000000")
	ctx := context.Background()

	probes := make([]probeInput, limit+1)
	for i := range probes {
		probes[i] = probeInput{
			Destination: "10.0.0.1",
			Protocol:    "ICMP",
			SourceArn:   fmt.Sprintf("arn:aws:ec2:us-east-1:000000000000:subnet/subnet-%02d", i),
		}
	}

	_, err := createMonitorWithProbes(ctx, b, "nested-quota-mon", probes)
	require.Error(t, err, "creating a monitor with 25 nested probes must be rejected")
	require.ErrorIs(t, err, ErrServiceQuotaExceeded)
}

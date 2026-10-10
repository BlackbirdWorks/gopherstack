package networkmonitor_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

func TestActivationDelay_StateProgression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		delay   time.Duration
		elapsed time.Duration
	}{
		{name: "default", want: "ACTIVE"},
		{name: "pending", delay: time.Hour, elapsed: time.Minute, want: "PENDING"},
		{name: "active", delay: time.Hour, elapsed: 2 * time.Hour, want: "ACTIVE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := networkmonitor.NewInMemoryBackend("us-east-1", "123456789012")
				b.SetActivationDelay(tt.delay)
				ctx := t.Context()

				_, err := b.CreateMonitor(ctx, "mon", nil, nil, nil)
				require.NoError(t, err)

				port := int32(80)
				probe, err := b.CreateProbe(ctx, "mon", &networkmonitor.ProbeInputForTest{
					SourceArn:       "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1",
					Destination:     "10.0.0.1",
					Protocol:        "TCP",
					DestinationPort: &port,
				}, nil)
				require.NoError(t, err)

				time.Sleep(tt.elapsed)

				mon, err := b.GetMonitor(ctx, "mon")
				require.NoError(t, err)
				assert.Equal(t, tt.want, mon.State)
				require.Len(t, mon.Probes, 1)
				assert.Equal(t, tt.want, mon.Probes[0].State)

				got, err := b.GetProbe(ctx, "mon", probe.ProbeID)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got.State)

				list, _, err := b.ListMonitors(ctx, tt.want, "", 0)
				require.NoError(t, err)
				assert.Len(t, list, 1)
			})
		})
	}
}

func TestCreateProbe_DestinationMustBeIP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destination string
		wantErr     bool
	}{
		{name: "ipv4", destination: "10.0.0.1"},
		{name: "ipv6", destination: "2001:db8::1"},
		{name: "hostname", destination: "example.com", wantErr: true},
		{name: "garbage", destination: "bad host!", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := networkmonitor.NewInMemoryBackend("us-east-1", "123456789012")
			_, err := b.CreateMonitor(t.Context(), "mon", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.CreateProbe(t.Context(), "mon", &networkmonitor.ProbeInputForTest{
				SourceArn:   "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1",
				Destination: tt.destination,
				Protocol:    "ICMP",
			}, nil)

			if tt.wantErr {
				require.ErrorIs(t, err, networkmonitor.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestErrorMessages_NoCodePrefix(t *testing.T) {
	t.Parallel()

	rec := doNMRequest(t, newTestHandler(t), "GET", "/monitors/nope", nil)
	require.Equal(t, 404, rec.Code)
	assert.NotContains(t, rec.Body.String(), "ResourceNotFoundException")
}

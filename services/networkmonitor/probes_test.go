package networkmonitor_test

import (
	"context"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

func TestProbeLifecycle(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	ctx := context.Background()

	if _, err := b.CreateMonitor(ctx, "probe-mon", nil, nil, nil); err != nil {
		t.Fatalf("create monitor: %v", err)
	}

	probe, err := b.CreateProbe(ctx, "probe-mon", &networkmonitor.ProbeInputForTest{
		Destination: "10.0.0.1",
		Protocol:    "ICMP",
		SourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
	}, nil)

	if err != nil {
		t.Fatalf("create probe: %v", err)
	}

	if probe.ProbeID == "" {
		t.Error("expected non-empty ProbeID")
	}

	got, err := b.GetProbe(ctx, "probe-mon", probe.ProbeID)
	if err != nil {
		t.Fatalf("get probe: %v", err)
	}

	if got.Destination != "10.0.0.1" {
		t.Errorf("destination: got %q, want 10.0.0.1", got.Destination)
	}

	if delErr := b.DeleteProbe(ctx, "probe-mon", probe.ProbeID); delErr != nil {
		t.Fatalf("delete probe: %v", delErr)
	}

	if _, getErr := b.GetProbe(ctx, "probe-mon", probe.ProbeID); getErr == nil {
		t.Fatal("expected not-found after delete")
	}
}

func TestCreateProbeTCPRequiresPort(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	ctx := context.Background()

	if _, err := b.CreateMonitor(ctx, "tcp-mon", nil, nil, nil); err != nil {
		t.Fatalf("create monitor: %v", err)
	}

	_, err := b.CreateProbe(ctx, "tcp-mon", &networkmonitor.ProbeInputForTest{
		Destination: "10.0.0.1",
		Protocol:    "TCP",
		SourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
	}, nil)

	if err == nil {
		t.Fatal("expected validation error: TCP requires port")
	}
}

func TestCreateProbeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name        string
		destination string
		protocol    string
		sourceArn   string
		destPort    *int32
		packetSize  *int32
		wantErr     bool
	}{
		{
			name:        "packet size too small",
			destination: "10.0.0.1",
			protocol:    "ICMP",
			sourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
			packetSize:  ptr(int32(55)),
			wantErr:     true,
		},
		{
			name:        "packet size too large",
			destination: "10.0.0.1",
			protocol:    "ICMP",
			sourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
			packetSize:  ptr(int32(8501)),
			wantErr:     true,
		},
		{
			name:        "packet size at lower bound",
			destination: "10.0.0.1",
			protocol:    "ICMP",
			sourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
			packetSize:  ptr(int32(56)),
		},
		{
			name:        "packet size at upper bound",
			destination: "10.0.0.1",
			protocol:    "ICMP",
			sourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
			packetSize:  ptr(int32(8500)),
		},
		{
			name:        "destination port too low",
			destination: "10.0.0.1",
			protocol:    "TCP",
			sourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
			destPort:    ptr(int32(0)),
			wantErr:     true,
		},
		{
			name:        "destination port too high",
			destination: "10.0.0.1",
			protocol:    "TCP",
			sourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
			destPort:    ptr(int32(65536)),
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			ctx := context.Background()

			if _, err := b.CreateMonitor(ctx, "validate-mon", nil, nil, nil); err != nil {
				t.Fatalf("create monitor: %v", err)
			}

			_, err := b.CreateProbe(ctx, "validate-mon", &networkmonitor.ProbeInputForTest{
				Destination:     tt.destination,
				Protocol:        tt.protocol,
				SourceArn:       tt.sourceArn,
				DestinationPort: tt.destPort,
				PacketSize:      tt.packetSize,
			}, nil)

			if tt.wantErr && err == nil {
				t.Fatal("expected validation error, got nil")
			}

			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestCreateProbeProtocolNormalized verifies that a lowercase/mixed-case
// protocol on input is stored and returned as the canonical uppercase AWS
// enum value ("TCP"/"ICMP"), matching the real API's wire contract.
func TestCreateProbeProtocolNormalized(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	ctx := context.Background()

	if _, err := b.CreateMonitor(ctx, "proto-mon", nil, nil, nil); err != nil {
		t.Fatalf("create monitor: %v", err)
	}

	probe, err := b.CreateProbe(ctx, "proto-mon", &networkmonitor.ProbeInputForTest{
		Destination: "10.0.0.1",
		Protocol:    "icmp",
		SourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
	}, nil)
	if err != nil {
		t.Fatalf("create probe: %v", err)
	}

	if probe.Protocol != "ICMP" {
		t.Errorf("protocol: got %q, want normalized %q", probe.Protocol, "ICMP")
	}

	got, err := b.GetProbe(ctx, "proto-mon", probe.ProbeID)
	if err != nil {
		t.Fatalf("get probe: %v", err)
	}

	if got.Protocol != "ICMP" {
		t.Errorf("stored protocol: got %q, want normalized %q", got.Protocol, "ICMP")
	}
}

func TestUpdateProbeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		req     networkmonitor.UpdateProbeRequestForTest
		wantErr bool
	}{
		{
			name:    "valid destination update",
			req:     networkmonitor.UpdateProbeRequestForTest{Destination: "10.0.0.9"},
			wantErr: false,
		},
		{
			name:    "invalid protocol",
			req:     networkmonitor.UpdateProbeRequestForTest{Protocol: "UDP"},
			wantErr: true,
		},
		{
			name:    "invalid destination port",
			req:     networkmonitor.UpdateProbeRequestForTest{DestinationPort: ptr(int32(70000))},
			wantErr: true,
		},
		{
			name:    "invalid packet size",
			req:     networkmonitor.UpdateProbeRequestForTest{PacketSize: ptr(int32(10))},
			wantErr: true,
		},
		{
			name:    "invalid state",
			req:     networkmonitor.UpdateProbeRequestForTest{State: "BOGUS"},
			wantErr: true,
		},
		{
			name:    "valid state",
			req:     networkmonitor.UpdateProbeRequestForTest{State: "inactive"},
			wantErr: false,
		},
		{
			name:    "switch to TCP without a port is rejected",
			req:     networkmonitor.UpdateProbeRequestForTest{Protocol: "TCP"},
			wantErr: true,
		},
		{
			name: "switch to TCP with a port is accepted",
			req: networkmonitor.UpdateProbeRequestForTest{
				Protocol:        "TCP",
				DestinationPort: ptr(int32(443)),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			ctx := context.Background()

			monName := "upd-probe-mon"

			if _, err := b.CreateMonitor(ctx, monName, nil, nil, nil); err != nil {
				t.Fatalf("create monitor: %v", err)
			}

			probe, err := b.CreateProbe(ctx, monName, &networkmonitor.ProbeInputForTest{
				Destination: "10.0.0.1",
				Protocol:    "ICMP",
				SourceArn:   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
			}, nil)
			if err != nil {
				t.Fatalf("create probe: %v", err)
			}

			updated, updErr := b.UpdateProbe(ctx, monName, probe.ProbeID, tt.req.ToUpdateProbeRequest())

			if tt.wantErr && updErr == nil {
				t.Fatal("expected validation error, got nil")
			}

			if !tt.wantErr {
				if updErr != nil {
					t.Fatalf("unexpected error: %v", updErr)
				}

				if tt.req.State != "" && updated.State != strings.ToUpper(tt.req.State) {
					t.Errorf("state: got %q, want normalized %q", updated.State, strings.ToUpper(tt.req.State))
				}

				if tt.req.Protocol != "" && updated.Protocol != strings.ToUpper(tt.req.Protocol) {
					t.Errorf(
						"protocol: got %q, want normalized %q",
						updated.Protocol,
						strings.ToUpper(tt.req.Protocol),
					)
				}
			}
		})
	}
}

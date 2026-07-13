package networkmonitor_test

import (
	"context"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

func newTestBackend(t *testing.T) *networkmonitor.InMemoryBackend {
	t.Helper()

	return networkmonitor.NewInMemoryBackend("us-east-1", "000000000000")
}

func ptr[T any](v T) *T {
	p := new(T)
	*p = v

	return p
}

func TestCreateMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		aggregationPeriod *int64
		name              string
		monitorName       string
		wantState         string
		wantPeriod        int64
		wantErr           bool
	}{
		{
			name:        "valid monitor no period",
			monitorName: "test-monitor",
			wantState:   "ACTIVE",
			wantPeriod:  60,
		},
		{
			name:              "valid monitor period 30",
			monitorName:       "monitor-30",
			aggregationPeriod: ptr(int64(30)),
			wantState:         "ACTIVE",
			wantPeriod:        30,
		},
		{
			name:              "valid monitor period 60",
			monitorName:       "monitor-60",
			aggregationPeriod: ptr(int64(60)),
			wantState:         "ACTIVE",
			wantPeriod:        60,
		},
		{
			name:              "invalid period",
			monitorName:       "bad-period",
			aggregationPeriod: ptr(int64(45)),
			wantErr:           true,
		},
		{
			name:        "invalid monitor name",
			monitorName: "bad name!",
			wantErr:     true,
		},
		{
			name:        "empty name",
			monitorName: "",
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			m, err := b.CreateMonitor(
				context.Background(),
				tc.monitorName,
				tc.aggregationPeriod,
				nil,
				nil,
			)

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if m.MonitorName != tc.monitorName {
				t.Errorf("name: got %q, want %q", m.MonitorName, tc.monitorName)
			}

			if m.State != tc.wantState {
				t.Errorf("state: got %q, want %q", m.State, tc.wantState)
			}

			if m.AggregationPeriod != tc.wantPeriod {
				t.Errorf("period: got %d, want %d", m.AggregationPeriod, tc.wantPeriod)
			}

			if m.MonitorArn == "" {
				t.Error("expected non-empty MonitorArn")
			}
		})
	}
}

func TestCreateMonitorDuplicate(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	ctx := context.Background()

	if _, err := b.CreateMonitor(ctx, "dup-monitor", nil, nil, nil); err != nil {
		t.Fatalf("first create: %v", err)
	}

	if _, err := b.CreateMonitor(ctx, "dup-monitor", nil, nil, nil); err == nil {
		t.Fatal("expected conflict error, got nil")
	}
}

func TestGetMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		monitorName string
		create      bool
		wantErr     bool
	}{
		{
			name:        "existing monitor",
			create:      true,
			monitorName: "exists",
		},
		{
			name:        "missing monitor",
			create:      false,
			monitorName: "missing",
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			ctx := context.Background()

			if tc.create {
				if _, err := b.CreateMonitor(ctx, tc.monitorName, nil, nil, nil); err != nil {
					t.Fatalf("create: %v", err)
				}
			}

			m, err := b.GetMonitor(ctx, tc.monitorName)

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if m.MonitorName != tc.monitorName {
				t.Errorf("name: got %q, want %q", m.MonitorName, tc.monitorName)
			}
		})
	}
}

func TestDeleteMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		monitorName string
		create      bool
		wantErr     bool
	}{
		{
			name:        "delete existing",
			create:      true,
			monitorName: "to-delete",
		},
		{
			name:        "delete missing",
			create:      false,
			monitorName: "ghost",
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			ctx := context.Background()

			if tc.create {
				if _, err := b.CreateMonitor(ctx, tc.monitorName, nil, nil, nil); err != nil {
					t.Fatalf("create: %v", err)
				}
			}

			err := b.DeleteMonitor(ctx, tc.monitorName)

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if _, getErr := b.GetMonitor(ctx, tc.monitorName); getErr == nil {
				t.Fatal("expected not-found after delete, got nil")
			}
		})
	}
}

func TestUpdateMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		monitorName       string
		aggregationPeriod int64
		wantErr           bool
		wantPeriod        int64
	}{
		{
			name:              "update to 30",
			monitorName:       "mon",
			aggregationPeriod: 30,
			wantPeriod:        30,
		},
		{
			name:              "update to 60",
			monitorName:       "mon",
			aggregationPeriod: 60,
			wantPeriod:        60,
		},
		{
			name:              "invalid period",
			monitorName:       "mon",
			aggregationPeriod: 45,
			wantErr:           true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			ctx := context.Background()

			if _, err := b.CreateMonitor(ctx, tc.monitorName, nil, nil, nil); err != nil {
				t.Fatalf("create: %v", err)
			}

			m, err := b.UpdateMonitor(ctx, tc.monitorName, tc.aggregationPeriod)

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if m.AggregationPeriod != tc.wantPeriod {
				t.Errorf("period: got %d, want %d", m.AggregationPeriod, tc.wantPeriod)
			}
		})
	}
}

func TestListMonitors(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	ctx := context.Background()

	for _, name := range []string{"alpha", "beta", "gamma"} {
		if _, err := b.CreateMonitor(ctx, name, nil, nil, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	summaries, token, err := b.ListMonitors(ctx, "", "", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(summaries) != 3 {
		t.Errorf("count: got %d, want 3", len(summaries))
	}

	if token != "" {
		t.Errorf("unexpected next token: %q", token)
	}
}

func TestListMonitorsPagination(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	ctx := context.Background()

	for _, name := range []string{"a-monitor", "b-monitor", "c-monitor", "d-monitor", "e-monitor"} {
		if _, err := b.CreateMonitor(ctx, name, nil, nil, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	page1, token, err := b.ListMonitors(ctx, "", "", 2)
	if err != nil {
		t.Fatalf("page1: %v", err)
	}

	if len(page1) != 2 {
		t.Errorf("page1 count: got %d, want 2", len(page1))
	}

	if token == "" {
		t.Fatal("expected next token for page2")
	}

	page2, _, err := b.ListMonitors(ctx, "", token, 2)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}

	if len(page2) != 2 {
		t.Errorf("page2 count: got %d, want 2", len(page2))
	}
}

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

func TestTagging(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	ctx := context.Background()

	m, err := b.CreateMonitor(ctx, "tagged-mon", nil, nil, map[string]string{"env": "test"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	tags, err := b.ListTagsForResource(ctx, m.MonitorArn)
	if err != nil {
		t.Fatalf("list tags: %v", err)
	}

	if tags["env"] != "test" {
		t.Errorf("tag env: got %q, want test", tags["env"])
	}

	if tagErr := b.TagResource(ctx, m.MonitorArn, map[string]string{"team": "sre"}); tagErr != nil {
		t.Fatalf("tag resource: %v", tagErr)
	}

	tags, err = b.ListTagsForResource(ctx, m.MonitorArn)
	if err != nil {
		t.Fatalf("list tags after add: %v", err)
	}

	if tags["team"] != "sre" {
		t.Errorf("tag team: got %q, want sre", tags["team"])
	}

	if untagErr := b.UntagResource(ctx, m.MonitorArn, []string{"env"}); untagErr != nil {
		t.Fatalf("untag: %v", untagErr)
	}

	tags, err = b.ListTagsForResource(ctx, m.MonitorArn)
	if err != nil {
		t.Fatalf("list tags after remove: %v", err)
	}

	if _, ok := tags["env"]; ok {
		t.Error("expected env tag removed")
	}
}

func TestRegionIsolation(t *testing.T) {
	t.Parallel()

	b := networkmonitor.NewInMemoryBackend("us-east-1", "000000000000")

	ctxEast := networkmonitor.WithRegion("us-east-1")
	ctxWest := networkmonitor.WithRegion("us-west-2")

	if _, err := b.CreateMonitor(ctxEast, "regional-mon", nil, nil, nil); err != nil {
		t.Fatalf("create in us-east-1: %v", err)
	}

	if _, err := b.GetMonitor(ctxWest, "regional-mon"); err == nil {
		t.Fatal("expected not-found in us-west-2")
	}

	if _, err := b.GetMonitor(ctxEast, "regional-mon"); err != nil {
		t.Fatalf("expected found in us-east-1: %v", err)
	}
}

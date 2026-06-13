package networkmonitor_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

func newTestBackend(t *testing.T) *networkmonitor.InMemoryBackend {
	t.Helper()

	return networkmonitor.NewInMemoryBackend("us-east-1", "000000000000")
}

func ptr[T any](v T) *T { return &v }

func TestCreateMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		monitorName       string
		aggregationPeriod *int64
		wantErr           bool
		wantState         string
		wantPeriod        int64
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
			m, err := b.CreateMonitor(context.Background(), tc.monitorName, tc.aggregationPeriod, nil, nil)

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
		create      bool
		monitorName string
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
		create      bool
		monitorName string
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

			if _, err := b.GetMonitor(ctx, tc.monitorName); err == nil {
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

	if probe.ProbeId == "" {
		t.Error("expected non-empty ProbeId")
	}

	got, err := b.GetProbe(ctx, "probe-mon", probe.ProbeId)
	if err != nil {
		t.Fatalf("get probe: %v", err)
	}

	if got.Destination != "10.0.0.1" {
		t.Errorf("destination: got %q, want 10.0.0.1", got.Destination)
	}

	if err := b.DeleteProbe(ctx, "probe-mon", probe.ProbeId); err != nil {
		t.Fatalf("delete probe: %v", err)
	}

	if _, err := b.GetProbe(ctx, "probe-mon", probe.ProbeId); err == nil {
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

	if err := b.TagResource(ctx, m.MonitorArn, map[string]string{"team": "sre"}); err != nil {
		t.Fatalf("tag resource: %v", err)
	}

	tags, err = b.ListTagsForResource(ctx, m.MonitorArn)
	if err != nil {
		t.Fatalf("list tags after add: %v", err)
	}

	if tags["team"] != "sre" {
		t.Errorf("tag team: got %q, want sre", tags["team"])
	}

	if err := b.UntagResource(ctx, m.MonitorArn, []string{"env"}); err != nil {
		t.Fatalf("untag: %v", err)
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

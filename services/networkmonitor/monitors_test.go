package networkmonitor_test

import (
	"context"
	"testing"
)

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

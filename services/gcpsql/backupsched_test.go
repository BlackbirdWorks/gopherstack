package gcpsql_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func backupSettings(enabled bool, start string, keep int64) *sqladmin.Settings {
	return &sqladmin.Settings{BackupConfiguration: &sqladmin.BackupConfiguration{
		Enabled: enabled, StartTime: start,
		BackupRetentionSettings: &sqladmin.BackupRetentionSettings{RetentionUnit: "COUNT", RetainedBackups: keep},
	}}
}

func TestScheduledBackups(t *testing.T) {
	t.Parallel()

	day := func(d, h, m int) time.Time { return time.Date(2026, 5, d, h, m, 0, 0, time.UTC) }

	tests := []struct {
		name     string
		start    string
		wantRuns []int
		times    []time.Time
		enabled  bool
	}{
		{
			name: "before the window", enabled: true, start: "02:00",
			times: []time.Time{day(1, 1, 0)}, wantRuns: []int{0},
		},
		{
			name: "after the window start", enabled: true, start: "02:00",
			times: []time.Time{day(1, 2, 30)}, wantRuns: []int{1},
		},
		{
			name: "once per day", enabled: true, start: "02:00",
			times: []time.Time{day(1, 2, 30), day(1, 2, 40), day(1, 23, 0)}, wantRuns: []int{1, 0, 0},
		},
		{
			name: "again the next day", enabled: true, start: "02:00",
			times: []time.Time{day(1, 3, 0), day(2, 3, 0)}, wantRuns: []int{1, 1},
		},
		{name: "disabled", start: "02:00", times: []time.Time{day(1, 3, 0)}, wantRuns: []int{0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "b1", "POSTGRES_16", backupSettings(tt.enabled, tt.start, 7))

			for i, at := range tt.times {
				assert.Equal(t, tt.wantRuns[i], e.backend.RunScheduledBackups(at))
				e.backend.Drain()
			}
		})
	}
}

func TestScheduledBackupRunsAndRetention(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "b2", "POSTGRES_16", backupSettings(true, "02:00", 2))

	op, err := e.svc.BackupRuns.Insert(project, "b2", &sqladmin.BackupRun{}).Context(t.Context()).Do()
	e.mustWait(t, op, err)

	for d := 1; d <= 4; d++ {
		require.Equal(t, 1, e.backend.RunScheduledBackups(time.Date(2026, 5, d, 3, 0, 0, 0, time.UTC)))
		e.backend.Drain()
	}

	list, err := e.svc.BackupRuns.List(project, "b2").Context(t.Context()).Do()
	require.NoError(t, err)

	var auto, manual int

	for _, r := range list.Items {
		assert.Equal(t, "SUCCESSFUL", r.Status)

		switch r.Type {
		case "AUTOMATED":
			auto++

			assert.Contains(t, r.WindowStartTime, "T02:00:00")
		case "ON_DEMAND":
			manual++
		}
	}

	assert.Equal(t, 2, auto, "only the two newest automated backups are retained")
	assert.Equal(t, 1, manual, "on-demand backups are never pruned")
}

func TestBackupSchedulerStopsOnClose(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.backend.StartBackupScheduler(time.Hour)
	e.backend.Reset()
	e.backend.Close()
}

func TestScheduledBackupsSkipStoppedInstances(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "b3", "POSTGRES_16", backupSettings(true, "02:00", 7))

	op, err := e.svc.Instances.Patch(project, "b3", &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{ActivationPolicy: "NEVER"},
	}).Context(t.Context()).Do()
	e.mustWait(t, op, err)

	at := time.Date(2026, 5, 1, 3, 0, 0, 0, time.UTC)
	assert.Equal(t, 0, e.backend.RunScheduledBackups(at), "a stopped instance takes no automated backup")

	op, err = e.svc.Instances.Patch(project, "b3", &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{ActivationPolicy: "ALWAYS"},
	}).Context(t.Context()).Do()
	e.mustWait(t, op, err)

	assert.Equal(t, 1, e.backend.RunScheduledBackups(at), "the missed window runs once the instance is back")
	e.backend.Drain()
}

package gcpsql_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func pitrSettings(version string, on bool) *sqladmin.Settings {
	bc := &sqladmin.BackupConfiguration{Enabled: true}

	if version == "POSTGRES_16" {
		bc.PointInTimeRecoveryEnabled = on
	} else {
		bc.BinaryLogEnabled = on
	}

	return &sqladmin.Settings{BackupConfiguration: bc}
}

func TestLatestRecoveryTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		on      bool
		want    int
	}{
		{name: "postgres enabled", version: "POSTGRES_16", on: true, want: http.StatusOK},
		{name: "mysql enabled", version: "MYSQL_8_4", on: true, want: http.StatusOK},
		{name: "postgres disabled", version: "POSTGRES_16", on: false, want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := &fakeLauncher{}
			e := newEnvWith(t, l)
			e.createInstance(t, "src", tt.version, pitrSettings(tt.version, tt.on))

			got, err := e.svc.Projects.Instances.GetLatestRecoveryTime(project, "src").Context(t.Context()).Do()
			if tt.want != http.StatusOK {
				require.Error(t, err)
				assert.Equal(t, tt.want, statusOf(t, err))
				assert.Zero(t, l.engineFor("src").snapshot().pitrEnabled)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "sql#getLatestRecoveryTime", got.Kind)

			earliest, perr := time.Parse(time.RFC3339, got.EarliestRecoveryTime)
			require.NoError(t, perr)
			latest, perr := time.Parse(time.RFC3339, got.LatestRecoveryTime)
			require.NoError(t, perr)
			assert.True(t, latest.After(earliest))
			assert.Equal(t, 1, l.engineFor("src").snapshot().pitrEnabled, "the engine starts archiving")
		})
	}
}

func TestPointInTimeClone(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name    string
		version string
		at      string
		wantMsg string
		want    int
	}{
		{
			name:    "postgres in window",
			version: "POSTGRES_16",
			at:      now.Add(-10 * time.Minute).Format(time.RFC3339Nano),
			want:    200,
		},
		{
			name:    "mysql in window",
			version: "MYSQL_8_4",
			at:      now.Add(-10 * time.Minute).Format(time.RFC3339Nano),
			want:    200,
		},
		{
			name: "after latest", version: "POSTGRES_16", at: now.Add(time.Hour).Format(time.RFC3339Nano),
			want: http.StatusBadRequest, wantMsg: "after the latest recovery time",
		},
		{
			name: "before earliest", version: "POSTGRES_16", at: now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
			want: http.StatusBadRequest, wantMsg: "before the earliest recovery time",
		},
		{
			name: "malformed", version: "POSTGRES_16", at: "yesterday",
			want: http.StatusBadRequest, wantMsg: "Failed to parse date/time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := &fakeLauncher{}
			e := newEnvWith(t, l)
			e.createInstance(t, "src", tt.version, pitrSettings(tt.version, true))

			op, err := e.svc.Instances.Clone(project, "src", &sqladmin.InstancesCloneRequest{
				CloneContext: &sqladmin.CloneContext{DestinationInstanceName: "dst", PointInTime: tt.at},
			}).Context(t.Context()).Do()

			if tt.want != 200 {
				require.Error(t, err)
				assert.Equal(t, tt.want, statusOf(t, err))
				assert.Contains(t, err.Error(), tt.wantMsg)

				return
			}

			e.mustWait(t, op, err)

			want, perr := time.Parse(time.RFC3339Nano, tt.at)
			require.NoError(t, perr)

			src := l.engineFor("src").snapshot()
			require.Len(t, src.prepared, 1)
			assert.WithinDuration(t, want, src.prepared[0], time.Millisecond)

			spec := l.specFor("dst")
			require.NotNil(t, spec.Recovery, "the clone launches from the source's recovery state")
			assert.Equal(t, "pitr-vol", spec.Recovery.Volume)

			dst := l.engineFor("dst").snapshot()
			require.Len(t, dst.applied, 1)
			assert.Equal(t, "replay", string(dst.applied[0].Script))
		})
	}
}

func TestPointInTimeCloneNeedsPITR(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "src", "POSTGRES_16", nil)

	_, err := e.svc.Instances.Clone(project, "src", &sqladmin.InstancesCloneRequest{
		CloneContext: &sqladmin.CloneContext{
			DestinationInstanceName: "dst", PointInTime: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
		},
	}).Context(t.Context()).Do()
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, statusOf(t, err))
	assert.Contains(t, err.Error(), "not enabled")
}

func TestPITRSettingFollowsPatch(t *testing.T) {
	t.Parallel()

	l := &fakeLauncher{}
	e := newEnvWith(t, l)
	ctx := t.Context()

	e.createInstance(t, "src", "POSTGRES_16", nil)

	op, err := e.svc.Instances.Patch(project, "src", &sqladmin.DatabaseInstance{
		Settings: pitrSettings("POSTGRES_16", true),
	}).Context(ctx).Do()
	e.mustWait(t, op, err)
	assert.Equal(t, 1, l.engineFor("src").snapshot().pitrEnabled)

	_, err = e.svc.Projects.Instances.GetLatestRecoveryTime(project, "src").Context(ctx).Do()
	require.NoError(t, err)

	disable := pitrSettings("POSTGRES_16", false)
	disable.BackupConfiguration.ForceSendFields = []string{"PointInTimeRecoveryEnabled"}

	op, err = e.svc.Instances.Patch(project, "src", &sqladmin.DatabaseInstance{Settings: disable}).Context(ctx).Do()
	e.mustWait(t, op, err)
	assert.Equal(t, 1, l.engineFor("src").snapshot().pitrDisabled)

	_, err = e.svc.Projects.Instances.GetLatestRecoveryTime(project, "src").Context(ctx).Do()
	require.Error(t, err)
}

func TestPITRStateOnly(t *testing.T) {
	t.Parallel()

	e := newEnvWith(t, nil)
	e.createInstance(t, "src", "POSTGRES_16", pitrSettings("POSTGRES_16", true))

	got, err := e.svc.Projects.Instances.GetLatestRecoveryTime(project, "src").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.NotEmpty(t, got.EarliestRecoveryTime)

	op, err := e.svc.Instances.Clone(project, "src", &sqladmin.InstancesCloneRequest{
		CloneContext: &sqladmin.CloneContext{
			DestinationInstanceName: "dst", PointInTime: time.Now().UTC().Format(time.RFC3339Nano),
		},
	}).Context(t.Context()).Do()
	e.mustWait(t, op, err)
}

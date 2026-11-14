package ocidatabase_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/database"
	"github.com/oracle/oci-go-sdk/v65/workrequests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) backups(
	t *testing.T,
	req database.ListAutonomousDatabaseBackupsRequest,
) []database.AutonomousDatabaseBackupSummary {
	t.Helper()

	resp, err := e.client.ListAutonomousDatabaseBackups(t.Context(), req)
	require.NoError(t, err)

	return resp.Items
}

func TestBackups(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.available(t, createOpts{})

	items := e.backups(t, database.ListAutonomousDatabaseBackupsRequest{AutonomousDatabaseId: db.Id})
	require.Len(t, items, 1)
	assert.True(t, *items[0].IsAutomatic)
	assert.Equal(t, database.AutonomousDatabaseBackupSummaryLifecycleStateActive, items[0].LifecycleState)
	assert.Equal(t, 60, *items[0].RetentionPeriodInDays)

	resp, err := e.client.CreateAutonomousDatabaseBackup(t.Context(), database.CreateAutonomousDatabaseBackupRequest{
		AutonomousDatabaseId: db.Id, DisplayName: new("before-change"),
	})
	require.NoError(t, err)
	assert.Equal(t, database.AutonomousDatabaseBackupLifecycleStateCreating, resp.LifecycleState)
	assert.False(t, *resp.IsAutomatic)
	assert.Equal(t, database.AutonomousDatabaseBackupTypeIncremental, resp.Type)
	assert.Equal(t, database.AutonomousDatabaseLifecycleStateBackupInProgress, e.get(t, db.Id).LifecycleState)
	assert.Equal(t, "IN_PROGRESS", string(e.wr(t, resp.OpcWorkRequestId).Status))

	_, err = e.client.CreateAutonomousDatabaseBackup(t.Context(), database.CreateAutonomousDatabaseBackupRequest{
		AutonomousDatabaseId: db.Id,
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "one backup at a time")

	e.settle()
	assert.Equal(t, database.AutonomousDatabaseLifecycleStateAvailable, e.get(t, db.Id).LifecycleState)
	assert.Equal(t, "SUCCEEDED", string(e.wr(t, resp.OpcWorkRequestId).Status))

	got, err := e.client.GetAutonomousDatabaseBackup(
		t.Context(),
		database.GetAutonomousDatabaseBackupRequest{AutonomousDatabaseBackupId: resp.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, database.AutonomousDatabaseBackupLifecycleStateActive, got.LifecycleState)
	assert.True(t, *got.IsRestorable)
	assert.NotNil(t, got.TimeEnded)

	tests := []struct {
		req  database.ListAutonomousDatabaseBackupsRequest
		name string
		want int
	}{
		{name: "by-database", req: database.ListAutonomousDatabaseBackupsRequest{AutonomousDatabaseId: db.Id}, want: 2},
		{
			name: "by-compartment",
			req:  database.ListAutonomousDatabaseBackupsRequest{CompartmentId: new(testCompartment)},
			want: 2,
		},
		{
			name: "by-name",
			req: database.ListAutonomousDatabaseBackupsRequest{
				AutonomousDatabaseId: db.Id,
				DisplayName:          new("before-change"),
			},
			want: 1,
		},
		{
			name: "by-type",
			req:  database.ListAutonomousDatabaseBackupsRequest{AutonomousDatabaseId: db.Id, Type: new("LONGTERM")},
			want: 0,
		},
		{
			name: "by-state", want: 2,
			req: database.ListAutonomousDatabaseBackupsRequest{
				AutonomousDatabaseId: db.Id,
				LifecycleState:       database.AutonomousDatabaseBackupSummaryLifecycleStateActive,
			},
		},
	}

	for _, tc := range tests {
		assert.Len(t, e.backups(t, tc.req), tc.want, tc.name)
	}

	_, err = e.client.ListAutonomousDatabaseBackups(t.Context(), database.ListAutonomousDatabaseBackupsRequest{})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	delResp, err := e.client.DeleteAutonomousDatabaseBackup(
		t.Context(),
		database.DeleteAutonomousDatabaseBackupRequest{AutonomousDatabaseBackupId: resp.Id},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, *delResp.OpcWorkRequestId)
	e.settle()
	assert.Len(t, e.backups(t, database.ListAutonomousDatabaseBackupsRequest{AutonomousDatabaseId: db.Id}), 1)

	_, err = e.client.DeleteAutonomousDatabaseBackup(
		t.Context(),
		database.DeleteAutonomousDatabaseBackupRequest{AutonomousDatabaseBackupId: items[0].Id},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "automatic backups expire on their own")

	_, err = e.client.GetAutonomousDatabaseBackup(
		t.Context(),
		database.GetAutonomousDatabaseBackupRequest{AutonomousDatabaseBackupId: resp.Id},
	)
	require.NoError(t, err, "deleted backups stay readable")
}

func TestLongTermBackupAndTerminate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mustDelete  bool
		wantBackups int
	}{
		{name: "keeps-long-term", wantBackups: 1},
		{name: "deletes-long-term", mustDelete: true, wantBackups: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.available(t, createOpts{})

			_, err := e.client.CreateAutonomousDatabaseBackup(
				t.Context(),
				database.CreateAutonomousDatabaseBackupRequest{
					AutonomousDatabaseId: db.Id, IsLongTermBackup: new(true), RetentionPeriodInDays: new(30),
				},
			)
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "retention below 90 days")

			resp, err := e.client.CreateAutonomousDatabaseBackup(
				t.Context(),
				database.CreateAutonomousDatabaseBackupRequest{
					AutonomousDatabaseId: db.Id, IsLongTermBackup: new(true), RetentionPeriodInDays: new(120),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, database.AutonomousDatabaseBackupTypeLongterm, resp.Type)
			e.settle()

			_, err = e.client.DeleteAutonomousDatabase(t.Context(), database.DeleteAutonomousDatabaseRequest{
				AutonomousDatabaseId: db.Id, MustDeleteAssociatedLongTermBackups: new(tc.mustDelete),
			})
			require.NoError(t, err)
			e.settle()

			got := e.backups(t, database.ListAutonomousDatabaseBackupsRequest{AutonomousDatabaseId: db.Id})
			assert.Len(t, got, tc.wantBackups)
		})
	}
}

func TestRestore(t *testing.T) {
	t.Parallel()

	now := func(e *env) *common.SDKTime { return &common.SDKTime{Time: e.clock.Now()} }

	tests := []struct {
		details  func(e *env) database.RestoreAutonomousDatabaseDetails
		name     string
		wantHTTP int
	}{
		{name: "latest", details: func(*env) database.RestoreAutonomousDatabaseDetails {
			return database.RestoreAutonomousDatabaseDetails{Latest: new(true)}
		}},
		{name: "timestamp", details: func(e *env) database.RestoreAutonomousDatabaseDetails {
			return database.RestoreAutonomousDatabaseDetails{Timestamp: now(e)}
		}},
		{name: "scn", details: func(*env) database.RestoreAutonomousDatabaseDetails {
			return database.RestoreAutonomousDatabaseDetails{DatabaseSCN: new("12345")}
		}},
		{name: "none", wantHTTP: http.StatusBadRequest, details: func(*env) database.RestoreAutonomousDatabaseDetails {
			return database.RestoreAutonomousDatabaseDetails{}
		}},
		{name: "two", wantHTTP: http.StatusBadRequest, details: func(*env) database.RestoreAutonomousDatabaseDetails {
			return database.RestoreAutonomousDatabaseDetails{Latest: new(true), DatabaseSCN: new("1")}
		}},
		{
			name:     "future",
			wantHTTP: http.StatusBadRequest,
			details: func(e *env) database.RestoreAutonomousDatabaseDetails {
				return database.RestoreAutonomousDatabaseDetails{
					Timestamp: &common.SDKTime{Time: e.clock.Now().Add(time.Hour)},
				}
			},
		},
		{
			name:     "before-creation",
			wantHTTP: http.StatusBadRequest,
			details: func(e *env) database.RestoreAutonomousDatabaseDetails {
				return database.RestoreAutonomousDatabaseDetails{
					Timestamp: &common.SDKTime{Time: e.clock.Now().Add(-24 * time.Hour)},
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.available(t, createOpts{})

			resp, err := e.client.RestoreAutonomousDatabase(t.Context(), database.RestoreAutonomousDatabaseRequest{
				AutonomousDatabaseId: db.Id, RestoreAutonomousDatabaseDetails: tc.details(e),
			})
			if tc.wantHTTP != 0 {
				assert.Equal(t, tc.wantHTTP, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, database.AutonomousDatabaseLifecycleStateRestoreInProgress, e.get(t, db.Id).LifecycleState)
			e.settle()
			assert.Equal(t, database.AutonomousDatabaseLifecycleStateAvailable, e.get(t, db.Id).LifecycleState)
			assert.Equal(t, "SUCCEEDED", string(e.wr(t, resp.OpcWorkRequestId).Status))
		})
	}
}

func TestWorkRequests(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	created := e.mustCreate(t, createOpts{})
	e.settle()

	stop, err := e.client.StopAutonomousDatabase(
		t.Context(),
		database.StopAutonomousDatabaseRequest{AutonomousDatabaseId: created.Id},
	)
	require.NoError(t, err)

	wr := e.wr(t, created.OpcWorkRequestId)
	assert.Equal(t, "CREATE_AUTONOMOUS_DATABASE", *wr.OperationType)
	assert.Equal(t, testCompartment, *wr.CompartmentId)
	require.Len(t, wr.Resources, 1)
	assert.Equal(t, *created.Id, *wr.Resources[0].Identifier)
	assert.Equal(t, workrequests.WorkRequestResourceActionTypeCreated, wr.Resources[0].ActionType)
	assert.InDelta(t, 100, *wr.PercentComplete, 0)
	assert.NotNil(t, wr.TimeFinished)

	assert.Less(t, *e.wr(t, stop.OpcWorkRequestId).PercentComplete, float32(100))

	list, err := e.wrs.ListWorkRequests(
		t.Context(),
		workrequests.ListWorkRequestsRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.Len(t, list.Items, 2)

	byRes, err := e.wrs.ListWorkRequests(t.Context(), workrequests.ListWorkRequestsRequest{
		CompartmentId: new(testCompartment), ResourceId: new("ocid1.autonomousdatabase.oc1..other"),
	})
	require.NoError(t, err)
	assert.Empty(t, byRes.Items)

	logs, err := e.wrs.ListWorkRequestLogs(
		t.Context(),
		workrequests.ListWorkRequestLogsRequest{WorkRequestId: created.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Len(t, logs.Items, 3)

	errs, err := e.wrs.ListWorkRequestErrors(
		t.Context(),
		workrequests.ListWorkRequestErrorsRequest{WorkRequestId: created.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Empty(t, errs.Items)

	_, err = e.wrs.GetWorkRequest(
		t.Context(),
		workrequests.GetWorkRequestRequest{WorkRequestId: new("ocid1.workrequest.oc1..nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.wrs.ListWorkRequests(t.Context(), workrequests.ListWorkRequestsRequest{CompartmentId: new("")})
	require.Error(t, err)
}

func TestAlwaysFreeLimits(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	free := e.available(
		t,
		createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.IsFreeTier = new(true) }},
	)

	tests := []struct {
		call func() error
		name string
	}{
		{name: "manual-backup", call: func() error {
			_, err := e.client.CreateAutonomousDatabaseBackup(
				t.Context(),
				database.CreateAutonomousDatabaseBackupRequest{
					AutonomousDatabaseId: free.Id, DisplayName: new("manual"),
				},
			)

			return err
		}},
		{name: "restore", call: func() error {
			_, err := e.client.RestoreAutonomousDatabase(t.Context(), database.RestoreAutonomousDatabaseRequest{
				AutonomousDatabaseId: free.Id,
				Latest:               new(true),
			})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call()
			require.Error(t, err)
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestFreeToPaidMinimum(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	free := e.available(
		t,
		createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.IsFreeTier = new(true) }},
	)

	_, err := e.client.UpdateAutonomousDatabase(t.Context(), database.UpdateAutonomousDatabaseRequest{
		AutonomousDatabaseId: free.Id,
		IsFreeTier:           new(false),
	})
	require.NoError(t, err)
	e.settle()

	got := e.get(t, free.Id)
	assert.False(t, *got.IsFreeTier)
	assert.InDelta(t, 4, *got.ComputeCount, 0)
	assert.Equal(t, 1024, *got.DataStorageSizeInGBs)
}

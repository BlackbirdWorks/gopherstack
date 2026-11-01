package ocidatabase_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) update(
	t *testing.T,
	id *string,
	d database.UpdateAutonomousDatabaseDetails,
) (database.UpdateAutonomousDatabaseResponse, error) {
	t.Helper()

	return e.client.UpdateAutonomousDatabase(t.Context(), database.UpdateAutonomousDatabaseRequest{
		AutonomousDatabaseId: id, UpdateAutonomousDatabaseDetails: d,
	})
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update      database.UpdateAutonomousDatabaseDetails
		check       func(*testing.T, database.AutonomousDatabase)
		create      func(*database.CreateAutonomousDatabaseDetails)
		name        string
		wantDuring  database.AutonomousDatabaseLifecycleStateEnum
		wantCode    string
		wantHTTP    int
		wantInstant bool
	}{
		{
			name: "rename-display-name", wantInstant: true,
			update: database.UpdateAutonomousDatabaseDetails{DisplayName: new("renamed")},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, "renamed", *db.DisplayName)
			},
		},
		{
			name: "tags-only", wantInstant: true,
			update: database.UpdateAutonomousDatabaseDetails{FreeformTags: map[string]string{"k": "v"}},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, map[string]string{"k": "v"}, db.FreeformTags)
			},
		},
		{
			name:       "scale-compute",
			wantDuring: database.AutonomousDatabaseLifecycleStateScaleInProgress,
			update: database.UpdateAutonomousDatabaseDetails{
				ComputeModel: database.UpdateAutonomousDatabaseDetailsComputeModelEcpu, ComputeCount: new(float32(8)),
			},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.InDelta(t, 8, *db.ComputeCount, 0)
			},
		},
		{
			name:       "scale-count-only",
			wantDuring: database.AutonomousDatabaseLifecycleStateScaleInProgress,
			update:     database.UpdateAutonomousDatabaseDetails{ComputeCount: new(float32(6))},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.InDelta(t, 6, *db.ComputeCount, 0)
				assert.Equal(t, database.AutonomousDatabaseComputeModelEcpu, db.ComputeModel)
			},
		},
		{
			name:       "to-ocpu",
			wantDuring: database.AutonomousDatabaseLifecycleStateScaleInProgress,
			update: database.UpdateAutonomousDatabaseDetails{
				ComputeModel: database.UpdateAutonomousDatabaseDetailsComputeModelOcpu, ComputeCount: new(float32(3)),
			},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, database.AutonomousDatabaseComputeModelOcpu, db.ComputeModel)
				assert.InDelta(t, 3, *db.OcpuCount, 0)
			},
		},
		{
			name: "compute-too-small", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			update: database.UpdateAutonomousDatabaseDetails{ComputeCount: new(float32(1))},
		},
		{
			name:       "scale-storage",
			wantDuring: database.AutonomousDatabaseLifecycleStateScaleInProgress,
			update:     database.UpdateAutonomousDatabaseDetails{DataStorageSizeInTBs: new(4)},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, 4, *db.DataStorageSizeInTBs)
			},
		},
		{
			name:       "auto-scaling",
			wantDuring: database.AutonomousDatabaseLifecycleStateScaleInProgress,
			update:     database.UpdateAutonomousDatabaseDetails{IsAutoScalingEnabled: new(true)},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.True(t, *db.IsAutoScalingEnabled)
			},
		},
		{
			name:       "acl",
			wantDuring: database.AutonomousDatabaseLifecycleStateUpdating,
			update:     database.UpdateAutonomousDatabaseDetails{WhitelistedIps: []string{"10.0.0.0/8"}},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, []string{"10.0.0.0/8"}, db.WhitelistedIps)
				assert.True(t, *db.IsAccessControlEnabled)
			},
		},
		{
			name: "acl-invalid", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			update: database.UpdateAutonomousDatabaseDetails{WhitelistedIps: []string{"300.1.1.1"}},
		},
		{
			name:       "mtls-required",
			wantDuring: database.AutonomousDatabaseLifecycleStateUpdating,
			update:     database.UpdateAutonomousDatabaseDetails{IsMtlsConnectionRequired: new(true)},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.True(t, *db.IsMtlsConnectionRequired)
				assert.Len(t, db.ConnectionStrings.Profiles, 5)
			},
		},
		{
			name:       "password",
			wantDuring: database.AutonomousDatabaseLifecycleStateUpdating,
			update:     database.UpdateAutonomousDatabaseDetails{AdminPassword: new("N3wPassw0rd_Adb1")},
		},
		{
			name: "password-weak", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			update: database.UpdateAutonomousDatabaseDetails{AdminPassword: new("weak")},
		},
		{
			name: "version-downgrade", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			update: database.UpdateAutonomousDatabaseDetails{DbVersion: new("19c")},
		},
		{
			name: "free-tier-scale", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			create: func(d *database.CreateAutonomousDatabaseDetails) { d.IsFreeTier = new(true) },
			update: database.UpdateAutonomousDatabaseDetails{ComputeCount: new(float32(8))},
		},
		{
			name:       "free-tier-upgrade",
			wantDuring: database.AutonomousDatabaseLifecycleStateUpdating,
			create:     func(d *database.CreateAutonomousDatabaseDetails) { d.IsFreeTier = new(true) },
			update:     database.UpdateAutonomousDatabaseDetails{IsFreeTier: new(false)},
			check:      func(t *testing.T, db database.AutonomousDatabase) { t.Helper(); assert.False(t, *db.IsFreeTier) },
		},
		{
			name: "free-tier-on-paid", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			update: database.UpdateAutonomousDatabaseDetails{IsFreeTier: new(true)},
		},
		{
			name:       "rename-dbname",
			wantDuring: database.AutonomousDatabaseLifecycleStateUpdating,
			update:     database.UpdateAutonomousDatabaseDetails{DbName: new("newname")},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, "newname", *db.DbName)
				assert.Contains(t, *db.ConnectionStrings.High, "_newname_high")
			},
		},
		{
			name: "dbname-invalid", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			update: database.UpdateAutonomousDatabaseDetails{DbName: new("9bad")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.available(t, createOpts{mutate: tc.create})

			resp, err := e.update(t, db.Id, tc.update)
			if tc.wantHTTP != 0 {
				se := svcErr(t, err)
				assert.Equal(t, tc.wantHTTP, se.GetHTTPStatusCode())
				assert.Equal(t, tc.wantCode, se.GetCode())

				return
			}

			require.NoError(t, err)

			if tc.wantInstant {
				assert.Equal(t, database.AutonomousDatabaseLifecycleStateAvailable, e.get(t, db.Id).LifecycleState)
				assert.Equal(t, "SUCCEEDED", string(e.wr(t, resp.OpcWorkRequestId).Status))
			} else {
				assert.Equal(t, tc.wantDuring, e.get(t, db.Id).LifecycleState)
				assert.Equal(t, tc.wantDuring, resp.LifecycleState)
				e.settle()
			}

			got := e.get(t, db.Id)
			assert.Equal(t, database.AutonomousDatabaseLifecycleStateAvailable, got.LifecycleState)

			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}
}

func TestUpdateStateAndName(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.available(t, createOpts{})

	other := e.mustCreate(
		t,
		createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DbName = new("second") }},
	)
	_, err := e.update(t, db.Id, database.UpdateAutonomousDatabaseDetails{DbName: new("SECOND")})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.update(t, other.Id, database.UpdateAutonomousDatabaseDetails{DisplayName: new("x")})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "still provisioning")

	_, err = e.client.StopAutonomousDatabase(
		t.Context(),
		database.StopAutonomousDatabaseRequest{AutonomousDatabaseId: db.Id},
	)
	require.NoError(t, err)
	e.settle()

	_, err = e.update(t, db.Id, database.UpdateAutonomousDatabaseDetails{DisplayName: new("x")})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "stopped")

	_, err = e.client.UpdateAutonomousDatabase(t.Context(), database.UpdateAutonomousDatabaseRequest{
		AutonomousDatabaseId: db.Id, IfMatch: new("stale"),
		DisplayName: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())
}

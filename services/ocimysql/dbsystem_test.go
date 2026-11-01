package ocimysql_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestDbSystemLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})
	ctx := t.Context()

	d := dbDetails("life")
	d.FreeformTags = map[string]string{"env": "test"}
	d.HostnameLabel = new("lifedb")
	created := e.createDB(t, d)
	assert.Equal(t, my.DbSystemLifecycleStateCreating, created.LifecycleState)
	assert.NotEmpty(t, created.Etag)

	wr, err := e.wr.GetWorkRequest(ctx, my.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationStatusInProgress, wr.Status)
	assert.Equal(t, my.WorkRequestOperationTypeCreateDbsystem, wr.OperationType)

	e.settle()

	got := e.getDB(t, created.Id)
	assert.Equal(t, my.DbSystemLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, "test", got.FreeformTags["env"])
	assert.Equal(t, "8.4.6", *got.MysqlVersion)
	assert.Equal(t, shapeSmall, *got.ShapeName)
	assert.Equal(t, 50, *got.DataStorageSizeInGBs)
	assert.Equal(t, "Uocm:US-ASHBURN-AD-2", *got.AvailabilityDomain)
	assert.Equal(t, "10.0.1.5", *got.IpAddress)
	assert.Equal(t, 3306, *got.Port)
	assert.Equal(t, 33060, *got.PortX)
	assert.Equal(t, my.DbSystemDatabaseModeWrite, got.DatabaseMode)
	assert.Equal(t, my.CrashRecoveryStatusEnabled, got.CrashRecovery)
	assert.True(t, *got.BackupPolicy.IsEnabled)
	assert.Equal(t, 7, *got.BackupPolicy.RetentionInDays)
	assert.False(t, *got.DeletionPolicy.IsDeleteProtected)
	assert.Equal(t, "lifedb", *got.HostnameLabel)
	assert.IsType(t, my.DbSystemSourceFromNone{}, got.Source)
	require.Len(t, got.Endpoints, 1)
	assert.Equal(t, "10.0.1.5", *got.Endpoints[0].IpAddress)
	assert.Equal(t, my.DbSystemEndpointStatusActive, got.Endpoints[0].Status)
	assert.NotEmpty(t, *got.ConfigurationId)

	status, body := rawDo(t, http.MethodGet, e.base+"/20190415/dbSystems/"+*got.Id, "")
	assert.Equal(t, http.StatusUnauthorized, status, "unsigned request is rejected under enforce")
	assert.NotContains(t, body, adminPass)

	wr, err = e.wr.GetWorkRequest(ctx, my.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationStatusSucceeded, wr.Status)
	assert.Equal(t, *got.Id, *wr.Resources[0].Identifier)

	lst, err := e.db.ListDbSystems(ctx, my.ListDbSystemsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, lst.Items, 1)
	assert.Equal(t, "life", *lst.Items[0].DisplayName)
}

func TestDbSystemStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		act     func(e *env, id *string) (*string, error)
		prep    func(t *testing.T, e *env, id *string)
		mid     my.DbSystemLifecycleStateEnum
		end     my.DbSystemLifecycleStateEnum
		wantOp  my.WorkRequestOperationTypeEnum
		wantErr int
	}{
		{
			name: "stop", mid: my.DbSystemLifecycleStateUpdating, end: my.DbSystemLifecycleStateInactive,
			wantOp: my.WorkRequestOperationTypeStopDbsystem,
			act: func(e *env, id *string) (*string, error) {
				r, err := e.db.StopDbSystem(e.ctx(), my.StopDbSystemRequest{
					DbSystemId:   id,
					ShutdownType: my.InnoDbShutdownModeFast,
				})

				return r.OpcWorkRequestId, err
			},
		},
		{
			name: "restart", mid: my.DbSystemLifecycleStateUpdating, end: my.DbSystemLifecycleStateActive,
			wantOp: my.WorkRequestOperationTypeRestartDbsystem,
			act: func(e *env, id *string) (*string, error) {
				r, err := e.db.RestartDbSystem(e.ctx(), my.RestartDbSystemRequest{
					DbSystemId:   id,
					ShutdownType: my.InnoDbShutdownModeImmediate,
				})

				return r.OpcWorkRequestId, err
			},
		},
		{
			name: "start from stopped", mid: my.DbSystemLifecycleStateUpdating, end: my.DbSystemLifecycleStateActive,
			wantOp: my.WorkRequestOperationTypeStartDbsystem,
			prep: func(t *testing.T, e *env, id *string) {
				t.Helper()

				_, err := e.db.StopDbSystem(t.Context(), my.StopDbSystemRequest{
					DbSystemId:   id,
					ShutdownType: my.InnoDbShutdownModeSlow,
				})
				require.NoError(t, err)
				e.settle()
			},
			act: func(e *env, id *string) (*string, error) {
				r, err := e.db.StartDbSystem(e.ctx(), my.StartDbSystemRequest{DbSystemId: id})

				return r.OpcWorkRequestId, err
			},
		},
		{
			name: "start while active", wantErr: http.StatusConflict,
			act: func(e *env, id *string) (*string, error) {
				r, err := e.db.StartDbSystem(e.ctx(), my.StartDbSystemRequest{DbSystemId: id})

				return r.OpcWorkRequestId, err
			},
		},
		{
			name: "delete", mid: my.DbSystemLifecycleStateDeleting, end: my.DbSystemLifecycleStateDeleted,
			wantOp: my.WorkRequestOperationTypeDeleteDbsystem,
			act: func(e *env, id *string) (*string, error) {
				r, err := e.db.DeleteDbSystem(e.ctx(), my.DeleteDbSystemRequest{DbSystemId: id})

				return r.OpcWorkRequestId, err
			},
		},
		{
			name: "restart while stopped", wantErr: http.StatusConflict,
			prep: func(t *testing.T, e *env, id *string) {
				t.Helper()

				_, err := e.db.StopDbSystem(t.Context(), my.StopDbSystemRequest{
					DbSystemId:   id,
					ShutdownType: my.InnoDbShutdownModeFast,
				})
				require.NoError(t, err)
				e.settle()
			},
			act: func(e *env, id *string) (*string, error) {
				r, err := e.db.RestartDbSystem(e.ctx(), my.RestartDbSystemRequest{
					DbSystemId:   id,
					ShutdownType: my.InnoDbShutdownModeFast,
				})

				return r.OpcWorkRequestId, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.mkDB(t, "sm", nil)

			if tt.prep != nil {
				tt.prep(t, e, db.Id)
			}

			wrID, err := tt.act(e, db.Id)
			if tt.wantErr != 0 {
				assert.Equal(t, tt.wantErr, svcErr(t, err).GetHTTPStatusCode())
				assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.mid, e.getDB(t, db.Id).LifecycleState)

			wr, err := e.wr.GetWorkRequest(t.Context(), my.GetWorkRequestRequest{WorkRequestId: wrID})
			require.NoError(t, err)
			assert.Equal(t, tt.wantOp, wr.OperationType)
			assert.Equal(t, my.WorkRequestOperationStatusInProgress, wr.Status)

			e.settle()

			assert.Equal(t, tt.end, e.getDB(t, db.Id).LifecycleState)

			wr, err = e.wr.GetWorkRequest(t.Context(), my.GetWorkRequestRequest{WorkRequestId: wrID})
			require.NoError(t, err)
			assert.Equal(t, my.WorkRequestOperationStatusSucceeded, wr.Status)
		})
	}
}

func TestCreateDbSystemValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod    func(*my.CreateDbSystemDetails)
		name   string
		code   string
		status int
	}{
		{
			func(d *my.CreateDbSystemDetails) { d.ShapeName = new("VM.Standard.E4.Flex") },
			"bad shape",
			"InvalidParameter",
			400,
		},
		{
			func(d *my.CreateDbSystemDetails) { d.ShapeName = new("MySQL.HeatWave.VM.Standard") },
			"heatwave shape",
			"InvalidParameter",
			400,
		},
		{func(d *my.CreateDbSystemDetails) { d.ShapeName = nil }, "no shape", "MissingParameter", 400},
		{func(d *my.CreateDbSystemDetails) { d.MysqlVersion = new("5.7.44") }, "bad version", "InvalidParameter", 400},
		{func(d *my.CreateDbSystemDetails) { d.AdminPassword = new("short1!A") }, "minimum length password", "", 0},
		{
			func(d *my.CreateDbSystemDetails) { d.AdminPassword = new("Sh0rt!") },
			"short password",
			"InvalidParameter",
			400,
		},
		{
			func(d *my.CreateDbSystemDetails) { d.AdminPassword = new("alllowercase1!") },
			"no upper",
			"InvalidParameter",
			400,
		},
		{
			func(d *my.CreateDbSystemDetails) { d.AdminPassword = new("ALLUPPERCASE1!") },
			"no lower",
			"InvalidParameter",
			400,
		},
		{
			func(d *my.CreateDbSystemDetails) { d.AdminPassword = new("NoDigitsHere!") },
			"no digit",
			"InvalidParameter",
			400,
		},
		{
			func(d *my.CreateDbSystemDetails) { d.AdminPassword = new("NoSpecial1234") },
			"no special",
			"InvalidParameter",
			400,
		},
		{
			func(d *my.CreateDbSystemDetails) { d.AdminPassword = new(strings.Repeat("aA1!", 9)) },
			"long password",
			"InvalidParameter",
			400,
		},
		{func(d *my.CreateDbSystemDetails) { d.AdminPassword = nil }, "no password", "MissingParameter", 400},
		{func(d *my.CreateDbSystemDetails) { d.AdminUsername = nil }, "no username", "MissingParameter", 400},
		{
			func(d *my.CreateDbSystemDetails) { d.AdminUsername = new("ociadmin") },
			"reserved user",
			"InvalidParameter",
			400,
		},
		{func(d *my.CreateDbSystemDetails) { d.AdminUsername = new("ROOT") }, "root user", "InvalidParameter", 400},
		{
			func(d *my.CreateDbSystemDetails) { d.AdminUsername = new(strings.Repeat("a", 33)) },
			"long username",
			"InvalidParameter",
			400,
		},
		{
			func(d *my.CreateDbSystemDetails) { d.SubnetId = new("ocid1.subnet.oc1.iad.nope") },
			"unknown subnet",
			"InvalidParameter",
			400,
		},
		{func(d *my.CreateDbSystemDetails) { d.SubnetId = new("not-an-ocid") }, "bad subnet", "InvalidParameter", 400},
		{func(d *my.CreateDbSystemDetails) { d.SubnetId = nil }, "no subnet", "MissingParameter", 400},
		{
			func(d *my.CreateDbSystemDetails) { d.DataStorageSizeInGBs = new(10) },
			"small storage",
			"InvalidParameter",
			400,
		},
		{
			func(d *my.CreateDbSystemDetails) { d.DataStorageSizeInGBs = new(200000) },
			"huge storage",
			"InvalidParameter",
			400,
		},
		{func(d *my.CreateDbSystemDetails) { d.DataStorageSizeInGBs = new(100) }, "ok storage", "", 0},
		{
			func(d *my.CreateDbSystemDetails) { d.Port = new(3306); d.PortX = new(3306) },
			"same ports",
			"InvalidParameter",
			400,
		},
		{func(d *my.CreateDbSystemDetails) { d.HostnameLabel = new("-bad-") }, "bad hostname", "InvalidParameter", 400},
		{
			func(d *my.CreateDbSystemDetails) { d.IpAddress = new("192.168.0.9") },
			"ip outside subnet",
			"InvalidParameter",
			400,
		},
		{
			func(d *my.CreateDbSystemDetails) { d.ConfigurationId = new("ocid1.mysqlconfiguration.oc1..nope") },
			"bad config",
			"InvalidParameter",
			400,
		},
		{func(d *my.CreateDbSystemDetails) {
			d.ShapeName = new("MySQL.Free")
			d.IsHighlyAvailable = new(true)
		}, "free ha", "InvalidParameter", 400},
		{func(d *my.CreateDbSystemDetails) {
			d.BackupPolicy = &my.CreateBackupPolicyDetails{RetentionInDays: new(99)}
		}, "retention", "InvalidParameter", 400},
		{func(d *my.CreateDbSystemDetails) {
			d.BackupPolicy = &my.CreateBackupPolicyDetails{WindowStartTime: new("nine o'clock")}
		}, "window", "InvalidParameter", 400},
		{func(d *my.CreateDbSystemDetails) {
			d.Source = my.CreateDbSystemSourceFromBackupDetails{BackupId: new("ocid1.mysqlbackup.oc1.iad.nope")}
		}, "unknown backup", "InvalidParameter", 400},
		{func(d *my.CreateDbSystemDetails) { d.CompartmentId = nil }, "no compartment", "MissingParameter", 400},
		{
			func(d *my.CreateDbSystemDetails) { d.DisplayName = new(strings.Repeat("n", 256)) },
			"long name",
			"InvalidParameter",
			400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			d := dbDetails("v")
			tt.mod(&d)

			_, err := e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{CreateDbSystemDetails: d})
			if tt.status == 0 {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestEndpointAddressing(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	a := e.mkDB(t, "a", nil)
	b := e.mkDB(t, "b", nil)
	assert.Equal(t, "10.0.1.5", *a.IpAddress)
	assert.Equal(t, "10.0.1.6", *b.IpAddress)

	explicit := e.mkDB(t, "c", func(d *my.CreateDbSystemDetails) { d.IpAddress = new("10.0.1.50") })
	assert.Equal(t, "10.0.1.50", *explicit.IpAddress)

	_, err := e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{
		CreateDbSystemDetails: func() my.CreateDbSystemDetails {
			d := dbDetails("dup")
			d.IpAddress = new("10.0.1.50")

			return d
		}(),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "address already taken")

	e.mkDB(t, "l1", func(d *my.CreateDbSystemDetails) { d.HostnameLabel = new("same") })

	_, err = e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{
		CreateDbSystemDetails: func() my.CreateDbSystemDetails {
			d := dbDetails("l2")
			d.HostnameLabel = new("SAME")

			return d
		}(),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "hostname label unique per subnet")

	for range 3 {
		e.mkDB(t, "tiny", func(d *my.CreateDbSystemDetails) { d.SubnetId = new(subB) })
	}

	_, err = e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{
		CreateDbSystemDetails: func() my.CreateDbSystemDetails {
			d := dbDetails("full")
			d.SubnetId = new(subB)

			return d
		}(),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "the /29 subnet runs out of addresses")

	standalone := newEnv(t, envOpts{noNet: true})
	free := standalone.mkDB(t, "free", nil)
	assert.Regexp(t, `^\d+\.\d+\.\d+\.\d+$`, *free.IpAddress, "no OCI Core wired: documentation range address")
}

func TestUpdateDbSystem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		upd        my.UpdateDbSystemDetails
		check      func(t *testing.T, got my.DbSystem)
		name       string
		wantStatus int
		transition bool
	}{
		{
			name: "rename is immediate",
			upd:  my.UpdateDbSystemDetails{DisplayName: new("renamed"), Description: new("d")},
			check: func(t *testing.T, got my.DbSystem) {
				t.Helper()
				assert.Equal(t, "renamed", *got.DisplayName)
				assert.Equal(t, "d", *got.Description)
			},
		},
		{
			name: "shape change", transition: true, upd: my.UpdateDbSystemDetails{ShapeName: new(shapeBig)},
			check: func(t *testing.T, got my.DbSystem) {
				t.Helper()
				assert.Equal(t, shapeBig, *got.ShapeName)
			},
		},
		{
			name: "version upgrade", transition: true, upd: my.UpdateDbSystemDetails{MysqlVersion: new("9.4.0")},
			check: func(t *testing.T, got my.DbSystem) {
				t.Helper()
				assert.Equal(t, "9.4.0", *got.MysqlVersion)
			},
		},
		{name: "version downgrade", upd: my.UpdateDbSystemDetails{MysqlVersion: new("8.0.39")}, wantStatus: 400},
		{
			name: "grow storage", transition: true, upd: my.UpdateDbSystemDetails{DataStorageSizeInGBs: new(80)},
			check: func(t *testing.T, got my.DbSystem) {
				t.Helper()
				assert.Equal(t, 80, *got.DataStorageSizeInGBs)
			},
		},
		{name: "shrink storage", upd: my.UpdateDbSystemDetails{DataStorageSizeInGBs: new(40)}, wantStatus: 400},
		{name: "bad shape", upd: my.UpdateDbSystemDetails{ShapeName: new("nope")}, wantStatus: 400},
		{name: "weak admin password", upd: my.UpdateDbSystemDetails{AdminPassword: new("weak")}, wantStatus: 400},
		{
			name:       "admin password reset",
			transition: true,
			upd:        my.UpdateDbSystemDetails{AdminPassword: new("N3wPassw0rd#")},
		},
		{
			name: "policies", upd: my.UpdateDbSystemDetails{
				BackupPolicy:   &my.UpdateBackupPolicyDetails{RetentionInDays: new(14), WindowStartTime: new("03:30")},
				DeletionPolicy: &my.UpdateDeletionPolicyDetails{IsDeleteProtected: new(true)},
			},
			check: func(t *testing.T, got my.DbSystem) {
				t.Helper()
				assert.Equal(t, 14, *got.BackupPolicy.RetentionInDays)
				assert.Equal(t, "03:30", *got.BackupPolicy.WindowStartTime)
				assert.True(t, *got.DeletionPolicy.IsDeleteProtected)
			},
		},
		{
			name: "ports", upd: my.UpdateDbSystemDetails{Port: new(3307), PortX: new(33061)},
			check: func(t *testing.T, got my.DbSystem) {
				t.Helper()
				assert.Equal(t, 3307, *got.Port)
			},
		},
		{name: "same ports", upd: my.UpdateDbSystemDetails{Port: new(33060)}, wantStatus: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.mkDB(t, "upd", nil)

			resp, err := e.db.UpdateDbSystem(t.Context(), my.UpdateDbSystemRequest{
				DbSystemId: db.Id, UpdateDbSystemDetails: tt.upd,
			})
			if tt.wantStatus != 0 {
				assert.Equal(t, tt.wantStatus, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp.OpcWorkRequestId)

			mid := e.getDB(t, db.Id)
			if tt.transition {
				assert.Equal(t, my.DbSystemLifecycleStateUpdating, mid.LifecycleState)
				e.settle()
			} else {
				assert.Equal(t, my.DbSystemLifecycleStateActive, mid.LifecycleState)
			}

			got := e.getDB(t, db.Id)
			assert.Equal(t, my.DbSystemLifecycleStateActive, got.LifecycleState)

			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

func TestUpdateRespectsEtagAndState(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "etag", nil)

	_, err := e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId:  db.Id,
		IfMatch:     new("stale"),
		DisplayName: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	got, err := e.db.GetDbSystem(ctx, my.GetDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id, IfMatch: got.Etag, ShapeName: new(shapeBig),
	})
	require.NoError(t, err)

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id, DisplayName: new("busy"),
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "UPDATING DB systems are not updatable")

	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: db.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}

func TestDeleteProtectionAndFinalBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy     *my.CreateDeletionPolicyDetails
		name       string
		wantBackup bool
		protected  bool
	}{
		{name: "protected", policy: &my.CreateDeletionPolicyDetails{IsDeleteProtected: new(true)}, protected: true},
		{name: "final backup", policy: &my.CreateDeletionPolicyDetails{
			FinalBackup: my.CreateDeletionPolicyDetailsFinalBackupRequireFinalBackup,
		}, wantBackup: true},
		{name: "default skips final backup", policy: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.mkDB(t, "del", func(d *my.CreateDbSystemDetails) { d.DeletionPolicy = tt.policy })

			_, err := e.db.DeleteDbSystem(t.Context(), my.DeleteDbSystemRequest{DbSystemId: db.Id})
			if tt.protected {
				se := svcErr(t, err)
				assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode())
				assert.Equal(t, my.DbSystemLifecycleStateActive, e.getDB(t, db.Id).LifecycleState)

				_, err = e.db.UpdateDbSystem(t.Context(), my.UpdateDbSystemRequest{
					DbSystemId:     db.Id,
					DeletionPolicy: &my.UpdateDeletionPolicyDetails{IsDeleteProtected: new(false)},
				})
				require.NoError(t, err)

				_, err = e.db.DeleteDbSystem(t.Context(), my.DeleteDbSystemRequest{DbSystemId: db.Id})
			}

			require.NoError(t, err)
			e.settle()
			assert.Equal(t, my.DbSystemLifecycleStateDeleted, e.getDB(t, db.Id).LifecycleState)

			bks, err := e.bk.ListBackups(t.Context(), my.ListBackupsRequest{CompartmentId: new(testCompartment)})
			require.NoError(t, err)

			if !tt.wantBackup {
				assert.Empty(t, bks.Items)

				return
			}

			require.Len(t, bks.Items, 1)
			assert.Equal(t, db.Id, bks.Items[0].DbSystemId)
		})
	}
}

func TestListDbSystemsFilters(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	names := []string{"charlie", "alpha", "bravo"}
	ids := map[string]*string{}

	for _, n := range names {
		ids[n] = e.mkDB(t, n, nil).Id
	}

	e.mkDB(t, "elsewhere", func(d *my.CreateDbSystemDetails) { d.CompartmentId = new(otherCompart) })

	del := e.mkDB(t, "gone", nil)
	_, err := e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: del.Id})
	require.NoError(t, err)
	e.settle()

	tests := []struct {
		name string
		req  my.ListDbSystemsRequest
		want []string
	}{
		{
			"by name asc",
			my.ListDbSystemsRequest{SortBy: my.ListDbSystemsSortByDisplayname, SortOrder: my.ListDbSystemsSortOrderAsc},
			[]string{"alpha", "bravo", "charlie"},
		},
		{
			"by name desc",
			my.ListDbSystemsRequest{
				SortBy:    my.ListDbSystemsSortByDisplayname,
				SortOrder: my.ListDbSystemsSortOrderDesc,
			},
			[]string{"charlie", "bravo", "alpha"},
		},
		{
			"by created asc",
			my.ListDbSystemsRequest{SortBy: my.ListDbSystemsSortByTimecreated, SortOrder: my.ListDbSystemsSortOrderAsc},
			[]string{"charlie", "alpha", "bravo"},
		},
		{"display name filter", my.ListDbSystemsRequest{DisplayName: new("bravo")}, []string{"bravo"}},
		{"deleted state", my.ListDbSystemsRequest{LifecycleState: my.DbSystemLifecycleStateDeleted}, []string{"gone"}},
		{"id filter", my.ListDbSystemsRequest{DbSystemId: ids["alpha"]}, []string{"alpha"}},
		{"heatwave attached", my.ListDbSystemsRequest{IsHeatWaveClusterAttached: new(true)}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.req.CompartmentId = new(testCompartment)

			got, lerr := e.db.ListDbSystems(t.Context(), tt.req)
			require.NoError(t, lerr)

			var have []string
			for _, it := range got.Items {
				have = append(have, *it.DisplayName)
			}

			assert.Equal(t, tt.want, have)
		})
	}

	page1, err := e.db.ListDbSystems(ctx, my.ListDbSystemsRequest{
		CompartmentId: new(testCompartment), Limit: new(2),
		SortBy: my.ListDbSystemsSortByDisplayname, SortOrder: my.ListDbSystemsSortOrderAsc,
	})
	require.NoError(t, err)
	require.Len(t, page1.Items, 2)
	require.NotNil(t, page1.OpcNextPage)

	page2, err := e.db.ListDbSystems(ctx, my.ListDbSystemsRequest{
		CompartmentId: new(testCompartment), Limit: new(2), Page: page1.OpcNextPage,
		SortBy: my.ListDbSystemsSortByDisplayname, SortOrder: my.ListDbSystemsSortOrderAsc,
	})
	require.NoError(t, err)
	require.Len(t, page2.Items, 1)
	assert.Equal(t, "charlie", *page2.Items[0].DisplayName)
	assert.Nil(t, page2.OpcNextPage)

	status, _ := rawDo(t, http.MethodGet, e.base+"/20190415/dbSystems", "")
	assert.Equal(t, http.StatusBadRequest, status, "compartmentId is required")

	status, _ = rawDo(
		t,
		http.MethodGet,
		e.base+"/20190415/dbSystems?compartmentId="+testCompartment+"&sortBy=bogus",
		"",
	)
	assert.Equal(t, http.StatusBadRequest, status)
}

func TestShapesAndVersions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	all, err := e.aas.ListShapes(ctx, my.ListShapesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)

	byName := map[string]my.ShapeSummary{}
	for _, s := range all.Items {
		byName[*s.Name] = s
	}

	assert.Equal(t, 16, *byName["MySQL.2"].MemorySizeInGBs)
	assert.Equal(t, 2, *byName["MySQL.2"].CpuCoreCount)
	assert.Contains(t, byName, "MySQL.Free")
	assert.Equal(t, []my.ShapeSummaryIsSupportedForEnum{my.ShapeSummaryIsSupportedForHeatwavecluster},
		byName["MySQL.HeatWave.VM.Standard"].IsSupportedFor)

	dbOnly, err := e.aas.ListShapes(ctx, my.ListShapesRequest{
		CompartmentId: new(
			testCompartment,
		),
		IsSupportedFor: []my.ListShapesIsSupportedForEnum{my.ListShapesIsSupportedForDbsystem},
	})
	require.NoError(t, err)

	for _, s := range dbOnly.Items {
		assert.NotContains(t, *s.Name, "HeatWave")
	}

	one, err := e.aas.ListShapes(ctx, my.ListShapesRequest{CompartmentId: new(testCompartment), Name: new("MySQL.8")})
	require.NoError(t, err)
	require.Len(t, one.Items, 1)

	vers, err := e.aas.ListVersions(ctx, my.ListVersionsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, vers.Items, 3)

	fams := map[string][]my.Version{}
	for _, f := range vers.Items {
		fams[*f.VersionFamily] = f.Versions
	}

	assert.Contains(t, fams, "8.0")
	assert.Contains(t, fams, "8.4")
	assert.Contains(t, fams, "9.x")
	assert.Equal(t, "8.4.3", *fams["8.4"][0].Version)

	status, _ := rawDo(t, http.MethodGet, e.base+"/20190415/shapes", "")
	assert.Equal(t, http.StatusBadRequest, status, "compartmentId is required")
}

func TestErrorEnvelope(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		code   string
		status int
	}{
		{
			"missing db system",
			http.MethodGet,
			"/20190415/dbSystems/ocid1.mysqldbsystem.oc1.iad.nope",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{
			"missing backup",
			http.MethodGet,
			"/20190415/backups/ocid1.mysqlbackup.oc1.iad.nope",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{
			"missing channel",
			http.MethodGet,
			"/20190415/channels/ocid1.mysqlchannel.oc1.iad.nope",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{
			"missing configuration",
			http.MethodGet,
			"/20190415/configurations/ocid1.mysqlconfiguration.oc1.iad.nope",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{
			"missing work request",
			http.MethodGet,
			"/20190415/workRequests/ocid1.mysqlworkrequest.oc1.iad.nope",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{"bad json", http.MethodPost, "/20190415/dbSystems", "{", "CannotParseRequest", 400},
		{"no body", http.MethodPost, "/20190415/dbSystems", "", "MissingParameter", 400},
		{"unknown route", http.MethodGet, "/20190415/nothing", "", "NotFound", 404},
		{"wrong method", http.MethodPatch, "/20190415/dbSystems", "", "NotFound", 404},
		{"stop without type", http.MethodPost, "/20190415/dbSystems/x/actions/stop", `{}`, "MissingParameter", 400},
		{
			"stop bad type",
			http.MethodPost,
			"/20190415/dbSystems/x/actions/stop",
			`{"shutdownType":"NOW"}`,
			"InvalidParameter",
			400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := rawDo(t, tt.method, e.base+tt.path, tt.body)
			assert.Equal(t, tt.status, status)
			assert.Contains(t, body, `"code":"`+tt.code+`"`)
			assert.Contains(t, body, `"message"`)
		})
	}
}

func TestRequestIDsAndAuth(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})

	resp, err := testclient.New(t).Get(e.base + "/20190415/shapes?compartmentId=" + testCompartment)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("Opc-Request-Id"))

	signed, err := e.aas.ListVersions(t.Context(), my.ListVersionsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.NotEmpty(t, signed.OpcRequestId)

	other := newKeyPair(t)
	stranger := common.NewRawConfigurationProvider(
		testTenancy,
		testUser,
		"us-ashburn-1",
		other.fprint,
		other.privPEM,
		nil,
	)
	cl, err := my.NewMysqlaasClientWithConfigurationProvider(stranger)
	require.NoError(t, err)

	cl.Host = e.base

	_, err = cl.ListVersions(t.Context(), my.ListVersionsRequest{CompartmentId: new(testCompartment)})
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode(), "unregistered key is rejected")
}

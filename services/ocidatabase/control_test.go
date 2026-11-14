package ocidatabase_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	svc "github.com/blackbirdworks/gopherstack/services/ocidatabase"
)

func TestCreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(*database.CreateAutonomousDatabaseDetails)
		name     string
		wantCode string
		wantHTTP int
	}{
		{name: "ok", mutate: func(*database.CreateAutonomousDatabaseDetails) {}, wantHTTP: http.StatusAccepted},
		{
			name: "no-dbname", wantHTTP: http.StatusBadRequest, wantCode: "MissingParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DbName = nil },
		},
		{
			name: "dbname-leading-digit", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DbName = new("1db") },
		},
		{
			name: "dbname-special-char", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DbName = new("my_db") },
		},
		{
			name: "dbname-too-long", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DbName = new("a" + strings.Repeat("b", 30)) },
		},
		{
			name: "dbname-max-length", wantHTTP: http.StatusAccepted,
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DbName = new("a" + strings.Repeat("b", 29)) },
		},
		{
			name: "no-password", wantHTTP: http.StatusBadRequest, wantCode: "MissingParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.AdminPassword = nil },
		},
		{
			name: "password-short", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.AdminPassword = new("Abcdefg1234") },
		},
		{
			name: "password-long", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.AdminPassword = new("Abcdefg1234" + strings.Repeat("x", 20))
			},
		},
		{
			name: "password-no-upper", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.AdminPassword = new("abcdefghij1234") },
		},
		{
			name: "password-no-lower", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.AdminPassword = new("ABCDEFGHIJ1234") },
		},
		{
			name: "password-no-digit", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.AdminPassword = new("Abcdefghijklmn") },
		},
		{
			name: "password-quote", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.AdminPassword = new(`Abcdefghij"1234`) },
		},
		{
			name: "password-admin", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.AdminPassword = new("MyAdMiNpass1234") },
		},
		{
			name: "secret-id", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.SecretId = new("ocid1.vaultsecret.oc1..x") },
		},
		{
			name: "no-compartment", wantHTTP: http.StatusBadRequest, wantCode: "MissingParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.CompartmentId = nil },
		},
		{
			name: "bad-workload", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.DbWorkload = database.CreateAutonomousDatabaseBaseDbWorkloadEnum("XYZ")
			},
		},
		{
			name: "ecpu-below-min", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.ComputeModel = database.CreateAutonomousDatabaseBaseComputeModelEcpu
				d.ComputeCount = new(float32(1))
			},
		},
		{
			name: "ecpu-ok", wantHTTP: http.StatusAccepted,
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.ComputeModel = database.CreateAutonomousDatabaseBaseComputeModelEcpu
				d.ComputeCount = new(float32(4))
			},
		},
		{
			name: "ecpu-above-max", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.ComputeModel = database.CreateAutonomousDatabaseBaseComputeModelEcpu
				d.ComputeCount = new(float32(100000))
			},
		},
		{
			name: "ocpu-fractional", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.ComputeModel = database.CreateAutonomousDatabaseBaseComputeModelOcpu
				d.ComputeCount = new(float32(1.5))
			},
		},
		{
			name: "ocpu-ok", wantHTTP: http.StatusAccepted,
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.ComputeModel = database.CreateAutonomousDatabaseBaseComputeModelOcpu
				d.ComputeCount = new(float32(2))
			},
		},
		{
			name: "model-without-count", wantHTTP: http.StatusBadRequest, wantCode: "MissingParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.ComputeModel = database.CreateAutonomousDatabaseBaseComputeModelEcpu
			},
		},
		{
			name: "cores-with-count", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.CpuCoreCount = new(2)
				d.ComputeCount = new(float32(2))
			},
		},
		{
			name: "legacy-cores", wantHTTP: http.StatusAccepted,
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.CpuCoreCount = new(2) },
		},
		{
			name: "tbs-and-gbs", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.DataStorageSizeInTBs = new(1)
				d.DataStorageSizeInGBs = new(100)
			},
		},
		{
			name: "storage-too-big", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DataStorageSizeInTBs = new(385) },
		},
		{
			name: "storage-zero", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DataStorageSizeInTBs = new(0) },
		},
		{
			name: "bad-acl", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.WhitelistedIps = []string{"not-an-ip"} },
		},
		{
			name: "acl-ok", wantHTTP: http.StatusAccepted,
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.WhitelistedIps = []string{"10.0.0.0/8", "1.2.3.4", "ocid1.vcn.oc1..v;10.1.1.1"}
			},
		},
		{
			name: "bad-version", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DbVersion = new("12c") },
		},
		{
			name: "free-tier-ok", wantHTTP: http.StatusAccepted,
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.IsFreeTier = new(true) },
		},
		{
			name: "free-tier-scaled", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.IsFreeTier = new(true)
				d.ComputeModel = database.CreateAutonomousDatabaseBaseComputeModelEcpu
				d.ComputeCount = new(float32(8))
			},
		},
		{
			name: "free-tier-autoscaling", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.IsFreeTier = new(true)
				d.IsAutoScalingEnabled = new(true)
			},
		},
		{
			name: "free-tier-storage", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.IsFreeTier = new(true)
				d.DataStorageSizeInTBs = new(5)
			},
		},
		{
			name: "dedicated", wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.IsDedicated = new(true) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			_, err := e.create(t, createOpts{mutate: tc.mutate})

			if tc.wantHTTP == http.StatusAccepted {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tc.wantHTTP, se.GetHTTPStatusCode())
			assert.Equal(t, tc.wantCode, se.GetCode())
		})
	}
}

func TestCreateShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*database.CreateAutonomousDatabaseDetails)
		check  func(*testing.T, database.AutonomousDatabase)
		name   string
	}{
		{
			name: "defaults", mutate: func(*database.CreateAutonomousDatabaseDetails) {},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, database.AutonomousDatabaseLifecycleStateProvisioning, db.LifecycleState)
				assert.Equal(t, database.AutonomousDatabaseDbWorkloadOltp, db.DbWorkload)
				assert.Equal(t, database.AutonomousDatabaseComputeModelEcpu, db.ComputeModel)
				assert.InDelta(t, 2, *db.ComputeCount, 0)
				assert.Equal(t, 1, *db.DataStorageSizeInTBs)
				assert.Equal(t, "mydb", *db.DbName)
				assert.Equal(t, "My Database", *db.DisplayName)
				assert.False(t, *db.IsFreeTier)
				assert.False(t, *db.IsMtlsConnectionRequired)
				assert.Equal(t, "26ai", *db.DbVersion)
				assert.Nil(t, db.OcpuCount)
			},
		},
		{
			name:   "display-name-defaults-to-dbname",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DisplayName = nil },
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, "mydb", *db.DisplayName)
			},
		},
		{
			name:   "ocpu-legacy",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.CpuCoreCount = new(4) },
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, database.AutonomousDatabaseComputeModelOcpu, db.ComputeModel)
				assert.InDelta(t, 4, *db.ComputeCount, 0)
				assert.InDelta(t, 4, *db.OcpuCount, 0)
				assert.Equal(t, 4, *db.CpuCoreCount)
			},
		},
		{
			name:   "storage-gbs",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DataStorageSizeInGBs = new(2048) },
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, 2048, *db.DataStorageSizeInGBs)
				assert.Equal(t, 2, *db.DataStorageSizeInTBs)
			},
		},
		{
			name:   "free-tier",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.IsFreeTier = new(true) },
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.True(t, *db.IsFreeTier)
				assert.InDelta(t, 2, *db.ComputeCount, 0)
				assert.Equal(t, 20, *db.DataStorageSizeInGBs)
			},
		},
		{
			name: "workloads-apex",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.DbWorkload = database.CreateAutonomousDatabaseBaseDbWorkloadApex
			},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, database.AutonomousDatabaseDbWorkloadApex, db.DbWorkload)
				assert.NotNil(t, db.ApexDetails)
				assert.Contains(t, *db.ConnectionUrls.ApexUrl, "/ords/apex")
			},
		},
		{
			name: "workloads-ajd",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.DbWorkload = database.CreateAutonomousDatabaseBaseDbWorkloadAjd
			},
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.NotNil(t, db.ConnectionUrls.MongoDbUrl)
			},
		},
		{
			name:   "tags",
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.FreeformTags = map[string]string{"env": "dev"} },
			check: func(t *testing.T, db database.AutonomousDatabase) {
				t.Helper()
				assert.Equal(t, map[string]string{"env": "dev"}, db.FreeformTags)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			resp := e.mustCreate(t, createOpts{mutate: tc.mutate})
			assert.NotEmpty(t, *resp.OpcWorkRequestId)
			assert.NotEmpty(t, *resp.Etag)
			tc.check(t, e.get(t, resp.Id))
		})
	}
}

func TestConnectionStrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate       func(*database.CreateAutonomousDatabaseDetails)
		name         string
		wantGroups   []string
		wantProfiles int
		wantServer   bool
	}{
		{name: "oltp-mtls", wantProfiles: 5, wantGroups: []string{"HIGH", "MEDIUM", "LOW", "TP", "TPURGENT"},
			mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.IsMtlsConnectionRequired = new(true) }},
		{name: "oltp-tls-optional", wantProfiles: 10, wantServer: true,
			wantGroups: []string{"HIGH", "MEDIUM", "LOW", "TP", "TPURGENT"},
			mutate:     func(*database.CreateAutonomousDatabaseDetails) {}},
		{name: "dw", wantProfiles: 6, wantServer: true, wantGroups: []string{"HIGH", "MEDIUM", "LOW"},
			mutate: func(d *database.CreateAutonomousDatabaseDetails) {
				d.DbWorkload = database.CreateAutonomousDatabaseBaseDbWorkloadDw
			}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.available(t, createOpts{mutate: tc.mutate})
			cs := db.ConnectionStrings
			require.NotNil(t, cs)
			assert.Len(t, cs.Profiles, tc.wantProfiles)
			assert.Len(t, cs.AllConnectionStrings, len(tc.wantGroups))

			for _, g := range tc.wantGroups {
				assert.Contains(t, cs.AllConnectionStrings, g)
			}

			assert.Contains(t, *cs.High, "(protocol=tcps)(port=1522)")
			assert.Contains(t, *cs.High, "_mydb_high.adb.oraclecloud.com")

			var sawServer bool

			for _, p := range cs.Profiles {
				assert.Equal(t, database.DatabaseConnectionStringProfileProtocolTcps, p.Protocol)
				assert.Equal(t, database.DatabaseConnectionStringProfileSessionModeDirect, p.SessionMode)

				if p.TlsAuthentication == database.DatabaseConnectionStringProfileTlsAuthenticationServer {
					sawServer = true

					assert.Contains(t, *p.Value, "(port=1521)")
				}
			}

			assert.Equal(t, tc.wantServer, sawServer)
		})
	}
}

func TestLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	resp := e.mustCreate(t, createOpts{})
	id := resp.Id

	got := e.get(t, id)
	assert.Equal(t, database.AutonomousDatabaseLifecycleStateProvisioning, got.LifecycleState)
	assert.Equal(t, "IN_PROGRESS", string(e.wr(t, resp.OpcWorkRequestId).Status))

	e.clock.Advance(time.Second)
	e.backend.Sweep()
	assert.Equal(t, database.AutonomousDatabaseLifecycleStateProvisioning, e.get(t, id).LifecycleState)

	e.settle()
	assert.Equal(t, database.AutonomousDatabaseLifecycleStateAvailable, e.get(t, id).LifecycleState)
	assert.Equal(t, "SUCCEEDED", string(e.wr(t, resp.OpcWorkRequestId).Status))

	steps := []struct {
		act     func() (*string, error)
		name    string
		during  database.AutonomousDatabaseLifecycleStateEnum
		settled database.AutonomousDatabaseLifecycleStateEnum
	}{
		{
			name: "stop", during: database.AutonomousDatabaseLifecycleStateStopping,
			settled: database.AutonomousDatabaseLifecycleStateStopped,
			act: func() (*string, error) {
				r, err := e.client.StopAutonomousDatabase(
					t.Context(),
					database.StopAutonomousDatabaseRequest{AutonomousDatabaseId: id},
				)

				return r.OpcWorkRequestId, err
			},
		},
		{
			name: "start", during: database.AutonomousDatabaseLifecycleStateStarting,
			settled: database.AutonomousDatabaseLifecycleStateAvailable,
			act: func() (*string, error) {
				r, err := e.client.StartAutonomousDatabase(
					t.Context(),
					database.StartAutonomousDatabaseRequest{AutonomousDatabaseId: id},
				)

				return r.OpcWorkRequestId, err
			},
		},
		{
			name: "restart", during: database.AutonomousDatabaseLifecycleStateRestarting,
			settled: database.AutonomousDatabaseLifecycleStateAvailable,
			act: func() (*string, error) {
				r, err := e.client.RestartAutonomousDatabase(
					t.Context(),
					database.RestartAutonomousDatabaseRequest{AutonomousDatabaseId: id},
				)

				return r.OpcWorkRequestId, err
			},
		},
		{
			name: "terminate", during: database.AutonomousDatabaseLifecycleStateTerminating,
			settled: database.AutonomousDatabaseLifecycleStateTerminated,
			act: func() (*string, error) {
				r, err := e.client.DeleteAutonomousDatabase(
					t.Context(),
					database.DeleteAutonomousDatabaseRequest{AutonomousDatabaseId: id},
				)

				return r.OpcWorkRequestId, err
			},
		},
	}

	for _, s := range steps {
		wrID, err := s.act()
		require.NoError(t, err, s.name)
		assert.Equal(t, s.during, e.get(t, id).LifecycleState, s.name)
		assert.Equal(t, "IN_PROGRESS", string(e.wr(t, wrID).Status), s.name)

		e.settle()
		assert.Equal(t, s.settled, e.get(t, id).LifecycleState, s.name)
		assert.Equal(t, "SUCCEEDED", string(e.wr(t, wrID).Status), s.name)
	}

	_, err := e.client.StartAutonomousDatabase(
		t.Context(),
		database.StartAutonomousDatabaseRequest{AutonomousDatabaseId: id},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}

func TestWrongState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act  func(e *env, id *string) error
		name string
	}{
		{name: "start-available", act: func(e *env, id *string) error {
			_, err := e.client.StartAutonomousDatabase(
				e.ctx(),
				database.StartAutonomousDatabaseRequest{AutonomousDatabaseId: id},
			)

			return err
		}},
		{name: "wallet-while-provisioning", act: func(e *env, id *string) error {
			_, err := e.client.GenerateAutonomousDatabaseWallet(
				e.ctx(),
				database.GenerateAutonomousDatabaseWalletRequest{
					AutonomousDatabaseId: id,
					Password:             new("Wallet#1234"),
				},
			)

			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})

			var id *string

			if tc.name == "start-available" {
				id = e.available(t, createOpts{}).Id
			} else {
				id = e.mustCreate(t, createOpts{}).Id
			}

			se := svcErr(t, tc.act(e, id))
			assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode())
			assert.Equal(t, "IncorrectState", se.GetCode())
		})
	}
}

func TestNotFoundAndEtag(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.available(t, createOpts{})

	_, err := e.client.GetAutonomousDatabase(
		t.Context(),
		database.GetAutonomousDatabaseRequest{AutonomousDatabaseId: new("ocid1.autonomousdatabase.oc1..nope")},
	)
	se := svcErr(t, err)
	assert.Equal(t, http.StatusNotFound, se.GetHTTPStatusCode())
	assert.Equal(t, "NotAuthorizedOrNotFound", se.GetCode())

	_, err = e.client.StopAutonomousDatabase(t.Context(), database.StopAutonomousDatabaseRequest{
		AutonomousDatabaseId: db.Id, IfMatch: new("stale"),
	})
	se = svcErr(t, err)
	assert.Equal(t, http.StatusPreconditionFailed, se.GetHTTPStatusCode())
	assert.Equal(t, "NoEtagMatch", se.GetCode())

	got, err := e.client.GetAutonomousDatabase(
		t.Context(),
		database.GetAutonomousDatabaseRequest{AutonomousDatabaseId: db.Id},
	)
	require.NoError(t, err)

	_, err = e.client.StopAutonomousDatabase(t.Context(), database.StopAutonomousDatabaseRequest{
		AutonomousDatabaseId: db.Id, IfMatch: got.Etag,
	})
	require.NoError(t, err)
}

func TestRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	first := e.mustCreate(t, createOpts{token: "tok-1"})
	second := e.mustCreate(t, createOpts{token: "tok-1"})

	assert.Equal(t, *first.Id, *second.Id)
	assert.Equal(t, *first.OpcWorkRequestId, *second.OpcWorkRequestId)

	_, err := e.create(t, createOpts{})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "same dbName without the token")
	assert.Equal(t, "NotAuthorizedOrResourceAlreadyExists", svcErr(t, err).GetCode())
}

func TestFreeTierLimitAndNameReuse(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	free := func(name string) createOpts {
		return createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) {
			d.IsFreeTier, d.DbName = new(true), new(name)
		}}
	}

	first := e.mustCreate(t, free("free1"))
	e.mustCreate(t, free("free2"))

	_, err := e.create(t, free("free3"))
	se := svcErr(t, err)
	assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
	assert.Equal(t, "LimitExceeded", se.GetCode())

	e.settle()

	_, err = e.client.DeleteAutonomousDatabase(
		t.Context(),
		database.DeleteAutonomousDatabaseRequest{AutonomousDatabaseId: first.Id},
	)
	require.NoError(t, err)
	e.settle()

	e.mustCreate(t, free("free3"))
	e.mustCreate(t, createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DbName = new("FREE1") }})
}

func TestList(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	names := []string{"alpha", "bravo", "charlie"}

	for i, n := range names {
		e.mustCreate(t, createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) {
			d.DbName, d.DisplayName = new(n), new("db-"+n)
			d.IsFreeTier = new(i == 0)

			if i == 2 {
				d.DbWorkload = database.CreateAutonomousDatabaseBaseDbWorkloadDw
			}
		}})
		e.clock.Advance(time.Second)
	}

	e.settle()

	tests := []struct {
		req  database.ListAutonomousDatabasesRequest
		name string
		want []string
	}{
		{
			name: "newest-first",
			req:  database.ListAutonomousDatabasesRequest{},
			want: []string{"db-charlie", "db-bravo", "db-alpha"},
		},
		{
			name: "by-name", want: []string{"db-alpha", "db-bravo", "db-charlie"},
			req: database.ListAutonomousDatabasesRequest{SortBy: database.ListAutonomousDatabasesSortByDisplayname},
		},
		{
			name: "free-only", want: []string{"db-alpha"},
			req: database.ListAutonomousDatabasesRequest{IsFreeTier: new(true)},
		},
		{
			name: "workload", want: []string{"db-charlie"},
			req: database.ListAutonomousDatabasesRequest{DbWorkload: database.AutonomousDatabaseSummaryDbWorkloadDw},
		},
		{
			name: "display-name", want: []string{"db-bravo"},
			req: database.ListAutonomousDatabasesRequest{DisplayName: new("db-bravo")},
		},
		{
			name: "state-not", want: []string{},
			req: database.ListAutonomousDatabasesRequest{
				LifecycleStateNotEqualTo: database.AutonomousDatabaseSummaryLifecycleStateAvailable,
			},
		},
		{
			name: "other-compartment", want: []string{},
			req: database.ListAutonomousDatabasesRequest{CompartmentId: new(otherCompart)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := tc.req
			if req.CompartmentId == nil {
				req.CompartmentId = new(testCompartment)
			}

			resp, err := e.client.ListAutonomousDatabases(t.Context(), req)
			require.NoError(t, err)

			got := []string{}
			for _, it := range resp.Items {
				got = append(got, *it.DisplayName)
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestListPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	for _, n := range []string{"p1", "p2", "p3"} {
		e.mustCreate(t, createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) { d.DbName = new(n) }})
		e.clock.Advance(time.Second)
	}

	var seen int

	var pageTok *string

	for range 5 {
		resp, err := e.client.ListAutonomousDatabases(t.Context(), database.ListAutonomousDatabasesRequest{
			CompartmentId: new(testCompartment), Limit: new(2), Page: pageTok,
		})
		require.NoError(t, err)

		seen += len(resp.Items)

		if resp.OpcNextPage == nil {
			break
		}

		pageTok = resp.OpcNextPage
	}

	assert.Equal(t, 3, seen)
}

func TestCompartmentChecks(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
		b.SetCompartmentChecker(func(id string) bool { return id == testCompartment || id == otherCompart })
	}})

	_, err := e.create(t, createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) {
		d.CompartmentId = new("ocid1.compartment.oc1..ghost")
	}})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	db := e.available(t, createOpts{})
	bk := e.manualBackup(t, db.Id)

	resp, err := e.client.ChangeAutonomousDatabaseCompartment(
		t.Context(),
		database.ChangeAutonomousDatabaseCompartmentRequest{
			AutonomousDatabaseId: db.Id,
			CompartmentId:        new("ocid1.compartment.oc1..ghost"),
		},
	)
	require.Error(t, err)
	_ = resp

	resp, err = e.client.ChangeAutonomousDatabaseCompartment(
		t.Context(),
		database.ChangeAutonomousDatabaseCompartmentRequest{
			AutonomousDatabaseId: db.Id,
			CompartmentId:        new(otherCompart),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, *resp.OpcWorkRequestId)
	assert.Equal(t, "SUCCEEDED", string(e.wr(t, resp.OpcWorkRequestId).Status))

	assert.Equal(t, otherCompart, *e.get(t, db.Id).CompartmentId)

	b, err := e.client.GetAutonomousDatabaseBackup(
		t.Context(),
		database.GetAutonomousDatabaseBackupRequest{AutonomousDatabaseBackupId: bk.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *b.CompartmentId, "backups follow the database")
}

func TestPrivateEndpoint(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
		b.SetSubnetResolver(func(id string) (svc.SubnetInfo, bool) {
			return svc.SubnetInfo{CIDR: "10.0.1.0/24"}, id == "ocid1.subnet.oc1..s1"
		})
	}})

	withSubnet := func(name, subnet, ip string) createOpts {
		return createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) {
			d.DbName, d.SubnetId, d.PrivateEndpointLabel = new(name), new(subnet), new("lbl"+name)

			if ip != "" {
				d.PrivateEndpointIp = new(ip)
			}
		}}
	}

	_, err := e.create(t, withSubnet("pe0", "ocid1.subnet.oc1..nope", ""))
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	first := e.mustCreate(t, withSubnet("pe1", "ocid1.subnet.oc1..s1", ""))
	got := e.get(t, first.Id)
	assert.Equal(t, "10.0.1.3", *got.PrivateEndpointIp)
	assert.Equal(t, "lblpe1.adb.us-ashburn-1.oraclecloud.com", *got.PrivateEndpoint)
	assert.Contains(t, *got.ConnectionStrings.High, "(host=lblpe1.adb.us-ashburn-1.oraclecloud.com)")

	second := e.mustCreate(t, withSubnet("pe2", "ocid1.subnet.oc1..s1", ""))
	assert.Equal(t, "10.0.1.4", *e.get(t, second.Id).PrivateEndpointIp)

	_, err = e.create(t, withSubnet("pe3", "ocid1.subnet.oc1..s1", "10.0.1.3"))
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.create(t, withSubnet("pe4", "ocid1.subnet.oc1..s1", "192.168.0.9"))
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.create(t, createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) {
		d.DbName, d.IsFreeTier, d.SubnetId = new("pe5"), new(true), new("ocid1.subnet.oc1..s1")
	}})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestUnknownRouteAndAuth(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})

	status, body := rawGet(t, e.base+"/20160918/autonomousDatabases?compartmentId="+testCompartment)
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Contains(t, body, "NotAuthenticated")

	_, err := e.create(t, createOpts{})
	require.NoError(t, err, "signed requests pass")

	status, body = rawGet(t, e.base+"/20160918/nothing")
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, body, "NotFound")
}

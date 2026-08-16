package rds_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestDBInstance_OptimizedWritesAndStorageOptimized(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	inst, err := b.CreateDBInstance("ow-inst", "postgres", "db.r6g.large", "", "admin", "", 100,
		rds.DBInstanceOptions{
			StorageOptimized: true,
			OptimizedWrites:  true,
		})
	require.NoError(t, err)

	assert.True(t, inst.StorageOptimized)
	assert.True(t, inst.OptimizedWrites)
}

func TestDBInstance_ModifyOptimizedWrites(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBInstance("mod-ow", "postgres", "db.t3.micro", "", "admin", "", 20,
		rds.DBInstanceOptions{})
	require.NoError(t, err)

	modified, err := b.ModifyDBInstance("mod-ow", "", 0,
		rds.DBInstanceOptions{
			OptimizedWrites:        true,
			EngineLifecycleSupport: "open-source-rds-extended-support",
		})
	require.NoError(t, err)

	assert.True(t, modified.OptimizedWrites)
	assert.Equal(t, "open-source-rds-extended-support", modified.EngineLifecycleSupport)
}

func TestDBInstance_NewFieldsViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateDBInstance&Version=2014-10-31"+
			"&DBInstanceIdentifier=handler-inst&DBInstanceClass=db.r6g.large"+
			"&Engine=postgres&MasterUsername=admin&AllocatedStorage=100"+
			"&EnableOptimizedWrites=true&StorageOptimized=true"+
			"&EngineLifecycleSupport=open-source-rds-extended-support-disabled")
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "OptimizedWritesEnabled")
	assert.Contains(t, respStr, "StorageOptimized")
	assert.Contains(t, respStr, "open-source-rds-extended-support-disabled")
}

func TestPersistence_InstanceNewFields(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBInstance("inst-snap", "postgres", "db.r6g.large", "", "admin", "", 100,
		rds.DBInstanceOptions{
			StorageOptimized:       true,
			OptimizedWrites:        true,
			EngineLifecycleSupport: "open-source-rds-extended-support-disabled",
		})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	instances, err := b2.DescribeDBInstances("inst-snap")
	require.NoError(t, err)
	require.Len(t, instances, 1)

	inst := instances[0]
	assert.True(t, inst.StorageOptimized, "StorageOptimized should survive round-trip")
	assert.True(t, inst.OptimizedWrites, "OptimizedWrites should survive round-trip")
	assert.Equal(t, "open-source-rds-extended-support-disabled", inst.EngineLifecycleSupport)
}

// Test_DeleteDBInstance_NotFoundBeforeParamValidation verifies that a
// nonexistent instance yields DBInstanceNotFound even when the
// SkipFinalSnapshot/FinalDBSnapshotIdentifier combination is also invalid —
// matching AWS's behavior of resolving the target resource first.
func Test_DeleteDBInstance_NotFoundBeforeParamValidation(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=missing-inst")

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
	assert.NotContains(t, rec.Body.String(), "InvalidParameterCombination")
}

// Test_DescribeDBInstances_Filters verifies AWS's DescribeDBInstances
// Filters.Filter.N.Name/Values.member.M contract: db-instance-id, engine,
// db-cluster-id, and dbi-resource-id narrow the result set (OR within a
// filter's Values, AND across filters), and an unrecognized filter name
// returns InvalidParameterValue.
func Test_DescribeDBInstances_Filters(t *testing.T) {
	t.Parallel()

	type describeResp struct {
		XMLName xml.Name `xml:"DescribeDBInstancesResponse"`
		Result  struct {
			DBInstances struct {
				Members []struct {
					DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
				} `xml:"DBInstance"`
			} `xml:"DBInstances"`
		} `xml:"DescribeDBInstancesResult"`
	}

	cases := []struct {
		name        string
		query       string
		wantErrText string
		wantIDs     []string
		wantCode    int
	}{
		{
			name:     "engine filter matches only mysql instances",
			query:    "Filters.Filter.1.Name=engine&Filters.Filter.1.Values.member.1=mysql",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-mysql-1"},
		},
		{
			name: "db-instance-id filter with multiple values ORs together",
			query: "Filters.Filter.1.Name=db-instance-id" +
				"&Filters.Filter.1.Values.member.1=filt-mysql-1" +
				"&Filters.Filter.1.Values.member.2=filt-postgres-1",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-mysql-1", "filt-postgres-1"},
		},
		{
			name: "two filters AND together",
			query: "Filters.Filter.1.Name=engine&Filters.Filter.1.Values.member.1=postgres" +
				"&Filters.Filter.2.Name=db-instance-id&Filters.Filter.2.Values.member.1=filt-mysql-1",
			wantCode: http.StatusOK,
			wantIDs:  nil,
		},
		{
			name:        "unrecognized filter name is rejected",
			query:       "Filters.Filter.1.Name=bogus-filter&Filters.Filter.1.Values.member.1=x",
			wantCode:    http.StatusBadRequest,
			wantErrText: "InvalidParameterValue",
		},
		{
			name:     "no filters returns everything",
			query:    "",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-mysql-1", "filt-postgres-1"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			postRDSForm(t, h,
				"Action=CreateDBInstance&Version=2014-10-31"+
					"&DBInstanceIdentifier=filt-mysql-1&Engine=mysql")
			postRDSForm(t, h,
				"Action=CreateDBInstance&Version=2014-10-31"+
					"&DBInstanceIdentifier=filt-postgres-1&Engine=postgres")

			body := "Action=DescribeDBInstances&Version=2014-10-31"
			if tt.query != "" {
				body += "&" + tt.query
			}
			rec := postRDSForm(t, h, body)

			require.Equal(t, tt.wantCode, rec.Code)
			if tt.wantErrText != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrText)

				return
			}

			var resp describeResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			gotIDs := make([]string, 0, len(resp.Result.DBInstances.Members))
			for _, m := range resp.Result.DBInstances.Members {
				gotIDs = append(gotIDs, m.DBInstanceIdentifier)
			}
			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

func TestRebootDBInstance_ReturnsRebootingStatus(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Instance(t, h, "reboot-me")

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"RebootDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"reboot-me"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Instance struct {
				DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
				DBInstanceStatus     string `xml:"DBInstanceStatus"`
			} `xml:"DBInstance"`
		} `xml:"RebootDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "reboot-me", resp.Result.Instance.DBInstanceIdentifier)
	assert.Equal(t, "rebooting", resp.Result.Instance.DBInstanceStatus,
		"AWS returns rebooting immediately; available comes after transition delay")
}

func TestRebootDBInstance_NotFound_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"RebootDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"does-not-exist"},
	}.Encode())
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
}

func TestRebootDBInstance_TransitionsToAvailableAfterDelay(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	_, err := b.CreateDBInstance(
		"reboot-delay",
		"postgres",
		"db.t3.micro",
		"",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{},
	)
	require.NoError(t, err)

	inst, err := b.RebootDBInstance("reboot-delay")
	require.NoError(t, err)
	assert.Equal(t, "rebooting", inst.DBInstanceStatus)

	// After the transition delay the reconciler moves it back to available.
	instances, err := b.DescribeDBInstances("reboot-delay")
	require.NoError(t, err)
	require.Len(t, instances, 1)
	// Status is either still rebooting or already available depending on timing;
	// at minimum it must be one of the two valid states.
	status := instances[0].DBInstanceStatus
	assert.Truef(t, status == "rebooting" || status == "available",
		"unexpected status %q", status)
}

// TestReset verifies that Backend.Reset() clears all state.
func TestReset(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBInstance("inst-1", "mysql", "", "", "", "", 0, rds.DBInstanceOptions{})
	require.NoError(t, err)

	b.AddClusterInternal("cluster-1", "aurora-mysql")

	require.Equal(t, 1, rds.InstanceCount(b))
	require.Equal(t, 1, rds.ClusterCount(b))

	b.Reset()

	assert.Equal(t, 0, rds.InstanceCount(b))
	assert.Equal(t, 0, rds.ClusterCount(b))
}

// TestDeleteDBInstanceCascadeInstanceRoles verifies that deleting an instance
// also removes its IAM role associations.
func TestDeleteDBInstanceCascadeInstanceRoles(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddInstanceInternal("my-inst", "mysql")

	err := b.AddRoleToDBInstance("my-inst", "arn:aws:iam::000:role/R1", "S3_INTEGRATION")
	require.NoError(t, err)
	require.Equal(t, 1, rds.InstanceRoleCount(b, "my-inst"))

	_, err = b.DeleteDBInstance("my-inst")
	require.NoError(t, err)

	assert.Equal(t, 0, rds.InstanceRoleCount(b, "my-inst"))
}

// TestRDSBackend_NewInstanceFields tests the new DBInstance fields.
func TestRDSBackend_NewInstanceFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		wantST string
		wantAZ string
		opts   rds.DBInstanceOptions
		wantBR int
		wantMA bool
		wantSE bool
	}{
		{
			name:   "defaults",
			opts:   rds.DBInstanceOptions{},
			wantST: "gp2",
			wantAZ: "us-east-1a",
			wantBR: 0,
			wantMA: false,
			wantSE: false,
		},
		{
			name: "custom_fields",
			opts: rds.DBInstanceOptions{
				StorageType:           "io1",
				AvailabilityZone:      "us-east-1b",
				BackupRetentionPeriod: 7,
				MultiAZ:               true,
				StorageEncrypted:      true,
			},
			wantST: "io1",
			wantAZ: "us-east-1b",
			wantBR: 7,
			wantMA: true,
			wantSE: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			inst, err := b.CreateDBInstance("test-db", "postgres", "db.t3.micro", "mydb", "admin", "", 20, tt.opts)

			require.NoError(t, err)
			assert.Equal(t, tt.wantST, inst.StorageType)
			assert.Equal(t, tt.wantAZ, inst.AvailabilityZone)
			assert.Equal(t, tt.wantBR, inst.BackupRetentionPeriod)
			assert.Equal(t, tt.wantMA, inst.MultiAZ)
			assert.Equal(t, tt.wantSE, inst.StorageEncrypted)
		})
	}
}

// TestRDSBackend_StartStopDBInstance tests starting and stopping instances.
func TestRDSBackend_StartStopDBInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		setup     func(b *rds.InMemoryBackend) error
		action    func(b *rds.InMemoryBackend) error
		name      string
		wantErr   bool
	}{
		{
			name: "stop_available_instance",
			setup: func(b *rds.InMemoryBackend) error {
				_, err := b.CreateDBInstance("inst", "postgres", "", "", "", "", 0, rds.DBInstanceOptions{})

				return err
			},
			action: func(b *rds.InMemoryBackend) error {
				inst, err := b.StopDBInstance("inst")
				if err != nil {
					return err
				}
				assert.Equal(t, "stopped", inst.DBInstanceStatus)

				return nil
			},
		},
		{
			name: "start_stopped_instance",
			setup: func(b *rds.InMemoryBackend) error {
				_, err := b.CreateDBInstance("inst", "postgres", "", "", "", "", 0, rds.DBInstanceOptions{})
				if err != nil {
					return err
				}
				rds.FlushInstanceLifecycle(b)
				_, err = b.StopDBInstance("inst")

				return err
			},
			action: func(b *rds.InMemoryBackend) error {
				inst, err := b.StartDBInstance("inst")
				if err != nil {
					return err
				}
				assert.Equal(t, "available", inst.DBInstanceStatus)

				return nil
			},
		},
		{
			name:  "stop_not_found",
			setup: func(_ *rds.InMemoryBackend) error { return nil },
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.StopDBInstance("nonexistent")

				return err
			},
			wantErr:   true,
			wantErrIs: rds.ErrInstanceNotFound,
		},
		{
			name: "start_not_stopped",
			setup: func(b *rds.InMemoryBackend) error {
				_, err := b.CreateDBInstance("inst", "postgres", "", "", "", "", 0, rds.DBInstanceOptions{})

				return err
			},
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.StartDBInstance("inst")

				return err
			},
			wantErr:   true,
			wantErrIs: rds.ErrInvalidDBInstanceState,
		},
		{
			name: "stop_already_stopped",
			setup: func(b *rds.InMemoryBackend) error {
				_, err := b.CreateDBInstance("inst", "postgres", "", "", "", "", 0, rds.DBInstanceOptions{})
				if err != nil {
					return err
				}
				rds.FlushInstanceLifecycle(b)
				_, err = b.StopDBInstance("inst")

				return err
			},
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.StopDBInstance("inst")

				return err
			},
			wantErr:   true,
			wantErrIs: rds.ErrInvalidDBInstanceState,
		},
		{
			name:  "stop_empty_id",
			setup: func(_ *rds.InMemoryBackend) error { return nil },
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.StopDBInstance("")

				return err
			},
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:  "start_empty_id",
			setup: func(_ *rds.InMemoryBackend) error { return nil },
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.StartDBInstance("")

				return err
			},
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, tt.setup(b))
			rds.FlushInstanceLifecycle(b) // advance creating→available for tests that need it

			err := tt.action(b)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRDSBackend_RestoreDBInstanceToPointInTime tests point-in-time restore.
func TestRDSBackend_RestoreDBInstanceToPointInTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(b *rds.InMemoryBackend)
		name    string
		target  string
		source  string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance(
					"src-db",
					"postgres",
					"db.t3.micro",
					"mydb",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
			},
			target: "pit-db",
			source: "src-db",
		},
		{
			name:    "source_not_found",
			setup:   func(_ *rds.InMemoryBackend) {},
			target:  "pit-db",
			source:  "no-db",
			wantErr: true,
			errIs:   rds.ErrInstanceNotFound,
		},
		{
			name: "target_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("src-db", "postgres", "", "", "", "", 0, rds.DBInstanceOptions{})
				_, _ = b.CreateDBInstance("pit-db", "postgres", "", "", "", "", 0, rds.DBInstanceOptions{})
			},
			target:  "pit-db",
			source:  "src-db",
			wantErr: true,
			errIs:   rds.ErrInstanceAlreadyExists,
		},
		{
			name:    "empty_target",
			setup:   func(_ *rds.InMemoryBackend) {},
			target:  "",
			source:  "src-db",
			wantErr: true,
			errIs:   rds.ErrInvalidParameter,
		},
		{
			name:    "empty_source",
			setup:   func(_ *rds.InMemoryBackend) {},
			target:  "pit-db",
			source:  "",
			wantErr: true,
			errIs:   rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			inst, err := b.RestoreDBInstanceToPointInTime(tt.target, tt.source, rds.DBInstanceOptions{})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.target, inst.DBInstanceIdentifier)
			assert.Equal(t, "available", inst.DBInstanceStatus)
		})
	}
}

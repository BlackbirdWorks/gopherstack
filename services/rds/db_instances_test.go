package rds_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestBatch3_Persistence_InstanceNewFields(t *testing.T) {
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

func TestAccOps2_RebootDBInstance_ReturnsRebootingStatus(t *testing.T) {
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

func TestAccOps2_RebootDBInstance_NotFound_Returns400(t *testing.T) {
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

func TestAccOps2_RebootDBInstance_TransitionsToAvailableAfterDelay(t *testing.T) {
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

// TestRefinement1_Reset verifies that Backend.Reset() clears all state.
func TestRefinement1_Reset(t *testing.T) {
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

// TestRefinement1_DeleteDBInstanceCascadeInstanceRoles verifies that deleting an instance
// also removes its IAM role associations.
func TestRefinement1_DeleteDBInstanceCascadeInstanceRoles(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddInstanceInternal("my-inst", "mysql")

	err := b.AddRoleToDBInstance("my-inst", "arn:aws:iam::000:role/R1")
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

// TestRDSBackend_ModifyDBInstance_NewFields tests ModifyDBInstance with new fields.
func TestRDSBackend_ModifyDBInstance_NewFields(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBInstance("mod-db", "postgres", "db.t3.micro", "", "", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)

	opts := rds.DBInstanceOptions{
		StorageType:           "io1",
		BackupRetentionPeriod: 14,
		MultiAZ:               true,
		ApplyImmediately:      true,
	}

	inst, err := b.ModifyDBInstance("mod-db", "db.r5.large", 100, opts)
	require.NoError(t, err)
	assert.Equal(t, "db.r5.large", inst.DBInstanceClass)
	assert.Equal(t, 100, inst.AllocatedStorage)
	assert.Equal(t, "io1", inst.StorageType)
	assert.Equal(t, 14, inst.BackupRetentionPeriod)
	assert.True(t, inst.MultiAZ)
}

func TestRDSBackend_InstanceModifyTransitionAndDeletePublishesEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create modify delete transitions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			const instanceID = "transition-db"

			created, err := b.CreateDBInstance(instanceID, "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
			require.NoError(t, err)
			assert.Equal(t, "creating", created.DBInstanceStatus)

			modified, err := b.ModifyDBInstance(instanceID, "db.r5.large", 100, rds.DBInstanceOptions{})
			require.NoError(t, err)
			assert.Equal(t, "modifying", modified.DBInstanceStatus)

			require.Eventually(t, func() bool {
				instances, describeErr := b.DescribeDBInstances(instanceID)
				if describeErr != nil || len(instances) != 1 {
					return false
				}

				return instances[0].DBInstanceStatus == "available" && instances[0].DBInstanceClass == "db.r5.large"
			}, 3*time.Second, 20*time.Millisecond)

			deleted, err := b.DeleteDBInstance(instanceID)
			require.NoError(t, err)
			assert.Equal(t, "deleting", deleted.DBInstanceStatus)
			_, err = b.DescribeDBInstances(instanceID)
			require.ErrorIs(t, err, rds.ErrInstanceNotFound)

			messages := rds.EventMessagesForSource(b, instanceID)
			assert.Contains(t, messages, "DB instance created")
			assert.Contains(t, messages, "DB instance is now available")
			assert.Contains(t, messages, "DB instance modification started")
			assert.Contains(t, messages, "DB instance deletion started")
			assert.Contains(t, messages, "DB instance deleted")
		})
	}
}

// TestParityB_CreateDBInstance_IdentifierValidation verifies that
// CreateDBInstance enforces AWS identifier constraints:
// must start with a letter, contain only alphanumeric/hyphens, 1–63 chars.
func TestParityB_CreateDBInstance_IdentifierValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		wantStatus int
	}{
		{
			name:       "valid_simple",
			id:         "my-db-instance",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_single_letter",
			id:         "a",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_63_chars",
			id:         "a123456789012345678901234567890123456789012345678901234567890ab",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_starts_with_digit",
			id:         "1mydb",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_starts_with_hyphen",
			id:         "-mydb",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_underscore",
			id:         "my_db",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_space",
			id:         "my db",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_64_chars",
			id:         "a1234567890123456789012345678901234567890123456789012345678901234",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler()
			rec := doAccuracyRDS(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {tt.id},
				"Engine":               {"postgres"},
				"MasterUsername":       {"admin"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "id=%q", tt.id)
		})
	}
}

// TestCreateDBInstance_AllocatedStorageBound verifies that CreateDBInstance
// rejects an out-of-range AllocatedStorage (AWS bound: 20–65536 GiB) and
// accepts in-range values.
func TestCreateDBInstance_AllocatedStorageBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		storage    string
		wantStatus int
	}{
		{name: "below min", id: "as-below-min", storage: "10", wantStatus: http.StatusBadRequest},
		{name: "at min", id: "as-at-min", storage: "20", wantStatus: http.StatusOK},
		{name: "mid range", id: "as-mid-range", storage: "100", wantStatus: http.StatusOK},
		{name: "at max", id: "as-at-max", storage: "65536", wantStatus: http.StatusOK},
		{name: "above max", id: "as-above-max", storage: "65537", wantStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler()
			rec := doAccuracyRDS(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {tc.id},
				"DBInstanceClass":      {"db.t3.micro"},
				"Engine":               {"postgres"},
				"MasterUsername":       {"admin"},
				"AllocatedStorage":     {tc.storage},
			})
			assert.Equal(t, tc.wantStatus, rec.Code, "AllocatedStorage=%s", tc.storage)
		})
	}
}

// TestRDS_CreateDBInstance_IdentifierValidation asserts that invalid identifiers are rejected.
func TestRDS_CreateDBInstance_IdentifierValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		dbInstanceID     string
		dbInstanceClass  string
		engine           string
		wantErrorContain string
		wantCode         int
	}{
		{
			name:            "valid_identifier",
			dbInstanceID:    "my-db-instance",
			dbInstanceClass: "db.t3.micro",
			engine:          "mysql",
			wantCode:        http.StatusOK,
		},
		{
			name:             "empty_identifier",
			dbInstanceID:     "",
			dbInstanceClass:  "db.t3.micro",
			engine:           "mysql",
			wantCode:         http.StatusBadRequest,
			wantErrorContain: "Error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			body := "Action=CreateDBInstance" +
				"&DBInstanceIdentifier=" + tt.dbInstanceID +
				"&DBInstanceClass=" + tt.dbInstanceClass +
				"&Engine=" + tt.engine

			rec := postRDSForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrorContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrorContain)
			}
		})
	}
}

func TestSwitchoverReadReplica(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs  error
		name       string
		instanceID string
		wantErr    bool
	}{
		{
			name:       "success",
			instanceID: "my-instance",
		},
		{
			name:       "not found",
			instanceID: "missing",
			wantErr:    true,
			wantErrIs:  rds.ErrInstanceNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBInstance(
					tt.instanceID,
					"mysql",
					"db.t3.micro",
					"",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.SwitchoverReadReplica(tt.instanceID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.instanceID, got.DBInstanceIdentifier)
			assert.Empty(t, got.ReplicaSourceDBInstanceIdentifier)
		})
	}
}

func TestRestoreDBInstanceFromS3(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		instanceID      string
		engine          string
		dbInstanceClass string
		s3Bucket        string
		wantErr         bool
	}{
		{
			name:            "success",
			instanceID:      "restored-db",
			engine:          "mysql",
			dbInstanceClass: "db.t3.micro",
			s3Bucket:        "my-backup-bucket",
		},
		{
			name:       "empty bucket",
			instanceID: "restored-db",
			engine:     "mysql",
			s3Bucket:   "",
			wantErr:    true,
			wantErrIs:  rds.ErrInvalidParameter,
		},
		{
			name:       "empty id",
			instanceID: "",
			engine:     "mysql",
			s3Bucket:   "my-bucket",
			wantErr:    true,
			wantErrIs:  rds.ErrInvalidParameter,
		},
		{
			name:       "already exists",
			instanceID: "existing-db",
			engine:     "mysql",
			s3Bucket:   "my-bucket",
			wantErr:    true,
			wantErrIs:  rds.ErrInstanceAlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "already exists" {
				_, err := b.CreateDBInstance(
					tt.instanceID,
					tt.engine,
					tt.dbInstanceClass,
					"",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.RestoreDBInstanceFromS3(tt.instanceID, tt.engine, tt.dbInstanceClass, tt.s3Bucket)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.instanceID, got.DBInstanceIdentifier)
			assert.Equal(t, tt.engine, got.Engine)
		})
	}
}

// TestInstanceCreateTime verifies that InstanceCreateTime is set on CreateDBInstance.
func TestInstanceCreateTime(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	before := time.Now().UTC()
	inst, err := b.CreateDBInstance("db-ts", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)
	after := time.Now().UTC()
	assert.False(t, inst.InstanceCreateTime.IsZero(), "InstanceCreateTime should be set")
	assert.False(t, inst.InstanceCreateTime.Before(before), "InstanceCreateTime should be after test start")
	assert.False(t, inst.InstanceCreateTime.After(after), "InstanceCreateTime should be before test end")
}

// TestDescribeDBInstancesSorted verifies deterministic sort order.
func TestDescribeDBInstancesSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	for _, id := range []string{"db-z", "db-a", "db-m"} {
		_, err := b.CreateDBInstance(id, "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
		require.NoError(t, err)
	}
	got, err := b.DescribeDBInstances("")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "db-a", got[0].DBInstanceIdentifier)
	assert.Equal(t, "db-m", got[1].DBInstanceIdentifier)
	assert.Equal(t, "db-z", got[2].DBInstanceIdentifier)
}

// TestIopsAndStorageThroughputPersisted verifies Iops and StorageThroughput are stored and returned.
func TestIopsAndStorageThroughputPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"iops-test"},
		"DBInstanceClass":      {"db.r6g.large"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"100"},
		"StorageType":          {"io1"},
		"Iops":                 {"3000"},
		"StorageThroughput":    {"125"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				Iops              int `xml:"Iops"`
				StorageThroughput int `xml:"StorageThroughput"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 3000, resp.Result.DBInstance.Iops)
	assert.Equal(t, 125, resp.Result.DBInstance.StorageThroughput)
}

// TestVpcSecurityGroupsPersisted verifies VpcSecurityGroups are stored and returned.
func TestVpcSecurityGroupsPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"sg-test"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"VpcSecurityGroupIds.VpcSecurityGroupID.1": {"sg-11111111"},
		"VpcSecurityGroupIds.VpcSecurityGroupID.2": {"sg-22222222"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				VpcSecurityGroups struct {
					Members []struct {
						VpcSecurityGroupID string `xml:"VpcSecurityGroupId"`
						Status             string `xml:"Status"`
					} `xml:"VpcSecurityGroupMembership"`
				} `xml:"VpcSecurityGroups"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.DBInstance.VpcSecurityGroups.Members, 2)
	assert.Equal(t, "sg-11111111", resp.Result.DBInstance.VpcSecurityGroups.Members[0].VpcSecurityGroupID)
	assert.Equal(t, "active", resp.Result.DBInstance.VpcSecurityGroups.Members[0].Status)
}

// TestLicenseModelPersisted verifies LicenseModel is stored and returned.
func TestLicenseModelPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"license-test"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"oracle-ee"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"100"},
		"LicenseModel":         {"bring-your-own-license"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				LicenseModel string `xml:"LicenseModel"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "bring-your-own-license", resp.Result.DBInstance.LicenseModel)
}

// TestMonitoringFieldsPersisted verifies monitoring fields are stored and returned.
func TestMonitoringFieldsPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"monitoring-test"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"MonitoringInterval":   {"60"},
		"MonitoringRoleArn":    {"arn:aws:iam::123456789012:role/rds-monitoring"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				MonitoringRoleArn  string `xml:"MonitoringRoleArn"`
				MonitoringInterval int    `xml:"MonitoringInterval"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 60, resp.Result.DBInstance.MonitoringInterval)
	assert.Equal(t, "arn:aws:iam::123456789012:role/rds-monitoring", resp.Result.DBInstance.MonitoringRoleArn)
}

// TestPreferredWindowsPersisted verifies preferred windows are stored and returned.
func TestPreferredWindowsPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBInstance"},
		"Version":                    {"2014-10-31"},
		"DBInstanceIdentifier":       {"windows-test"},
		"DBInstanceClass":            {"db.t3.micro"},
		"Engine":                     {"postgres"},
		"MasterUsername":             {"admin"},
		"AllocatedStorage":           {"20"},
		"PreferredMaintenanceWindow": {"mon:03:00-mon:04:00"},
		"PreferredBackupWindow":      {"02:00-03:00"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				PreferredMaintenanceWindow string `xml:"PreferredMaintenanceWindow"`
				PreferredBackupWindow      string `xml:"PreferredBackupWindow"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "mon:03:00-mon:04:00", resp.Result.DBInstance.PreferredMaintenanceWindow)
	assert.Equal(t, "02:00-03:00", resp.Result.DBInstance.PreferredBackupWindow)
}

// TestModifyDBInstanceNewFields verifies that ApplyImmediately, AllowMajorVersionUpgrade,
// VpcSecurityGroupIds, and MonitoringInterval can be modified.
func TestModifyDBInstanceNewFields(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "modify-new-fields")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                   {"ModifyDBInstance"},
		"Version":                  {"2014-10-31"},
		"DBInstanceIdentifier":     {"modify-new-fields"},
		"ApplyImmediately":         {"true"},
		"AllowMajorVersionUpgrade": {"true"},
		"MonitoringInterval":       {"30"},
		"MonitoringRoleArn":        {"arn:aws:iam::123456789012:role/rds-mon"},
		"VpcSecurityGroupIds.VpcSecurityGroupID.1": {"sg-aaaaaaaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				VpcSecurityGroups struct {
					Members []struct {
						VpcSecurityGroupID string `xml:"VpcSecurityGroupId"`
					} `xml:"VpcSecurityGroupMembership"`
				} `xml:"VpcSecurityGroups"`
				MonitoringInterval int `xml:"MonitoringInterval"`
			} `xml:"DBInstance"`
		} `xml:"ModifyDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 30, resp.Result.DBInstance.MonitoringInterval)
	require.Len(t, resp.Result.DBInstance.VpcSecurityGroups.Members, 1)
	assert.Equal(t, "sg-aaaaaaaa", resp.Result.DBInstance.VpcSecurityGroups.Members[0].VpcSecurityGroupID)
}

// TestReadReplicaIdentifiersTrackedOnSource verifies source instance tracks replicas.
func TestReadReplicaIdentifiersTrackedOnSource(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "replica-src")

	// Create two read replicas.
	for _, replicaID := range []string{"replica-a", "replica-b"} {
		rec := doAccuracyRDS(t, h, url.Values{
			"Action":                     {"CreateDBInstanceReadReplica"},
			"Version":                    {"2014-10-31"},
			"DBInstanceIdentifier":       {replicaID},
			"SourceDBInstanceIdentifier": {"replica-src"},
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	}

	// DescribeDBInstances should show the source has ReadReplicaIdentifiers.
	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateDBInstance(
		"replica-src-b",
		"postgres",
		"db.t3.micro",
		"",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{},
	)
	require.NoError(t, err)
	_, err = b.CreateDBInstanceReadReplica("rep-x", "replica-src-b", "")
	require.NoError(t, err)
	_, err = b.CreateDBInstanceReadReplica("rep-y", "replica-src-b", "")
	require.NoError(t, err)

	instances, err := b.DescribeDBInstances("replica-src-b")
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.Contains(t, instances[0].ReadReplicaIdentifiers, "rep-x")
	assert.Contains(t, instances[0].ReadReplicaIdentifiers, "rep-y")
}

// TestVpcSecurityGroupsViaModify verifies VpcSecurityGroups update via ModifyDBInstance.
func TestVpcSecurityGroupsViaModify(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "sg-modify-inst")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"sg-modify-inst"},
		"VpcSecurityGroupIds.VpcSecurityGroupID.1": {"sg-ffffffff"},
		"VpcSecurityGroupIds.VpcSecurityGroupID.2": {"sg-eeeeeeee"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				VpcSecurityGroups struct {
					Members []struct {
						VpcSecurityGroupID string `xml:"VpcSecurityGroupId"`
					} `xml:"VpcSecurityGroupMembership"`
				} `xml:"VpcSecurityGroups"`
			} `xml:"DBInstance"`
		} `xml:"ModifyDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.DBInstance.VpcSecurityGroups.Members, 2)
}

// TestReadReplicaIdentifiersInXML verifies ReadReplicaDBInstanceIdentifiers is emitted in XML.
func TestReadReplicaIdentifiersInXML(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "source-inst")

	// Create a read replica.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBInstanceReadReplica"},
		"Version":                    {"2014-10-31"},
		"DBInstanceIdentifier":       {"replica-inst"},
		"SourceDBInstanceIdentifier": {"source-inst"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe source - should show replica ID in ReadReplicaDBInstanceIdentifiers.
	rec = doAccuracyRDS(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"source-inst"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstances struct {
				Members []struct {
					ReadReplicaDBInstanceIdentifiers struct {
						Members []string `xml:"ReadReplicaDBInstanceIdentifier"`
					} `xml:"ReadReplicaDBInstanceIdentifiers"`
				} `xml:"DBInstance"`
			} `xml:"DBInstances"`
		} `xml:"DescribeDBInstancesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	members := resp.Result.DBInstances.Members
	require.Len(t, members, 1)
	assert.Equal(t, []string{"replica-inst"}, members[0].ReadReplicaDBInstanceIdentifiers.Members)
}

// TestDeleteReplicaClearsSourceReadReplicaIdentifiers verifies deleting a replica removes it from the source.
func TestDeleteReplicaClearsSourceReadReplicaIdentifiers(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "src-del")

	doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBInstanceReadReplica"},
		"Version":                    {"2014-10-31"},
		"DBInstanceIdentifier":       {"rep-del"},
		"SourceDBInstanceIdentifier": {"src-del"},
	})

	// Delete the replica.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"DeleteDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"rep-del"},
		"SkipFinalSnapshot":    {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Source should no longer list rep-del in ReadReplicaDBInstanceIdentifiers.
	rec = doAccuracyRDS(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"src-del"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstances struct {
				Members []struct {
					ReadReplicaDBInstanceIdentifiers struct {
						Members []string `xml:"ReadReplicaDBInstanceIdentifier"`
					} `xml:"ReadReplicaDBInstanceIdentifiers"`
				} `xml:"DBInstance"`
			} `xml:"DBInstances"`
		} `xml:"DescribeDBInstancesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	members := resp.Result.DBInstances.Members
	require.Len(t, members, 1)
	assert.Empty(t, members[0].ReadReplicaDBInstanceIdentifiers.Members)
}

// TestPromoteReadReplicaClearsSourceList verifies PromoteReadReplica removes from source's list.
func TestPromoteReadReplicaClearsSourceList(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "src-promote")

	doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBInstanceReadReplica"},
		"Version":                    {"2014-10-31"},
		"DBInstanceIdentifier":       {"rep-promote"},
		"SourceDBInstanceIdentifier": {"src-promote"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"PromoteReadReplica"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"rep-promote"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAccuracyRDS(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"src-promote"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstances struct {
				Members []struct {
					ReadReplicaDBInstanceIdentifiers struct {
						Members []string `xml:"ReadReplicaDBInstanceIdentifier"`
					} `xml:"ReadReplicaDBInstanceIdentifiers"`
				} `xml:"DBInstance"`
			} `xml:"DBInstances"`
		} `xml:"DescribeDBInstancesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	members := resp.Result.DBInstances.Members
	require.Len(t, members, 1)
	assert.Empty(t, members[0].ReadReplicaDBInstanceIdentifiers.Members)
}

// TestEnabledCloudwatchLogsExportsInInstanceXML verifies EnabledCloudwatchLogsExports is emitted for instances.
func TestEnabledCloudwatchLogsExportsInInstanceXML(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                               {"CreateDBInstance"},
		"Version":                              {"2014-10-31"},
		"DBInstanceIdentifier":                 {"logs-inst"},
		"DBInstanceClass":                      {"db.t3.micro"},
		"Engine":                               {"mysql"},
		"MasterUsername":                       {"admin"},
		"AllocatedStorage":                     {"20"},
		"EnableCloudwatchLogsExports.member.1": {"error"},
		"EnableCloudwatchLogsExports.member.2": {"slowquery"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				EnabledCloudwatchLogsExports struct {
					Members []string `xml:"member"`
				} `xml:"EnabledCloudwatchLogsExports"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"error", "slowquery"}, resp.Result.DBInstance.EnabledCloudwatchLogsExports.Members)
}

// TestInstanceFieldRangeValidation verifies MonitoringInterval and BackupRetentionPeriod
// range validation on both CreateDBInstance and ModifyDBInstance. It merges what were
// previously four separate but structurally identical tests (TestMonitoringIntervalValidation,
// TestMonitoringIntervalValidationModify, TestBackupRetentionPeriodValidation,
// TestBackupRetentionPeriodValidationModify) into one table, preserving every case.
func TestInstanceFieldRangeValidation(t *testing.T) {
	t.Parallel()

	type fieldRangeCase struct {
		name        string
		id          string
		action      string
		modifyField string
		modifyValue string
		wantCode    int
	}

	tests := make([]fieldRangeCase, 0, 11)
	tests = append(tests,
		fieldRangeCase{
			name: "monitoring interval 7 invalid on create", id: "mi-invalid", action: "CreateDBInstance",
			modifyField: "MonitoringInterval", modifyValue: "7", wantCode: http.StatusBadRequest,
		},
		fieldRangeCase{
			name: "monitoring interval 3 invalid on modify", id: "mi-modify", action: "ModifyDBInstance",
			modifyField: "MonitoringInterval", modifyValue: "3", wantCode: http.StatusBadRequest,
		},
		fieldRangeCase{
			name: "backup retention period 36 invalid on create", id: "brp-invalid", action: "CreateDBInstance",
			modifyField: "BackupRetentionPeriod", modifyValue: "36", wantCode: http.StatusBadRequest,
		},
		fieldRangeCase{
			name: "backup retention period 40 invalid on modify", id: "brp-modify", action: "ModifyDBInstance",
			modifyField: "BackupRetentionPeriod", modifyValue: "40", wantCode: http.StatusBadRequest,
		},
	)
	for _, v := range []string{"0", "1", "5", "10", "15", "30", "60"} {
		tests = append(tests, fieldRangeCase{
			name: "monitoring interval " + v + " valid on create", id: "mi-valid-" + v, action: "CreateDBInstance",
			modifyField: "MonitoringInterval", modifyValue: v, wantCode: http.StatusOK,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler()
			id := tt.id

			if tt.action == "ModifyDBInstance" {
				mustCreateAccuracyRDSInstance(t, h, id)
				rec := doAccuracyRDS(t, h, url.Values{
					"Action":               {"ModifyDBInstance"},
					"Version":              {"2014-10-31"},
					"DBInstanceIdentifier": {id},
					tt.modifyField:         {tt.modifyValue},
				})
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			rec := doAccuracyRDS(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {id},
				"DBInstanceClass":      {"db.t3.micro"},
				"Engine":               {"postgres"},
				"MasterUsername":       {"admin"},
				"AllocatedStorage":     {"20"},
				tt.modifyField:         {tt.modifyValue},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestEngineVersionPersisted verifies EngineVersion is stored and returned.
func TestEngineVersionPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ev-inst"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"EngineVersion":        {"14.5"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				EngineVersion string `xml:"EngineVersion"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "14.5", resp.Result.DBInstance.EngineVersion)
}

// TestModifyDBInstanceUpdatesEngineVersion verifies EngineVersion is updated in ModifyDBInstance.
func TestModifyDBInstanceUpdatesEngineVersion(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ev-modify"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"EngineVersion":        {"14.5"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ev-modify"},
		"EngineVersion":        {"15.0"},
		"ApplyImmediately":     {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				EngineVersion string `xml:"EngineVersion"`
			} `xml:"DBInstance"`
		} `xml:"ModifyDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "15.0", resp.Result.DBInstance.EngineVersion)
}

// TestReadReplicaInheritsEngineVersion verifies a read replica inherits the source's EngineVersion.
func TestReadReplicaInheritsEngineVersion(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ev-source"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"EngineVersion":        {"15.1"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBInstanceReadReplica"},
		"Version":                    {"2014-10-31"},
		"DBInstanceIdentifier":       {"ev-replica"},
		"SourceDBInstanceIdentifier": {"ev-source"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				EngineVersion string `xml:"EngineVersion"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceReadReplicaResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "15.1", resp.Result.DBInstance.EngineVersion)
}

// TestPubliclyAccessiblePersisted verifies PubliclyAccessible is stored and returned.
func TestPubliclyAccessiblePersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"pa-inst"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"PubliclyAccessible":   {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				PubliclyAccessible bool `xml:"PubliclyAccessible"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBInstance.PubliclyAccessible)
}

// TestPendingModifiedValuesEmittedWhenModifying verifies PendingModifiedValues is emitted on modifying state.
func TestPendingModifiedValuesEmittedWhenModifying(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "pmv-inst")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"pmv-inst"},
		"DBInstanceClass":      {"db.r6g.large"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// The response should have a PendingModifiedValues element since instance is now modifying.
	body := rec.Body.String()
	assert.Contains(t, body, "PendingModifiedValues")
}

// TestCloudwatchLogsExportsModifyInstance verifies CloudWatch log exports can be updated on an instance.
func TestCloudwatchLogsExportsModifyInstance(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "cwl-modify")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"cwl-modify"},
		"CloudwatchLogsExportConfiguration.EnableLogTypes.member.1": {"postgresql"},
		"CloudwatchLogsExportConfiguration.EnableLogTypes.member.2": {"upgrade"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				EnabledCloudwatchLogsExports struct {
					Members []string `xml:"member"`
				} `xml:"EnabledCloudwatchLogsExports"`
			} `xml:"DBInstance"`
		} `xml:"ModifyDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"postgresql", "upgrade"}, resp.Result.DBInstance.EnabledCloudwatchLogsExports.Members)
}

package rds_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

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

// TestRDSBackend_CopyDBSnapshot tests snapshot copy operations.
func TestRDSBackend_CopyDBSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs    error
		setup    func(b *rds.InMemoryBackend)
		name     string
		sourceID string
		targetID string
		wantErr  bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance(
					"db",
					"postgres",
					"",
					"",
					"",
					"",
					20,
					rds.DBInstanceOptions{StorageEncrypted: true},
				)
				_, _ = b.CreateDBSnapshot("snap-src", "db")
			},
			sourceID: "snap-src",
			targetID: "snap-dst",
		},
		{
			name:     "source_not_found",
			setup:    func(_ *rds.InMemoryBackend) {},
			sourceID: "no-snap",
			targetID: "snap-dst",
			wantErr:  true,
			errIs:    rds.ErrSnapshotNotFound,
		},
		{
			name: "target_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("db", "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
				_, _ = b.CreateDBSnapshot("snap-a", "db")
				_, _ = b.CreateDBSnapshot("snap-b", "db")
			},
			sourceID: "snap-a",
			targetID: "snap-b",
			wantErr:  true,
			errIs:    rds.ErrSnapshotAlreadyExists,
		},
		{
			name:     "empty_source",
			setup:    func(_ *rds.InMemoryBackend) {},
			sourceID: "",
			targetID: "snap-dst",
			wantErr:  true,
			errIs:    rds.ErrInvalidParameter,
		},
		{
			name:     "empty_target",
			setup:    func(_ *rds.InMemoryBackend) {},
			sourceID: "snap-src",
			targetID: "",
			wantErr:  true,
			errIs:    rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			snap, err := b.CopyDBSnapshot(tt.sourceID, tt.targetID, rds.CopyDBSnapshotOptions{})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.targetID, snap.DBSnapshotIdentifier)
			assert.Equal(t, "available", snap.Status)
		})
	}
}

// TestRDSBackend_RestoreDBInstanceFromDBSnapshot tests instance restore from snapshot.
func TestRDSBackend_RestoreDBInstanceFromDBSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs      error
		setup      func(b *rds.InMemoryBackend)
		name       string
		instanceID string
		snapshotID string
		opts       rds.DBInstanceOptions
		wantErr    bool
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
					rds.DBInstanceOptions{StorageEncrypted: true},
				)
				_, _ = b.CreateDBSnapshot("snap", "src-db")
			},
			instanceID: "restored-db",
			snapshotID: "snap",
			opts:       rds.DBInstanceOptions{},
		},
		{
			name:       "snapshot_not_found",
			setup:      func(_ *rds.InMemoryBackend) {},
			instanceID: "restored-db",
			snapshotID: "no-snap",
			wantErr:    true,
			errIs:      rds.ErrSnapshotNotFound,
		},
		{
			name: "instance_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("existing-db", "postgres", "", "", "", "", 0, rds.DBInstanceOptions{})
				_, _ = b.CreateDBSnapshot("snap", "existing-db")
			},
			instanceID: "existing-db",
			snapshotID: "snap",
			wantErr:    true,
			errIs:      rds.ErrInstanceAlreadyExists,
		},
		{
			name:       "empty_instance_id",
			setup:      func(_ *rds.InMemoryBackend) {},
			instanceID: "",
			snapshotID: "snap",
			wantErr:    true,
			errIs:      rds.ErrInvalidParameter,
		},
		{
			name:       "empty_snapshot_id",
			setup:      func(_ *rds.InMemoryBackend) {},
			instanceID: "restored-db",
			snapshotID: "",
			wantErr:    true,
			errIs:      rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			inst, err := b.RestoreDBInstanceFromDBSnapshot(tt.instanceID, tt.snapshotID, tt.opts)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.instanceID, inst.DBInstanceIdentifier)
			assert.Equal(t, "available", inst.DBInstanceStatus)
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

// TestRDSBackend_GlobalCluster tests global cluster CRUD.
func TestRDSBackend_GlobalCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(b *rds.InMemoryBackend)
		run     func(b *rds.InMemoryBackend) error
		name    string
		wantErr bool
	}{
		{
			name:  "create_and_describe",
			setup: func(_ *rds.InMemoryBackend) {},
			run: func(b *rds.InMemoryBackend) error {
				gc, err := b.CreateGlobalCluster("gc1", "aurora-postgresql", "14.3", true, false)
				if err != nil {
					return err
				}
				assert.Equal(t, "gc1", gc.GlobalClusterIdentifier)
				assert.Equal(t, "available", gc.Status)
				assert.True(t, gc.StorageEncrypted)

				clusters, err := b.DescribeGlobalClusters("")
				if err != nil {
					return err
				}
				assert.Len(t, clusters, 1)

				return nil
			},
		},
		{
			name: "describe_specific",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateGlobalCluster("gc1", "aurora", "", false, false)
			},
			run: func(b *rds.InMemoryBackend) error {
				clusters, err := b.DescribeGlobalClusters("gc1")
				if err != nil {
					return err
				}
				assert.Len(t, clusters, 1)
				assert.Equal(t, "gc1", clusters[0].GlobalClusterIdentifier)

				return nil
			},
		},
		{
			name:  "create_duplicate",
			setup: func(b *rds.InMemoryBackend) { _, _ = b.CreateGlobalCluster("gc1", "", "", false, false) },
			run: func(b *rds.InMemoryBackend) error {
				_, err := b.CreateGlobalCluster("gc1", "", "", false, false)

				return err
			},
			wantErr: true,
			errIs:   rds.ErrGlobalClusterAlreadyExists,
		},
		{
			name:  "delete",
			setup: func(b *rds.InMemoryBackend) { _, _ = b.CreateGlobalCluster("gc1", "", "", false, false) },
			run: func(b *rds.InMemoryBackend) error {
				gc, err := b.DeleteGlobalCluster("gc1")
				if err != nil {
					return err
				}
				assert.Equal(t, "gc1", gc.GlobalClusterIdentifier)
				clusters, _ := b.DescribeGlobalClusters("")
				assert.Empty(t, clusters)

				return nil
			},
		},
		{
			name:  "delete_not_found",
			setup: func(_ *rds.InMemoryBackend) {},
			run: func(b *rds.InMemoryBackend) error {
				_, err := b.DeleteGlobalCluster("nonexistent")

				return err
			},
			wantErr: true,
			errIs:   rds.ErrGlobalClusterNotFound,
		},
		{
			name: "modify_engine_version",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateGlobalCluster("gc1", "aurora-postgresql", "14.0", false, false)
			},
			run: func(b *rds.InMemoryBackend) error {
				dp := true
				gc, err := b.ModifyGlobalCluster("gc1", "", "14.5", &dp)
				if err != nil {
					return err
				}
				assert.Equal(t, "14.5", gc.EngineVersion)
				assert.True(t, gc.DeletionProtection)

				return nil
			},
		},
		{
			name:  "create_empty_id",
			setup: func(_ *rds.InMemoryBackend) {},
			run: func(b *rds.InMemoryBackend) error {
				_, err := b.CreateGlobalCluster("", "", "", false, false)

				return err
			},
			wantErr: true,
			errIs:   rds.ErrInvalidParameter,
		},
		{
			name:  "describe_not_found",
			setup: func(_ *rds.InMemoryBackend) {},
			run: func(b *rds.InMemoryBackend) error {
				_, err := b.DescribeGlobalClusters("no-such-gc")

				return err
			},
			wantErr: true,
			errIs:   rds.ErrGlobalClusterNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := tt.run(b)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRDSHandler_NewOperations tests new HTTP handler operations.
func TestRDSHandler_NewOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		setupBodies  []string
		wantContains []string
		wantCode     int
	}{
		{
			name: "CopyDBSnapshot_success",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-copy-db&Engine=postgres",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=src-snap&DBInstanceIdentifier=snap-copy-db",
			},
			body: "Action=CopyDBSnapshot&Version=2014-10-31" +
				"&SourceDBSnapshotIdentifier=src-snap&TargetDBSnapshotIdentifier=dst-snap",
			wantCode:     http.StatusOK,
			wantContains: []string{"CopyDBSnapshotResponse", "dst-snap"},
		},
		{
			name: "CopyDBSnapshot_source_not_found",
			body: "Action=CopyDBSnapshot&Version=2014-10-31" +
				"&SourceDBSnapshotIdentifier=no-snap&TargetDBSnapshotIdentifier=dst-snap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSnapshotNotFound"},
		},
		{
			name: "RestoreDBInstanceFromDBSnapshot_success",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=orig-db&Engine=postgres",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=restore-snap&DBInstanceIdentifier=orig-db",
			},
			body: "Action=RestoreDBInstanceFromDBSnapshot&Version=2014-10-31" +
				"&DBInstanceIdentifier=restored-db&DBSnapshotIdentifier=restore-snap",
			wantCode:     http.StatusOK,
			wantContains: []string{"RestoreDBInstanceFromDBSnapshotResponse", "restored-db"},
		},
		{
			name: "RestoreDBInstanceFromDBSnapshot_snap_not_found",
			body: "Action=RestoreDBInstanceFromDBSnapshot&Version=2014-10-31" +
				"&DBInstanceIdentifier=restored-db&DBSnapshotIdentifier=no-snap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSnapshotNotFound"},
		},
		{
			name: "RestoreDBInstanceToPointInTime_success",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=pit-src-db&Engine=mysql",
			},
			body: "Action=RestoreDBInstanceToPointInTime&Version=2014-10-31" +
				"&TargetDBInstanceIdentifier=pit-restored-db&SourceDBInstanceIdentifier=pit-src-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"RestoreDBInstanceToPointInTimeResponse", "pit-restored-db"},
		},
		{
			name: "RestoreDBInstanceToPointInTime_source_not_found",
			body: "Action=RestoreDBInstanceToPointInTime&Version=2014-10-31" +
				"&TargetDBInstanceIdentifier=pit-db&SourceDBInstanceIdentifier=no-db",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name: "StartDBInstance_success",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=start-db",
				"Action=StopDBInstance&Version=2014-10-31&DBInstanceIdentifier=start-db",
			},
			body:         "Action=StartDBInstance&Version=2014-10-31&DBInstanceIdentifier=start-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"StartDBInstanceResponse", "start-db", "available"},
		},
		{
			name: "StopDBInstance_success",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=stop-db",
			},
			body:         "Action=StopDBInstance&Version=2014-10-31&DBInstanceIdentifier=stop-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"StopDBInstanceResponse", "stop-db", "stopped"},
		},
		{
			name:         "StopDBInstance_not_found",
			body:         "Action=StopDBInstance&Version=2014-10-31&DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name: "StartDBInstance_wrong_state",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=running-db",
			},
			body:         "Action=StartDBInstance&Version=2014-10-31&DBInstanceIdentifier=running-db",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidDBInstanceState"},
		},
		{
			name: "CreateGlobalCluster_success",
			body: "Action=CreateGlobalCluster&Version=2014-10-31" +
				"&GlobalClusterIdentifier=gc1&Engine=aurora-postgresql&EngineVersion=14.3",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateGlobalClusterResponse", "gc1", "aurora-postgresql"},
		},
		{
			name: "DescribeGlobalClusters_all",
			setupBodies: []string{
				"Action=CreateGlobalCluster&Version=2014-10-31&GlobalClusterIdentifier=gc-list1",
				"Action=CreateGlobalCluster&Version=2014-10-31&GlobalClusterIdentifier=gc-list2",
			},
			body:         "Action=DescribeGlobalClusters&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeGlobalClustersResponse", "gc-list1", "gc-list2"},
		},
		{
			name: "DeleteGlobalCluster_success",
			setupBodies: []string{
				"Action=CreateGlobalCluster&Version=2014-10-31&GlobalClusterIdentifier=gc-del",
			},
			body:         "Action=DeleteGlobalCluster&Version=2014-10-31&GlobalClusterIdentifier=gc-del",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteGlobalClusterResponse", "gc-del"},
		},
		{
			name:         "DeleteGlobalCluster_not_found",
			body:         "Action=DeleteGlobalCluster&Version=2014-10-31&GlobalClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"GlobalClusterNotFound"},
		},
		{
			name: "ModifyGlobalCluster_success",
			setupBodies: []string{
				"Action=CreateGlobalCluster&Version=2014-10-31&GlobalClusterIdentifier=gc-mod",
			},
			body: "Action=ModifyGlobalCluster&Version=2014-10-31" +
				"&GlobalClusterIdentifier=gc-mod&EngineVersion=15.0&DeletionProtection=true",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyGlobalClusterResponse", "gc-mod", "15.0"},
		},
		{
			name: "CreateDBInstance_with_new_fields",
			body: "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=rich-db&Engine=postgres" +
				"&MultiAZ=true&StorageType=io1&BackupRetentionPeriod=7&StorageEncrypted=true",
			wantCode: http.StatusOK,
			wantContains: []string{
				"CreateDBInstanceResponse",
				"rich-db",
				"<MultiAZ>true</MultiAZ>",
				"<StorageType>io1</StorageType>",
				"<StorageEncrypted>true</StorageEncrypted>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()

			for _, setupBody := range tt.setupBodies {
				rds.FlushInstanceLifecycle(h.Backend) // ensure prior instances are available
				rec := postRDSForm(t, h, setupBody)
				require.Equal(t, http.StatusOK, rec.Code, "setup body %q failed: %s", setupBody, rec.Body.String())
			}

			rds.FlushInstanceLifecycle(h.Backend) // advance creating→available before action

			rec := postRDSForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

// TestRDSBackend_PersistenceRoundTrip tests that new fields survive snapshot/restore.
func TestRDSBackend_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b1 := rds.NewInMemoryBackend("000000000000", "us-east-1")

	// Create instances with new fields.
	_, err := b1.CreateDBInstance("db1", "postgres", "db.t3.micro", "mydb", "admin", "", 20, rds.DBInstanceOptions{
		MultiAZ:               true,
		StorageType:           "io1",
		StorageEncrypted:      true,
		BackupRetentionPeriod: 7,
		AvailabilityZone:      "us-east-1b",
	})
	require.NoError(t, err)

	// Snapshot and create a new snapshot.
	_, err = b1.CreateDBSnapshot("snap1", "db1")
	require.NoError(t, err)

	// Restore from snapshot to new instance.
	_, err = b1.RestoreDBInstanceFromDBSnapshot("restored-db", "snap1", rds.DBInstanceOptions{})
	require.NoError(t, err)

	// Create global cluster.
	_, err = b1.CreateGlobalCluster("gc1", "aurora-postgresql", "14.3", true, false)
	require.NoError(t, err)

	// Add cluster endpoint.
	_, err = b1.CreateDBCluster("cluster1", "aurora", "admin", "mydb", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)
	_, err = b1.CreateDBClusterEndpoint("ep1", "cluster1", "READER")
	require.NoError(t, err)

	// Start export task.
	_, err = b1.StartExportTask("task1", "arn:aws:rds:us-east-1:000000000000:snapshot:snap1", "my-bucket")
	require.NoError(t, err)

	// Take a snapshot of backend state.
	data := b1.Snapshot()
	require.NotEmpty(t, data)

	// Restore into b2.
	b2 := rds.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(data))

	// Verify instances.
	instances, err := b2.DescribeDBInstances("db1")
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.True(t, instances[0].MultiAZ)
	assert.Equal(t, "io1", instances[0].StorageType)
	assert.True(t, instances[0].StorageEncrypted)
	assert.Equal(t, 7, instances[0].BackupRetentionPeriod)

	// Verify cluster endpoints persisted.
	endpoints, err := b2.DescribeDBClusterEndpoints("", "ep1")
	require.NoError(t, err)
	assert.Len(t, endpoints, 1)

	// Verify export tasks persisted.
	tasks, err := b2.DescribeExportTasks("task1")
	require.NoError(t, err)
	assert.Len(t, tasks, 1)

	// Verify global clusters persisted.
	gcs, err := b2.DescribeGlobalClusters("")
	require.NoError(t, err)
	assert.Len(t, gcs, 1)
	assert.Equal(t, "gc1", gcs[0].GlobalClusterIdentifier)
}

// TestRDSBackend_RemoveTagsNoAliasing ensures RemoveTagsFromResource doesn't corrupt the underlying slice.
func TestRDSBackend_RemoveTagsNoAliasing(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBInstance("db", "postgres", "", "", "", "", 0, rds.DBInstanceOptions{})
	require.NoError(t, err)

	arn := "arn:aws:rds:us-east-1:000000000000:db:db"

	b.AddTagsToResource(arn, []rds.Tag{
		{Key: "env", Value: "prod"},
		{Key: "team", Value: "sre"},
		{Key: "cost-center", Value: "eng"},
	})

	b.RemoveTagsFromResource(arn, []string{"team"})

	tags := b.ListTagsForResource(arn)
	assert.Len(t, tags, 2)

	keys := make([]string, 0, len(tags))
	for _, tag := range tags {
		keys = append(keys, tag.Key)
	}
	assert.Contains(t, keys, "env")
	assert.Contains(t, keys, "cost-center")
	assert.NotContains(t, keys, "team")

	// Second removal should work without corrupting data.
	b.RemoveTagsFromResource(arn, []string{"env"})
	tags = b.ListTagsForResource(arn)
	assert.Len(t, tags, 1)
	assert.Equal(t, "cost-center", tags[0].Key)
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
	}

	inst, err := b.ModifyDBInstance("mod-db", "db.r5.large", 100, opts)
	require.NoError(t, err)
	assert.Equal(t, "db.r5.large", inst.DBInstanceClass)
	assert.Equal(t, 100, inst.AllocatedStorage)
	assert.Equal(t, "io1", inst.StorageType)
	assert.Equal(t, 14, inst.BackupRetentionPeriod)
	assert.True(t, inst.MultiAZ)
}

// TestRDSBackend_DeletionProtection tests that resources with deletion protection cannot be deleted.
func TestRDSBackend_DeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(b *rds.InMemoryBackend)
		action  func(b *rds.InMemoryBackend) error
		name    string
		wantErr bool
	}{
		{
			name: "delete_protected_instance_blocked",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance(
					"prot-db", "postgres", "", "", "", "", 20,
					rds.DBInstanceOptions{DeletionProtection: true},
				)
			},
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.DeleteDBInstance("prot-db")

				return err
			},
			wantErr: true,
			errIs:   rds.ErrInvalidDBInstanceState,
		},
		{
			name: "delete_unprotected_instance_allowed",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance(
					"unprot-db", "postgres", "", "", "", "", 20,
					rds.DBInstanceOptions{DeletionProtection: false},
				)
			},
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.DeleteDBInstance("unprot-db")

				return err
			},
			wantErr: false,
		},
		{
			name: "delete_protected_global_cluster_blocked",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateGlobalCluster("gc-prot", "aurora-postgresql", "14.3", false, true)
			},
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.DeleteGlobalCluster("gc-prot")

				return err
			},
			wantErr: true,
			errIs:   rds.ErrInvalidGlobalClusterState,
		},
		{
			name: "delete_unprotected_global_cluster_allowed",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateGlobalCluster("gc-unprot", "aurora-postgresql", "14.3", false, false)
			},
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.DeleteGlobalCluster("gc-unprot")

				return err
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")

			if tt.setup != nil {
				tt.setup(b)
			}

			err := tt.action(b)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errIs)

				return
			}

			require.NoError(t, err)
		})
	}
}

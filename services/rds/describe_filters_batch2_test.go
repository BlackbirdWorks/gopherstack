package rds_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestDescribeBlueGreenDeployments_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	_, err := client.CreateBlueGreenDeployment(t.Context(), &rdssdk.CreateBlueGreenDeploymentInput{
		BlueGreenDeploymentName: aws.String("flt-bgd-a"),
		Source:                  aws.String("flt-src-a"),
	})
	require.NoError(t, err)
	_, err = client.CreateBlueGreenDeployment(t.Context(), &rdssdk.CreateBlueGreenDeploymentInput{
		BlueGreenDeploymentName: aws.String("flt-bgd-b"),
		Source:                  aws.String("flt-src-b"),
	})
	require.NoError(t, err)

	cases := []struct {
		name       string
		filterName string
		value      string
		wantName   string
	}{
		{
			"blue-green-deployment-identifier narrows",
			"blue-green-deployment-identifier",
			"bgd-flt-bgd-a",
			"flt-bgd-a",
		},
		{
			"blue-green-deployment-name narrows",
			"blue-green-deployment-name",
			"flt-bgd-b",
			"flt-bgd-b",
		},
		{"source narrows", "source", "flt-src-a", "flt-bgd-a"},
		{"target narrows", "target", "flt-src-b-green", "flt-bgd-b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, filterErr := client.DescribeBlueGreenDeployments(
				t.Context(),
				&rdssdk.DescribeBlueGreenDeploymentsInput{
					Filters: []types.Filter{
						{Name: aws.String(tc.filterName), Values: []string{tc.value}},
					},
				},
			)
			require.NoError(t, filterErr)
			require.Len(t, out.BlueGreenDeployments, 1)
			assert.Equal(
				t,
				tc.wantName,
				aws.ToString(out.BlueGreenDeployments[0].BlueGreenDeploymentName),
			)
		})
	}

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, filterErr := client.DescribeBlueGreenDeployments(
			t.Context(),
			&rdssdk.DescribeBlueGreenDeploymentsInput{
				Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
			},
		)
		wantInvalidParameterValue(t, filterErr)
	})
}

// TestDescribeDBClusterAutomatedBackups_Filters exercises the write path
// entirely through the real client: CreateDBCluster with
// BackupRetentionPeriod>0 now registers a cluster automated backup itself
// (db_clusters.go, gopherstack-qpxye), so no InMemoryBackend seam is needed
// here any more.
func TestDescribeDBClusterAutomatedBackups_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier:   aws.String("flt-cab-a"),
		Engine:                aws.String("aurora-mysql"),
		MasterUsername:        aws.String("admin"),
		BackupRetentionPeriod: aws.Int32(7),
	})
	require.NoError(t, err)
	_, err = client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier:   aws.String("flt-cab-b"),
		Engine:                aws.String("aurora-mysql"),
		MasterUsername:        aws.String("admin"),
		BackupRetentionPeriod: aws.Int32(7),
	})
	require.NoError(t, err)

	t.Run("db-cluster-id narrows to matching backup", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBClusterAutomatedBackups(
			t.Context(), &rdssdk.DescribeDBClusterAutomatedBackupsInput{
				Filters: []types.Filter{
					{Name: aws.String("db-cluster-id"), Values: []string{"flt-cab-a"}},
				},
			})
		require.NoError(t, filterErr)
		require.Len(t, out.DBClusterAutomatedBackups, 1)
		assert.Equal(
			t,
			"flt-cab-a",
			aws.ToString(out.DBClusterAutomatedBackups[0].DBClusterIdentifier),
		)
	})

	t.Run("db-cluster-resource-id narrows to matching backup", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBClusterAutomatedBackups(
			t.Context(), &rdssdk.DescribeDBClusterAutomatedBackupsInput{
				Filters: []types.Filter{
					{
						Name:   aws.String("db-cluster-resource-id"),
						Values: []string{"cluster-flt-cab-b"},
					},
				},
			})
		require.NoError(t, filterErr)
		require.Len(t, out.DBClusterAutomatedBackups, 1)
		assert.Equal(
			t,
			"flt-cab-b",
			aws.ToString(out.DBClusterAutomatedBackups[0].DBClusterIdentifier),
		)
	})

	t.Run("status filter excludes backups with a different status", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBClusterAutomatedBackups(
			t.Context(), &rdssdk.DescribeDBClusterAutomatedBackupsInput{
				Filters: []types.Filter{{Name: aws.String("status"), Values: []string{"retained"}}},
			})
		require.NoError(t, filterErr)
		assert.Empty(
			t,
			out.DBClusterAutomatedBackups,
			"both backups are status=available, so a retained filter must exclude them",
		)
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, filterErr := client.DescribeDBClusterAutomatedBackups(
			t.Context(), &rdssdk.DescribeDBClusterAutomatedBackupsInput{
				Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
			})
		wantInvalidParameterValue(t, filterErr)
	})
}

func TestDescribeDBInstanceAutomatedBackups_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier:  aws.String("flt-iab-a"),
		DBInstanceClass:       aws.String("db.t3.micro"),
		Engine:                aws.String("postgres"),
		BackupRetentionPeriod: aws.Int32(7),
	})
	require.NoError(t, err)
	_, err = client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier:  aws.String("flt-iab-b"),
		DBInstanceClass:       aws.String("db.t3.micro"),
		Engine:                aws.String("postgres"),
		BackupRetentionPeriod: aws.Int32(7),
	})
	require.NoError(t, err)

	t.Run("db-instance-id narrows to matching backup", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBInstanceAutomatedBackups(
			t.Context(), &rdssdk.DescribeDBInstanceAutomatedBackupsInput{
				Filters: []types.Filter{
					{Name: aws.String("db-instance-id"), Values: []string{"flt-iab-a"}},
				},
			})
		require.NoError(t, filterErr)
		require.Len(t, out.DBInstanceAutomatedBackups, 1)
		assert.Equal(
			t,
			"flt-iab-a",
			aws.ToString(out.DBInstanceAutomatedBackups[0].DBInstanceIdentifier),
		)
	})

	t.Run("dbi-resource-id narrows to matching backup", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBInstanceAutomatedBackups(
			t.Context(), &rdssdk.DescribeDBInstanceAutomatedBackupsInput{
				Filters: []types.Filter{
					{Name: aws.String("dbi-resource-id"), Values: []string{"flt-iab-b"}},
				},
			})
		require.NoError(t, filterErr)
		require.Len(t, out.DBInstanceAutomatedBackups, 1)
		assert.Equal(
			t,
			"flt-iab-b",
			aws.ToString(out.DBInstanceAutomatedBackups[0].DBInstanceIdentifier),
		)
	})

	t.Run("status filter excludes backups with a different status", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBInstanceAutomatedBackups(
			t.Context(), &rdssdk.DescribeDBInstanceAutomatedBackupsInput{
				Filters: []types.Filter{{Name: aws.String("status"), Values: []string{"retained"}}},
			})
		require.NoError(t, filterErr)
		assert.Empty(
			t,
			out.DBInstanceAutomatedBackups,
			"both backups are status=available, so a retained filter must exclude them",
		)
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, filterErr := client.DescribeDBInstanceAutomatedBackups(
			t.Context(), &rdssdk.DescribeDBInstanceAutomatedBackupsInput{
				Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
			})
		wantInvalidParameterValue(t, filterErr)
	})
}

// TestDescribeDBClusterBacktracks_Filters exercises the write path through
// the real client: BacktrackDBCluster now persists the DBClusterBacktrack it
// builds (db_clusters.go, gopherstack-qpxye), so DescribeDBClusterBacktracks
// and its Filters contract can narrow real records instead of an always-empty
// list.
func TestDescribeDBClusterBacktracks_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("flt-backtrack-clu"),
		Engine:              aws.String("aurora-mysql"),
		MasterUsername:      aws.String("admin"),
	})
	require.NoError(t, err)

	btA, err := client.BacktrackDBCluster(t.Context(), &rdssdk.BacktrackDBClusterInput{
		DBClusterIdentifier: aws.String("flt-backtrack-clu"),
		BacktrackTo:         aws.Time(time.Unix(1_700_000_000, 0)),
	})
	require.NoError(t, err)
	_, err = client.BacktrackDBCluster(t.Context(), &rdssdk.BacktrackDBClusterInput{
		DBClusterIdentifier: aws.String("flt-backtrack-clu"),
		BacktrackTo:         aws.Time(time.Unix(1_700_000_100, 0)),
	})
	require.NoError(t, err)

	t.Run("db-cluster-backtrack-id narrows to matching backtrack", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBClusterBacktracks(
			t.Context(), &rdssdk.DescribeDBClusterBacktracksInput{
				DBClusterIdentifier: aws.String("flt-backtrack-clu"),
				Filters: []types.Filter{
					{
						Name:   aws.String("db-cluster-backtrack-id"),
						Values: []string{aws.ToString(btA.BacktrackIdentifier)},
					},
				},
			})
		require.NoError(t, filterErr)
		require.Len(t, out.DBClusterBacktracks, 1)
		assert.Equal(
			t,
			aws.ToString(btA.BacktrackIdentifier),
			aws.ToString(out.DBClusterBacktracks[0].BacktrackIdentifier),
		)
	})

	t.Run("db-cluster-backtrack-status narrows to matching backtrack", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBClusterBacktracks(
			t.Context(), &rdssdk.DescribeDBClusterBacktracksInput{
				DBClusterIdentifier: aws.String("flt-backtrack-clu"),
				Filters: []types.Filter{
					{Name: aws.String("db-cluster-backtrack-status"), Values: []string{"applying"}},
				},
			})
		require.NoError(t, filterErr)
		assert.Len(t, out.DBClusterBacktracks, 2, "both backtracks start in applying status")
	})

	t.Run("no filters returns both backtracks", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBClusterBacktracks(
			t.Context(), &rdssdk.DescribeDBClusterBacktracksInput{
				DBClusterIdentifier: aws.String("flt-backtrack-clu"),
			})
		require.NoError(t, filterErr)
		assert.Len(t, out.DBClusterBacktracks, 2)
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, filterErr := client.DescribeDBClusterBacktracks(
			t.Context(),
			&rdssdk.DescribeDBClusterBacktracksInput{
				DBClusterIdentifier: aws.String("flt-backtrack-clu"),
				Filters:             []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
			},
		)
		wantInvalidParameterValue(t, filterErr)
	})
}

// TestDescribeDBRecommendations_Filters seeds recommendations through
// InMemoryBackend.AddDBRecommendation directly: unlike the other ops in this
// batch, this isn't a gopherstack completeness gap -- AWS itself has no
// CreateDBRecommendation operation (recommendations are generated by RDS,
// not created by a client), so AddDBRecommendation's doc comment
// (recommendations.go) calling it a test seam matches AWS's own read-only
// structure for this resource.
func TestDescribeDBRecommendations_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	h.Backend.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "flt-rec-a",
		Status:           "active",
		Severity:         "high",
		TypeID:           "type-a",
		ResourceARN:      "arn:aws:rds:us-east-1:123456789012:db:flt-inst-a",
	})
	h.Backend.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "flt-rec-b",
		Status:           "dismissed",
		Severity:         "low",
		TypeID:           "type-b",
		ResourceARN:      "arn:aws:rds:us-east-1:123456789012:db:flt-inst-b",
	})

	cases := []struct {
		name       string
		filterName string
		value      string
		wantID     string
	}{
		{"recommendation-id narrows", "recommendation-id", "flt-rec-a", "flt-rec-a"},
		{"status narrows", "status", "dismissed", "flt-rec-b"},
		{"severity narrows", "severity", "high", "flt-rec-a"},
		{"type-id narrows", "type-id", "type-b", "flt-rec-b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, filterErr := client.DescribeDBRecommendations(
				t.Context(),
				&rdssdk.DescribeDBRecommendationsInput{
					Filters: []types.Filter{
						{Name: aws.String(tc.filterName), Values: []string{tc.value}},
					},
				},
			)
			require.NoError(t, filterErr)
			require.Len(t, out.DBRecommendations, 1)
			assert.Equal(t, tc.wantID, aws.ToString(out.DBRecommendations[0].RecommendationId))
		})
	}

	t.Run("dbi-resource-id filter is accepted but does not narrow", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBRecommendations(
			t.Context(),
			&rdssdk.DescribeDBRecommendationsInput{
				Filters: []types.Filter{
					{Name: aws.String("dbi-resource-id"), Values: []string{"flt-inst-a"}},
				},
			},
		)
		require.NoError(t, filterErr)
		assert.Len(t, out.DBRecommendations, 2)
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, filterErr := client.DescribeDBRecommendations(
			t.Context(),
			&rdssdk.DescribeDBRecommendationsInput{
				Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
			},
		)
		wantInvalidParameterValue(t, filterErr)
	})
}

// TestDescribeDBSnapshotTenantDatabases_Filters exercises the write path
// through the real client: CreateDBSnapshot now copies each tenant database
// on the snapshotted instance into DescribeDBSnapshotTenantDatabases data
// (tenant_databases.go, db_snapshots.go, gopherstack-qpxye), so no
// InMemoryBackend seam is needed here any more.
func TestDescribeDBSnapshotTenantDatabases_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	for _, tc := range []struct{ inst, snap, tdb string }{
		{"flt-inst-a", "flt-snap-a", "flt-tdb-a"},
		{"flt-inst-b", "flt-snap-b", "flt-tdb-b"},
	} {
		_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String(tc.inst),
			Engine:               aws.String("custom-oracle-ee-cdb"),
			DBInstanceClass:      aws.String("db.t3.micro"),
			MasterUsername:       aws.String("admin"),
			AllocatedStorage:     aws.Int32(20),
		})
		require.NoError(t, err)

		_, err = client.CreateTenantDatabase(t.Context(), &rdssdk.CreateTenantDatabaseInput{
			DBInstanceIdentifier: aws.String(tc.inst),
			TenantDBName:         aws.String(tc.tdb),
			MasterUsername:       aws.String("tenantadmin"),
		})
		require.NoError(t, err)

		_, err = client.CreateDBSnapshot(t.Context(), &rdssdk.CreateDBSnapshotInput{
			DBSnapshotIdentifier: aws.String(tc.snap),
			DBInstanceIdentifier: aws.String(tc.inst),
		})
		require.NoError(t, err)
	}

	t.Run("tenant-db-name narrows to matching entry", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBSnapshotTenantDatabases(
			t.Context(), &rdssdk.DescribeDBSnapshotTenantDatabasesInput{
				Filters: []types.Filter{
					{Name: aws.String("tenant-db-name"), Values: []string{"flt-tdb-a"}},
				},
			})
		require.NoError(t, filterErr)
		require.Len(t, out.DBSnapshotTenantDatabases, 1)
		assert.Equal(
			t,
			"flt-snap-a",
			aws.ToString(out.DBSnapshotTenantDatabases[0].DBSnapshotIdentifier),
		)
	})

	t.Run("db-instance-id narrows to matching entry", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBSnapshotTenantDatabases(
			t.Context(), &rdssdk.DescribeDBSnapshotTenantDatabasesInput{
				Filters: []types.Filter{
					{Name: aws.String("db-instance-id"), Values: []string{"flt-inst-b"}},
				},
			})
		require.NoError(t, filterErr)
		require.Len(t, out.DBSnapshotTenantDatabases, 1)
		assert.Equal(
			t,
			"flt-snap-b",
			aws.ToString(out.DBSnapshotTenantDatabases[0].DBSnapshotIdentifier),
		)
	})

	t.Run("db-snapshot-id narrows to matching entry", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBSnapshotTenantDatabases(
			t.Context(), &rdssdk.DescribeDBSnapshotTenantDatabasesInput{
				Filters: []types.Filter{
					{Name: aws.String("db-snapshot-id"), Values: []string{"flt-snap-a"}},
				},
			})
		require.NoError(t, filterErr)
		require.Len(t, out.DBSnapshotTenantDatabases, 1)
		assert.Equal(t, "flt-tdb-a", aws.ToString(out.DBSnapshotTenantDatabases[0].TenantDBName))
	})

	t.Run("snapshot-type filter is accepted but does not narrow", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeDBSnapshotTenantDatabases(
			t.Context(), &rdssdk.DescribeDBSnapshotTenantDatabasesInput{
				Filters: []types.Filter{
					{Name: aws.String("snapshot-type"), Values: []string{"manual"}},
				},
			})
		require.NoError(t, filterErr)
		assert.Len(t, out.DBSnapshotTenantDatabases, 2)
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, filterErr := client.DescribeDBSnapshotTenantDatabases(
			t.Context(), &rdssdk.DescribeDBSnapshotTenantDatabasesInput{
				Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
			})
		wantInvalidParameterValue(t, filterErr)
	})
}

// TestDescribeEngineDefaultParameters_UnknownFilterErrors is the only part
// of this op's Filters contract observable through the real client: this
// backend never generates default-parameter data for any family
// (parameter_groups.go's own comment on DescribeEngineDefaultParameters), so
// a narrowing assertion against real data is impossible here. The narrowing
// logic is the same applyDBParameterFilters already covered by
// TestDescribeDBParameters_Filters and TestDescribeDBClusterParameters_Filters
// (describe_filters_batch1_test.go) and directly by
// TestApplyDBParameterFilters_EngineDefaults (in-package,
// whitebox_filters_test.go).
func TestDescribeEngineDefaultParameters_UnknownFilterErrors(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	_, err := client.DescribeEngineDefaultParameters(
		t.Context(),
		&rdssdk.DescribeEngineDefaultParametersInput{
			DBParameterGroupFamily: aws.String("postgres15"),
			Filters: []types.Filter{
				{Name: aws.String("bogus"), Values: []string{"x"}},
			},
		},
	)
	wantInvalidParameterValue(t, err)
}

func TestDescribeTenantDatabases_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	_, err := client.CreateTenantDatabase(t.Context(), &rdssdk.CreateTenantDatabaseInput{
		DBInstanceIdentifier: aws.String("flt-tdb-inst-a"),
		TenantDBName:         aws.String("flt-tenant-a"),
		MasterUsername:       aws.String("admin"),
	})
	require.NoError(t, err)
	_, err = client.CreateTenantDatabase(t.Context(), &rdssdk.CreateTenantDatabaseInput{
		DBInstanceIdentifier: aws.String("flt-tdb-inst-b"),
		TenantDBName:         aws.String("flt-tenant-b"),
		MasterUsername:       aws.String("admin"),
	})
	require.NoError(t, err)

	t.Run("tenant-db-name narrows to matching tenant", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeTenantDatabases(
			t.Context(),
			&rdssdk.DescribeTenantDatabasesInput{
				Filters: []types.Filter{
					{Name: aws.String("tenant-db-name"), Values: []string{"flt-tenant-a"}},
				},
			},
		)
		require.NoError(t, filterErr)
		require.Len(t, out.TenantDatabases, 1)
		assert.Equal(t, "flt-tdb-inst-a", aws.ToString(out.TenantDatabases[0].DBInstanceIdentifier))
	})

	t.Run("dbi-resource-id narrows to matching tenant", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeTenantDatabases(
			t.Context(),
			&rdssdk.DescribeTenantDatabasesInput{
				Filters: []types.Filter{
					{
						Name:   aws.String("dbi-resource-id"),
						Values: []string{"db-flt-tdb-inst-b-flt-tenant-b"},
					},
				},
			},
		)
		require.NoError(t, filterErr)
		require.Len(t, out.TenantDatabases, 1)
		assert.Equal(t, "flt-tdb-inst-b", aws.ToString(out.TenantDatabases[0].DBInstanceIdentifier))
	})

	t.Run("tenant-database-resource-id filter is accepted but does not narrow", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeTenantDatabases(
			t.Context(),
			&rdssdk.DescribeTenantDatabasesInput{
				Filters: []types.Filter{
					{Name: aws.String("tenant-database-resource-id"), Values: []string{"tdbr-x"}},
				},
			},
		)
		require.NoError(t, filterErr)
		assert.Len(t, out.TenantDatabases, 2)
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, filterErr := client.DescribeTenantDatabases(
			t.Context(),
			&rdssdk.DescribeTenantDatabasesInput{
				Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
			},
		)
		wantInvalidParameterValue(t, filterErr)
	})
}

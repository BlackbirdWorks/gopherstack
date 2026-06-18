package dms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

// TestDeleteDataMigration_ARNIndexBounded verifies that creating and deleting data
// migrations by ARN keeps both the primary store and the ARN index in sync, so
// neither map grows without bound across repeated create/delete cycles.
func TestDeleteDataMigration_ARNIndexBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
	}{
		{name: "single", names: []string{"mig-a"}},
		{name: "many", names: []string{"mig-a", "mig-b", "mig-c"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const (
				accountID = "123456789012"
				region    = "us-east-1"
			)

			b := dms.NewInMemoryBackend(accountID, region)
			ctx := context.Background()

			arns := make([]string, 0, len(tc.names))

			for _, name := range tc.names {
				dm, err := b.CreateDataMigration(ctx, name, "", "full-load", "", "", 1, false, nil)
				require.NoError(t, err)
				arns = append(arns, dm.DataMigrationArn)
			}

			require.Equal(t, len(tc.names), b.DataMigrationCount(), "count before delete")
			require.Equal(t, len(tc.names), b.DataMigrationByARNCount(), "ARN index before delete")

			for _, arnStr := range arns {
				_, err := b.DeleteDataMigration(ctx, arnStr)
				require.NoError(t, err, "delete by ARN must succeed")
			}

			require.Equal(t, 0, b.DataMigrationCount(), "primary store must be empty after delete")
			require.Equal(t, 0, b.DataMigrationByARNCount(), "ARN index must be empty after delete — no leak")
		})
	}
}

// TestDeleteDataProvider_ARNIndexBounded verifies ARN index stays in sync
// when data providers are deleted by ARN.
func TestDeleteDataProvider_ARNIndexBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
	}{
		{name: "single", names: []string{"prov-a"}},
		{name: "many", names: []string{"prov-a", "prov-b", "prov-c"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const (
				accountID = "123456789012"
				region    = "us-east-1"
			)

			b := dms.NewInMemoryBackend(accountID, region)
			ctx := context.Background()

			arns := make([]string, 0, len(tc.names))

			for _, name := range tc.names {
				dp, err := b.CreateDataProvider(ctx, name, "mysql", "", nil)
				require.NoError(t, err)
				arns = append(arns, dp.DataProviderArn)
			}

			require.Equal(t, len(tc.names), b.DataProviderCount(), "count before delete")
			require.Equal(t, len(tc.names), b.DataProviderByARNCount(), "ARN index before delete")

			for _, arnStr := range arns {
				_, err := b.DeleteDataProvider(ctx, arnStr)
				require.NoError(t, err, "delete by ARN must succeed")
			}

			require.Equal(t, 0, b.DataProviderCount(), "primary store must be empty after delete")
			require.Equal(t, 0, b.DataProviderByARNCount(), "ARN index must be empty after delete — no leak")
		})
	}
}

// TestDeleteInstanceProfile_ARNIndexBounded verifies ARN index stays in sync
// when instance profiles are deleted by ARN.
func TestDeleteInstanceProfile_ARNIndexBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
	}{
		{name: "single", names: []string{"profile-a"}},
		{name: "many", names: []string{"profile-a", "profile-b", "profile-c"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const (
				accountID = "123456789012"
				region    = "us-east-1"
			)

			b := dms.NewInMemoryBackend(accountID, region)
			ctx := context.Background()

			arns := make([]string, 0, len(tc.names))

			for _, name := range tc.names {
				ip, err := b.CreateInstanceProfile(ctx, name, "", "", "", "", "", false, nil)
				require.NoError(t, err)
				arns = append(arns, ip.InstanceProfileArn)
			}

			require.Equal(t, len(tc.names), b.InstanceProfileCount(), "count before delete")
			require.Equal(t, len(tc.names), b.InstanceProfileByARNCount(), "ARN index before delete")

			for _, arnStr := range arns {
				err := b.DeleteInstanceProfile(ctx, arnStr)
				require.NoError(t, err, "delete by ARN must succeed")
			}

			require.Equal(t, 0, b.InstanceProfileCount(), "primary store must be empty after delete")
			require.Equal(t, 0, b.InstanceProfileByARNCount(), "ARN index must be empty after delete — no leak")
		})
	}
}

// TestDeleteFleetAdvisorCollector_IDIndexBounded verifies that deleting a Fleet
// Advisor collector by its UUID (CollectorReferencedID) removes it from both the
// primary name store and the ID index, keeping both maps bounded.
func TestDeleteFleetAdvisorCollector_IDIndexBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
	}{
		{name: "single", names: []string{"col-a"}},
		{name: "many", names: []string{"col-a", "col-b", "col-c"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const (
				accountID = "123456789012"
				region    = "us-east-1"
			)

			b := dms.NewInMemoryBackend(accountID, region)
			ctx := context.Background()

			ids := make([]string, 0, len(tc.names))

			for _, name := range tc.names {
				col, err := b.CreateFleetAdvisorCollector(ctx, name, "", "", "")
				require.NoError(t, err)
				ids = append(ids, col.CollectorReferencedID)
			}

			require.Equal(t, len(tc.names), b.FleetAdvisorCollectorCount(), "count before delete")
			require.Equal(t, len(tc.names), b.FleetAdvisorCollectorByIDCount(), "ID index before delete")

			for _, id := range ids {
				err := b.DeleteFleetAdvisorCollector(ctx, id)
				require.NoError(t, err, "delete by ID must succeed")
			}

			require.Equal(t, 0, b.FleetAdvisorCollectorCount(), "primary store must be empty after delete")
			require.Equal(t, 0, b.FleetAdvisorCollectorByIDCount(), "ID index must be empty after delete — no leak")
		})
	}
}

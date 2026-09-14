package dms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

// TestInMemoryBackend_TagsSurviveRestore covers gopherstack-3aw8x: Tags is
// json:"-" on every taggable DMS resource struct, and reinitTagsLocked used
// to leave every restored value with a freshly created, EMPTY Tags
// container, so all tags were lost across a restart. Each subtest tags a
// resource with two tags, removes one, snapshots, restores into a fresh
// backend, and checks ListTagsForResource reflects exactly the survivor.
//
// Covers the 9 resource kinds reachable through AddTagsToResource/
// ListTagsForResource/RemoveTagsFromResource (see findResourceTags in
// tags.go). EventSubscription and FleetAdvisorCollector also carry a Tags
// field but have no ARN and are not addressable through that generic API in
// this backend or in real AWS -- see resourceTagsSnapshot in persistence.go.
func TestInMemoryBackend_TagsSurviveRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, b *dms.InMemoryBackend) string
		name   string
	}{
		{
			name: "replicationinstance",
			create: func(t *testing.T, b *dms.InMemoryBackend) string {
				t.Helper()

				ri, err := b.CreateReplicationInstance(
					t.Context(), "ri-1", "dms.t3.micro", "", "", 0,
					false, false, false, nil, dms.ReplicationInstanceSettings{},
				)
				require.NoError(t, err)

				return ri.ReplicationInstanceArn
			},
		},
		{
			name: "endpoint",
			create: func(t *testing.T, b *dms.InMemoryBackend) string {
				t.Helper()

				ep, err := b.CreateEndpoint(
					t.Context(), "ep-1", "source", "mysql", "", "", "", "", 0,
					nil, dms.EndpointConnectionSettings{},
				)
				require.NoError(t, err)

				return ep.EndpointArn
			},
		},
		{
			name: "replicationtask",
			create: func(t *testing.T, b *dms.InMemoryBackend) string {
				t.Helper()

				ctx := t.Context()

				ri, err := b.CreateReplicationInstance(
					ctx, "ri-task", "dms.t3.micro", "", "", 0,
					false, false, false, nil, dms.ReplicationInstanceSettings{},
				)
				require.NoError(t, err)

				src, err := b.CreateEndpoint(
					ctx, "ep-src", "source", "mysql", "", "", "", "", 0,
					nil, dms.EndpointConnectionSettings{},
				)
				require.NoError(t, err)

				tgt, err := b.CreateEndpoint(
					ctx, "ep-tgt", "target", "mysql", "", "", "", "", 0,
					nil, dms.EndpointConnectionSettings{},
				)
				require.NoError(t, err)

				rt, err := b.CreateReplicationTask(
					ctx, "task-1", src.EndpointArn, tgt.EndpointArn, ri.ReplicationInstanceArn,
					"full-load", "", "", nil, dms.ReplicationTaskCDCSettings{},
				)
				require.NoError(t, err)

				return rt.ReplicationTaskArn
			},
		},
		{
			name: "datamigration",
			create: func(t *testing.T, b *dms.InMemoryBackend) string {
				t.Helper()

				dm, err := b.CreateDataMigration(t.Context(), "dm-1", "", "full-load", "", "", 0, false, nil)
				require.NoError(t, err)

				return dm.DataMigrationArn
			},
		},
		{
			name: "dataprovider",
			create: func(t *testing.T, b *dms.InMemoryBackend) string {
				t.Helper()

				dp, err := b.CreateDataProvider(t.Context(), "dp-1", "mysql", "", nil)
				require.NoError(t, err)

				return dp.DataProviderArn
			},
		},
		{
			name: "instanceprofile",
			create: func(t *testing.T, b *dms.InMemoryBackend) string {
				t.Helper()

				ip, err := b.CreateInstanceProfile(t.Context(), "ip-1", "", "", "", "", "", false, nil)
				require.NoError(t, err)

				return ip.InstanceProfileArn
			},
		},
		{
			name: "migrationproject",
			create: func(t *testing.T, b *dms.InMemoryBackend) string {
				t.Helper()

				ctx := t.Context()

				ip, err := b.CreateInstanceProfile(ctx, "ip-mp", "", "", "", "", "", false, nil)
				require.NoError(t, err)

				dp, err := b.CreateDataProvider(ctx, "dp-mp", "mysql", "", nil)
				require.NoError(t, err)

				mp, err := b.CreateMigrationProject(ctx, "mp-1", "", ip.InstanceProfileName,
					[]dms.DataProviderDescriptorInput{{DataProviderIdentifier: dp.DataProviderName}},
					[]dms.DataProviderDescriptorInput{{DataProviderIdentifier: dp.DataProviderName}},
					nil)
				require.NoError(t, err)

				return mp.MigrationProjectArn
			},
		},
		{
			name: "replicationsubnetgroup",
			create: func(t *testing.T, b *dms.InMemoryBackend) string {
				t.Helper()

				sg, err := b.CreateReplicationSubnetGroup(t.Context(), "sg-1", "", "vpc-1", nil, nil)
				require.NoError(t, err)

				return sg.ReplicationSubnetGroupArn
			},
		},
		{
			name: "replicationconfig",
			create: func(t *testing.T, b *dms.InMemoryBackend) string {
				t.Helper()

				ctx := t.Context()

				src, err := b.CreateEndpoint(
					ctx, "ep-rc-src", "source", "mysql", "", "", "", "", 0,
					nil, dms.EndpointConnectionSettings{},
				)
				require.NoError(t, err)

				tgt, err := b.CreateEndpoint(
					ctx, "ep-rc-tgt", "target", "mysql", "", "", "", "", 0,
					nil, dms.EndpointConnectionSettings{},
				)
				require.NoError(t, err)

				maxCapacityUnits := int32(4)
				rc, err := b.CreateReplicationConfig(ctx, dms.CreateReplicationConfigParams{
					Identifier:        "rc-1",
					ReplicationType:   "full-load",
					SourceEndpointArn: src.EndpointArn,
					TargetEndpointArn: tgt.EndpointArn,
					TableMappings:     `{"rules":[]}`,
					ComputeConfig:     &dms.ComputeConfig{MaxCapacityUnits: &maxCapacityUnits},
				}, nil)
				require.NoError(t, err)

				return rc.ReplicationConfigArn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			b := dms.NewInMemoryBackend("123456789012", "us-east-1")
			resourceArn := tt.create(t, b)

			require.NoError(t, b.AddTagsToResource(ctx, resourceArn, map[string]string{"k1": "v1", "k2": "v2"}))
			require.NoError(t, b.RemoveTagsFromResource(ctx, resourceArn, []string{"k1"}))

			snap := b.Snapshot(ctx)
			require.NotNil(t, snap)

			fresh := dms.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, fresh.Restore(ctx, snap))

			got, err := fresh.ListTagsForResource(ctx, resourceArn)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"k2": "v2"}, got)
		})
	}
}

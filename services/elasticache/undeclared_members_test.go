package elasticache_test

import (
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_UndeclaredMembersAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  []url.Values
		action url.Values
		absent []string
		want   []string
	}{
		{
			name:  "replication_group_members",
			setup: nil,
			action: url.Values{
				"Action":                      {"CreateReplicationGroup"},
				"ReplicationGroupId":          {"rg"},
				"ReplicationGroupDescription": {"d"},
				"EngineVersion":               {"7.0"},
				"PreferredMaintenanceWindow":  {"sun:05:00-sun:06:00"},
			},
			absent: []string{"<EngineVersion>", "<CacheParameterGroupName>", "<PreferredMaintenanceWindow>"},
			want:   []string{"<ReplicationGroupId>rg</ReplicationGroupId>"},
		},
		{
			name: "replication_group_pending_values",
			setup: []url.Values{
				{
					"Action":                      {"CreateReplicationGroup"},
					"ReplicationGroupId":          {"rgp"},
					"ReplicationGroupDescription": {"d"},
				},
			},
			action: url.Values{
				"Action":             {"ModifyReplicationGroup"},
				"ReplicationGroupId": {"rgp"},
				"EngineVersion":      {"7.0.7"},
				"ApplyImmediately":   {"false"},
			},
			absent: []string{"<NumCacheNodes>", "<EngineVersion>"},
			want:   []string{"<ReplicationGroupId>rgp</ReplicationGroupId>"},
		},
		{
			name: "global_replication_group",
			setup: []url.Values{
				{
					"Action":                      {"CreateReplicationGroup"},
					"ReplicationGroupId":          {"prim"},
					"ReplicationGroupDescription": {"d"},
				},
			},
			action: url.Values{
				"Action":                         {"CreateGlobalReplicationGroup"},
				"GlobalReplicationGroupIdSuffix": {"g"},
				"PrimaryReplicationGroupId":      {"prim"},
			},
			absent: []string{"<NodeGroupCount>"},
			want:   []string{"<GlobalReplicationGroupId>"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srvURL := newRawFieldTestServer(t)
			for _, s := range tc.setup {
				postFormRaw(t, srvURL, s)
			}

			body := postFormRaw(t, srvURL, tc.action)
			for _, w := range tc.want {
				assert.Contains(t, body, w)
			}

			for _, a := range tc.absent {
				assert.NotContains(t, body, a)
			}
		})
	}
}

func TestSDK_ReplicationGroupRoundTripAfterTrim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "basic", id: "rt-basic"},
		{name: "second", id: "rt-second"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			ctx := t.Context()

			_, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId:          aws.String(tc.id),
				ReplicationGroupDescription: aws.String("d"),
			})
			require.NoError(t, err)

			_, err = client.ModifyReplicationGroup(ctx, &elasticachesdk.ModifyReplicationGroupInput{
				ReplicationGroupId: aws.String(tc.id),
				EngineVersion:      aws.String("7.0.7"),
				ApplyImmediately:   aws.Bool(false),
			})
			require.NoError(t, err)

			got, err := client.DescribeReplicationGroups(ctx, &elasticachesdk.DescribeReplicationGroupsInput{
				ReplicationGroupId: aws.String(tc.id),
			})
			require.NoError(t, err)
			require.Len(t, got.ReplicationGroups, 1)

			rg := got.ReplicationGroups[0]
			assert.Equal(t, tc.id, aws.ToString(rg.ReplicationGroupId))
			assert.NotEmpty(t, aws.ToString(rg.ARN))
			assert.NotEmpty(t, aws.ToString(rg.Status))
			assert.Nil(t, rg.PendingModifiedValues)
		})
	}
}

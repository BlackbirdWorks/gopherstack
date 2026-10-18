package timestreaminfluxdb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBParameterGroups(t *testing.T) {
	t.Parallel()

	t.Run("create get list", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		out, err := f.client.CreateDbParameterGroup(t.Context(), &sdk.CreateDbParameterGroupInput{
			Name: aws.String("tuned"), Description: aws.String("fast"),
			Parameters: &types.ParametersMemberInfluxDBv2{Value: types.InfluxDBv2Parameters{
				LogLevel: types.LogLevelDebug, QueryConcurrency: aws.Int32(10), NoTasks: aws.Bool(true),
				HttpIdleTimeout: &types.Duration{DurationType: types.DurationTypeSeconds, Value: aws.Int64(30)},
			}},
			Tags: map[string]string{"a": "b"},
		})
		require.NoError(t, err)
		assert.Regexp(t, `:db-parameter-group/`+aws.ToString(out.Id)+`$`, aws.ToString(out.Arn))

		got, err := f.client.GetDbParameterGroup(t.Context(), &sdk.GetDbParameterGroupInput{Identifier: out.Id})
		require.NoError(t, err)
		assert.Equal(t, "fast", aws.ToString(got.Description))

		v2 := got.Parameters.(*types.ParametersMemberInfluxDBv2).Value
		assert.Equal(t, types.LogLevelDebug, v2.LogLevel)
		assert.Equal(t, int32(10), aws.ToInt32(v2.QueryConcurrency))
		assert.True(t, aws.ToBool(v2.NoTasks))
		assert.Equal(t, int64(30), aws.ToInt64(v2.HttpIdleTimeout.Value))
		assert.Equal(t, types.DurationTypeSeconds, v2.HttpIdleTimeout.DurationType)

		list, err := f.client.ListDbParameterGroups(t.Context(), &sdk.ListDbParameterGroupsInput{})
		require.NoError(t, err)
		require.Len(t, list.Items, 1)
		assert.Equal(t, "tuned", aws.ToString(list.Items[0].Name))

		_, err = f.client.CreateDbParameterGroup(
			t.Context(),
			&sdk.CreateDbParameterGroupInput{Name: aws.String("TUNED")},
		)
		assert.Equal(t, "ConflictException", apiCode(err))

		_, err = f.client.GetDbParameterGroup(
			t.Context(),
			&sdk.GetDbParameterGroupInput{Identifier: aws.String("missing")},
		)
		assert.Equal(t, "ResourceNotFoundException", apiCode(err))
	})

	t.Run("v3 engines", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		_, err := f.client.CreateDbParameterGroup(t.Context(), &sdk.CreateDbParameterGroupInput{
			Name: aws.String("v3-core"),
			Parameters: &types.ParametersMemberInfluxDBv3Core{Value: types.InfluxDBv3CoreParameters{
				LogFilter: aws.String("debug"), QueryFileLimit: aws.Int32(100),
			}},
		})
		require.NoError(t, err)
	})

	t.Run("validation", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)

		for name, params := range map[string]types.Parameters{
			"negative duration": &types.ParametersMemberInfluxDBv2{Value: types.InfluxDBv2Parameters{
				HttpReadTimeout: &types.Duration{DurationType: types.DurationTypeHours, Value: aws.Int64(-1)},
			}},
		} {
			_, err := f.client.CreateDbParameterGroup(
				t.Context(),
				&sdk.CreateDbParameterGroupInput{Name: aws.String("param-bad"), Parameters: params},
			)
			assert.Equal(t, "ValidationException", apiCode(err), name)
		}

		_, err := f.client.CreateDbParameterGroup(t.Context(), &sdk.CreateDbParameterGroupInput{Name: aws.String("x")})
		assert.Equal(t, "ValidationException", apiCode(err))
	})
}

func TestDBBackups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "instance backup lifecycle", run: func(t *testing.T, f *fixture) {
			t.Helper()

			inst := f.instance(t, "source-db")
			out, err := f.client.CreateDbBackup(t.Context(), &sdk.CreateDbBackupInput{
				DbResourceId:  inst.Id,
				Name:          aws.String("nightly"),
				RetentionDays: aws.Int32(10),
				Tags:          map[string]string{"k": "v"},
			})
			require.NoError(t, err)
			assert.Equal(t, types.DbBackupStatusInProgress, out.Status)
			assert.Equal(t, types.DbBackupTypeOnDemand, out.Type)
			assert.Equal(t, aws.ToString(inst.Id), aws.ToString(out.DbResourceId))
			assert.Equal(t, int32(100), aws.ToInt32(out.AllocatedStorage))
			assert.Equal(t, types.EngineTypeInfluxdbV2, out.EngineType)
			assert.Equal(t, f.clock.now().AddDate(0, 0, 10).Format(time.RFC3339), aws.ToString(out.ExpiresAfter))
			assert.WithinDuration(t, f.clock.now(), aws.ToTime(out.CreatedAt), time.Second)

			f.settle()

			got, err := f.client.GetDbBackup(t.Context(), &sdk.GetDbBackupInput{Identifier: out.Id})
			require.NoError(t, err)
			assert.Equal(t, types.DbBackupStatusCompleted, got.Status)

			list, err := f.client.ListDbBackups(t.Context(), &sdk.ListDbBackupsInput{DbResourceId: inst.Id})
			require.NoError(t, err)
			require.Len(t, list.Items, 1)
			assert.Equal(t, "nightly", aws.ToString(list.Items[0].Name))

			other, err := f.client.ListDbBackups(
				t.Context(),
				&sdk.ListDbBackupsInput{DbResourceId: aws.String("other")},
			)
			require.NoError(t, err)
			assert.Empty(t, other.Items)

			del, err := f.client.DeleteDbBackup(t.Context(), &sdk.DeleteDbBackupInput{Identifier: out.Id})
			require.NoError(t, err)
			assert.Equal(t, types.DbBackupStatusDeleting, del.Status)
			f.settle()

			_, err = f.client.GetDbBackup(t.Context(), &sdk.GetDbBackupInput{Identifier: out.Id})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "errors", run: func(t *testing.T, f *fixture) {
			t.Helper()

			_, err := f.client.CreateDbBackup(
				t.Context(),
				&sdk.CreateDbBackupInput{DbResourceId: aws.String("missing"), Name: aws.String("bk-one")},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			creating, err := f.client.CreateDbInstance(t.Context(), instanceInput("busy-db"))
			require.NoError(t, err)

			_, err = f.client.CreateDbBackup(
				t.Context(),
				&sdk.CreateDbBackupInput{DbResourceId: creating.Id, Name: aws.String("bk-one")},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			f.settle()

			_, err = f.client.CreateDbBackup(
				t.Context(),
				&sdk.CreateDbBackupInput{
					DbResourceId:  creating.Id,
					Name:          aws.String("bk-one"),
					RetentionDays: aws.Int32(999),
				},
			)
			assert.Equal(t, "ValidationException", apiCode(err))

			bk, err := f.client.CreateDbBackup(
				t.Context(),
				&sdk.CreateDbBackupInput{DbResourceId: creating.Id, Name: aws.String("bk-one")},
			)
			require.NoError(t, err)

			_, err = f.client.CreateDbBackup(
				t.Context(),
				&sdk.CreateDbBackupInput{DbResourceId: creating.Id, Name: aws.String("BK-ONE")},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			_, err = f.client.DeleteDbBackup(t.Context(), &sdk.DeleteDbBackupInput{Identifier: bk.Id})
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "restore creates a new instance", run: func(t *testing.T, f *fixture) {
			t.Helper()

			inst := f.instance(t, "origin-db")
			bk, err := f.client.CreateDbBackup(
				t.Context(),
				&sdk.CreateDbBackupInput{DbResourceId: inst.Id, Name: aws.String("snap-one")},
			)
			require.NoError(t, err)

			_, err = f.client.RestoreFromDbBackup(
				t.Context(),
				&sdk.RestoreFromDbBackupInput{DbBackupId: bk.Id, Name: aws.String("clone-db")},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			f.settle()

			res, err := f.client.RestoreFromDbBackup(t.Context(), &sdk.RestoreFromDbBackupInput{
				DbBackupId: bk.Id,
				Name:       aws.String("clone-db"),
				Port:       aws.Int32(9000),
				Tags:       map[string]string{"restored": "yes"},
			})
			require.NoError(t, err)
			assert.Equal(t, types.RestoreStatusRestoring, res.RestoreStatus)
			assert.Equal(t, types.ResourceTypeDbInstance, res.ResourceType)
			assert.Equal(t, types.ResourceDeploymentTypeSingleAz, res.DeploymentType)
			assert.NotEqual(t, aws.ToString(inst.Id), aws.ToString(res.RestoredDbResourceId))

			got, err := f.client.GetDbInstance(
				t.Context(),
				&sdk.GetDbInstanceInput{Identifier: res.RestoredDbResourceId},
			)
			require.NoError(t, err)
			assert.Equal(t, types.StatusRestoring, got.Status)
			assert.Equal(t, "clone-db", aws.ToString(got.Name))
			assert.Equal(t, int32(9000), aws.ToInt32(got.Port))
			assert.Equal(t, int32(100), aws.ToInt32(got.AllocatedStorage))

			f.settle()

			got, err = f.client.GetDbInstance(
				t.Context(),
				&sdk.GetDbInstanceInput{Identifier: res.RestoredDbResourceId},
			)
			require.NoError(t, err)
			assert.Equal(t, types.StatusAvailable, got.Status)

			_, err = f.client.RestoreFromDbBackup(
				t.Context(),
				&sdk.RestoreFromDbBackupInput{DbBackupId: bk.Id, Name: aws.String("clone-db")},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			_, err = f.client.RestoreFromDbBackup(t.Context(), &sdk.RestoreFromDbBackupInput{
				DbBackupId: bk.Id, Name: aws.String("timed"), RestoreToTime: aws.Time(f.clock.now()),
			})
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
		{name: "restore replaces the existing resource", run: func(t *testing.T, f *fixture) {
			t.Helper()

			inst := f.instance(t, "replace-db")
			bk, err := f.client.CreateDbBackup(
				t.Context(),
				&sdk.CreateDbBackupInput{DbResourceId: inst.Id, Name: aws.String("snap-two")},
			)
			require.NoError(t, err)
			f.settle()

			_, err = f.client.UpdateDbInstance(
				t.Context(),
				&sdk.UpdateDbInstanceInput{Identifier: inst.Id, AllocatedStorage: aws.Int32(500)},
			)
			require.NoError(t, err)
			f.settle()

			res, err := f.client.RestoreFromDbBackup(t.Context(), &sdk.RestoreFromDbBackupInput{
				DbBackupId: bk.Id, Name: aws.String("ignored"), RestoreMode: types.RestoreModeReplaceExisting,
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(inst.Id), aws.ToString(res.RestoredDbResourceId))

			got, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: inst.Id})
			require.NoError(t, err)
			assert.Equal(t, types.StatusRestoring, got.Status)
			assert.Equal(t, int32(100), aws.ToInt32(got.AllocatedStorage))
		}},
		{name: "cluster backup and restore", run: func(t *testing.T, f *fixture) {
			t.Helper()

			id := f.cluster(t, "src-cluster")
			bk, err := f.client.CreateDbBackup(
				t.Context(),
				&sdk.CreateDbBackupInput{DbResourceId: aws.String(id), Name: aws.String("cluster-snap")},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ResourceDeploymentTypeMultiNodeReadReplicas, bk.DeploymentType)
			require.NotNil(t, bk.ClusterConfiguration)
			f.settle()

			res, err := f.client.RestoreFromDbBackup(
				t.Context(),
				&sdk.RestoreFromDbBackupInput{DbBackupId: bk.Id, Name: aws.String("dst-cluster")},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ResourceTypeDbCluster, res.ResourceType)

			c, err := f.client.GetDbCluster(t.Context(), &sdk.GetDbClusterInput{DbClusterId: res.RestoredDbResourceId})
			require.NoError(t, err)
			assert.Equal(t, types.ClusterStatusRestoring, c.Status)

			members, err := f.client.ListDbInstancesForCluster(
				t.Context(),
				&sdk.ListDbInstancesForClusterInput{DbClusterId: res.RestoredDbResourceId},
			)
			require.NoError(t, err)
			assert.Len(t, members.Items, 3)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newFixture(t))
		})
	}
}

func TestTags(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.instance(t, "tagged-db")
	clusterID := f.cluster(t, "tagged-cluster")
	cl, err := f.client.GetDbCluster(t.Context(), &sdk.GetDbClusterInput{DbClusterId: aws.String(clusterID)})
	require.NoError(t, err)
	grp, err := f.client.CreateDbParameterGroup(
		t.Context(),
		&sdk.CreateDbParameterGroupInput{Name: aws.String("tagged-group")},
	)
	require.NoError(t, err)
	bk, err := f.client.CreateDbBackup(
		t.Context(),
		&sdk.CreateDbBackupInput{DbResourceId: inst.Id, Name: aws.String("tagged-backup")},
	)
	require.NoError(t, err)

	arns := map[string]*string{"instance": inst.Arn, "cluster": cl.Arn, "group": grp.Arn, "backup": bk.Arn}

	for name, arn := range arns {
		_, tagErr := f.client.TagResource(
			t.Context(),
			&sdk.TagResourceInput{ResourceArn: arn, Tags: map[string]string{"a": "1", "b": "2"}},
		)
		require.NoError(t, tagErr, name)

		_, untagErr := f.client.UntagResource(
			t.Context(),
			&sdk.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"a"}},
		)
		require.NoError(t, untagErr, name)

		out, listErr := f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: arn})
		require.NoError(t, listErr, name)
		assert.Equal(t, map[string]string{"b": "2"}, out.Tags, name)
	}

	_, err = f.client.TagResource(t.Context(), &sdk.TagResourceInput{
		ResourceArn: aws.String(
			"arn:aws:timestream-influxdb:us-east-1:" + "000000000000" + ":db-instance/ffffffffff",
		),
		Tags: map[string]string{"a": "b"},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.TagResource(
		t.Context(),
		&sdk.TagResourceInput{ResourceArn: aws.String("arn:aws:s3:::b"), Tags: map[string]string{"a": "b"}},
	)
	assert.Equal(t, "ValidationException", apiCode(err))
}

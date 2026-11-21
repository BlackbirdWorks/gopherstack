package connect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicateInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, inst string)
		name string
	}{
		{
			name: "replica shares the id and lives in the replica region",
			run: func(t *testing.T, f *fixture, inst string) {
				t.Helper()

				out, err := f.client.ReplicateInstance(t.Context(), &sdk.ReplicateInstanceInput{
					InstanceId: aws.String(
						inst,
					),
					ReplicaRegion: aws.String("eu-west-1"),
					ReplicaAlias:  aws.String("replica-one"),
				})
				require.NoError(t, err)
				assert.Equal(t, inst, aws.ToString(out.Id))
				assert.Equal(t, "arn:aws:connect:eu-west-1:"+testAccount+":instance/"+inst, aws.ToString(out.Arn))

				src, err := f.client.DescribeInstance(
					t.Context(),
					&sdk.DescribeInstanceInput{InstanceId: aws.String(inst)},
				)
				require.NoError(t, err)
				cfg := src.ReplicationConfiguration
				require.NotNil(t, cfg)
				assert.Equal(t, "us-east-1", aws.ToString(cfg.SourceRegion))
				require.Len(t, cfg.ReplicationStatusSummaryList, 1)
				assert.Equal(t, "eu-west-1", aws.ToString(cfg.ReplicationStatusSummaryList[0].Region))
				assert.Equal(t, types.InstanceReplicationStatusInstanceReplicationInProgress,
					cfg.ReplicationStatusSummaryList[0].ReplicationStatus)

				f.clock.advance(time.Minute)

				src, err = f.client.DescribeInstance(
					t.Context(),
					&sdk.DescribeInstanceInput{InstanceId: aws.String(inst)},
				)
				require.NoError(t, err)
				assert.Equal(t, types.InstanceReplicationStatusInstanceReplicationComplete,
					src.ReplicationConfiguration.ReplicationStatusSummaryList[0].ReplicationStatus)
				assert.Equal(
					t,
					"arn:aws:connect:us-east-1:"+testAccount+":instance/"+inst,
					aws.ToString(src.Instance.Arn),
				)

				rep, err := f.inRegion(t, "eu-west-1").DescribeInstance(t.Context(),
					&sdk.DescribeInstanceInput{InstanceId: aws.String(inst)})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(out.Arn), aws.ToString(rep.Instance.Arn))
				assert.Equal(t, "replica-one", aws.ToString(rep.Instance.InstanceAlias))
				assert.Equal(t, "us-east-1", aws.ToString(rep.ReplicationConfiguration.SourceRegion))
			},
		},
		{
			name: "an unreplicated instance has no replication configuration",
			run: func(t *testing.T, f *fixture, inst string) {
				t.Helper()

				out, err := f.client.DescribeInstance(
					t.Context(),
					&sdk.DescribeInstanceInput{InstanceId: aws.String(inst)},
				)
				require.NoError(t, err)
				assert.Nil(t, out.ReplicationConfiguration)
			},
		},
		{name: "rejections", run: func(t *testing.T, f *fixture, inst string) {
			t.Helper()

			replicate := func(id, region, alias string) error {
				in := &sdk.ReplicateInstanceInput{InstanceId: aws.String(id), ReplicaRegion: aws.String(region)}
				if alias != "" {
					in.ReplicaAlias = aws.String(alias)
				}

				_, err := f.client.ReplicateInstance(t.Context(), in)

				return err
			}

			assert.Equal(t, "InvalidRequestException", apiCode(replicate(inst, "us-east-1", "")), "source region")
			assert.Equal(t, "InvalidParameterException", apiCode(replicate(inst, "mars", "")), "region syntax")
			assert.Equal(
				t,
				"InvalidParameterException",
				apiCode(replicate(inst, "eu-west-1", "Bad_Alias")),
				"alias syntax",
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(replicate("missing", "eu-west-1", "")))
			require.NoError(t, replicate(inst, "eu-west-1", "taken-alias"))
			assert.Equal(t, "ResourceConflictException", apiCode(replicate(inst, "eu-west-1", "")), "region twice")
			assert.Equal(
				t,
				"ResourceConflictException",
				apiCode(replicate(inst, "eu-west-2", "taken-alias")),
				"alias reuse",
			)
			assert.Equal(t, "ResourceConflictException",
				apiCode(replicate(inst, "eu-west-2", "src-alias")), "source alias reuse")
		}},
		{name: "a new instance is not ready to replicate", run: func(t *testing.T, f *fixture, _ string) {
			t.Helper()

			fresh := f.newInstance(t, "fresh-one")
			_, err := f.client.ReplicateInstance(t.Context(), &sdk.ReplicateInstanceInput{
				InstanceId: aws.String(fresh), ReplicaRegion: aws.String("eu-west-1"),
			})
			assert.Equal(t, "ResourceNotReadyException", apiCode(err))
		}},
		{name: "a repeated client token replays", run: func(t *testing.T, f *fixture, inst string) {
			t.Helper()

			in := &sdk.ReplicateInstanceInput{
				InstanceId: aws.String(inst), ReplicaRegion: aws.String("eu-west-1"), ClientToken: aws.String("tok-1"),
			}
			first, err := f.client.ReplicateInstance(t.Context(), in)
			require.NoError(t, err)

			second, err := f.client.ReplicateInstance(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.Arn), aws.ToString(second.Arn))
		}},
		{name: "replication survives a restore", run: func(t *testing.T, f *fixture, inst string) {
			t.Helper()

			_, err := f.client.ReplicateInstance(t.Context(), &sdk.ReplicateInstanceInput{
				InstanceId: aws.String(inst), ReplicaRegion: aws.String("eu-west-1"),
			})
			require.NoError(t, err)

			r := f.restored(t)
			out, err := r.client.DescribeInstance(t.Context(), &sdk.DescribeInstanceInput{InstanceId: aws.String(inst)})
			require.NoError(t, err)
			require.NotNil(t, out.ReplicationConfiguration)
			assert.Len(t, out.ReplicationConfiguration.ReplicationStatusSummaryList, 1)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			inst := f.newInstance(t, "src-alias")
			f.clock.advance(time.Minute)
			tt.run(t, f, inst)
		})
	}
}

func TestCrossRegionRouting(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "grid")
	f.clock.advance(time.Minute)

	get := func() ([]string, error) {
		out, err := f.client.GetCrossRegionRouting(
			t.Context(),
			&sdk.GetCrossRegionRoutingInput{InstanceId: aws.String(inst)},
		)
		if err != nil {
			return nil, err
		}

		return out.IsolatedRegions, nil
	}
	set := func(all bool) error {
		_, err := f.client.UpdateCrossRegionRouting(t.Context(),
			&sdk.UpdateCrossRegionRoutingInput{InstanceId: aws.String(inst), IsolatedAll: all})

		return err
	}

	_, err := get()
	assert.Equal(t, "InvalidRequestException", apiCode(err), "no replica yet")
	assert.Equal(t, "InvalidRequestException", apiCode(set(true)))

	_, err = f.client.ReplicateInstance(t.Context(), &sdk.ReplicateInstanceInput{
		InstanceId: aws.String(inst), ReplicaRegion: aws.String("eu-west-1"),
	})
	require.NoError(t, err)

	regions, err := get()
	require.NoError(t, err)
	assert.Empty(t, regions)

	require.NoError(t, set(true))

	regions, err = get()
	require.NoError(t, err)
	assert.Equal(t, []string{"eu-west-1", "us-east-1"}, regions)

	r := f.restored(t)
	out, err := r.client.GetCrossRegionRouting(
		t.Context(),
		&sdk.GetCrossRegionRoutingInput{InstanceId: aws.String(inst)},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"eu-west-1", "us-east-1"}, out.IsolatedRegions)

	require.NoError(t, set(false))

	regions, err = get()
	require.NoError(t, err)
	assert.Empty(t, regions)

	_, err = f.client.GetCrossRegionRouting(
		t.Context(),
		&sdk.GetCrossRegionRoutingInput{InstanceId: aws.String("missing")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

package resourceexplorer2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	rtypes "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetLifecycleDelay(time.Minute)

	c := f.client(t)
	ctx := t.Context()

	created, err := c.CreateIndex(ctx, &sdk.CreateIndexInput{Tags: map[string]string{"a": "b"}})
	require.NoError(t, err)
	assert.Equal(t, rtypes.IndexStateCreating, created.State)

	got, err := c.GetIndex(ctx, &sdk.GetIndexInput{})
	require.NoError(t, err)
	assert.Equal(t, rtypes.IndexStateCreating, got.State)
	assert.Equal(t, rtypes.IndexTypeLocal, got.Type)
	assert.Equal(t, map[string]string{"a": "b"}, got.Tags)

	f.clock.advance(time.Minute)

	got, err = c.GetIndex(ctx, &sdk.GetIndexInput{})
	require.NoError(t, err)
	assert.Equal(t, rtypes.IndexStateActive, got.State)

	_, err = c.CreateIndex(ctx, &sdk.CreateIndexInput{})
	assert.Equal(t, "ConflictException", apiCode(err))

	upd, err := c.UpdateIndexType(ctx, &sdk.UpdateIndexTypeInput{Arn: created.Arn, Type: rtypes.IndexTypeAggregator})
	require.NoError(t, err)
	assert.Equal(t, rtypes.IndexStateUpdating, upd.State)
	assert.Equal(t, rtypes.IndexTypeAggregator, upd.Type)

	_, err = c.UpdateIndexType(ctx, &sdk.UpdateIndexTypeInput{Arn: created.Arn, Type: rtypes.IndexTypeLocal})
	assert.Equal(t, "ConflictException", apiCode(err), "an UPDATING index cannot be updated")

	f.clock.advance(time.Minute)

	del, err := c.DeleteIndex(ctx, &sdk.DeleteIndexInput{Arn: created.Arn})
	require.NoError(t, err)
	assert.Equal(t, rtypes.IndexStateDeleting, del.State)

	_, err = c.DeleteIndex(ctx, &sdk.DeleteIndexInput{Arn: created.Arn})
	assert.Equal(t, "ConflictException", apiCode(err))

	f.clock.advance(time.Minute)

	_, err = c.GetIndex(ctx, &sdk.GetIndexInput{})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestIndexErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(c *sdk.Client, arn *string) error
		name string
		code string
	}{
		{
			name: "get_missing",
			call: func(c *sdk.Client, _ *string) error {
				_, err := c.GetIndex(t.Context(), &sdk.GetIndexInput{})

				return err
			},
			code: "ResourceNotFoundException",
		},
		{
			name: "update_unknown_arn",
			call: func(c *sdk.Client, _ *string) error {
				_, err := c.UpdateIndexType(t.Context(), &sdk.UpdateIndexTypeInput{
					Arn: aws.String(
						"arn:aws:resource-explorer-2:us-east-1:1:index/x",
					),
					Type: rtypes.IndexTypeAggregator,
				})

				return err
			},
			code: "ResourceNotFoundException",
		},
		{
			name: "update_bad_type",
			call: func(c *sdk.Client, arn *string) error {
				_, err := c.UpdateIndexType(t.Context(), &sdk.UpdateIndexTypeInput{Arn: arn, Type: "BOGUS"})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "update_same_type",
			call: func(c *sdk.Client, arn *string) error {
				_, err := c.UpdateIndexType(
					t.Context(),
					&sdk.UpdateIndexTypeInput{Arn: arn, Type: rtypes.IndexTypeLocal},
				)

				return err
			},
			code: "ConflictException",
		},
		{
			name: "create_bad_tag",
			call: func(c *sdk.Client, _ *string) error {
				_, err := c.CreateIndex(t.Context(), &sdk.CreateIndexInput{Tags: map[string]string{"aws:x": "y"}})

				return err
			},
			code: "ValidationException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			c := f.client(t)

			var arn *string

			if tc.name != "get_missing" && tc.name != "create_bad_tag" {
				out, err := c.CreateIndex(t.Context(), &sdk.CreateIndexInput{})
				require.NoError(t, err)

				arn = out.Arn
			}

			assert.Equal(t, tc.code, apiCode(tc.call(c, arn)))
		})
	}
}

func TestAggregatorAcrossRegions(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	east, west := f.client(t), f.clientIn(t, "us-west-2", "test")
	ctx := t.Context()

	eastIdx, err := east.CreateIndex(ctx, &sdk.CreateIndexInput{})
	require.NoError(t, err)

	westIdx, err := west.CreateIndex(ctx, &sdk.CreateIndexInput{})
	require.NoError(t, err)

	_, err = east.UpdateIndexType(ctx, &sdk.UpdateIndexTypeInput{Arn: eastIdx.Arn, Type: rtypes.IndexTypeAggregator})
	require.NoError(t, err)

	_, err = west.UpdateIndexType(ctx, &sdk.UpdateIndexTypeInput{Arn: westIdx.Arn, Type: rtypes.IndexTypeAggregator})
	assert.Equal(t, "ConflictException", apiCode(err), "only one aggregator per account")

	agg, err := east.GetIndex(ctx, &sdk.GetIndexInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"us-west-2"}, agg.ReplicatingFrom)

	local, err := west.GetIndex(ctx, &sdk.GetIndexInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"us-east-1"}, local.ReplicatingTo)

	list, err := east.ListIndexes(ctx, &sdk.ListIndexesInput{Type: rtypes.IndexTypeAggregator})
	require.NoError(t, err)
	require.Len(t, list.Indexes, 1)
	assert.Equal(t, "us-east-1", aws.ToString(list.Indexes[0].Region))

	list, err = east.ListIndexes(ctx, &sdk.ListIndexesInput{Regions: []string{"us-west-2"}})
	require.NoError(t, err)
	require.Len(t, list.Indexes, 1)
	assert.Equal(t, rtypes.IndexTypeLocal, list.Indexes[0].Type)

	_, err = east.UpdateIndexType(ctx, &sdk.UpdateIndexTypeInput{Arn: eastIdx.Arn, Type: rtypes.IndexTypeLocal})
	require.NoError(t, err)

	_, err = west.UpdateIndexType(ctx, &sdk.UpdateIndexTypeInput{Arn: westIdx.Arn, Type: rtypes.IndexTypeAggregator})
	assert.Equal(t, "ConflictException", apiCode(err), "a new aggregator is blocked for 24 hours")

	f.clock.advance(25 * time.Hour)

	_, err = west.UpdateIndexType(ctx, &sdk.UpdateIndexTypeInput{Arn: westIdx.Arn, Type: rtypes.IndexTypeAggregator})
	require.NoError(t, err)
}

func TestCreateIndexIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode string
		wantSame bool
	}{
		{name: "same_params_replays", tags: map[string]string{"k": "v"}, wantSame: true},
		{name: "different_params_conflict", tags: map[string]string{"k": "other"}, wantCode: "ConflictException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newFixture(t).client(t)
			first, err := c.CreateIndex(t.Context(), &sdk.CreateIndexInput{
				ClientToken: aws.String("tok"), Tags: map[string]string{"k": "v"},
			})
			require.NoError(t, err)

			second, err := c.CreateIndex(
				t.Context(),
				&sdk.CreateIndexInput{ClientToken: aws.String("tok"), Tags: tc.tags},
			)
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantSame, aws.ToString(first.Arn) == aws.ToString(second.Arn))
		})
	}
}

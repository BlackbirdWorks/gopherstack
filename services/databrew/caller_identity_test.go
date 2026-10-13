package databrew_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/databrew"
)

func callerCtx(arn string) context.Context {
	if arn == "" {
		return context.Background()
	}

	return awsmeta.Set(context.Background(), &awsmeta.Metadata{Principal: &awsmeta.Principal{Arn: arn}})
}

func TestCallerIdentityStamps(t *testing.T) {
	t.Parallel()

	const (
		alice = "arn:aws:iam::123456789012:user/alice"
		bob   = "arn:aws:iam::123456789012:user/bob"
	)

	tests := []struct {
		name         string
		creator      string
		updater      string
		wantCreated  string
		wantModified string
	}{
		{
			name:         "created_and_modified_by_different_callers",
			creator:      alice,
			updater:      bob,
			wantCreated:  alice,
			wantModified: bob,
		},
		{name: "same_caller", creator: alice, updater: alice, wantCreated: alice, wantModified: alice},
		{name: "unauthenticated_update_keeps_creator", creator: alice, wantCreated: alice, wantModified: alice},
		{name: "unauthenticated_omits"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			cctx, uctx := callerCtx(tt.creator), callerCtx(tt.updater)

			_, err := b.CreateDataset(cctx, "ds", "CSV", s3Input("bkt", "k"), databrew.DatasetFormatOptions{}, nil, nil)
			require.NoError(t, err)
			require.NoError(
				t,
				b.UpdateDataset(uctx, "ds", "CSV", s3Input("bkt", "k2"), databrew.DatasetFormatOptions{}, nil),
			)

			ds, err := b.DescribeDataset(context.Background(), "ds")
			require.NoError(t, err)
			assert.Equal(t, tt.wantCreated, ds.CreatedBy)
			assert.Equal(t, tt.wantModified, ds.LastModifiedBy)

			_, err = b.CreateRecipe(cctx, "rec", "", nil, nil)
			require.NoError(t, err)
			require.NoError(t, b.UpdateRecipe(uctx, "rec", "d2", nil))
			require.NoError(t, b.PublishRecipe(uctx, "rec", ""))

			rec, err := b.DescribeRecipe(context.Background(), "rec", "LATEST_WORKING")
			require.NoError(t, err)
			assert.Equal(t, tt.wantCreated, rec.CreatedBy)
			assert.Equal(t, tt.wantModified, rec.LastModifiedBy)

			pub, err := b.DescribeRecipe(context.Background(), "rec", "LATEST_PUBLISHED")
			require.NoError(t, err)
			assert.Equal(t, tt.updater, pub.PublishedBy)

			_, err = b.CreateRuleset(cctx, "rs", "", "arn:x", nil, nil)
			require.NoError(t, err)
			require.NoError(t, b.UpdateRuleset(uctx, "rs", "d", nil))

			rs, err := b.DescribeRuleset(context.Background(), "rs")
			require.NoError(t, err)
			assert.Equal(t, tt.wantCreated, rs.CreatedBy)
			assert.Equal(t, tt.wantModified, rs.LastModifiedBy)

			_, err = b.CreateProject(
				cctx,
				"pr",
				"ds",
				"rec",
				"arn:aws:iam::123456789012:role/R",
				databrew.Sample{},
				nil,
			)
			require.NoError(t, err)

			opened, err := b.OpenProjectSession(uctx, "pr")
			require.NoError(t, err)
			assert.Equal(t, tt.updater, opened.OpenedBy)
			assert.Equal(t, tt.wantCreated, opened.CreatedBy)

			_, err = b.CreateJob(
				cctx,
				"job",
				"RECIPE",
				"ds",
				"",
				"rec",
				"arn:aws:iam::123456789012:role/R",
				[]databrew.Output{
					{Location: databrew.S3Location{Bucket: "o", Key: "p/"}, Format: "CSV"},
				},
				nil,
				databrew.JobExtras{},
			)
			require.NoError(t, err)

			run, err := b.StartJobRun(uctx, "job")
			require.NoError(t, err)
			assert.Equal(t, tt.updater, run.StartedBy)

			job, err := b.DescribeJob(context.Background(), "job")
			require.NoError(t, err)
			assert.Equal(t, tt.wantCreated, job.CreatedBy)
		})
	}
}

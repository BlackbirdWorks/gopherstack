package cloudhsmv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/cloudhsmv2"
	"github.com/aws/aws-sdk-go-v2/service/cloudhsmv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tag(k, v string) types.Tag { return types.Tag{Key: aws.String(k), Value: aws.String(v)} }

func TestRealClient_Tags(t *testing.T) {
	t.Parallel()

	cases := []struct {
		resource func(t *testing.T, f *fixture) string
		name     string
	}{
		{func(t *testing.T, f *fixture) string {
			t.Helper()

			return aws.ToString(f.createCluster(t).ClusterId)
		}, "cluster"},
		{func(t *testing.T, f *fixture) string {
			t.Helper()

			_, bk := f.readyBackup(t)

			return aws.ToString(bk.BackupId)
		}, "backup"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ctx := t.Context()
			id := aws.String(tc.resource(t, f))

			_, err := f.client.TagResource(ctx, &sdk.TagResourceInput{
				ResourceId: id, TagList: []types.Tag{tag("b", "2"), tag("a", "1")},
			})
			require.NoError(t, err)

			_, err = f.client.TagResource(
				ctx,
				&sdk.TagResourceInput{ResourceId: id, TagList: []types.Tag{tag("a", "9")}},
			)
			require.NoError(t, err)

			out, err := f.client.ListTags(ctx, &sdk.ListTagsInput{ResourceId: id})
			require.NoError(t, err)
			assert.Equal(t, []types.Tag{tag("a", "9"), tag("b", "2")}, out.TagList)

			p1, err := f.client.ListTags(ctx, &sdk.ListTagsInput{ResourceId: id, MaxResults: aws.Int32(1)})
			require.NoError(t, err)
			require.Len(t, p1.TagList, 1)
			require.NotNil(t, p1.NextToken)

			p2, err := f.client.ListTags(
				ctx,
				&sdk.ListTagsInput{ResourceId: id, MaxResults: aws.Int32(1), NextToken: p1.NextToken},
			)
			require.NoError(t, err)
			assert.Equal(t, []types.Tag{tag("b", "2")}, p2.TagList)

			_, err = f.client.UntagResource(
				ctx,
				&sdk.UntagResourceInput{ResourceId: id, TagKeyList: []string{"a", "missing"}},
			)
			require.NoError(t, err)

			out, err = f.client.ListTags(ctx, &sdk.ListTagsInput{ResourceId: id})
			require.NoError(t, err)
			assert.Equal(t, []types.Tag{tag("b", "2")}, out.TagList)

			_, err = f.client.TagResource(
				ctx,
				&sdk.TagResourceInput{ResourceId: id, TagList: []types.Tag{tag("aws:x", "1")}},
			)
			assert.Equal(t, "CloudHsmTagException", apiCode(err))
		})
	}
}

func TestRealClient_TagErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		call func(f *fixture) error
		name string
	}{
		{func(f *fixture) error {
			_, err := f.client.ListTags(f.ctx(), &sdk.ListTagsInput{ResourceId: aws.String("cluster-missing")})

			return err
		}, "list_missing"},
		{func(f *fixture) error {
			_, err := f.client.TagResource(f.ctx(), &sdk.TagResourceInput{
				ResourceId: aws.String("hsm-123"), TagList: []types.Tag{tag("a", "1")},
			})

			return err
		}, "tag_unsupported"},
		{func(f *fixture) error {
			_, err := f.client.UntagResource(f.ctx(), &sdk.UntagResourceInput{
				ResourceId: aws.String("backup-missing"), TagKeyList: []string{"a"},
			})

			return err
		}, "untag_missing"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.t = t
			assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(tc.call(f)))
		})
	}
}

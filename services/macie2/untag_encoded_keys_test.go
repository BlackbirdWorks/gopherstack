package macie2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUntagResource_PercentEncodedKeys_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		untag  []string
		remain []string
	}{
		{name: "colon", untag: []string{"team:core"}, remain: []string{"plain", "my key"}},
		{name: "space", untag: []string{"my key"}, remain: []string{"plain", "team:core"}},
		{name: "all", untag: []string{"plain", "team:core", "my key"}, remain: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMacie2Client(t)

			created, err := client.CreateAllowList(t.Context(), &macie2sdk.CreateAllowListInput{
				Name:     aws.String("enc-al"),
				Criteria: &types.AllowListCriteria{Regex: aws.String("x.*")},
			})
			require.NoError(t, err)

			_, err = client.TagResource(t.Context(), &macie2sdk.TagResourceInput{
				ResourceArn: created.Arn,
				Tags:        map[string]string{"plain": "1", "team:core": "2", "my key": "3"},
			})
			require.NoError(t, err)

			_, err = client.UntagResource(
				t.Context(),
				&macie2sdk.UntagResourceInput{ResourceArn: created.Arn, TagKeys: tt.untag},
			)
			require.NoError(t, err)

			out, err := client.ListTagsForResource(
				t.Context(),
				&macie2sdk.ListTagsForResourceInput{ResourceArn: created.Arn},
			)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Tags))
			for k := range out.Tags {
				got = append(got, k)
			}

			assert.ElementsMatch(t, tt.remain, got)
		})
	}
}

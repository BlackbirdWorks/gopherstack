package guardduty_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
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
		{name: "plain key", untag: []string{"plain"}, remain: []string{"team:core", "my key", "a/b&c"}},
		{name: "colon", untag: []string{"team:core"}, remain: []string{"plain", "my key", "a/b&c"}},
		{name: "space", untag: []string{"my key"}, remain: []string{"plain", "team:core", "a/b&c"}},
		{name: "slash and ampersand", untag: []string{"a/b&c"}, remain: []string{"plain", "team:core", "my key"}},
		{name: "all", untag: []string{"plain", "team:core", "my key", "a/b&c"}, remain: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			arn := "arn:aws:guardduty:us-east-1:000000000000:detector/" + createRealClientDetector(t, client)

			_, err := client.TagResource(t.Context(), &guarddutysdk.TagResourceInput{
				ResourceArn: aws.String(arn),
				Tags:        map[string]string{"plain": "1", "team:core": "2", "my key": "3", "a/b&c": "4"},
			})
			require.NoError(t, err)

			_, err = client.UntagResource(
				t.Context(),
				&guarddutysdk.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: tt.untag},
			)
			require.NoError(t, err)

			out, err := client.ListTagsForResource(
				t.Context(),
				&guarddutysdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
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

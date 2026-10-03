package accessanalyzer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
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

			client := newTestAccessAnalyzerClient(
				t,
				accessanalyzer.NewHandler(accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")),
			)

			a, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String("enc-analyzer"),
				Type:         aatypes.TypeAccount,
			})
			require.NoError(t, err)

			_, err = client.TagResource(t.Context(), &aasdk.TagResourceInput{
				ResourceArn: a.Arn,
				Tags:        map[string]string{"plain": "1", "team:core": "2", "my key": "3"},
			})
			require.NoError(t, err)

			_, err = client.UntagResource(t.Context(), &aasdk.UntagResourceInput{ResourceArn: a.Arn, TagKeys: tt.untag})
			require.NoError(t, err)

			out, err := client.ListTagsForResource(t.Context(), &aasdk.ListTagsForResourceInput{ResourceArn: a.Arn})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Tags))
			for k := range out.Tags {
				got = append(got, k)
			}

			assert.ElementsMatch(t, tt.remain, got)
		})
	}
}

package lakeformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// TestListLFTags_SDKRoundTrip_BoundaryWalkAndTamperedToken drives ListLFTags
// through the real aws-sdk-go-v2/service/lakeformation client. ListLFTags'
// backend method (lf_tags.go) delegates straight to the generic paginate[T]
// (store.go), which is itself a thin wrapper over pkgs/page.New -- this
// ties that reuse to observable behaviour: a full boundary walk reproduces
// every created tag, and a garbage NextToken terminates cleanly rather than
// panicking or restarting at page one.
func TestListLFTags_SDKRoundTrip_BoundaryWalkAndTamperedToken(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	want := make([]string, 0, 5)
	for i := range 5 {
		key := "tag-" + string(rune('a'+i))
		_, err := client.CreateLFTag(t.Context(), &lakeformationsdk.CreateLFTagInput{
			TagKey:    aws.String(key),
			TagValues: []string{"v1"},
		})
		require.NoError(t, err)

		want = append(want, key)
	}

	var seen []string

	token := ""
	for {
		in := &lakeformationsdk.ListLFTagsInput{MaxResults: aws.Int32(2)}
		if token != "" {
			in.NextToken = aws.String(token)
		}

		out, err := client.ListLFTags(t.Context(), in)
		require.NoError(t, err)

		for _, tag := range out.LFTags {
			seen = append(seen, aws.ToString(tag.TagKey))
		}

		if out.NextToken == nil {
			break
		}

		token = aws.ToString(out.NextToken)
	}

	assert.Equal(t, want, seen, "walking every page must reproduce every created tag, in order, no drops or dupes")

	require.NotPanics(t, func() {
		out, err := client.ListLFTags(t.Context(), &lakeformationsdk.ListLFTagsInput{
			NextToken: aws.String("not-a-valid-offset-token"),
		})
		require.NoError(t, err)
		assert.NotNil(t, out)
	})
}

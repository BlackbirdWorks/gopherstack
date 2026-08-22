package textract_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	textractsdk "github.com/aws/aws-sdk-go-v2/service/textract"
	textracttypes "github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListAdapterVersions_AdapterIDOptional covers gopherstack-2wvq.
// ListAdapterVersionsInput declares no required members at all
// (textract@v1.43.4 api_op_ListAdapterVersions.go: AdapterId is a plain
// *string filter, "A string containing a unique ID for the adapter to match
// for when listing adapter versions" -- not the sole identifier). The
// handler previously rejected any request without AdapterId with a 400,
// making the documented all-adapters listing unreachable.
func TestListAdapterVersions_AdapterIDOptional(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestTextractClient(t, h)

	adapterA := createTestAdapter(t, client, "2wvq-adapter-a")
	adapterB := createTestAdapter(t, client, "2wvq-adapter-b")

	verA1 := createTestAdapterVersion(t, client, adapterA)
	verA2 := createTestAdapterVersion(t, client, adapterA)
	verB1 := createTestAdapterVersion(t, client, adapterB)

	t.Run("omitted adapter id lists across all adapters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListAdapterVersions(t.Context(), &textractsdk.ListAdapterVersionsInput{})
		require.NoError(t, err)

		seenAdapters := map[string]bool{}
		seenVersions := map[string]bool{}

		for _, v := range out.AdapterVersions {
			seenAdapters[aws.ToString(v.AdapterId)] = true
			seenVersions[aws.ToString(v.AdapterId)+"#"+aws.ToString(v.AdapterVersion)] = true
		}

		assert.True(t, seenAdapters[adapterA], "must include adapter A's versions")
		assert.True(t, seenAdapters[adapterB], "must include adapter B's versions")
		assert.GreaterOrEqual(t, len(seenAdapters), 2, "must span more than one adapter")

		for _, want := range []string{
			adapterA + "#" + verA1, adapterA + "#" + verA2, adapterB + "#" + verB1,
		} {
			assert.True(t, seenVersions[want], "expected version %q in the merged set", want)
		}
	})

	t.Run("adapter id still narrows to one adapter", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListAdapterVersions(t.Context(), &textractsdk.ListAdapterVersionsInput{
			AdapterId: aws.String(adapterA),
		})
		require.NoError(t, err)

		assert.Len(t, out.AdapterVersions, 2, "adapter A has exactly 2 versions")

		for _, v := range out.AdapterVersions {
			assert.Equal(t, adapterA, aws.ToString(v.AdapterId))
		}
	})

	t.Run("unknown adapter id still errors", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListAdapterVersions(t.Context(), &textractsdk.ListAdapterVersionsInput{
			AdapterId: aws.String("no-such-adapter"),
		})
		require.Error(t, err)
	})
}

// TestListAdapterVersions_AllAdaptersPagination verifies NextToken paging is
// stable and complete across the merged, cross-adapter set: paging through
// with a small MaxResults must yield the same versions as the unpaginated
// call, each exactly once.
func TestListAdapterVersions_AllAdaptersPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestTextractClient(t, h)

	adapterA := createTestAdapter(t, client, "2wvq-page-adapter-a")
	adapterB := createTestAdapter(t, client, "2wvq-page-adapter-b")
	adapterC := createTestAdapter(t, client, "2wvq-page-adapter-c")

	want := map[string]bool{}
	for _, adapterID := range []string{adapterA, adapterB, adapterC} {
		for range 2 {
			v := createTestAdapterVersion(t, client, adapterID)
			want[adapterID+"#"+v] = true
		}
	}

	got := map[string]bool{}

	const pageSize = 2

	var nextToken *string

	for pages := 0; ; pages++ {
		require.Less(t, pages, len(want)+1, "pagination did not terminate")

		out, err := client.ListAdapterVersions(t.Context(), &textractsdk.ListAdapterVersionsInput{
			MaxResults: aws.Int32(pageSize),
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		for _, v := range out.AdapterVersions {
			key := aws.ToString(v.AdapterId) + "#" + aws.ToString(v.AdapterVersion)
			assert.False(t, got[key], "duplicate %q across pages", key)
			got[key] = true
		}

		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}

		nextToken = out.NextToken
	}

	assert.Equal(t, want, got, "paged union must equal the whole merged set with no duplicates")
}

func createTestAdapter(t *testing.T, client *textractsdk.Client, name string) string {
	t.Helper()

	out, err := client.CreateAdapter(t.Context(), &textractsdk.CreateAdapterInput{
		AdapterName:  aws.String(name),
		FeatureTypes: []textracttypes.FeatureType{textracttypes.FeatureTypeQueries},
	})
	require.NoError(t, err)

	return aws.ToString(out.AdapterId)
}

// createTestAdapterVersion supplies DatasetConfig and OutputConfig because
// CreateAdapterVersionInput's real client-side validator requires both (and
// OutputConfig.S3Bucket) even though this test only cares about the
// resulting AdapterVersion -- an empty request never reaches the wire.
func createTestAdapterVersion(t *testing.T, client *textractsdk.Client, adapterID string) string {
	t.Helper()

	out, err := client.CreateAdapterVersion(t.Context(), &textractsdk.CreateAdapterVersionInput{
		AdapterId: aws.String(adapterID),
		DatasetConfig: &textracttypes.AdapterVersionDatasetConfig{
			ManifestS3Object: &textracttypes.S3Object{
				Bucket: aws.String("2wvq-dataset-bucket"),
				Name:   aws.String("manifest.json"),
			},
		},
		OutputConfig: &textracttypes.OutputConfig{
			S3Bucket: aws.String("2wvq-output-bucket"),
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.AdapterVersion)
}

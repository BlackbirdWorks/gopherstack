//go:build !integration

package mediastoredata_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	msdsdk "github.com/aws/aws-sdk-go-v2/service/mediastoredata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediastoredata"
)

// api_op_PutObject.go:75 (StorageClass defaults to TEMPORAL), api_op_ListItems.go:38
// (MaxResults defaults to a 1,000-per-page cap).
func TestValueSemantics_PutDescribeOverwriteAndListDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults  *int32
		name        string
		wantItems   int
		wantHasNext bool
	}{
		{name: "default_page_is_1000", maxResults: nil, wantItems: 1000, wantHasNext: true},
		{name: "explicit_max_results", maxResults: aws.Int32(2), wantItems: 2, wantHasNext: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMSDSDKClient(t, mediastoredata.NewHandler(mediastoredata.NewInMemoryBackend("us-east-1")))

			put := func(path, body, ctype string) {
				_, err := client.PutObject(t.Context(), &msdsdk.PutObjectInput{
					Path:        aws.String(path),
					Body:        bytesReader([]byte(body)),
					ContentType: aws.String(ctype),
				})
				require.NoError(t, err)
			}

			put("obj0000", "v1", "text/plain")
			put("obj0000", "version-two", "application/json")

			desc, err := client.DescribeObject(t.Context(), &msdsdk.DescribeObjectInput{Path: aws.String("obj0000")})
			require.NoError(t, err)
			assert.Equal(t, "application/json", aws.ToString(desc.ContentType))
			assert.Equal(t, int64(len("version-two")), aws.ToInt64(desc.ContentLength))

			for i := 1; i <= 1000; i++ {
				put(fmt.Sprintf("obj%04d", i), "x", "text/plain")
			}

			list, err := client.ListItems(t.Context(), &msdsdk.ListItemsInput{MaxResults: tt.maxResults})
			require.NoError(t, err)
			assert.Len(t, list.Items, tt.wantItems)
			assert.Equal(t, tt.wantHasNext, list.NextToken != nil)
			assert.Equal(t, "obj0000", aws.ToString(list.Items[0].Name))
			assert.Equal(t, "application/json", aws.ToString(list.Items[0].ContentType))
		})
	}
}

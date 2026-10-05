package omics_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListReadSetUploadParts_SourceAndWindow(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)

	tests := []struct {
		filter     *types.ReadSetUploadPartListFilter
		name       string
		source     types.ReadSetPartSource
		wantPages  []int
		maxResults int32
	}{
		{name: "source1 one page", source: types.ReadSetPartSourceSource1, wantPages: []int{3}},
		{name: "source2 only", source: types.ReadSetPartSourceSource2, wantPages: []int{1}},
		{name: "source1 pages of two", source: types.ReadSetPartSourceSource1, maxResults: 2, wantPages: []int{2, 1}},
		{
			name: "created after future", source: types.ReadSetPartSourceSource1,
			filter: &types.ReadSetUploadPartListFilter{CreatedAfter: &future}, wantPages: []int{0},
		},
		{
			name: "created before future", source: types.ReadSetPartSourceSource1,
			filter: &types.ReadSetUploadPartListFilter{CreatedBefore: &future}, wantPages: []int{3},
		},
		{
			name: "created before past", source: types.ReadSetPartSourceSource1,
			filter: &types.ReadSetUploadPartListFilter{CreatedBefore: &past}, wantPages: []int{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)

			store, err := client.CreateSequenceStore(t.Context(), &omicssdk.CreateSequenceStoreInput{
				Name: aws.String("parts-store"),
			})
			require.NoError(t, err)

			upload, err := client.CreateMultipartReadSetUpload(t.Context(), &omicssdk.CreateMultipartReadSetUploadInput{
				SequenceStoreId: store.Id,
				Name:            aws.String("parts-upload"),
				SourceFileType:  types.FileTypeFastq,
				SubjectId:       aws.String("subject-1"),
				SampleId:        aws.String("sample-1"),
			})
			require.NoError(t, err)

			for _, part := range []struct {
				src types.ReadSetPartSource
				n   int32
			}{
				{types.ReadSetPartSourceSource1, 3},
				{types.ReadSetPartSourceSource1, 1},
				{types.ReadSetPartSourceSource1, 2},
				{types.ReadSetPartSourceSource2, 1},
			} {
				_, err = client.UploadReadSetPart(t.Context(), &omicssdk.UploadReadSetPartInput{
					SequenceStoreId: store.Id,
					UploadId:        upload.UploadId,
					PartSource:      part.src,
					PartNumber:      aws.Int32(part.n),
					Payload:         bytes.NewReader([]byte("data")),
				})
				require.NoError(t, err)
			}

			in := &omicssdk.ListReadSetUploadPartsInput{
				SequenceStoreId: store.Id,
				UploadId:        upload.UploadId,
				PartSource:      tt.source,
				Filter:          tt.filter,
			}
			if tt.maxResults > 0 {
				in.MaxResults = aws.Int32(tt.maxResults)
			}

			var sizes []int
			var numbers []int32

			for {
				out, lErr := client.ListReadSetUploadParts(t.Context(), in)
				require.NoError(t, lErr)

				sizes = append(sizes, len(out.Parts))
				for _, p := range out.Parts {
					assert.Equal(t, tt.source, p.PartSource)
					numbers = append(numbers, aws.ToInt32(p.PartNumber))
				}

				if out.NextToken == nil {
					break
				}

				in.NextToken = out.NextToken
			}

			assert.Equal(t, tt.wantPages, sizes)
			assert.IsIncreasing(t, numbers)
		})
	}
}

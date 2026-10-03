package omics_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListFilters_CreatedWindowAndStatus_RealClient(t *testing.T) {
	t.Parallel()

	hourAgo := time.Now().Add(-time.Hour)
	hourAhead := time.Now().Add(time.Hour)

	tests := []struct {
		run  func(t *testing.T, client *omicssdk.Client, storeID string) int
		name string
		want int
	}{
		{name: "seq store window includes", want: 1, run: func(t *testing.T, c *omicssdk.Client, _ string) int {
			t.Helper()

			out, err := c.ListSequenceStores(t.Context(), &omicssdk.ListSequenceStoresInput{
				Filter: &types.SequenceStoreFilter{CreatedAfter: &hourAgo, CreatedBefore: &hourAhead},
			})
			require.NoError(t, err)

			return len(out.SequenceStores)
		}},
		{name: "seq store created after future", want: 0, run: func(t *testing.T, c *omicssdk.Client, _ string) int {
			t.Helper()

			out, err := c.ListSequenceStores(t.Context(), &omicssdk.ListSequenceStoresInput{
				Filter: &types.SequenceStoreFilter{CreatedAfter: &hourAhead},
			})
			require.NoError(t, err)

			return len(out.SequenceStores)
		}},
		{name: "seq store created before past", want: 0, run: func(t *testing.T, c *omicssdk.Client, _ string) int {
			t.Helper()

			out, err := c.ListSequenceStores(t.Context(), &omicssdk.ListSequenceStoresInput{
				Filter: &types.SequenceStoreFilter{CreatedBefore: &hourAgo},
			})
			require.NoError(t, err)

			return len(out.SequenceStores)
		}},
		{name: "seq store updated before past", want: 0, run: func(t *testing.T, c *omicssdk.Client, _ string) int {
			t.Helper()

			out, err := c.ListSequenceStores(t.Context(), &omicssdk.ListSequenceStoresInput{
				Filter: &types.SequenceStoreFilter{UpdatedBefore: &hourAgo},
			})
			require.NoError(t, err)

			return len(out.SequenceStores)
		}},
		{name: "seq store status match", want: 1, run: func(t *testing.T, c *omicssdk.Client, _ string) int {
			t.Helper()

			out, err := c.ListSequenceStores(t.Context(), &omicssdk.ListSequenceStoresInput{
				Filter: &types.SequenceStoreFilter{Status: types.SequenceStoreStatusActive},
			})
			require.NoError(t, err)

			return len(out.SequenceStores)
		}},
		{name: "seq store status mismatch", want: 0, run: func(t *testing.T, c *omicssdk.Client, _ string) int {
			t.Helper()

			out, err := c.ListSequenceStores(t.Context(), &omicssdk.ListSequenceStoresInput{
				Filter: &types.SequenceStoreFilter{Status: types.SequenceStoreStatusDeleting},
			})
			require.NoError(t, err)

			return len(out.SequenceStores)
		}},
		{name: "import job created after future", want: 0, run: func(t *testing.T, c *omicssdk.Client, id string) int {
			t.Helper()

			out, err := c.ListReadSetImportJobs(t.Context(), &omicssdk.ListReadSetImportJobsInput{
				SequenceStoreId: aws.String(id),
				Filter:          &types.ImportReadSetFilter{CreatedAfter: &hourAhead},
			})
			require.NoError(t, err)

			return len(out.ImportJobs)
		}},
		{name: "import job status and window", want: 1, run: func(t *testing.T, c *omicssdk.Client, id string) int {
			t.Helper()

			out, err := c.ListReadSetImportJobs(t.Context(), &omicssdk.ListReadSetImportJobsInput{
				SequenceStoreId: aws.String(id),
				Filter: &types.ImportReadSetFilter{
					Status: types.ReadSetImportJobStatusCompleted, CreatedAfter: &hourAgo,
				},
			})
			require.NoError(t, err)

			return len(out.ImportJobs)
		}},
		{name: "import job status mismatch", want: 0, run: func(t *testing.T, c *omicssdk.Client, id string) int {
			t.Helper()

			out, err := c.ListReadSetImportJobs(t.Context(), &omicssdk.ListReadSetImportJobsInput{
				SequenceStoreId: aws.String(id),
				Filter:          &types.ImportReadSetFilter{Status: types.ReadSetImportJobStatusFailed},
			})
			require.NoError(t, err)

			return len(out.ImportJobs)
		}},
		{name: "read set sample match", want: 1, run: func(t *testing.T, c *omicssdk.Client, id string) int {
			t.Helper()

			out, err := c.ListReadSets(t.Context(), &omicssdk.ListReadSetsInput{
				SequenceStoreId: aws.String(id),
				Filter: &types.ReadSetFilter{
					SampleId:  aws.String("sample-1"),
					SubjectId: aws.String("subject-1"),
				},
			})
			require.NoError(t, err)

			return len(out.ReadSets)
		}},
		{name: "read set sample mismatch", want: 0, run: func(t *testing.T, c *omicssdk.Client, id string) int {
			t.Helper()

			out, err := c.ListReadSets(t.Context(), &omicssdk.ListReadSetsInput{
				SequenceStoreId: aws.String(id),
				Filter:          &types.ReadSetFilter{SampleId: aws.String("other")},
			})
			require.NoError(t, err)

			return len(out.ReadSets)
		}},
		{name: "read set created before past", want: 0, run: func(t *testing.T, c *omicssdk.Client, id string) int {
			t.Helper()

			out, err := c.ListReadSets(t.Context(), &omicssdk.ListReadSetsInput{
				SequenceStoreId: aws.String(id),
				Filter:          &types.ReadSetFilter{CreatedBefore: &hourAgo},
			})
			require.NoError(t, err)

			return len(out.ReadSets)
		}},
		{
			name: "reference store created after future",
			want: 0,
			run: func(t *testing.T, c *omicssdk.Client, _ string) int {
				t.Helper()

				out, err := c.ListReferenceStores(t.Context(), &omicssdk.ListReferenceStoresInput{
					Filter: &types.ReferenceStoreFilter{CreatedAfter: &hourAhead},
				})
				require.NoError(t, err)

				return len(out.ReferenceStores)
			},
		},
		{name: "reference store window includes", want: 1, run: func(t *testing.T, c *omicssdk.Client, _ string) int {
			t.Helper()

			out, err := c.ListReferenceStores(t.Context(), &omicssdk.ListReferenceStoresInput{
				Filter: &types.ReferenceStoreFilter{CreatedAfter: &hourAgo, CreatedBefore: &hourAhead},
			})
			require.NoError(t, err)

			return len(out.ReferenceStores)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)

			store, err := client.CreateSequenceStore(
				t.Context(),
				&omicssdk.CreateSequenceStoreInput{Name: aws.String("flt-store")},
			)
			require.NoError(t, err)

			_, err = client.CreateReferenceStore(
				t.Context(),
				&omicssdk.CreateReferenceStoreInput{Name: aws.String("flt-ref")},
			)
			require.NoError(t, err)

			_, err = client.StartReadSetImportJob(t.Context(), &omicssdk.StartReadSetImportJobInput{
				SequenceStoreId: store.Id,
				RoleArn:         aws.String("arn:aws:iam::000000000000:role/omics-role"),
				Sources: []types.StartReadSetImportJobSourceItem{{
					SampleId:       aws.String("sample-1"),
					SubjectId:      aws.String("subject-1"),
					SourceFileType: types.FileTypeFastq,
					SourceFiles:    &types.SourceFiles{Source1: aws.String("s3://bucket/rs1.fastq")},
					Name:           aws.String("read-set-1"),
				}},
			})
			require.NoError(t, err)

			assert.Equal(t, tt.want, tt.run(t, client, aws.ToString(store.Id)))
		})
	}
}

package bedrockagent_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	bedrockagenttypes "github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/require"
)

// TestListIngestionJobs_FiltersAndSortHonoured proves that ListIngestionJobs
// applies its filters (STATUS/EQ, the only attribute/operator the real SDK
// defines -- types/enums.go) and sortBy (STARTED_AT), which the handler
// used to parse from the wrong wire location (URL query string) and never
// pass to the backend at all.
func TestListIngestionJobs_FiltersAndSortHonoured(t *testing.T) {
	t.Parallel()

	fixture := newIngestionFixture(t)
	client := newRoundTripClient(t, fixture.h)

	const jobCount = 3

	jobIDs := make([]string, jobCount)

	for i := range jobCount {
		out, err := client.StartIngestionJob(t.Context(), &bedrockagentsdk.StartIngestionJobInput{
			KnowledgeBaseId: aws.String(fixture.kbID),
			DataSourceId:    aws.String(fixture.dsID),
		})
		require.NoError(t, err)
		jobIDs[i] = aws.ToString(out.IngestionJob.IngestionJobId)
	}

	_, err := client.StopIngestionJob(t.Context(), &bedrockagentsdk.StopIngestionJobInput{
		KnowledgeBaseId: aws.String(fixture.kbID),
		DataSourceId:    aws.String(fixture.dsID),
		IngestionJobId:  aws.String(jobIDs[1]),
	})
	require.NoError(t, err)

	stopped, err := client.ListIngestionJobs(t.Context(), &bedrockagentsdk.ListIngestionJobsInput{
		KnowledgeBaseId: aws.String(fixture.kbID),
		DataSourceId:    aws.String(fixture.dsID),
		Filters: []bedrockagenttypes.IngestionJobFilter{{
			Attribute: bedrockagenttypes.IngestionJobFilterAttributeStatus,
			Operator:  bedrockagenttypes.IngestionJobFilterOperatorEq,
			Values:    []string{"STOPPED"},
		}},
	})
	require.NoError(t, err)
	require.Len(t, stopped.IngestionJobSummaries, 1, "STATUS EQ STOPPED must exclude the two COMPLETE jobs")
	require.Equal(t, jobIDs[1], aws.ToString(stopped.IngestionJobSummaries[0].IngestionJobId))

	sorted, err := client.ListIngestionJobs(t.Context(), &bedrockagentsdk.ListIngestionJobsInput{
		KnowledgeBaseId: aws.String(fixture.kbID),
		DataSourceId:    aws.String(fixture.dsID),
		SortBy: &bedrockagenttypes.IngestionJobSortBy{
			Attribute: bedrockagenttypes.IngestionJobSortByAttributeStartedAt,
			Order:     bedrockagenttypes.SortOrderDescending,
		},
	})
	require.NoError(t, err)
	require.Len(t, sorted.IngestionJobSummaries, jobCount)
	require.Equal(
		t, jobIDs[jobCount-1], aws.ToString(sorted.IngestionJobSummaries[0].IngestionJobId),
		"STARTED_AT DESC must put the most recently started job first",
	)
	require.Equal(
		t, jobIDs[0], aws.ToString(sorted.IngestionJobSummaries[jobCount-1].IngestionJobId),
		"STARTED_AT DESC must put the earliest job last",
	)
}

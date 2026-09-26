package macie2_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListSummaries_MemberRoundTrips proves this pass's over-wide-response
// fixes (gopherstack list-summary-shapes sweep, 2026-09-19): ListAllowLists'
// AllowListSummary leaked a tags map the real types.AllowListSummary doesn't
// have; ListClassificationJobs' JobSummary leaked tags/description/
// lastRunTime (types.JobSummary has neither); ListFindingsFilters' list item
// leaked description/position (types.FindingsFilterListItem has neither).
// Each subtest drives the real aws-sdk-go-v2 client end to end.
func TestListSummaries_MemberRoundTrips(t *testing.T) {
	t.Parallel()

	t.Run("allow list summary omits tags", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestMacie2SDKClient(t, h)

		created, err := client.CreateAllowList(t.Context(), &macie2sdk.CreateAllowListInput{
			ClientToken: aws.String("tok-list-summary-al"),
			Name:        aws.String("list-summary-al"),
			Description: aws.String("al desc"),
			Criteria:    &types.AllowListCriteria{Regex: aws.String("test")},
			Tags:        map[string]string{"k": "v"},
		})
		require.NoError(t, err)

		listed, err := client.ListAllowLists(t.Context(), &macie2sdk.ListAllowListsInput{})
		require.NoError(t, err)
		require.Len(t, listed.AllowLists, 1)
		assert.Equal(t, aws.ToString(created.Arn), aws.ToString(listed.AllowLists[0].Arn))
		assert.Equal(t, "al desc", aws.ToString(listed.AllowLists[0].Description))

		rec := doRequest(t, h, http.MethodGet, "/allow-lists", nil)
		assert.NotContains(t, rec.Body.String(), `"tags"`, "ListAllowLists must not leak the allow list's tags map")
	})

	t.Run("classification job summary omits tags description lastruntime", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestMacie2SDKClient(t, h)

		created, err := client.CreateClassificationJob(t.Context(), &macie2sdk.CreateClassificationJobInput{
			ClientToken: aws.String("tok-list-summary-job"),
			Name:        aws.String("list-summary-job"),
			JobType:     types.JobTypeOneTime,
			Description: aws.String("job desc"),
			Tags:        map[string]string{"k": "v"},
			S3JobDefinition: &types.S3JobDefinition{
				BucketDefinitions: []types.S3BucketDefinitionForJob{},
			},
		})
		require.NoError(t, err)

		listed, err := client.ListClassificationJobs(t.Context(), &macie2sdk.ListClassificationJobsInput{})
		require.NoError(t, err)
		require.Len(t, listed.Items, 1)
		assert.Equal(t, aws.ToString(created.JobId), aws.ToString(listed.Items[0].JobId))
		assert.Equal(t, types.JobTypeOneTime, listed.Items[0].JobType)

		rec := doRequest(t, h, http.MethodPost, "/jobs/list", map[string]any{})
		body := rec.Body.String()
		assert.NotContains(t, body, `"tags"`, "ListClassificationJobs must not leak the job's tags map")
		assert.NotContains(t, body, `"description"`, "ListClassificationJobs must not leak the job's description")
		assert.NotContains(t, body, `"lastRunTime"`, "ListClassificationJobs must not leak lastRunTime")
	})

	t.Run("findings filter list item omits description position", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestMacie2SDKClient(t, h)

		created, err := client.CreateFindingsFilter(t.Context(), &macie2sdk.CreateFindingsFilterInput{
			Name:            aws.String("list-summary-ff"),
			Action:          types.FindingsFilterActionArchive,
			Description:     aws.String("filter desc"),
			Position:        aws.Int32(3),
			FindingCriteria: &types.FindingCriteria{},
			Tags:            map[string]string{"k": "v"},
		})
		require.NoError(t, err)

		listed, err := client.ListFindingsFilters(t.Context(), &macie2sdk.ListFindingsFiltersInput{})
		require.NoError(t, err)
		require.Len(t, listed.FindingsFilterListItems, 1)
		assert.Equal(t, aws.ToString(created.Arn), aws.ToString(listed.FindingsFilterListItems[0].Arn))
		assert.Equal(t, types.FindingsFilterActionArchive, listed.FindingsFilterListItems[0].Action)
		assert.Equal(t, map[string]string{"k": "v"}, listed.FindingsFilterListItems[0].Tags)

		rec := doRequest(t, h, http.MethodGet, "/findingsfilters", nil)
		body := rec.Body.String()
		assert.NotContains(t, body, `"description"`, "ListFindingsFilters must not leak the filter's description")
		assert.NotContains(t, body, `"position"`, "ListFindingsFilters must not leak the filter's position")
	})
}

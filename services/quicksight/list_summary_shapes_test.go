package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListThemes_NoTypeLeak covers gopherstack-21my: types.ThemeSummary
// (types.go:21801-21821, quicksight@v1.129.0) has no Type member -- that's
// Theme/DescribeThemeOutput-only. ListThemes fabricated it on every summary;
// a real typed client silently ignores an unknown key, so the proof is the
// raw wire body.
func TestListThemes_NoTypeLeak(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "POST", accountPath("/themes/wff-21my-theme"), map[string]any{
		"Name":        "wff-21my-theme",
		"BaseThemeId": "SEASIDE",
	})
	require.Equal(t, 200, createRec.Code, createRec.Body.String())

	listRec := doRequest(t, h, "GET", accountPath("/themes"), nil)
	require.Equal(t, 200, listRec.Code, listRec.Body.String())
	assert.NotContains(t, listRec.Body.String(), `"Type"`,
		"ListThemes must not leak the Describe-only Type member")
	assert.Contains(t, listRec.Body.String(), `"ThemeId":"wff-21my-theme"`)
}

// TestListTopics_NoTimestampLeak_UserExperienceVersionRoundTrips covers
// gopherstack-21my: types.TopicSummary (types.go:23074-23090,
// quicksight@v1.129.0) has only Arn, Name, TopicId and
// UserExperienceVersion -- no CreatedTime/LastUpdatedTime (Topic/
// DescribeTopicOutput-only), and handleListTopics fabricated both while
// dropping the one real optional member. The leak is unobservable through
// the typed client (no field to decode into), so it's proven via raw body;
// UserExperienceVersion is proven via the typed client since it's a real
// field.
func TestListTopics_NoTimestampLeak_UserExperienceVersionRoundTrips(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "POST", accountPath("/topics"), map[string]any{
		"TopicId": "wff-21my-topic",
		"Topic": map[string]any{
			"Name":                  "wff-21my-topic",
			"UserExperienceVersion": "NEW_READER_EXPERIENCE",
		},
	})
	require.Equal(t, 200, createRec.Code, createRec.Body.String())

	listRec := doRequest(t, h, "GET", accountPath("/topics"), nil)
	require.Equal(t, 200, listRec.Code, listRec.Body.String())
	body := listRec.Body.String()
	assert.NotContains(t, body, "CreatedTime",
		"ListTopics must not leak the Describe-only CreatedTime member")
	assert.NotContains(t, body, "LastUpdatedTime",
		"ListTopics must not leak the Describe-only LastUpdatedTime member")
	assert.Contains(t, body, `"UserExperienceVersion":"NEW_READER_EXPERIENCE"`)

	client := newTestQuickSightClient(t, h)
	listed, err := client.ListTopics(t.Context(), &quicksightsdk.ListTopicsInput{
		AwsAccountId: aws.String(testAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.TopicsSummaries, 1)
	assert.Equal(t,
		types.TopicUserExperienceVersionNewReaderExperience,
		listed.TopicsSummaries[0].UserExperienceVersion,
	)
}

// TestListTopicRefreshSchedules_DatasetNameRoundTrips covers
// gopherstack-21my: types.TopicRefreshScheduleSummary (deserializers.go's
// awsRestjson1_deserializeDocumentTopicRefreshScheduleSummary) carries
// DatasetName alongside DatasetId/DatasetArn/RefreshSchedule, derived from
// the dataset's own stored Name -- ListTopicRefreshSchedules never surfaced
// it.
func TestListTopicRefreshSchedules_DatasetNameRoundTrips(t *testing.T) {
	t.Parallel()

	client := newQuickSightTestClient(t)
	ctx := t.Context()

	dataset, err := client.CreateDataSet(ctx, &quicksightsdk.CreateDataSetInput{
		AwsAccountId: aws.String(qsTestAccountID),
		DataSetId:    aws.String("wff-21my-dataset"),
		Name:         aws.String("WFF 21my Dataset"),
		ImportMode:   types.DataSetImportModeDirectQuery,
		PhysicalTableMap: map[string]types.PhysicalTable{
			"pt1": &types.PhysicalTableMemberRelationalTable{
				Value: types.RelationalTable{
					DataSourceArn: aws.String(
						"arn:aws:quicksight:us-east-1:000000000000:datasource/wff-src",
					),
					Name:   aws.String("orders"),
					Schema: aws.String("public"),
					InputColumns: []types.InputColumn{
						{Name: aws.String("id"), Type: types.InputColumnDataTypeInteger},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateTopic(ctx, &quicksightsdk.CreateTopicInput{
		AwsAccountId: aws.String(qsTestAccountID),
		TopicId:      aws.String("wff-21my-topic-2"),
		Topic:        &types.TopicDetails{Name: aws.String("wff-21my-topic-2")},
	})
	require.NoError(t, err)

	_, err = client.CreateTopicRefreshSchedule(ctx, &quicksightsdk.CreateTopicRefreshScheduleInput{
		AwsAccountId: aws.String(qsTestAccountID),
		TopicId:      aws.String("wff-21my-topic-2"),
		DatasetArn:   dataset.Arn,
		RefreshSchedule: &types.TopicRefreshSchedule{
			IsEnabled: aws.Bool(true),
		},
	})
	require.NoError(t, err)

	listed, err := client.ListTopicRefreshSchedules(ctx, &quicksightsdk.ListTopicRefreshSchedulesInput{
		AwsAccountId: aws.String(qsTestAccountID),
		TopicId:      aws.String("wff-21my-topic-2"),
	})
	require.NoError(t, err)
	require.Len(t, listed.RefreshSchedules, 1)
	assert.Equal(t, "WFF 21my Dataset", aws.ToString(listed.RefreshSchedules[0].DatasetName))
}

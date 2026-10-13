package swf_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func apiErrorCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.NotContains(t, apiErr.ErrorMessage(), apiErr.ErrorCode()+":")

	return apiErr.ErrorCode()
}

func TestSDK_RegisterNamesValidated(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		value    string
		wantCode string
	}{
		{"plain", "my-domain", ""},
		{"colon", "bad:name", "ValidationException"},
		{"slash", "bad/name", "ValidationException"},
		{"pipe", "bad|name", "ValidationException"},
		{"literal_arn", "arn", "ValidationException"},
		{"leading_space", " name", "ValidationException"},
		{"control", "a\u0001b", "ValidationException"},
		{"too_long", strings.Repeat("n", 257), "ValidationException"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSWFSDKClient(t, swf.NewHandler(swf.NewInMemoryBackend()))
			_, err := client.RegisterDomain(t.Context(), &swfsdk.RegisterDomainInput{
				Name:                                   aws.String(tc.value),
				WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
			})

			if tc.wantCode == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tc.wantCode, apiErrorCode(t, err))
		})
	}
}

func TestSDK_UnknownResourcesAreBadRequests(t *testing.T) {
	t.Parallel()

	client := newTestSWFSDKClient(t, swf.NewHandler(swf.NewInMemoryBackend()))
	ctx := t.Context()

	_, err := client.DescribeDomain(ctx, &swfsdk.DescribeDomainInput{Name: aws.String("nope")})
	assert.Equal(t, "UnknownResourceFault", apiErrorCode(t, err))

	var respErr interface{ HTTPStatusCode() int }
	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, 400, respErr.HTTPStatusCode())

	_, err = client.PollForDecisionTask(ctx, &swfsdk.PollForDecisionTaskInput{
		Domain: aws.String("nope"), TaskList: &swftypes.TaskList{Name: aws.String("tl")},
	})
	assert.Equal(t, "UnknownResourceFault", apiErrorCode(t, err))

	_, err = client.GetWorkflowExecutionHistory(ctx, &swfsdk.GetWorkflowExecutionHistoryInput{
		Domain:    aws.String("nope"),
		Execution: &swftypes.WorkflowExecution{WorkflowId: aws.String("w"), RunId: aws.String("r")},
	})
	assert.Equal(t, "UnknownResourceFault", apiErrorCode(t, err))
}

func TestSDK_ListPagingParamsValidated(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		token string
		size  int32
	}{
		{"junk_token", "!!!", 0},
		{"too_big", "", 1001},
		{"negative", "", -1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "1"))
			client := newTestSWFSDKClient(t, swf.NewHandler(b))

			in := &swfsdk.ListDomainsInput{
				RegistrationStatus: swftypes.RegistrationStatusRegistered,
				MaximumPageSize:    tc.size,
			}
			if tc.token != "" {
				in.NextPageToken = aws.String(tc.token)
			}

			_, err := client.ListDomains(t.Context(), in)
			require.Error(t, err)
			assert.Equal(t, "ValidationException", apiErrorCode(t, err))
		})
	}
}

func TestBackend_ScheduleActivityTaskResolution(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		wantQueue    string
		wantFailure  string
		attrTaskList string
		registered   bool
	}{
		{name: "type_default_list", registered: true, wantQueue: "type-list"},
		{name: "explicit_list_wins", registered: true, attrTaskList: "explicit", wantQueue: "explicit"},
		{name: "unregistered", registered: false, wantFailure: "ACTIVITY_TYPE_DOES_NOT_EXIST"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

			if tc.registered {
				require.NoError(t, b.RegisterActivityType("dom", "act", "1", "",
					swf.ActivityTypeDefaults{DefaultTaskList: "type-list"}))
			}

			started, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain: "dom", WorkflowID: "wf", TaskList: "wf-list",
			})
			require.NoError(t, err)

			b.EnqueueDecisionTaskInternal("dom", "wf-list", "wf", started.RunID)
			token := pollDecisionTask(t, b, "dom", "wf-list")

			require.NoError(t, b.RespondDecisionTaskCompleted(token, "", []swf.Decision{{
				DecisionType: "ScheduleActivityTask",
				ScheduleActivityTaskAttrs: &swf.ScheduleActivityTaskDecisionAttrs{
					ActivityType: swf.ActivityTaskActivityType{Name: "act", Version: "1"},
					ActivityID:   "a1",
					TaskList:     tc.attrTaskList,
				},
			}}))

			if tc.wantFailure != "" {
				events, _ := b.GetWorkflowExecutionHistory("dom", "wf", started.RunID, 0, "", false)
				last := events[len(events)-1]
				assert.Equal(t, "ScheduleActivityTaskFailed", last.EventType)
				assert.Equal(t, 0, b.CountPendingActivityTasks("dom", "wf-list"))

				return
			}

			assert.Equal(t, 1, b.CountPendingActivityTasks("dom", tc.wantQueue))
		})
	}
}

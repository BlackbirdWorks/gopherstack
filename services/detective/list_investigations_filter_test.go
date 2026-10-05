package detective_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	detectivesdk "github.com/aws/aws-sdk-go-v2/service/detective"
	detectivetypes "github.com/aws/aws-sdk-go-v2/service/detective/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

// ListInvestigations FilterCriteria (types.FilterCriteria) and SortCriteria.
func TestListInvestigations_FilterAndSort(t *testing.T) {
	t.Parallel()

	const (
		userA = "arn:aws:iam::000000000000:user/a"
		userB = "arn:aws:iam::000000000000:user/b"
	)

	tests := []struct {
		name    string
		input   detectivesdk.ListInvestigationsInput
		want    []string
		ordered bool
	}{
		{name: "no filter", want: []string{userA, userB}},
		{
			name: "entity arn", want: []string{userB},
			input: detectivesdk.ListInvestigationsInput{
				FilterCriteria: &detectivetypes.FilterCriteria{
					EntityArn: &detectivetypes.StringFilter{Value: aws.String(userB)},
				},
			},
		},
		{
			name: "state archived", want: []string{userA},
			input: detectivesdk.ListInvestigationsInput{
				FilterCriteria: &detectivetypes.FilterCriteria{
					State: &detectivetypes.StringFilter{Value: aws.String("ARCHIVED")},
				},
			},
		},
		{
			name: "severity no match", want: []string{},
			input: detectivesdk.ListInvestigationsInput{
				FilterCriteria: &detectivetypes.FilterCriteria{
					Severity: &detectivetypes.StringFilter{Value: aws.String("CRITICAL")},
				},
			},
		},
		{
			name: "created time window excludes all", want: []string{},
			input: detectivesdk.ListInvestigationsInput{
				FilterCriteria: &detectivetypes.FilterCriteria{CreatedTime: &detectivetypes.DateFilter{
					StartInclusive: aws.Time(
						time.Now().Add(-48 * time.Hour),
					),
					EndInclusive: aws.Time(time.Now().Add(-24 * time.Hour)),
				}},
			},
		},
		{
			name: "created time window includes all", want: []string{userA, userB},
			input: detectivesdk.ListInvestigationsInput{
				FilterCriteria: &detectivetypes.FilterCriteria{CreatedTime: &detectivetypes.DateFilter{
					StartInclusive: aws.Time(
						time.Now().Add(-time.Hour),
					),
					EndInclusive: aws.Time(time.Now().Add(time.Hour)),
				}},
			},
		},
		{
			name: "created time desc", want: []string{userB, userA}, ordered: true,
			input: detectivesdk.ListInvestigationsInput{
				SortCriteria: &detectivetypes.SortCriteria{
					Field:     detectivetypes.FieldCreatedTime,
					SortOrder: detectivetypes.SortOrderDesc,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDetectiveSDKClient(
				t,
				detective.NewHandler(detective.NewInMemoryBackend("000000000000", detectiveRTRegion)),
			)
			ctx := t.Context()

			graph, err := client.CreateGraph(ctx, &detectivesdk.CreateGraphInput{})
			require.NoError(t, err)

			ids := map[string]*string{}

			for _, arn := range []string{userA, userB} {
				out, startErr := client.StartInvestigation(ctx, &detectivesdk.StartInvestigationInput{
					GraphArn: graph.GraphArn, EntityArn: aws.String(arn),
					ScopeStartTime: aws.Time(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
					ScopeEndTime:   aws.Time(time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)),
				})
				require.NoError(t, startErr)

				ids[arn] = out.InvestigationId
			}

			_, err = client.UpdateInvestigationState(ctx, &detectivesdk.UpdateInvestigationStateInput{
				GraphArn: graph.GraphArn, InvestigationId: ids[userA], State: detectivetypes.StateArchived,
			})
			require.NoError(t, err)

			in := tt.input
			in.GraphArn = graph.GraphArn

			out, err := client.ListInvestigations(ctx, &in)
			require.NoError(t, err)

			got := make([]string, 0, len(out.InvestigationDetails))
			for _, d := range out.InvestigationDetails {
				got = append(got, aws.ToString(d.EntityArn))
			}

			if tt.ordered {
				assert.Equal(t, tt.want, got)
			} else {
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}

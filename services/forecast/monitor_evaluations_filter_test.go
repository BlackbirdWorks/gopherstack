package forecast_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListMonitorEvaluations_FilterAndPaging checks EvaluationState filters and a bad NextToken via the SDK.
func TestListMonitorEvaluations_FilterAndPaging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		token   string
		filter  []types.Filter
		want    int
		wantErr bool
	}{
		{name: "no_filter", want: 1},
		{name: "is_success", filter: evalFilter(types.FilterConditionStringIs, "SUCCESS"), want: 1},
		{name: "is_failure", filter: evalFilter(types.FilterConditionStringIs, "FAILURE"), want: 0},
		{name: "is_not_success", filter: evalFilter(types.FilterConditionStringIsNot, "SUCCESS"), want: 0},
		{name: "bad_token", token: "!!bad", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			client := newTestForecastClient(t, h)
			mon, err := client.CreateMonitor(t.Context(), &forecastsdk.CreateMonitorInput{
				MonitorName: aws.String("ev-mon"), ResourceArn: aws.String(createPredictor(t, h)),
			})
			require.NoError(t, err)

			in := &forecastsdk.ListMonitorEvaluationsInput{
				MonitorArn: mon.MonitorArn, Filters: tt.filter, MaxResults: aws.Int32(5),
			}
			if tt.token != "" {
				in.NextToken = aws.String(tt.token)
			}

			out, err := client.ListMonitorEvaluations(t.Context(), in)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidNextTokenException")

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.PredictorMonitorEvaluations, tt.want)
			assert.Nil(t, out.NextToken)
		})
	}
}

func evalFilter(cond types.FilterConditionString, value string) []types.Filter {
	return []types.Filter{{Condition: cond, Key: aws.String("EvaluationState"), Value: aws.String(value)}}
}

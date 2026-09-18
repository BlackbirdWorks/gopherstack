package forecast_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeAutoPredictor_NoInputOnlyFieldLeak covers gopherstack-21my:
// resourceOutput (handler.go) clones the stored Create*Input body verbatim
// for Describe/Create-echo responses. CreateAutoPredictorInput's
// ExplainPredictor and ReferencePredictorArn have no same-named member on
// the real DescribeAutoPredictorOutput (forecast@v1.44.4
// api_op_DescribeAutoPredictor.go declares ExplainabilityInfo and
// ReferencePredictorSummary instead -- different name and shape, already
// documented as unpopulated gaps) -- both leaked as fabricated raw wire
// members whenever a CreateAutoPredictor request supplied them.
func TestDescribeAutoPredictor_NoInputOnlyFieldLeak(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newTestForecastClient(t, h)
	ctx := t.Context()

	dsGroup, err := client.CreateDatasetGroup(ctx, &forecastsdk.CreateDatasetGroupInput{
		DatasetGroupName: aws.String("no_leak_dsg"),
		Domain:           types.DomainCustom,
	})
	require.NoError(t, err)

	created, err := client.CreateAutoPredictor(ctx, &forecastsdk.CreateAutoPredictorInput{
		PredictorName:    aws.String("no_leak_autopred"),
		ExplainPredictor: aws.Bool(true),
		DataConfig:       &types.DataConfig{DatasetGroupArn: dsGroup.DatasetGroupArn},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "DescribeAutoPredictor", map[string]any{
		"PredictorArn": aws.ToString(created.PredictorArn),
	})
	require.Equal(t, 200, rec.Code)
	assert.NotContains(t, rec.Body.String(), "ExplainPredictor",
		"DescribeAutoPredictorOutput has no ExplainPredictor member")
	assert.NotContains(t, rec.Body.String(), "ReferencePredictorArn",
		"DescribeAutoPredictorOutput has no ReferencePredictorArn member")
}

package forecast_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/forecast"
)

func TestTypedErrors_PerOperationCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(*forecastsdk.Client) error
		want any
		name string
	}{
		{
			name: "describe_missing_is_not_found",
			want: new(*types.ResourceNotFoundException),
			call: func(c *forecastsdk.Client) error {
				_, err := c.DescribeDataset(t.Context(), &forecastsdk.DescribeDatasetInput{
					DatasetArn: aws.String("arn:aws:forecast:us-east-1:000000000000:dataset/none"),
				})

				return err
			},
		},
		{
			name: "delete_missing_is_not_found",
			want: new(*types.ResourceNotFoundException),
			call: func(c *forecastsdk.Client) error {
				_, err := c.DeleteDataset(t.Context(), &forecastsdk.DeleteDatasetInput{
					DatasetArn: aws.String("arn:aws:forecast:us-east-1:000000000000:dataset/none"),
				})

				return err
			},
		},
		{
			name: "list_bad_token_is_invalid_next_token",
			want: new(*types.InvalidNextTokenException),
			call: func(c *forecastsdk.Client) error {
				_, err := c.ListDatasets(t.Context(), &forecastsdk.ListDatasetsInput{NextToken: aws.String("%%bad")})

				return err
			},
		},
		{
			name: "create_bad_name_is_invalid_input",
			want: new(*types.InvalidInputException),
			call: func(c *forecastsdk.Client) error {
				_, err := c.CreateDatasetGroup(t.Context(), &forecastsdk.CreateDatasetGroupInput{
					DatasetGroupName: aws.String("bad name!"), Domain: types.DomainRetail,
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestForecastClient(
				t, forecast.NewHandler(forecast.NewInMemoryBackend("000000000000", "us-east-1")),
			)
			require.ErrorAs(t, tt.call(client), tt.want)
		})
	}
}

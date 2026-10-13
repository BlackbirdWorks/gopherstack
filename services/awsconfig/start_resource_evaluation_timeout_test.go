package awsconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartResourceEvaluation_TimeoutBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout int32
		wantErr bool
	}{
		{name: "zero_default", timeout: 0},
		{name: "max", timeout: 3600},
		{name: "over_max", timeout: 3601, wantErr: true},
		{name: "negative", timeout: -1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newOpenItemsClient(t)

			_, err := client.StartResourceEvaluation(t.Context(), &configservicesdk.StartResourceEvaluationInput{
				EvaluationMode:    types.EvaluationModeDetective,
				EvaluationTimeout: tt.timeout,
				ResourceDetails: &types.ResourceDetails{
					ResourceId:            aws.String("b1"),
					ResourceType:          aws.String("AWS::S3::Bucket"),
					ResourceConfiguration: aws.String(`{}`),
				},
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var invalid *types.InvalidParameterValueException

			assert.ErrorAs(t, err, &invalid, "got %v", err)
		})
	}
}

package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuppressionValidationOptions(t *testing.T) {
	t.Parallel()

	validation := func(
		enabled sesv2types.FeatureStatus, verdict sesv2types.SuppressionConfidenceVerdictThreshold,
	) *sesv2types.SuppressionValidationOptions {
		cond := &sesv2types.SuppressionConditionThreshold{ConditionThresholdEnabled: enabled}
		if verdict != "" {
			cond.OverallConfidenceThreshold = &sesv2types.SuppressionConfidenceThreshold{
				ConfidenceVerdictThreshold: verdict,
			}
		}

		return &sesv2types.SuppressionValidationOptions{ConditionThreshold: cond}
	}

	tests := []struct {
		opts        *sesv2types.SuppressionOptions
		name        string
		wantEnabled sesv2types.FeatureStatus
		wantVerdict sesv2types.SuppressionConfidenceVerdictThreshold
		viaCreate   bool
		wantErr     bool
	}{
		{name: "put_enabled_high", opts: &sesv2types.SuppressionOptions{
			ValidationOptions: validation(
				sesv2types.FeatureStatusEnabled,
				sesv2types.SuppressionConfidenceVerdictThresholdHigh,
			),
		}, wantEnabled: sesv2types.FeatureStatusEnabled, wantVerdict: sesv2types.SuppressionConfidenceVerdictThresholdHigh},
		{name: "create_disabled_no_verdict", viaCreate: true, opts: &sesv2types.SuppressionOptions{
			ValidationOptions: validation(sesv2types.FeatureStatusDisabled, ""),
		}, wantEnabled: sesv2types.FeatureStatusDisabled},
		{name: "invalid_verdict", opts: &sesv2types.SuppressionOptions{
			ValidationOptions: validation(sesv2types.FeatureStatusEnabled, "LOW"),
		}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newSESv2TestHandler(t)
			client := newSESv2SDKClient(t, h)
			ctx := t.Context()

			in := &sesv2sdk.CreateConfigurationSetInput{ConfigurationSetName: aws.String("cs")}
			if tt.viaCreate {
				in.SuppressionOptions = tt.opts
			}

			_, err := client.CreateConfigurationSet(ctx, in)
			require.NoError(t, err)

			if !tt.viaCreate {
				_, err = client.PutConfigurationSetSuppressionOptions(
					ctx,
					&sesv2sdk.PutConfigurationSetSuppressionOptionsInput{
						ConfigurationSetName: aws.String("cs"),
						ValidationOptions:    tt.opts.ValidationOptions,
					},
				)
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
			}

			got, err := client.GetConfigurationSet(
				ctx,
				&sesv2sdk.GetConfigurationSetInput{ConfigurationSetName: aws.String("cs")},
			)
			require.NoError(t, err)
			require.NotNil(t, got.SuppressionOptions.ValidationOptions)

			cond := got.SuppressionOptions.ValidationOptions.ConditionThreshold
			assert.Equal(t, tt.wantEnabled, cond.ConditionThresholdEnabled)

			if tt.wantVerdict == "" {
				assert.Nil(t, cond.OverallConfidenceThreshold)
			} else {
				assert.Equal(t, tt.wantVerdict, cond.OverallConfidenceThreshold.ConfidenceVerdictThreshold)
			}
		})
	}
}

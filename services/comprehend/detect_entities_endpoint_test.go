package comprehend_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	comprehendsdk "github.com/aws/aws-sdk-go-v2/service/comprehend"
	"github.com/aws/aws-sdk-go-v2/service/comprehend/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

// TestDetectEntitiesEndpointArn proves DetectEntitiesInput.EndpointArn
// (dropped from the wire entirely before this fix) is read and applied: a
// custom entity recognition endpoint's own configured entity types are used
// for detection instead of the built-in generic vocabulary, and an unknown
// endpoint fails.
func TestDetectEntitiesEndpointArn(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestComprehendSDKClient(t, comprehend.NewHandler(backend))
	ctx := t.Context()

	recognizerOut, err := client.CreateEntityRecognizer(ctx, &comprehendsdk.CreateEntityRecognizerInput{
		RecognizerName:    aws.String("s18-custom-recognizer"),
		LanguageCode:      types.LanguageCodeEn,
		DataAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/comprehend-role"),
		InputDataConfig: &types.EntityRecognizerInputDataConfig{
			EntityTypes: []types.EntityTypesListItem{
				{Type: aws.String("CUSTOM_PRODUCT")},
			},
		},
	})
	require.NoError(t, err)
	recognizerArn := aws.ToString(recognizerOut.EntityRecognizerArn)

	endpointOut, err := client.CreateEndpoint(ctx, &comprehendsdk.CreateEndpointInput{
		EndpointName:          aws.String("s18-endpoint"),
		ModelArn:              aws.String(recognizerArn),
		DesiredInferenceUnits: aws.Int32(1),
	})
	require.NoError(t, err)
	endpointArn := aws.ToString(endpointOut.EndpointArn)

	customOut, err := client.DetectEntities(ctx, &comprehendsdk.DetectEntitiesInput{
		Text:        aws.String("Widget Gizmo"),
		EndpointArn: aws.String(endpointArn),
	})
	require.NoError(t, err)
	require.NotEmpty(t, customOut.Entities, "capitalized words must still be detected")

	for _, e := range customOut.Entities {
		assert.Equal(t, "CUSTOM_PRODUCT", string(e.Type),
			"a custom recognizer endpoint must use its own configured entity type, not the built-in vocabulary")
	}

	builtinOut, err := client.DetectEntities(ctx, &comprehendsdk.DetectEntitiesInput{
		Text:         aws.String("Widget Gizmo"),
		LanguageCode: types.LanguageCodeEn,
	})
	require.NoError(t, err)
	require.NotEmpty(t, builtinOut.Entities)

	for _, e := range builtinOut.Entities {
		assert.NotEqual(t, "CUSTOM_PRODUCT", string(e.Type),
			"without EndpointArn, detection must use the built-in vocabulary, not the custom type")
	}

	_, err = client.DetectEntities(ctx, &comprehendsdk.DetectEntitiesInput{
		Text:        aws.String("Widget Gizmo"),
		EndpointArn: aws.String("arn:aws:comprehend:us-east-1:000000000000:entity-recognizer-endpoint/does-not-exist"),
	})
	require.Error(t, err, "an unknown EndpointArn must fail")
}

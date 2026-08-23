package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// TestTrainingPlanExtension_TimestampsDecodeAsEpoch drives
// SearchTrainingPlanOfferings and ExtendTrainingPlan through the real
// aws-sdk-go-v2 sagemaker client. TrainingPlanExtensionOffering/
// TrainingPlanExtension's StartDate/EndDate/ExtendedAt deserialize from a
// json.Number via ParseEpochSeconds (deserializers.go) -- gopherstack
// previously emitted RFC3339 strings there via the plain time.Time struct
// fields, which fails every real client's decode outright once a training
// plan has any extension offering or purchased extension.
func TestTrainingPlanExtension_TimestampsDecodeAsEpoch(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("123456789012", "us-east-1")
	h := sagemaker.NewHandler(b)
	client := newTestSageMakerClient(t, h)

	plan, err := b.CreateTrainingPlan(t.Context(), "my-plan", "tpo-p5-48xlarge-30d", 0, nil)
	require.NoError(t, err)

	searchOut, err := client.SearchTrainingPlanOfferings(t.Context(), &sagemakersdk.SearchTrainingPlanOfferingsInput{
		TrainingPlanArn: aws.String(plan.TrainingPlanArn),
	})
	require.NoError(t, err, "real SDK client must decode SearchTrainingPlanOfferings without error")
	require.NotEmpty(t, searchOut.TrainingPlanExtensionOfferings)
	offering := searchOut.TrainingPlanExtensionOfferings[0]
	assert.NotNil(t, offering.StartDate)
	assert.NotNil(t, offering.EndDate)

	extendOut, err := client.ExtendTrainingPlan(t.Context(), &sagemakersdk.ExtendTrainingPlanInput{
		TrainingPlanExtensionOfferingId: offering.TrainingPlanExtensionOfferingId,
	})
	require.NoError(t, err, "real SDK client must decode ExtendTrainingPlan without error")
	require.NotEmpty(t, extendOut.TrainingPlanExtensions)
	ext := extendOut.TrainingPlanExtensions[0]
	assert.NotNil(t, ext.ExtendedAt)
	assert.NotNil(t, ext.StartDate)
	assert.NotNil(t, ext.EndDate)
}

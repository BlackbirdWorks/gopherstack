package comprehend_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	comprehendsdk "github.com/aws/aws-sdk-go-v2/service/comprehend"
	"github.com/aws/aws-sdk-go-v2/service/comprehend/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

// newTestComprehendSDKClient stands up the real aws-sdk-go-v2 comprehend
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production -- so a
// shape is verified by the real client's own
// awsAwsjson11_deserializeOpDocument<Op>Output, not gopherstack's own map
// keys.
func newTestComprehendSDKClient(t *testing.T, h *comprehend.Handler) *comprehendsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return comprehendsdk.NewFromConfig(cfg, func(o *comprehendsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestDetectTargetedSentiment_EntityShapeSDKRoundTrip proves
// DetectTargetedSentimentOutput.Entities is types.TargetedSentimentEntity
// (aws-sdk-go-v2/service/comprehend@v1.43.4 types/types.go:2799 -- only
// DescriptiveMentionIndex+Mentions), NOT a flattened Text/Score/
// BeginOffset/EndOffset/Type/Mentions shape. Before this fix, handler_
// detection.go's detectTargetedSentiment built each entity by starting
// from matchResult() (Text/Score/BeginOffset/EndOffset/Type) and bolting a
// "Mentions" key onto it -- those five extra top-level keys don't exist on
// types.TargetedSentimentEntity at all (confirmed against deserializers.go:
// 20032's awsAwsjson11_deserializeDocumentTargetedSentimentEntity, whose
// switch only recognizes "DescriptiveMentionIndex"/"Mentions" and silently
// drops any other key via its default case) -- and the real per-mention
// fields (types.TargetedSentimentMention: Text/Type/Score/BeginOffset/
// EndOffset/MentionSentiment, types/types.go:2821) were duplicated by hand
// instead of derived from the same mention object.
func TestDetectTargetedSentiment_EntityShapeSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	out, err := client.DetectTargetedSentiment(t.Context(), &comprehendsdk.DetectTargetedSentimentInput{
		LanguageCode: "en",
		Text:         aws.String("Alice loves the new product."),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Entities)

	entity := out.Entities[0]
	require.NotEmpty(t, entity.Mentions, "TargetedSentimentEntity.Mentions must be populated")
	require.NotEmpty(t, entity.DescriptiveMentionIndex, "DescriptiveMentionIndex must point into Mentions")
	assert.Equal(t, int32(0), entity.DescriptiveMentionIndex[0])

	mention := entity.Mentions[0]
	assert.NotEmpty(t, aws.ToString(mention.Text), "TargetedSentimentMention.Text must be populated")
	assert.NotNil(t, mention.Score)
	assert.NotNil(t, mention.BeginOffset)
	assert.NotNil(t, mention.EndOffset)
	require.NotNil(t, mention.MentionSentiment)
	assert.NotEmpty(t, string(mention.MentionSentiment.Sentiment))
}

// TestPutResourcePolicy_RevisionMismatchSDKRoundTrip proves a stale
// PolicyRevisionId on PutResourcePolicy surfaces as
// types.InvalidRequestException, not types.ResourceInUseException.
// PutResourcePolicy's own awsAwsjson11_deserializeOpErrorPutResourcePolicy
// switch (aws-sdk-go-v2/service/comprehend@v1.43.4 deserializers.go) types
// only InternalServerException, InvalidRequestException and
// ResourceNotFoundException -- ResourceInUseException is absent, so a
// ResourceInUseException wire code decodes to an untyped
// smithy.GenericAPIError and errors.As into *types.ResourceInUseException
// fails.
func TestPutResourcePolicy_RevisionMismatchSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	arn := "arn:aws:comprehend:us-east-1:000000000000:document-classifier/test"

	_, err := client.PutResourcePolicy(t.Context(), &comprehendsdk.PutResourcePolicyInput{
		ResourceArn:    aws.String(arn),
		ResourcePolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.PutResourcePolicy(t.Context(), &comprehendsdk.PutResourcePolicyInput{
		ResourceArn:      aws.String(arn),
		ResourcePolicy:   aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		PolicyRevisionId: aws.String("stale-revision"),
	})
	require.Error(t, err)

	var invalidRequest *types.InvalidRequestException
	require.ErrorAs(t, err, &invalidRequest)

	var resourceInUse *types.ResourceInUseException
	assert.NotErrorAs(t, err, &resourceInUse)
}

// TestDeleteResourcePolicy_RevisionMismatchSDKRoundTrip is the DeleteResourcePolicy
// counterpart: its deserializeOpError switch models the identical three
// codes as PutResourcePolicy's, also excluding ResourceInUseException.
func TestDeleteResourcePolicy_RevisionMismatchSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	arn := "arn:aws:comprehend:us-east-1:000000000000:document-classifier/test"

	_, err := client.PutResourcePolicy(t.Context(), &comprehendsdk.PutResourcePolicyInput{
		ResourceArn:    aws.String(arn),
		ResourcePolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteResourcePolicy(t.Context(), &comprehendsdk.DeleteResourcePolicyInput{
		ResourceArn:      aws.String(arn),
		PolicyRevisionId: aws.String("stale-revision"),
	})
	require.Error(t, err)

	var invalidRequest *types.InvalidRequestException
	require.ErrorAs(t, err, &invalidRequest)

	var resourceInUse *types.ResourceInUseException
	assert.NotErrorAs(t, err, &resourceInUse)
}

// TestCreateEndpoint_StatusSDKRoundTrip proves a freshly created Endpoint's
// Status is a real types.EndpointStatus value and CurrentInferenceUnits is
// populated. types.EndpointStatus's generated const set
// (aws-sdk-go-v2/service/comprehend@v1.43.4/types/enums.go:248-256) is
// CREATING/DELETING/FAILED/IN_SERVICE/UPDATING -- there is no "ACTIVE" value
// (EndpointProperties.Status's doc comment text mentioning "Ready" is stale
// relative to the generated enum). Before this fix, store.go's
// initialResourceStatus set a freshly created endpoint to the invented
// literal "ACTIVE", which a real client's readiness check
// (resp.Status == types.EndpointStatusInService, the pattern Terraform/CDK
// waiters use) would never observe -- and CurrentInferenceUnits, a real
// EndpointProperties member (types/types.go:1230-1284), was never set at
// all since resourceMap only ever echoed the request's DesiredInferenceUnits
// key back verbatim.
func TestCreateEndpoint_StatusSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	created, err := client.CreateEndpoint(t.Context(), &comprehendsdk.CreateEndpointInput{
		EndpointName:          aws.String("test-endpoint"),
		DesiredInferenceUnits: aws.Int32(2),
	})
	require.NoError(t, err)

	desc, err := client.DescribeEndpoint(t.Context(), &comprehendsdk.DescribeEndpointInput{
		EndpointArn: created.EndpointArn,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.EndpointProperties)
	assert.Equal(t, types.EndpointStatusInService, desc.EndpointProperties.Status)
	require.NotNil(t, desc.EndpointProperties.CurrentInferenceUnits, "CurrentInferenceUnits must be populated")
	assert.Equal(t, int32(2), *desc.EndpointProperties.CurrentInferenceUnits)
}

// TestUpdateEndpoint_ModelArnConvergesSDKRoundTrip proves that after
// UpdateEndpoint accepts a DesiredModelArn/DesiredInferenceUnits change, a
// subsequent DescribeEndpoint reflects the change on the "current" fields
// (ModelArn/CurrentInferenceUnits) a real client actually reads to confirm
// the update took effect -- this emulator has no async update lag (matching
// every other fast-forward-to-terminal-state simplification in this
// service), so UpdateEndpointInput's "Desired*" members
// (aws-sdk-go-v2/service/comprehend@v1.43.4/api_op_UpdateEndpoint.go)
// converge immediately rather than sitting alongside a stale current value
// forever. Before this fix, UpdateResource's generic maps.Copy of the raw
// request body onto resource.Configuration stored "DesiredModelArn" as a new,
// never-reconciled key, leaving the original "ModelArn" (and the never-set
// CurrentInferenceUnits) permanently stale.
func TestUpdateEndpoint_ModelArnConvergesSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	modelA := "arn:aws:comprehend:us-east-1:000000000000:document-classifier/modelA"
	modelB := "arn:aws:comprehend:us-east-1:000000000000:document-classifier/modelB"

	created, err := client.CreateEndpoint(t.Context(), &comprehendsdk.CreateEndpointInput{
		EndpointName:          aws.String("converge-endpoint"),
		ModelArn:              aws.String(modelA),
		DesiredInferenceUnits: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.UpdateEndpoint(t.Context(), &comprehendsdk.UpdateEndpointInput{
		EndpointArn:           created.EndpointArn,
		DesiredModelArn:       aws.String(modelB),
		DesiredInferenceUnits: aws.Int32(5),
	})
	require.NoError(t, err)

	desc, err := client.DescribeEndpoint(t.Context(), &comprehendsdk.DescribeEndpointInput{
		EndpointArn: created.EndpointArn,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.EndpointProperties)
	assert.Equal(t, modelB, aws.ToString(desc.EndpointProperties.ModelArn), "ModelArn must converge to the new model")
	require.NotNil(t, desc.EndpointProperties.CurrentInferenceUnits)
	assert.Equal(t, int32(5), *desc.EndpointProperties.CurrentInferenceUnits)
}

// TestCreateFlywheel_StatusSDKRoundTrip proves a freshly created Flywheel's
// Status is a real types.FlywheelStatus value. The generated const set
// (types/enums.go:352-360) is CREATING/ACTIVE/UPDATING/DELETING/FAILED.
// Before this fix, initialResourceStatus set a freshly created flywheel to
// the invented literal "READY", which does not appear anywhere in
// FlywheelStatus.Values().
func TestCreateFlywheel_StatusSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	created, err := client.CreateFlywheel(t.Context(), &comprehendsdk.CreateFlywheelInput{
		FlywheelName:      aws.String("test-flywheel"),
		DataAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/comprehend-role"),
		DataLakeS3Uri:     aws.String("s3://bucket/prefix"),
		ModelType:         types.ModelTypeDocumentClassifier,
	})
	require.NoError(t, err)

	desc, err := client.DescribeFlywheel(t.Context(), &comprehendsdk.DescribeFlywheelInput{
		FlywheelArn: created.FlywheelArn,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.FlywheelProperties)
	assert.Equal(t, types.FlywheelStatusActive, desc.FlywheelProperties.Status)
}

// TestCreateDataset_StatusSDKRoundTrip proves a freshly created Dataset's
// Status is a real types.DatasetStatus value. The generated const set
// (types/enums.go:63-69) is CREATING/COMPLETED/FAILED -- DatasetProperties.
// Status's own doc comment (types/types.go:580-582) is explicit: "When the
// dataset is ready to use, the status changes to COMPLETED." Before this
// fix, initialResourceStatus set a freshly created dataset to the invented
// literal "READY", which is not a member of DatasetStatus at all.
func TestCreateDataset_StatusSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	created, err := client.CreateDataset(t.Context(), &comprehendsdk.CreateDatasetInput{
		DatasetName:     aws.String("test-dataset"),
		FlywheelArn:     aws.String("arn:aws:comprehend:us-east-1:000000000000:flywheel/test-flywheel"),
		InputDataConfig: &types.DatasetInputDataConfig{},
	})
	require.NoError(t, err)

	desc, err := client.DescribeDataset(t.Context(), &comprehendsdk.DescribeDatasetInput{
		DatasetArn: created.DatasetArn,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.DatasetProperties)
	assert.Equal(t, types.DatasetStatusCompleted, desc.DatasetProperties.Status)
}

// TestFlywheelIteration_StatusFieldSDKRoundTrip proves
// FlywheelIterationProperties.Status arrives under the real wire key
// "Status", not "FlywheelIterationStatus". Confirmed against
// awsAwsjson11_deserializeDocumentFlywheelIterationProperties
// (aws-sdk-go-v2/service/comprehend@v1.43.4 deserializers.go:16022): its
// switch recognizes exactly CreationTime/EndTime/EvaluatedModelArn/
// EvaluatedModelMetrics/EvaluationManifestS3Prefix/FlywheelArn/
// FlywheelIterationId/Message/Status/TrainedModelArn/TrainedModelMetrics --
// there is no "FlywheelIterationStatus" case at all, so a real client's
// FlywheelIterationProperties.Status was always the zero value regardless of
// what handler_flywheels.go's iterationMap emitted under that name. Also
// confirms Status is a real (non-invented) types.FlywheelIterationStatus
// value once training completes: the generated const set (types/enums.go:
// 325-334) is TRAINING/EVALUATING/COMPLETED/FAILED/STOP_REQUESTED/STOPPED --
// no SUBMITTED or IN_PROGRESS -- so gopherstack's original
// SUBMITTED->IN_PROGRESS->COMPLETED progression was doubly wrong (wrong key,
// wrong values).
func TestFlywheelIteration_StatusFieldSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	flywheel, err := client.CreateFlywheel(t.Context(), &comprehendsdk.CreateFlywheelInput{
		FlywheelName:      aws.String("iteration-flywheel"),
		DataAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/comprehend-role"),
		DataLakeS3Uri:     aws.String("s3://bucket/prefix"),
		ModelType:         types.ModelTypeDocumentClassifier,
	})
	require.NoError(t, err)

	started, err := client.StartFlywheelIteration(t.Context(), &comprehendsdk.StartFlywheelIterationInput{
		FlywheelArn: flywheel.FlywheelArn,
	})
	require.NoError(t, err)

	var last types.FlywheelIterationProperties
	for range 3 {
		desc, descErr := client.DescribeFlywheelIteration(t.Context(), &comprehendsdk.DescribeFlywheelIterationInput{
			FlywheelArn:         flywheel.FlywheelArn,
			FlywheelIterationId: started.FlywheelIterationId,
		})
		require.NoError(t, descErr)
		require.NotNil(t, desc.FlywheelIterationProperties)
		last = *desc.FlywheelIterationProperties

		assert.NotEmpty(t, string(last.Status), "Status must be populated under the real wire key")

		valid := map[types.FlywheelIterationStatus]bool{
			types.FlywheelIterationStatusTraining:      true,
			types.FlywheelIterationStatusEvaluating:    true,
			types.FlywheelIterationStatusCompleted:     true,
			types.FlywheelIterationStatusFailed:        true,
			types.FlywheelIterationStatusStopRequested: true,
			types.FlywheelIterationStatusStopped:       true,
		}
		assert.True(t, valid[last.Status], "unexpected FlywheelIterationStatus value %q", last.Status)
	}

	assert.Equal(t, types.FlywheelIterationStatusCompleted, last.Status)
}

// TestListFlywheelIterationHistory_Filter proves ListFlywheelIterationHistoryInput.Filter
// (types.FlywheelIterationFilter: CreationTimeBefore/CreationTimeAfter,
// api_op_ListFlywheelIterationHistory.go) constrains the returned iterations.
func TestListFlywheelIterationHistory_Filter(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	flywheel, err := client.CreateFlywheel(t.Context(), &comprehendsdk.CreateFlywheelInput{
		FlywheelName:      aws.String("filter-flywheel"),
		DataAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/comprehend-role"),
		DataLakeS3Uri:     aws.String("s3://bucket/prefix"),
		ModelType:         types.ModelTypeDocumentClassifier,
	})
	require.NoError(t, err)

	_, err = client.StartFlywheelIteration(t.Context(), &comprehendsdk.StartFlywheelIterationInput{
		FlywheelArn: flywheel.FlywheelArn,
	})
	require.NoError(t, err)

	// CreationTimeAfter/Before are wire-encoded as whole epoch seconds (see
	// filterTime, handler_jobs.go), so the two iterations must land in
	// different UTC seconds for the filter to distinguish them.
	time.Sleep(1200 * time.Millisecond)
	cutoff := time.Now()
	time.Sleep(1200 * time.Millisecond)

	second, err := client.StartFlywheelIteration(t.Context(), &comprehendsdk.StartFlywheelIterationInput{
		FlywheelArn: flywheel.FlywheelArn,
	})
	require.NoError(t, err)

	out, err := client.ListFlywheelIterationHistory(t.Context(), &comprehendsdk.ListFlywheelIterationHistoryInput{
		FlywheelArn: flywheel.FlywheelArn,
		Filter:      &types.FlywheelIterationFilter{CreationTimeAfter: aws.Time(cutoff)},
	})
	require.NoError(t, err)
	require.Len(t, out.FlywheelIterationPropertiesList, 1)
	assert.Equal(
		t,
		aws.ToString(second.FlywheelIterationId),
		aws.ToString(out.FlywheelIterationPropertiesList[0].FlywheelIterationId),
	)

	before, err := client.ListFlywheelIterationHistory(t.Context(), &comprehendsdk.ListFlywheelIterationHistoryInput{
		FlywheelArn: flywheel.FlywheelArn,
		Filter:      &types.FlywheelIterationFilter{CreationTimeBefore: aws.Time(cutoff)},
	})
	require.NoError(t, err)
	require.Len(t, before.FlywheelIterationPropertiesList, 1)
	assert.NotEqual(
		t,
		aws.ToString(second.FlywheelIterationId),
		aws.ToString(before.FlywheelIterationPropertiesList[0].FlywheelIterationId),
	)
}

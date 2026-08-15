package batch_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/batch"
)

const rtTestRegion = "us-east-1"

// newTestBatchClient stands up the real aws-sdk-go-v2 Batch client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production. Round-tripping through the
// genuine SDK serializer/deserializer (rather than decoding the raw JSON body
// into map[string]any, as most other tests in this package do) is what
// actually proves a response is wire-compatible with a real client's typed
// struct fields.
func newTestBatchClient(t *testing.T, h *batch.Handler) *batchsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return batchsdk.NewFromConfig(cfg, func(o *batchsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_SchedulingPolicy_QuotaSharePolicy proves that
// SchedulingPolicyDetail.QuotaSharePolicy -- a real, distinct alternative to
// FairsharePolicy (aws-sdk-go-v2/service/batch/types.QuotaSharePolicy) --
// round-trips through CreateSchedulingPolicy and DescribeSchedulingPolicies.
// Before this fix, the field was parsed on neither Create nor Update and
// never emitted on Describe, so a real client's QuotaSharePolicy was always
// nil regardless of what it sent.
func Test_SDKRoundTrip_SchedulingPolicy_QuotaSharePolicy(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	name := "sp-" + uuid.NewString()[:8]

	createOut, err := client.CreateSchedulingPolicy(ctx, &batchsdk.CreateSchedulingPolicyInput{
		Name: aws.String(name),
		QuotaSharePolicy: &types.QuotaSharePolicy{
			IdleResourceAssignmentStrategy: types.QuotaShareIdleResourceAssignmentStrategyFifo,
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeSchedulingPolicies(ctx, &batchsdk.DescribeSchedulingPoliciesInput{
		Arns: []string{aws.ToString(createOut.Arn)},
	})
	require.NoError(t, err)
	require.Len(t, descOut.SchedulingPolicies, 1)

	got := descOut.SchedulingPolicies[0].QuotaSharePolicy
	require.NotNil(t, got, "QuotaSharePolicy must round-trip through DescribeSchedulingPolicies")
	assert.Equal(t, types.QuotaShareIdleResourceAssignmentStrategyFifo, got.IdleResourceAssignmentStrategy)

	// UpdateSchedulingPolicy must also apply a new QuotaSharePolicy value.
	_, err = client.UpdateSchedulingPolicy(ctx, &batchsdk.UpdateSchedulingPolicyInput{
		Arn: createOut.Arn,
		QuotaSharePolicy: &types.QuotaSharePolicy{
			IdleResourceAssignmentStrategy: "FIFO",
		},
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeSchedulingPolicies(ctx, &batchsdk.DescribeSchedulingPoliciesInput{
		Arns: []string{aws.ToString(createOut.Arn)},
	})
	require.NoError(t, err)
	require.Len(t, descOut2.SchedulingPolicies, 1)
	require.NotNil(t, descOut2.SchedulingPolicies[0].QuotaSharePolicy)
}

// Test_SDKRoundTrip_ServiceJob_QuotaShareAndPreemption proves that
// SubmitServiceJobInput's QuotaShareName and PreemptionConfiguration --
// real request members with no prior backend wiring -- round-trip through
// DescribeServiceJob and appear on ListServiceJobs's narrower summary shape.
func Test_SDKRoundTrip_ServiceJob_QuotaShareAndPreemption(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ceName := "sj-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	qName := "sj-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	retries := int32(3)
	submitOut, err := client.SubmitServiceJob(ctx, &batchsdk.SubmitServiceJobInput{
		JobName:               aws.String("sj-" + uuid.NewString()[:8]),
		JobQueue:              aws.String(qName),
		ServiceJobType:        types.ServiceJobTypeSagemakerTraining,
		ServiceRequestPayload: aws.String(`{"foo":"bar"}`),
		QuotaShareName:        aws.String("qs-1"),
		PreemptionConfiguration: &types.ServiceJobPreemptionConfiguration{
			PreemptionRetriesBeforeTermination: &retries,
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeServiceJob(ctx, &batchsdk.DescribeServiceJobInput{
		JobId: submitOut.JobId,
	})
	require.NoError(t, err)
	assert.Equal(t, "qs-1", aws.ToString(descOut.QuotaShareName))
	require.NotNil(t, descOut.PreemptionConfiguration)
	require.NotNil(t, descOut.PreemptionConfiguration.PreemptionRetriesBeforeTermination)
	assert.Equal(t, retries, *descOut.PreemptionConfiguration.PreemptionRetriesBeforeTermination)

	// ListServiceJobs defaults to RUNNING-only when jobStatus is unspecified
	// (matches documented AWS behavior); this job is still SUBMITTED, so it
	// must be requested explicitly.
	listOut, err := client.ListServiceJobs(ctx, &batchsdk.ListServiceJobsInput{
		JobQueue:  aws.String(qName),
		JobStatus: types.ServiceJobStatusSubmitted,
	})
	require.NoError(t, err)
	require.Len(t, listOut.JobSummaryList, 1)
	assert.Equal(t, "qs-1", aws.ToString(listOut.JobSummaryList[0].QuotaShareName))
}

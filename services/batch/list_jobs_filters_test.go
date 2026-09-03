package batch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// Test_SDKRoundTrip_ListJobs_Filters proves ListJobsInput.Filters (a
// []types.KeyValuesPair, e.g. JOB_NAME with case-insensitive prefix-star
// matching per api_op_ListJobs.go) is actually applied. Before this fix,
// listJobsInput had no Filters field at all -- the handler never read it, so
// every real client's Filters was silently dropped and ListJobs returned the
// full (status-filtered) set regardless of what was requested.
func Test_SDKRoundTrip_ListJobs_Filters(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ceName := "ljf-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	qName := "ljf-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	jdName := "ljf-jd-" + uuid.NewString()[:8]
	_, err = client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName: aws.String(jdName),
		Type:              types.JobDefinitionTypeContainer,
		ContainerProperties: &types.ContainerProperties{
			Image: aws.String("busybox"),
		},
	})
	require.NoError(t, err)

	suffix := uuid.NewString()[:8]

	_, err = client.SubmitJob(ctx, &batchsdk.SubmitJobInput{
		JobName:       aws.String("alpha-" + suffix),
		JobQueue:      aws.String(qName),
		JobDefinition: aws.String(jdName),
	})
	require.NoError(t, err)

	_, err = client.SubmitJob(ctx, &batchsdk.SubmitJobInput{
		JobName:       aws.String("beta-" + suffix),
		JobQueue:      aws.String(qName),
		JobDefinition: aws.String(jdName),
	})
	require.NoError(t, err)

	// Real behavior: JOB_NAME matches case-insensitively, and a trailing '*'
	// is a prefix match, so "ALPHA-*" (uppercase, wrong case from the actual
	// job name) must still match "alpha-<suffix>" and must not match "beta-*".
	listOut, err := client.ListJobs(ctx, &batchsdk.ListJobsInput{
		JobQueue: aws.String(qName),
		Filters: []types.KeyValuesPair{
			{Name: aws.String("JOB_NAME"), Values: []string{"ALPHA-*"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, listOut.JobSummaryList, 1, "JOB_NAME filter must be applied and be case-insensitive")
	require.Equal(t, "alpha-"+suffix, aws.ToString(listOut.JobSummaryList[0].JobName))
}

// Test_SDKRoundTrip_ListConsumableResources_Filters proves
// ListConsumableResourcesInput.Filters (CONSUMABLE_RESOURCE_NAME,
// case-insensitive with trailing '*' prefix matching per
// api_op_ListConsumableResources.go) is applied. Before this fix,
// listConsumableResourcesInput had no Filters field at all -- the handler
// never read it, so every real client's Filters was silently dropped.
func Test_SDKRoundTrip_ListConsumableResources_Filters(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]

	_, err := client.CreateConsumableResource(ctx, &batchsdk.CreateConsumableResourceInput{
		ConsumableResourceName: aws.String("alpha-res-" + suffix),
		TotalQuantity:          aws.Int64(10),
	})
	require.NoError(t, err)

	_, err = client.CreateConsumableResource(ctx, &batchsdk.CreateConsumableResourceInput{
		ConsumableResourceName: aws.String("beta-res-" + suffix),
		TotalQuantity:          aws.Int64(10),
	})
	require.NoError(t, err)

	listOut, err := client.ListConsumableResources(ctx, &batchsdk.ListConsumableResourcesInput{
		Filters: []types.KeyValuesPair{
			{Name: aws.String("CONSUMABLE_RESOURCE_NAME"), Values: []string{"ALPHA-RES-*"}},
		},
	})
	require.NoError(t, err)
	require.Len(
		t, listOut.ConsumableResources, 1,
		"CONSUMABLE_RESOURCE_NAME filter must be applied and be case-insensitive",
	)
	require.Equal(t, "alpha-res-"+suffix, aws.ToString(listOut.ConsumableResources[0].ConsumableResourceName))
}

// Test_SDKRoundTrip_ListServiceJobs_MaxResultsAndFilters proves
// ListServiceJobsInput.MaxResults/NextToken and Filters (JOB_NAME etc., per
// api_op_ListServiceJobs.go) are applied. Before this fix, listServiceJobsInput
// had neither field -- the handler always returned every service job in the
// queue regardless of maxResults, and Filters was silently dropped.
func Test_SDKRoundTrip_ListServiceJobs_MaxResultsAndFilters(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ceName := "lsj-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	qName := "lsj-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	suffix := uuid.NewString()[:8]

	_, err = client.SubmitServiceJob(ctx, &batchsdk.SubmitServiceJobInput{
		JobName:               aws.String("alpha-sj-" + suffix),
		JobQueue:              aws.String(qName),
		ServiceJobType:        types.ServiceJobTypeSagemakerTraining,
		ServiceRequestPayload: aws.String(`{"foo":"bar"}`),
	})
	require.NoError(t, err)

	_, err = client.SubmitServiceJob(ctx, &batchsdk.SubmitServiceJobInput{
		JobName:               aws.String("beta-sj-" + suffix),
		JobQueue:              aws.String(qName),
		ServiceJobType:        types.ServiceJobTypeSagemakerTraining,
		ServiceRequestPayload: aws.String(`{"foo":"bar"}`),
	})
	require.NoError(t, err)

	listOut, err := client.ListServiceJobs(ctx, &batchsdk.ListServiceJobsInput{
		JobQueue: aws.String(qName),
		Filters: []types.KeyValuesPair{
			{Name: aws.String("JOB_NAME"), Values: []string{"ALPHA-SJ-*"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, listOut.JobSummaryList, 1, "JOB_NAME filter must be applied")
	require.Equal(t, "alpha-sj-"+suffix, aws.ToString(listOut.JobSummaryList[0].JobName))

	allOut, err := client.ListServiceJobs(ctx, &batchsdk.ListServiceJobsInput{
		JobQueue:   aws.String(qName),
		MaxResults: aws.Int32(1),
		Filters: []types.KeyValuesPair{
			{Name: aws.String("JOB_NAME"), Values: []string{"alpha-sj-*", "beta-sj-*"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, allOut.JobSummaryList, 1, "maxResults must truncate the page")
	require.NotEmpty(t, aws.ToString(allOut.NextToken), "a truncated page must return a NextToken")
}

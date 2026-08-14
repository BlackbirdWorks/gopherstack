package omics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

// This file proves, through the real aws-sdk-go-v2 omics client, that ten
// List ops decode a non-empty collection. Before the fix each handler wrapped
// its list under a resource-specific key (e.g. "runGroups", "workflows",
// "importJobs") copied from the Get/Start sibling shapes; the real wire key
// for most of these ops is the generic "items" (or, for the two import-job
// families, "annotationImportJobs"/"variantImportJobs" rather than the
// generic "importJobs" ListReferenceImportJobs/ListReadSetImportJobs use).
// The SDK deserializer's switch never matched the wrong key, so every one of
// these collections decoded as a silent empty/nil slice with err == nil --
// confirmed against omics@v1.49.5 deserializers.go's
// awsRestjson1_deserializeOpDocumentList<Op>Output functions.

func TestSDKRoundTrip_ListRunGroups_Items(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateRunGroup(t.Context(), &omicssdk.CreateRunGroupInput{
		Name:      aws.String("rg-wrapper-test"),
		RequestId: aws.String(uuid.NewString()),
	})
	require.NoError(t, err)

	listed, err := client.ListRunGroups(t.Context(), &omicssdk.ListRunGroupsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listed.Items, "ListRunGroupsOutput.Items must decode a non-empty slice")
	assert.Equal(t, "rg-wrapper-test", aws.ToString(listed.Items[0].Name))
}

func TestSDKRoundTrip_ListRuns_Items(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	wf, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
		Name:      aws.String("wf-for-runs"),
		Engine:    types.WorkflowEngineWdl,
		RequestId: aws.String(uuid.NewString()),
	})
	require.NoError(t, err)

	_, err = client.StartRun(t.Context(), &omicssdk.StartRunInput{
		WorkflowId: wf.Id,
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/role"),
		OutputUri:  aws.String("s3://bucket/out/"),
		RequestId:  aws.String(uuid.NewString()),
		Name:       aws.String("run-wrapper-test"),
	})
	require.NoError(t, err)

	listed, err := client.ListRuns(t.Context(), &omicssdk.ListRunsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listed.Items, "ListRunsOutput.Items must decode a non-empty slice")
	assert.Equal(t, "run-wrapper-test", aws.ToString(listed.Items[0].Name))
}

func TestSDKRoundTrip_ListRunTasks_Items(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	wf, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
		Name:      aws.String("wf-for-tasks"),
		Engine:    types.WorkflowEngineWdl,
		RequestId: aws.String(uuid.NewString()),
	})
	require.NoError(t, err)

	run, err := client.StartRun(t.Context(), &omicssdk.StartRunInput{
		WorkflowId: wf.Id,
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/role"),
		OutputUri:  aws.String("s3://bucket/out/"),
		RequestId:  aws.String(uuid.NewString()),
	})
	require.NoError(t, err)

	listed, err := client.ListRunTasks(t.Context(), &omicssdk.ListRunTasksInput{Id: run.Id})
	require.NoError(t, err)
	require.NotEmpty(t, listed.Items, "ListRunTasksOutput.Items must decode a non-empty slice")
}

func TestSDKRoundTrip_ListRunCaches_Items(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateRunCache(t.Context(), &omicssdk.CreateRunCacheInput{
		CacheS3Location: aws.String("s3://bucket/cache/"),
		RequestId:       aws.String(uuid.NewString()),
		Name:            aws.String("cache-wrapper-test"),
		CacheBehavior:   types.CacheBehaviorCacheAlways,
	})
	require.NoError(t, err)

	listed, err := client.ListRunCaches(t.Context(), &omicssdk.ListRunCachesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listed.Items, "ListRunCachesOutput.Items must decode a non-empty slice")
	assert.Equal(t, "cache-wrapper-test", aws.ToString(listed.Items[0].Name))
}

func TestSDKRoundTrip_ListBatch_Items(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	wf, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
		Name:      aws.String("wf-for-batch"),
		Engine:    types.WorkflowEngineWdl,
		RequestId: aws.String(uuid.NewString()),
	})
	require.NoError(t, err)

	_, err = client.StartRunBatch(t.Context(), &omicssdk.StartRunBatchInput{
		RequestId: aws.String(uuid.NewString()),
		BatchName: aws.String("batch-wrapper-test"),
		DefaultRunSetting: &types.DefaultRunSetting{
			RoleArn:    aws.String("arn:aws:iam::000000000000:role/role"),
			WorkflowId: wf.Id,
		},
		BatchRunSettings: &types.BatchRunSettingsMemberInlineSettings{
			Value: []types.InlineSetting{{RunSettingId: aws.String("s1")}},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListBatch(t.Context(), &omicssdk.ListBatchInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listed.Items, "ListBatchOutput.Items must decode a non-empty slice")
	assert.Equal(t, "batch-wrapper-test", aws.ToString(listed.Items[0].Name))
}

func TestSDKRoundTrip_ListWorkflows_Items(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
		Name:      aws.String("wf-wrapper-test"),
		Engine:    types.WorkflowEngineWdl,
		RequestId: aws.String(uuid.NewString()),
	})
	require.NoError(t, err)

	listed, err := client.ListWorkflows(t.Context(), &omicssdk.ListWorkflowsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listed.Items, "ListWorkflowsOutput.Items must decode a non-empty slice")
	assert.Equal(t, "wf-wrapper-test", aws.ToString(listed.Items[0].Name))
}

func TestSDKRoundTrip_ListWorkflowVersions_Items(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	wf, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
		Name:      aws.String("wf-for-versions"),
		Engine:    types.WorkflowEngineWdl,
		RequestId: aws.String(uuid.NewString()),
	})
	require.NoError(t, err)

	_, err = client.CreateWorkflowVersion(t.Context(), &omicssdk.CreateWorkflowVersionInput{
		WorkflowId:  wf.Id,
		VersionName: aws.String("v1"),
		RequestId:   aws.String(uuid.NewString()),
	})
	require.NoError(t, err)

	listed, err := client.ListWorkflowVersions(t.Context(), &omicssdk.ListWorkflowVersionsInput{
		WorkflowId: wf.Id,
	})
	require.NoError(t, err)
	require.NotEmpty(t, listed.Items, "ListWorkflowVersionsOutput.Items must decode a non-empty slice")
	assert.Equal(t, "v1", aws.ToString(listed.Items[0].VersionName))
}

func TestSDKRoundTrip_ListConfigurations_Items(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateConfiguration(t.Context(), &omicssdk.CreateConfigurationInput{
		Name:              aws.String("cfg-wrapper-test"),
		RequestId:         aws.String(uuid.NewString()),
		RunConfigurations: &types.RunConfigurations{},
	})
	require.NoError(t, err)

	listed, err := client.ListConfigurations(t.Context(), &omicssdk.ListConfigurationsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listed.Items, "ListConfigurationsOutput.Items must decode a non-empty slice")
	assert.Equal(t, "cfg-wrapper-test", aws.ToString(listed.Items[0].Name))
}

func TestSDKRoundTrip_ListAnnotationImportJobs_AnnotationImportJobs(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("as-wrapper-test"),
		StoreFormat: types.StoreFormatVcf,
	})
	require.NoError(t, err)

	_, err = client.StartAnnotationImportJob(t.Context(), &omicssdk.StartAnnotationImportJobInput{
		DestinationName: aws.String("as-wrapper-test"),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
		Items: []types.AnnotationImportItemSource{
			{Source: aws.String("s3://bucket/ann.vcf")},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListAnnotationImportJobs(t.Context(), &omicssdk.ListAnnotationImportJobsInput{})
	require.NoError(t, err)
	require.NotEmpty(
		t, listed.AnnotationImportJobs,
		"ListAnnotationImportJobsOutput.AnnotationImportJobs must decode a non-empty slice",
	)
	assert.Equal(t, "as-wrapper-test", aws.ToString(listed.AnnotationImportJobs[0].DestinationName))
}

func TestSDKRoundTrip_ListVariantImportJobs_VariantImportJobs(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateVariantStore(t.Context(), &omicssdk.CreateVariantStoreInput{
		Name:      aws.String("vs-wrapper-test"),
		Reference: &types.ReferenceItemMemberReferenceArn{Value: testReferenceArn},
	})
	require.NoError(t, err)

	_, err = client.StartVariantImportJob(t.Context(), &omicssdk.StartVariantImportJobInput{
		DestinationName: aws.String("vs-wrapper-test"),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
		Items: []types.VariantImportItemSource{
			{Source: aws.String("s3://bucket/var.vcf")},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListVariantImportJobs(t.Context(), &omicssdk.ListVariantImportJobsInput{})
	require.NoError(t, err)
	require.NotEmpty(
		t, listed.VariantImportJobs,
		"ListVariantImportJobsOutput.VariantImportJobs must decode a non-empty slice",
	)
	assert.Equal(t, "vs-wrapper-test", aws.ToString(listed.VariantImportJobs[0].DestinationName))
}

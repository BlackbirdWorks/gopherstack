package omics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/document"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDroppedMembersWorkflow(t *testing.T, client *omicssdk.Client) string {
	t.Helper()

	out, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
		Name:   aws.String("wf-dropped"),
		Engine: types.WorkflowEngineWdl,
	})
	require.NoError(t, err)

	return aws.ToString(out.Id)
}

func TestSDK_SequenceStoreMembersAndToken(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	in := &omicssdk.CreateSequenceStoreInput{
		Name:                   aws.String("seq-dropped"),
		Description:            aws.String("first"),
		ClientToken:            aws.String("seq-token"),
		FallbackLocation:       aws.String("s3://fallback/"),
		PropagatedSetLevelTags: []string{"team", "env"},
		SseConfig:              testSseConfig(),
	}

	created, err := client.CreateSequenceStore(ctx, in)
	require.NoError(t, err)
	retry, err := client.CreateSequenceStore(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Id), aws.ToString(retry.Id))

	reused := *in
	reused.Name = aws.String("other-name")
	_, err = client.CreateSequenceStore(ctx, &reused)
	require.Error(t, err)

	got, err := client.GetSequenceStore(ctx, &omicssdk.GetSequenceStoreInput{Id: created.Id})
	require.NoError(t, err)
	assert.Equal(t, "s3://fallback/", aws.ToString(got.FallbackLocation))
	assert.Equal(t, []string{"team", "env"}, got.PropagatedSetLevelTags)
	require.NotNil(t, got.SseConfig)
	assert.Equal(t, types.EncryptionTypeKms, got.SseConfig.Type)
	assert.Equal(t, types.ETagAlgorithmFamilyMd5up, got.ETagAlgorithmFamily)

	upd, err := client.UpdateSequenceStore(ctx, &omicssdk.UpdateSequenceStoreInput{
		Id:                     created.Id,
		FallbackLocation:       aws.String("s3://new-fallback/"),
		PropagatedSetLevelTags: []string{"only"},
		S3AccessConfig:         &types.S3AccessConfig{AccessLogLocation: aws.String("s3://logs/")},
	})
	require.NoError(t, err)
	assert.Equal(t, "s3://new-fallback/", aws.ToString(upd.FallbackLocation))
	assert.Equal(t, []string{"only"}, upd.PropagatedSetLevelTags)
	assert.Equal(t, "first", aws.ToString(upd.Description))

	list, err := client.ListSequenceStores(ctx, &omicssdk.ListSequenceStoresInput{})
	require.NoError(t, err)
	require.Len(t, list.SequenceStores, 1)
	assert.Equal(t, "s3://new-fallback/", aws.ToString(list.SequenceStores[0].FallbackLocation))
}

func TestSDK_ReferenceStoreSseConfigAndToken(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	in := &omicssdk.CreateReferenceStoreInput{
		Name:        aws.String("ref-dropped"),
		ClientToken: aws.String("ref-token"),
		SseConfig:   testSseConfig(),
	}

	created, err := client.CreateReferenceStore(ctx, in)
	require.NoError(t, err)
	retry, err := client.CreateReferenceStore(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Id), aws.ToString(retry.Id))

	got, err := client.GetReferenceStore(ctx, &omicssdk.GetReferenceStoreInput{Id: created.Id})
	require.NoError(t, err)
	require.NotNil(t, got.SseConfig)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/k", aws.ToString(got.SseConfig.KeyArn))
}

func TestSDK_StoreDescriptionsAndVersionOptions(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateVariantStore(ctx, &omicssdk.CreateVariantStoreInput{
		Name:        aws.String("var-desc"),
		Description: aws.String("variants"),
		Reference: &types.ReferenceItemMemberReferenceArn{
			Value: "arn:aws:omics:us-east-1:000000000000:referenceStore/1/reference/1",
		},
	})
	require.NoError(t, err)

	varGot, err := client.GetVariantStore(ctx, &omicssdk.GetVariantStoreInput{Name: aws.String("var-desc")})
	require.NoError(t, err)
	assert.Equal(t, "variants", aws.ToString(varGot.Description))

	annOut, err := client.CreateAnnotationStore(ctx, &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("ann-desc"),
		Description: aws.String("annotations"),
		VersionName: aws.String("v1"),
		StoreFormat: types.StoreFormatVcf,
	})
	require.NoError(t, err)
	assert.Equal(t, types.StoreFormatVcf, annOut.StoreFormat)

	annGot, err := client.GetAnnotationStore(ctx, &omicssdk.GetAnnotationStoreInput{Name: aws.String("ann-desc")})
	require.NoError(t, err)
	assert.Equal(t, "annotations", aws.ToString(annGot.Description))
	assert.EqualValues(t, 1, aws.ToInt32(annGot.NumVersions))

	_, err = client.GetAnnotationStoreVersion(ctx, &omicssdk.GetAnnotationStoreVersionInput{
		Name: aws.String("ann-desc"), VersionName: aws.String("v1"),
	})
	require.NoError(t, err)

	opts := &types.VersionOptionsMemberTsvVersionOptions{Value: types.TsvVersionOptions{
		AnnotationType: types.AnnotationTypeGeneric,
		FormatToHeader: map[string]string{"CHR": "chromosome"},
	}}
	v2, err := client.CreateAnnotationStoreVersion(ctx, &omicssdk.CreateAnnotationStoreVersionInput{
		Name: aws.String("ann-desc"), VersionName: aws.String("v2"), VersionOptions: opts,
	})
	require.NoError(t, err)
	require.NotNil(t, v2.VersionOptions)

	v2Got, err := client.GetAnnotationStoreVersion(ctx, &omicssdk.GetAnnotationStoreVersionInput{
		Name: aws.String("ann-desc"), VersionName: aws.String("v2"),
	})
	require.NoError(t, err)

	gotOpts, ok := v2Got.VersionOptions.(*types.VersionOptionsMemberTsvVersionOptions)
	require.True(t, ok)
	assert.Equal(t, types.AnnotationTypeGeneric, gotOpts.Value.AnnotationType)
	assert.Equal(t, "chromosome", gotOpts.Value.FormatToHeader["CHR"])
}

func TestSDK_RunCacheDescriptionOwnerAndToken(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	in := &omicssdk.CreateRunCacheInput{
		Name:               aws.String("cache-dropped"),
		Description:        aws.String("shared cache"),
		CacheS3Location:    aws.String("s3://bucket/cache"),
		CacheBucketOwnerId: aws.String("123456789012"),
		RequestId:          aws.String("cache-token"),
	}

	created, err := client.CreateRunCache(ctx, in)
	require.NoError(t, err)
	retry, err := client.CreateRunCache(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Id), aws.ToString(retry.Id))

	got, err := client.GetRunCache(ctx, &omicssdk.GetRunCacheInput{Id: created.Id})
	require.NoError(t, err)
	assert.Equal(t, "shared cache", aws.ToString(got.Description))
	assert.Equal(t, "123456789012", aws.ToString(got.CacheBucketOwnerId))
}

func TestSDK_StartRunPriorityLogLevelAndToken(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()
	wfID := newDroppedMembersWorkflow(t, client)

	in := &omicssdk.StartRunInput{
		WorkflowId:      aws.String(wfID),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/r"),
		RequestId:       aws.String("run-token"),
		OutputUri:       aws.String("s3://bucket/out/"),
		Priority:        aws.Int32(7),
		LogLevel:        types.RunLogLevelAll,
		WorkflowOwnerId: aws.String("111122223333"),
		EngineSettings:  document.NewLazyDocument(map[string]any{"profile": "test"}),
	}

	started, err := client.StartRun(ctx, in)
	require.NoError(t, err)
	retry, err := client.StartRun(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(started.Id), aws.ToString(retry.Id))

	got, err := client.GetRun(ctx, &omicssdk.GetRunInput{Id: started.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 7, aws.ToInt32(got.Priority))
	assert.Equal(t, types.RunLogLevelAll, got.LogLevel)
	assert.Equal(t, "111122223333", aws.ToString(got.WorkflowOwnerId))
	require.NotNil(t, got.EngineSettings)

	list, err := client.ListRuns(ctx, &omicssdk.ListRunsInput{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.EqualValues(t, 7, aws.ToInt32(list.Items[0].Priority))
}

func TestSDK_StartRunBatchDefaultsAreApplied(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()
	wfID := newDroppedMembersWorkflow(t, client)

	in := &omicssdk.StartRunBatchInput{
		RequestId: aws.String("batch-token"),
		DefaultRunSetting: &types.DefaultRunSetting{
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/r"),
			WorkflowId:      aws.String(wfID),
			Priority:        aws.Int32(3),
			LogLevel:        types.RunLogLevelError,
			RunTags:         map[string]string{"shared": "1", "both": "default"},
			StorageType:     types.StorageTypeDynamic,
			NetworkingMode:  types.NetworkingModeVpc,
			EngineSettings:  document.NewLazyDocument(map[string]any{"profile": "batch"}),
			WorkflowOwnerId: aws.String("111122223333"),
		},
		BatchRunSettings: &types.BatchRunSettingsMemberInlineSettings{Value: []types.InlineSetting{
			{RunSettingId: aws.String("a")},
			{RunSettingId: aws.String("b"), Priority: aws.Int32(9), RunTags: map[string]string{"both": "inline"}},
		}},
	}

	started, err := client.StartRunBatch(ctx, in)
	require.NoError(t, err)
	retry, err := client.StartRunBatch(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(started.Id), aws.ToString(retry.Id))

	batch, err := client.GetBatch(ctx, &omicssdk.GetBatchInput{BatchId: started.Id})
	require.NoError(t, err)
	require.NotNil(t, batch.DefaultRunSetting)
	assert.EqualValues(t, 3, aws.ToInt32(batch.DefaultRunSetting.Priority))
	assert.Equal(t, types.RunLogLevelError, batch.DefaultRunSetting.LogLevel)

	runs, err := client.ListRuns(ctx, &omicssdk.ListRunsInput{})
	require.NoError(t, err)
	require.Len(t, runs.Items, 2)

	prios := map[int32]bool{}
	for _, r := range runs.Items {
		prios[aws.ToInt32(r.Priority)] = true
		assert.Equal(t, types.StorageTypeDynamic, r.StorageType)

		run, getErr := client.GetRun(ctx, &omicssdk.GetRunInput{Id: r.Id})
		require.NoError(t, getErr)
		assert.Equal(t, "1", run.Tags["shared"])
		assert.Equal(t, types.RunLogLevelError, run.LogLevel)
		assert.Equal(t, types.NetworkingModeVpc, run.NetworkingMode)
	}

	assert.Equal(t, map[int32]bool{3: true, 9: true}, prios)
}

func TestSDK_WorkflowAcceleratorsMainRegistryAndToken(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	in := &omicssdk.CreateWorkflowInput{
		Name:         aws.String("wf-members"),
		Engine:       types.WorkflowEngineWdl,
		RequestId:    aws.String("wf-token"),
		Accelerators: types.AcceleratorsGpu,
		Main:         aws.String("workflows/main.wdl"),
		ContainerRegistryMap: &types.ContainerRegistryMap{
			RegistryMappings: []types.RegistryMapping{{
				UpstreamRegistryUrl:      aws.String("registry.example.com"),
				EcrRepositoryPrefix:      aws.String("example"),
				UpstreamRepositoryPrefix: aws.String("up"),
			}},
		},
		ReadmeMarkdown: aws.String("# one"),
	}

	created, err := client.CreateWorkflow(ctx, in)
	require.NoError(t, err)
	retry, err := client.CreateWorkflow(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Id), aws.ToString(retry.Id))

	_, err = client.UpdateWorkflow(ctx, &omicssdk.UpdateWorkflowInput{
		Id: created.Id, ReadmeMarkdown: aws.String("# two"),
	})
	require.NoError(t, err)

	got, err := client.GetWorkflow(ctx, &omicssdk.GetWorkflowInput{Id: created.Id})
	require.NoError(t, err)
	assert.Equal(t, types.AcceleratorsGpu, got.Accelerators)
	assert.Equal(t, "workflows/main.wdl", aws.ToString(got.Main))
	require.NotNil(t, got.ContainerRegistryMap)
	require.Len(t, got.ContainerRegistryMap.RegistryMappings, 1)
	assert.Equal(t, "# two", aws.ToString(got.Readme))

	verIn := &omicssdk.CreateWorkflowVersionInput{
		WorkflowId:   created.Id,
		VersionName:  aws.String("v1"),
		RequestId:    aws.String("ver-token"),
		Engine:       types.WorkflowEngineNextflow,
		Main:         aws.String("main.nf"),
		Accelerators: types.AcceleratorsGpu,
	}

	ver, err := client.CreateWorkflowVersion(ctx, verIn)
	require.NoError(t, err)
	verRetry, err := client.CreateWorkflowVersion(ctx, verIn)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(ver.Arn), aws.ToString(verRetry.Arn))
	assert.NotEmpty(t, aws.ToString(ver.Uuid))

	_, err = client.UpdateWorkflowVersion(ctx, &omicssdk.UpdateWorkflowVersionInput{
		WorkflowId: created.Id, VersionName: aws.String("v1"), ReadmeMarkdown: aws.String("# v1 readme"),
	})
	require.NoError(t, err)

	verGot, err := client.GetWorkflowVersion(ctx, &omicssdk.GetWorkflowVersionInput{
		WorkflowId: created.Id, VersionName: aws.String("v1"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.WorkflowEngineNextflow, verGot.Engine)
	assert.Equal(t, "main.nf", aws.ToString(verGot.Main))
	assert.Equal(t, "# v1 readme", aws.ToString(verGot.Readme))
}

func testSseConfig() *types.SseConfig {
	return &types.SseConfig{
		Type:   types.EncryptionTypeKms,
		KeyArn: aws.String("arn:aws:kms:us-east-1:000000000000:key/k"),
	}
}

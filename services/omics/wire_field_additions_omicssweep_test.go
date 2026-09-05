package omics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

// registerTestWorkflow creates a minimal workflow for use as StartRun's
// workflowId in these tests; the fields under test here don't depend on its
// content.
func registerTestWorkflow(t *testing.T, client *omicssdk.Client) string {
	t.Helper()

	out, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
		Name:   aws.String("wire-sweep-workflow"),
		Engine: types.WorkflowEngineWdl,
	})
	require.NoError(t, err)

	return *out.Id
}

// TestOmics_CreateRunCache_UpdateRunCache_CacheBehavior_Echoed proves
// CreateRunCacheInput.CacheBehavior's own doc comment: "If you don't specify
// a value, the default behavior is CACHE_ON_FAILURE." The create test omits
// the field entirely; the update test proves an explicit value is applied
// and echoed (UpdateRunCacheInput.CacheBehavior was entirely undeclared
// before this fix).
func TestOmics_CreateRunCache_UpdateRunCache_CacheBehavior_Echoed(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	createOut, err := client.CreateRunCache(t.Context(), &omicssdk.CreateRunCacheInput{
		Name:            aws.String("cache-default-behavior"),
		CacheS3Location: aws.String("s3://bucket/cache"),
	})
	require.NoError(t, err)

	getOut, err := client.GetRunCache(t.Context(), &omicssdk.GetRunCacheInput{Id: createOut.Id})
	require.NoError(t, err)
	require.Equal(t, types.CacheBehaviorCacheOnFailure, getOut.CacheBehavior)

	_, err = client.UpdateRunCache(t.Context(), &omicssdk.UpdateRunCacheInput{
		Id:            createOut.Id,
		CacheBehavior: types.CacheBehaviorCacheAlways,
	})
	require.NoError(t, err)

	getOut2, err := client.GetRunCache(t.Context(), &omicssdk.GetRunCacheInput{Id: createOut.Id})
	require.NoError(t, err)
	require.Equal(t, types.CacheBehaviorCacheAlways, getOut2.CacheBehavior)
}

// TestOmics_StartRun_DocumentedDefaults_Omitted proves five StartRunInput
// fields' own documented defaults, all of which were entirely undeclared
// before this fix: RetentionMode defaults to RETAIN, ScratchStorageMode to
// SHARED, StorageType to STATIC, StorageCapacity to 1200 GiB (only because
// StorageType resolves to STATIC), and WorkflowType to PRIVATE. Every field
// under test is omitted from the request entirely.
func TestOmics_StartRun_DocumentedDefaults_Omitted(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	workflowID := registerTestWorkflow(t, client)

	startOut, err := client.StartRun(t.Context(), &omicssdk.StartRunInput{
		WorkflowId: aws.String(workflowID),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/omics-role"),
		Name:       aws.String("defaults-run"),
		OutputUri:  aws.String("s3://bucket/output"),
	})
	require.NoError(t, err)

	getOut, err := client.GetRun(t.Context(), &omicssdk.GetRunInput{Id: startOut.Id})
	require.NoError(t, err)

	require.Equal(t, types.RunRetentionModeRetain, getOut.RetentionMode)
	require.Equal(t, types.ScratchStorageModeShared, getOut.ScratchStorageMode)
	require.Equal(t, types.StorageTypeStatic, getOut.StorageType)
	require.NotNil(t, getOut.StorageCapacity, "STATIC storage must default to a non-nil capacity")
	require.Equal(t, int32(1200), *getOut.StorageCapacity)
	require.Equal(t, types.WorkflowTypePrivate, getOut.WorkflowType)
}

// TestOmics_StartRun_DynamicStorage_NoCapacityFabricated proves that when
// StorageType is explicitly DYNAMIC, no StorageCapacity value is invented --
// real AWS's own doc comment states DYNAMIC storage "ignores any value that
// you enter", so fabricating one here would misrepresent behaviour this
// backend cannot honestly claim.
func TestOmics_StartRun_DynamicStorage_NoCapacityFabricated(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	workflowID := registerTestWorkflow(t, client)

	startOut, err := client.StartRun(t.Context(), &omicssdk.StartRunInput{
		WorkflowId:  aws.String(workflowID),
		RoleArn:     aws.String("arn:aws:iam::000000000000:role/omics-role"),
		Name:        aws.String("dynamic-run"),
		OutputUri:   aws.String("s3://bucket/output"),
		StorageType: types.StorageTypeDynamic,
	})
	require.NoError(t, err)

	getOut, err := client.GetRun(t.Context(), &omicssdk.GetRunInput{Id: startOut.Id})
	require.NoError(t, err)
	require.Equal(t, types.StorageTypeDynamic, getOut.StorageType)
	require.Nil(t, getOut.StorageCapacity)
}

// TestOmics_StartRun_WorkflowVersionName_Echoed proves an explicit
// WorkflowVersionName (entirely undeclared before this fix) round-trips.
func TestOmics_StartRun_WorkflowVersionName_Echoed(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	workflowID := registerTestWorkflow(t, client)

	_, err := client.CreateWorkflowVersion(t.Context(), &omicssdk.CreateWorkflowVersionInput{
		WorkflowId:  aws.String(workflowID),
		VersionName: aws.String("v1"),
	})
	require.NoError(t, err)

	startOut, err := client.StartRun(t.Context(), &omicssdk.StartRunInput{
		WorkflowId:          aws.String(workflowID),
		RoleArn:             aws.String("arn:aws:iam::000000000000:role/omics-role"),
		Name:                aws.String("versioned-run"),
		OutputUri:           aws.String("s3://bucket/output"),
		WorkflowVersionName: aws.String("v1"),
	})
	require.NoError(t, err)

	getOut, err := client.GetRun(t.Context(), &omicssdk.GetRunInput{Id: startOut.Id})
	require.NoError(t, err)
	require.NotNil(t, getOut.WorkflowVersionName)
	require.Equal(t, "v1", *getOut.WorkflowVersionName)
}

// TestOmics_StartRun_CacheBehavior_DefaultsFromReferencedCache proves
// StartRunInput.CacheBehavior's own doc comment: "You specify this value if
// you want to override the default behavior for the cache. You had set the
// default value when you created the cache." Two caches are seeded with
// different CacheBehavior values so the test can tell "inherited the
// referenced cache's default" apart from "picked some fixed default" -- one
// cache each on both sides of the distinction.
func TestOmics_StartRun_CacheBehavior_DefaultsFromReferencedCache(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	workflowID := registerTestWorkflow(t, client)

	onFailureCache, err := client.CreateRunCache(t.Context(), &omicssdk.CreateRunCacheInput{
		Name:            aws.String("cache-on-failure"),
		CacheS3Location: aws.String("s3://bucket/cache-1"),
		CacheBehavior:   types.CacheBehaviorCacheOnFailure,
	})
	require.NoError(t, err)

	alwaysCache, err := client.CreateRunCache(t.Context(), &omicssdk.CreateRunCacheInput{
		Name:            aws.String("cache-always"),
		CacheS3Location: aws.String("s3://bucket/cache-2"),
		CacheBehavior:   types.CacheBehaviorCacheAlways,
	})
	require.NoError(t, err)

	runA, err := client.StartRun(t.Context(), &omicssdk.StartRunInput{
		WorkflowId: aws.String(workflowID),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/omics-role"),
		Name:       aws.String("run-a"),
		OutputUri:  aws.String("s3://bucket/output"),
		CacheId:    onFailureCache.Id,
	})
	require.NoError(t, err)

	runB, err := client.StartRun(t.Context(), &omicssdk.StartRunInput{
		WorkflowId: aws.String(workflowID),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/omics-role"),
		Name:       aws.String("run-b"),
		OutputUri:  aws.String("s3://bucket/output"),
		CacheId:    alwaysCache.Id,
	})
	require.NoError(t, err)

	getA, err := client.GetRun(t.Context(), &omicssdk.GetRunInput{Id: runA.Id})
	require.NoError(t, err)
	require.Equal(t, types.CacheBehaviorCacheOnFailure, getA.CacheBehavior)
	require.NotNil(t, getA.CacheId)
	require.Equal(t, *onFailureCache.Id, *getA.CacheId)

	getB, err := client.GetRun(t.Context(), &omicssdk.GetRunInput{Id: runB.Id})
	require.NoError(t, err)
	require.Equal(t, types.CacheBehaviorCacheAlways, getB.CacheBehavior)

	// An explicit CacheBehavior on StartRun overrides the cache's own default.
	runC, err := client.StartRun(t.Context(), &omicssdk.StartRunInput{
		WorkflowId:    aws.String(workflowID),
		RoleArn:       aws.String("arn:aws:iam::000000000000:role/omics-role"),
		Name:          aws.String("run-c"),
		OutputUri:     aws.String("s3://bucket/output"),
		CacheId:       onFailureCache.Id,
		CacheBehavior: types.CacheBehaviorCacheAlways,
	})
	require.NoError(t, err)

	getC, err := client.GetRun(t.Context(), &omicssdk.GetRunInput{Id: runC.Id})
	require.NoError(t, err)
	require.Equal(t, types.CacheBehaviorCacheAlways, getC.CacheBehavior)
}

// TestOmics_Workflow_StorageCapacityStorageTypeParameterTemplate_Echoed
// proves CreateWorkflow/UpdateWorkflow's StorageCapacity/StorageType/
// ParameterTemplate (all entirely undeclared before this fix) round-trip.
// No default is asserted here: CreateWorkflowInput's own doc comments for
// these fields describe what the value means for runs, not what
// CreateWorkflow itself defaults to on omission (unlike StartRun's own
// fields, which do state a fixed default -- see the defaults test above).
func TestOmics_Workflow_StorageCapacityStorageTypeParameterTemplate_Echoed(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	createOut, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
		Name:            aws.String("storage-workflow"),
		Engine:          types.WorkflowEngineWdl,
		StorageType:     types.StorageTypeDynamic,
		StorageCapacity: aws.Int32(2400),
		ParameterTemplate: map[string]types.WorkflowParameter{
			"input_bam": {Description: aws.String("input BAM file"), Optional: aws.Bool(false)},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetWorkflow(t.Context(), &omicssdk.GetWorkflowInput{Id: createOut.Id})
	require.NoError(t, err)
	require.Equal(t, types.StorageTypeDynamic, getOut.StorageType)
	require.NotNil(t, getOut.StorageCapacity)
	require.Equal(t, int32(2400), *getOut.StorageCapacity)
	require.Contains(t, getOut.ParameterTemplate, "input_bam")
	require.Equal(t, "input BAM file", *getOut.ParameterTemplate["input_bam"].Description)

	_, err = client.UpdateWorkflow(t.Context(), &omicssdk.UpdateWorkflowInput{
		Id:              createOut.Id,
		StorageType:     types.StorageTypeStatic,
		StorageCapacity: aws.Int32(1200),
	})
	require.NoError(t, err)

	getOut2, err := client.GetWorkflow(t.Context(), &omicssdk.GetWorkflowInput{Id: createOut.Id})
	require.NoError(t, err)
	require.Equal(t, types.StorageTypeStatic, getOut2.StorageType)
	require.NotNil(t, getOut2.StorageCapacity)
	require.Equal(t, int32(1200), *getOut2.StorageCapacity)
}

// TestOmics_WorkflowVersion_StorageCapacityStorageType_Echoed is the
// CreateWorkflowVersion/UpdateWorkflowVersion analogue of the CreateWorkflow
// test above.
func TestOmics_WorkflowVersion_StorageCapacityStorageType_Echoed(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	workflowID := registerTestWorkflow(t, client)

	_, err := client.CreateWorkflowVersion(t.Context(), &omicssdk.CreateWorkflowVersionInput{
		WorkflowId:      aws.String(workflowID),
		VersionName:     aws.String("v1"),
		StorageType:     types.StorageTypeDynamic,
		StorageCapacity: aws.Int32(2400),
		ParameterTemplate: map[string]types.WorkflowParameter{
			"input_bam": {Description: aws.String("input BAM file"), Optional: aws.Bool(false)},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetWorkflowVersion(t.Context(), &omicssdk.GetWorkflowVersionInput{
		WorkflowId: aws.String(workflowID), VersionName: aws.String("v1"),
	})
	require.NoError(t, err)
	require.Equal(t, types.StorageTypeDynamic, getOut.StorageType)
	require.NotNil(t, getOut.StorageCapacity)
	require.Equal(t, int32(2400), *getOut.StorageCapacity)
	require.Contains(t, getOut.ParameterTemplate, "input_bam")

	_, err = client.UpdateWorkflowVersion(t.Context(), &omicssdk.UpdateWorkflowVersionInput{
		WorkflowId:      aws.String(workflowID),
		VersionName:     aws.String("v1"),
		StorageType:     types.StorageTypeStatic,
		StorageCapacity: aws.Int32(1200),
	})
	require.NoError(t, err)

	getOut2, err := client.GetWorkflowVersion(t.Context(), &omicssdk.GetWorkflowVersionInput{
		WorkflowId: aws.String(workflowID), VersionName: aws.String("v1"),
	})
	require.NoError(t, err)
	require.Equal(t, types.StorageTypeStatic, getOut2.StorageType)
	require.NotNil(t, getOut2.StorageCapacity)
	require.Equal(t, int32(1200), *getOut2.StorageCapacity)
}

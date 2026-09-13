package omics_test

import (
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

// newSlice16OmicsClient stands up a fresh backend/handler/client triple for
// gopherstack-n3zi typed slice 16.
func newSlice16OmicsClient(t *testing.T) *omicssdk.Client {
	t.Helper()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)

	return newTestOmicsClient(t, h)
}

// TestTypedSlice16OmicsRealClient drives every op the census still listed as
// uncovered before this pass (gopherstack-n3zi typed-client coverage slice 16).
func TestTypedSlice16OmicsRealClient(t *testing.T) {
	t.Parallel()

	t.Run("sequence_store_lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		created, err := client.CreateSequenceStore(t.Context(), &omicssdk.CreateSequenceStoreInput{
			Name: aws.String("seq-store-1"),
		})
		require.NoError(t, err)
		id := aws.ToString(created.Id)
		require.NotEmpty(t, id)

		got, err := client.GetSequenceStore(t.Context(), &omicssdk.GetSequenceStoreInput{
			Id: aws.String(id),
		})
		require.NoError(t, err)
		assert.Equal(t, "seq-store-1", aws.ToString(got.Name))

		listed, err := client.ListSequenceStores(t.Context(), &omicssdk.ListSequenceStoresInput{})
		require.NoError(t, err)
		require.Len(t, listed.SequenceStores, 1)
		assert.Equal(t, id, aws.ToString(listed.SequenceStores[0].Id))

		_, err = client.UpdateSequenceStore(t.Context(), &omicssdk.UpdateSequenceStoreInput{
			Id:   aws.String(id),
			Name: aws.String("seq-store-1-renamed"),
		})
		require.NoError(t, err)

		got, err = client.GetSequenceStore(t.Context(), &omicssdk.GetSequenceStoreInput{
			Id: aws.String(id),
		})
		require.NoError(t, err)
		assert.Equal(t, "seq-store-1-renamed", aws.ToString(got.Name))

		_, err = client.DeleteSequenceStore(t.Context(), &omicssdk.DeleteSequenceStoreInput{
			Id: aws.String(id),
		})
		require.NoError(t, err)

		_, err = client.GetSequenceStore(t.Context(), &omicssdk.GetSequenceStoreInput{
			Id: aws.String(id),
		})
		require.Error(t, err)
	})

	t.Run("reference_store_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		store, err := client.CreateReferenceStore(t.Context(), &omicssdk.CreateReferenceStoreInput{
			Name: aws.String("ref-store-1"),
		})
		require.NoError(t, err)
		storeID := aws.ToString(store.Id)
		require.NotEmpty(t, storeID)

		imported, err := client.StartReferenceImportJob(t.Context(), &omicssdk.StartReferenceImportJobInput{
			ReferenceStoreId: aws.String(storeID),
			RoleArn:          aws.String("arn:aws:iam::000000000000:role/omics-role"),
			Sources: []types.StartReferenceImportJobSourceItem{
				{Name: aws.String("ref-1"), SourceFile: aws.String("s3://bucket/ref.fasta")},
			},
		})
		require.NoError(t, err)
		jobID := aws.ToString(imported.Id)
		require.NotEmpty(t, jobID)

		gotJob, err := client.GetReferenceImportJob(t.Context(), &omicssdk.GetReferenceImportJobInput{
			ReferenceStoreId: aws.String(storeID),
			Id:               aws.String(jobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.ReferenceImportJobStatusCompleted, gotJob.Status)

		listedJobs, err := client.ListReferenceImportJobs(
			t.Context(),
			&omicssdk.ListReferenceImportJobsInput{ReferenceStoreId: aws.String(storeID)},
		)
		require.NoError(t, err)
		require.Len(t, listedJobs.ImportJobs, 1)
		assert.Equal(t, jobID, aws.ToString(listedJobs.ImportJobs[0].Id))

		listedRefs, err := client.ListReferences(t.Context(), &omicssdk.ListReferencesInput{
			ReferenceStoreId: aws.String(storeID),
		})
		require.NoError(t, err)
		require.Len(t, listedRefs.References, 1)
		refID := aws.ToString(listedRefs.References[0].Id)
		require.NotEmpty(t, refID)
		assert.Equal(t, "ref-1", aws.ToString(listedRefs.References[0].Name))

		gotMeta, err := client.GetReferenceMetadata(t.Context(), &omicssdk.GetReferenceMetadataInput{
			ReferenceStoreId: aws.String(storeID),
			Id:               aws.String(refID),
		})
		require.NoError(t, err)
		assert.Equal(t, "ref-1", aws.ToString(gotMeta.Name))

		gotBytes, err := client.GetReference(t.Context(), &omicssdk.GetReferenceInput{
			ReferenceStoreId: aws.String(storeID),
			Id:               aws.String(refID),
			PartNumber:       aws.Int32(1),
		})
		require.NoError(t, err)
		payload, err := io.ReadAll(gotBytes.Payload)
		require.NoError(t, err)
		assert.Empty(t, payload)

		_, err = client.DeleteReference(t.Context(), &omicssdk.DeleteReferenceInput{
			ReferenceStoreId: aws.String(storeID),
			Id:               aws.String(refID),
		})
		require.NoError(t, err)

		_, err = client.GetReferenceMetadata(t.Context(), &omicssdk.GetReferenceMetadataInput{
			ReferenceStoreId: aws.String(storeID),
			Id:               aws.String(refID),
		})
		require.Error(t, err)
	})

	t.Run("read_set_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		store, err := client.CreateSequenceStore(t.Context(), &omicssdk.CreateSequenceStoreInput{
			Name: aws.String("seq-store-rs"),
		})
		require.NoError(t, err)
		storeID := aws.ToString(store.Id)

		importJob, err := client.StartReadSetImportJob(t.Context(), &omicssdk.StartReadSetImportJobInput{
			SequenceStoreId: aws.String(storeID),
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/omics-role"),
			Sources: []types.StartReadSetImportJobSourceItem{
				{
					SampleId:       aws.String("sample-1"),
					SubjectId:      aws.String("subject-1"),
					SourceFileType: types.FileTypeFastq,
					SourceFiles:    &types.SourceFiles{Source1: aws.String("s3://bucket/rs1.fastq")},
					Name:           aws.String("read-set-1"),
				},
			},
		})
		require.NoError(t, err)
		importJobID := aws.ToString(importJob.Id)
		require.NotEmpty(t, importJobID)

		gotImportJob, err := client.GetReadSetImportJob(t.Context(), &omicssdk.GetReadSetImportJobInput{
			SequenceStoreId: aws.String(storeID),
			Id:              aws.String(importJobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.ReadSetImportJobStatusCompleted, gotImportJob.Status)

		listedImportJobs, err := client.ListReadSetImportJobs(
			t.Context(),
			&omicssdk.ListReadSetImportJobsInput{SequenceStoreId: aws.String(storeID)},
		)
		require.NoError(t, err)
		require.Len(t, listedImportJobs.ImportJobs, 1)
		assert.Equal(t, importJobID, aws.ToString(listedImportJobs.ImportJobs[0].Id))

		listedReadSets, err := client.ListReadSets(t.Context(), &omicssdk.ListReadSetsInput{
			SequenceStoreId: aws.String(storeID),
		})
		require.NoError(t, err)
		require.Len(t, listedReadSets.ReadSets, 1)
		readSetID := aws.ToString(listedReadSets.ReadSets[0].Id)
		require.NotEmpty(t, readSetID)
		assert.Equal(t, "read-set-1", aws.ToString(listedReadSets.ReadSets[0].Name))

		gotReadSet, err := client.GetReadSet(t.Context(), &omicssdk.GetReadSetInput{
			SequenceStoreId: aws.String(storeID),
			Id:              aws.String(readSetID),
			PartNumber:      aws.Int32(1),
		})
		require.NoError(t, err)
		payload, err := io.ReadAll(gotReadSet.Payload)
		require.NoError(t, err)
		assert.Empty(t, payload)

		activationJob, err := client.StartReadSetActivationJob(
			t.Context(),
			&omicssdk.StartReadSetActivationJobInput{
				SequenceStoreId: aws.String(storeID),
				Sources: []types.StartReadSetActivationJobSourceItem{
					{ReadSetId: aws.String(readSetID)},
				},
			},
		)
		require.NoError(t, err)
		activationJobID := aws.ToString(activationJob.Id)
		require.NotEmpty(t, activationJobID)

		gotActivationJob, err := client.GetReadSetActivationJob(
			t.Context(),
			&omicssdk.GetReadSetActivationJobInput{
				SequenceStoreId: aws.String(storeID),
				Id:              aws.String(activationJobID),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, types.ReadSetActivationJobStatusCompleted, gotActivationJob.Status)

		listedActivationJobs, err := client.ListReadSetActivationJobs(
			t.Context(),
			&omicssdk.ListReadSetActivationJobsInput{SequenceStoreId: aws.String(storeID)},
		)
		require.NoError(t, err)
		require.Len(t, listedActivationJobs.ActivationJobs, 1)
		assert.Equal(t, activationJobID, aws.ToString(listedActivationJobs.ActivationJobs[0].Id))

		exportJob, err := client.StartReadSetExportJob(t.Context(), &omicssdk.StartReadSetExportJobInput{
			SequenceStoreId: aws.String(storeID),
			Destination:     aws.String("s3://bucket/export/"),
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/omics-role"),
			Sources: []types.ExportReadSet{
				{ReadSetId: aws.String(readSetID)},
			},
		})
		require.NoError(t, err)
		exportJobID := aws.ToString(exportJob.Id)
		require.NotEmpty(t, exportJobID)

		gotExportJob, err := client.GetReadSetExportJob(t.Context(), &omicssdk.GetReadSetExportJobInput{
			SequenceStoreId: aws.String(storeID),
			Id:              aws.String(exportJobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.ReadSetExportJobStatusCompleted, gotExportJob.Status)

		listedExportJobs, err := client.ListReadSetExportJobs(
			t.Context(),
			&omicssdk.ListReadSetExportJobsInput{SequenceStoreId: aws.String(storeID)},
		)
		require.NoError(t, err)
		require.Len(t, listedExportJobs.ExportJobs, 1)
		assert.Equal(t, exportJobID, aws.ToString(listedExportJobs.ExportJobs[0].Id))

		batchDeleted, err := client.BatchDeleteReadSet(t.Context(), &omicssdk.BatchDeleteReadSetInput{
			SequenceStoreId: aws.String(storeID),
			Ids:             []string{readSetID},
		})
		require.NoError(t, err)
		assert.Empty(t, batchDeleted.Errors)

		listedReadSets, err = client.ListReadSets(t.Context(), &omicssdk.ListReadSetsInput{
			SequenceStoreId: aws.String(storeID),
		})
		require.NoError(t, err)
		assert.Empty(t, listedReadSets.ReadSets)
	})

	t.Run("multipart_upload_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		store, err := client.CreateSequenceStore(t.Context(), &omicssdk.CreateSequenceStoreInput{
			Name: aws.String("seq-store-mp"),
		})
		require.NoError(t, err)
		storeID := aws.ToString(store.Id)

		upload, err := client.CreateMultipartReadSetUpload(
			t.Context(),
			&omicssdk.CreateMultipartReadSetUploadInput{
				SequenceStoreId: aws.String(storeID),
				Name:            aws.String("mp-rs-1"),
				SourceFileType:  types.FileTypeFastq,
				SubjectId:       aws.String("subject-1"),
				SampleId:        aws.String("sample-1"),
			},
		)
		require.NoError(t, err)
		uploadID := aws.ToString(upload.UploadId)
		require.NotEmpty(t, uploadID)

		partsListed, err := client.ListReadSetUploadParts(
			t.Context(),
			&omicssdk.ListReadSetUploadPartsInput{
				SequenceStoreId: aws.String(storeID),
				UploadId:        aws.String(uploadID),
				PartSource:      types.ReadSetPartSourceSource1,
			},
		)
		require.NoError(t, err)
		assert.Empty(t, partsListed.Parts)

		uploadsListed, err := client.ListMultipartReadSetUploads(
			t.Context(),
			&omicssdk.ListMultipartReadSetUploadsInput{SequenceStoreId: aws.String(storeID)},
		)
		require.NoError(t, err)
		require.Len(t, uploadsListed.Uploads, 1)
		assert.Equal(t, uploadID, aws.ToString(uploadsListed.Uploads[0].UploadId))

		_, err = client.AbortMultipartReadSetUpload(
			t.Context(),
			&omicssdk.AbortMultipartReadSetUploadInput{
				SequenceStoreId: aws.String(storeID),
				UploadId:        aws.String(uploadID),
			},
		)
		require.NoError(t, err)

		uploadsListed, err = client.ListMultipartReadSetUploads(
			t.Context(),
			&omicssdk.ListMultipartReadSetUploadsInput{SequenceStoreId: aws.String(storeID)},
		)
		require.NoError(t, err)
		assert.Empty(t, uploadsListed.Uploads)
	})

	t.Run("annotation_store_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
			Name:        aws.String("ann-store-1"),
			StoreFormat: types.StoreFormatVcf,
		})
		require.NoError(t, err)

		_, err = client.UpdateAnnotationStore(t.Context(), &omicssdk.UpdateAnnotationStoreInput{
			Name:        aws.String("ann-store-1"),
			Description: aws.String("updated description"),
		})
		require.NoError(t, err)

		got, err := client.GetAnnotationStore(t.Context(), &omicssdk.GetAnnotationStoreInput{
			Name: aws.String("ann-store-1"),
		})
		require.NoError(t, err)
		assert.Equal(t, "updated description", aws.ToString(got.Description))

		listed, err := client.ListAnnotationStores(t.Context(), &omicssdk.ListAnnotationStoresInput{})
		require.NoError(t, err)
		require.Len(t, listed.AnnotationStores, 1)
		assert.Equal(t, "ann-store-1", aws.ToString(listed.AnnotationStores[0].Name))

		started, err := client.StartAnnotationImportJob(
			t.Context(),
			&omicssdk.StartAnnotationImportJobInput{
				DestinationName: aws.String("ann-store-1"),
				RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
				Items: []types.AnnotationImportItemSource{
					{Source: aws.String("s3://bucket/ann.vcf")},
				},
			},
		)
		require.NoError(t, err)
		jobID := aws.ToString(started.JobId)
		require.NotEmpty(t, jobID)

		_, err = client.CancelAnnotationImportJob(t.Context(), &omicssdk.CancelAnnotationImportJobInput{
			JobId: aws.String(jobID),
		})
		require.NoError(t, err)

		_, err = client.CreateAnnotationStoreVersion(
			t.Context(),
			&omicssdk.CreateAnnotationStoreVersionInput{
				Name:        aws.String("ann-store-1"),
				VersionName: aws.String("v1"),
			},
		)
		require.NoError(t, err)

		delResp, err := client.DeleteAnnotationStoreVersions(
			t.Context(),
			&omicssdk.DeleteAnnotationStoreVersionsInput{
				Name:     aws.String("ann-store-1"),
				Versions: []string{"v1"},
			},
		)
		require.NoError(t, err)
		assert.Empty(t, delResp.Errors)

		_, err = client.DeleteAnnotationStore(t.Context(), &omicssdk.DeleteAnnotationStoreInput{
			Name: aws.String("ann-store-1"),
		})
		require.NoError(t, err)

		_, err = client.GetAnnotationStore(t.Context(), &omicssdk.GetAnnotationStoreInput{
			Name: aws.String("ann-store-1"),
		})
		require.Error(t, err)
	})

	t.Run("variant_store_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		_, err := client.CreateVariantStore(t.Context(), &omicssdk.CreateVariantStoreInput{
			Name:      aws.String("var-store-1"),
			Reference: &types.ReferenceItemMemberReferenceArn{Value: testReferenceArn},
		})
		require.NoError(t, err)

		_, err = client.UpdateVariantStore(t.Context(), &omicssdk.UpdateVariantStoreInput{
			Name:        aws.String("var-store-1"),
			Description: aws.String("updated variant description"),
		})
		require.NoError(t, err)

		got, err := client.GetVariantStore(t.Context(), &omicssdk.GetVariantStoreInput{
			Name: aws.String("var-store-1"),
		})
		require.NoError(t, err)
		assert.Equal(t, "updated variant description", aws.ToString(got.Description))

		started, err := client.StartVariantImportJob(t.Context(), &omicssdk.StartVariantImportJobInput{
			DestinationName: aws.String("var-store-1"),
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
			Items: []types.VariantImportItemSource{
				{Source: aws.String("s3://bucket/var.vcf")},
			},
		})
		require.NoError(t, err)
		jobID := aws.ToString(started.JobId)
		require.NotEmpty(t, jobID)

		_, err = client.CancelVariantImportJob(t.Context(), &omicssdk.CancelVariantImportJobInput{
			JobId: aws.String(jobID),
		})
		require.NoError(t, err)

		_, err = client.DeleteVariantStore(t.Context(), &omicssdk.DeleteVariantStoreInput{
			Name: aws.String("var-store-1"),
		})
		require.NoError(t, err)

		_, err = client.GetVariantStore(t.Context(), &omicssdk.GetVariantStoreInput{
			Name: aws.String("var-store-1"),
		})
		require.Error(t, err)
	})

	t.Run("run_group_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		created, err := client.CreateRunGroup(t.Context(), &omicssdk.CreateRunGroupInput{
			Name: aws.String("rg-1"),
		})
		require.NoError(t, err)
		id := aws.ToString(created.Id)
		require.NotEmpty(t, id)

		_, err = client.UpdateRunGroup(t.Context(), &omicssdk.UpdateRunGroupInput{
			Id:      aws.String(id),
			MaxCpus: aws.Int32(16),
		})
		require.NoError(t, err)

		got, err := client.GetRunGroup(t.Context(), &omicssdk.GetRunGroupInput{Id: aws.String(id)})
		require.NoError(t, err)
		require.NotNil(t, got.MaxCpus)
		assert.Equal(t, int32(16), *got.MaxCpus)

		_, err = client.DeleteRunGroup(t.Context(), &omicssdk.DeleteRunGroupInput{Id: aws.String(id)})
		require.NoError(t, err)

		_, err = client.GetRunGroup(t.Context(), &omicssdk.GetRunGroupInput{Id: aws.String(id)})
		require.Error(t, err)
	})

	t.Run("run_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		workflowID := registerTestWorkflow(t, client)

		started, err := client.StartRun(t.Context(), &omicssdk.StartRunInput{
			WorkflowId: aws.String(workflowID),
			RoleArn:    aws.String("arn:aws:iam::000000000000:role/omics-role"),
			Name:       aws.String("run-1"),
			OutputUri:  aws.String("s3://bucket/output"),
		})
		require.NoError(t, err)
		runID := aws.ToString(started.Id)
		require.NotEmpty(t, runID)

		tasksListed, err := client.ListRunTasks(t.Context(), &omicssdk.ListRunTasksInput{Id: aws.String(runID)})
		require.NoError(t, err)
		require.Len(t, tasksListed.Items, 1)
		taskID := aws.ToString(tasksListed.Items[0].TaskId)
		require.NotEmpty(t, taskID)

		gotTask, err := client.GetRunTask(t.Context(), &omicssdk.GetRunTaskInput{
			Id:     aws.String(runID),
			TaskId: aws.String(taskID),
		})
		require.NoError(t, err)
		assert.Equal(t, "task-1", aws.ToString(gotTask.Name))

		_, err = client.CancelRun(t.Context(), &omicssdk.CancelRunInput{Id: aws.String(runID)})
		require.NoError(t, err)

		gotRun, err := client.GetRun(t.Context(), &omicssdk.GetRunInput{Id: aws.String(runID)})
		require.NoError(t, err)
		assert.Equal(t, types.RunStatusCancelled, gotRun.Status)

		_, err = client.DeleteRun(t.Context(), &omicssdk.DeleteRunInput{Id: aws.String(runID)})
		require.NoError(t, err)

		_, err = client.GetRun(t.Context(), &omicssdk.GetRunInput{Id: aws.String(runID)})
		require.Error(t, err)
	})

	t.Run("run_cache_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		created, err := client.CreateRunCache(t.Context(), &omicssdk.CreateRunCacheInput{
			Name:            aws.String("cache-1"),
			CacheS3Location: aws.String("s3://bucket/cache"),
		})
		require.NoError(t, err)
		id := aws.ToString(created.Id)
		require.NotEmpty(t, id)

		_, err = client.DeleteRunCache(t.Context(), &omicssdk.DeleteRunCacheInput{Id: aws.String(id)})
		require.NoError(t, err)

		_, err = client.GetRunCache(t.Context(), &omicssdk.GetRunCacheInput{Id: aws.String(id)})
		require.Error(t, err)
	})

	t.Run("run_batch_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		workflowID := registerTestWorkflow(t, client)

		started, err := client.StartRunBatch(t.Context(), &omicssdk.StartRunBatchInput{
			RequestId: aws.String("req-token-1"),
			BatchName: aws.String("batch-1"),
			DefaultRunSetting: &types.DefaultRunSetting{
				WorkflowId: aws.String(workflowID),
				RoleArn:    aws.String("arn:aws:iam::000000000000:role/omics-role"),
			},
			BatchRunSettings: &types.BatchRunSettingsMemberInlineSettings{
				Value: []types.InlineSetting{
					{RunSettingId: aws.String("setting-1"), Name: aws.String("batch-run-1")},
				},
			},
		})
		require.NoError(t, err)
		batchID := aws.ToString(started.Id)
		require.NotEmpty(t, batchID)

		gotBatch, err := client.GetBatch(t.Context(), &omicssdk.GetBatchInput{BatchId: aws.String(batchID)})
		require.NoError(t, err)
		assert.Equal(t, types.BatchStatusProcessed, gotBatch.Status)
		require.NotNil(t, gotBatch.TotalRuns)
		assert.Equal(t, int32(1), *gotBatch.TotalRuns)

		runsInBatch, err := client.ListRunsInBatch(t.Context(), &omicssdk.ListRunsInBatchInput{
			BatchId: aws.String(batchID),
		})
		require.NoError(t, err)
		require.Len(t, runsInBatch.Runs, 1)

		// StartRunBatch's constituent runs complete synchronously (like every
		// other job family in this backend), so the batch is already in a
		// terminal (PROCESSED) BatchStatus by the time a real client can call
		// CancelRunBatch -- real AWS's own doc comment on CancelRunBatch
		// restricts cancellation to non-terminal batches, so this failure is
		// the correct, expected outcome, not a bug.
		_, err = client.CancelRunBatch(t.Context(), &omicssdk.CancelRunBatchInput{BatchId: aws.String(batchID)})
		require.Error(t, err)

		_, err = client.DeleteRunBatch(t.Context(), &omicssdk.DeleteRunBatchInput{BatchId: aws.String(batchID)})
		require.NoError(t, err)

		_, err = client.DeleteBatch(t.Context(), &omicssdk.DeleteBatchInput{BatchId: aws.String(batchID)})
		require.NoError(t, err)

		_, err = client.GetBatch(t.Context(), &omicssdk.GetBatchInput{BatchId: aws.String(batchID)})
		require.Error(t, err)
	})

	t.Run("workflow_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		created, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
			Name:   aws.String("wf-del-1"),
			Engine: types.WorkflowEngineWdl,
		})
		require.NoError(t, err)
		workflowID := aws.ToString(created.Id)
		require.NotEmpty(t, workflowID)

		versionCreated, err := client.CreateWorkflowVersion(t.Context(), &omicssdk.CreateWorkflowVersionInput{
			WorkflowId:  aws.String(workflowID),
			VersionName: aws.String("v1"),
		})
		require.NoError(t, err)
		require.NotNil(t, versionCreated.VersionName)

		_, err = client.DeleteWorkflowVersion(t.Context(), &omicssdk.DeleteWorkflowVersionInput{
			WorkflowId:  aws.String(workflowID),
			VersionName: aws.String("v1"),
		})
		require.NoError(t, err)

		_, err = client.GetWorkflowVersion(t.Context(), &omicssdk.GetWorkflowVersionInput{
			WorkflowId:  aws.String(workflowID),
			VersionName: aws.String("v1"),
		})
		require.Error(t, err)

		_, err = client.DeleteWorkflow(t.Context(), &omicssdk.DeleteWorkflowInput{
			Id: aws.String(workflowID),
		})
		require.NoError(t, err)

		_, err = client.GetWorkflow(t.Context(), &omicssdk.GetWorkflowInput{Id: aws.String(workflowID)})
		require.Error(t, err)
	})

	t.Run("share_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		created, err := client.CreateShare(t.Context(), &omicssdk.CreateShareInput{
			ResourceArn:         aws.String("arn:aws:omics:us-east-1:000000000000:annotationStore/share-family-test"),
			PrincipalSubscriber: aws.String("123456789012"),
			ShareName:           aws.String("share-1"),
		})
		require.NoError(t, err)
		shareID := aws.ToString(created.ShareId)
		require.NotEmpty(t, shareID)

		listed, err := client.ListShares(t.Context(), &omicssdk.ListSharesInput{
			ResourceOwner: types.ResourceOwnerSelf,
		})
		require.NoError(t, err)
		require.Len(t, listed.Shares, 1)
		assert.Equal(t, shareID, aws.ToString(listed.Shares[0].ShareId))

		_, err = client.DeleteShare(t.Context(), &omicssdk.DeleteShareInput{ShareId: aws.String(shareID)})
		require.NoError(t, err)

		_, err = client.GetShare(t.Context(), &omicssdk.GetShareInput{ShareId: aws.String(shareID)})
		require.Error(t, err)
	})

	t.Run("configuration_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		created, err := client.CreateConfiguration(t.Context(), &omicssdk.CreateConfigurationInput{
			Name:      aws.String("cfg-1"),
			RequestId: aws.String("cfg-req-1"),
			RunConfigurations: &types.RunConfigurations{
				VpcConfig: &types.VpcConfig{SubnetIds: []string{"subnet-1"}},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(created.Arn))

		got, err := client.GetConfiguration(t.Context(), &omicssdk.GetConfigurationInput{
			Name: aws.String("cfg-1"),
		})
		require.NoError(t, err)
		assert.Equal(t, "cfg-1", aws.ToString(got.Name))

		_, err = client.DeleteConfiguration(t.Context(), &omicssdk.DeleteConfigurationInput{
			Name: aws.String("cfg-1"),
		})
		require.NoError(t, err)

		_, err = client.GetConfiguration(t.Context(), &omicssdk.GetConfigurationInput{
			Name: aws.String("cfg-1"),
		})
		require.Error(t, err)
	})

	t.Run("s3_access_policy_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		const s3AccessPointARN = "arn:aws:s3:us-east-1:000000000000:accesspoint/omics-ap"

		_, err := client.PutS3AccessPolicy(t.Context(), &omicssdk.PutS3AccessPolicyInput{
			S3AccessPointArn: aws.String(s3AccessPointARN),
			S3AccessPolicy:   aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		require.NoError(t, err)

		got, err := client.GetS3AccessPolicy(t.Context(), &omicssdk.GetS3AccessPolicyInput{
			S3AccessPointArn: aws.String(s3AccessPointARN),
		})
		require.NoError(t, err)
		assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, aws.ToString(got.S3AccessPolicy))

		_, err = client.DeleteS3AccessPolicy(t.Context(), &omicssdk.DeleteS3AccessPolicyInput{
			S3AccessPointArn: aws.String(s3AccessPointARN),
		})
		require.NoError(t, err)

		_, err = client.GetS3AccessPolicy(t.Context(), &omicssdk.GetS3AccessPolicyInput{
			S3AccessPointArn: aws.String(s3AccessPointARN),
		})
		require.Error(t, err)
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		client := newSlice16OmicsClient(t)

		created, err := client.CreateRunGroup(t.Context(), &omicssdk.CreateRunGroupInput{
			Name: aws.String("rg-tags"),
			Tags: map[string]string{"env": "prod"},
		})
		require.NoError(t, err)
		arnStr := aws.ToString(created.Arn)
		require.NotEmpty(t, arnStr)

		listed, err := client.ListTagsForResource(t.Context(), &omicssdk.ListTagsForResourceInput{
			ResourceArn: aws.String(arnStr),
		})
		require.NoError(t, err)
		require.Contains(t, listed.Tags, "env")
		assert.Equal(t, "prod", listed.Tags["env"])

		_, err = client.UntagResource(t.Context(), &omicssdk.UntagResourceInput{
			ResourceArn: aws.String(arnStr),
			TagKeys:     []string{"env"},
		})
		require.NoError(t, err)

		listed, err = client.ListTagsForResource(t.Context(), &omicssdk.ListTagsForResourceInput{
			ResourceArn: aws.String(arnStr),
		})
		require.NoError(t, err)
		assert.Empty(t, listed.Tags)
	})
}

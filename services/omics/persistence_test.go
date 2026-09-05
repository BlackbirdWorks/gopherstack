package omics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := omics.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := omics.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateRunGroup("seed-group", 0, 0, 0, 0, nil)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	_, _, err = b.ListRunGroups(nil, 10, "")
	require.NoError(t, err)

	groups, _, err := b.ListRunGroups(nil, 10, "")
	require.NoError(t, err)
	assert.Empty(t, groups)
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot in the pre-Phase-3.3 shape (the old Snapshot serialised
// b.regions directly via encoding/json; since every regionState field was
// unexported this always produced an empty object per region and never
// actually round-tripped anything) decodes with Version == 0, which
// mismatches omicsSnapshotVersion and is discarded the same way any other
// incompatible version is -- not partially applied.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := omics.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateRunGroup("seed-group", 0, 0, 0, 0, nil)
	require.NoError(t, err)

	err = b.Restore(t.Context(), []byte(`{"us-east-1":{}}`))
	require.NoError(t, err)

	groups, _, err := b.ListRunGroups(nil, 10, "")
	require.NoError(t, err)
	assert.Empty(t, groups)
}

// TestInMemoryBackend_RestoreV1AnnotationStoreVersionDiscarded proves
// gopherstack-hjdd's fix: a v1 snapshot holding AnnotationStore.Arn and
// AnnotationStoreVersion.Arn/StoreName under the pre-c41d36cb6/95edfe255 keys
// ("arn", "storeName") must be discarded wholesale now that
// omicsSnapshotVersion is 2, not silently decoded with StoreArn/VersionArn/
// StoreName zero-valued.
func TestInMemoryBackend_RestoreV1AnnotationStoreVersionDiscarded(t *testing.T) {
	t.Parallel()

	b := omics.NewInMemoryBackend("000000000000", "us-east-1")

	v1Snapshot := `{
		"version": 1,
		"accountID": "000000000000",
		"region": "us-east-1",
		"tables": {
			"annotationStores": [{
				"creationTime": "2024-01-01T00:00:00Z",
				"updateTime": "2024-01-01T00:00:00Z",
				"tags": {},
				"arn": "arn:aws:omics:us-east-1:000000000000:annotationStore/store-1",
				"id": "store-1",
				"name": "store-1",
				"description": "",
				"storeFormat": "TSV",
				"status": "ACTIVE"
			}],
			"annotationVersions": [{
				"creationTime": "2024-01-01T00:00:00Z",
				"updateTime": "2024-01-01T00:00:00Z",
				"tags": {},
				"arn": "arn:aws:omics:us-east-1:000000000000:annotationStore/store-1/version/v1",
				"storeId": "store-1",
				"storeName": "store-1",
				"versionName": "v1",
				"description": "",
				"status": "ACTIVE"
			}]
		}
	}`

	require.NoError(t, b.Restore(t.Context(), []byte(v1Snapshot)))

	_, err := b.GetAnnotationStoreVersion("store-1", "v1")
	require.ErrorIs(t, err, omics.ErrNotFound,
		"a v1-shaped AnnotationStoreVersion must never surface with StoreName/VersionArn silently zeroed")

	// AnnotationStore.Name was never renamed, so under the pre-fix bug this
	// store would still decode and be findable by "store-1" (the version
	// lookup above would fail either way, from a clean discard or from the
	// version's own StoreName/VersionArn silently zeroing and misfiling it --
	// this is the assertion that actually distinguishes the two: a clean
	// discard clears the whole registry, so the store itself must also be
	// gone, not just its version).
	_, err = b.GetAnnotationStore("store-1")
	require.ErrorIs(t, err, omics.ErrNotFound,
		"incompatible-version snapshot must reset to empty, not just fail the (misfiled) version lookup")
}

// TestInMemoryBackend_SnapshotRestore_EmptyState verifies an empty backend
// round-trips to another empty backend without error.
func TestInMemoryBackend_SnapshotRestore_EmptyState(t *testing.T) {
	t.Parallel()

	original := omics.NewInMemoryBackend("000000000000", "us-east-1")

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := omics.NewInMemoryBackend("111111111111", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, "000000000000", fresh.AccountID())
	assert.Equal(t, "us-east-1", fresh.Region())

	groups, _, err := fresh.ListRunGroups(nil, 10, "")
	require.NoError(t, err)
	assert.Empty(t, groups)
}

// TestHandler_SnapshotRestoreDelegate verifies the Handler-level Snapshot and
// Restore -- which cli.go's generic setupPersistence actually calls, since it
// type-asserts the registered service.Registerable (Handler), not the
// backend directly -- correctly delegate to the backend. Before this
// conversion, omics had no Handler.Snapshot/Restore at all (only Handler.
// Reset delegated), so InMemoryBackend's Snapshot/Restore were never actually
// wired into the CLI's persistence manager despite the backend implementing
// them.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := omics.NewHandler(omics.NewInMemoryBackend("000000000000", "us-east-1"))

	_, err := h.Backend.CreateRunGroup("delegate-group", 0, 0, 0, 0, nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := omics.NewHandler(omics.NewInMemoryBackend("111111111111", "us-west-2"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	groups, _, err := h2.Backend.ListRunGroups(nil, 10, "")
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "delegate-group", groups[0].Name)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched -- both the directly keyed collections (reference/
// sequence/run/workflow/annotation/variant stores, run groups, run caches,
// run batches, configurations, s3 access policies, shares) and the
// parent-nested composite-key ones (references, reference import jobs, read
// sets, read set activation/export/import jobs, multipart uploads, run
// tasks, workflow versions, annotation store versions) -- plus the five
// left-raw maps (uploadParts, uploadPartData, readSetBytes, referenceBytes,
// tags) that remained plain maps because their values aren't *V or their
// order matters.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := omics.NewInMemoryBackend("111122223333", "us-west-2")
	ctx := t.Context()

	// ReferenceStore + references + referenceImportJobs + referenceBytes(raw).
	refStore, err := original.CreateReferenceStore("ref-store-1", "desc", map[string]string{"env": "test"})
	require.NoError(t, err)

	refImportJob, err := original.StartReferenceImportJob(refStore.ID, "role-arn", []omics.ReferenceImportJobSource{
		{SourceFile: "s3://bucket/ref.fa", Name: "ref-1"},
	})
	require.NoError(t, err)

	refs, _, err := original.ListReferences(refStore.ID, nil, 10, "")
	require.NoError(t, err)
	require.Len(t, refs, 1)
	refID := refs[0].ID

	refBytes, err := original.GetReferenceBytes(refStore.ID, refID)
	require.NoError(t, err)
	assert.NotNil(t, refBytes)

	// SequenceStore + readSets + readSetImportJobs.
	seqStore, err := original.CreateSequenceStore("seq-store-1", "desc", "", "", map[string]string{"env": "test"})
	require.NoError(t, err)

	readSetImportJob, err := original.StartReadSetImportJob(seqStore.ID, "role-arn", []omics.ReadSetImportJobSource{
		{SourceFileType: "FASTQ", Name: "read-1"},
	})
	require.NoError(t, err)

	readSets, _, err := original.ListReadSets(seqStore.ID, nil, 10, "")
	require.NoError(t, err)
	require.Len(t, readSets, 1)
	readSetID := readSets[0].ID

	// readSetActivationJobs / readSetExportJobs.
	activationJob, err := original.StartReadSetActivationJob(
		seqStore.ID, []omics.ReadSetActivationJobSource{{ReadSetID: readSetID}},
	)
	require.NoError(t, err)

	exportJob, err := original.StartReadSetExportJob(
		seqStore.ID, "s3://bucket/export", []omics.ReadSetExportJobSource{{ReadSetID: readSetID}},
	)
	require.NoError(t, err)

	// multipartUploads + uploadParts(raw) + uploadPartData(raw): left
	// in-progress (not completed) so the table/raw-map entries survive into
	// the snapshot rather than being converted into a readSet.
	upload, err := original.CreateMultipartReadSetUpload(
		seqStore.ID, "upload-1", "FASTQ", "sample-1", "subject-1", "", "", "", nil,
	)
	require.NoError(t, err)

	_, err = original.UploadReadSetPart(seqStore.ID, upload.UploadID, 1, "SOURCE1", []byte("part-data"))
	require.NoError(t, err)

	// A second, completed upload exercises readSetBytes(raw).
	upload2, err := original.CreateMultipartReadSetUpload(
		seqStore.ID, "upload-2", "FASTQ", "sample-2", "subject-2", "", "", "", nil,
	)
	require.NoError(t, err)

	_, err = original.UploadReadSetPart(seqStore.ID, upload2.UploadID, 1, "SOURCE1", []byte("completed-part"))
	require.NoError(t, err)

	completedReadSet, err := original.CompleteMultipartReadSetUpload(seqStore.ID, upload2.UploadID)
	require.NoError(t, err)

	readSetBytes, err := original.GetReadSetBytes(seqStore.ID, completedReadSet.ID)
	require.NoError(t, err)
	assert.Equal(t, []byte("completed-part"), readSetBytes)

	// RunGroup.
	runGroup, err := original.CreateRunGroup("run-group-1", 10, 5, 3600, 2, map[string]string{"env": "test"})
	require.NoError(t, err)

	// Workflow + workflowVersions.
	workflow, err := original.CreateWorkflow(omics.CreateWorkflowInput{
		Name: "wf-1", Description: "desc", Engine: "WDL", Tags: map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	wfVersion, err := original.CreateWorkflowVersion(omics.CreateWorkflowVersionInput{
		WorkflowID: workflow.ID, VersionName: "v1", Description: "desc", Tags: map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	// Run + runTasks (StartRun auto-creates one task).
	run, err := original.StartRun(omics.StartRunInput{
		WorkflowID: workflow.ID,
		RoleARN:    "role-arn",
		Name:       "run-1",
		Tags:       map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	tasks, _, err := original.ListRunTasks(run.ID, nil, 10, "")
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskID := tasks[0].TaskID

	// AnnotationStore + annotationVersions + annotationImportJobs.
	annStore, err := original.CreateAnnotationStore(
		"ann-store-1", "VCF", nil, nil, nil, map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	annVersion, err := original.CreateAnnotationStoreVersion(
		annStore.Name, "v1", "desc", map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	annImportJob, err := original.StartAnnotationImportJob(
		annStore.Name, "role-arn", []omics.AnnotationImportItem{{Source: "s3://bucket/ann.vcf"}},
		nil, nil, false, "",
	)
	require.NoError(t, err)

	// VariantStore + variantImportJobs.
	varStore, err := original.CreateVariantStore("var-store-1", nil, nil, map[string]string{"env": "test"})
	require.NoError(t, err)

	varImportJob, err := original.StartVariantImportJob(
		varStore.Name, "role-arn", []omics.VariantImportItem{{Source: "s3://bucket/var.vcf"}},
		nil, false,
	)
	require.NoError(t, err)

	// Share.
	share, err := original.CreateShare("arn:aws:omics:us-west-2:111122223333:workflow/1", "222233334444", "share-1")
	require.NoError(t, err)

	// RunCache.
	runCache, err := original.CreateRunCache("run-cache-1", "s3://bucket/cache", "", map[string]string{"env": "test"})
	require.NoError(t, err)

	// RunBatch.
	runBatch, err := original.StartRunBatch(
		"run-batch-1",
		omics.DefaultRunSetting{WorkflowID: workflow.ID, RoleARN: "role-arn"},
		[]omics.InlineRunSetting{{RunSettingID: "setting-1"}},
		nil,
	)
	require.NoError(t, err)

	// Configuration.
	config, err := original.CreateConfiguration("config-1", "desc", &omics.ConfigurationRunConfigurations{
		VpcConfig: &omics.ConfigurationVpcConfig{SubnetIDs: []string{"subnet-1"}},
	}, map[string]string{"env": "test"})
	require.NoError(t, err)

	// S3AccessPolicy.
	require.NoError(t, original.PutS3AccessPolicy("arn:aws:s3:us-west-2:111122223333:accesspoint/ap-1", "{}"))

	// tags(raw): verify via ListTagsForResource for a couple of tagged ARNs.
	refStoreTags, err := original.ListTagsForResource(refStore.Arn)
	require.NoError(t, err)
	assert.Equal(t, "test", refStoreTags["env"])

	// ── Snapshot -> Restore ──────────────────────────────────────────────
	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := omics.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	assert.Equal(t, "111122223333", fresh.AccountID())
	assert.Equal(t, "us-west-2", fresh.Region())

	// referenceStores / references / referenceImportJobs / referenceBytes.
	gotRefStore, err := fresh.GetReferenceStore(refStore.ID)
	require.NoError(t, err)
	assert.Equal(t, "ref-store-1", gotRefStore.Name)

	gotRef, err := fresh.GetReferenceMetadata(refStore.ID, refID)
	require.NoError(t, err)
	assert.Equal(t, "ref-1", gotRef.Name)

	gotRefImportJob, err := fresh.GetReferenceImportJob(refStore.ID, refImportJob.ID)
	require.NoError(t, err)
	assert.Equal(t, refImportJob.ID, gotRefImportJob.ID)

	gotRefBytes, err := fresh.GetReferenceBytes(refStore.ID, refID)
	require.NoError(t, err)
	assert.Equal(t, refBytes, gotRefBytes)

	// sequenceStores / readSets / readSetImportJobs / activation / export.
	gotSeqStore, err := fresh.GetSequenceStore(seqStore.ID)
	require.NoError(t, err)
	assert.Equal(t, "seq-store-1", gotSeqStore.Name)

	gotReadSet, err := fresh.GetReadSetMetadata(seqStore.ID, readSetID)
	require.NoError(t, err)
	assert.Equal(t, "read-1", gotReadSet.Name)

	gotReadSetImportJob, err := fresh.GetReadSetImportJob(seqStore.ID, readSetImportJob.ID)
	require.NoError(t, err)
	assert.Equal(t, readSetImportJob.ID, gotReadSetImportJob.ID)

	gotActivationJob, err := fresh.GetReadSetActivationJob(seqStore.ID, activationJob.ID)
	require.NoError(t, err)
	assert.Equal(t, activationJob.ID, gotActivationJob.ID)

	gotExportJob, err := fresh.GetReadSetExportJob(seqStore.ID, exportJob.ID)
	require.NoError(t, err)
	assert.Equal(t, exportJob.ID, gotExportJob.ID)

	// multipartUploads (still in-progress) + uploadParts(raw) + uploadPartData(raw).
	uploads, _, err := fresh.ListMultipartReadSetUploads(seqStore.ID, 10, "")
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	assert.Equal(t, upload.UploadID, uploads[0].UploadID)

	parts, _, err := fresh.ListReadSetUploadParts(seqStore.ID, upload.UploadID, 10, "")
	require.NoError(t, err)
	require.Len(t, parts, 1)
	assert.Equal(t, 1, parts[0].PartNumber)

	// readSetBytes(raw), via the readSet created by the completed upload.
	gotReadSetBytes, err := fresh.GetReadSetBytes(seqStore.ID, completedReadSet.ID)
	require.NoError(t, err)
	assert.Equal(t, readSetBytes, gotReadSetBytes)

	// runGroups.
	gotRunGroup, err := fresh.GetRunGroup(runGroup.ID)
	require.NoError(t, err)
	assert.Equal(t, "run-group-1", gotRunGroup.Name)

	// workflows / workflowVersions.
	gotWorkflow, err := fresh.GetWorkflow(workflow.ID)
	require.NoError(t, err)
	assert.Equal(t, "wf-1", gotWorkflow.Name)

	gotWFVersion, err := fresh.GetWorkflowVersion(workflow.ID, wfVersion.VersionName)
	require.NoError(t, err)
	assert.Equal(t, "v1", gotWFVersion.VersionName)

	// runs / runTasks.
	gotRun, err := fresh.GetRun(run.ID)
	require.NoError(t, err)
	assert.Equal(t, "run-1", gotRun.Name)

	gotTask, err := fresh.GetRunTask(run.ID, taskID)
	require.NoError(t, err)
	assert.Equal(t, taskID, gotTask.TaskID)

	// annotationStores / annotationVersions / annotationImportJobs.
	gotAnnStore, err := fresh.GetAnnotationStore(annStore.Name)
	require.NoError(t, err)
	assert.Equal(t, annStore.Name, gotAnnStore.Name)

	gotAnnVersion, err := fresh.GetAnnotationStoreVersion(annStore.Name, annVersion.VersionName)
	require.NoError(t, err)
	assert.Equal(t, "v1", gotAnnVersion.VersionName)

	gotAnnImportJob, err := fresh.GetAnnotationImportJob(annImportJob.ID)
	require.NoError(t, err)
	assert.Equal(t, annImportJob.ID, gotAnnImportJob.ID)

	// variantStores / variantImportJobs.
	gotVarStore, err := fresh.GetVariantStore(varStore.Name)
	require.NoError(t, err)
	assert.Equal(t, varStore.Name, gotVarStore.Name)

	gotVarImportJob, err := fresh.GetVariantImportJob(varImportJob.ID)
	require.NoError(t, err)
	assert.Equal(t, varImportJob.ID, gotVarImportJob.ID)

	// shares.
	gotShare, err := fresh.GetShare(share.ShareID)
	require.NoError(t, err)
	assert.Equal(t, "share-1", gotShare.Name)

	// runCaches.
	gotRunCache, err := fresh.GetRunCache(runCache.ID)
	require.NoError(t, err)
	assert.Equal(t, "run-cache-1", gotRunCache.Name)

	// runBatches.
	gotRunBatch, err := fresh.GetRunBatch(runBatch.ID)
	require.NoError(t, err)
	assert.Equal(t, "run-batch-1", gotRunBatch.Name)

	// configurations.
	gotConfig, err := fresh.GetConfiguration(config.Name)
	require.NoError(t, err)
	assert.Equal(t, "config-1", gotConfig.Name)
	require.NotNil(t, gotConfig.RunConfigurations)
	require.NotNil(t, gotConfig.RunConfigurations.VpcConfig)
	assert.Equal(t, []string{"subnet-1"}, gotConfig.RunConfigurations.VpcConfig.SubnetIDs)
	assert.Equal(t, "test", gotConfig.Tags["env"])

	// s3AccessPolicies.
	gotPolicy, err := fresh.GetS3AccessPolicy("arn:aws:s3:us-west-2:111122223333:accesspoint/ap-1")
	require.NoError(t, err)
	assert.Equal(t, "{}", gotPolicy.Policy)

	// tags(raw).
	gotRefStoreTags, err := fresh.ListTagsForResource(refStore.Arn)
	require.NoError(t, err)
	assert.Equal(t, "test", gotRefStoreTags["env"])
}

package omics

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// resource collection previously stored as a bare map[string]*T (or, for
// parent-nested collections, map[string]map[string]*T) is replaced by a
// single *store.Table[T] registered on b.registry. Directly identified
// resources (their own ID, Name, or ShareID field) are keyed by that field
// directly. Resources previously nested under a parent (reference store,
// sequence store, run, workflow, or annotation store) are keyed by the
// composite "parent|id" string built by parentKey in store.go, with a
// companion *store.Index grouping entries by parent for the per-parent
// listing every List* operation needs -- the same region-qualified-table
// pattern services/emr and services/codeartifact use, applied here to
// genuine parent/child nesting rather than a region dimension (see the
// InMemoryBackend doc comment in store.go for why no region qualifier is
// needed).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func referenceStoreKeyFn(v *ReferenceStore) string           { return v.ID }
func sequenceStoreKeyFn(v *SequenceStore) string             { return v.ID }
func runGroupKeyFn(v *RunGroup) string                       { return v.ID }
func runKeyFn(v *Run) string                                 { return v.ID }
func workflowKeyFn(v *Workflow) string                       { return v.ID }
func annotationStoreKeyFn(v *AnnotationStore) string         { return v.Name }
func annotationImportJobKeyFn(v *AnnotationImportJob) string { return v.ID }
func variantStoreKeyFn(v *VariantStore) string               { return v.Name }
func variantImportJobKeyFn(v *VariantImportJob) string       { return v.ID }
func shareKeyFn(v *Share) string                             { return v.ShareID }
func runCacheKeyFn(v *RunCache) string                       { return v.ID }
func runBatchKeyFn(v *RunBatch) string                       { return v.ID }
func configurationKeyFn(v *Configuration) string             { return v.Name }
func s3AccessPolicyKeyFn(v *S3AccessPolicy) string           { return v.S3AccessPointARN }

func referenceKeyFn(v *ReferenceMetadata) string     { return parentKey(v.ReferenceStoreID, v.ID) }
func referenceStoreIDFn(v *ReferenceMetadata) string { return v.ReferenceStoreID }

func referenceImportJobKeyFn(v *ReferenceImportJob) string {
	return parentKey(v.ReferenceStoreID, v.ID)
}
func referenceImportJobStoreIDFn(v *ReferenceImportJob) string { return v.ReferenceStoreID }

func readSetKeyFn(v *ReadSetMetadata) string     { return parentKey(v.SequenceStoreID, v.ID) }
func readSetStoreIDFn(v *ReadSetMetadata) string { return v.SequenceStoreID }

func readSetActivationJobKeyFn(v *ReadSetActivationJob) string {
	return parentKey(v.SequenceStoreID, v.ID)
}
func readSetActivationJobStoreIDFn(v *ReadSetActivationJob) string { return v.SequenceStoreID }

func readSetExportJobKeyFn(v *ReadSetExportJob) string {
	return parentKey(v.SequenceStoreID, v.ID)
}
func readSetExportJobStoreIDFn(v *ReadSetExportJob) string { return v.SequenceStoreID }

func readSetImportJobKeyFn(v *ReadSetImportJob) string {
	return parentKey(v.SequenceStoreID, v.ID)
}
func readSetImportJobStoreIDFn(v *ReadSetImportJob) string { return v.SequenceStoreID }

func multipartUploadKeyFn(v *MultipartReadSetUpload) string {
	return parentKey(v.SequenceStoreID, v.UploadID)
}
func multipartUploadStoreIDFn(v *MultipartReadSetUpload) string { return v.SequenceStoreID }

func runTaskKeyFn(v *RunTask) string   { return parentKey(v.RunID, v.TaskID) }
func runTaskRunIDFn(v *RunTask) string { return v.RunID }

func workflowVersionKeyFn(v *WorkflowVersion) string {
	return parentKey(v.WorkflowID, v.VersionName)
}
func workflowVersionWorkflowIDFn(v *WorkflowVersion) string { return v.WorkflowID }

func annotationVersionKeyFn(v *AnnotationStoreVersion) string {
	return parentKey(v.StoreName, v.VersionName)
}
func annotationVersionStoreNameFn(v *AnnotationStoreVersion) string { return v.StoreName }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend), never
// on every Reset() -- store.Register panics on a duplicate name, so runtime
// resets go through b.registry.ResetAll() instead (see InMemoryBackend.Reset
// in store.go).
func registerAllTables(b *InMemoryBackend) {
	b.referenceStores = store.Register(b.registry, "referenceStores", store.New(referenceStoreKeyFn))
	b.sequenceStores = store.Register(b.registry, "sequenceStores", store.New(sequenceStoreKeyFn))
	b.runGroups = store.Register(b.registry, "runGroups", store.New(runGroupKeyFn))
	b.runs = store.Register(b.registry, "runs", store.New(runKeyFn))
	b.workflows = store.Register(b.registry, "workflows", store.New(workflowKeyFn))
	b.annotationStores = store.Register(b.registry, "annotationStores", store.New(annotationStoreKeyFn))
	b.annotationImportJobs = store.Register(
		b.registry, "annotationImportJobs", store.New(annotationImportJobKeyFn),
	)
	b.variantStores = store.Register(b.registry, "variantStores", store.New(variantStoreKeyFn))
	b.variantImportJobs = store.Register(b.registry, "variantImportJobs", store.New(variantImportJobKeyFn))
	b.shares = store.Register(b.registry, "shares", store.New(shareKeyFn))
	b.runCaches = store.Register(b.registry, "runCaches", store.New(runCacheKeyFn))
	b.runBatches = store.Register(b.registry, "runBatches", store.New(runBatchKeyFn))
	b.configurations = store.Register(b.registry, "configurations", store.New(configurationKeyFn))
	b.s3AccessPolicies = store.Register(b.registry, "s3AccessPolicies", store.New(s3AccessPolicyKeyFn))

	b.references = store.Register(b.registry, "references", store.New(referenceKeyFn))
	b.referencesByStore = b.references.AddIndex("byStore", referenceStoreIDFn)

	b.referenceImportJobs = store.Register(b.registry, "referenceImportJobs", store.New(referenceImportJobKeyFn))
	b.referenceImportJobsByStore = b.referenceImportJobs.AddIndex("byStore", referenceImportJobStoreIDFn)

	b.readSets = store.Register(b.registry, "readSets", store.New(readSetKeyFn))
	b.readSetsByStore = b.readSets.AddIndex("byStore", readSetStoreIDFn)

	b.readSetActivationJobs = store.Register(
		b.registry, "readSetActivationJobs", store.New(readSetActivationJobKeyFn),
	)
	b.readSetActivationJobsByStore = b.readSetActivationJobs.AddIndex("byStore", readSetActivationJobStoreIDFn)

	b.readSetExportJobs = store.Register(b.registry, "readSetExportJobs", store.New(readSetExportJobKeyFn))
	b.readSetExportJobsByStore = b.readSetExportJobs.AddIndex("byStore", readSetExportJobStoreIDFn)

	b.readSetImportJobs = store.Register(b.registry, "readSetImportJobs", store.New(readSetImportJobKeyFn))
	b.readSetImportJobsByStore = b.readSetImportJobs.AddIndex("byStore", readSetImportJobStoreIDFn)

	b.multipartUploads = store.Register(b.registry, "multipartUploads", store.New(multipartUploadKeyFn))
	b.multipartUploadsByStore = b.multipartUploads.AddIndex("byStore", multipartUploadStoreIDFn)

	b.runTasks = store.Register(b.registry, "runTasks", store.New(runTaskKeyFn))
	b.runTasksByRun = b.runTasks.AddIndex("byRun", runTaskRunIDFn)

	b.workflowVersions = store.Register(b.registry, "workflowVersions", store.New(workflowVersionKeyFn))
	b.workflowVersionsByWorkflow = b.workflowVersions.AddIndex("byWorkflow", workflowVersionWorkflowIDFn)

	b.annotationVersions = store.Register(b.registry, "annotationVersions", store.New(annotationVersionKeyFn))
	b.annotationVersionsByStore = b.annotationVersions.AddIndex("byStore", annotationVersionStoreNameFn)
}

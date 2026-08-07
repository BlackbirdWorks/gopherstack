package omics

import (
	"context"
)

// StorageBackend is the interface for HealthOmics storage operations.
type StorageBackend interface {
	// ReferenceStore
	CreateReferenceStore(name, description string, tags map[string]string) (*ReferenceStore, error)
	DeleteReferenceStore(id string) error
	GetReferenceStore(id string) (*ReferenceStore, error)
	ListReferenceStores(
		filter *ReferenceStoreFilter,
		maxResults int,
		nextToken string,
	) ([]*ReferenceStore, string, error)

	// Reference
	DeleteReference(referenceStoreID, id string) error
	GetReferenceMetadata(referenceStoreID, id string) (*ReferenceMetadata, error)
	ListReferences(
		referenceStoreID string,
		filter *ReferenceFilter,
		maxResults int,
		nextToken string,
	) ([]*ReferenceMetadata, string, error)
	StartReferenceImportJob(
		referenceStoreID, roleARN string,
		sources []ReferenceImportJobSource,
	) (*ReferenceImportJob, error)
	GetReferenceImportJob(referenceStoreID, jobID string) (*ReferenceImportJob, error)
	ListReferenceImportJobs(
		referenceStoreID string,
		filter *ReferenceImportJobFilter,
		maxResults int,
		nextToken string,
	) ([]*ReferenceImportJob, string, error)

	// SequenceStore
	CreateSequenceStore(name, description string, tags map[string]string) (*SequenceStore, error)
	DeleteSequenceStore(id string) error
	GetSequenceStore(id string) (*SequenceStore, error)
	ListSequenceStores(
		filter *SequenceStoreFilter,
		maxResults int,
		nextToken string,
	) ([]*SequenceStore, string, error)
	UpdateSequenceStore(id, name, description string) (*SequenceStore, error)

	// ReadSet
	BatchDeleteReadSet(sequenceStoreID string, ids []string) ([]ReadSetBatchError, error)
	GetReadSetMetadata(sequenceStoreID, id string) (*ReadSetMetadata, error)
	ListReadSets(
		sequenceStoreID string,
		filter *ReadSetFilter,
		maxResults int,
		nextToken string,
	) ([]*ReadSetMetadata, string, error)
	StartReadSetActivationJob(
		sequenceStoreID string,
		sources []ReadSetActivationJobSource,
	) (*ReadSetActivationJob, error)
	GetReadSetActivationJob(sequenceStoreID, jobID string) (*ReadSetActivationJob, error)
	ListReadSetActivationJobs(
		sequenceStoreID string,
		maxResults int,
		nextToken string,
	) ([]*ReadSetActivationJob, string, error)
	StartReadSetExportJob(
		sequenceStoreID, destination string,
		sources []ReadSetExportJobSource,
	) (*ReadSetExportJob, error)
	GetReadSetExportJob(sequenceStoreID, jobID string) (*ReadSetExportJob, error)
	ListReadSetExportJobs(
		sequenceStoreID string,
		maxResults int,
		nextToken string,
	) ([]*ReadSetExportJob, string, error)
	StartReadSetImportJob(
		sequenceStoreID, roleARN string,
		sources []ReadSetImportJobSource,
	) (*ReadSetImportJob, error)
	GetReadSetImportJob(sequenceStoreID, jobID string) (*ReadSetImportJob, error)
	ListReadSetImportJobs(
		sequenceStoreID string,
		maxResults int,
		nextToken string,
	) ([]*ReadSetImportJob, string, error)

	// Multipart ReadSet Upload
	CreateMultipartReadSetUpload(
		sequenceStoreID, name, sourceFileType, sampleID, subjectID, generatedFrom, referenceARN, description string,
		tags map[string]string,
	) (*MultipartReadSetUpload, error)
	AbortMultipartReadSetUpload(sequenceStoreID, uploadID string) error
	CompleteMultipartReadSetUpload(sequenceStoreID, uploadID string) (*ReadSetMetadata, error)
	ListMultipartReadSetUploads(
		sequenceStoreID string,
		maxResults int,
		nextToken string,
	) ([]*MultipartReadSetUpload, string, error)
	ListReadSetUploadParts(
		sequenceStoreID, uploadID string,
		maxResults int,
		nextToken string,
	) ([]*ReadSetUploadPart, string, error)
	UploadReadSetPart(
		sequenceStoreID, uploadID string,
		partNumber int,
		partSource string,
		data []byte,
	) (string, error)
	GetReadSetBytes(sequenceStoreID, id string) ([]byte, error)
	GetReferenceBytes(referenceStoreID, id string) ([]byte, error)

	// RunGroup
	CreateRunGroup(
		name string,
		maxCPUs, maxRuns, maxDuration int,
		maxGPUs int,
		tags map[string]string,
	) (*RunGroup, error)
	DeleteRunGroup(id string) error
	GetRunGroup(id string) (*RunGroup, error)
	ListRunGroups(filter *RunGroupFilter, maxResults int, nextToken string) ([]*RunGroup, string, error)
	UpdateRunGroup(
		id, name string,
		maxCPUs, maxRuns, maxDuration int,
		maxGPUs int,
	) (*RunGroup, error)

	// Run
	StartRun(
		workflowID, roleARN, name, runGroupID, runBatchID, networkingMode, runOutputURI string,
		params map[string]any,
		tags map[string]string,
	) (*Run, error)
	CancelRun(id string) error
	DeleteRun(id string) error
	GetRun(id string) (*Run, error)
	ListRuns(filter *RunFilter, maxResults int, nextToken string) ([]*Run, string, error)
	GetRunTask(runID, taskID string) (*RunTask, error)
	ListRunTasks(
		runID string,
		filter *RunTaskFilter,
		maxResults int,
		nextToken string,
	) ([]*RunTask, string, error)

	// Workflow
	CreateWorkflow(
		name, description, definitionZip, definitionURI, engine string,
		tags map[string]string,
	) (*Workflow, error)
	DeleteWorkflow(id string) error
	GetWorkflow(id string) (*Workflow, error)
	ListWorkflows(filter *WorkflowFilter, maxResults int, nextToken string) ([]*Workflow, string, error)
	UpdateWorkflow(id, name, description string) error

	// AnnotationStore
	CreateAnnotationStore(
		name, storeFormat string,
		reference, sseConfig, storeOptions map[string]any,
		tags map[string]string,
	) (*AnnotationStore, error)
	DeleteAnnotationStore(name string) (*AnnotationStore, error)
	GetAnnotationStore(name string) (*AnnotationStore, error)
	ListAnnotationStores(maxResults int, nextToken string) ([]*AnnotationStore, string, error)
	UpdateAnnotationStore(name, description string) (*AnnotationStore, error)
	StartAnnotationImportJob(
		destinationName, roleARN string,
		items []AnnotationImportItem,
	) (*AnnotationImportJob, error)
	GetAnnotationImportJob(jobID string) (*AnnotationImportJob, error)
	ListAnnotationImportJobs(
		filter *ImportJobFilter,
		ids []string,
		maxResults int,
		nextToken string,
	) ([]*AnnotationImportJob, string, error)
	CancelAnnotationImportJob(jobID string) error

	// AnnotationStoreVersion
	CreateAnnotationStoreVersion(
		name, versionName, description string,
		tags map[string]string,
	) (*AnnotationStoreVersion, error)
	DeleteAnnotationStoreVersions(name string, versionNames []string) ([]VersionDeleteError, error)
	GetAnnotationStoreVersion(name, versionName string) (*AnnotationStoreVersion, error)
	ListAnnotationStoreVersions(
		name string,
		maxResults int,
		nextToken string,
	) ([]*AnnotationStoreVersion, string, error)
	UpdateAnnotationStoreVersion(
		name, versionName, description string,
	) (*AnnotationStoreVersion, error)

	// VariantStore
	CreateVariantStore(name string, reference map[string]any, tags map[string]string) (*VariantStore, error)
	DeleteVariantStore(name string) (*VariantStore, error)
	GetVariantStore(name string) (*VariantStore, error)
	ListVariantStores(maxResults int, nextToken string) ([]*VariantStore, string, error)
	UpdateVariantStore(name, description string) (*VariantStore, error)
	StartVariantImportJob(
		destinationName, roleARN string,
		items []VariantImportItem,
	) (*VariantImportJob, error)
	GetVariantImportJob(jobID string) (*VariantImportJob, error)
	ListVariantImportJobs(
		filter *ImportJobFilter,
		ids []string,
		maxResults int,
		nextToken string,
	) ([]*VariantImportJob, string, error)
	CancelVariantImportJob(jobID string) error

	// Share
	CreateShare(resourceARN, principalSubscriber, name string) (*Share, error)
	AcceptShare(shareID string) (*Share, error)
	DeleteShare(shareID string) (*Share, error)
	GetShare(shareID string) (*Share, error)
	ListShares(resourceOwner string, maxResults int, nextToken string) ([]*Share, string, error)

	// RunCache
	CreateRunCache(name, cacheS3Location string, tags map[string]string) (*RunCache, error)
	DeleteRunCache(id string) error
	GetRunCache(id string) (*RunCache, error)
	ListRunCaches(maxResults int, nextToken string) ([]*RunCache, string, error)
	UpdateRunCache(id, name, description string) error

	// RunBatch
	StartRunBatch(
		batchName string, def DefaultRunSetting, inlineSettings []InlineRunSetting, tags map[string]string,
	) (*RunBatch, error)
	CancelRunBatch(id string) error
	DeleteRunBatch(id string) error
	GetRunBatch(id string) (*RunBatch, error)
	GetRunBatchSummary(id string) (RunBatchSummary, error)
	ListRunBatches(filter *RunBatchFilter, maxResults int, nextToken string) ([]*RunBatch, string, error)
	DeleteRunsInBatch(batchID string) error
	ListRunsInBatch(
		batchID string,
		filter *RunsInBatchFilter,
		maxResults int,
		nextToken string,
	) ([]*Run, string, error)

	// Configuration
	CreateConfiguration(name, description string) (*Configuration, error)
	DeleteConfiguration(name string) error
	GetConfiguration(name string) (*Configuration, error)
	ListConfigurations(maxResults int, nextToken string) ([]*Configuration, string, error)

	// WorkflowVersion
	CreateWorkflowVersion(
		workflowID, versionName, description string,
		tags map[string]string,
	) (*WorkflowVersion, error)
	DeleteWorkflowVersion(workflowID, versionName string) error
	GetWorkflowVersion(workflowID, versionName string) (*WorkflowVersion, error)
	ListWorkflowVersions(
		workflowID string,
		filter *WorkflowVersionFilter,
		maxResults int,
		nextToken string,
	) ([]*WorkflowVersion, string, error)
	UpdateWorkflowVersion(workflowID, versionName, description string) error

	// S3 Access Policy
	PutS3AccessPolicy(s3AccessPointARN, policy string) error
	GetS3AccessPolicy(s3AccessPointARN string) (*S3AccessPolicy, error)
	DeleteS3AccessPolicy(s3AccessPointARN string) error

	// Tags
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// var _ StorageBackend = (*InMemoryBackend)(nil) confirms InMemoryBackend
// (see store.go) implements StorageBackend in full; the resource struct types
// referenced throughout this interface live in models.go.
var _ StorageBackend = (*InMemoryBackend)(nil)

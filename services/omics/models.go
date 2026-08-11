package omics

import (
	"time"
)

// ReferenceStore represents an HealthOmics reference store.
type ReferenceStore struct {
	CreationTime time.Time         `json:"creationTime"`
	SseConfig    map[string]any    `json:"sseConfig,omitempty"`
	S3Access     map[string]any    `json:"s3Access,omitempty"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Description  string            `json:"description"`
}

// ReferenceStoreFilter is filter criteria for listing reference stores.
type ReferenceStoreFilter struct {
	Name string
}

// ReferenceMetadata holds metadata for a genomic reference.
type ReferenceMetadata struct {
	CreationTime     time.Time         `json:"creationTime"`
	UpdateTime       time.Time         `json:"updateTime"`
	Files            map[string]any    `json:"files,omitempty"`
	Tags             map[string]string `json:"tags"`
	Arn              string            `json:"arn"`
	ID               string            `json:"id"`
	ReferenceStoreID string            `json:"referenceStoreId"`
	Name             string            `json:"name"`
	Description      string            `json:"description"`
	Status           string            `json:"status"`
	MD5              string            `json:"md5"`
}

// ReferenceFilter is filter criteria for listing references.
type ReferenceFilter struct {
	Name string
}

// ReferenceImportJobFilter is filter criteria for listing reference import jobs.
type ReferenceImportJobFilter struct {
	Status string
}

// ReferenceImportJob represents a reference import job.
type ReferenceImportJob struct {
	CreationTime     time.Time                  `json:"creationTime"`
	CompletionTime   *time.Time                 `json:"completionTime,omitempty"`
	ID               string                     `json:"id"`
	ReferenceStoreID string                     `json:"referenceStoreId"`
	RoleARN          string                     `json:"roleArn"`
	Status           string                     `json:"status"`
	Sources          []ReferenceImportJobSource `json:"sources"`
}

// ReferenceImportJobSource is a source for a reference import job.
type ReferenceImportJobSource struct {
	SourceFile  string `json:"sourceFile"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// SequenceStore represents an HealthOmics sequence store.
type SequenceStore struct {
	CreationTime  time.Time         `json:"creationTime"`
	UpdateTime    time.Time         `json:"updateTime"`
	SseConfig     map[string]any    `json:"sseConfig,omitempty"`
	S3Access      map[string]any    `json:"s3Access,omitempty"`
	Tags          map[string]string `json:"tags"`
	Arn           string            `json:"arn"`
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Description   string            `json:"description"`
	ETagAlgorithm string            `json:"eTagAlgorithm,omitempty"`
	Status        string            `json:"status"`
}

// SequenceStoreFilter is filter criteria for listing sequence stores.
type SequenceStoreFilter struct {
	Name string
}

// ReadSetMetadata holds metadata for a read set.
type ReadSetMetadata struct {
	CreationTime    time.Time         `json:"creationTime"`
	UpdateTime      time.Time         `json:"updateTime"`
	Files           map[string]any    `json:"files,omitempty"`
	Tags            map[string]string `json:"tags"`
	Arn             string            `json:"arn"`
	ID              string            `json:"id"`
	SequenceStoreID string            `json:"sequenceStoreId"`
	Name            string            `json:"name"`
	Description     string            `json:"description"`
	StatusMessage   string            `json:"statusMessage,omitempty"`
	Status          string            `json:"status"`
	// FileType is the real GetReadSetMetadataOutput/ReadSetListItem wire key
	// ("fileType") -- confirmed against the SDK deserializer. It was
	// previously (incorrectly) serialized as "sequenceType", a key that
	// doesn't exist anywhere in the real API surface.
	FileType     string `json:"fileType"`
	SubjectID    string `json:"subjectId"`
	SampleID     string `json:"sampleId"`
	ReferenceARN string `json:"referenceArn"`
}

// ReadSetFilter is filter criteria for listing read sets.
type ReadSetFilter struct {
	Name   string
	Status string
}

// ReadSetBatchError is an error item from a batch delete operation.
type ReadSetBatchError struct {
	ID      string `json:"id"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ReadSetActivationJobSource is a source for activating a read set.
type ReadSetActivationJobSource struct {
	ReadSetID string `json:"readSetId"`
}

// ReadSetActivationJob represents a read set activation job.
type ReadSetActivationJob struct {
	CreationTime    time.Time                    `json:"creationTime"`
	CompletionTime  *time.Time                   `json:"completionTime,omitempty"`
	ID              string                       `json:"id"`
	SequenceStoreID string                       `json:"sequenceStoreId"`
	Status          string                       `json:"status"`
	Sources         []ReadSetActivationJobSource `json:"sources"`
}

// ReadSetExportJobSource is a source for a read set export job.
type ReadSetExportJobSource struct {
	ReadSetID string `json:"readSetId"`
}

// ReadSetExportJob represents a read set export job.
type ReadSetExportJob struct {
	CreationTime    time.Time                `json:"creationTime"`
	CompletionTime  *time.Time               `json:"completionTime,omitempty"`
	ID              string                   `json:"id"`
	SequenceStoreID string                   `json:"sequenceStoreId"`
	Destination     string                   `json:"destination"`
	Status          string                   `json:"status"`
	Sources         []ReadSetExportJobSource `json:"sources"`
}

// ReadSetImportJobSource is a source for a read set import job.
type ReadSetImportJobSource struct {
	SourceFileType string `json:"sourceFileType"`
	SourceFiles    struct {
		Source1 string `json:"source1"`
		Source2 string `json:"source2,omitempty"`
	} `json:"sourceFiles"`
	SubjectID    string `json:"subjectId"`
	SampleID     string `json:"sampleId"`
	ReferenceARN string `json:"referenceArn"`
	Name         string `json:"name"`
	Description  string `json:"description"`
}

// ReadSetImportJob represents a read set import job.
type ReadSetImportJob struct {
	CreationTime    time.Time                `json:"creationTime"`
	CompletionTime  *time.Time               `json:"completionTime,omitempty"`
	ID              string                   `json:"id"`
	SequenceStoreID string                   `json:"sequenceStoreId"`
	RoleARN         string                   `json:"roleArn"`
	Status          string                   `json:"status"`
	Sources         []ReadSetImportJobSource `json:"sources"`
}

// MultipartReadSetUpload represents an in-progress multipart read set upload.
//
// Field names/JSON keys were field-diffed against
// CreateMultipartReadSetUploadInput/Output and MultipartReadSetUploadListItem:
// the real wire key for the upload's file type is "sourceFileType" (this
// struct previously used the invented key "sequenceType", which appears
// nowhere in the real API), SampleID/SubjectID are real required fields that
// were previously missing entirely, and there is no real "status" field on
// this resource at all (removed).
type MultipartReadSetUpload struct {
	CreationTime    time.Time         `json:"creationTime"`
	Tags            map[string]string `json:"tags"`
	UploadID        string            `json:"uploadId"`
	SequenceStoreID string            `json:"sequenceStoreId"`
	Name            string            `json:"name,omitempty"`
	SourceFileType  string            `json:"sourceFileType"`
	SampleID        string            `json:"sampleId"`
	SubjectID       string            `json:"subjectId"`
	GeneratedFrom   string            `json:"generatedFrom,omitempty"`
	ReferenceARN    string            `json:"referenceArn,omitempty"`
	Description     string            `json:"description,omitempty"`
}

// ReadSetUploadPart represents a single part of a multipart read set upload.
type ReadSetUploadPart struct {
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	Checksum        string    `json:"checksum"`
	Source          string    `json:"source"`
	PartNumber      int       `json:"partNumber"`
	PartSize        int64     `json:"partSize"`
}

// RunGroupFilter is filter criteria for listing run groups.
type RunGroupFilter struct {
	Name string
}

// RunGroup represents an HealthOmics run group.
type RunGroup struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	MaxCPUs      int               `json:"maxCpus"`
	MaxRuns      int               `json:"maxRuns"`
	MaxDuration  int               `json:"maxDuration"`
	MaxGPUs      int               `json:"maxGpus"`
}

// RunFilter is filter criteria for listing runs.
type RunFilter struct {
	Name       string
	RunGroupID string
	BatchID    string
	Status     string
}

// Run represents an HealthOmics workflow run.
type Run struct {
	StartTime     *time.Time            `json:"startTime,omitempty"`
	StopTime      *time.Time            `json:"stopTime,omitempty"`
	CreationTime  time.Time             `json:"creationTime"`
	Configuration *ConfigurationDetails `json:"configuration,omitempty"`
	Tags          map[string]string     `json:"tags"`
	Params        map[string]any        `json:"parameters"`
	Arn           string                `json:"arn"`
	ID            string                `json:"id"`
	Name          string                `json:"name"`
	WorkflowID    string                `json:"workflowId"`
	RoleARN       string                `json:"roleArn"`
	RunGroupID    string                `json:"runGroupId,omitempty"`
	// RunBatchID is serialized as "batchId" (real GetRunOutput/RunListItem
	// wire key -- confirmed against the SDK deserializer; there is no real
	// StartRunInput field to set this, it's populated internally by
	// DeleteRunsInBatch/ListRunsInBatch's caller association).
	RunBatchID string `json:"batchId,omitempty"`
	// RunSettingID mirrors the batch caller's InlineSetting.RunSettingId (real
	// StartRunBatch request field, see types.InlineSetting) so ListRunsInBatch can map
	// each created run back to the caller-supplied per-run configuration it came from.
	// Empty for runs started via plain StartRun (there is no such field on that op).
	RunSettingID   string `json:"runSettingId,omitempty"`
	NetworkingMode string `json:"networkingMode,omitempty"`
	RunOutputURI   string `json:"runOutputUri,omitempty"`
	UUID           string `json:"uuid,omitempty"`
	Status         string `json:"status"`
	pollCount      int    // tracks PENDING→RUNNING→COMPLETED progression; not serialized
}

// ConfigurationDetails describes the configuration used for a workflow run
// (real AWS ConfigurationDetails: arn/name/uuid of the Configuration used).
type ConfigurationDetails struct {
	Arn  string `json:"arn,omitempty"`
	Name string `json:"name,omitempty"`
	UUID string `json:"uuid,omitempty"`
}

// RunTaskFilter is filter criteria for listing run tasks.
type RunTaskFilter struct {
	Status string
}

// RunTask represents a task within a workflow run.
type RunTask struct {
	StartTime    *time.Time `json:"startTime,omitempty"`
	StopTime     *time.Time `json:"stopTime,omitempty"`
	CreationTime time.Time  `json:"creationTime"`
	TaskID       string     `json:"taskId"`
	RunID        string     `json:"runId"`
	Name         string     `json:"name"`
	Status       string     `json:"status"`
	CPUs         int        `json:"cpus"`
	Memory       int        `json:"memory"`
	pollCount    int        // tracks PENDING→RUNNING→COMPLETED progression; not serialized
}

// WorkflowFilter is filter criteria for listing workflows.
type WorkflowFilter struct {
	Name string
	Type string
}

// Workflow represents an HealthOmics workflow.
type Workflow struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Description  string            `json:"description"`
	Engine       string            `json:"engine"`
	Type         string            `json:"type,omitempty"`
	UUID         string            `json:"uuid,omitempty"`
	Status       string            `json:"status"`
	pollCount    int               // tracks CREATING→ACTIVE progression; not serialized
}

// WorkflowVersionFilter is filter criteria for listing workflow versions.
type WorkflowVersionFilter struct {
	Type string
}

// WorkflowVersion represents a version of a workflow.
type WorkflowVersion struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	WorkflowID   string            `json:"workflowId"`
	VersionName  string            `json:"versionName"`
	Description  string            `json:"description"`
	Engine       string            `json:"engine,omitempty"`
	Type         string            `json:"type,omitempty"`
	Status       string            `json:"status"`
	pollCount    int               // tracks CREATING→ACTIVE progression; not serialized
}

// StoreStatusFilter is filter criteria shared by ListAnnotationStores,
// ListVariantStores, and ListAnnotationStoreVersions (status only; real AWS
// ListAnnotationStoresFilter/ListVariantStoresFilter/
// ListAnnotationStoreVersionsFilter, omics@v1.49.5 types/types.go:987,1018,997).
type StoreStatusFilter struct {
	Status string
}

// AnnotationStore represents an HealthOmics annotation store.
type AnnotationStore struct {
	CreationTime time.Time         `json:"creationTime"`
	UpdateTime   time.Time         `json:"updateTime"`
	Reference    map[string]any    `json:"reference,omitempty"`
	SseConfig    map[string]any    `json:"sseConfig,omitempty"`
	StoreOptions map[string]any    `json:"storeOptions,omitempty"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Description  string            `json:"description"`
	StoreFormat  string            `json:"storeFormat"`
	Status       string            `json:"status"`
	pollCount    int               // tracks CREATING→ACTIVE progression; not serialized
}

// AnnotationStoreVersion represents a version of an annotation store.
type AnnotationStoreVersion struct {
	CreationTime time.Time         `json:"creationTime"`
	UpdateTime   time.Time         `json:"updateTime"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	StoreID      string            `json:"storeId"`
	StoreName    string            `json:"storeName"`
	VersionName  string            `json:"versionName"`
	Description  string            `json:"description"`
	Status       string            `json:"status"`
}

// VersionDeleteError is an error item from a version delete operation.
type VersionDeleteError struct {
	VersionName string `json:"versionName"`
	Code        string `json:"code"`
	Message     string `json:"message"`
}

// AnnotationImportItem is a source item for an annotation import job.
type AnnotationImportItem struct {
	Source string `json:"source"`
}

// ImportJobFilter is filter criteria shared by ListAnnotationImportJobs and
// ListVariantImportJobs (status + owning store name).
type ImportJobFilter struct {
	Status    string
	StoreName string
}

// AnnotationImportJob represents an annotation import job.
type AnnotationImportJob struct {
	CreationTime    time.Time              `json:"creationTime"`
	CompletionTime  *time.Time             `json:"completionTime,omitempty"`
	ID              string                 `json:"id"`
	DestinationName string                 `json:"destinationName"`
	RoleARN         string                 `json:"roleArn"`
	Status          string                 `json:"status"`
	Items           []AnnotationImportItem `json:"items"`
}

// VariantStore represents an HealthOmics variant store.
type VariantStore struct {
	CreationTime time.Time         `json:"creationTime"`
	UpdateTime   time.Time         `json:"updateTime"`
	Reference    map[string]any    `json:"reference,omitempty"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Description  string            `json:"description"`
	Status       string            `json:"status"`
	pollCount    int               // tracks CREATING→ACTIVE progression; not serialized
}

// VariantImportItem is a source item for a variant import job.
type VariantImportItem struct {
	Source string `json:"source"`
}

// VariantImportJob represents a variant import job.
type VariantImportJob struct {
	CreationTime    time.Time           `json:"creationTime"`
	CompletionTime  *time.Time          `json:"completionTime,omitempty"`
	ID              string              `json:"id"`
	DestinationName string              `json:"destinationName"`
	RoleARN         string              `json:"roleArn"`
	Status          string              `json:"status"`
	Items           []VariantImportItem `json:"items"`
}

// ShareFilter is filter criteria for ListShares (real AWS types.Filter,
// omics@v1.49.5 types/types.go:678: resourceArns/status/type are each an
// "any of" list).
type ShareFilter struct {
	ResourceArns []string
	Status       []string
	Type         []string
}

// Share represents an HealthOmics resource share.
type Share struct {
	CreationTime        time.Time  `json:"creationTime"`
	UpdateTime          *time.Time `json:"updateTime,omitempty"`
	ShareID             string     `json:"shareId"`
	ResourceARN         string     `json:"resourceArn"`
	PrincipalSubscriber string     `json:"principalSubscriber"`
	Name                string     `json:"name"`
	Status              string     `json:"status"`
}

// RunCache represents an HealthOmics run cache.
type RunCache struct {
	CreationTime    time.Time         `json:"creationTime"`
	Tags            map[string]string `json:"tags"`
	Arn             string            `json:"arn"`
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Description     string            `json:"description,omitempty"`
	CacheS3Location string            `json:"cacheS3Location"`
	Status          string            `json:"status"`
}

// RunBatch represents an HealthOmics run batch (real GetBatchOutput shape --
// RunSummary/SubmissionSummary/TotalRuns are NOT stored here; they are computed live
// from the batch's constituent Run records by summarizeRunBatchLocked, since this
// backend creates/completes them synchronously and a stored, independently-updated
// counter would drift from the real Run rows it's meant to summarize).
type RunBatch struct {
	CreationTime  time.Time         `json:"creationTime"`
	SubmittedTime time.Time         `json:"submittedTime,omitzero"`
	ProcessedTime time.Time         `json:"processedTime,omitzero"`
	Tags          map[string]string `json:"tags"`
	Arn           string            `json:"arn"`
	ID            string            `json:"id"`
	UUID          string            `json:"uuid"`
	Name          string            `json:"name,omitempty"`
	WorkflowID    string            `json:"workflowId"`
	RoleARN       string            `json:"roleArn"`
	RunGroupID    string            `json:"runGroupId,omitempty"`
	OutputURI     string            `json:"outputUri,omitempty"`
	Status        string            `json:"status"`
	// TotalRuns is the number of constituent runs requested at submission time
	// (len(InlineSettings)). SubmissionSuccessCount/SubmissionFailureCount and
	// DeletedRunCount are one-time/monotonic outcomes of synchronous events
	// (submission at StartRunBatch, deletion at DeleteRunsInBatch) and are stored
	// directly rather than derived, since the Run rows they summarize either never
	// existed (failed submissions) or are removed entirely (deleted runs) -- there is
	// nothing left to recompute them from later. The remaining RunSummary counts
	// (pending/running/completed/cancelled/failed) change as runs progress and are
	// computed live from surviving Run rows by summarizeRunBatchLocked instead.
	TotalRuns              int32
	SubmissionSuccessCount int32
	SubmissionFailureCount int32
	DeletedRunCount        int32
}

// DefaultRunSetting is the shared configuration StartRunBatch applies to every run in
// the batch (real types.DefaultRunSetting). Only the subset of fields this backend's
// StartRun already models is accepted -- see the RunBatch family note in PARITY.md for
// the rest (CacheBehavior/CacheId/ConfigurationName/EngineSettings/LogLevel/
// NetworkingMode/OutputBucketOwnerId/Parameters/RetentionMode/ScratchStorageMode/
// StorageCapacity/StorageType/WorkflowOwnerId), which are accepted on the wire but not
// wired to real behavior.
type DefaultRunSetting struct {
	RunTags    map[string]string
	RoleARN    string
	WorkflowID string
	Name       string
	OutputURI  string
	RunGroupID string
	Priority   int32
}

// InlineRunSetting is one caller-supplied per-run override within a
// StartRunBatch request's BatchRunSettings.InlineSettings list (real
// types.InlineSetting). Overrides Name/OutputURI/Priority/RunTags from
// DefaultRunSetting when set; RunSettingID is required and has no default.
type InlineRunSetting struct {
	RunTags      map[string]string
	Priority     *int32
	RunSettingID string
	Name         string
	OutputURI    string
}

// RunBatchFilter is filter criteria for listing run batches (ListBatch).
type RunBatchFilter struct {
	Name       string
	RunGroupID string
	Status     string
}

// RunsInBatchFilter is filter criteria for listing the runs within a batch
// (ListRunsInBatch).
type RunsInBatchFilter struct {
	RunID            string
	RunSettingID     string
	SubmissionStatus string
}

// Configuration represents an HealthOmics configuration.
type Configuration struct {
	CreationTime time.Time `json:"creationTime"`
	Name         string    `json:"name"`
	Description  string    `json:"description"`
	Value        string    `json:"value"`
}

// S3AccessPolicy holds an S3 access policy for HealthOmics.
// S3AccessPolicy holds an S3 access policy for HealthOmics. Policy is
// serialized as "s3AccessPolicy" -- the real GetS3AccessPolicyOutput wire key
// (confirmed against the SDK deserializer); this struct previously used the
// key "policy", which real SDK clients never populate, so GetS3AccessPolicy
// responses were silently missing their policy document on the wire.
type S3AccessPolicy struct {
	UpdateTime       *time.Time `json:"updateTime,omitempty"`
	S3AccessPointARN string     `json:"s3AccessPointArn"`
	Policy           string     `json:"s3AccessPolicy"`
	StoreID          string     `json:"storeId,omitempty"`
	StoreType        string     `json:"storeType,omitempty"`
}

package omics

import "time"

// StorageBackend is the interface for HealthOmics storage operations.
type StorageBackend interface {
	// ReferenceStore
	CreateReferenceStore(name, description string, tags map[string]string) (*ReferenceStore, error)
	DeleteReferenceStore(id string) error
	GetReferenceStore(id string) (*ReferenceStore, error)
	ListReferenceStores(filter *ReferenceStoreFilter, maxResults int, nextToken string) ([]*ReferenceStore, string, error)

	// Reference
	DeleteReference(referenceStoreID, id string) error
	GetReferenceMetadata(referenceStoreID, id string) (*ReferenceMetadata, error)
	ListReferences(referenceStoreID string, filter *ReferenceFilter, maxResults int, nextToken string) ([]*ReferenceMetadata, string, error)
	StartReferenceImportJob(referenceStoreID, roleARN string, sources []ReferenceImportJobSource) (*ReferenceImportJob, error)
	GetReferenceImportJob(referenceStoreID, jobID string) (*ReferenceImportJob, error)
	ListReferenceImportJobs(referenceStoreID string, maxResults int, nextToken string) ([]*ReferenceImportJob, string, error)

	// SequenceStore
	CreateSequenceStore(name, description string, tags map[string]string) (*SequenceStore, error)
	DeleteSequenceStore(id string) error
	GetSequenceStore(id string) (*SequenceStore, error)
	ListSequenceStores(filter *SequenceStoreFilter, maxResults int, nextToken string) ([]*SequenceStore, string, error)
	UpdateSequenceStore(id, name, description string) (*SequenceStore, error)

	// ReadSet
	BatchDeleteReadSet(sequenceStoreID string, ids []string) ([]ReadSetBatchError, error)
	GetReadSetMetadata(sequenceStoreID, id string) (*ReadSetMetadata, error)
	ListReadSets(sequenceStoreID string, filter *ReadSetFilter, maxResults int, nextToken string) ([]*ReadSetMetadata, string, error)
	StartReadSetActivationJob(sequenceStoreID string, sources []ReadSetActivationJobSource) (*ReadSetActivationJob, error)
	GetReadSetActivationJob(sequenceStoreID, jobID string) (*ReadSetActivationJob, error)
	ListReadSetActivationJobs(sequenceStoreID string, maxResults int, nextToken string) ([]*ReadSetActivationJob, string, error)
	StartReadSetExportJob(sequenceStoreID, destination string, sources []ReadSetExportJobSource) (*ReadSetExportJob, error)
	GetReadSetExportJob(sequenceStoreID, jobID string) (*ReadSetExportJob, error)
	ListReadSetExportJobs(sequenceStoreID string, maxResults int, nextToken string) ([]*ReadSetExportJob, string, error)
	StartReadSetImportJob(sequenceStoreID, roleARN string, sources []ReadSetImportJobSource) (*ReadSetImportJob, error)
	GetReadSetImportJob(sequenceStoreID, jobID string) (*ReadSetImportJob, error)
	ListReadSetImportJobs(sequenceStoreID string, maxResults int, nextToken string) ([]*ReadSetImportJob, string, error)

	// Multipart ReadSet Upload
	CreateMultipartReadSetUpload(sequenceStoreID, name, sequenceType string, tags map[string]string) (*MultipartReadSetUpload, error)
	AbortMultipartReadSetUpload(sequenceStoreID, uploadID string) error
	CompleteMultipartReadSetUpload(sequenceStoreID, uploadID string) (*ReadSetMetadata, error)
	ListMultipartReadSetUploads(sequenceStoreID string, maxResults int, nextToken string) ([]*MultipartReadSetUpload, string, error)
	ListReadSetUploadParts(sequenceStoreID, uploadID string, maxResults int, nextToken string) ([]*ReadSetUploadPart, string, error)

	// RunGroup
	CreateRunGroup(name string, maxCPUs, maxRuns, maxDuration int, maxGPUs int, tags map[string]string) (*RunGroup, error)
	DeleteRunGroup(id string) error
	GetRunGroup(id string) (*RunGroup, error)
	ListRunGroups(maxResults int, nextToken string) ([]*RunGroup, string, error)
	UpdateRunGroup(id, name string, maxCPUs, maxRuns, maxDuration int, maxGPUs int) (*RunGroup, error)

	// Run
	StartRun(workflowID, roleARN, name string, params map[string]any, tags map[string]string) (*Run, error)
	CancelRun(id string) error
	DeleteRun(id string) error
	GetRun(id string) (*Run, error)
	ListRuns(maxResults int, nextToken string) ([]*Run, string, error)
	GetRunTask(runID, taskID string) (*RunTask, error)
	ListRunTasks(runID string, maxResults int, nextToken string) ([]*RunTask, string, error)

	// Workflow
	CreateWorkflow(name, description, definitionZip, engine string, tags map[string]string) (*Workflow, error)
	DeleteWorkflow(id string) error
	GetWorkflow(id string) (*Workflow, error)
	ListWorkflows(maxResults int, nextToken string) ([]*Workflow, string, error)
	UpdateWorkflow(id, name, description string) error

	// AnnotationStore
	CreateAnnotationStore(name, storeFormat string, tags map[string]string) (*AnnotationStore, error)
	DeleteAnnotationStore(name string) (*AnnotationStore, error)
	GetAnnotationStore(name string) (*AnnotationStore, error)
	ListAnnotationStores(maxResults int, nextToken string) ([]*AnnotationStore, string, error)
	UpdateAnnotationStore(name, description string) (*AnnotationStore, error)
	StartAnnotationImportJob(destinationName, roleARN string, items []AnnotationImportItem) (*AnnotationImportJob, error)
	GetAnnotationImportJob(jobID string) (*AnnotationImportJob, error)
	ListAnnotationImportJobs(maxResults int, nextToken string) ([]*AnnotationImportJob, string, error)
	CancelAnnotationImportJob(jobID string) error

	// AnnotationStoreVersion
	CreateAnnotationStoreVersion(name, versionName, description string, tags map[string]string) (*AnnotationStoreVersion, error)
	DeleteAnnotationStoreVersions(name string, versionNames []string) ([]VersionDeleteError, error)
	GetAnnotationStoreVersion(name, versionName string) (*AnnotationStoreVersion, error)
	ListAnnotationStoreVersions(name string, maxResults int, nextToken string) ([]*AnnotationStoreVersion, string, error)
	UpdateAnnotationStoreVersion(name, versionName, description string) (*AnnotationStoreVersion, error)

	// VariantStore
	CreateVariantStore(name string, tags map[string]string) (*VariantStore, error)
	DeleteVariantStore(name string) (*VariantStore, error)
	GetVariantStore(name string) (*VariantStore, error)
	ListVariantStores(maxResults int, nextToken string) ([]*VariantStore, string, error)
	UpdateVariantStore(name, description string) (*VariantStore, error)
	StartVariantImportJob(destinationName, roleARN string, items []VariantImportItem) (*VariantImportJob, error)
	GetVariantImportJob(jobID string) (*VariantImportJob, error)
	ListVariantImportJobs(maxResults int, nextToken string) ([]*VariantImportJob, string, error)
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
	StartRunBatch(workflowID, roleARN, name string) (*RunBatch, error)
	CancelRunBatch(id string) error
	DeleteRunBatch(id string) error
	GetRunBatch(id string) (*RunBatch, error)
	ListRunBatches(maxResults int, nextToken string) ([]*RunBatch, string, error)
	DeleteRunBatches(ids []string) ([]RunBatchDeleteError, error)
	ListRunsInBatch(batchID string, maxResults int, nextToken string) ([]*Run, string, error)

	// Configuration
	CreateConfiguration(name, description string) (*Configuration, error)
	DeleteConfiguration(name string) error
	GetConfiguration(name string) (*Configuration, error)
	ListConfigurations(maxResults int, nextToken string) ([]*Configuration, string, error)

	// WorkflowVersion
	CreateWorkflowVersion(workflowID, versionName, description string, tags map[string]string) (*WorkflowVersion, error)
	DeleteWorkflowVersion(workflowID, versionName string) error
	GetWorkflowVersion(workflowID, versionName string) (*WorkflowVersion, error)
	ListWorkflowVersions(workflowID string, maxResults int, nextToken string) ([]*WorkflowVersion, string, error)
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
	Snapshot() []byte
	Restore(data []byte) error
}

// ReferenceStore represents an HealthOmics reference store.
type ReferenceStore struct {
	CreationTime time.Time         `json:"creationTime"`
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

// ReferenceImportJob represents a reference import job.
type ReferenceImportJob struct {
	CreationTime     time.Time                  `json:"creationTime"`
	CompletionTime   *time.Time                 `json:"completionTime,omitempty"`
	Sources          []ReferenceImportJobSource `json:"sources"`
	ID               string                     `json:"id"`
	ReferenceStoreID string                     `json:"referenceStoreId"`
	RoleARN          string                     `json:"roleArn"`
	Status           string                     `json:"status"`
}

// ReferenceImportJobSource is a source for a reference import job.
type ReferenceImportJobSource struct {
	SourceFile  string `json:"sourceFile"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// SequenceStore represents an HealthOmics sequence store.
type SequenceStore struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Description  string            `json:"description"`
}

// SequenceStoreFilter is filter criteria for listing sequence stores.
type SequenceStoreFilter struct {
	Name string
}

// ReadSetMetadata holds metadata for a read set.
type ReadSetMetadata struct {
	CreationTime     time.Time         `json:"creationTime"`
	Tags             map[string]string `json:"tags"`
	Arn              string            `json:"arn"`
	ID               string            `json:"id"`
	SequenceStoreID  string            `json:"sequenceStoreId"`
	Name             string            `json:"name"`
	Description      string            `json:"description"`
	Status           string            `json:"status"`
	SequenceType     string            `json:"sequenceType"`
	SubjectID        string            `json:"subjectId"`
	SampleID         string            `json:"sampleId"`
	ReferenceARN     string            `json:"referenceArn"`
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
	Sources         []ReadSetActivationJobSource `json:"sources"`
	ID              string                       `json:"id"`
	SequenceStoreID string                       `json:"sequenceStoreId"`
	Status          string                       `json:"status"`
}

// ReadSetExportJobSource is a source for a read set export job.
type ReadSetExportJobSource struct {
	ReadSetID string `json:"readSetId"`
}

// ReadSetExportJob represents a read set export job.
type ReadSetExportJob struct {
	CreationTime    time.Time                `json:"creationTime"`
	CompletionTime  *time.Time               `json:"completionTime,omitempty"`
	Sources         []ReadSetExportJobSource `json:"sources"`
	ID              string                   `json:"id"`
	SequenceStoreID string                   `json:"sequenceStoreId"`
	Destination     string                   `json:"destination"`
	Status          string                   `json:"status"`
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
	Sources         []ReadSetImportJobSource `json:"sources"`
	ID              string                   `json:"id"`
	SequenceStoreID string                   `json:"sequenceStoreId"`
	RoleARN         string                   `json:"roleArn"`
	Status          string                   `json:"status"`
}

// MultipartReadSetUpload represents an in-progress multipart read set upload.
type MultipartReadSetUpload struct {
	CreationTime    time.Time         `json:"creationTime"`
	Tags            map[string]string `json:"tags"`
	UploadID        string            `json:"uploadId"`
	SequenceStoreID string            `json:"sequenceStoreId"`
	Name            string            `json:"name"`
	SequenceType    string            `json:"sequenceType"`
	Status          string            `json:"status"`
}

// ReadSetUploadPart represents a single part of a multipart read set upload.
type ReadSetUploadPart struct {
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	PartNumber      int       `json:"partNumber"`
	PartSize        int64     `json:"partSize"`
	Checksum        string    `json:"checksum"`
	Source          string    `json:"source"`
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

// Run represents an HealthOmics workflow run.
type Run struct {
	StartTime    *time.Time        `json:"startTime,omitempty"`
	StopTime     *time.Time        `json:"stopTime,omitempty"`
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	Params       map[string]any    `json:"parameters"`
	Arn          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	WorkflowID   string            `json:"workflowId"`
	RoleARN      string            `json:"roleArn"`
	Status       string            `json:"status"`
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
	Status       string            `json:"status"`
}

// WorkflowVersion represents a version of a workflow.
type WorkflowVersion struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	WorkflowID   string            `json:"workflowId"`
	VersionName  string            `json:"versionName"`
	Description  string            `json:"description"`
	Status       string            `json:"status"`
}

// AnnotationStore represents an HealthOmics annotation store.
type AnnotationStore struct {
	CreationTime time.Time         `json:"creationTime"`
	UpdateTime   time.Time         `json:"updateTime"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Description  string            `json:"description"`
	StoreFormat  string            `json:"storeFormat"`
	Status       string            `json:"status"`
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

// AnnotationImportJob represents an annotation import job.
type AnnotationImportJob struct {
	CreationTime    time.Time              `json:"creationTime"`
	CompletionTime  *time.Time             `json:"completionTime,omitempty"`
	Items           []AnnotationImportItem `json:"items"`
	ID              string                 `json:"id"`
	DestinationName string                 `json:"destinationName"`
	RoleARN         string                 `json:"roleArn"`
	Status          string                 `json:"status"`
}

// VariantStore represents an HealthOmics variant store.
type VariantStore struct {
	CreationTime time.Time         `json:"creationTime"`
	UpdateTime   time.Time         `json:"updateTime"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Description  string            `json:"description"`
	Status       string            `json:"status"`
}

// VariantImportItem is a source item for a variant import job.
type VariantImportItem struct {
	Source string `json:"source"`
}

// VariantImportJob represents a variant import job.
type VariantImportJob struct {
	CreationTime    time.Time           `json:"creationTime"`
	CompletionTime  *time.Time          `json:"completionTime,omitempty"`
	Items           []VariantImportItem `json:"items"`
	ID              string              `json:"id"`
	DestinationName string              `json:"destinationName"`
	RoleARN         string              `json:"roleArn"`
	Status          string              `json:"status"`
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
	CacheS3Location string            `json:"cacheS3Location"`
	Status          string            `json:"status"`
}

// RunBatch represents an HealthOmics run batch.
type RunBatch struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	WorkflowID   string            `json:"workflowId"`
	RoleARN      string            `json:"roleArn"`
	Status       string            `json:"status"`
}

// RunBatchDeleteError is an error item from a batch delete run batch operation.
type RunBatchDeleteError struct {
	ID      string `json:"id"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Configuration represents an HealthOmics configuration.
type Configuration struct {
	CreationTime time.Time `json:"creationTime"`
	Name         string    `json:"name"`
	Description  string    `json:"description"`
	Value        string    `json:"value"`
}

// S3AccessPolicy holds an S3 access policy for HealthOmics.
type S3AccessPolicy struct {
	S3AccessPointARN string `json:"s3AccessPointArn"`
	Policy           string `json:"policy"`
}

var _ StorageBackend = (*InMemoryBackend)(nil)

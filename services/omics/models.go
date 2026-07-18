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
	SequenceType    string            `json:"sequenceType"`
	SubjectID       string            `json:"subjectId"`
	SampleID        string            `json:"sampleId"`
	ReferenceARN    string            `json:"referenceArn"`
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
	Checksum        string    `json:"checksum"`
	Source          string    `json:"source"`
	PartNumber      int       `json:"partNumber"`
	PartSize        int64     `json:"partSize"`
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
	RunBatchID   string            `json:"runBatchId,omitempty"`
	Status       string            `json:"status"`
	pollCount    int               // tracks PENDING→RUNNING→COMPLETED progression; not serialized
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
	Status       string            `json:"status"`
	pollCount    int               // tracks CREATING→ACTIVE progression; not serialized
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

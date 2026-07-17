package textract

import "time"

// Job status constants.
const (
	jobStatusSucceeded  = "SUCCEEDED"
	jobStatusInProgress = "IN_PROGRESS"
	jobStatusFailed     = "FAILED"
)

// Job type constants.
const (
	jobTypeDocumentAnalysis = "DocumentAnalysis"
	jobTypeTextDetection    = "TextDetection"
	jobTypeExpenseAnalysis  = "ExpenseAnalysis"
	jobTypeLendingAnalysis  = "LendingAnalysis"
)

// Adapter feature type constants.
const (
	featureTypeTables     = "TABLES"
	featureTypeForms      = "FORMS"
	featureTypeQueries    = "QUERIES"
	featureTypeSignatures = "SIGNATURES"
	featureTypeLayout     = "LAYOUT"
)

// Adapter auto-update constants.
const (
	autoUpdateEnabled  = "ENABLED"
	autoUpdateDisabled = "DISABLED"
)

// adapterVersionActive is the status for a ready adapter version.
const adapterVersionActive = "ACTIVE"

// adapterVersionCreating is the status during creation.
const adapterVersionCreating = "CREATION_IN_PROGRESS"

// adapterAllowedFeatureTypes restricts adapter creation to FORMS and QUERIES only
// per AWS API constraints. Package-level var avoids reallocation per call.
var adapterAllowedFeatureTypes = map[string]bool{ //nolint:gochecknoglobals // package-level avoids per-call alloc
	featureTypeForms:   true,
	featureTypeQueries: true,
}

// BoundingBox represents the bounding box of a detected element.
type BoundingBox struct {
	Width  float64 `json:"Width"`
	Height float64 `json:"Height"`
	Left   float64 `json:"Left"`
	Top    float64 `json:"Top"`
}

// Point represents a polygon vertex.
type Point struct {
	X float64 `json:"X"`
	Y float64 `json:"Y"`
}

// Geometry contains bounding box and polygon for a block.
type Geometry struct {
	BoundingBox *BoundingBox `json:"BoundingBox"`
	Polygon     []Point      `json:"Polygon"`
}

// Relationship describes child/value relationships between blocks.
type Relationship struct {
	Type string   `json:"Type"`
	Ids  []string `json:"Ids"` //nolint:revive // AWS SDK field name convention
}

// QueryBlock holds query metadata for QUERY blocks.
type QueryBlock struct {
	Text  string   `json:"Text"`
	Alias string   `json:"Alias"`
	Pages []string `json:"Pages"`
}

// Block represents a detected text element returned by Textract.
type Block struct {
	RowIndex        *int           `json:"RowIndex,omitempty"`
	ColumnIndex     *int           `json:"ColumnIndex,omitempty"`
	Query           *QueryBlock    `json:"Query,omitempty"`
	Page            *int           `json:"Page,omitempty"`
	Geometry        *Geometry      `json:"Geometry,omitempty"`
	ColumnSpan      *int           `json:"ColumnSpan,omitempty"`
	RowSpan         *int           `json:"RowSpan,omitempty"`
	BlockType       string         `json:"BlockType"`
	SelectionStatus string         `json:"SelectionStatus,omitempty"`
	Text            string         `json:"Text"`
	ID              string         `json:"Id"`
	TextType        string         `json:"TextType,omitempty"`
	EntityTypes     []string       `json:"EntityTypes,omitempty"`
	Relationships   []Relationship `json:"Relationships,omitempty"`
	Confidence      float64        `json:"Confidence"`
	ColumnHeader    bool           `json:"ColumnHeader,omitempty"`
}

// WarningBlock represents a warning returned in async job responses.
type WarningBlock struct {
	ErrorCode string `json:"ErrorCode"`
	Pages     []int  `json:"Pages"`
}

// OutputConfig holds S3 output configuration for async jobs.
type OutputConfig struct {
	S3Bucket string `json:"S3Bucket"`
	S3Prefix string `json:"S3Prefix"`
}

// NotificationChannel holds SNS topic configuration.
type NotificationChannel struct {
	RoleArn     string `json:"RoleArn"`
	SNSTopicArn string `json:"SNSTopicArn"`
}

// QueriesConfig holds query configuration for AnalyzeDocument.
type QueriesConfig struct {
	Queries []QueryEntry `json:"Queries"`
}

// QueryEntry is a single query with text, alias and page filters.
type QueryEntry struct {
	Text  string   `json:"Text"`
	Alias string   `json:"Alias"`
	Pages []string `json:"Pages"`
}

// DocumentJob represents an asynchronous Textract document job.
type DocumentJob struct {
	CreationTime time.Time `json:"creationTime"`
	JobID        string    `json:"jobId"`
	// Region is the store.Table[DocumentJob] key material (see jobKey in
	// store_setup.go): this backend nests jobs by region, and DocumentJob
	// otherwise carries no field recording which region bucket it lives in.
	// Excluded from persisted JSON -- persistence.go's DTO-based
	// Snapshot/Restore captures Region as a real field instead (Phase 3.3).
	Region              string               `json:"-"`
	JobStatus           string               `json:"jobStatus"`
	JobType             string               `json:"jobType"` // "DocumentAnalysis" or "TextDetection"
	Blocks              []Block              `json:"blocks"`
	OutputConfig        *OutputConfig        `json:"outputConfig,omitempty"`
	NotificationChannel *NotificationChannel `json:"notificationChannel,omitempty"`
	JobTag              string               `json:"jobTag,omitempty"`
	ClientRequestToken  string               `json:"clientRequestToken,omitempty"`
	StatusMessage       string               `json:"statusMessage,omitempty"`
	Warnings            []WarningBlock       `json:"warnings,omitempty"`
}

// ExpenseDetection holds a detected expense field value.
type ExpenseDetection struct {
	Geometry   *Geometry `json:"Geometry,omitempty"`
	Text       string    `json:"Text"`
	Confidence float64   `json:"Confidence"`
}

// ExpenseGroupProperty describes an expense group membership.
type ExpenseGroupProperty struct {
	Id    string   `json:"Id"` //nolint:revive,staticcheck // AWS SDK field name convention
	Types []string `json:"Types"`
}

// ExpenseField represents a single field in an expense document.
type ExpenseField struct {
	Type            *ExpenseDetection      `json:"Type,omitempty"`
	LabelDetection  *ExpenseDetection      `json:"LabelDetection,omitempty"`
	ValueDetection  *ExpenseDetection      `json:"ValueDetection,omitempty"`
	Currency        *ExpenseDetection      `json:"Currency,omitempty"`
	GroupProperties []ExpenseGroupProperty `json:"GroupProperties,omitempty"`
	PageNumber      int                    `json:"PageNumber"`
}

// LineItem represents a single line item in an expense document.
type LineItem struct {
	LineItemExpenseFields []ExpenseField `json:"LineItemExpenseFields"`
}

// LineItemGroup represents a group of line items.
type LineItemGroup struct {
	LineItems          []LineItem `json:"LineItems"`
	LineItemGroupIndex int        `json:"LineItemGroupIndex"`
}

// ExpenseDocument represents a single expense document result.
type ExpenseDocument struct {
	Blocks         []Block         `json:"Blocks"`
	SummaryFields  []ExpenseField  `json:"SummaryFields,omitempty"`
	LineItemGroups []LineItemGroup `json:"LineItemGroups,omitempty"`
	ExpenseIndex   int             `json:"ExpenseIndex"`
}

// NormalizedValue holds a normalized field value.
type NormalizedValue struct {
	Value     string `json:"Value"`
	ValueType string `json:"ValueType"`
}

// AnalyzeIDDetections holds a detected ID field.
type AnalyzeIDDetections struct {
	Geometry        *Geometry        `json:"Geometry,omitempty"`
	NormalizedValue *NormalizedValue `json:"NormalizedValue,omitempty"`
	Text            string           `json:"Text"`
	Confidence      float64          `json:"Confidence"`
}

// IdentityDocumentField holds a single field from an ID document.
type IdentityDocumentField struct {
	Type           *AnalyzeIDDetections `json:"Type,omitempty"`
	ValueDetection *AnalyzeIDDetections `json:"ValueDetection,omitempty"`
}

// IdentityDocument represents a single identity document result.
type IdentityDocument struct {
	Blocks                 []Block                 `json:"Blocks"`
	IdentityDocumentFields []IdentityDocumentField `json:"IdentityDocumentFields,omitempty"`
	DocumentIndex          int                     `json:"DocumentIndex"`
}

// LendingDetection holds a detected lending field value.
type LendingDetection struct {
	Geometry        *Geometry `json:"Geometry,omitempty"`
	Text            string    `json:"Text,omitempty"`
	SelectionStatus string    `json:"SelectionStatus,omitempty"`
	Confidence      float64   `json:"Confidence"`
}

// LendingField is a single field detected in a lending document.
type LendingField struct {
	Type           *LendingDetection `json:"Type,omitempty"`
	ValueDetection *LendingDetection `json:"ValueDetection,omitempty"`
	KeyDetection   *LendingDetection `json:"KeyDetection,omitempty"`
	PageNumber     int               `json:"PageNumber"`
}

// SignatureDetection represents a detected signature.
type SignatureDetection struct {
	Geometry   *Geometry `json:"Geometry,omitempty"`
	Confidence float64   `json:"Confidence"`
}

// LendingDocument holds the extracted fields for a lending page.
type LendingDocument struct {
	LendingFields       []LendingField       `json:"LendingFields,omitempty"`
	SignatureDetections []SignatureDetection `json:"SignatureDetections,omitempty"`
}

// Extraction holds the extraction results for a single lending page.
type Extraction struct {
	LendingDocument *LendingDocument `json:"LendingDocument,omitempty"`
	ExpenseDocument *ExpenseDocument `json:"ExpenseDocument,omitempty"`
}

// PageClassification holds the page classification for a lending page.
type PageClassification struct {
	PageType   []LendingDetection `json:"PageType"`
	PageNumber []LendingDetection `json:"PageNumber"`
}

// LendingResult represents a single lending analysis result page.
type LendingResult struct {
	PageClassification *PageClassification `json:"PageClassification,omitempty"`
	Extractions        []Extraction        `json:"Extractions,omitempty"`
	Page               int                 `json:"Page"`
}

// SplitDocument describes a sub-document within a group.
type SplitDocument struct {
	Pages []int `json:"Pages"`
	Index int   `json:"Index"`
}

// DetectedSignature describes a detected signature page.
type DetectedSignature struct {
	Page int `json:"Page"`
}

// DocumentGroup groups documents of the same type in a lending summary.
type DocumentGroup struct {
	Type                 string              `json:"Type"`
	SplitDocuments       []SplitDocument     `json:"SplitDocuments,omitempty"`
	DetectedSignatures   []DetectedSignature `json:"DetectedSignatures,omitempty"`
	UndetectedSignatures []DetectedSignature `json:"UndetectedSignatures,omitempty"`
}

// LendingSummary summarizes lending analysis results.
type LendingSummary struct {
	DocumentGroups          []DocumentGroup `json:"DocumentGroups,omitempty"`
	UndetectedDocumentTypes []string        `json:"UndetectedDocumentTypes,omitempty"`
}

// LendingJob represents an asynchronous Textract lending analysis job.
type LendingJob struct {
	CreationTime time.Time `json:"creationTime"`
	JobID        string    `json:"jobId"`
	// Region is the store.Table[LendingJob] key material -- see the
	// DocumentJob.Region doc comment above for why this field exists and is
	// excluded from persisted JSON.
	Region              string               `json:"-"`
	JobStatus           string               `json:"jobStatus"`
	Results             []LendingResult      `json:"results"`
	Summary             *LendingSummary      `json:"summary,omitempty"`
	OutputConfig        *OutputConfig        `json:"outputConfig,omitempty"`
	NotificationChannel *NotificationChannel `json:"notificationChannel,omitempty"`
	JobTag              string               `json:"jobTag,omitempty"`
	ClientRequestToken  string               `json:"clientRequestToken,omitempty"`
	StatusMessage       string               `json:"statusMessage,omitempty"`
	Warnings            []WarningBlock       `json:"warnings,omitempty"`
}

// EvaluationMetrics holds model evaluation metrics for an adapter version.
type EvaluationMetrics struct {
	F1Score   float64 `json:"F1Score"`
	Precision float64 `json:"Precision"`
	Recall    float64 `json:"Recall"`
}

// DatasetConfig holds dataset configuration for an adapter version.
type DatasetConfig struct {
	ManifestS3Object *S3ObjectRef `json:"ManifestS3Object,omitempty"`
}

// S3ObjectRef is an S3 object reference.
type S3ObjectRef struct {
	Bucket  string `json:"Bucket"`
	Name    string `json:"Name"`
	Version string `json:"Version,omitempty"`
}

// Adapter represents a Textract Adapter.
type Adapter struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	AdapterID    string            `json:"adapterId"`
	// Region is the store.Table[Adapter] key material -- see the
	// DocumentJob.Region doc comment above for why this field exists and is
	// excluded from persisted JSON.
	Region             string   `json:"-"`
	AdapterName        string   `json:"adapterName"`
	AutoUpdate         string   `json:"autoUpdate"`
	Description        string   `json:"description"`
	ClientRequestToken string   `json:"clientRequestToken,omitempty"`
	FeatureTypes       []string `json:"featureTypes"`
}

// AdapterVersion represents a version of a Textract Adapter.
type AdapterVersion struct {
	CreationTime      time.Time          `json:"creationTime"`
	Tags              map[string]string  `json:"tags"`
	DatasetConfig     *DatasetConfig     `json:"datasetConfig,omitempty"`
	OutputConfig      *OutputConfig      `json:"outputConfig,omitempty"`
	EvaluationMetrics *EvaluationMetrics `json:"evaluationMetrics,omitempty"`
	AdapterVersion    string             `json:"adapterVersion"`
	AdapterID         string             `json:"adapterId"`
	Status            string             `json:"status"`
	StatusMessage     string             `json:"statusMessage"`
	//nolint:revive,staticcheck // KMSKeyId: AWS SDK field name convention
	KMSKeyId           string   `json:"kmsKeyId,omitempty"`
	ClientRequestToken string   `json:"clientRequestToken,omitempty"`
	Region             string   `json:"-"`
	FeatureTypes       []string `json:"featureTypes"`
}

// ExpenseJob represents an asynchronous Textract expense analysis job.
type ExpenseJob struct {
	CreationTime time.Time `json:"creationTime"`
	JobID        string    `json:"jobId"`
	// Region is the store.Table[ExpenseJob] key material -- see the
	// DocumentJob.Region doc comment above for why this field exists and is
	// excluded from persisted JSON.
	Region              string               `json:"-"`
	JobStatus           string               `json:"jobStatus"`
	ExpenseDocuments    []ExpenseDocument    `json:"expenseDocuments"`
	OutputConfig        *OutputConfig        `json:"outputConfig,omitempty"`
	NotificationChannel *NotificationChannel `json:"notificationChannel,omitempty"`
	JobTag              string               `json:"jobTag,omitempty"`
	ClientRequestToken  string               `json:"clientRequestToken,omitempty"`
	StatusMessage       string               `json:"statusMessage,omitempty"`
	Warnings            []WarningBlock       `json:"warnings,omitempty"`
}

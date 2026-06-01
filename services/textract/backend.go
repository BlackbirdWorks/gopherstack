package textract

import (
	"encoding/base64"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrJobNotFound is returned when a document job is not found.
	ErrJobNotFound = awserr.New("InvalidJobIdException", awserr.ErrNotFound)
	// ErrAdapterNotFound is returned when an adapter is not found.
	ErrAdapterNotFound = awserr.New("InvalidParameterException", awserr.ErrNotFound)
	// ErrAdapterVersionNotFound is returned when an adapter version is not found.
	ErrAdapterVersionNotFound = awserr.New("InvalidParameterException", awserr.ErrNotFound)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

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
	CreationTime        time.Time            `json:"creationTime"`
	JobID               string               `json:"jobId"`
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
	CreationTime        time.Time            `json:"creationTime"`
	JobID               string               `json:"jobId"`
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
	CreationTime       time.Time         `json:"creationTime"`
	Tags               map[string]string `json:"tags"`
	AdapterID          string            `json:"adapterId"`
	AdapterName        string            `json:"adapterName"`
	AutoUpdate         string            `json:"autoUpdate"`
	Description        string            `json:"description"`
	ClientRequestToken string            `json:"clientRequestToken,omitempty"`
	FeatureTypes       []string          `json:"featureTypes"`
}

// AdapterVersion represents a version of a Textract Adapter.
type AdapterVersion struct {
	CreationTime      time.Time          `json:"creationTime"`
	Tags              map[string]string  `json:"tags"`
	DatasetConfig     *DatasetConfig     `json:"datasetConfig,omitempty"`
	OutputConfig      *OutputConfig      `json:"outputConfig,omitempty"`
	EvaluationMetrics *EvaluationMetrics `json:"evaluationMetrics,omitempty"`
	AdapterID         string             `json:"adapterId"`
	AdapterVersion    string             `json:"adapterVersion"`
	Status            string             `json:"status"`
	StatusMessage     string             `json:"statusMessage"`
	//nolint:revive,staticcheck // KMSKeyId: AWS SDK field name convention
	KMSKeyId           string   `json:"kmsKeyId,omitempty"`
	ClientRequestToken string   `json:"clientRequestToken,omitempty"`
	FeatureTypes       []string `json:"featureTypes"`
}

// maxJobHistory is the maximum number of completed jobs retained in memory.
const maxJobHistory = 10000

// MaxJobHistory is the exported value for testing.
const MaxJobHistory = maxJobHistory

// ExpenseJob represents an asynchronous Textract expense analysis job.
type ExpenseJob struct {
	CreationTime        time.Time            `json:"creationTime"`
	JobID               string               `json:"jobId"`
	JobStatus           string               `json:"jobStatus"`
	ExpenseDocuments    []ExpenseDocument    `json:"expenseDocuments"`
	OutputConfig        *OutputConfig        `json:"outputConfig,omitempty"`
	NotificationChannel *NotificationChannel `json:"notificationChannel,omitempty"`
	JobTag              string               `json:"jobTag,omitempty"`
	ClientRequestToken  string               `json:"clientRequestToken,omitempty"`
	StatusMessage       string               `json:"statusMessage,omitempty"`
	Warnings            []WarningBlock       `json:"warnings,omitempty"`
}

// InMemoryBackend is the in-memory store for Textract jobs.
type InMemoryBackend struct {
	jobs                   map[string]*DocumentJob
	expenseJobs            map[string]*ExpenseJob
	lendingJobs            map[string]*LendingJob
	adapters               map[string]*Adapter
	adapterVersions        map[string]*AdapterVersion // key: adapterId+"#"+version
	clientTokenToJobID     map[string]string          // dedup: ClientRequestToken → JobID
	adapterClientTokenToID map[string]string          // dedup: ClientRequestToken → AdapterID
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
	maxJobs                int
	asyncJobDelay          time.Duration // how long before async jobs transition to SUCCEEDED
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		jobs:                   make(map[string]*DocumentJob),
		expenseJobs:            make(map[string]*ExpenseJob),
		lendingJobs:            make(map[string]*LendingJob),
		adapters:               make(map[string]*Adapter),
		adapterVersions:        make(map[string]*AdapterVersion),
		clientTokenToJobID:     make(map[string]string),
		adapterClientTokenToID: make(map[string]string),
		mu:                     lockmetrics.New("textract"),
		accountID:              accountID,
		region:                 region,
		maxJobs:                maxJobHistory,
		asyncJobDelay:          defaultAsyncJobDelay,
	}
}

// AccountID returns the AWS account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored resources, resetting the backend to its initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.jobs = make(map[string]*DocumentJob)
	b.expenseJobs = make(map[string]*ExpenseJob)
	b.lendingJobs = make(map[string]*LendingJob)
	b.adapters = make(map[string]*Adapter)
	b.adapterVersions = make(map[string]*AdapterVersion)
	b.clientTokenToJobID = make(map[string]string)
	b.adapterClientTokenToID = make(map[string]string)
}

const (
	confidencePage  = 99.0
	confidenceLine  = 99.5
	confidenceWord1 = 99.8
	confidenceWord2 = 99.7
	confidenceWord3 = 99.9
	blockTypeWord   = "WORD"
	textTypePrinted = "PRINTED"

	// Block type string constants to avoid repeated literals.
	blockTypeKeyValueSet = "KEY_VALUE_SET"
	blockTypeCell        = "CELL"

	// Relationship type constants.
	relTypeChild  = "CHILD"
	relTypeValue  = "VALUE"
	relTypeAnswer = "ANSWER"

	// Confidence constants for generated blocks.
	confidenceKV          = 95.0
	confidenceKV2         = 93.0
	confidenceWordForm    = 99.0
	confidenceSelElem     = 97.0
	confidenceTable       = 99.0
	confidenceTableCell   = 98.0
	confidenceQuery       = 100.0
	confidenceQueryResult = 50.0
	confidenceSignature   = 80.0
	confidenceLayout      = 95.0

	// Geometry coordinate constants for generated blocks.
	geoLeft   = 0.05
	geoTop1   = 0.05
	geoTop2   = 0.12
	geoWidth  = 0.9
	geoHeight = 0.04
	geoWide   = 0.6

	geoWordLeft1  = 0.05
	geoWordLeft2  = 0.16
	geoWordLeft3  = 0.27
	geoWordWidth  = 0.10
	geoWordWidth3 = 0.06

	geoFormValLeft = 0.36
	geoFormWidth   = 0.30
	geoFormTop1    = 0.22
	geoFormTop2    = 0.28
	geoSelTop      = 0.34
	geoSelWidth    = 0.02
	geoSelHeight   = 0.02

	geoTableLeft   = 0.05
	geoTableTop    = 0.40
	geoTableWidth  = 0.80
	geoTableHeight = 0.20
	geoCell1Left   = 0.05
	geoCell1Width  = 0.40
	geoCell2Left   = 0.45
	geoCellTop1    = 0.40
	geoCellTop2    = 0.48
	geoCellHeight  = 0.08

	geoSigLeft   = 0.05
	geoSigTop    = 0.65
	geoSigWidth  = 0.20
	geoSigHeight = 0.06

	geoHdrLeft   = 0.05
	geoHdrTop    = 0.02
	geoHdrWidth  = 0.90
	geoHdrHeight = 0.04

	geoBodyLeft   = 0.05
	geoBodyTop    = 0.08
	geoBodyWidth  = 0.90
	geoBodyHeight = 0.50

	geoFtrLeft   = 0.05
	geoFtrTop    = 0.92
	geoFtrWidth  = 0.90
	geoFtrHeight = 0.04

	// Confidence for expense/ID/lending generated fields.
	confidenceExpenseHigh = 99.0
	confidenceExpenseMed  = 98.0
	confidenceExpenseLI   = 97.0
	confidenceExpenseLI2  = 96.0

	confidenceLending  = 95.0
	confidenceLending2 = 94.0
	confidenceLendSig  = 85.0

	// Geometry for lending signature.
	geoLendSigLeft   = 0.1
	geoLendSigTop    = 0.8
	geoLendSigWidth  = 0.3
	geoLendSigHeight = 0.06
)

// makeGeometry creates a simple bounding-box geometry with a polygon.
func makeGeometry(left, top, width, height float64) *Geometry {
	return &Geometry{
		BoundingBox: &BoundingBox{
			Left:   left,
			Top:    top,
			Width:  width,
			Height: height,
		},
		Polygon: []Point{
			{X: left, Y: top},
			{X: left + width, Y: top},
			{X: left + width, Y: top + height},
			{X: left, Y: top + height},
		},
	}
}

// syntheticBlocks returns a properly structured set of PAGE/LINE/WORD blocks
// with Geometry and Relationships wired per AWS spec.
func syntheticBlocks(documentURI string) []Block {
	pageID := uuid.NewString()
	line1ID := uuid.NewString()
	line2ID := uuid.NewString()
	word1ID := uuid.NewString()
	word2ID := uuid.NewString()
	word3ID := uuid.NewString()
	word4ID := uuid.NewString()
	word5ID := uuid.NewString()

	page1 := 1

	pageBlock := Block{
		BlockType:  "PAGE",
		Text:       "",
		Confidence: confidencePage,
		ID:         pageID,
		Page:       &page1,
		Geometry:   makeGeometry(0, 0, 1, 1),
		Relationships: []Relationship{
			{Type: relTypeChild, Ids: []string{line1ID, line2ID}},
		},
	}

	line1Text := "Synthetic extracted text from " + documentURI
	line1Block := Block{
		BlockType:  "LINE",
		Text:       line1Text,
		Confidence: confidenceLine,
		ID:         line1ID,
		Page:       &page1,
		Geometry:   makeGeometry(geoLeft, geoTop1, geoWidth, geoHeight),
		Relationships: []Relationship{
			{Type: relTypeChild, Ids: []string{word1ID, word2ID, word3ID}},
		},
	}

	line2Block := Block{
		BlockType:  "LINE",
		Text:       "Document processed successfully",
		Confidence: confidenceLine,
		ID:         line2ID,
		Page:       &page1,
		Geometry:   makeGeometry(geoLeft, geoTop2, geoWide, geoHeight),
		Relationships: []Relationship{
			{Type: relTypeChild, Ids: []string{word4ID, word5ID}},
		},
	}

	return []Block{
		pageBlock,
		line1Block,
		line2Block,
		{
			BlockType:  blockTypeWord,
			Text:       "Synthetic",
			TextType:   textTypePrinted,
			Confidence: confidenceWord1,
			ID:         word1ID,
			Page:       &page1,
			Geometry:   makeGeometry(geoWordLeft1, geoTop1, geoWordWidth, geoHeight),
		},
		{
			BlockType:  blockTypeWord,
			Text:       "extracted",
			TextType:   textTypePrinted,
			Confidence: confidenceWord2,
			ID:         word2ID,
			Page:       &page1,
			Geometry:   makeGeometry(geoWordLeft2, geoTop1, geoWordWidth, geoHeight),
		},
		{
			BlockType:  blockTypeWord,
			Text:       "text",
			TextType:   textTypePrinted,
			Confidence: confidenceWord3,
			ID:         word3ID,
			Page:       &page1,
			Geometry:   makeGeometry(geoWordLeft3, geoTop1, geoWordWidth3, geoHeight),
		},
		{
			BlockType:  blockTypeWord,
			Text:       "Document",
			TextType:   textTypePrinted,
			Confidence: confidenceWord1,
			ID:         word4ID,
			Page:       &page1,
			Geometry:   makeGeometry(geoWordLeft1, geoTop2, geoWordWidth, geoHeight),
		},
		{
			BlockType:  blockTypeWord,
			Text:       "processed",
			TextType:   textTypePrinted,
			Confidence: confidenceWord2,
			ID:         word5ID,
			Page:       &page1,
			Geometry:   makeGeometry(geoWordLeft2, geoTop2, geoWordWidth, geoHeight),
		},
	}
}

// analyzeDocumentBlocks generates blocks for AnalyzeDocument based on feature types
// and queries config.
func analyzeDocumentBlocks(documentURI string, featureTypes []string, queries *QueriesConfig) []Block {
	blocks := syntheticBlocks(documentURI)

	featureSet := make(map[string]bool, len(featureTypes))
	for _, ft := range featureTypes {
		featureSet[ft] = true
	}

	page1 := 1

	if featureSet[featureTypeForms] {
		blocks = append(blocks, buildFormsBlocks(page1)...)
	}

	if featureSet[featureTypeTables] {
		blocks = append(blocks, buildTablesBlocks(page1)...)
	}

	if featureSet[featureTypeQueries] && queries != nil {
		blocks = append(blocks, buildQueryBlocks(queries, page1)...)
	}

	if featureSet[featureTypeSignatures] {
		blocks = append(blocks, buildSignatureBlocks(page1)...)
	}

	if featureSet[featureTypeLayout] {
		blocks = append(blocks, buildLayoutBlocks(page1)...)
	}

	return blocks
}

// buildFormsBlocks builds KEY_VALUE_SET and SELECTION_ELEMENT blocks.
func buildFormsBlocks(page int) []Block {
	keyWord1ID := uuid.NewString()
	keyWord2ID := uuid.NewString()
	valueWord1ID := uuid.NewString()
	valueWord2ID := uuid.NewString()
	key1ID := uuid.NewString()
	val1ID := uuid.NewString()
	key2ID := uuid.NewString()
	val2ID := uuid.NewString()
	selID := uuid.NewString()

	blocks := []Block{
		// Key 1
		{
			BlockType:   blockTypeKeyValueSet,
			ID:          key1ID,
			Confidence:  confidenceKV,
			EntityTypes: []string{"KEY"},
			Page:        &page,
			Geometry:    makeGeometry(geoLeft, geoFormTop1, geoFormWidth, geoHeight),
			Relationships: []Relationship{
				{Type: relTypeValue, Ids: []string{val1ID}},
				{Type: relTypeChild, Ids: []string{keyWord1ID}},
			},
		},
		// Value 1
		{
			BlockType:   blockTypeKeyValueSet,
			ID:          val1ID,
			Confidence:  confidenceKV,
			EntityTypes: []string{"VALUE"},
			Page:        &page,
			Geometry:    makeGeometry(geoFormValLeft, geoFormTop1, geoFormWidth, geoHeight),
			Relationships: []Relationship{
				{Type: relTypeChild, Ids: []string{valueWord1ID}},
			},
		},
		// Word for key1
		{
			BlockType:  blockTypeWord,
			ID:         keyWord1ID,
			Text:       "Name:",
			TextType:   textTypePrinted,
			Confidence: confidenceWordForm,
			Page:       &page,
			Geometry:   makeGeometry(geoLeft, geoFormTop1, geoWordWidth, geoHeight),
		},
		// Word for value1
		{
			BlockType:  blockTypeWord,
			ID:         valueWord1ID,
			Text:       "John",
			TextType:   textTypePrinted,
			Confidence: confidenceWordForm,
			Page:       &page,
			Geometry:   makeGeometry(geoFormValLeft, geoFormTop1, geoWordWidth, geoHeight),
		},
		// Key 2
		{
			BlockType:   blockTypeKeyValueSet,
			ID:          key2ID,
			Confidence:  confidenceKV2,
			EntityTypes: []string{"KEY"},
			Page:        &page,
			Geometry:    makeGeometry(geoLeft, geoFormTop2, geoFormWidth, geoHeight),
			Relationships: []Relationship{
				{Type: relTypeValue, Ids: []string{val2ID}},
				{Type: relTypeChild, Ids: []string{keyWord2ID}},
			},
		},
		// Value 2
		{
			BlockType:   blockTypeKeyValueSet,
			ID:          val2ID,
			Confidence:  confidenceKV2,
			EntityTypes: []string{"VALUE"},
			Page:        &page,
			Geometry:    makeGeometry(geoFormValLeft, geoFormTop2, geoFormWidth, geoHeight),
			Relationships: []Relationship{
				{Type: relTypeChild, Ids: []string{valueWord2ID}},
			},
		},
		// Word for key2
		{
			BlockType:  blockTypeWord,
			ID:         keyWord2ID,
			Text:       "Status:",
			TextType:   textTypePrinted,
			Confidence: confidenceWordForm,
			Page:       &page,
			Geometry:   makeGeometry(geoLeft, geoFormTop2, geoWordWidth, geoHeight),
		},
		// Word for value2
		{
			BlockType:  blockTypeWord,
			ID:         valueWord2ID,
			Text:       "Active",
			TextType:   textTypePrinted,
			Confidence: confidenceWordForm,
			Page:       &page,
			Geometry:   makeGeometry(geoFormValLeft, geoFormTop2, geoWordWidth, geoHeight),
		},
		// Selection element
		{
			BlockType:       "SELECTION_ELEMENT",
			ID:              selID,
			Confidence:      confidenceSelElem,
			SelectionStatus: "SELECTED",
			Page:            &page,
			Geometry:        makeGeometry(geoLeft, geoSelTop, geoSelWidth, geoSelHeight),
		},
	}

	return blocks
}

// buildTablesBlocks builds TABLE and CELL blocks.
func buildTablesBlocks(page int) []Block {
	tableID := uuid.NewString()
	cell11 := uuid.NewString()
	cell12 := uuid.NewString()
	cell21 := uuid.NewString()
	cell22 := uuid.NewString()

	row1 := 1
	row2 := 2
	col1 := 1
	col2 := 2
	span1 := 1

	return []Block{
		{
			BlockType:  "TABLE",
			ID:         tableID,
			Confidence: confidenceTable,
			Page:       &page,
			Geometry:   makeGeometry(geoTableLeft, geoTableTop, geoTableWidth, geoTableHeight),
			Relationships: []Relationship{
				{Type: relTypeChild, Ids: []string{cell11, cell12, cell21, cell22}},
			},
		},
		{
			BlockType:    blockTypeCell,
			ID:           cell11,
			Text:         "Header 1",
			Confidence:   confidenceTable,
			Page:         &page,
			Geometry:     makeGeometry(geoCell1Left, geoCellTop1, geoCell1Width, geoCellHeight),
			RowIndex:     &row1,
			ColumnIndex:  &col1,
			RowSpan:      &span1,
			ColumnSpan:   &span1,
			EntityTypes:  []string{"COLUMN_HEADER"},
			ColumnHeader: true,
		},
		{
			BlockType:    blockTypeCell,
			ID:           cell12,
			Text:         "Header 2",
			Confidence:   confidenceTable,
			Page:         &page,
			Geometry:     makeGeometry(geoCell2Left, geoCellTop1, geoCell1Width, geoCellHeight),
			RowIndex:     &row1,
			ColumnIndex:  &col2,
			RowSpan:      &span1,
			ColumnSpan:   &span1,
			EntityTypes:  []string{"COLUMN_HEADER"},
			ColumnHeader: true,
		},
		{
			BlockType:   blockTypeCell,
			ID:          cell21,
			Text:        "Value 1",
			Confidence:  confidenceTableCell,
			Page:        &page,
			Geometry:    makeGeometry(geoCell1Left, geoCellTop2, geoCell1Width, geoCellHeight),
			RowIndex:    &row2,
			ColumnIndex: &col1,
			RowSpan:     &span1,
			ColumnSpan:  &span1,
		},
		{
			BlockType:   blockTypeCell,
			ID:          cell22,
			Text:        "Value 2",
			Confidence:  confidenceTableCell,
			Page:        &page,
			Geometry:    makeGeometry(geoCell2Left, geoCellTop2, geoCell1Width, geoCellHeight),
			RowIndex:    &row2,
			ColumnIndex: &col2,
			RowSpan:     &span1,
			ColumnSpan:  &span1,
		},
	}
}

// buildQueryBlocks builds QUERY and QUERY_RESULT blocks for each query.
func buildQueryBlocks(queries *QueriesConfig, page int) []Block {
	const blocksPerQuery = 2 // one QUERY + one QUERY_RESULT per query entry
	blocks := make([]Block, 0, len(queries.Queries)*blocksPerQuery)

	for _, q := range queries.Queries {
		queryID := uuid.NewString()
		resultID := uuid.NewString()

		pages := q.Pages
		if len(pages) == 0 {
			pages = []string{"1"}
		}

		blocks = append(blocks,
			Block{
				BlockType:  "QUERY",
				ID:         queryID,
				Confidence: confidenceQuery,
				Page:       &page,
				Query: &QueryBlock{
					Text:  q.Text,
					Alias: q.Alias,
					Pages: pages,
				},
				Relationships: []Relationship{
					{Type: relTypeAnswer, Ids: []string{resultID}},
				},
			},
			Block{
				BlockType:  "QUERY_RESULT",
				ID:         resultID,
				Text:       "(no answer)",
				Confidence: confidenceQueryResult,
				Page:       &page,
			},
		)
	}

	return blocks
}

// buildSignatureBlocks builds SIGNATURE blocks.
func buildSignatureBlocks(page int) []Block {
	return []Block{
		{
			BlockType:  "SIGNATURE",
			ID:         uuid.NewString(),
			Confidence: confidenceSignature,
			Page:       &page,
			Geometry:   makeGeometry(geoSigLeft, geoSigTop, geoSigWidth, geoSigHeight),
		},
	}
}

// buildLayoutBlocks builds LAYOUT_* blocks.
func buildLayoutBlocks(page int) []Block {
	return []Block{
		{
			BlockType:  "LAYOUT_HEADER",
			ID:         uuid.NewString(),
			Text:       "Document Header",
			Confidence: confidenceLayout,
			Page:       &page,
			Geometry:   makeGeometry(geoHdrLeft, geoHdrTop, geoHdrWidth, geoHdrHeight),
		},
		{
			BlockType:  "LAYOUT_TEXT",
			ID:         uuid.NewString(),
			Text:       "Body text content",
			Confidence: confidenceLayout,
			Page:       &page,
			Geometry:   makeGeometry(geoBodyLeft, geoBodyTop, geoBodyWidth, geoBodyHeight),
		},
		{
			BlockType:  "LAYOUT_FOOTER",
			ID:         uuid.NewString(),
			Text:       "Page 1",
			Confidence: confidenceLayout,
			Page:       &page,
			Geometry:   makeGeometry(geoFtrLeft, geoFtrTop, geoFtrWidth, geoFtrHeight),
		},
	}
}

// cloneJob returns a deep copy of a DocumentJob.
func cloneJob(j *DocumentJob) *DocumentJob {
	cp := *j
	cp.Blocks = make([]Block, len(j.Blocks))
	copy(cp.Blocks, j.Blocks)

	if j.Warnings != nil {
		cp.Warnings = make([]WarningBlock, len(j.Warnings))
		copy(cp.Warnings, j.Warnings)
	}

	return &cp
}

// cloneExpenseJob returns a deep copy of an ExpenseJob.
func cloneExpenseJob(j *ExpenseJob) *ExpenseJob {
	cp := *j
	cp.ExpenseDocuments = make([]ExpenseDocument, len(j.ExpenseDocuments))

	for i, doc := range j.ExpenseDocuments {
		docCopy := doc
		docCopy.Blocks = make([]Block, len(doc.Blocks))
		copy(docCopy.Blocks, doc.Blocks)
		docCopy.SummaryFields = make([]ExpenseField, len(doc.SummaryFields))
		copy(docCopy.SummaryFields, doc.SummaryFields)
		docCopy.LineItemGroups = make([]LineItemGroup, len(doc.LineItemGroups))
		copy(docCopy.LineItemGroups, doc.LineItemGroups)
		cp.ExpenseDocuments[i] = docCopy
	}

	return &cp
}

// cloneLendingJob returns a deep copy of a LendingJob.
func cloneLendingJob(j *LendingJob) *LendingJob {
	cp := *j
	cp.Results = make([]LendingResult, len(j.Results))
	copy(cp.Results, j.Results)

	if j.Warnings != nil {
		cp.Warnings = make([]WarningBlock, len(j.Warnings))
		copy(cp.Warnings, j.Warnings)
	}

	return &cp
}

// trimJobsIfNeeded removes the oldest jobs when the job count exceeds maxJobs.
// Caller must hold the write lock.
func (b *InMemoryBackend) trimJobsIfNeeded() {
	if len(b.jobs) <= b.maxJobs {
		return
	}

	// Collect jobs sorted by creation time (oldest first).
	type entry struct {
		job *DocumentJob
		id  string
	}

	entries := make([]entry, 0, len(b.jobs))
	for id, j := range b.jobs {
		entries = append(entries, entry{id: id, job: j})
	}

	sort.Slice(entries, func(i, k int) bool {
		return entries[i].job.CreationTime.Before(entries[k].job.CreationTime)
	})

	// Remove oldest entries until we are at the limit.
	excess := len(b.jobs) - b.maxJobs
	for i := range excess {
		delete(b.jobs, entries[i].id)
	}
}

// trimExpenseJobsIfNeeded bounds the expenseJobs map by evicting oldest entries.
// Caller must hold the write lock.
func (b *InMemoryBackend) trimExpenseJobsIfNeeded() {
	if len(b.expenseJobs) <= b.maxJobs {
		return
	}

	type entry struct {
		t  time.Time
		id string
	}

	entries := make([]entry, 0, len(b.expenseJobs))
	for id, j := range b.expenseJobs {
		entries = append(entries, entry{id: id, t: j.CreationTime})
	}

	sort.Slice(entries, func(i, k int) bool { return entries[i].t.Before(entries[k].t) })

	excess := len(b.expenseJobs) - b.maxJobs
	for i := range excess {
		delete(b.expenseJobs, entries[i].id)
	}
}

// trimLendingJobsIfNeeded bounds the lendingJobs map by evicting oldest entries.
// Caller must hold the write lock.
func (b *InMemoryBackend) trimLendingJobsIfNeeded() {
	if len(b.lendingJobs) <= b.maxJobs {
		return
	}

	type entry struct {
		t  time.Time
		id string
	}

	entries := make([]entry, 0, len(b.lendingJobs))
	for id, j := range b.lendingJobs {
		entries = append(entries, entry{id: id, t: j.CreationTime})
	}

	sort.Slice(entries, func(i, k int) bool { return entries[i].t.Before(entries[k].t) })

	excess := len(b.lendingJobs) - b.maxJobs
	for i := range excess {
		delete(b.lendingJobs, entries[i].id)
	}
}

// AnalyzeDocument performs a synchronous document analysis and returns blocks
// based on the requested feature types.
func (b *InMemoryBackend) AnalyzeDocument(documentURI string) []Block {
	return syntheticBlocks(documentURI)
}

// AnalyzeDocumentWithFeatures performs synchronous document analysis using feature types.
func (b *InMemoryBackend) AnalyzeDocumentWithFeatures(
	documentURI string,
	featureTypes []string,
	queries *QueriesConfig,
) []Block {
	return analyzeDocumentBlocks(documentURI, featureTypes, queries)
}

// DetectDocumentText performs synchronous text detection and returns proper blocks.
func (b *InMemoryBackend) DetectDocumentText(documentURI string) []Block {
	return syntheticBlocks(documentURI)
}

// defaultAsyncJobDelay is the default time before a new async job transitions from IN_PROGRESS to SUCCEEDED.
const defaultAsyncJobDelay = 200 * time.Millisecond

// StartDocumentAnalysis creates an async document analysis job.
func (b *InMemoryBackend) StartDocumentAnalysis(documentURI string) (*DocumentJob, error) {
	return b.StartDocumentAnalysisWithOptions(documentURI, nil, nil, nil, "", "")
}

// StartDocumentAnalysisWithOptions creates an async document analysis job with full options.
func (b *InMemoryBackend) StartDocumentAnalysisWithOptions(
	documentURI string,
	featureTypes []string,
	queries *QueriesConfig,
	outputConfig *OutputConfig,
	jobTag, clientRequestToken string,
) (*DocumentJob, error) {
	b.mu.Lock("StartDocumentAnalysis")

	// Idempotency: if token already seen, return existing job.
	if clientRequestToken != "" {
		if existingID, ok := b.clientTokenToJobID[clientRequestToken]; ok {
			if existing, ok2 := b.jobs[existingID]; ok2 {
				result := cloneJob(existing)
				b.mu.Unlock()

				return result, nil
			}
		}
	}

	jobID := uuid.NewString()
	blocks := analyzeDocumentBlocks(documentURI, featureTypes, queries)
	job := &DocumentJob{
		JobID:              jobID,
		JobStatus:          jobStatusInProgress,
		JobType:            jobTypeDocumentAnalysis,
		CreationTime:       time.Now(),
		Blocks:             blocks,
		OutputConfig:       outputConfig,
		JobTag:             jobTag,
		ClientRequestToken: clientRequestToken,
	}
	b.jobs[jobID] = job
	b.trimJobsIfNeeded()

	if clientRequestToken != "" {
		b.clientTokenToJobID[clientRequestToken] = jobID
	}

	if b.asyncJobDelay == 0 {
		// Synchronous mode (tests): complete immediately under the same lock.
		job.JobStatus = jobStatusSucceeded
		result := cloneJob(job)
		b.mu.Unlock()

		return result, nil
	}

	b.mu.Unlock()

	// Transition to SUCCEEDED after a short delay.
	go func() {
		time.Sleep(b.asyncJobDelay)

		b.mu.Lock("StartDocumentAnalysis-complete")
		defer b.mu.Unlock()

		if j, ok := b.jobs[jobID]; ok {
			j.JobStatus = jobStatusSucceeded
		}
	}()

	b.mu.RLock("StartDocumentAnalysis-read")
	result := cloneJob(b.jobs[jobID])
	b.mu.RUnlock()

	return result, nil
}

// GetDocumentAnalysis retrieves the results of a document analysis job.
// Returns a clone of the stored job.
func (b *InMemoryBackend) GetDocumentAnalysis(jobID string) (*DocumentJob, error) {
	b.mu.RLock("GetDocumentAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobID]
	if !ok || job.JobType != jobTypeDocumentAnalysis {
		return nil, fmt.Errorf("%w: job %s not found", ErrJobNotFound, jobID)
	}

	return cloneJob(job), nil
}

// StartDocumentTextDetection creates an async text detection job.
func (b *InMemoryBackend) StartDocumentTextDetection(documentURI string) (*DocumentJob, error) {
	return b.StartDocumentTextDetectionWithOptions(documentURI, nil, nil, "", "")
}

// StartDocumentTextDetectionWithOptions creates an async text detection job with options.
func (b *InMemoryBackend) StartDocumentTextDetectionWithOptions(
	documentURI string,
	outputConfig *OutputConfig,
	notificationChannel *NotificationChannel,
	jobTag, clientRequestToken string,
) (*DocumentJob, error) {
	b.mu.Lock("StartDocumentTextDetection")

	// Idempotency: if token already seen, return existing job.
	if clientRequestToken != "" {
		if existingID, ok := b.clientTokenToJobID[clientRequestToken]; ok {
			if existing, ok2 := b.jobs[existingID]; ok2 {
				result := cloneJob(existing)
				b.mu.Unlock()

				return result, nil
			}
		}
	}

	jobID := uuid.NewString()
	job := &DocumentJob{
		JobID:               jobID,
		JobStatus:           jobStatusInProgress,
		JobType:             jobTypeTextDetection,
		CreationTime:        time.Now(),
		Blocks:              syntheticBlocks(documentURI),
		OutputConfig:        outputConfig,
		NotificationChannel: notificationChannel,
		JobTag:              jobTag,
		ClientRequestToken:  clientRequestToken,
	}
	b.jobs[jobID] = job
	b.trimJobsIfNeeded()

	if clientRequestToken != "" {
		b.clientTokenToJobID[clientRequestToken] = jobID
	}

	if b.asyncJobDelay == 0 {
		job.JobStatus = jobStatusSucceeded
		result := cloneJob(job)
		b.mu.Unlock()

		return result, nil
	}

	b.mu.Unlock()

	go func() {
		time.Sleep(b.asyncJobDelay)

		b.mu.Lock("StartDocumentTextDetection-complete")
		defer b.mu.Unlock()

		if j, ok := b.jobs[jobID]; ok {
			j.JobStatus = jobStatusSucceeded
		}
	}()

	b.mu.RLock("StartDocumentTextDetection-read")
	result := cloneJob(b.jobs[jobID])
	b.mu.RUnlock()

	return result, nil
}

// GetDocumentTextDetection retrieves the results of a text detection job.
// Returns a clone of the stored job.
func (b *InMemoryBackend) GetDocumentTextDetection(jobID string) (*DocumentJob, error) {
	b.mu.RLock("GetDocumentTextDetection")
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobID]
	if !ok || job.JobType != jobTypeTextDetection {
		return nil, fmt.Errorf("%w: job %s not found", ErrJobNotFound, jobID)
	}

	return cloneJob(job), nil
}

// ListJobs returns all stored jobs sorted by creation time (newest first).
func (b *InMemoryBackend) ListJobs() []DocumentJob {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	out := make([]DocumentJob, 0, len(b.jobs))
	for _, j := range b.jobs {
		out = append(out, *cloneJob(j))
	}

	// Sort newest first by creation time.
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreationTime.After(out[j].CreationTime)
	})

	return out
}

// paginateBlocks applies MaxResults and NextToken pagination to a blocks slice.
// Returns the page of blocks and a new NextToken (empty string means no more pages).
func paginateBlocks(blocks []Block, maxResults int, nextToken string) ([]Block, string) {
	offset := 0

	if nextToken != "" {
		decoded, err := base64.StdEncoding.DecodeString(nextToken)
		if err == nil {
			if n, err2 := strconv.Atoi(string(decoded)); err2 == nil && n >= 0 && n < len(blocks) {
				offset = n
			}
		}
	}

	if offset >= len(blocks) {
		return []Block{}, ""
	}

	end := len(blocks)
	if maxResults > 0 && offset+maxResults < end {
		end = offset + maxResults
	}

	page := make([]Block, end-offset)
	copy(page, blocks[offset:end])

	newToken := ""
	if end < len(blocks) {
		newToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}

	return page, newToken
}

// adapterVersionKey returns the storage key for an adapter version.
func adapterVersionKey(adapterID, version string) string {
	return adapterID + "#" + version
}

// cloneAdapter returns a deep copy of an Adapter.
func cloneAdapter(a *Adapter) *Adapter {
	cp := *a
	cp.FeatureTypes = make([]string, len(a.FeatureTypes))
	copy(cp.FeatureTypes, a.FeatureTypes)
	cp.Tags = cloneTags(a.Tags)

	return &cp
}

// cloneAdapterVersion returns a deep copy of an AdapterVersion.
func cloneAdapterVersion(av *AdapterVersion) *AdapterVersion {
	cp := *av
	cp.FeatureTypes = make([]string, len(av.FeatureTypes))
	copy(cp.FeatureTypes, av.FeatureTypes)
	cp.Tags = cloneTags(av.Tags)

	return &cp
}

// cloneTags returns a non-nil copy of a tags map.
func cloneTags(tags map[string]string) map[string]string {
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// syntheticExpenseDocument builds a realistic expense document.
func syntheticExpenseDocument(documentURI string) ExpenseDocument {
	blocks := syntheticBlocks(documentURI)

	summaryFields := []ExpenseField{
		{
			Type:           &ExpenseDetection{Text: "VENDOR_NAME", Confidence: confidenceExpenseHigh},
			ValueDetection: &ExpenseDetection{Text: "Acme Corp", Confidence: confidenceExpenseHigh},
			PageNumber:     1,
		},
		{
			Type:           &ExpenseDetection{Text: "TOTAL", Confidence: confidenceExpenseHigh},
			ValueDetection: &ExpenseDetection{Text: "$123.45", Confidence: confidenceExpenseHigh},
			PageNumber:     1,
			Currency:       &ExpenseDetection{Text: "USD", Confidence: confidenceExpenseHigh},
		},
		{
			Type:           &ExpenseDetection{Text: "INVOICE_RECEIPT_DATE", Confidence: confidenceExpenseMed},
			ValueDetection: &ExpenseDetection{Text: "2024-01-01", Confidence: confidenceExpenseMed},
			PageNumber:     1,
		},
	}

	lineItemGroups := []LineItemGroup{
		{
			LineItemGroupIndex: 1,
			LineItems: []LineItem{
				{
					LineItemExpenseFields: []ExpenseField{
						{
							Type:           &ExpenseDetection{Text: "ITEM", Confidence: confidenceExpenseLI},
							ValueDetection: &ExpenseDetection{Text: "Widget A", Confidence: confidenceExpenseLI},
							PageNumber:     1,
						},
						{
							Type:           &ExpenseDetection{Text: "PRICE", Confidence: confidenceExpenseLI},
							ValueDetection: &ExpenseDetection{Text: "$45.00", Confidence: confidenceExpenseLI},
							PageNumber:     1,
						},
					},
				},
				{
					LineItemExpenseFields: []ExpenseField{
						{
							Type:           &ExpenseDetection{Text: "ITEM", Confidence: confidenceExpenseLI2},
							ValueDetection: &ExpenseDetection{Text: "Widget B", Confidence: confidenceExpenseLI2},
							PageNumber:     1,
						},
						{
							Type:           &ExpenseDetection{Text: "PRICE", Confidence: confidenceExpenseLI2},
							ValueDetection: &ExpenseDetection{Text: "$78.45", Confidence: confidenceExpenseLI2},
							PageNumber:     1,
						},
					},
				},
			},
		},
	}

	return ExpenseDocument{
		ExpenseIndex:   1,
		Blocks:         blocks,
		SummaryFields:  summaryFields,
		LineItemGroups: lineItemGroups,
	}
}

// AnalyzeExpense performs a synchronous expense analysis and returns expense documents.
func (b *InMemoryBackend) AnalyzeExpense(documentURI string) []ExpenseDocument {
	doc := syntheticExpenseDocument(documentURI)

	return []ExpenseDocument{doc}
}

// syntheticIDDocument builds a realistic identity document.
func syntheticIDDocument(documentURI string, index int) IdentityDocument {
	blocks := syntheticBlocks(documentURI)

	fields := []IdentityDocumentField{
		{
			Type:           &AnalyzeIDDetections{Text: "FIRST_NAME", Confidence: confidenceExpenseHigh},
			ValueDetection: &AnalyzeIDDetections{Text: "JANE", Confidence: confidenceExpenseHigh},
		},
		{
			Type:           &AnalyzeIDDetections{Text: "LAST_NAME", Confidence: confidenceExpenseHigh},
			ValueDetection: &AnalyzeIDDetections{Text: "DOE", Confidence: confidenceExpenseHigh},
		},
		{
			Type: &AnalyzeIDDetections{Text: "DATE_OF_BIRTH", Confidence: confidenceExpenseMed},
			ValueDetection: &AnalyzeIDDetections{
				Text:            "1990-01-15",
				Confidence:      confidenceExpenseMed,
				NormalizedValue: &NormalizedValue{Value: "1990-01-15", ValueType: "DATE"},
			},
		},
		{
			Type: &AnalyzeIDDetections{Text: "EXPIRATION_DATE", Confidence: confidenceExpenseMed},
			ValueDetection: &AnalyzeIDDetections{
				Text:            "2030-12-31",
				Confidence:      confidenceExpenseMed,
				NormalizedValue: &NormalizedValue{Value: "2030-12-31", ValueType: "DATE"},
			},
		},
		{
			Type:           &AnalyzeIDDetections{Text: "DOCUMENT_NUMBER", Confidence: confidenceExpenseHigh},
			ValueDetection: &AnalyzeIDDetections{Text: "D12345678", Confidence: confidenceExpenseHigh},
		},
	}

	return IdentityDocument{
		DocumentIndex:          index,
		Blocks:                 blocks,
		IdentityDocumentFields: fields,
	}
}

// AnalyzeID performs a synchronous ID analysis and returns identity documents.
func (b *InMemoryBackend) AnalyzeID(documentURIs []string) []IdentityDocument {
	docs := make([]IdentityDocument, 0, len(documentURIs))
	for i, uri := range documentURIs {
		docs = append(docs, syntheticIDDocument(uri, i+1))
	}

	return docs
}

// syntheticLendingResults builds realistic lending results.
func syntheticLendingResults() []LendingResult {
	return []LendingResult{
		{
			Page: 1,
			PageClassification: &PageClassification{
				PageType:   []LendingDetection{{Text: "PAYSTUB", Confidence: confidenceLending}},
				PageNumber: []LendingDetection{{Text: "1", Confidence: confidencePage}},
			},
			Extractions: []Extraction{
				{
					LendingDocument: &LendingDocument{
						LendingFields: []LendingField{
							{
								Type:           &LendingDetection{Text: "BORROWER_NAME", Confidence: confidenceLending},
								ValueDetection: &LendingDetection{Text: "Jane Doe", Confidence: confidenceLending},
								PageNumber:     1,
							},
							{
								Type:           &LendingDetection{Text: "GROSS_INCOME", Confidence: confidenceLending2},
								ValueDetection: &LendingDetection{Text: "$5000.00", Confidence: confidenceLending2},
								PageNumber:     1,
							},
						},
						SignatureDetections: []SignatureDetection{
							{
								Confidence: confidenceLendSig,
								Geometry: makeGeometry(
									geoLendSigLeft, geoLendSigTop,
									geoLendSigWidth, geoLendSigHeight,
								),
							},
						},
					},
				},
			},
		},
	}
}

// syntheticLendingSummary builds a realistic lending summary.
func syntheticLendingSummary() *LendingSummary {
	return &LendingSummary{
		DocumentGroups: []DocumentGroup{
			{
				Type: "PAYSTUB",
				SplitDocuments: []SplitDocument{
					{Index: 1, Pages: []int{1}},
				},
				DetectedSignatures: []DetectedSignature{
					{Page: 1},
				},
			},
		},
		UndetectedDocumentTypes: []string{},
	}
}

// StartExpenseAnalysis creates an async expense analysis job.
func (b *InMemoryBackend) StartExpenseAnalysis(documentURI string) (*ExpenseJob, error) {
	b.mu.Lock("StartExpenseAnalysis")

	jobID := uuid.NewString()
	job := &ExpenseJob{
		JobID:            jobID,
		JobStatus:        jobStatusInProgress,
		CreationTime:     time.Now(),
		ExpenseDocuments: []ExpenseDocument{syntheticExpenseDocument(documentURI)},
	}
	b.expenseJobs[jobID] = job
	b.trimExpenseJobsIfNeeded()

	if b.asyncJobDelay == 0 {
		job.JobStatus = jobStatusSucceeded
		result := cloneExpenseJob(job)
		b.mu.Unlock()

		return result, nil
	}

	b.mu.Unlock()

	go func() {
		time.Sleep(b.asyncJobDelay)

		b.mu.Lock("StartExpenseAnalysis-complete")
		defer b.mu.Unlock()

		if j, ok := b.expenseJobs[jobID]; ok {
			j.JobStatus = jobStatusSucceeded
		}
	}()

	b.mu.RLock("StartExpenseAnalysis-read")
	result := cloneExpenseJob(b.expenseJobs[jobID])
	b.mu.RUnlock()

	return result, nil
}

// GetExpenseAnalysis retrieves the results of an expense analysis job.
// Returns a deep clone so callers may safely mutate the returned value.
func (b *InMemoryBackend) GetExpenseAnalysis(jobID string) (*ExpenseJob, error) {
	b.mu.RLock("GetExpenseAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.expenseJobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: expense job %s not found", ErrJobNotFound, jobID)
	}

	return cloneExpenseJob(job), nil
}

// StartLendingAnalysis creates an async lending analysis job.
func (b *InMemoryBackend) StartLendingAnalysis(_ string) (*LendingJob, error) {
	b.mu.Lock("StartLendingAnalysis")

	jobID := uuid.NewString()
	job := &LendingJob{
		JobID:        jobID,
		JobStatus:    jobStatusInProgress,
		CreationTime: time.Now(),
		Results:      syntheticLendingResults(),
		Summary:      syntheticLendingSummary(),
	}
	b.lendingJobs[jobID] = job
	b.trimLendingJobsIfNeeded()

	if b.asyncJobDelay == 0 {
		job.JobStatus = jobStatusSucceeded
		result := cloneLendingJob(job)
		b.mu.Unlock()

		return result, nil
	}

	b.mu.Unlock()

	go func() {
		time.Sleep(b.asyncJobDelay)

		b.mu.Lock("StartLendingAnalysis-complete")
		defer b.mu.Unlock()

		if j, ok := b.lendingJobs[jobID]; ok {
			j.JobStatus = jobStatusSucceeded
		}
	}()

	b.mu.RLock("StartLendingAnalysis-read")
	result := cloneLendingJob(b.lendingJobs[jobID])
	b.mu.RUnlock()

	return result, nil
}

// GetLendingAnalysis retrieves the results of a lending analysis job.
// Returns a clone of the stored job.
func (b *InMemoryBackend) GetLendingAnalysis(jobID string) (*LendingJob, error) {
	b.mu.RLock("GetLendingAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.lendingJobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: lending job %s not found", ErrJobNotFound, jobID)
	}

	return cloneLendingJob(job), nil
}

// validateAdapterFeatureTypes checks that adapter feature types are restricted to FORMS and QUERIES.
func validateAdapterFeatureTypes(featureTypes []string) error {
	if len(featureTypes) == 0 {
		return fmt.Errorf("%w: FeatureTypes must contain at least one value", ErrValidation)
	}

	for _, ft := range featureTypes {
		if !adapterAllowedFeatureTypes[ft] {
			return fmt.Errorf(
				"%w: invalid FeatureType %q for adapter (valid: FORMS, QUERIES)",
				ErrValidation,
				ft,
			)
		}
	}

	return nil
}

// buildAdapterARN constructs an ARN for a Textract adapter.
func buildAdapterARN(region, accountID, adapterID string) string {
	return fmt.Sprintf("arn:aws:textract:%s:%s:adapter/%s", region, accountID, adapterID)
}

// buildAdapterVersionARN constructs an ARN for a Textract adapter version.
func buildAdapterVersionARN(region, accountID, adapterID, version string) string {
	return fmt.Sprintf("arn:aws:textract:%s:%s:adapter/%s/version/%s", region, accountID, adapterID, version)
}

// arnAdapterID extracts the adapter ID from a Textract adapter ARN.
// Returns empty string if the ARN is not an adapter ARN (e.g. it's a version ARN).
func arnAdapterID(resourceARN string) string {
	if len(resourceARN) <= 8 || resourceARN[:4] != "arn:" {
		return ""
	}

	const prefix = "adapter/"

	idx := lastIndex(resourceARN, prefix)
	if idx < 0 {
		return ""
	}

	rest := resourceARN[idx+len(prefix):]
	if contains(rest, "/version/") {
		return ""
	}

	return rest
}

// resolveARNToAdapter finds an adapter by ARN or adapter ID.
func (b *InMemoryBackend) resolveARNToAdapter(resourceARN string) (*Adapter, bool) {
	// Try direct adapter ID match first.
	for _, a := range b.adapters {
		if a.AdapterID == resourceARN {
			return a, true
		}
	}

	// Try ARN match: arn:aws:textract:region:account:adapter/adapterID
	adapterID := arnAdapterID(resourceARN)
	if adapterID == "" {
		return nil, false
	}

	for _, a := range b.adapters {
		if a.AdapterID == adapterID {
			return a, true
		}
	}

	return nil, false
}

// resolveARNToAdapterVersion finds an adapter version by ARN.
func (b *InMemoryBackend) resolveARNToAdapterVersion(resourceARN string) (*AdapterVersion, bool) {
	const versionPrefix = "/version/"

	idx := lastIndex(resourceARN, versionPrefix)
	if idx < 0 {
		return nil, false
	}

	adapterPart := resourceARN[:idx]
	version := resourceARN[idx+len(versionPrefix):]

	// Extract adapter ID from adapter part.
	const adapterPrefix = "adapter/"

	adIdx := lastIndex(adapterPart, adapterPrefix)
	if adIdx < 0 {
		return nil, false
	}

	adapterID := adapterPart[adIdx+len(adapterPrefix):]
	key := adapterVersionKey(adapterID, version)
	av, ok := b.adapterVersions[key]

	return av, ok
}

// lastIndex finds the last occurrence of substr in s.
func lastIndex(s, substr string) int {
	last := -1
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			last = i
		}
	}

	return last
}

// contains checks if s contains substr.
func contains(s, substr string) bool {
	return lastIndex(s, substr) >= 0
}

// CreateAdapter creates a new Textract adapter and returns it.
func (b *InMemoryBackend) CreateAdapter(
	name, description, autoUpdate string,
	featureTypes []string,
	tags map[string]string,
) (*Adapter, error) {
	return b.CreateAdapterWithToken(name, description, autoUpdate, featureTypes, tags, "")
}

// CreateAdapterWithToken creates an adapter with ClientRequestToken dedup.
func (b *InMemoryBackend) CreateAdapterWithToken(
	name, description, autoUpdate string,
	featureTypes []string,
	tags map[string]string,
	clientRequestToken string,
) (*Adapter, error) {
	if err := validateAdapterFeatureTypes(featureTypes); err != nil {
		return nil, err
	}

	if autoUpdate == "" {
		autoUpdate = autoUpdateDisabled
	} else if autoUpdate != autoUpdateEnabled && autoUpdate != autoUpdateDisabled {
		return nil, fmt.Errorf(
			"%w: AutoUpdate must be ENABLED or DISABLED",
			ErrValidation,
		)
	}

	b.mu.Lock("CreateAdapter")
	defer b.mu.Unlock()

	// Idempotency check.
	if clientRequestToken != "" {
		if existingID, ok := b.adapterClientTokenToID[clientRequestToken]; ok {
			if existing, ok2 := b.adapters[existingID]; ok2 {
				return cloneAdapter(existing), nil
			}
		}
	}

	adapterID := uuid.NewString()
	adapter := &Adapter{
		AdapterID:          adapterID,
		AdapterName:        name,
		AutoUpdate:         autoUpdate,
		CreationTime:       time.Now(),
		Description:        description,
		FeatureTypes:       append([]string{}, featureTypes...),
		Tags:               cloneTags(tags),
		ClientRequestToken: clientRequestToken,
	}
	b.adapters[adapterID] = adapter

	if clientRequestToken != "" {
		b.adapterClientTokenToID[clientRequestToken] = adapterID
	}

	return cloneAdapter(adapter), nil
}

// GetAdapter retrieves an adapter by ID.
func (b *InMemoryBackend) GetAdapter(adapterID string) (*Adapter, error) {
	b.mu.RLock("GetAdapter")
	defer b.mu.RUnlock()

	adapter, ok := b.adapters[adapterID]
	if !ok {
		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	return cloneAdapter(adapter), nil
}

// UpdateAdapter updates mutable fields on an existing adapter.
func (b *InMemoryBackend) UpdateAdapter(adapterID, description, autoUpdate string) (*Adapter, error) {
	if autoUpdate != "" && autoUpdate != autoUpdateEnabled && autoUpdate != autoUpdateDisabled {
		return nil, fmt.Errorf("%w: AutoUpdate must be ENABLED or DISABLED", ErrValidation)
	}

	b.mu.Lock("UpdateAdapter")
	defer b.mu.Unlock()

	adapter, ok := b.adapters[adapterID]
	if !ok {
		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	if description != "" {
		adapter.Description = description
	}

	if autoUpdate != "" {
		adapter.AutoUpdate = autoUpdate
	}

	return cloneAdapter(adapter), nil
}

// ListAdapters returns a sorted list of all adapters.
func (b *InMemoryBackend) ListAdapters() []Adapter {
	b.mu.RLock("ListAdapters")
	defer b.mu.RUnlock()

	out := make([]Adapter, 0, len(b.adapters))
	for _, a := range b.adapters {
		out = append(out, *cloneAdapter(a))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].AdapterID < out[j].AdapterID
	})

	return out
}

// DeleteAdapter removes an adapter and all its versions by ID.
func (b *InMemoryBackend) DeleteAdapter(adapterID string) error {
	b.mu.Lock("DeleteAdapter")
	defer b.mu.Unlock()

	if _, ok := b.adapters[adapterID]; !ok {
		return fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	delete(b.adapters, adapterID)

	// Remove all versions belonging to this adapter.
	for key, av := range b.adapterVersions {
		if av.AdapterID == adapterID {
			delete(b.adapterVersions, key)
		}
	}

	return nil
}

// deterministic evaluation metrics for adapter versions.
const (
	evalF1Score   = 0.85
	evalPrecision = 0.88
	evalRecall    = 0.82
)

// CreateAdapterVersion creates a new version for an existing adapter.
func (b *InMemoryBackend) CreateAdapterVersion(adapterID string, tags map[string]string) (*AdapterVersion, error) {
	return b.CreateAdapterVersionWithOptions(adapterID, tags, nil, nil, "", "")
}

// CreateAdapterVersionWithOptions creates an adapter version with full options.
func (b *InMemoryBackend) CreateAdapterVersionWithOptions(
	adapterID string,
	tags map[string]string,
	datasetConfig *DatasetConfig,
	outputConfig *OutputConfig,
	kmsKeyID, clientRequestToken string,
) (*AdapterVersion, error) {
	b.mu.Lock("CreateAdapterVersion")

	adapter, ok := b.adapters[adapterID]
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	version := uuid.NewString()
	av := &AdapterVersion{
		AdapterID:      adapterID,
		AdapterVersion: version,
		CreationTime:   time.Now(),
		FeatureTypes:   append([]string{}, adapter.FeatureTypes...),
		// Start in CREATION_IN_PROGRESS; goroutine transitions to ACTIVE.
		Status:             adapterVersionCreating,
		Tags:               cloneTags(tags),
		DatasetConfig:      datasetConfig,
		OutputConfig:       outputConfig,
		KMSKeyId:           kmsKeyID,
		ClientRequestToken: clientRequestToken,
		EvaluationMetrics: &EvaluationMetrics{
			F1Score:   evalF1Score,
			Precision: evalPrecision,
			Recall:    evalRecall,
		},
	}
	b.adapterVersions[adapterVersionKey(adapterID, version)] = av

	if b.asyncJobDelay == 0 {
		av.Status = adapterVersionActive
		result := cloneAdapterVersion(av)
		b.mu.Unlock()

		return result, nil
	}

	b.mu.Unlock()

	// Transition to ACTIVE after a short delay.
	go func() {
		time.Sleep(b.asyncJobDelay)

		b.mu.Lock("CreateAdapterVersion-complete")
		defer b.mu.Unlock()

		key := adapterVersionKey(adapterID, version)
		if stored, ok2 := b.adapterVersions[key]; ok2 {
			stored.Status = adapterVersionActive
		}
	}()

	b.mu.RLock("CreateAdapterVersion-read")
	result := cloneAdapterVersion(b.adapterVersions[adapterVersionKey(adapterID, version)])
	b.mu.RUnlock()

	return result, nil
}

// GetAdapterVersion retrieves a specific adapter version.
func (b *InMemoryBackend) GetAdapterVersion(adapterID, version string) (*AdapterVersion, error) {
	b.mu.RLock("GetAdapterVersion")
	defer b.mu.RUnlock()

	av, ok := b.adapterVersions[adapterVersionKey(adapterID, version)]
	if !ok {
		return nil, fmt.Errorf("%w: adapter version %s/%s not found", ErrAdapterVersionNotFound, adapterID, version)
	}

	return cloneAdapterVersion(av), nil
}

// ListAdapterVersions returns all versions for a given adapter, sorted by version string.
func (b *InMemoryBackend) ListAdapterVersions(adapterID string) ([]AdapterVersion, error) {
	b.mu.RLock("ListAdapterVersions")
	defer b.mu.RUnlock()

	if _, ok := b.adapters[adapterID]; !ok {
		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	out := make([]AdapterVersion, 0, len(b.adapterVersions))
	for _, av := range b.adapterVersions {
		if av.AdapterID == adapterID {
			out = append(out, *cloneAdapterVersion(av))
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].AdapterVersion < out[j].AdapterVersion
	})

	return out, nil
}

// DeleteAdapterVersion removes a specific adapter version.
func (b *InMemoryBackend) DeleteAdapterVersion(adapterID, version string) error {
	b.mu.Lock("DeleteAdapterVersion")
	defer b.mu.Unlock()

	key := adapterVersionKey(adapterID, version)
	if _, ok := b.adapterVersions[key]; !ok {
		return fmt.Errorf("%w: adapter version %s/%s not found", ErrAdapterVersionNotFound, adapterID, version)
	}

	delete(b.adapterVersions, key)

	return nil
}

// TagResource adds or replaces tags on an adapter or adapter version identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	// Try adapter version first (ARN contains /version/).
	if av, ok := b.resolveARNToAdapterVersion(resourceARN); ok {
		maps.Copy(av.Tags, tags)

		return nil
	}

	// Try adapter.
	if a, ok := b.resolveARNToAdapter(resourceARN); ok {
		maps.Copy(a.Tags, tags)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrAdapterNotFound, resourceARN)
}

// UntagResource removes the specified tag keys from an adapter or adapter version.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	// Try adapter version first.
	if av, ok := b.resolveARNToAdapterVersion(resourceARN); ok {
		for _, k := range tagKeys {
			delete(av.Tags, k)
		}

		return nil
	}

	// Try adapter.
	if a, ok := b.resolveARNToAdapter(resourceARN); ok {
		for _, k := range tagKeys {
			delete(a.Tags, k)
		}

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrAdapterNotFound, resourceARN)
}

// ListTagsForResource returns a copy of the tags for an adapter or adapter version.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	// Try adapter version first.
	if av, ok := b.resolveARNToAdapterVersion(resourceARN); ok {
		return cloneTags(av.Tags), nil
	}

	// Try adapter.
	if a, ok := b.resolveARNToAdapter(resourceARN); ok {
		return cloneTags(a.Tags), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrAdapterNotFound, resourceARN)
}

// GetLendingAnalysisSummary returns a summary of a lending analysis job.
func (b *InMemoryBackend) GetLendingAnalysisSummary(jobID string) (*LendingJob, error) {
	b.mu.RLock("GetLendingAnalysisSummary")
	defer b.mu.RUnlock()

	job, ok := b.lendingJobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: lending job %s not found", ErrJobNotFound, jobID)
	}

	// Return a summary: same job status and summary, without per-page lending results.
	cp := *job
	cp.Results = nil

	return &cp, nil
}

// BuildAdapterARN returns the ARN for an adapter (exported for handler use).
func (b *InMemoryBackend) BuildAdapterARN(adapterID string) string {
	return buildAdapterARN(b.region, b.accountID, adapterID)
}

// BuildAdapterVersionARN returns the ARN for an adapter version (exported for handler use).
func (b *InMemoryBackend) BuildAdapterVersionARN(adapterID, version string) string {
	return buildAdapterVersionARN(b.region, b.accountID, adapterID, version)
}

// PaginateBlocks applies pagination to blocks (exported for handler use).
func PaginateBlocks(blocks []Block, maxResults int, nextToken string) ([]Block, string) {
	return paginateBlocks(blocks, maxResults, nextToken)
}

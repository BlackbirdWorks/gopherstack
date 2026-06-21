package textract

import "context"

// StorageBackend is the interface for Textract storage operations.
type StorageBackend interface {
	AnalyzeDocument(ctx context.Context, documentURI string) []Block
	AnalyzeExpense(ctx context.Context, documentURI string) []ExpenseDocument
	AnalyzeID(ctx context.Context, documentURIs []string) []IdentityDocument
	CreateAdapter(
		ctx context.Context,
		name, description, autoUpdate string,
		featureTypes []string, tags map[string]string,
	) (*Adapter, error)
	CreateAdapterVersion(ctx context.Context, adapterID string, tags map[string]string) (
		*AdapterVersion, error,
	)
	DeleteAdapter(ctx context.Context, adapterID string) error
	DeleteAdapterVersion(ctx context.Context, adapterID, version string) error
	DetectDocumentText(ctx context.Context, documentURI string) []Block
	GetAdapter(ctx context.Context, adapterID string) (*Adapter, error)
	GetAdapterVersion(ctx context.Context, adapterID, version string) (*AdapterVersion, error)
	GetDocumentAnalysis(ctx context.Context, jobID string) (*DocumentJob, error)
	GetDocumentTextDetection(ctx context.Context, jobID string) (*DocumentJob, error)
	GetExpenseAnalysis(ctx context.Context, jobID string) (*ExpenseJob, error)
	GetLendingAnalysis(ctx context.Context, jobID string) (*LendingJob, error)
	GetLendingAnalysisSummary(ctx context.Context, jobID string) (*LendingJob, error)
	ListAdapterVersions(ctx context.Context, adapterID string) ([]AdapterVersion, error)
	ListAdapters(ctx context.Context) []Adapter
	ListJobs(ctx context.Context) []DocumentJob
	ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error)
	Region() string
	Reset()
	Restore(ctx context.Context, data []byte) error
	Snapshot(ctx context.Context) []byte
	StartDocumentAnalysis(ctx context.Context, documentURI string) (*DocumentJob, error)
	StartDocumentTextDetection(ctx context.Context, documentURI string) (*DocumentJob, error)
	StartExpenseAnalysis(ctx context.Context, documentURI string) (*ExpenseJob, error)
	StartLendingAnalysis(ctx context.Context, documentURI string) (*LendingJob, error)
	TagResource(ctx context.Context, resourceARN string, tags map[string]string) error
	UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error
	UpdateAdapter(ctx context.Context, adapterID, description, autoUpdate string) (
		*Adapter, error,
	)
}

var _ StorageBackend = (*InMemoryBackend)(nil)

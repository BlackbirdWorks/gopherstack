package textract

// StorageBackend is the interface for Textract storage operations.
type StorageBackend interface {
	AnalyzeDocument(documentURI string) []Block
	AnalyzeExpense(documentURI string) []ExpenseDocument
	AnalyzeID(documentURIs []string) []IdentityDocument
	CreateAdapter(name, description, autoUpdate string, featureTypes []string, tags map[string]string) (*Adapter, error)
	CreateAdapterVersion(adapterID string, tags map[string]string) (*AdapterVersion, error)
	DeleteAdapter(adapterID string) error
	DeleteAdapterVersion(adapterID, version string) error
	DetectDocumentText(documentURI string) []Block
	GetAdapter(adapterID string) (*Adapter, error)
	GetAdapterVersion(adapterID, version string) (*AdapterVersion, error)
	GetDocumentAnalysis(jobID string) (*DocumentJob, error)
	GetDocumentTextDetection(jobID string) (*DocumentJob, error)
	GetExpenseAnalysis(jobID string) (*ExpenseJob, error)
	GetLendingAnalysis(jobID string) (*LendingJob, error)
	GetLendingAnalysisSummary(jobID string) (*LendingJob, error)
	ListAdapterVersions(adapterID string) ([]AdapterVersion, error)
	ListAdapters() []Adapter
	ListJobs() []DocumentJob
	ListTagsForResource(resourceARN string) (map[string]string, error)
	Reset()
	Restore(data []byte) error
	Snapshot() []byte
	StartDocumentAnalysis(documentURI string) (*DocumentJob, error)
	StartDocumentTextDetection(documentURI string) (*DocumentJob, error)
	StartExpenseAnalysis(documentURI string) (*ExpenseJob, error)
	StartLendingAnalysis(documentURI string) (*LendingJob, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	UpdateAdapter(adapterID, description, autoUpdate string) (*Adapter, error)
}

var _ StorageBackend = (*InMemoryBackend)(nil)

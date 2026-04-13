package textract

// StorageBackend is the interface for Textract storage operations.
type StorageBackend interface {
	AnalyzeDocument(documentURI string) []Block
	DetectDocumentText(documentURI string) []Block
	StartDocumentAnalysis(documentURI string) (*DocumentJob, error)
	GetDocumentAnalysis(jobID string) (*DocumentJob, error)
	StartDocumentTextDetection(documentURI string) (*DocumentJob, error)
	GetDocumentTextDetection(jobID string) (*DocumentJob, error)
	ListJobs() []DocumentJob
	AnalyzeExpense(documentURI string) []ExpenseDocument
	AnalyzeID(documentURIs []string) []IdentityDocument
	CreateAdapter(name, description string, featureTypes []string, tags map[string]string) (*Adapter, error)
	GetAdapter(adapterID string) (*Adapter, error)
	DeleteAdapter(adapterID string) error
	CreateAdapterVersion(adapterID string, tags map[string]string) (*AdapterVersion, error)
	GetAdapterVersion(adapterID, version string) (*AdapterVersion, error)
	DeleteAdapterVersion(adapterID, version string) error
	GetExpenseAnalysis(jobID string) (*ExpenseJob, error)
	GetLendingAnalysis(jobID string) (*LendingJob, error)
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)

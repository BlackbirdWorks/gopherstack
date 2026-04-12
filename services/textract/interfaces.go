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
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)

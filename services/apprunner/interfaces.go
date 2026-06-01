package apprunner

import "time"

// StorageBackend is the interface for App Runner storage operations.
type StorageBackend interface {
	CreateService(name string, cpu, memory string, imageURI string, tags map[string]string) (*Service, error)
	DescribeService(serviceArn string) (*Service, error)
	UpdateService(serviceArn string, cpu, memory, imageURI string) (*Service, error)
	DeleteService(serviceArn string) (*Service, error)
	ListServices(maxResults int32, nextToken string) ([]*ServiceSummary, string, error)
	PauseService(serviceArn string) (*Service, error)
	ResumeService(serviceArn string) (*Service, error)
	ListOperations(serviceArn string, maxResults int32, nextToken string) ([]*OperationSummary, string, error)
	StartDeployment(serviceArn string) (string, error)

	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, keys []string) error
	ListTagsForResource(resourceArn string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// Service represents an App Runner service with full details.
// CreatedAt is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type Service struct {
	CreatedAt   time.Time
	UpdatedAt   time.Time
	ServiceArn  string
	ServiceID   string
	ServiceName string
	ServiceURL  string
	Status      string
	CPU         string
	Memory      string
	ImageURI    string
}

// ServiceSummary is a service entry in a list response.
// CreatedAt is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type ServiceSummary struct {
	CreatedAt   time.Time
	ServiceArn  string
	ServiceID   string
	ServiceName string
	ServiceURL  string
	Status      string
}

// OperationSummary is an operation entry in a list response.
// StartedAt is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type OperationSummary struct {
	StartedAt time.Time
	EndedAt   time.Time
	ID        string
	Type      string
	Status    string
	TargetArn string
}

var _ StorageBackend = (*InMemoryBackend)(nil)

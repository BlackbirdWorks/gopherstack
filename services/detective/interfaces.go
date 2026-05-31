package detective

import "time"

// StorageBackend is the interface for Detective storage operations.
type StorageBackend interface {
	CreateGraph(tags map[string]string) (*Graph, error)
	DeleteGraph(graphARN string) error
	ListGraphs(maxResults int32, nextToken string) ([]*Graph, string, error)

	CreateMembers(graphARN string, accounts []Account, message string) ([]*MemberDetail, []UnprocessedAccount, error)
	DeleteMembers(graphARN string, accountIDs []string) ([]string, []UnprocessedAccount, error)
	GetMembers(graphARN string, accountIDs []string) ([]*MemberDetail, []UnprocessedAccount, error)
	ListMembers(graphARN string, maxResults int32, nextToken string) ([]*MemberDetail, string, error)

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// Graph represents a Detective behavior graph.
// CreatedTime is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type Graph struct {
	CreatedTime time.Time
	Tags        map[string]string
	Arn         string
}

// Account is an AWS account for member operations.
type Account struct {
	AccountID    string
	EmailAddress string
}

// MemberDetail is the detail of a behavior graph member.
// time.Time fields are first so their non-pointer prefix reduces GC pointer bytes.
type MemberDetail struct {
	InvitedTime     time.Time
	UpdatedTime     time.Time
	AccountID       string
	AdministratorID string
	EmailAddress    string
	GraphARN        string
	Status          string
}

// UnprocessedAccount is an account that could not be processed.
type UnprocessedAccount struct {
	AccountID string
	Reason    string
}

var _ StorageBackend = (*InMemoryBackend)(nil)

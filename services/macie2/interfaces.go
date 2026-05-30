package macie2

import "time"

// StorageBackend is the interface for Macie2 storage operations.
type StorageBackend interface {
	// Session management
	GetSession() *Session
	EnableMacie(clientToken, frequency, status string) error
	DisableMacie() error
	UpdateMacieSession(frequency, status string) error

	// Allow list operations
	CreateAllowList(name, description string, criteria AllowListCriteria, tags map[string]string) (*AllowListSummary, error)
	GetAllowList(id string) (*AllowListDetail, error)
	UpdateAllowList(id, name, description string, criteria AllowListCriteria) (*AllowListSummary, error)
	DeleteAllowList(id string) error
	ListAllowLists() ([]*AllowListSummary, error)

	// Custom data identifier operations
	CreateCustomDataIdentifier(
		name, description, regex string,
		ignoreWords, keywords []string,
		maxMatchDistance *int32,
		tags map[string]string,
	) (string, error)
	GetCustomDataIdentifier(id string) (*CustomDataIdentifier, error)
	DeleteCustomDataIdentifier(id string) error
	ListCustomDataIdentifiers() ([]*CustomDataIdentifierSummary, error)
	TestCustomDataIdentifier(regex string, ignoreWords, keywords []string, maxMatchDistance *int32, sampleText string) (int32, error)

	// Findings filter operations
	CreateFindingsFilter(name, description, action string, position *int32, criteria map[string]any, tags map[string]string) (*FindingsFilterSummary, error)
	GetFindingsFilter(id string) (*FindingsFilterDetail, error)
	UpdateFindingsFilter(id, name, description, action string, position *int32, criteria map[string]any) (*FindingsFilterSummary, error)
	DeleteFindingsFilter(id string) error
	ListFindingsFilters() ([]*FindingsFilterSummary, error)

	// Finding operations
	GetFindings(findingIDs []string) ([]*Finding, error)
	ListFindings(criteria map[string]any, maxResults int, nextToken string) ([]string, string, error)
	CreateSampleFindings(findingTypes []string) error
	GetFindingStatistics(groupBy string, criteria map[string]any) ([]FindingStatisticsGroup, error)

	// Tag operations
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Lifecycle
	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// Session represents the Macie account state.
type Session struct {
	CreatedAt                  time.Time `json:"createdAt"`
	UpdatedAt                  time.Time `json:"updatedAt"`
	FindingPublishingFrequency string    `json:"findingPublishingFrequency"`
	ServiceRole                string    `json:"serviceRole"`
	Status                     string    `json:"status"`
	Enabled                    bool      `json:"-"`
}

// AllowListCriteria holds criteria for an allow list.
type AllowListCriteria struct {
	Regex        *string        `json:"regex,omitempty"`
	S3WordsList  *S3WordsList   `json:"s3WordsList,omitempty"`
}

// S3WordsList references an S3 object containing ignore words.
type S3WordsList struct {
	BucketName string `json:"bucketName"`
	ObjectKey  string `json:"objectKey"`
}

// AllowListStatus describes the status of an allow list.
type AllowListStatus struct {
	Code        string `json:"code"`
	Description string `json:"description,omitempty"`
}

// AllowListSummary is the summary view of an allow list.
type AllowListSummary struct {
	Arn         string            `json:"arn"`
	CreatedAt   time.Time         `json:"createdAt"`
	Description string            `json:"description,omitempty"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// AllowListDetail is the full detail view of an allow list.
type AllowListDetail struct {
	Arn         string            `json:"arn"`
	CreatedAt   time.Time         `json:"createdAt"`
	Criteria    AllowListCriteria `json:"criteria"`
	Description string            `json:"description,omitempty"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Status      AllowListStatus   `json:"status"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// CustomDataIdentifier represents a custom data identifier.
type CustomDataIdentifier struct {
	Arn                  string            `json:"arn"`
	CreatedAt            time.Time         `json:"createdAt"`
	Deleted              bool              `json:"deleted"`
	Description          string            `json:"description,omitempty"`
	ID                   string            `json:"id"`
	IgnoreWords          []string          `json:"ignoreWords,omitempty"`
	Keywords             []string          `json:"keywords,omitempty"`
	MaximumMatchDistance int32             `json:"maximumMatchDistance"`
	Name                 string            `json:"name"`
	Regex                string            `json:"regex"`
	Tags                 map[string]string `json:"tags,omitempty"`
}

// CustomDataIdentifierSummary is the summary view of a custom data identifier.
type CustomDataIdentifierSummary struct {
	Arn         string    `json:"arn"`
	CreatedAt   time.Time `json:"createdAt"`
	Description string    `json:"description,omitempty"`
	ID          string    `json:"id"`
	Name        string    `json:"name"`
}

// FindingsFilterDetail is the full detail of a findings filter.
type FindingsFilterDetail struct {
	Action          string            `json:"action"`
	Arn             string            `json:"arn"`
	Description     string            `json:"description,omitempty"`
	FindingCriteria map[string]any    `json:"findingCriteria,omitempty"`
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Position        int32             `json:"position"`
	Tags            map[string]string `json:"tags,omitempty"`
}

// FindingsFilterSummary is the summary view of a findings filter.
type FindingsFilterSummary struct {
	Action      string            `json:"action"`
	Arn         string            `json:"arn"`
	Description string            `json:"description,omitempty"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Position    int32             `json:"position"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// FindingType represents the type of a finding.
type FindingType string

// Finding represents a Macie finding.
type Finding struct { //nolint:govet // fieldalignment: readability over padding
	AccountID   string    `json:"accountId"`
	Archived    bool      `json:"archived"`
	Category    string    `json:"category"`
	CreatedAt   time.Time `json:"createdAt"`
	Description string    `json:"description"`
	ID          string    `json:"id"`
	Region      string    `json:"region"`
	Severity    Severity  `json:"severity"`
	Title       string    `json:"title"`
	Type        string    `json:"type"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// Severity holds finding severity details.
type Severity struct {
	Description string  `json:"description"`
	Score       float64 `json:"score"`
}

// FindingStatisticsGroup holds a group of finding statistics.
type FindingStatisticsGroup struct {
	Count    int64  `json:"count"`
	GroupKey string `json:"groupKey"`
}

var _ StorageBackend = (*InMemoryBackend)(nil)

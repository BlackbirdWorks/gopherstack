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

	AcceptInvitation(graphARN string) error
	RejectInvitation(graphARN string) error
	DisassociateMembership(graphARN string) error
	ListInvitations(maxResults int32, nextToken string) ([]*MemberDetail, string, error)

	StartInvestigation(graphARN, entityARN, entityType string, scopeStart, scopeEnd time.Time) (string, error)
	GetInvestigation(graphARN, investigationID string) (*Investigation, error)
	ListInvestigations(graphARN string, maxResults int32, nextToken string) ([]*InvestigationDetail, string, error)
	UpdateInvestigationState(graphARN, investigationID, state string) error
	ListIndicators(
		graphARN, investigationID, indicatorType string,
		maxResults int32,
		nextToken string,
	) ([]*Indicator, string, error)

	ListDatasourcePackages(
		graphARN string,
		maxResults int32,
		nextToken string,
	) (map[string]DatasourcePackageIngestDetail, string, error)
	UpdateDatasourcePackages(graphARN string, packages []string) error
	BatchGetGraphMemberDatasources(
		graphARN string,
		accountIDs []string,
	) ([]MembershipDatasources, []UnprocessedAccount, error)
	BatchGetMembershipDatasources(graphARNs []string) ([]MembershipDatasources, []UnprocessedGraph, error)

	EnableOrganizationAdminAccount(accountID string) error
	DisableOrganizationAdminAccount() error
	ListOrganizationAdminAccounts(maxResults int32, nextToken string) ([]*OrgAdmin, string, error)
	DescribeOrganizationConfiguration(graphARN string) (bool, error)
	UpdateOrganizationConfiguration(graphARN string, autoEnable bool) error

	StartMonitoringMember(graphARN, accountID string) error

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

// UnprocessedGraph is a graph that could not be processed.
type UnprocessedGraph struct {
	GraphArn string
	Reason   string
}

// Investigation holds investigation report data.
// time.Time fields are first so their non-pointer prefix reduces GC pointer bytes.
type Investigation struct {
	CreatedTime     time.Time
	ScopeStartTime  time.Time
	ScopeEndTime    time.Time
	GraphARN        string
	InvestigationID string
	EntityARN       string
	EntityType      string
	Severity        string
	State           string
	Status          string
}

// InvestigationDetail is a summary of an investigation used in list responses.
// time.Time is first so its non-pointer prefix reduces GC pointer bytes.
type InvestigationDetail struct {
	CreatedTime     time.Time
	EntityARN       string
	EntityType      string
	InvestigationID string
	Severity        string
	State           string
	Status          string
}

// Indicator is a compromise indicator within an investigation.
type Indicator struct {
	IndicatorType string
	Title         string
}

// DatasourcePackageIngestDetail holds the ingest state for a datasource package.
type DatasourcePackageIngestDetail struct {
	IngestState string
}

// MembershipDatasources holds datasource package info for a member or graph.
type MembershipDatasources struct {
	DatasourcePackageIngestStates map[string]string
	AccountID                     string
	GraphARN                      string
}

// OrgAdmin represents a Detective organization administrator account.
// DelegationTime is first so its non-pointer prefix reduces GC pointer bytes.
type OrgAdmin struct {
	DelegationTime time.Time
	AccountID      string
	GraphARN       string
}

var _ StorageBackend = (*InMemoryBackend)(nil)

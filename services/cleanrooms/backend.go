// Package cleanrooms implements an in-memory AWS Clean Rooms service backend.
package cleanrooms

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

const (
	statusActive    = "ACTIVE"
	errCodeNotFound = "ResourceNotFoundException"
	errMsgNotFound  = "not found"
)

// ---- types ----

type MemberSpec struct {
	PaymentConfig map[string]any `json:"paymentConfiguration,omitempty"`
	AccountID     string         `json:"accountId"`
	DisplayName   string         `json:"displayName"`
	Abilities     []string       `json:"memberAbilities"`
}

type MemberSummary struct {
	AccountID   string   `json:"accountId"`
	DisplayName string   `json:"displayName"`
	Status      string   `json:"status"`
	Abilities   []string `json:"abilities"`
	CreateTime  float64  `json:"createTime,omitempty"`
	UpdateTime  float64  `json:"updateTime,omitempty"`
}

type Collaboration struct {
	Tags                    map[string]string `json:"tags,omitempty"`
	CollaborationIdentifier string            `json:"collaborationIdentifier"`
	// ID mirrors CollaborationIdentifier under the canonical AWS API key ("id").
	// The AWS SDK/Terraform provider read this resource via the "id" field, so it
	// must be emitted in addition to the legacy "collaborationIdentifier" key.
	ID                 string           `json:"id"`
	Arn                string           `json:"arn"`
	Name               string           `json:"name"`
	Description        string           `json:"description,omitempty"`
	CreatorAccountID   string           `json:"creatorAccountId"`
	CreatorDisplayName string           `json:"creatorDisplayName"`
	MemberStatus       string           `json:"memberStatus"`
	QueryLogStatus     string           `json:"queryLogStatus,omitempty"`
	MemberAbilities    []string         `json:"memberAbilities,omitempty"`
	Members            []*MemberSummary `json:"members,omitempty"`
	CreateTime         float64          `json:"createTime,omitempty"`
	UpdateTime         float64          `json:"updateTime,omitempty"`
}

type CollaborationSummary struct {
	CollaborationIdentifier string `json:"collaborationIdentifier"`
	// ID mirrors CollaborationIdentifier under the canonical AWS API key ("id").
	ID                 string  `json:"id"`
	Arn                string  `json:"arn"`
	Name               string  `json:"name"`
	CreatorAccountID   string  `json:"creatorAccountId"`
	CreatorDisplayName string  `json:"creatorDisplayName"`
	MemberStatus       string  `json:"memberStatus"`
	CreateTime         float64 `json:"createTime,omitempty"`
	UpdateTime         float64 `json:"updateTime,omitempty"`
}

type Membership struct {
	DefaultResultConfiguration      map[string]any `json:"defaultResultConfiguration,omitempty"`
	PaymentConfiguration            map[string]any `json:"paymentConfiguration,omitempty"`
	QueryLogStatus                  string         `json:"queryLogStatus,omitempty"`
	CollaborationIdentifier         string         `json:"collaborationIdentifier"`
	CollaborationCreatorAccountID   string         `json:"collaborationCreatorAccountId"`
	CollaborationCreatorDisplayName string         `json:"collaborationCreatorDisplayName"`
	MembershipIdentifier            string         `json:"membershipIdentifier"`
	Status                          string         `json:"status"`
	CollaborationName               string         `json:"collaborationName"`
	CollaborationArn                string         `json:"collaborationArn"`
	Arn                             string         `json:"arn"`
	CollaborationID                 string         `json:"collaborationId"`
	ID                              string         `json:"id"`
	MemberAbilities                 []string       `json:"memberAbilities,omitempty"`
	UpdateTime                      float64        `json:"updateTime,omitempty"`
	CreateTime                      float64        `json:"createTime,omitempty"`
}

type MembershipSummary struct {
	CollaborationName               string   `json:"collaborationName"`
	Arn                             string   `json:"arn"`
	CollaborationIdentifier         string   `json:"collaborationIdentifier"`
	CollaborationArn                string   `json:"collaborationArn"`
	CollaborationCreatorAccountID   string   `json:"collaborationCreatorAccountId"`
	CollaborationCreatorDisplayName string   `json:"collaborationCreatorDisplayName"`
	MembershipIdentifier            string   `json:"membershipIdentifier"`
	Status                          string   `json:"status"`
	ID                              string   `json:"id"`
	CollaborationID                 string   `json:"collaborationId"`
	MemberAbilities                 []string `json:"memberAbilities,omitempty"`
	CreateTime                      float64  `json:"createTime,omitempty"`
	UpdateTime                      float64  `json:"updateTime,omitempty"`
}

type ConfiguredTable struct {
	TableReference            map[string]any    `json:"tableReference,omitempty"`
	Tags                      map[string]string `json:"tags,omitempty"`
	ConfiguredTableIdentifier string            `json:"configuredTableIdentifier"`
	Arn                       string            `json:"arn"`
	Name                      string            `json:"name"`
	Description               string            `json:"description,omitempty"`
	AnalysisMethod            string            `json:"analysisMethod,omitempty"`
	ID                        string            `json:"id"`
	AllowedColumns            []string          `json:"allowedColumns,omitempty"`
	AnalysisRuleTypes         []string          `json:"analysisRuleTypes,omitempty"`
	CreateTime                float64           `json:"createTime,omitempty"`
	UpdateTime                float64           `json:"updateTime,omitempty"`
}

type ConfiguredTableSummary struct {
	ConfiguredTableIdentifier string   `json:"configuredTableIdentifier"`
	Arn                       string   `json:"arn"`
	Name                      string   `json:"name"`
	AnalysisMethod            string   `json:"analysisMethod,omitempty"`
	ID                        string   `json:"id"`
	AnalysisRuleTypes         []string `json:"analysisRuleTypes,omitempty"`
	CreateTime                float64  `json:"createTime,omitempty"`
	UpdateTime                float64  `json:"updateTime,omitempty"`
}

type ConfiguredTableAnalysisRule struct {
	Policy                    map[string]any `json:"policy,omitempty"`
	ConfiguredTableIdentifier string         `json:"configuredTableIdentifier"`
	ConfiguredTableArn        string         `json:"configuredTableArn"`
	Type                      string         `json:"type"`
	ConfiguredTableID         string         `json:"configuredTableId"`
	CreateTime                float64        `json:"createTime,omitempty"`
	UpdateTime                float64        `json:"updateTime,omitempty"`
}

type ConfiguredTableAssociation struct {
	Tags                                 map[string]string `json:"tags,omitempty"`
	RoleArn                              string            `json:"roleArn,omitempty"`
	Name                                 string            `json:"name"`
	MembershipArn                        string            `json:"membershipArn"`
	ConfiguredTableIdentifier            string            `json:"configuredTableIdentifier"`
	ConfiguredTableArn                   string            `json:"configuredTableArn"`
	ConfiguredTableAssociationIdentifier string            `json:"configuredTableAssociationIdentifier"`
	MembershipIdentifier                 string            `json:"membershipIdentifier"`
	ConfiguredTableID                    string            `json:"configuredTableId"`
	Description                          string            `json:"description,omitempty"`
	MembershipID                         string            `json:"membershipId"`
	Arn                                  string            `json:"arn"`
	ID                                   string            `json:"id"`
	AnalysisRuleTypes                    []string          `json:"analysisRuleTypes,omitempty"`
	UpdateTime                           float64           `json:"updateTime,omitempty"`
	CreateTime                           float64           `json:"createTime,omitempty"`
}

type ConfiguredTableAssociationSummary struct {
	ConfiguredTableAssociationIdentifier string  `json:"configuredTableAssociationIdentifier"`
	Arn                                  string  `json:"arn"`
	MembershipIdentifier                 string  `json:"membershipIdentifier"`
	MembershipArn                        string  `json:"membershipArn"`
	ConfiguredTableIdentifier            string  `json:"configuredTableIdentifier"`
	Name                                 string  `json:"name"`
	ID                                   string  `json:"id"`
	MembershipID                         string  `json:"membershipId"`
	ConfiguredTableID                    string  `json:"configuredTableId"`
	CreateTime                           float64 `json:"createTime,omitempty"`
	UpdateTime                           float64 `json:"updateTime,omitempty"`
}

type ConfiguredTableAssociationAnalysisRule struct {
	Policy                               map[string]any `json:"policy,omitempty"`
	ConfiguredTableAssociationIdentifier string         `json:"configuredTableAssociationIdentifier"`
	ConfiguredTableAssociationArn        string         `json:"configuredTableAssociationArn"`
	MembershipIdentifier                 string         `json:"membershipIdentifier"`
	MembershipArn                        string         `json:"membershipArn"`
	Type                                 string         `json:"type"`
	CreateTime                           float64        `json:"createTime,omitempty"`
	UpdateTime                           float64        `json:"updateTime,omitempty"`
}

type AnalysisTemplate struct {
	Source                     map[string]any    `json:"source,omitempty"`
	Tags                       map[string]string `json:"tags,omitempty"`
	Schema                     map[string]any    `json:"schema,omitempty"`
	AnalysisTemplateIdentifier string            `json:"analysisTemplateIdentifier"`
	Format                     string            `json:"format,omitempty"`
	MembershipArn              string            `json:"membershipArn"`
	Name                       string            `json:"name"`
	Description                string            `json:"description,omitempty"`
	CollaborationIdentifier    string            `json:"collaborationIdentifier"`
	CollaborationArn           string            `json:"collaborationArn"`
	MembershipIdentifier       string            `json:"membershipIdentifier"`
	Arn                        string            `json:"arn"`
	CollaborationID            string            `json:"collaborationId"`
	MembershipID               string            `json:"membershipId"`
	ID                         string            `json:"id"`
	AnalysisParameters         []map[string]any  `json:"analysisParameters,omitempty"`
	UpdateTime                 float64           `json:"updateTime,omitempty"`
	CreateTime                 float64           `json:"createTime,omitempty"`
}

type AnalysisTemplateSummary struct {
	AnalysisTemplateIdentifier string  `json:"analysisTemplateIdentifier"`
	Arn                        string  `json:"arn"`
	CollaborationArn           string  `json:"collaborationArn"`
	CollaborationIdentifier    string  `json:"collaborationIdentifier"`
	MembershipIdentifier       string  `json:"membershipIdentifier"`
	MembershipArn              string  `json:"membershipArn"`
	Name                       string  `json:"name"`
	ID                         string  `json:"id"`
	MembershipID               string  `json:"membershipId"`
	CollaborationID            string  `json:"collaborationId"`
	CreateTime                 float64 `json:"createTime,omitempty"`
	UpdateTime                 float64 `json:"updateTime,omitempty"`
}

type BatchError struct {
	Arn     string `json:"arn,omitempty"`
	Name    string `json:"name,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Schema struct {
	CollaborationArn        string           `json:"collaborationArn"`
	CollaborationIdentifier string           `json:"collaborationIdentifier"`
	CreatorAccountID        string           `json:"creatorAccountId"`
	Name                    string           `json:"name"`
	Type                    string           `json:"type"`
	AnalysisMethod          string           `json:"analysisMethod,omitempty"`
	Columns                 []map[string]any `json:"columns,omitempty"`
	PartitionKeys           []map[string]any `json:"partitionKeys,omitempty"`
	AnalysisRuleTypes       []string         `json:"analysisRuleTypes,omitempty"`
	CreateTime              float64          `json:"createTime,omitempty"`
	UpdateTime              float64          `json:"updateTime,omitempty"`
}

type SchemaSummary struct {
	CollaborationArn        string   `json:"collaborationArn"`
	CollaborationIdentifier string   `json:"collaborationIdentifier"`
	CreatorAccountID        string   `json:"creatorAccountId"`
	Name                    string   `json:"name"`
	Type                    string   `json:"type"`
	AnalysisMethod          string   `json:"analysisMethod,omitempty"`
	AnalysisRuleTypes       []string `json:"analysisRuleTypes,omitempty"`
	CreateTime              float64  `json:"createTime,omitempty"`
	UpdateTime              float64  `json:"updateTime,omitempty"`
}

type SchemaAnalysisRule struct {
	Policy                  map[string]any `json:"policy,omitempty"`
	CollaborationArn        string         `json:"collaborationArn"`
	CollaborationIdentifier string         `json:"collaborationIdentifier"`
	Name                    string         `json:"name"`
	Type                    string         `json:"type"`
	CreateTime              float64        `json:"createTime,omitempty"`
	UpdateTime              float64        `json:"updateTime,omitempty"`
}

type ProtectedQuery struct {
	SQLParameters        map[string]any `json:"sqlParameters,omitempty"`
	ResultConfiguration  map[string]any `json:"resultConfiguration,omitempty"`
	ComputeConfiguration map[string]any `json:"computeConfiguration,omitempty"`
	Statistics           map[string]any `json:"statistics,omitempty"`
	Result               map[string]any `json:"result,omitempty"`
	Error                map[string]any `json:"error,omitempty"`
	ID                   string         `json:"id"`
	MembershipIdentifier string         `json:"membershipIdentifier"`
	MembershipArn        string         `json:"membershipArn"`
	Status               string         `json:"status"`
	MembershipID         string         `json:"membershipId"`
	CreateTime           float64        `json:"createTime,omitempty"`
}

type ProtectedQuerySummary struct {
	ID                   string  `json:"id"`
	MembershipIdentifier string  `json:"membershipIdentifier"`
	MembershipArn        string  `json:"membershipArn"`
	Status               string  `json:"status"`
	MembershipID         string  `json:"membershipId"`
	CreateTime           float64 `json:"createTime,omitempty"`
}

type ProtectedJob struct {
	JobParameters        map[string]any `json:"jobParameters,omitempty"`
	ResultConfiguration  map[string]any `json:"resultConfiguration,omitempty"`
	Statistics           map[string]any `json:"statistics,omitempty"`
	Result               map[string]any `json:"result,omitempty"`
	Error                map[string]any `json:"error,omitempty"`
	ID                   string         `json:"id"`
	MembershipIdentifier string         `json:"membershipIdentifier"`
	MembershipArn        string         `json:"membershipArn"`
	Status               string         `json:"status"`
	Type                 string         `json:"type"`
	MembershipID         string         `json:"membershipId"`
	CreateTime           float64        `json:"createTime,omitempty"`
}

type ProtectedJobSummary struct {
	ID                   string  `json:"id"`
	MembershipIdentifier string  `json:"membershipIdentifier"`
	MembershipArn        string  `json:"membershipArn"`
	Status               string  `json:"status"`
	Type                 string  `json:"type"`
	MembershipID         string  `json:"membershipId"`
	CreateTime           float64 `json:"createTime,omitempty"`
}

type PrivacyBudgetTemplate struct {
	Parameters                      map[string]any    `json:"parameters,omitempty"`
	Tags                            map[string]string `json:"tags,omitempty"`
	MembershipArn                   string            `json:"membershipArn"`
	Arn                             string            `json:"arn"`
	CollaborationArn                string            `json:"collaborationArn"`
	CollaborationIdentifier         string            `json:"collaborationIdentifier"`
	PrivacyBudgetTemplateIdentifier string            `json:"privacyBudgetTemplateIdentifier"`
	MembershipIdentifier            string            `json:"membershipIdentifier"`
	PrivacyBudgetType               string            `json:"privacyBudgetType"`
	AutoRefresh                     string            `json:"autoRefresh,omitempty"`
	ID                              string            `json:"id"`
	MembershipID                    string            `json:"membershipId"`
	CollaborationID                 string            `json:"collaborationId"`
	CreateTime                      float64           `json:"createTime,omitempty"`
	UpdateTime                      float64           `json:"updateTime,omitempty"`
}

type PrivacyBudgetTemplateSummary struct {
	PrivacyBudgetTemplateIdentifier string  `json:"privacyBudgetTemplateIdentifier"`
	Arn                             string  `json:"arn"`
	CollaborationArn                string  `json:"collaborationArn"`
	CollaborationIdentifier         string  `json:"collaborationIdentifier"`
	MembershipArn                   string  `json:"membershipArn"`
	MembershipIdentifier            string  `json:"membershipIdentifier"`
	PrivacyBudgetType               string  `json:"privacyBudgetType"`
	ID                              string  `json:"id"`
	MembershipID                    string  `json:"membershipId"`
	CollaborationID                 string  `json:"collaborationId"`
	CreateTime                      float64 `json:"createTime,omitempty"`
	UpdateTime                      float64 `json:"updateTime,omitempty"`
}

type PrivacyBudget struct {
	Budget                          map[string]any `json:"budget,omitempty"`
	ID                              string         `json:"id"`
	PrivacyBudgetTemplateArn        string         `json:"privacyBudgetTemplateArn"`
	PrivacyBudgetTemplateIdentifier string         `json:"privacyBudgetTemplateIdentifier"`
	CollaborationArn                string         `json:"collaborationArn"`
	CollaborationIdentifier         string         `json:"collaborationIdentifier"`
	MembershipArn                   string         `json:"membershipArn"`
	MembershipIdentifier            string         `json:"membershipIdentifier"`
	PrivacyBudgetType               string         `json:"privacyBudgetType"`
	MembershipID                    string         `json:"membershipId"`
	CollaborationID                 string         `json:"collaborationId"`
}

type IDMappingTable struct {
	InputReferenceConfig     map[string]any    `json:"inputReferenceConfig,omitempty"`
	Tags                     map[string]string `json:"tags,omitempty"`
	InputReferenceProperties map[string]any    `json:"inputReferenceProperties,omitempty"`
	IDMappingTableIdentifier string            `json:"idMappingTableIdentifier"`
	KmsKeyArn                string            `json:"kmsKeyArn,omitempty"`
	MembershipIdentifier     string            `json:"membershipIdentifier"`
	Name                     string            `json:"name"`
	Description              string            `json:"description,omitempty"`
	CollaborationIdentifier  string            `json:"collaborationIdentifier"`
	CollaborationArn         string            `json:"collaborationArn"`
	MembershipArn            string            `json:"membershipArn"`
	Arn                      string            `json:"arn"`
	CollaborationID          string            `json:"collaborationId"`
	MembershipID             string            `json:"membershipId"`
	ID                       string            `json:"id"`
	UpdateTime               float64           `json:"updateTime,omitempty"`
	CreateTime               float64           `json:"createTime,omitempty"`
}

type IDMappingTableSummary struct {
	IDMappingTableIdentifier string  `json:"idMappingTableIdentifier"`
	Arn                      string  `json:"arn"`
	CollaborationArn         string  `json:"collaborationArn"`
	CollaborationIdentifier  string  `json:"collaborationIdentifier"`
	MembershipArn            string  `json:"membershipArn"`
	MembershipIdentifier     string  `json:"membershipIdentifier"`
	Name                     string  `json:"name"`
	ID                       string  `json:"id"`
	MembershipID             string  `json:"membershipId"`
	CollaborationID          string  `json:"collaborationId"`
	CreateTime               float64 `json:"createTime,omitempty"`
	UpdateTime               float64 `json:"updateTime,omitempty"`
}

type IDNamespaceAssociation struct {
	InputReferenceConfig             map[string]any    `json:"inputReferenceConfig,omitempty"`
	Tags                             map[string]string `json:"tags,omitempty"`
	IDMappingConfig                  map[string]any    `json:"idMappingConfig,omitempty"`
	InputReferenceProperties         map[string]any    `json:"inputReferenceProperties,omitempty"`
	MembershipArn                    string            `json:"membershipArn"`
	MembershipIdentifier             string            `json:"membershipIdentifier"`
	Name                             string            `json:"name"`
	Description                      string            `json:"description,omitempty"`
	CollaborationIdentifier          string            `json:"collaborationIdentifier"`
	IDNamespaceAssociationIdentifier string            `json:"idNamespaceAssociationIdentifier"`
	CollaborationArn                 string            `json:"collaborationArn"`
	Arn                              string            `json:"arn"`
	ID                               string            `json:"id"`
	MembershipID                     string            `json:"membershipId"`
	CollaborationID                  string            `json:"collaborationId"`
	CreateTime                       float64           `json:"createTime,omitempty"`
	UpdateTime                       float64           `json:"updateTime,omitempty"`
}

type IDNamespaceAssociationSummary struct {
	IDNamespaceAssociationIdentifier string  `json:"idNamespaceAssociationIdentifier"`
	Arn                              string  `json:"arn"`
	CollaborationArn                 string  `json:"collaborationArn"`
	CollaborationIdentifier          string  `json:"collaborationIdentifier"`
	MembershipArn                    string  `json:"membershipArn"`
	MembershipIdentifier             string  `json:"membershipIdentifier"`
	Name                             string  `json:"name"`
	ID                               string  `json:"id"`
	MembershipID                     string  `json:"membershipId"`
	CollaborationID                  string  `json:"collaborationId"`
	CreateTime                       float64 `json:"createTime,omitempty"`
	UpdateTime                       float64 `json:"updateTime,omitempty"`
}

type ConfiguredAudienceModelAssociation struct {
	Tags                                         map[string]string `json:"tags,omitempty"`
	Description                                  string            `json:"description,omitempty"`
	ConfiguredAudienceModelArn                   string            `json:"configuredAudienceModelArn"`
	CollaborationIdentifier                      string            `json:"collaborationIdentifier"`
	MembershipArn                                string            `json:"membershipArn"`
	MembershipIdentifier                         string            `json:"membershipIdentifier"`
	ConfiguredAudienceModelAssociationIdentifier string            `json:"configuredAudienceModelAssociationIdentifier"`
	CollaborationArn                             string            `json:"collaborationArn"`
	CollaborationID                              string            `json:"collaborationId"`
	Name                                         string            `json:"name"`
	MembershipID                                 string            `json:"membershipId"`
	Arn                                          string            `json:"arn"`
	ID                                           string            `json:"id"`
	CreateTime                                   float64           `json:"createTime,omitempty"`
	UpdateTime                                   float64           `json:"updateTime,omitempty"`
	ManageResourcePolicies                       bool              `json:"manageResourcePolicies"`
}

type ConfiguredAudienceModelAssociationSummary struct {
	ConfiguredAudienceModelAssociationIdentifier string  `json:"configuredAudienceModelAssociationIdentifier"`
	Arn                                          string  `json:"arn"`
	CollaborationArn                             string  `json:"collaborationArn"`
	CollaborationIdentifier                      string  `json:"collaborationIdentifier"`
	MembershipArn                                string  `json:"membershipArn"`
	MembershipIdentifier                         string  `json:"membershipIdentifier"`
	Name                                         string  `json:"name"`
	ID                                           string  `json:"id"`
	MembershipID                                 string  `json:"membershipId"`
	CollaborationID                              string  `json:"collaborationId"`
	CreateTime                                   float64 `json:"createTime,omitempty"`
	UpdateTime                                   float64 `json:"updateTime,omitempty"`
}

type CollaborationChangeRequest struct {
	Details                 map[string]any `json:"details,omitempty"`
	ChangeRequestIdentifier string         `json:"changeRequestIdentifier"`
	CollaborationIdentifier string         `json:"collaborationIdentifier"`
	CollaborationArn        string         `json:"collaborationArn"`
	Status                  string         `json:"status"`
	Type                    string         `json:"type"`
	CreateTime              float64        `json:"createTime,omitempty"`
	UpdateTime              float64        `json:"updateTime,omitempty"`
}

// ---- InMemoryBackend ----

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// registry holds every converted resource table directly, including every
// composite-keyed one (see store_setup.go): each derives its store.Table key
// entirely from real, already-wire-visible identity field(s) already present
// on the value type (e.g. ConfiguredTableAnalysisRule carries both
// ConfiguredTableIdentifier and Type; AnalysisTemplate carries both
// MembershipIdentifier and its own ID), so plain registry.SnapshotAll/
// RestoreAll round-trips every table losslessly with no DTO wrapper needed
// anywhere -- unlike services/workmail or services/codeartifact, which each
// hide at least one unexported field behind a DTO. See persistence.go.
//
// tagsByArn is the one field left as a plain (non-store.Table) map: its
// values are map[string]string, not *T, so there is nothing for store.Table
// to key on. It is persisted directly (see persistence.go).
type InMemoryBackend struct {
	registry *store.Registry

	collaborations *store.Table[Collaboration]
	memberships    *store.Table[Membership]

	configuredTables *store.Table[ConfiguredTable]

	ctAnalysisRules        *store.Table[ConfiguredTableAnalysisRule]
	ctAnalysisRulesByTable *store.Index[ConfiguredTableAnalysisRule]

	ctAssociations             *store.Table[ConfiguredTableAssociation]
	ctAssociationsByMembership *store.Index[ConfiguredTableAssociation]

	ctaAnalysisRules              *store.Table[ConfiguredTableAssociationAnalysisRule]
	ctaAnalysisRulesByAssociation *store.Index[ConfiguredTableAssociationAnalysisRule]

	analysisTemplates             *store.Table[AnalysisTemplate]
	analysisTemplatesByMembership *store.Index[AnalysisTemplate]

	privacyBudgetTemplates             *store.Table[PrivacyBudgetTemplate]
	privacyBudgetTemplatesByMembership *store.Index[PrivacyBudgetTemplate]

	idMappingTables             *store.Table[IDMappingTable]
	idMappingTablesByMembership *store.Index[IDMappingTable]

	idNamespaceAssociations             *store.Table[IDNamespaceAssociation]
	idNamespaceAssociationsByMembership *store.Index[IDNamespaceAssociation]

	camaAssociations             *store.Table[ConfiguredAudienceModelAssociation]
	camaAssociationsByMembership *store.Index[ConfiguredAudienceModelAssociation]

	changeRequests                *store.Table[CollaborationChangeRequest]
	changeRequestsByCollaboration *store.Index[CollaborationChangeRequest]

	schemas                *store.Table[Schema]
	schemasByCollaboration *store.Index[Schema]

	schemaAnalysisRules *store.Table[SchemaAnalysisRule]

	protectedQueries             *store.Table[ProtectedQuery]
	protectedQueriesByMembership *store.Index[ProtectedQuery]

	protectedJobs             *store.Table[ProtectedJob]
	protectedJobsByMembership *store.Index[ProtectedJob]

	tagsByArn map[string]map[string]string

	nowFn func() float64
	mu    *lockmetrics.RWMutex

	accountID string
	region    string
	muNow     sync.Mutex
}

// NewInMemoryBackendWithContext creates a backend tied to svcCtx (ignored; no lifecycle goroutines).
func NewInMemoryBackendWithContext(_ context.Context, accountID, region string) *InMemoryBackend {
	return NewInMemoryBackend(accountID, region)
}

// NewInMemoryBackend creates a new in-memory Clean Rooms backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("cleanrooms"),
		accountID: accountID,
		region:    region,
		registry:  store.NewRegistry(),
		tagsByArn: make(map[string]map[string]string),
		nowFn:     func() float64 { return float64(time.Now().Unix()) },
	}
	registerAllTables(b)

	return b
}

func (b *InMemoryBackend) Region() string    { return b.region }
func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tagsByArn = make(map[string]map[string]string)
}

// ---- composite keys ----
//
// Every nested resource collection in this backend was previously a
// two-or-three-level map (outer key(s) = a parent identifier or two;
// innermost key = the resource's own identifier or type). Phase 3.3
// flattens each into a single *store.Table keyed by a composite "a|b[|c]"
// string, with a companion *store.Index (where a per-parent scan or cascade
// delete needs one) grouping entries by the parent -- see store_setup.go.
// Every value type already carries the real, wire-visible field(s) each key
// component below reads, so no hidden field or DTO wrapper is needed
// anywhere in this backend.
func membershipKey(membershipID, id string) string { return membershipID + "|" + id }

func collaborationKey(collaborationID, id string) string { return collaborationID + "|" + id }

func ctAnalysisRuleKey(configuredTableID, ruleType string) string {
	return configuredTableID + "|" + ruleType
}

func ctaAnalysisRuleKey(assocID, ruleType string) string { return assocID + "|" + ruleType }

func schemaAnalysisRuleKey(collaborationID, name, ruleType string) string {
	return collaborationKey(collaborationID, name) + "|" + ruleType
}

// ---- ARN helpers ----

func (b *InMemoryBackend) collaborationARN(id string) string {
	return arn.Build("cleanrooms", b.region, b.accountID, "collaboration/"+id)
}
func (b *InMemoryBackend) membershipARN(id string) string {
	return arn.Build("cleanrooms", b.region, b.accountID, "membership/"+id)
}
func (b *InMemoryBackend) configuredTableARN(id string) string {
	return arn.Build("cleanrooms", b.region, b.accountID, "configuredtable/"+id)
}
func (b *InMemoryBackend) ctAssociationARN(membershipID, assocID string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/configuredtableassociation/%s", membershipID, assocID),
	)
}
func (b *InMemoryBackend) analysisTemplateARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/analysistemplate/%s", membershipID, id),
	)
}
func (b *InMemoryBackend) privacyBudgetTemplateARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/privacybudgettemplate/%s", membershipID, id),
	)
}
func (b *InMemoryBackend) idMappingTableARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/idmappingtable/%s", membershipID, id),
	)
}
func (b *InMemoryBackend) idNamespaceAssocARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/idnamespaceassociation/%s", membershipID, id),
	)
}
func (b *InMemoryBackend) camaARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/configuredaudiencemodelassociation/%s", membershipID, id),
	)
}

// ---- pagination and listing helpers ----

// listItems ranges over a slice of items (typically the result of a
// store.Index.Get lookup), optionally skipping items where include returns
// false, converts each item to a summary, sorts by the less predicate, then
// paginates.
func listItems[T, S any](
	items []*T,
	include func(*T) bool,
	convert func(*T) *S,
	less func(a, b *S) bool,
	maxResults, nextToken string,
) ([]*S, string) {
	result := make([]*S, 0, len(items))
	for _, t := range items {
		if include != nil && !include(t) {
			continue
		}
		result = append(result, convert(t))
	}
	sort.Slice(result, func(i, j int) bool { return less(result[i], result[j]) })

	return paginate(result, maxResults, nextToken)
}

// listNestedItems ranges over a flat slice of items (typically the result of
// a store.Table.All() account-wide scan), collecting items that satisfy
// match, converts them to summaries, sorts, and paginates.
func listNestedItems[T, S any](
	allItems []*T,
	match func(*T) bool,
	convert func(*T) *S,
	less func(a, b *S) bool,
	maxResults, nextToken string,
) ([]*S, string) {
	var result []*S
	for _, t := range allItems {
		if match(t) {
			result = append(result, convert(t))
		}
	}
	sort.Slice(result, func(i, j int) bool { return less(result[i], result[j]) })

	return paginate(result, maxResults, nextToken)
}

func paginate[T any](items []T, maxResultsStr, nextToken string) ([]T, string) {
	if len(items) == 0 {
		return items, ""
	}
	pageSize := 100
	if maxResultsStr != "" {
		_, _ = fmt.Sscanf(maxResultsStr, "%d", &pageSize)
	}
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 100
	}
	start := 0
	if nextToken != "" {
		_, _ = fmt.Sscanf(nextToken, "%d", &start)
	}
	if start >= len(items) {
		return []T{}, ""
	}
	end := start + pageSize
	if end >= len(items) {
		return items[start:], ""
	}

	return items[start:end], strconv.Itoa(end)
}

func toAnalysisTemplateSummary(t *AnalysisTemplate) *AnalysisTemplateSummary {
	return &AnalysisTemplateSummary{
		AnalysisTemplateIdentifier: t.AnalysisTemplateIdentifier,
		Arn:                        t.Arn,
		CollaborationArn:           t.CollaborationArn,
		CollaborationIdentifier:    t.CollaborationIdentifier,
		MembershipIdentifier:       t.MembershipIdentifier,
		MembershipArn:              t.MembershipArn,
		Name:                       t.Name,
		CreateTime:                 t.CreateTime,
		UpdateTime:                 t.UpdateTime,
		ID:                         t.AnalysisTemplateIdentifier,
		MembershipID:               t.MembershipIdentifier,
		CollaborationID:            t.CollaborationIdentifier,
	}
}

func toIDMappingTableSummary(t *IDMappingTable) *IDMappingTableSummary {
	return &IDMappingTableSummary{
		IDMappingTableIdentifier: t.IDMappingTableIdentifier,
		Arn:                      t.Arn,
		CollaborationArn:         t.CollaborationArn,
		CollaborationIdentifier:  t.CollaborationIdentifier,
		MembershipArn:            t.MembershipArn,
		MembershipIdentifier:     t.MembershipIdentifier,
		Name:                     t.Name,
		CreateTime:               t.CreateTime,
		UpdateTime:               t.UpdateTime,
		ID:                       t.IDMappingTableIdentifier,
		MembershipID:             t.MembershipIdentifier,
		CollaborationID:          t.CollaborationIdentifier,
	}
}

func toPrivacyBudgetTemplateSummary(t *PrivacyBudgetTemplate) *PrivacyBudgetTemplateSummary {
	return &PrivacyBudgetTemplateSummary{
		PrivacyBudgetTemplateIdentifier: t.PrivacyBudgetTemplateIdentifier,
		Arn:                             t.Arn,
		CollaborationArn:                t.CollaborationArn,
		CollaborationIdentifier:         t.CollaborationIdentifier,
		MembershipArn:                   t.MembershipArn,
		MembershipIdentifier:            t.MembershipIdentifier,
		PrivacyBudgetType:               t.PrivacyBudgetType,
		CreateTime:                      t.CreateTime,
		UpdateTime:                      t.UpdateTime,
		ID:                              t.PrivacyBudgetTemplateIdentifier,
		MembershipID:                    t.MembershipIdentifier,
		CollaborationID:                 t.CollaborationIdentifier,
	}
}

func toSchemaSummary(s *Schema) *SchemaSummary {
	return &SchemaSummary{
		CollaborationArn:        s.CollaborationArn,
		CollaborationIdentifier: s.CollaborationIdentifier,
		CreatorAccountID:        s.CreatorAccountID,
		Name:                    s.Name,
		Type:                    s.Type,
		AnalysisRuleTypes:       s.AnalysisRuleTypes,
		AnalysisMethod:          s.AnalysisMethod,
		CreateTime:              s.CreateTime,
		UpdateTime:              s.UpdateTime,
	}
}

func toIDNamespaceAssociationSummary(a *IDNamespaceAssociation) *IDNamespaceAssociationSummary {
	return &IDNamespaceAssociationSummary{
		IDNamespaceAssociationIdentifier: a.IDNamespaceAssociationIdentifier,
		Arn:                              a.Arn,
		CollaborationArn:                 a.CollaborationArn,
		CollaborationIdentifier:          a.CollaborationIdentifier,
		MembershipArn:                    a.MembershipArn,
		MembershipIdentifier:             a.MembershipIdentifier,
		Name:                             a.Name,
		CreateTime:                       a.CreateTime,
		UpdateTime:                       a.UpdateTime,
		ID:                               a.IDNamespaceAssociationIdentifier,
		MembershipID:                     a.MembershipIdentifier,
		CollaborationID:                  a.CollaborationIdentifier,
	}
}

func toConfiguredAudienceModelAssociationSummary(
	a *ConfiguredAudienceModelAssociation,
) *ConfiguredAudienceModelAssociationSummary {
	return &ConfiguredAudienceModelAssociationSummary{
		ConfiguredAudienceModelAssociationIdentifier: a.ConfiguredAudienceModelAssociationIdentifier,
		Arn:                     a.Arn,
		CollaborationArn:        a.CollaborationArn,
		CollaborationIdentifier: a.CollaborationIdentifier,
		MembershipArn:           a.MembershipArn,
		MembershipIdentifier:    a.MembershipIdentifier,
		Name:                    a.Name,
		CreateTime:              a.CreateTime,
		UpdateTime:              a.UpdateTime,
		ID:                      a.ConfiguredAudienceModelAssociationIdentifier,
		MembershipID:            a.MembershipIdentifier,
		CollaborationID:         a.CollaborationIdentifier,
	}
}

// ---- now helper ----

func (b *InMemoryBackend) now() float64 {
	b.muNow.Lock()
	defer b.muNow.Unlock()

	return b.nowFn()
}

// ---- Collaboration ----

func (b *InMemoryBackend) CreateCollaboration(
	name, description, creatorDisplayName string,
	creatorMemberAbilities []string,
	members []MemberSpec,
	queryLogStatus string,
	tags map[string]string,
) (*Collaboration, error) {
	b.mu.Lock("CreateCollaboration")
	defer b.mu.Unlock()
	if name == "" {
		return nil, ErrValidation
	}
	id := uuid.NewString()
	ts := b.now()
	memberSummaries := make([]*MemberSummary, 0, len(members)+1)
	memberSummaries = append(memberSummaries, &MemberSummary{
		AccountID:   b.accountID,
		DisplayName: creatorDisplayName,
		Abilities:   creatorMemberAbilities,
		Status:      statusActive,
		CreateTime:  ts,
		UpdateTime:  ts,
	})
	for _, m := range members {
		memberSummaries = append(memberSummaries, &MemberSummary{
			AccountID:   m.AccountID,
			DisplayName: m.DisplayName,
			Abilities:   m.Abilities,
			Status:      "INVITED",
			CreateTime:  ts,
			UpdateTime:  ts,
		})
	}
	collab := &Collaboration{
		CollaborationIdentifier: id,
		ID:                      id,
		Arn:                     b.collaborationARN(id),
		Name:                    name,
		Description:             description,
		CreatorAccountID:        b.accountID,
		CreatorDisplayName:      creatorDisplayName,
		MemberStatus:            statusActive,
		MemberAbilities:         creatorMemberAbilities,
		Members:                 memberSummaries,
		QueryLogStatus:          queryLogStatus,
		CreateTime:              ts,
		UpdateTime:              ts,
		Tags:                    tags,
	}
	b.collaborations.Put(collab)
	if len(tags) > 0 {
		b.tagsByArn[collab.Arn] = maps.Clone(tags)
	}

	return collab, nil
}

func (b *InMemoryBackend) GetCollaboration(id string) (*Collaboration, error) {
	b.mu.RLock("GetCollaboration")
	defer b.mu.RUnlock()
	c, ok := b.collaborations.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return c, nil
}

func (b *InMemoryBackend) ListCollaborations(
	_, maxResults, nextToken string,
) ([]*CollaborationSummary, string) {
	b.mu.RLock("ListCollaborations")
	defer b.mu.RUnlock()
	all := b.collaborations.All()
	items := make([]*CollaborationSummary, 0, len(all))
	for _, c := range all {
		items = append(items, &CollaborationSummary{
			CollaborationIdentifier: c.CollaborationIdentifier,
			ID:                      c.CollaborationIdentifier,
			Arn:                     c.Arn,
			Name:                    c.Name,
			CreatorAccountID:        c.CreatorAccountID,
			CreatorDisplayName:      c.CreatorDisplayName,
			MemberStatus:            statusActive,
			CreateTime:              c.CreateTime,
			UpdateTime:              c.UpdateTime,
		})
	}
	sort.Slice(
		items,
		func(i, j int) bool { return items[i].CollaborationIdentifier < items[j].CollaborationIdentifier },
	)
	page, next := paginate(items, maxResults, nextToken)

	return page, next
}

func (b *InMemoryBackend) UpdateCollaboration(
	id, name, description string,
) (*Collaboration, error) {
	b.mu.Lock("UpdateCollaboration")
	defer b.mu.Unlock()
	c, ok := b.collaborations.Get(id)
	if !ok {
		return nil, ErrNotFound
	}
	if name != "" {
		c.Name = name
	}
	if description != "" {
		c.Description = description
	}
	c.UpdateTime = b.now()

	return c, nil
}

func (b *InMemoryBackend) DeleteCollaboration(id string) error {
	b.mu.Lock("DeleteCollaboration")
	defer b.mu.Unlock()
	c, ok := b.collaborations.Get(id)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, c.Arn)
	b.collaborations.Delete(id)

	return nil
}

func (b *InMemoryBackend) ListMembers(
	collaborationID string,
	maxResults, nextToken string,
) ([]*MemberSummary, string, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()
	c, ok := b.collaborations.Get(collaborationID)
	if !ok {
		return nil, "", ErrNotFound
	}
	members := make([]*MemberSummary, len(c.Members))
	copy(members, c.Members)
	page, next := paginate(members, maxResults, nextToken)

	return page, next, nil
}

func (b *InMemoryBackend) DeleteMember(collaborationID, accountID string) error {
	b.mu.Lock("DeleteMember")
	defer b.mu.Unlock()
	c, ok := b.collaborations.Get(collaborationID)
	if !ok {
		return ErrNotFound
	}
	for i, m := range c.Members {
		if m.AccountID == accountID {
			c.Members = append(c.Members[:i], c.Members[i+1:]...)

			return nil
		}
	}

	return ErrNotFound
}

// ---- Membership ----

func (b *InMemoryBackend) CreateMembership(
	collaborationID, queryLogStatus string,
	memberAbilities []string,
	defaultResultConfiguration map[string]any,
	paymentConfiguration map[string]any,
	tags map[string]string,
) (*Membership, error) {
	b.mu.Lock("CreateMembership")
	defer b.mu.Unlock()
	if collaborationID == "" {
		return nil, ErrValidation
	}
	collab, ok := b.collaborations.Get(collaborationID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	m := &Membership{
		MembershipIdentifier:            id,
		Arn:                             b.membershipARN(id),
		CollaborationIdentifier:         collaborationID,
		CollaborationArn:                collab.Arn,
		CollaborationCreatorAccountID:   collab.CreatorAccountID,
		CollaborationCreatorDisplayName: collab.CreatorDisplayName,
		CollaborationName:               collab.Name,
		Status:                          statusActive,
		QueryLogStatus:                  queryLogStatus,
		MemberAbilities:                 memberAbilities,
		DefaultResultConfiguration:      defaultResultConfiguration,
		PaymentConfiguration:            paymentConfiguration,
		CreateTime:                      ts,
		UpdateTime:                      ts,
		ID:                              id,
		CollaborationID:                 collaborationID,
	}
	b.memberships.Put(m)
	if len(tags) > 0 {
		b.tagsByArn[m.Arn] = maps.Clone(tags)
	}

	return m, nil
}

func (b *InMemoryBackend) GetMembership(id string) (*Membership, error) {
	b.mu.RLock("GetMembership")
	defer b.mu.RUnlock()
	m, ok := b.memberships.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return m, nil
}

func (b *InMemoryBackend) ListMemberships(
	status, maxResults, nextToken string,
) ([]*MembershipSummary, string) {
	b.mu.RLock("ListMemberships")
	defer b.mu.RUnlock()
	var items []*MembershipSummary
	for _, m := range b.memberships.All() {
		if status != "" && m.Status != status {
			continue
		}
		items = append(items, &MembershipSummary{
			MembershipIdentifier:            m.MembershipIdentifier,
			Arn:                             m.Arn,
			CollaborationIdentifier:         m.CollaborationIdentifier,
			CollaborationArn:                m.CollaborationArn,
			CollaborationCreatorAccountID:   m.CollaborationCreatorAccountID,
			CollaborationCreatorDisplayName: m.CollaborationCreatorDisplayName,
			CollaborationName:               m.CollaborationName,
			Status:                          m.Status,
			MemberAbilities:                 m.MemberAbilities,
			CreateTime:                      m.CreateTime,
			UpdateTime:                      m.UpdateTime,
			ID:                              m.MembershipIdentifier,
			CollaborationID:                 m.CollaborationIdentifier,
		})
	}
	sort.Slice(
		items,
		func(i, j int) bool { return items[i].MembershipIdentifier < items[j].MembershipIdentifier },
	)
	page, next := paginate(items, maxResults, nextToken)

	return page, next
}

func (b *InMemoryBackend) UpdateMembership(
	id, queryLogStatus string,
	defaultResultConfiguration map[string]any,
) (*Membership, error) {
	b.mu.Lock("UpdateMembership")
	defer b.mu.Unlock()
	m, ok := b.memberships.Get(id)
	if !ok {
		return nil, ErrNotFound
	}
	if queryLogStatus != "" {
		m.QueryLogStatus = queryLogStatus
	}
	if defaultResultConfiguration != nil {
		m.DefaultResultConfiguration = defaultResultConfiguration
	}
	m.UpdateTime = b.now()

	return m, nil
}

func (b *InMemoryBackend) DeleteMembership(id string) error {
	b.mu.Lock("DeleteMembership")
	defer b.mu.Unlock()
	m, ok := b.memberships.Get(id)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, m.Arn)
	b.memberships.Delete(id)

	return nil
}

// ---- ConfiguredTable ----

func (b *InMemoryBackend) CreateConfiguredTable(
	name, description string,
	tableReference map[string]any,
	allowedColumns []string,
	analysisMethod string,
	tags map[string]string,
) (*ConfiguredTable, error) {
	b.mu.Lock("CreateConfiguredTable")
	defer b.mu.Unlock()
	if name == "" {
		return nil, ErrValidation
	}
	id := uuid.NewString()
	ts := b.now()
	ct := &ConfiguredTable{
		ConfiguredTableIdentifier: id,
		Arn:                       b.configuredTableARN(id),
		Name:                      name,
		Description:               description,
		TableReference:            tableReference,
		AllowedColumns:            allowedColumns,
		AnalysisMethod:            analysisMethod,
		CreateTime:                ts,
		UpdateTime:                ts,
		Tags:                      tags,
		ID:                        id,
	}
	b.configuredTables.Put(ct)
	if len(tags) > 0 {
		b.tagsByArn[ct.Arn] = maps.Clone(tags)
	}

	return ct, nil
}

func (b *InMemoryBackend) GetConfiguredTable(id string) (*ConfiguredTable, error) {
	b.mu.RLock("GetConfiguredTable")
	defer b.mu.RUnlock()
	ct, ok := b.configuredTables.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return ct, nil
}

func (b *InMemoryBackend) ListConfiguredTables(
	maxResults, nextToken string,
) ([]*ConfiguredTableSummary, string) {
	b.mu.RLock("ListConfiguredTables")
	defer b.mu.RUnlock()
	all := b.configuredTables.All()
	items := make([]*ConfiguredTableSummary, 0, len(all))
	for _, ct := range all {
		items = append(items, &ConfiguredTableSummary{
			ConfiguredTableIdentifier: ct.ConfiguredTableIdentifier,
			Arn:                       ct.Arn,
			Name:                      ct.Name,
			AnalysisMethod:            ct.AnalysisMethod,
			AnalysisRuleTypes:         ct.AnalysisRuleTypes,
			CreateTime:                ct.CreateTime,
			UpdateTime:                ct.UpdateTime,
			ID:                        ct.ConfiguredTableIdentifier,
		})
	}
	sort.Slice(
		items,
		func(i, j int) bool { return items[i].ConfiguredTableIdentifier < items[j].ConfiguredTableIdentifier },
	)
	page, next := paginate(items, maxResults, nextToken)

	return page, next
}

func (b *InMemoryBackend) UpdateConfiguredTable(
	id, name, description string,
) (*ConfiguredTable, error) {
	b.mu.Lock("UpdateConfiguredTable")
	defer b.mu.Unlock()
	ct, ok := b.configuredTables.Get(id)
	if !ok {
		return nil, ErrNotFound
	}
	if name != "" {
		ct.Name = name
	}
	if description != "" {
		ct.Description = description
	}
	ct.UpdateTime = b.now()

	return ct, nil
}

func (b *InMemoryBackend) DeleteConfiguredTable(id string) error {
	b.mu.Lock("DeleteConfiguredTable")
	defer b.mu.Unlock()
	ct, ok := b.configuredTables.Get(id)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, ct.Arn)
	b.configuredTables.Delete(id)

	for _, rule := range slices.Clone(b.ctAnalysisRulesByTable.Get(id)) {
		b.ctAnalysisRules.Delete(ctAnalysisRuleKey(rule.ConfiguredTableIdentifier, rule.Type))
	}

	return nil
}

// ---- ConfiguredTableAnalysisRule ----

func (b *InMemoryBackend) CreateConfiguredTableAnalysisRule(
	configuredTableID, analysisRuleType string,
	policy map[string]any,
) (*ConfiguredTableAnalysisRule, error) {
	b.mu.Lock("CreateConfiguredTableAnalysisRule")
	defer b.mu.Unlock()
	ct, ok := b.configuredTables.Get(configuredTableID)
	if !ok {
		return nil, ErrNotFound
	}
	if b.ctAnalysisRules.Has(ctAnalysisRuleKey(configuredTableID, analysisRuleType)) {
		return nil, ErrAlreadyExists
	}
	ts := b.now()
	rule := &ConfiguredTableAnalysisRule{
		ConfiguredTableIdentifier: configuredTableID,
		ConfiguredTableArn:        ct.Arn,
		Type:                      analysisRuleType,
		Policy:                    policy,
		CreateTime:                ts,
		UpdateTime:                ts,
		ConfiguredTableID:         configuredTableID,
	}
	b.ctAnalysisRules.Put(rule)
	if !contains(ct.AnalysisRuleTypes, analysisRuleType) {
		ct.AnalysisRuleTypes = append(ct.AnalysisRuleTypes, analysisRuleType)
	}

	return rule, nil
}

func (b *InMemoryBackend) GetConfiguredTableAnalysisRule(
	configuredTableID, analysisRuleType string,
) (*ConfiguredTableAnalysisRule, error) {
	b.mu.RLock("GetConfiguredTableAnalysisRule")
	defer b.mu.RUnlock()
	rule, ok := b.ctAnalysisRules.Get(ctAnalysisRuleKey(configuredTableID, analysisRuleType))
	if !ok {
		return nil, ErrNotFound
	}

	return rule, nil
}

func (b *InMemoryBackend) UpdateConfiguredTableAnalysisRule(
	configuredTableID, analysisRuleType string,
	policy map[string]any,
) (*ConfiguredTableAnalysisRule, error) {
	b.mu.Lock("UpdateConfiguredTableAnalysisRule")
	defer b.mu.Unlock()
	rule, ok := b.ctAnalysisRules.Get(ctAnalysisRuleKey(configuredTableID, analysisRuleType))
	if !ok {
		return nil, ErrNotFound
	}
	rule.Policy = policy
	rule.UpdateTime = b.now()

	return rule, nil
}

func (b *InMemoryBackend) DeleteConfiguredTableAnalysisRule(
	configuredTableID, analysisRuleType string,
) error {
	b.mu.Lock("DeleteConfiguredTableAnalysisRule")
	defer b.mu.Unlock()
	if !b.ctAnalysisRules.Delete(ctAnalysisRuleKey(configuredTableID, analysisRuleType)) {
		return ErrNotFound
	}
	if ct, ctOK := b.configuredTables.Get(configuredTableID); ctOK {
		ct.AnalysisRuleTypes = removeFrom(ct.AnalysisRuleTypes, analysisRuleType)
	}

	return nil
}

// ---- ConfiguredTableAssociation ----

func (b *InMemoryBackend) CreateConfiguredTableAssociation(
	membershipID, name, description, configuredTableID, roleArn string,
	tags map[string]string,
) (*ConfiguredTableAssociation, error) {
	b.mu.Lock("CreateConfiguredTableAssociation")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	ct, ok := b.configuredTables.Get(configuredTableID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	assoc := &ConfiguredTableAssociation{
		ConfiguredTableAssociationIdentifier: id,
		Arn:                                  b.ctAssociationARN(membershipID, id),
		MembershipIdentifier:                 membershipID,
		MembershipArn:                        mem.Arn,
		ConfiguredTableIdentifier:            configuredTableID,
		ConfiguredTableArn:                   ct.Arn,
		Name:                                 name,
		Description:                          description,
		RoleArn:                              roleArn,
		CreateTime:                           ts,
		UpdateTime:                           ts,
		Tags:                                 tags,
		ID:                                   id,
		MembershipID:                         membershipID,
		ConfiguredTableID:                    configuredTableID,
	}
	b.ctAssociations.Put(assoc)
	if len(tags) > 0 {
		b.tagsByArn[assoc.Arn] = maps.Clone(tags)
	}

	return assoc, nil
}

func (b *InMemoryBackend) GetConfiguredTableAssociation(
	membershipID, assocID string,
) (*ConfiguredTableAssociation, error) {
	b.mu.RLock("GetConfiguredTableAssociation")
	defer b.mu.RUnlock()
	assoc, ok := b.ctAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}

	return assoc, nil
}

func (b *InMemoryBackend) ListConfiguredTableAssociations(
	membershipID, maxResults, nextToken string,
) ([]*ConfiguredTableAssociationSummary, string, error) {
	b.mu.RLock("ListConfiguredTableAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	var items []*ConfiguredTableAssociationSummary
	for _, a := range b.ctAssociationsByMembership.Get(membershipID) {
		items = append(items, &ConfiguredTableAssociationSummary{
			ConfiguredTableAssociationIdentifier: a.ConfiguredTableAssociationIdentifier,
			Arn:                                  a.Arn,
			MembershipIdentifier:                 a.MembershipIdentifier,
			MembershipArn:                        a.MembershipArn,
			ConfiguredTableIdentifier:            a.ConfiguredTableIdentifier,
			Name:                                 a.Name,
			CreateTime:                           a.CreateTime,
			UpdateTime:                           a.UpdateTime,
			ID:                                   a.ConfiguredTableAssociationIdentifier,
			MembershipID:                         a.MembershipIdentifier,
			ConfiguredTableID:                    a.ConfiguredTableIdentifier,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].ConfiguredTableAssociationIdentifier < items[j].ConfiguredTableAssociationIdentifier
	})
	page, next := paginate(items, maxResults, nextToken)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateConfiguredTableAssociation(
	membershipID, assocID, description, roleArn string,
) (*ConfiguredTableAssociation, error) {
	b.mu.Lock("UpdateConfiguredTableAssociation")
	defer b.mu.Unlock()
	assoc, ok := b.ctAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}
	if description != "" {
		assoc.Description = description
	}
	if roleArn != "" {
		assoc.RoleArn = roleArn
	}
	assoc.UpdateTime = b.now()

	return assoc, nil
}

func (b *InMemoryBackend) DeleteConfiguredTableAssociation(membershipID, assocID string) error {
	b.mu.Lock("DeleteConfiguredTableAssociation")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, assocID)
	assoc, ok := b.ctAssociations.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, assoc.Arn)
	b.ctAssociations.Delete(key)

	for _, rule := range slices.Clone(b.ctaAnalysisRulesByAssociation.Get(assocID)) {
		b.ctaAnalysisRules.Delete(ctaAnalysisRuleKey(rule.ConfiguredTableAssociationIdentifier, rule.Type))
	}

	return nil
}

// ---- ConfiguredTableAssociationAnalysisRule ----

func (b *InMemoryBackend) CreateConfiguredTableAssociationAnalysisRule(
	membershipID, assocID, ruleType string,
	policy map[string]any,
) (*ConfiguredTableAssociationAnalysisRule, error) {
	b.mu.Lock("CreateConfiguredTableAssociationAnalysisRule")
	defer b.mu.Unlock()
	assoc, ok := b.ctAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}
	if b.ctaAnalysisRules.Has(ctaAnalysisRuleKey(assocID, ruleType)) {
		return nil, ErrAlreadyExists
	}
	mem, _ := b.memberships.Get(membershipID)
	ts := b.now()
	rule := &ConfiguredTableAssociationAnalysisRule{
		ConfiguredTableAssociationIdentifier: assocID,
		ConfiguredTableAssociationArn:        assoc.Arn,
		MembershipIdentifier:                 membershipID,
		MembershipArn:                        mem.Arn,
		Type:                                 ruleType,
		Policy:                               policy,
		CreateTime:                           ts,
		UpdateTime:                           ts,
	}
	b.ctaAnalysisRules.Put(rule)
	if !contains(assoc.AnalysisRuleTypes, ruleType) {
		assoc.AnalysisRuleTypes = append(assoc.AnalysisRuleTypes, ruleType)
	}

	return rule, nil
}

func (b *InMemoryBackend) GetConfiguredTableAssociationAnalysisRule(
	_, assocID, ruleType string,
) (*ConfiguredTableAssociationAnalysisRule, error) {
	b.mu.RLock("GetConfiguredTableAssociationAnalysisRule")
	defer b.mu.RUnlock()
	rule, ok := b.ctaAnalysisRules.Get(ctaAnalysisRuleKey(assocID, ruleType))
	if !ok {
		return nil, ErrNotFound
	}

	return rule, nil
}

func (b *InMemoryBackend) UpdateConfiguredTableAssociationAnalysisRule(
	_, assocID, ruleType string,
	policy map[string]any,
) (*ConfiguredTableAssociationAnalysisRule, error) {
	b.mu.Lock("UpdateConfiguredTableAssociationAnalysisRule")
	defer b.mu.Unlock()
	rule, ok := b.ctaAnalysisRules.Get(ctaAnalysisRuleKey(assocID, ruleType))
	if !ok {
		return nil, ErrNotFound
	}
	rule.Policy = policy
	rule.UpdateTime = b.now()

	return rule, nil
}

func (b *InMemoryBackend) DeleteConfiguredTableAssociationAnalysisRule(
	membershipID, assocID, ruleType string,
) error {
	b.mu.Lock("DeleteConfiguredTableAssociationAnalysisRule")
	defer b.mu.Unlock()
	if !b.ctaAnalysisRules.Delete(ctaAnalysisRuleKey(assocID, ruleType)) {
		return ErrNotFound
	}
	if assoc, assocOK := b.ctAssociations.Get(membershipKey(membershipID, assocID)); assocOK {
		assoc.AnalysisRuleTypes = removeFrom(assoc.AnalysisRuleTypes, ruleType)
	}

	return nil
}

// ---- AnalysisTemplate ----

func (b *InMemoryBackend) CreateAnalysisTemplate(
	membershipID, name, description, format string,
	source map[string]any,
	analysisParameters []map[string]any,
	tags map[string]string,
) (*AnalysisTemplate, error) {
	b.mu.Lock("CreateAnalysisTemplate")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationIdentifier)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	tmpl := &AnalysisTemplate{
		AnalysisTemplateIdentifier: id,
		Arn:                        b.analysisTemplateARN(membershipID, id),
		CollaborationArn:           collabArn,
		CollaborationIdentifier:    mem.CollaborationIdentifier,
		MembershipIdentifier:       membershipID,
		MembershipArn:              mem.Arn,
		Name:                       name,
		Description:                description,
		Format:                     format,
		Source:                     source,
		AnalysisParameters:         analysisParameters,
		CreateTime:                 ts,
		UpdateTime:                 ts,
		Tags:                       tags,
		ID:                         id,
		MembershipID:               membershipID,
		CollaborationID:            mem.CollaborationIdentifier,
	}
	b.analysisTemplates.Put(tmpl)
	if len(tags) > 0 {
		b.tagsByArn[tmpl.Arn] = maps.Clone(tags)
	}

	return tmpl, nil
}

func (b *InMemoryBackend) GetAnalysisTemplate(
	membershipID, templateID string,
) (*AnalysisTemplate, error) {
	b.mu.RLock("GetAnalysisTemplate")
	defer b.mu.RUnlock()
	tmpl, ok := b.analysisTemplates.Get(membershipKey(membershipID, templateID))
	if !ok {
		return nil, ErrNotFound
	}

	return tmpl, nil
}

func (b *InMemoryBackend) ListAnalysisTemplates(
	membershipID, maxResults, nextToken string,
) ([]*AnalysisTemplateSummary, string, error) {
	b.mu.RLock("ListAnalysisTemplates")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.analysisTemplatesByMembership.Get(membershipID),
		nil,
		toAnalysisTemplateSummary,
		func(a, c *AnalysisTemplateSummary) bool {
			return a.AnalysisTemplateIdentifier < c.AnalysisTemplateIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateAnalysisTemplate(
	membershipID, templateID, description string,
) (*AnalysisTemplate, error) {
	b.mu.Lock("UpdateAnalysisTemplate")
	defer b.mu.Unlock()
	tmpl, ok := b.analysisTemplates.Get(membershipKey(membershipID, templateID))
	if !ok {
		return nil, ErrNotFound
	}
	tmpl.Description = description
	tmpl.UpdateTime = b.now()

	return tmpl, nil
}

func (b *InMemoryBackend) DeleteAnalysisTemplate(membershipID, templateID string) error {
	b.mu.Lock("DeleteAnalysisTemplate")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, templateID)
	tmpl, ok := b.analysisTemplates.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, tmpl.Arn)
	b.analysisTemplates.Delete(key)

	return nil
}

func (b *InMemoryBackend) GetCollaborationAnalysisTemplate(
	collaborationID, templateArn string,
) (*AnalysisTemplate, error) {
	b.mu.RLock("GetCollaborationAnalysisTemplate")
	defer b.mu.RUnlock()
	var found *AnalysisTemplate
	b.analysisTemplates.Range(func(t *AnalysisTemplate) bool {
		if t.CollaborationIdentifier == collaborationID && t.Arn == templateArn {
			found = t

			return false
		}

		return true
	})
	if found == nil {
		return nil, ErrNotFound
	}

	return found, nil
}

func (b *InMemoryBackend) ListCollaborationAnalysisTemplates(
	collaborationID, maxResults, nextToken string,
) ([]*AnalysisTemplateSummary, string, error) {
	b.mu.RLock("ListCollaborationAnalysisTemplates")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listNestedItems(
		b.analysisTemplates.All(),
		func(t *AnalysisTemplate) bool { return t.CollaborationIdentifier == collaborationID },
		toAnalysisTemplateSummary,
		func(a, c *AnalysisTemplateSummary) bool {
			return a.AnalysisTemplateIdentifier < c.AnalysisTemplateIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) BatchGetCollaborationAnalysisTemplate(
	collaborationID string,
	templateArns []string,
) ([]*AnalysisTemplate, []BatchError, error) {
	b.mu.RLock("BatchGetCollaborationAnalysisTemplate")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, nil, ErrNotFound
	}
	all := b.analysisTemplates.All()
	var results []*AnalysisTemplate
	var errors []BatchError
	for _, arnStr := range templateArns {
		found := false
		for _, t := range all {
			if t.CollaborationIdentifier == collaborationID && t.Arn == arnStr {
				results = append(results, t)
				found = true

				break
			}
		}
		if !found {
			errors = append(
				errors,
				BatchError{Arn: arnStr, Code: errCodeNotFound, Message: errMsgNotFound},
			)
		}
	}

	return results, errors, nil
}

// ---- Schema ----

func (b *InMemoryBackend) GetSchema(collaborationID, name string) (*Schema, error) {
	b.mu.RLock("GetSchema")
	defer b.mu.RUnlock()
	s, ok := b.schemas.Get(collaborationKey(collaborationID, name))
	if !ok {
		return nil, ErrNotFound
	}

	return s, nil
}

func (b *InMemoryBackend) ListSchemas(
	collaborationID, schemaType, maxResults, nextToken string,
) ([]*SchemaSummary, string, error) {
	b.mu.RLock("ListSchemas")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.schemasByCollaboration.Get(collaborationID),
		func(s *Schema) bool { return schemaType == "" || s.Type == schemaType },
		toSchemaSummary,
		func(a, c *SchemaSummary) bool { return a.Name < c.Name },
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) BatchGetSchema(
	collaborationID string,
	names []string,
) ([]*Schema, []BatchError, error) {
	b.mu.RLock("BatchGetSchema")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, nil, ErrNotFound
	}
	var results []*Schema
	var errors []BatchError
	for _, name := range names {
		s, ok := b.schemas.Get(collaborationKey(collaborationID, name))
		if ok {
			results = append(results, s)
		} else {
			errors = append(errors, BatchError{Name: name, Code: errCodeNotFound, Message: errMsgNotFound})
		}
	}

	return results, errors, nil
}

func (b *InMemoryBackend) GetSchemaAnalysisRule(
	collaborationID, name, ruleType string,
) (*SchemaAnalysisRule, error) {
	b.mu.RLock("GetSchemaAnalysisRule")
	defer b.mu.RUnlock()
	rule, ok := b.schemaAnalysisRules.Get(schemaAnalysisRuleKey(collaborationID, name, ruleType))
	if !ok {
		return nil, ErrNotFound
	}

	return rule, nil
}

func (b *InMemoryBackend) BatchGetSchemaAnalysisRule(
	collaborationID string,
	names []string,
	ruleType string,
) ([]*SchemaAnalysisRule, []BatchError, error) {
	b.mu.RLock("BatchGetSchemaAnalysisRule")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, nil, ErrNotFound
	}
	var results []*SchemaAnalysisRule
	var errors []BatchError
	for _, name := range names {
		rule, ok := b.schemaAnalysisRules.Get(schemaAnalysisRuleKey(collaborationID, name, ruleType))
		if ok {
			results = append(results, rule)

			continue
		}
		errors = append(
			errors,
			BatchError{Name: name, Code: errCodeNotFound, Message: errMsgNotFound},
		)
	}

	return results, errors, nil
}

// ---- ProtectedQuery ----

func (b *InMemoryBackend) StartProtectedQuery(
	membershipID, sqlText string,
	resultConfig map[string]any,
	computeConfiguration map[string]any,
) (*ProtectedQuery, error) {
	b.mu.Lock("StartProtectedQuery")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	var sqlParams map[string]any
	if sqlText != "" {
		sqlParams = map[string]any{"queryString": sqlText}
	}
	q := &ProtectedQuery{
		ID:                   id,
		MembershipIdentifier: membershipID,
		MembershipArn:        mem.Arn,
		Status:               "SUBMITTED",
		SQLParameters:        sqlParams,
		ResultConfiguration:  resultConfig,
		ComputeConfiguration: computeConfiguration,
		CreateTime:           ts,
		MembershipID:         membershipID,
	}
	b.protectedQueries.Put(q)

	return q, nil
}

func (b *InMemoryBackend) GetProtectedQuery(membershipID, queryID string) (*ProtectedQuery, error) {
	b.mu.RLock("GetProtectedQuery")
	defer b.mu.RUnlock()
	q, ok := b.protectedQueries.Get(membershipKey(membershipID, queryID))
	if !ok {
		return nil, ErrNotFound
	}

	return q, nil
}

func (b *InMemoryBackend) ListProtectedQueries(
	membershipID, status, maxResults, nextToken string,
) ([]*ProtectedQuerySummary, string, error) {
	b.mu.RLock("ListProtectedQueries")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	var items []*ProtectedQuerySummary
	for _, q := range b.protectedQueriesByMembership.Get(membershipID) {
		if status != "" && q.Status != status {
			continue
		}
		items = append(items, &ProtectedQuerySummary{
			ID:                   q.ID,
			MembershipIdentifier: q.MembershipIdentifier,
			MembershipArn:        q.MembershipArn,
			Status:               q.Status,
			CreateTime:           q.CreateTime,
			MembershipID:         q.MembershipIdentifier,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	page, next := paginate(items, maxResults, nextToken)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateProtectedQuery(
	membershipID, queryID, status string,
) (*ProtectedQuery, error) {
	b.mu.Lock("UpdateProtectedQuery")
	defer b.mu.Unlock()
	q, ok := b.protectedQueries.Get(membershipKey(membershipID, queryID))
	if !ok {
		return nil, ErrNotFound
	}
	q.Status = status

	return q, nil
}

// ---- ProtectedJob ----

func (b *InMemoryBackend) StartProtectedJob(
	membershipID, jobType string,
	jobParameters map[string]any,
	resultConfig map[string]any,
) (*ProtectedJob, error) {
	b.mu.Lock("StartProtectedJob")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	j := &ProtectedJob{
		ID:                   id,
		MembershipIdentifier: membershipID,
		MembershipArn:        mem.Arn,
		Status:               "SUBMITTED",
		Type:                 jobType,
		JobParameters:        jobParameters,
		ResultConfiguration:  resultConfig,
		CreateTime:           b.now(),
		MembershipID:         membershipID,
	}
	b.protectedJobs.Put(j)

	return j, nil
}

func (b *InMemoryBackend) GetProtectedJob(membershipID, jobID string) (*ProtectedJob, error) {
	b.mu.RLock("GetProtectedJob")
	defer b.mu.RUnlock()
	j, ok := b.protectedJobs.Get(membershipKey(membershipID, jobID))
	if !ok {
		return nil, ErrNotFound
	}

	return j, nil
}

func (b *InMemoryBackend) ListProtectedJobs(
	membershipID, status, maxResults, nextToken string,
) ([]*ProtectedJobSummary, string, error) {
	b.mu.RLock("ListProtectedJobs")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	var items []*ProtectedJobSummary
	for _, j := range b.protectedJobsByMembership.Get(membershipID) {
		if status != "" && j.Status != status {
			continue
		}
		items = append(items, &ProtectedJobSummary{
			ID:                   j.ID,
			MembershipIdentifier: j.MembershipIdentifier,
			MembershipArn:        j.MembershipArn,
			Status:               j.Status,
			Type:                 j.Type,
			CreateTime:           j.CreateTime,
			MembershipID:         j.MembershipIdentifier,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	page, next := paginate(items, maxResults, nextToken)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateProtectedJob(
	membershipID, jobID, status string,
) (*ProtectedJob, error) {
	b.mu.Lock("UpdateProtectedJob")
	defer b.mu.Unlock()
	j, ok := b.protectedJobs.Get(membershipKey(membershipID, jobID))
	if !ok {
		return nil, ErrNotFound
	}
	j.Status = status

	return j, nil
}

// ---- PrivacyBudgetTemplate ----

func (b *InMemoryBackend) CreatePrivacyBudgetTemplate(
	membershipID, privacyBudgetType, autoRefresh string,
	parameters map[string]any,
	tags map[string]string,
) (*PrivacyBudgetTemplate, error) {
	b.mu.Lock("CreatePrivacyBudgetTemplate")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationIdentifier)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	tmpl := &PrivacyBudgetTemplate{
		PrivacyBudgetTemplateIdentifier: id,
		Arn:                             b.privacyBudgetTemplateARN(membershipID, id),
		CollaborationArn:                collabArn,
		CollaborationIdentifier:         mem.CollaborationIdentifier,
		MembershipArn:                   mem.Arn,
		MembershipIdentifier:            membershipID,
		PrivacyBudgetType:               privacyBudgetType,
		AutoRefresh:                     autoRefresh,
		Parameters:                      parameters,
		CreateTime:                      ts,
		UpdateTime:                      ts,
		Tags:                            tags,
		ID:                              id,
		MembershipID:                    membershipID,
		CollaborationID:                 mem.CollaborationIdentifier,
	}
	b.privacyBudgetTemplates.Put(tmpl)
	if len(tags) > 0 {
		b.tagsByArn[tmpl.Arn] = maps.Clone(tags)
	}

	return tmpl, nil
}

func (b *InMemoryBackend) GetPrivacyBudgetTemplate(
	membershipID, templateID string,
) (*PrivacyBudgetTemplate, error) {
	b.mu.RLock("GetPrivacyBudgetTemplate")
	defer b.mu.RUnlock()
	tmpl, ok := b.privacyBudgetTemplates.Get(membershipKey(membershipID, templateID))
	if !ok {
		return nil, ErrNotFound
	}

	return tmpl, nil
}

func (b *InMemoryBackend) ListPrivacyBudgetTemplates(
	membershipID, privacyBudgetType, maxResults, nextToken string,
) ([]*PrivacyBudgetTemplateSummary, string, error) {
	b.mu.RLock("ListPrivacyBudgetTemplates")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.privacyBudgetTemplatesByMembership.Get(membershipID),
		func(t *PrivacyBudgetTemplate) bool {
			return privacyBudgetType == "" || t.PrivacyBudgetType == privacyBudgetType
		},
		toPrivacyBudgetTemplateSummary,
		func(a, c *PrivacyBudgetTemplateSummary) bool {
			return a.PrivacyBudgetTemplateIdentifier < c.PrivacyBudgetTemplateIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) UpdatePrivacyBudgetTemplate(
	membershipID, templateID, autoRefresh string,
	parameters map[string]any,
) (*PrivacyBudgetTemplate, error) {
	b.mu.Lock("UpdatePrivacyBudgetTemplate")
	defer b.mu.Unlock()
	tmpl, ok := b.privacyBudgetTemplates.Get(membershipKey(membershipID, templateID))
	if !ok {
		return nil, ErrNotFound
	}
	if autoRefresh != "" {
		tmpl.AutoRefresh = autoRefresh
	}
	if parameters != nil {
		tmpl.Parameters = parameters
	}
	tmpl.UpdateTime = b.now()

	return tmpl, nil
}

func (b *InMemoryBackend) DeletePrivacyBudgetTemplate(membershipID, templateID string) error {
	b.mu.Lock("DeletePrivacyBudgetTemplate")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, templateID)
	tmpl, ok := b.privacyBudgetTemplates.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, tmpl.Arn)
	b.privacyBudgetTemplates.Delete(key)

	return nil
}

func (b *InMemoryBackend) ListPrivacyBudgets(
	membershipID, _, _, _ string,
) ([]*PrivacyBudget, string, error) {
	b.mu.RLock("ListPrivacyBudgets")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}

	return []*PrivacyBudget{}, "", nil
}

func (b *InMemoryBackend) ListCollaborationPrivacyBudgets(
	collaborationID, _, _, _ string,
) ([]*PrivacyBudget, string, error) {
	b.mu.RLock("ListCollaborationPrivacyBudgets")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}

	return []*PrivacyBudget{}, "", nil
}

func (b *InMemoryBackend) GetCollaborationPrivacyBudgetTemplate(
	collaborationID, templateID string,
) (*PrivacyBudgetTemplate, error) {
	b.mu.RLock("GetCollaborationPrivacyBudgetTemplate")
	defer b.mu.RUnlock()
	var found *PrivacyBudgetTemplate
	b.privacyBudgetTemplates.Range(func(t *PrivacyBudgetTemplate) bool {
		if t.CollaborationIdentifier == collaborationID && t.PrivacyBudgetTemplateIdentifier == templateID {
			found = t

			return false
		}

		return true
	})
	if found == nil {
		return nil, ErrNotFound
	}

	return found, nil
}

func (b *InMemoryBackend) ListCollaborationPrivacyBudgetTemplates(
	collaborationID, maxResults, nextToken string,
) ([]*PrivacyBudgetTemplateSummary, string, error) {
	b.mu.RLock("ListCollaborationPrivacyBudgetTemplates")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listNestedItems(
		b.privacyBudgetTemplates.All(),
		func(t *PrivacyBudgetTemplate) bool { return t.CollaborationIdentifier == collaborationID },
		toPrivacyBudgetTemplateSummary,
		func(a, c *PrivacyBudgetTemplateSummary) bool {
			return a.PrivacyBudgetTemplateIdentifier < c.PrivacyBudgetTemplateIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) PreviewPrivacyImpact(
	membershipID string,
	_ map[string]any,
) (map[string]any, error) {
	b.mu.RLock("PreviewPrivacyImpact")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, ErrNotFound
	}

	return map[string]any{"privacyImpact": map[string]any{"aggregationCount": []any{}}}, nil
}

// ---- IDMappingTable ----

func (b *InMemoryBackend) CreateIDMappingTable(
	membershipID, name, description string,
	inputReferenceConfig map[string]any,
	kmsKeyArn string,
	tags map[string]string,
) (*IDMappingTable, error) {
	if name == "" {
		return nil, ErrValidation
	}
	b.mu.Lock("CreateIDMappingTable")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationIdentifier)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	t := &IDMappingTable{
		IDMappingTableIdentifier: id,
		Arn:                      b.idMappingTableARN(membershipID, id),
		CollaborationArn:         collabArn,
		CollaborationIdentifier:  mem.CollaborationIdentifier,
		MembershipArn:            mem.Arn,
		MembershipIdentifier:     membershipID,
		Name:                     name,
		Description:              description,
		InputReferenceConfig:     inputReferenceConfig,
		KmsKeyArn:                kmsKeyArn,
		CreateTime:               ts,
		UpdateTime:               ts,
		Tags:                     tags,
		ID:                       id,
		MembershipID:             membershipID,
		CollaborationID:          mem.CollaborationIdentifier,
	}
	b.idMappingTables.Put(t)
	if len(tags) > 0 {
		b.tagsByArn[t.Arn] = maps.Clone(tags)
	}

	return t, nil
}

func (b *InMemoryBackend) GetIDMappingTable(membershipID, tableID string) (*IDMappingTable, error) {
	b.mu.RLock("GetIDMappingTable")
	defer b.mu.RUnlock()
	t, ok := b.idMappingTables.Get(membershipKey(membershipID, tableID))
	if !ok {
		return nil, ErrNotFound
	}

	return t, nil
}

func (b *InMemoryBackend) ListIDMappingTables(
	membershipID, maxResults, nextToken string,
) ([]*IDMappingTableSummary, string, error) {
	b.mu.RLock("ListIDMappingTables")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.idMappingTablesByMembership.Get(membershipID),
		nil,
		toIDMappingTableSummary,
		func(a, c *IDMappingTableSummary) bool {
			return a.IDMappingTableIdentifier < c.IDMappingTableIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateIDMappingTable(
	membershipID, tableID, description, kmsKeyArn string,
) (*IDMappingTable, error) {
	b.mu.Lock("UpdateIDMappingTable")
	defer b.mu.Unlock()
	t, ok := b.idMappingTables.Get(membershipKey(membershipID, tableID))
	if !ok {
		return nil, ErrNotFound
	}
	if description != "" {
		t.Description = description
	}
	if kmsKeyArn != "" {
		t.KmsKeyArn = kmsKeyArn
	}
	t.UpdateTime = b.now()

	return t, nil
}

func (b *InMemoryBackend) DeleteIDMappingTable(membershipID, tableID string) error {
	b.mu.Lock("DeleteIDMappingTable")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, tableID)
	t, ok := b.idMappingTables.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, t.Arn)
	b.idMappingTables.Delete(key)

	return nil
}

func (b *InMemoryBackend) PopulateIDMappingTable(
	membershipID, tableID string,
) (map[string]any, error) {
	b.mu.RLock("PopulateIDMappingTable")
	defer b.mu.RUnlock()
	if _, ok := b.idMappingTables.Get(membershipKey(membershipID, tableID)); !ok {
		return nil, ErrNotFound
	}

	return map[string]any{"mappedJobIdentifier": uuid.NewString()}, nil
}

// ---- IDNamespaceAssociation ----

func (b *InMemoryBackend) CreateIDNamespaceAssociation(
	membershipID, name, description string,
	inputReferenceConfig map[string]any,
	idMappingConfig map[string]any,
	tags map[string]string,
) (*IDNamespaceAssociation, error) {
	b.mu.Lock("CreateIDNamespaceAssociation")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationIdentifier)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	assoc := &IDNamespaceAssociation{
		IDNamespaceAssociationIdentifier: id,
		Arn:                              b.idNamespaceAssocARN(membershipID, id),
		CollaborationArn:                 collabArn,
		CollaborationIdentifier:          mem.CollaborationIdentifier,
		MembershipArn:                    mem.Arn,
		MembershipIdentifier:             membershipID,
		Name:                             name,
		Description:                      description,
		InputReferenceConfig:             inputReferenceConfig,
		IDMappingConfig:                  idMappingConfig,
		CreateTime:                       ts,
		UpdateTime:                       ts,
		Tags:                             tags,
		ID:                               id,
		MembershipID:                     membershipID,
		CollaborationID:                  mem.CollaborationIdentifier,
	}
	b.idNamespaceAssociations.Put(assoc)
	if len(tags) > 0 {
		b.tagsByArn[assoc.Arn] = maps.Clone(tags)
	}

	return assoc, nil
}

func (b *InMemoryBackend) GetIDNamespaceAssociation(
	membershipID, assocID string,
) (*IDNamespaceAssociation, error) {
	b.mu.RLock("GetIDNamespaceAssociation")
	defer b.mu.RUnlock()
	assoc, ok := b.idNamespaceAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}

	return assoc, nil
}

func (b *InMemoryBackend) ListIDNamespaceAssociations(
	membershipID, maxResults, nextToken string,
) ([]*IDNamespaceAssociationSummary, string, error) {
	b.mu.RLock("ListIDNamespaceAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.idNamespaceAssociationsByMembership.Get(membershipID),
		nil,
		toIDNamespaceAssociationSummary,
		func(a, c *IDNamespaceAssociationSummary) bool {
			return a.IDNamespaceAssociationIdentifier < c.IDNamespaceAssociationIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateIDNamespaceAssociation(
	membershipID, assocID, description string,
	idMappingConfig map[string]any,
) (*IDNamespaceAssociation, error) {
	b.mu.Lock("UpdateIDNamespaceAssociation")
	defer b.mu.Unlock()
	assoc, ok := b.idNamespaceAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}
	if description != "" {
		assoc.Description = description
	}
	if idMappingConfig != nil {
		assoc.IDMappingConfig = idMappingConfig
	}
	assoc.UpdateTime = b.now()

	return assoc, nil
}

func (b *InMemoryBackend) DeleteIDNamespaceAssociation(membershipID, assocID string) error {
	b.mu.Lock("DeleteIDNamespaceAssociation")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, assocID)
	assoc, ok := b.idNamespaceAssociations.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, assoc.Arn)
	b.idNamespaceAssociations.Delete(key)

	return nil
}

func (b *InMemoryBackend) GetCollaborationIDNamespaceAssociation(
	collaborationID, assocID string,
) (*IDNamespaceAssociation, error) {
	b.mu.RLock("GetCollaborationIDNamespaceAssociation")
	defer b.mu.RUnlock()
	var found *IDNamespaceAssociation
	b.idNamespaceAssociations.Range(func(a *IDNamespaceAssociation) bool {
		if a.CollaborationIdentifier == collaborationID && a.IDNamespaceAssociationIdentifier == assocID {
			found = a

			return false
		}

		return true
	})
	if found == nil {
		return nil, ErrNotFound
	}

	return found, nil
}

func (b *InMemoryBackend) ListCollaborationIDNamespaceAssociations(
	collaborationID, maxResults, nextToken string,
) ([]*IDNamespaceAssociationSummary, string, error) {
	b.mu.RLock("ListCollaborationIDNamespaceAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listNestedItems(
		b.idNamespaceAssociations.All(),
		func(a *IDNamespaceAssociation) bool { return a.CollaborationIdentifier == collaborationID },
		toIDNamespaceAssociationSummary,
		func(a, c *IDNamespaceAssociationSummary) bool {
			return a.IDNamespaceAssociationIdentifier < c.IDNamespaceAssociationIdentifier
		},
		maxResults,
		nextToken,
	)

	return page, next, nil
}

// ---- ConfiguredAudienceModelAssociation ----

func (b *InMemoryBackend) CreateConfiguredAudienceModelAssociation(
	membershipID, configuredAudienceModelArn, name, description string,
	manageResourcePolicies bool,
	tags map[string]string,
) (*ConfiguredAudienceModelAssociation, error) {
	if configuredAudienceModelArn == "" || name == "" {
		return nil, ErrValidation
	}
	b.mu.Lock("CreateConfiguredAudienceModelAssociation")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationIdentifier)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	assoc := &ConfiguredAudienceModelAssociation{
		ConfiguredAudienceModelAssociationIdentifier: id,
		Arn:                        b.camaARN(membershipID, id),
		CollaborationArn:           collabArn,
		CollaborationIdentifier:    mem.CollaborationIdentifier,
		MembershipArn:              mem.Arn,
		MembershipIdentifier:       membershipID,
		ConfiguredAudienceModelArn: configuredAudienceModelArn,
		Name:                       name,
		Description:                description,
		ManageResourcePolicies:     manageResourcePolicies,
		CreateTime:                 ts,
		UpdateTime:                 ts,
		Tags:                       tags,
		ID:                         id,
		MembershipID:               membershipID,
		CollaborationID:            mem.CollaborationIdentifier,
	}
	b.camaAssociations.Put(assoc)
	if len(tags) > 0 {
		b.tagsByArn[assoc.Arn] = maps.Clone(tags)
	}

	return assoc, nil
}

func (b *InMemoryBackend) GetConfiguredAudienceModelAssociation(
	membershipID, assocID string,
) (*ConfiguredAudienceModelAssociation, error) {
	b.mu.RLock("GetConfiguredAudienceModelAssociation")
	defer b.mu.RUnlock()
	assoc, ok := b.camaAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}

	return assoc, nil
}

func (b *InMemoryBackend) ListConfiguredAudienceModelAssociations(
	membershipID, maxResults, nextToken string,
) ([]*ConfiguredAudienceModelAssociationSummary, string, error) {
	b.mu.RLock("ListConfiguredAudienceModelAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.camaAssociationsByMembership.Get(membershipID),
		nil,
		toConfiguredAudienceModelAssociationSummary,
		func(a, c *ConfiguredAudienceModelAssociationSummary) bool {
			return a.ConfiguredAudienceModelAssociationIdentifier < c.ConfiguredAudienceModelAssociationIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateConfiguredAudienceModelAssociation(
	membershipID, assocID, name, description string,
) (*ConfiguredAudienceModelAssociation, error) {
	b.mu.Lock("UpdateConfiguredAudienceModelAssociation")
	defer b.mu.Unlock()
	assoc, ok := b.camaAssociations.Get(membershipKey(membershipID, assocID))
	if !ok {
		return nil, ErrNotFound
	}
	if name != "" {
		assoc.Name = name
	}
	if description != "" {
		assoc.Description = description
	}
	assoc.UpdateTime = b.now()

	return assoc, nil
}

func (b *InMemoryBackend) DeleteConfiguredAudienceModelAssociation(
	membershipID, assocID string,
) error {
	b.mu.Lock("DeleteConfiguredAudienceModelAssociation")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, assocID)
	assoc, ok := b.camaAssociations.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, assoc.Arn)
	b.camaAssociations.Delete(key)

	return nil
}

func (b *InMemoryBackend) GetCollaborationConfiguredAudienceModelAssociation(
	collaborationID, assocID string,
) (*ConfiguredAudienceModelAssociation, error) {
	b.mu.RLock("GetCollaborationConfiguredAudienceModelAssociation")
	defer b.mu.RUnlock()
	var found *ConfiguredAudienceModelAssociation
	b.camaAssociations.Range(func(a *ConfiguredAudienceModelAssociation) bool {
		if a.CollaborationIdentifier == collaborationID && a.ConfiguredAudienceModelAssociationIdentifier == assocID {
			found = a

			return false
		}

		return true
	})
	if found == nil {
		return nil, ErrNotFound
	}

	return found, nil
}

func (b *InMemoryBackend) ListCollaborationConfiguredAudienceModelAssociations(
	collaborationID, maxResults, nextToken string,
) ([]*ConfiguredAudienceModelAssociationSummary, string, error) {
	b.mu.RLock("ListCollaborationConfiguredAudienceModelAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listNestedItems(
		b.camaAssociations.All(),
		func(a *ConfiguredAudienceModelAssociation) bool {
			return a.CollaborationIdentifier == collaborationID
		},
		toConfiguredAudienceModelAssociationSummary,
		func(a, c *ConfiguredAudienceModelAssociationSummary) bool {
			return a.ConfiguredAudienceModelAssociationIdentifier < c.ConfiguredAudienceModelAssociationIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

// ---- CollaborationChangeRequest ----

func (b *InMemoryBackend) CreateCollaborationChangeRequest(
	collaborationID, changeRequestType string,
	details map[string]any,
) (*CollaborationChangeRequest, error) {
	b.mu.Lock("CreateCollaborationChangeRequest")
	defer b.mu.Unlock()
	collab, ok := b.collaborations.Get(collaborationID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	req := &CollaborationChangeRequest{
		ChangeRequestIdentifier: id,
		CollaborationIdentifier: collaborationID,
		CollaborationArn:        collab.Arn,
		Status:                  "PENDING",
		Type:                    changeRequestType,
		Details:                 details,
		CreateTime:              ts,
		UpdateTime:              ts,
	}
	b.changeRequests.Put(req)

	return req, nil
}

func (b *InMemoryBackend) GetCollaborationChangeRequest(
	collaborationID, changeRequestID string,
) (*CollaborationChangeRequest, error) {
	b.mu.RLock("GetCollaborationChangeRequest")
	defer b.mu.RUnlock()
	req, ok := b.changeRequests.Get(collaborationKey(collaborationID, changeRequestID))
	if !ok {
		return nil, ErrNotFound
	}

	return req, nil
}

func (b *InMemoryBackend) ListCollaborationChangeRequests(
	collaborationID, maxResults, nextToken string,
) ([]*CollaborationChangeRequest, string, error) {
	b.mu.RLock("ListCollaborationChangeRequests")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	items := slices.Clone(b.changeRequestsByCollaboration.Get(collaborationID))
	sort.Slice(
		items,
		func(i, j int) bool { return items[i].ChangeRequestIdentifier < items[j].ChangeRequestIdentifier },
	)
	page, next := paginate(items, maxResults, nextToken)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateCollaborationChangeRequest(
	collaborationID, changeRequestID, status string,
) (*CollaborationChangeRequest, error) {
	b.mu.Lock("UpdateCollaborationChangeRequest")
	defer b.mu.Unlock()
	req, ok := b.changeRequests.Get(collaborationKey(collaborationID, changeRequestID))
	if !ok {
		return nil, ErrNotFound
	}
	req.Status = status
	req.UpdateTime = b.now()

	return req, nil
}

// ---- Tags ----

func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	if tags, ok := b.tagsByArn[resourceArn]; ok {
		return maps.Clone(tags), nil
	}

	return map[string]string{}, nil
}

func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()
	if b.tagsByArn[resourceArn] == nil {
		b.tagsByArn[resourceArn] = make(map[string]string)
	}
	maps.Copy(b.tagsByArn[resourceArn], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()
	tags := b.tagsByArn[resourceArn]
	for _, k := range tagKeys {
		delete(tags, k)
	}

	return nil
}

// ---- helpers ----

func contains(ss []string, s string) bool {
	return slices.Contains(ss, s)
}

func removeFrom(ss []string, s string) []string {
	var out []string
	for _, v := range ss {
		if v != s {
			out = append(out, v)
		}
	}

	return out
}

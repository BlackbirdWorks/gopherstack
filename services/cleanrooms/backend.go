// Package cleanrooms implements an in-memory AWS Clean Rooms service backend.
package cleanrooms

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// ---- types ----

type MemberSpec struct {
	AccountID     string         `json:"accountId"`
	DisplayName   string         `json:"displayName"`
	Abilities     []string       `json:"memberAbilities"`
	PaymentConfig map[string]any `json:"paymentConfiguration,omitempty"`
}

type MemberSummary struct {
	AccountID   string   `json:"accountId"`
	DisplayName string   `json:"displayName"`
	Abilities   []string `json:"abilities"`
	Status      string   `json:"status"`
	CreateTime  float64  `json:"createTime,omitempty"`
	UpdateTime  float64  `json:"updateTime,omitempty"`
}

type Collaboration struct {
	CollaborationIdentifier string            `json:"collaborationIdentifier"`
	Arn                     string            `json:"arn"`
	Name                    string            `json:"name"`
	Description             string            `json:"description,omitempty"`
	CreatorAccountId        string            `json:"creatorAccountId"`
	CreatorDisplayName      string            `json:"creatorDisplayName"`
	MemberAbilities         []string          `json:"memberAbilities,omitempty"`
	Members                 []*MemberSummary  `json:"members,omitempty"`
	QueryLogStatus          string            `json:"queryLogStatus,omitempty"`
	CreateTime              float64           `json:"createTime,omitempty"`
	UpdateTime              float64           `json:"updateTime,omitempty"`
	Tags                    map[string]string `json:"tags,omitempty"`
}

type CollaborationSummary struct {
	CollaborationIdentifier string  `json:"collaborationIdentifier"`
	Arn                     string  `json:"arn"`
	Name                    string  `json:"name"`
	CreatorAccountId        string  `json:"creatorAccountId"`
	CreatorDisplayName      string  `json:"creatorDisplayName"`
	MemberStatus            string  `json:"memberStatus"`
	CreateTime              float64 `json:"createTime,omitempty"`
	UpdateTime              float64 `json:"updateTime,omitempty"`
}

type Membership struct {
	MembershipIdentifier            string         `json:"membershipIdentifier"`
	Arn                             string         `json:"arn"`
	CollaborationIdentifier         string         `json:"collaborationIdentifier"`
	CollaborationArn                string         `json:"collaborationArn"`
	CollaborationCreatorAccountId   string         `json:"collaborationCreatorAccountId"`
	CollaborationCreatorDisplayName string         `json:"collaborationCreatorDisplayName"`
	CollaborationName               string         `json:"collaborationName"`
	Status                          string         `json:"status"`
	MemberAbilities                 []string       `json:"memberAbilities,omitempty"`
	QueryLogStatus                  string         `json:"queryLogStatus,omitempty"`
	DefaultResultConfiguration      map[string]any `json:"defaultResultConfiguration,omitempty"`
	PaymentConfiguration            map[string]any `json:"paymentConfiguration,omitempty"`
	CreateTime                      float64        `json:"createTime,omitempty"`
	UpdateTime                      float64        `json:"updateTime,omitempty"`
}

type MembershipSummary struct {
	MembershipIdentifier            string   `json:"membershipIdentifier"`
	Arn                             string   `json:"arn"`
	CollaborationIdentifier         string   `json:"collaborationIdentifier"`
	CollaborationArn                string   `json:"collaborationArn"`
	CollaborationCreatorAccountId   string   `json:"collaborationCreatorAccountId"`
	CollaborationCreatorDisplayName string   `json:"collaborationCreatorDisplayName"`
	CollaborationName               string   `json:"collaborationName"`
	Status                          string   `json:"status"`
	MemberAbilities                 []string `json:"memberAbilities,omitempty"`
	CreateTime                      float64  `json:"createTime,omitempty"`
	UpdateTime                      float64  `json:"updateTime,omitempty"`
}

type ConfiguredTable struct {
	ConfiguredTableIdentifier string            `json:"configuredTableIdentifier"`
	Arn                       string            `json:"arn"`
	Name                      string            `json:"name"`
	Description               string            `json:"description,omitempty"`
	TableReference            map[string]any    `json:"tableReference,omitempty"`
	AllowedColumns            []string          `json:"allowedColumns,omitempty"`
	AnalysisMethod            string            `json:"analysisMethod,omitempty"`
	AnalysisRuleTypes         []string          `json:"analysisRuleTypes,omitempty"`
	CreateTime                float64           `json:"createTime,omitempty"`
	UpdateTime                float64           `json:"updateTime,omitempty"`
	Tags                      map[string]string `json:"tags,omitempty"`
}

type ConfiguredTableSummary struct {
	ConfiguredTableIdentifier string   `json:"configuredTableIdentifier"`
	Arn                       string   `json:"arn"`
	Name                      string   `json:"name"`
	AnalysisMethod            string   `json:"analysisMethod,omitempty"`
	AnalysisRuleTypes         []string `json:"analysisRuleTypes,omitempty"`
	CreateTime                float64  `json:"createTime,omitempty"`
	UpdateTime                float64  `json:"updateTime,omitempty"`
}

type ConfiguredTableAnalysisRule struct {
	ConfiguredTableIdentifier string         `json:"configuredTableIdentifier"`
	ConfiguredTableArn        string         `json:"configuredTableArn"`
	Type                      string         `json:"type"`
	Policy                    map[string]any `json:"policy,omitempty"`
	CreateTime                float64        `json:"createTime,omitempty"`
	UpdateTime                float64        `json:"updateTime,omitempty"`
}

type ConfiguredTableAssociation struct {
	ConfiguredTableAssociationIdentifier string            `json:"configuredTableAssociationIdentifier"`
	Arn                                  string            `json:"arn"`
	MembershipIdentifier                 string            `json:"membershipIdentifier"`
	MembershipArn                        string            `json:"membershipArn"`
	ConfiguredTableIdentifier            string            `json:"configuredTableIdentifier"`
	ConfiguredTableArn                   string            `json:"configuredTableArn"`
	Name                                 string            `json:"name"`
	Description                          string            `json:"description,omitempty"`
	RoleArn                              string            `json:"roleArn,omitempty"`
	AnalysisRuleTypes                    []string          `json:"analysisRuleTypes,omitempty"`
	CreateTime                           float64           `json:"createTime,omitempty"`
	UpdateTime                           float64           `json:"updateTime,omitempty"`
	Tags                                 map[string]string `json:"tags,omitempty"`
}

type ConfiguredTableAssociationSummary struct {
	ConfiguredTableAssociationIdentifier string  `json:"configuredTableAssociationIdentifier"`
	Arn                                  string  `json:"arn"`
	MembershipIdentifier                 string  `json:"membershipIdentifier"`
	MembershipArn                        string  `json:"membershipArn"`
	ConfiguredTableIdentifier            string  `json:"configuredTableIdentifier"`
	Name                                 string  `json:"name"`
	CreateTime                           float64 `json:"createTime,omitempty"`
	UpdateTime                           float64 `json:"updateTime,omitempty"`
}

type ConfiguredTableAssociationAnalysisRule struct {
	ConfiguredTableAssociationIdentifier string         `json:"configuredTableAssociationIdentifier"`
	ConfiguredTableAssociationArn        string         `json:"configuredTableAssociationArn"`
	MembershipIdentifier                 string         `json:"membershipIdentifier"`
	MembershipArn                        string         `json:"membershipArn"`
	Type                                 string         `json:"type"`
	Policy                               map[string]any `json:"policy,omitempty"`
	CreateTime                           float64        `json:"createTime,omitempty"`
	UpdateTime                           float64        `json:"updateTime,omitempty"`
}

type AnalysisTemplate struct {
	AnalysisTemplateIdentifier string            `json:"analysisTemplateIdentifier"`
	Arn                        string            `json:"arn"`
	CollaborationArn           string            `json:"collaborationArn"`
	CollaborationIdentifier    string            `json:"collaborationIdentifier"`
	MembershipIdentifier       string            `json:"membershipIdentifier"`
	MembershipArn              string            `json:"membershipArn"`
	Name                       string            `json:"name"`
	Description                string            `json:"description,omitempty"`
	Source                     map[string]any    `json:"source,omitempty"`
	Schema                     map[string]any    `json:"schema,omitempty"`
	Format                     string            `json:"format,omitempty"`
	AnalysisParameters         []map[string]any  `json:"analysisParameters,omitempty"`
	CreateTime                 float64           `json:"createTime,omitempty"`
	UpdateTime                 float64           `json:"updateTime,omitempty"`
	Tags                       map[string]string `json:"tags,omitempty"`
}

type AnalysisTemplateSummary struct {
	AnalysisTemplateIdentifier string  `json:"analysisTemplateIdentifier"`
	Arn                        string  `json:"arn"`
	CollaborationArn           string  `json:"collaborationArn"`
	CollaborationIdentifier    string  `json:"collaborationIdentifier"`
	MembershipIdentifier       string  `json:"membershipIdentifier"`
	MembershipArn              string  `json:"membershipArn"`
	Name                       string  `json:"name"`
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
	CreatorAccountId        string           `json:"creatorAccountId"`
	Name                    string           `json:"name"`
	Type                    string           `json:"type"`
	Columns                 []map[string]any `json:"columns,omitempty"`
	PartitionKeys           []map[string]any `json:"partitionKeys,omitempty"`
	AnalysisRuleTypes       []string         `json:"analysisRuleTypes,omitempty"`
	AnalysisMethod          string           `json:"analysisMethod,omitempty"`
	CreateTime              float64          `json:"createTime,omitempty"`
	UpdateTime              float64          `json:"updateTime,omitempty"`
}

type SchemaSummary struct {
	CollaborationArn        string   `json:"collaborationArn"`
	CollaborationIdentifier string   `json:"collaborationIdentifier"`
	CreatorAccountId        string   `json:"creatorAccountId"`
	Name                    string   `json:"name"`
	Type                    string   `json:"type"`
	AnalysisRuleTypes       []string `json:"analysisRuleTypes,omitempty"`
	AnalysisMethod          string   `json:"analysisMethod,omitempty"`
	CreateTime              float64  `json:"createTime,omitempty"`
	UpdateTime              float64  `json:"updateTime,omitempty"`
}

type SchemaAnalysisRule struct {
	CollaborationArn        string         `json:"collaborationArn"`
	CollaborationIdentifier string         `json:"collaborationIdentifier"`
	Name                    string         `json:"name"`
	Type                    string         `json:"type"`
	Policy                  map[string]any `json:"policy,omitempty"`
	CreateTime              float64        `json:"createTime,omitempty"`
	UpdateTime              float64        `json:"updateTime,omitempty"`
}

type ProtectedQuery struct {
	Id                   string         `json:"id"`
	MembershipIdentifier string         `json:"membershipIdentifier"`
	MembershipArn        string         `json:"membershipArn"`
	Status               string         `json:"status"`
	SqlParameters        map[string]any `json:"sqlParameters,omitempty"`
	ResultConfiguration  map[string]any `json:"resultConfiguration,omitempty"`
	ComputeConfiguration map[string]any `json:"computeConfiguration,omitempty"`
	Statistics           map[string]any `json:"statistics,omitempty"`
	Result               map[string]any `json:"result,omitempty"`
	Error                map[string]any `json:"error,omitempty"`
	CreateTime           float64        `json:"createTime,omitempty"`
}

type ProtectedQuerySummary struct {
	Id                   string  `json:"id"`
	MembershipIdentifier string  `json:"membershipIdentifier"`
	MembershipArn        string  `json:"membershipArn"`
	Status               string  `json:"status"`
	CreateTime           float64 `json:"createTime,omitempty"`
}

type ProtectedJob struct {
	Id                   string         `json:"id"`
	MembershipIdentifier string         `json:"membershipIdentifier"`
	MembershipArn        string         `json:"membershipArn"`
	Status               string         `json:"status"`
	Type                 string         `json:"type"`
	JobParameters        map[string]any `json:"jobParameters,omitempty"`
	ResultConfiguration  map[string]any `json:"resultConfiguration,omitempty"`
	Statistics           map[string]any `json:"statistics,omitempty"`
	Result               map[string]any `json:"result,omitempty"`
	Error                map[string]any `json:"error,omitempty"`
	CreateTime           float64        `json:"createTime,omitempty"`
}

type ProtectedJobSummary struct {
	Id                   string  `json:"id"`
	MembershipIdentifier string  `json:"membershipIdentifier"`
	MembershipArn        string  `json:"membershipArn"`
	Status               string  `json:"status"`
	Type                 string  `json:"type"`
	CreateTime           float64 `json:"createTime,omitempty"`
}

type PrivacyBudgetTemplate struct {
	PrivacyBudgetTemplateIdentifier string            `json:"privacyBudgetTemplateIdentifier"`
	Arn                             string            `json:"arn"`
	CollaborationArn                string            `json:"collaborationArn"`
	CollaborationIdentifier         string            `json:"collaborationIdentifier"`
	MembershipArn                   string            `json:"membershipArn"`
	MembershipIdentifier            string            `json:"membershipIdentifier"`
	PrivacyBudgetType               string            `json:"privacyBudgetType"`
	AutoRefresh                     string            `json:"autoRefresh,omitempty"`
	Parameters                      map[string]any    `json:"parameters,omitempty"`
	CreateTime                      float64           `json:"createTime,omitempty"`
	UpdateTime                      float64           `json:"updateTime,omitempty"`
	Tags                            map[string]string `json:"tags,omitempty"`
}

type PrivacyBudgetTemplateSummary struct {
	PrivacyBudgetTemplateIdentifier string  `json:"privacyBudgetTemplateIdentifier"`
	Arn                             string  `json:"arn"`
	CollaborationArn                string  `json:"collaborationArn"`
	CollaborationIdentifier         string  `json:"collaborationIdentifier"`
	MembershipArn                   string  `json:"membershipArn"`
	MembershipIdentifier            string  `json:"membershipIdentifier"`
	PrivacyBudgetType               string  `json:"privacyBudgetType"`
	CreateTime                      float64 `json:"createTime,omitempty"`
	UpdateTime                      float64 `json:"updateTime,omitempty"`
}

type PrivacyBudget struct {
	Id                              string         `json:"id"`
	PrivacyBudgetTemplateArn        string         `json:"privacyBudgetTemplateArn"`
	PrivacyBudgetTemplateIdentifier string         `json:"privacyBudgetTemplateIdentifier"`
	CollaborationArn                string         `json:"collaborationArn"`
	CollaborationIdentifier         string         `json:"collaborationIdentifier"`
	MembershipArn                   string         `json:"membershipArn"`
	MembershipIdentifier            string         `json:"membershipIdentifier"`
	PrivacyBudgetType               string         `json:"privacyBudgetType"`
	Budget                          map[string]any `json:"budget,omitempty"`
}

type IdMappingTable struct {
	IdMappingTableIdentifier string            `json:"idMappingTableIdentifier"`
	Arn                      string            `json:"arn"`
	CollaborationArn         string            `json:"collaborationArn"`
	CollaborationIdentifier  string            `json:"collaborationIdentifier"`
	MembershipArn            string            `json:"membershipArn"`
	MembershipIdentifier     string            `json:"membershipIdentifier"`
	Name                     string            `json:"name"`
	Description              string            `json:"description,omitempty"`
	InputReferenceConfig     map[string]any    `json:"inputReferenceConfig,omitempty"`
	InputReferenceProperties map[string]any    `json:"inputReferenceProperties,omitempty"`
	KmsKeyArn                string            `json:"kmsKeyArn,omitempty"`
	CreateTime               float64           `json:"createTime,omitempty"`
	UpdateTime               float64           `json:"updateTime,omitempty"`
	Tags                     map[string]string `json:"tags,omitempty"`
}

type IdMappingTableSummary struct {
	IdMappingTableIdentifier string  `json:"idMappingTableIdentifier"`
	Arn                      string  `json:"arn"`
	CollaborationArn         string  `json:"collaborationArn"`
	CollaborationIdentifier  string  `json:"collaborationIdentifier"`
	MembershipArn            string  `json:"membershipArn"`
	MembershipIdentifier     string  `json:"membershipIdentifier"`
	Name                     string  `json:"name"`
	CreateTime               float64 `json:"createTime,omitempty"`
	UpdateTime               float64 `json:"updateTime,omitempty"`
}

type IdNamespaceAssociation struct {
	IdNamespaceAssociationIdentifier string            `json:"idNamespaceAssociationIdentifier"`
	Arn                              string            `json:"arn"`
	CollaborationArn                 string            `json:"collaborationArn"`
	CollaborationIdentifier          string            `json:"collaborationIdentifier"`
	MembershipArn                    string            `json:"membershipArn"`
	MembershipIdentifier             string            `json:"membershipIdentifier"`
	Name                             string            `json:"name"`
	Description                      string            `json:"description,omitempty"`
	InputReferenceConfig             map[string]any    `json:"inputReferenceConfig,omitempty"`
	InputReferenceProperties         map[string]any    `json:"inputReferenceProperties,omitempty"`
	IdMappingConfig                  map[string]any    `json:"idMappingConfig,omitempty"`
	CreateTime                       float64           `json:"createTime,omitempty"`
	UpdateTime                       float64           `json:"updateTime,omitempty"`
	Tags                             map[string]string `json:"tags,omitempty"`
}

type IdNamespaceAssociationSummary struct {
	IdNamespaceAssociationIdentifier string  `json:"idNamespaceAssociationIdentifier"`
	Arn                              string  `json:"arn"`
	CollaborationArn                 string  `json:"collaborationArn"`
	CollaborationIdentifier          string  `json:"collaborationIdentifier"`
	MembershipArn                    string  `json:"membershipArn"`
	MembershipIdentifier             string  `json:"membershipIdentifier"`
	Name                             string  `json:"name"`
	CreateTime                       float64 `json:"createTime,omitempty"`
	UpdateTime                       float64 `json:"updateTime,omitempty"`
}

type ConfiguredAudienceModelAssociation struct {
	ConfiguredAudienceModelAssociationIdentifier string            `json:"configuredAudienceModelAssociationIdentifier"`
	Arn                                          string            `json:"arn"`
	CollaborationArn                             string            `json:"collaborationArn"`
	CollaborationIdentifier                      string            `json:"collaborationIdentifier"`
	MembershipArn                                string            `json:"membershipArn"`
	MembershipIdentifier                         string            `json:"membershipIdentifier"`
	ConfiguredAudienceModelArn                   string            `json:"configuredAudienceModelArn"`
	Name                                         string            `json:"name"`
	Description                                  string            `json:"description,omitempty"`
	ManageResourcePolicies                       bool              `json:"manageResourcePolicies"`
	CreateTime                                   float64           `json:"createTime,omitempty"`
	UpdateTime                                   float64           `json:"updateTime,omitempty"`
	Tags                                         map[string]string `json:"tags,omitempty"`
}

type ConfiguredAudienceModelAssociationSummary struct {
	ConfiguredAudienceModelAssociationIdentifier string  `json:"configuredAudienceModelAssociationIdentifier"`
	Arn                                          string  `json:"arn"`
	CollaborationArn                             string  `json:"collaborationArn"`
	CollaborationIdentifier                      string  `json:"collaborationIdentifier"`
	MembershipArn                                string  `json:"membershipArn"`
	MembershipIdentifier                         string  `json:"membershipIdentifier"`
	Name                                         string  `json:"name"`
	CreateTime                                   float64 `json:"createTime,omitempty"`
	UpdateTime                                   float64 `json:"updateTime,omitempty"`
}

type CollaborationChangeRequest struct {
	ChangeRequestIdentifier string         `json:"changeRequestIdentifier"`
	CollaborationIdentifier string         `json:"collaborationIdentifier"`
	CollaborationArn        string         `json:"collaborationArn"`
	Status                  string         `json:"status"`
	Type                    string         `json:"type"`
	Details                 map[string]any `json:"details,omitempty"`
	CreateTime              float64        `json:"createTime,omitempty"`
	UpdateTime              float64        `json:"updateTime,omitempty"`
}

// ---- InMemoryBackend ----

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	accountID string
	region    string

	collaborations          map[string]*Collaboration
	memberships             map[string]*Membership
	configuredTables        map[string]*ConfiguredTable
	ctAnalysisRules         map[string]map[string]*ConfiguredTableAnalysisRule
	ctAssociations          map[string]map[string]*ConfiguredTableAssociation
	ctaAnalysisRules        map[string]map[string]*ConfiguredTableAssociationAnalysisRule
	analysisTemplates       map[string]map[string]*AnalysisTemplate
	protectedQueries        map[string]map[string]*ProtectedQuery
	protectedJobs           map[string]map[string]*ProtectedJob
	privacyBudgetTemplates  map[string]map[string]*PrivacyBudgetTemplate
	idMappingTables         map[string]map[string]*IdMappingTable
	idNamespaceAssociations map[string]map[string]*IdNamespaceAssociation
	camaAssociations        map[string]map[string]*ConfiguredAudienceModelAssociation
	changeRequests          map[string]map[string]*CollaborationChangeRequest
	schemas                 map[string]map[string]*Schema
	schemaAnalysisRules     map[string]map[string]map[string]*SchemaAnalysisRule
	tagsByArn               map[string]map[string]string
}

// NewInMemoryBackendWithContext creates a backend tied to svcCtx (ignored; no lifecycle goroutines).
func NewInMemoryBackendWithContext(_ context.Context, accountID, region string) *InMemoryBackend {
	return NewInMemoryBackend(accountID, region)
}

// NewInMemoryBackend creates a new in-memory Clean Rooms backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:               lockmetrics.New("cleanrooms"),
		accountID:        accountID,
		region:           region,
		collaborations:   make(map[string]*Collaboration),
		memberships:      make(map[string]*Membership),
		configuredTables: make(map[string]*ConfiguredTable),
		ctAnalysisRules:  make(map[string]map[string]*ConfiguredTableAnalysisRule),
		ctAssociations:   make(map[string]map[string]*ConfiguredTableAssociation),
		ctaAnalysisRules: make(
			map[string]map[string]*ConfiguredTableAssociationAnalysisRule,
		),
		analysisTemplates:       make(map[string]map[string]*AnalysisTemplate),
		protectedQueries:        make(map[string]map[string]*ProtectedQuery),
		protectedJobs:           make(map[string]map[string]*ProtectedJob),
		privacyBudgetTemplates:  make(map[string]map[string]*PrivacyBudgetTemplate),
		idMappingTables:         make(map[string]map[string]*IdMappingTable),
		idNamespaceAssociations: make(map[string]map[string]*IdNamespaceAssociation),
		camaAssociations:        make(map[string]map[string]*ConfiguredAudienceModelAssociation),
		changeRequests:          make(map[string]map[string]*CollaborationChangeRequest),
		schemas:                 make(map[string]map[string]*Schema),
		schemaAnalysisRules:     make(map[string]map[string]map[string]*SchemaAnalysisRule),
		tagsByArn:               make(map[string]map[string]string),
	}
}

func (b *InMemoryBackend) Region() string    { return b.region }
func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.collaborations = make(map[string]*Collaboration)
	b.memberships = make(map[string]*Membership)
	b.configuredTables = make(map[string]*ConfiguredTable)
	b.ctAnalysisRules = make(map[string]map[string]*ConfiguredTableAnalysisRule)
	b.ctAssociations = make(map[string]map[string]*ConfiguredTableAssociation)
	b.ctaAnalysisRules = make(map[string]map[string]*ConfiguredTableAssociationAnalysisRule)
	b.analysisTemplates = make(map[string]map[string]*AnalysisTemplate)
	b.protectedQueries = make(map[string]map[string]*ProtectedQuery)
	b.protectedJobs = make(map[string]map[string]*ProtectedJob)
	b.privacyBudgetTemplates = make(map[string]map[string]*PrivacyBudgetTemplate)
	b.idMappingTables = make(map[string]map[string]*IdMappingTable)
	b.idNamespaceAssociations = make(map[string]map[string]*IdNamespaceAssociation)
	b.camaAssociations = make(map[string]map[string]*ConfiguredAudienceModelAssociation)
	b.changeRequests = make(map[string]map[string]*CollaborationChangeRequest)
	b.schemas = make(map[string]map[string]*Schema)
	b.schemaAnalysisRules = make(map[string]map[string]map[string]*SchemaAnalysisRule)
	b.tagsByArn = make(map[string]map[string]string)
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

// listItems ranges over a flat map, optionally skipping items where include returns false,
// converts each item to a summary, sorts by the less predicate, then paginates.
func listItems[T, S any](
	items map[string]*T,
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

// listNestedItems ranges over a nested map, collecting items that satisfy match,
// converts them to summaries, sorts, and paginates.
func listNestedItems[T, S any](
	allItems map[string]map[string]*T,
	match func(*T) bool,
	convert func(*T) *S,
	less func(a, b *S) bool,
	maxResults, nextToken string,
) ([]*S, string) {
	var result []*S
	for _, inner := range allItems {
		for _, t := range inner {
			if match(t) {
				result = append(result, convert(t))
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return less(result[i], result[j]) })
	return paginate(result, maxResults, nextToken)
}

func paginate[T any](items []T, maxResultsStr, nextToken string) ([]T, string) {
	if len(items) == 0 {
		return items, ""
	}
	max := 100
	if maxResultsStr != "" {
		fmt.Sscanf(maxResultsStr, "%d", &max)
	}
	if max <= 0 || max > 1000 {
		max = 100
	}
	start := 0
	if nextToken != "" {
		fmt.Sscanf(nextToken, "%d", &start)
	}
	if start >= len(items) {
		return []T{}, ""
	}
	end := start + max
	if end >= len(items) {
		return items[start:], ""
	}
	return items[start:end], fmt.Sprintf("%d", end)
}

// ---- now helper ----

var nowFn = func() float64 { return float64(time.Now().Unix()) }
var muNow sync.Mutex

func now() float64 {
	muNow.Lock()
	defer muNow.Unlock()
	return nowFn()
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
	ts := now()
	memberSummaries := make([]*MemberSummary, 0, len(members)+1)
	memberSummaries = append(memberSummaries, &MemberSummary{
		AccountID:   b.accountID,
		DisplayName: creatorDisplayName,
		Abilities:   creatorMemberAbilities,
		Status:      "ACTIVE",
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
		Arn:                     b.collaborationARN(id),
		Name:                    name,
		Description:             description,
		CreatorAccountId:        b.accountID,
		CreatorDisplayName:      creatorDisplayName,
		MemberAbilities:         creatorMemberAbilities,
		Members:                 memberSummaries,
		QueryLogStatus:          queryLogStatus,
		CreateTime:              ts,
		UpdateTime:              ts,
		Tags:                    tags,
	}
	b.collaborations[id] = collab
	if len(tags) > 0 {
		b.tagsByArn[collab.Arn] = maps.Clone(tags)
	}
	return collab, nil
}

func (b *InMemoryBackend) GetCollaboration(id string) (*Collaboration, error) {
	b.mu.RLock("GetCollaboration")
	defer b.mu.RUnlock()
	c, ok := b.collaborations[id]
	if !ok {
		return nil, ErrNotFound
	}
	return c, nil
}

func (b *InMemoryBackend) ListCollaborations(
	memberStatus, maxResults, nextToken string,
) ([]*CollaborationSummary, string) {
	b.mu.RLock("ListCollaborations")
	defer b.mu.RUnlock()
	var items []*CollaborationSummary
	for _, c := range b.collaborations {
		items = append(items, &CollaborationSummary{
			CollaborationIdentifier: c.CollaborationIdentifier,
			Arn:                     c.Arn,
			Name:                    c.Name,
			CreatorAccountId:        c.CreatorAccountId,
			CreatorDisplayName:      c.CreatorDisplayName,
			MemberStatus:            "ACTIVE",
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
	c, ok := b.collaborations[id]
	if !ok {
		return nil, ErrNotFound
	}
	if name != "" {
		c.Name = name
	}
	if description != "" {
		c.Description = description
	}
	c.UpdateTime = now()
	return c, nil
}

func (b *InMemoryBackend) DeleteCollaboration(id string) error {
	b.mu.Lock("DeleteCollaboration")
	defer b.mu.Unlock()
	c, ok := b.collaborations[id]
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, c.Arn)
	delete(b.collaborations, id)
	return nil
}

func (b *InMemoryBackend) ListMembers(
	collaborationID string,
	maxResults, nextToken string,
) ([]*MemberSummary, string, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()
	c, ok := b.collaborations[collaborationID]
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
	c, ok := b.collaborations[collaborationID]
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
	defaultResultConfiguration map[string]any,
	paymentConfiguration map[string]any,
	tags map[string]string,
) (*Membership, error) {
	b.mu.Lock("CreateMembership")
	defer b.mu.Unlock()
	if collaborationID == "" {
		return nil, ErrValidation
	}
	collab, ok := b.collaborations[collaborationID]
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := now()
	m := &Membership{
		MembershipIdentifier:            id,
		Arn:                             b.membershipARN(id),
		CollaborationIdentifier:         collaborationID,
		CollaborationArn:                collab.Arn,
		CollaborationCreatorAccountId:   collab.CreatorAccountId,
		CollaborationCreatorDisplayName: collab.CreatorDisplayName,
		CollaborationName:               collab.Name,
		Status:                          "ACTIVE",
		QueryLogStatus:                  queryLogStatus,
		DefaultResultConfiguration:      defaultResultConfiguration,
		PaymentConfiguration:            paymentConfiguration,
		CreateTime:                      ts,
		UpdateTime:                      ts,
	}
	b.memberships[id] = m
	if len(tags) > 0 {
		b.tagsByArn[m.Arn] = maps.Clone(tags)
	}
	return m, nil
}

func (b *InMemoryBackend) GetMembership(id string) (*Membership, error) {
	b.mu.RLock("GetMembership")
	defer b.mu.RUnlock()
	m, ok := b.memberships[id]
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
	for _, m := range b.memberships {
		if status != "" && m.Status != status {
			continue
		}
		items = append(items, &MembershipSummary{
			MembershipIdentifier:            m.MembershipIdentifier,
			Arn:                             m.Arn,
			CollaborationIdentifier:         m.CollaborationIdentifier,
			CollaborationArn:                m.CollaborationArn,
			CollaborationCreatorAccountId:   m.CollaborationCreatorAccountId,
			CollaborationCreatorDisplayName: m.CollaborationCreatorDisplayName,
			CollaborationName:               m.CollaborationName,
			Status:                          m.Status,
			MemberAbilities:                 m.MemberAbilities,
			CreateTime:                      m.CreateTime,
			UpdateTime:                      m.UpdateTime,
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
	m, ok := b.memberships[id]
	if !ok {
		return nil, ErrNotFound
	}
	if queryLogStatus != "" {
		m.QueryLogStatus = queryLogStatus
	}
	if defaultResultConfiguration != nil {
		m.DefaultResultConfiguration = defaultResultConfiguration
	}
	m.UpdateTime = now()
	return m, nil
}

func (b *InMemoryBackend) DeleteMembership(id string) error {
	b.mu.Lock("DeleteMembership")
	defer b.mu.Unlock()
	m, ok := b.memberships[id]
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, m.Arn)
	delete(b.memberships, id)
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
	ts := now()
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
	}
	b.configuredTables[id] = ct
	if len(tags) > 0 {
		b.tagsByArn[ct.Arn] = maps.Clone(tags)
	}
	return ct, nil
}

func (b *InMemoryBackend) GetConfiguredTable(id string) (*ConfiguredTable, error) {
	b.mu.RLock("GetConfiguredTable")
	defer b.mu.RUnlock()
	ct, ok := b.configuredTables[id]
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
	var items []*ConfiguredTableSummary
	for _, ct := range b.configuredTables {
		items = append(items, &ConfiguredTableSummary{
			ConfiguredTableIdentifier: ct.ConfiguredTableIdentifier,
			Arn:                       ct.Arn,
			Name:                      ct.Name,
			AnalysisMethod:            ct.AnalysisMethod,
			AnalysisRuleTypes:         ct.AnalysisRuleTypes,
			CreateTime:                ct.CreateTime,
			UpdateTime:                ct.UpdateTime,
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
	ct, ok := b.configuredTables[id]
	if !ok {
		return nil, ErrNotFound
	}
	if name != "" {
		ct.Name = name
	}
	if description != "" {
		ct.Description = description
	}
	ct.UpdateTime = now()
	return ct, nil
}

func (b *InMemoryBackend) DeleteConfiguredTable(id string) error {
	b.mu.Lock("DeleteConfiguredTable")
	defer b.mu.Unlock()
	ct, ok := b.configuredTables[id]
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, ct.Arn)
	delete(b.configuredTables, id)
	delete(b.ctAnalysisRules, id)
	return nil
}

// ---- ConfiguredTableAnalysisRule ----

func (b *InMemoryBackend) CreateConfiguredTableAnalysisRule(
	configuredTableID, analysisRuleType string,
	policy map[string]any,
) (*ConfiguredTableAnalysisRule, error) {
	b.mu.Lock("CreateConfiguredTableAnalysisRule")
	defer b.mu.Unlock()
	ct, ok := b.configuredTables[configuredTableID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.ctAnalysisRules[configuredTableID] == nil {
		b.ctAnalysisRules[configuredTableID] = make(map[string]*ConfiguredTableAnalysisRule)
	}
	if _, exists := b.ctAnalysisRules[configuredTableID][analysisRuleType]; exists {
		return nil, ErrAlreadyExists
	}
	ts := now()
	rule := &ConfiguredTableAnalysisRule{
		ConfiguredTableIdentifier: configuredTableID,
		ConfiguredTableArn:        ct.Arn,
		Type:                      analysisRuleType,
		Policy:                    policy,
		CreateTime:                ts,
		UpdateTime:                ts,
	}
	b.ctAnalysisRules[configuredTableID][analysisRuleType] = rule
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
	rules, ok := b.ctAnalysisRules[configuredTableID]
	if !ok {
		return nil, ErrNotFound
	}
	rule, ok := rules[analysisRuleType]
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
	rules, ok := b.ctAnalysisRules[configuredTableID]
	if !ok {
		return nil, ErrNotFound
	}
	rule, ok := rules[analysisRuleType]
	if !ok {
		return nil, ErrNotFound
	}
	rule.Policy = policy
	rule.UpdateTime = now()
	return rule, nil
}

func (b *InMemoryBackend) DeleteConfiguredTableAnalysisRule(
	configuredTableID, analysisRuleType string,
) error {
	b.mu.Lock("DeleteConfiguredTableAnalysisRule")
	defer b.mu.Unlock()
	rules, ok := b.ctAnalysisRules[configuredTableID]
	if !ok {
		return ErrNotFound
	}
	if _, ok := rules[analysisRuleType]; !ok {
		return ErrNotFound
	}
	delete(rules, analysisRuleType)
	if ct, ok := b.configuredTables[configuredTableID]; ok {
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
	mem, ok := b.memberships[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	ct, ok := b.configuredTables[configuredTableID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.ctAssociations[membershipID] == nil {
		b.ctAssociations[membershipID] = make(map[string]*ConfiguredTableAssociation)
	}
	id := uuid.NewString()
	ts := now()
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
	}
	b.ctAssociations[membershipID][id] = assoc
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
	assocs, ok := b.ctAssociations[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	assoc, ok := assocs[assocID]
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
	if _, ok := b.memberships[membershipID]; !ok {
		return nil, "", ErrNotFound
	}
	var items []*ConfiguredTableAssociationSummary
	for _, a := range b.ctAssociations[membershipID] {
		items = append(items, &ConfiguredTableAssociationSummary{
			ConfiguredTableAssociationIdentifier: a.ConfiguredTableAssociationIdentifier,
			Arn:                                  a.Arn,
			MembershipIdentifier:                 a.MembershipIdentifier,
			MembershipArn:                        a.MembershipArn,
			ConfiguredTableIdentifier:            a.ConfiguredTableIdentifier,
			Name:                                 a.Name,
			CreateTime:                           a.CreateTime,
			UpdateTime:                           a.UpdateTime,
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
	assocs, ok := b.ctAssociations[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	assoc, ok := assocs[assocID]
	if !ok {
		return nil, ErrNotFound
	}
	if description != "" {
		assoc.Description = description
	}
	if roleArn != "" {
		assoc.RoleArn = roleArn
	}
	assoc.UpdateTime = now()
	return assoc, nil
}

func (b *InMemoryBackend) DeleteConfiguredTableAssociation(membershipID, assocID string) error {
	b.mu.Lock("DeleteConfiguredTableAssociation")
	defer b.mu.Unlock()
	assocs, ok := b.ctAssociations[membershipID]
	if !ok {
		return ErrNotFound
	}
	assoc, ok := assocs[assocID]
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, assoc.Arn)
	delete(assocs, assocID)
	delete(b.ctaAnalysisRules, assocID)
	return nil
}

// ---- ConfiguredTableAssociationAnalysisRule ----

func (b *InMemoryBackend) CreateConfiguredTableAssociationAnalysisRule(
	membershipID, assocID, ruleType string,
	policy map[string]any,
) (*ConfiguredTableAssociationAnalysisRule, error) {
	b.mu.Lock("CreateConfiguredTableAssociationAnalysisRule")
	defer b.mu.Unlock()
	assocs, ok := b.ctAssociations[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	assoc, ok := assocs[assocID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.ctaAnalysisRules[assocID] == nil {
		b.ctaAnalysisRules[assocID] = make(map[string]*ConfiguredTableAssociationAnalysisRule)
	}
	if _, exists := b.ctaAnalysisRules[assocID][ruleType]; exists {
		return nil, ErrAlreadyExists
	}
	mem := b.memberships[membershipID]
	ts := now()
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
	b.ctaAnalysisRules[assocID][ruleType] = rule
	if !contains(assoc.AnalysisRuleTypes, ruleType) {
		assoc.AnalysisRuleTypes = append(assoc.AnalysisRuleTypes, ruleType)
	}
	return rule, nil
}

func (b *InMemoryBackend) GetConfiguredTableAssociationAnalysisRule(
	membershipID, assocID, ruleType string,
) (*ConfiguredTableAssociationAnalysisRule, error) {
	b.mu.RLock("GetConfiguredTableAssociationAnalysisRule")
	defer b.mu.RUnlock()
	rules, ok := b.ctaAnalysisRules[assocID]
	if !ok {
		return nil, ErrNotFound
	}
	rule, ok := rules[ruleType]
	if !ok {
		return nil, ErrNotFound
	}
	return rule, nil
}

func (b *InMemoryBackend) UpdateConfiguredTableAssociationAnalysisRule(
	membershipID, assocID, ruleType string,
	policy map[string]any,
) (*ConfiguredTableAssociationAnalysisRule, error) {
	b.mu.Lock("UpdateConfiguredTableAssociationAnalysisRule")
	defer b.mu.Unlock()
	rules, ok := b.ctaAnalysisRules[assocID]
	if !ok {
		return nil, ErrNotFound
	}
	rule, ok := rules[ruleType]
	if !ok {
		return nil, ErrNotFound
	}
	rule.Policy = policy
	rule.UpdateTime = now()
	return rule, nil
}

func (b *InMemoryBackend) DeleteConfiguredTableAssociationAnalysisRule(
	membershipID, assocID, ruleType string,
) error {
	b.mu.Lock("DeleteConfiguredTableAssociationAnalysisRule")
	defer b.mu.Unlock()
	rules, ok := b.ctaAnalysisRules[assocID]
	if !ok {
		return ErrNotFound
	}
	if _, ok := rules[ruleType]; !ok {
		return ErrNotFound
	}
	delete(rules, ruleType)
	if assocs, ok := b.ctAssociations[membershipID]; ok {
		if assoc, ok := assocs[assocID]; ok {
			assoc.AnalysisRuleTypes = removeFrom(assoc.AnalysisRuleTypes, ruleType)
		}
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
	mem, ok := b.memberships[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.analysisTemplates[membershipID] == nil {
		b.analysisTemplates[membershipID] = make(map[string]*AnalysisTemplate)
	}
	id := uuid.NewString()
	ts := now()
	collab := b.collaborations[mem.CollaborationIdentifier]
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
	}
	b.analysisTemplates[membershipID][id] = tmpl
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
	tmpls, ok := b.analysisTemplates[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	tmpl, ok := tmpls[templateID]
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
	if _, ok := b.memberships[membershipID]; !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.analysisTemplates[membershipID],
		nil,
		func(t *AnalysisTemplate) *AnalysisTemplateSummary {
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
			}
		},
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
	tmpls, ok := b.analysisTemplates[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	tmpl, ok := tmpls[templateID]
	if !ok {
		return nil, ErrNotFound
	}
	tmpl.Description = description
	tmpl.UpdateTime = now()
	return tmpl, nil
}

func (b *InMemoryBackend) DeleteAnalysisTemplate(membershipID, templateID string) error {
	b.mu.Lock("DeleteAnalysisTemplate")
	defer b.mu.Unlock()
	tmpls, ok := b.analysisTemplates[membershipID]
	if !ok {
		return ErrNotFound
	}
	tmpl, ok := tmpls[templateID]
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, tmpl.Arn)
	delete(tmpls, templateID)
	return nil
}

func (b *InMemoryBackend) GetCollaborationAnalysisTemplate(
	collaborationID, templateArn string,
) (*AnalysisTemplate, error) {
	b.mu.RLock("GetCollaborationAnalysisTemplate")
	defer b.mu.RUnlock()
	for _, tmpls := range b.analysisTemplates {
		for _, t := range tmpls {
			if t.CollaborationIdentifier == collaborationID && t.Arn == templateArn {
				return t, nil
			}
		}
	}
	return nil, ErrNotFound
}

func (b *InMemoryBackend) ListCollaborationAnalysisTemplates(
	collaborationID, maxResults, nextToken string,
) ([]*AnalysisTemplateSummary, string, error) {
	b.mu.RLock("ListCollaborationAnalysisTemplates")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations[collaborationID]; !ok {
		return nil, "", ErrNotFound
	}
	page, next := listNestedItems(
		b.analysisTemplates,
		func(t *AnalysisTemplate) bool { return t.CollaborationIdentifier == collaborationID },
		func(t *AnalysisTemplate) *AnalysisTemplateSummary {
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
			}
		},
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
	if _, ok := b.collaborations[collaborationID]; !ok {
		return nil, nil, ErrNotFound
	}
	var results []*AnalysisTemplate
	var errors []BatchError
	for _, arnStr := range templateArns {
		found := false
		for _, tmpls := range b.analysisTemplates {
			for _, t := range tmpls {
				if t.CollaborationIdentifier == collaborationID && t.Arn == arnStr {
					results = append(results, t)
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			errors = append(
				errors,
				BatchError{Arn: arnStr, Code: "ResourceNotFoundException", Message: "not found"},
			)
		}
	}
	return results, errors, nil
}

// ---- Schema ----

func (b *InMemoryBackend) GetSchema(collaborationID, name string) (*Schema, error) {
	b.mu.RLock("GetSchema")
	defer b.mu.RUnlock()
	schemas, ok := b.schemas[collaborationID]
	if !ok {
		return nil, ErrNotFound
	}
	s, ok := schemas[name]
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
	if _, ok := b.collaborations[collaborationID]; !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.schemas[collaborationID],
		func(s *Schema) bool { return schemaType == "" || s.Type == schemaType },
		func(s *Schema) *SchemaSummary {
			return &SchemaSummary{
				CollaborationArn:        s.CollaborationArn,
				CollaborationIdentifier: s.CollaborationIdentifier,
				CreatorAccountId:        s.CreatorAccountId,
				Name:                    s.Name,
				Type:                    s.Type,
				AnalysisRuleTypes:       s.AnalysisRuleTypes,
				AnalysisMethod:          s.AnalysisMethod,
				CreateTime:              s.CreateTime,
				UpdateTime:              s.UpdateTime,
			}
		},
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
	if _, ok := b.collaborations[collaborationID]; !ok {
		return nil, nil, ErrNotFound
	}
	var results []*Schema
	var errors []BatchError
	for _, name := range names {
		s, ok := b.schemas[collaborationID][name]
		if ok {
			results = append(results, s)
		} else {
			errors = append(errors, BatchError{Name: name, Code: "ResourceNotFoundException", Message: "not found"})
		}
	}
	return results, errors, nil
}

func (b *InMemoryBackend) GetSchemaAnalysisRule(
	collaborationID, name, ruleType string,
) (*SchemaAnalysisRule, error) {
	b.mu.RLock("GetSchemaAnalysisRule")
	defer b.mu.RUnlock()
	collabRules, ok := b.schemaAnalysisRules[collaborationID]
	if !ok {
		return nil, ErrNotFound
	}
	schemaRules, ok := collabRules[name]
	if !ok {
		return nil, ErrNotFound
	}
	rule, ok := schemaRules[ruleType]
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
	if _, ok := b.collaborations[collaborationID]; !ok {
		return nil, nil, ErrNotFound
	}
	var results []*SchemaAnalysisRule
	var errors []BatchError
	for _, name := range names {
		collabRules := b.schemaAnalysisRules[collaborationID]
		if collabRules != nil {
			if schemaRules, ok := collabRules[name]; ok {
				if rule, ok := schemaRules[ruleType]; ok {
					results = append(results, rule)
					continue
				}
			}
		}
		errors = append(
			errors,
			BatchError{Name: name, Code: "ResourceNotFoundException", Message: "not found"},
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
	mem, ok := b.memberships[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.protectedQueries[membershipID] == nil {
		b.protectedQueries[membershipID] = make(map[string]*ProtectedQuery)
	}
	id := uuid.NewString()
	ts := now()
	var sqlParams map[string]any
	if sqlText != "" {
		sqlParams = map[string]any{"queryString": sqlText}
	}
	q := &ProtectedQuery{
		Id:                   id,
		MembershipIdentifier: membershipID,
		MembershipArn:        mem.Arn,
		Status:               "STARTED",
		SqlParameters:        sqlParams,
		ResultConfiguration:  resultConfig,
		ComputeConfiguration: computeConfiguration,
		CreateTime:           ts,
	}
	b.protectedQueries[membershipID][id] = q
	return q, nil
}

func (b *InMemoryBackend) GetProtectedQuery(membershipID, queryID string) (*ProtectedQuery, error) {
	b.mu.RLock("GetProtectedQuery")
	defer b.mu.RUnlock()
	queries, ok := b.protectedQueries[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	q, ok := queries[queryID]
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
	if _, ok := b.memberships[membershipID]; !ok {
		return nil, "", ErrNotFound
	}
	var items []*ProtectedQuerySummary
	for _, q := range b.protectedQueries[membershipID] {
		if status != "" && q.Status != status {
			continue
		}
		items = append(items, &ProtectedQuerySummary{
			Id:                   q.Id,
			MembershipIdentifier: q.MembershipIdentifier,
			MembershipArn:        q.MembershipArn,
			Status:               q.Status,
			CreateTime:           q.CreateTime,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Id < items[j].Id })
	page, next := paginate(items, maxResults, nextToken)
	return page, next, nil
}

func (b *InMemoryBackend) UpdateProtectedQuery(
	membershipID, queryID, status string,
) (*ProtectedQuery, error) {
	b.mu.Lock("UpdateProtectedQuery")
	defer b.mu.Unlock()
	queries, ok := b.protectedQueries[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	q, ok := queries[queryID]
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
	mem, ok := b.memberships[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.protectedJobs[membershipID] == nil {
		b.protectedJobs[membershipID] = make(map[string]*ProtectedJob)
	}
	id := uuid.NewString()
	j := &ProtectedJob{
		Id:                   id,
		MembershipIdentifier: membershipID,
		MembershipArn:        mem.Arn,
		Status:               "STARTED",
		Type:                 jobType,
		JobParameters:        jobParameters,
		ResultConfiguration:  resultConfig,
		CreateTime:           now(),
	}
	b.protectedJobs[membershipID][id] = j
	return j, nil
}

func (b *InMemoryBackend) GetProtectedJob(membershipID, jobID string) (*ProtectedJob, error) {
	b.mu.RLock("GetProtectedJob")
	defer b.mu.RUnlock()
	jobs, ok := b.protectedJobs[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	j, ok := jobs[jobID]
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
	if _, ok := b.memberships[membershipID]; !ok {
		return nil, "", ErrNotFound
	}
	var items []*ProtectedJobSummary
	for _, j := range b.protectedJobs[membershipID] {
		if status != "" && j.Status != status {
			continue
		}
		items = append(items, &ProtectedJobSummary{
			Id:                   j.Id,
			MembershipIdentifier: j.MembershipIdentifier,
			MembershipArn:        j.MembershipArn,
			Status:               j.Status,
			Type:                 j.Type,
			CreateTime:           j.CreateTime,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Id < items[j].Id })
	page, next := paginate(items, maxResults, nextToken)
	return page, next, nil
}

func (b *InMemoryBackend) UpdateProtectedJob(
	membershipID, jobID, status string,
) (*ProtectedJob, error) {
	b.mu.Lock("UpdateProtectedJob")
	defer b.mu.Unlock()
	jobs, ok := b.protectedJobs[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	j, ok := jobs[jobID]
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
	mem, ok := b.memberships[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.privacyBudgetTemplates[membershipID] == nil {
		b.privacyBudgetTemplates[membershipID] = make(map[string]*PrivacyBudgetTemplate)
	}
	id := uuid.NewString()
	ts := now()
	collab := b.collaborations[mem.CollaborationIdentifier]
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
	}
	b.privacyBudgetTemplates[membershipID][id] = tmpl
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
	tmpls, ok := b.privacyBudgetTemplates[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	tmpl, ok := tmpls[templateID]
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
	if _, ok := b.memberships[membershipID]; !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.privacyBudgetTemplates[membershipID],
		func(t *PrivacyBudgetTemplate) bool {
			return privacyBudgetType == "" || t.PrivacyBudgetType == privacyBudgetType
		},
		func(t *PrivacyBudgetTemplate) *PrivacyBudgetTemplateSummary {
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
			}
		},
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
	tmpls, ok := b.privacyBudgetTemplates[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	tmpl, ok := tmpls[templateID]
	if !ok {
		return nil, ErrNotFound
	}
	if autoRefresh != "" {
		tmpl.AutoRefresh = autoRefresh
	}
	if parameters != nil {
		tmpl.Parameters = parameters
	}
	tmpl.UpdateTime = now()
	return tmpl, nil
}

func (b *InMemoryBackend) DeletePrivacyBudgetTemplate(membershipID, templateID string) error {
	b.mu.Lock("DeletePrivacyBudgetTemplate")
	defer b.mu.Unlock()
	tmpls, ok := b.privacyBudgetTemplates[membershipID]
	if !ok {
		return ErrNotFound
	}
	tmpl, ok := tmpls[templateID]
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, tmpl.Arn)
	delete(tmpls, templateID)
	return nil
}

func (b *InMemoryBackend) ListPrivacyBudgets(
	membershipID, privacyBudgetType, maxResults, nextToken string,
) ([]*PrivacyBudget, string, error) {
	b.mu.RLock("ListPrivacyBudgets")
	defer b.mu.RUnlock()
	if _, ok := b.memberships[membershipID]; !ok {
		return nil, "", ErrNotFound
	}
	return []*PrivacyBudget{}, "", nil
}

func (b *InMemoryBackend) ListCollaborationPrivacyBudgets(
	collaborationID, privacyBudgetType, maxResults, nextToken string,
) ([]*PrivacyBudget, string, error) {
	b.mu.RLock("ListCollaborationPrivacyBudgets")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations[collaborationID]; !ok {
		return nil, "", ErrNotFound
	}
	return []*PrivacyBudget{}, "", nil
}

func (b *InMemoryBackend) GetCollaborationPrivacyBudgetTemplate(
	collaborationID, templateID string,
) (*PrivacyBudgetTemplate, error) {
	b.mu.RLock("GetCollaborationPrivacyBudgetTemplate")
	defer b.mu.RUnlock()
	for _, tmpls := range b.privacyBudgetTemplates {
		for _, t := range tmpls {
			if t.CollaborationIdentifier == collaborationID &&
				t.PrivacyBudgetTemplateIdentifier == templateID {
				return t, nil
			}
		}
	}
	return nil, ErrNotFound
}

func (b *InMemoryBackend) ListCollaborationPrivacyBudgetTemplates(
	collaborationID, maxResults, nextToken string,
) ([]*PrivacyBudgetTemplateSummary, string, error) {
	b.mu.RLock("ListCollaborationPrivacyBudgetTemplates")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations[collaborationID]; !ok {
		return nil, "", ErrNotFound
	}
	page, next := listNestedItems(
		b.privacyBudgetTemplates,
		func(t *PrivacyBudgetTemplate) bool { return t.CollaborationIdentifier == collaborationID },
		func(t *PrivacyBudgetTemplate) *PrivacyBudgetTemplateSummary {
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
			}
		},
		func(a, c *PrivacyBudgetTemplateSummary) bool {
			return a.PrivacyBudgetTemplateIdentifier < c.PrivacyBudgetTemplateIdentifier
		},
		maxResults, nextToken,
	)
	return page, next, nil
}

func (b *InMemoryBackend) PreviewPrivacyImpact(
	membershipID string,
	parameters map[string]any,
) (map[string]any, error) {
	b.mu.RLock("PreviewPrivacyImpact")
	defer b.mu.RUnlock()
	if _, ok := b.memberships[membershipID]; !ok {
		return nil, ErrNotFound
	}
	return map[string]any{"privacyImpact": map[string]any{"aggregationCount": []any{}}}, nil
}

// ---- IdMappingTable ----

func (b *InMemoryBackend) CreateIdMappingTable(
	membershipID, name, description string,
	inputReferenceConfig map[string]any,
	kmsKeyArn string,
	tags map[string]string,
) (*IdMappingTable, error) {
	b.mu.Lock("CreateIdMappingTable")
	defer b.mu.Unlock()
	mem, ok := b.memberships[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.idMappingTables[membershipID] == nil {
		b.idMappingTables[membershipID] = make(map[string]*IdMappingTable)
	}
	id := uuid.NewString()
	ts := now()
	collab := b.collaborations[mem.CollaborationIdentifier]
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	t := &IdMappingTable{
		IdMappingTableIdentifier: id,
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
	}
	b.idMappingTables[membershipID][id] = t
	if len(tags) > 0 {
		b.tagsByArn[t.Arn] = maps.Clone(tags)
	}
	return t, nil
}

func (b *InMemoryBackend) GetIdMappingTable(membershipID, tableID string) (*IdMappingTable, error) {
	b.mu.RLock("GetIdMappingTable")
	defer b.mu.RUnlock()
	tables, ok := b.idMappingTables[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	t, ok := tables[tableID]
	if !ok {
		return nil, ErrNotFound
	}
	return t, nil
}

func (b *InMemoryBackend) ListIdMappingTables(
	membershipID, maxResults, nextToken string,
) ([]*IdMappingTableSummary, string, error) {
	b.mu.RLock("ListIdMappingTables")
	defer b.mu.RUnlock()
	if _, ok := b.memberships[membershipID]; !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.idMappingTables[membershipID],
		nil,
		func(t *IdMappingTable) *IdMappingTableSummary {
			return &IdMappingTableSummary{
				IdMappingTableIdentifier: t.IdMappingTableIdentifier,
				Arn:                      t.Arn,
				CollaborationArn:         t.CollaborationArn,
				CollaborationIdentifier:  t.CollaborationIdentifier,
				MembershipArn:            t.MembershipArn,
				MembershipIdentifier:     t.MembershipIdentifier,
				Name:                     t.Name,
				CreateTime:               t.CreateTime,
				UpdateTime:               t.UpdateTime,
			}
		},
		func(a, c *IdMappingTableSummary) bool {
			return a.IdMappingTableIdentifier < c.IdMappingTableIdentifier
		},
		maxResults, nextToken,
	)
	return page, next, nil
}

func (b *InMemoryBackend) UpdateIdMappingTable(
	membershipID, tableID, description, kmsKeyArn string,
) (*IdMappingTable, error) {
	b.mu.Lock("UpdateIdMappingTable")
	defer b.mu.Unlock()
	tables, ok := b.idMappingTables[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	t, ok := tables[tableID]
	if !ok {
		return nil, ErrNotFound
	}
	if description != "" {
		t.Description = description
	}
	if kmsKeyArn != "" {
		t.KmsKeyArn = kmsKeyArn
	}
	t.UpdateTime = now()
	return t, nil
}

func (b *InMemoryBackend) DeleteIdMappingTable(membershipID, tableID string) error {
	b.mu.Lock("DeleteIdMappingTable")
	defer b.mu.Unlock()
	tables, ok := b.idMappingTables[membershipID]
	if !ok {
		return ErrNotFound
	}
	t, ok := tables[tableID]
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, t.Arn)
	delete(tables, tableID)
	return nil
}

func (b *InMemoryBackend) PopulateIdMappingTable(
	membershipID, tableID string,
) (map[string]any, error) {
	b.mu.RLock("PopulateIdMappingTable")
	defer b.mu.RUnlock()
	if _, ok := b.idMappingTables[membershipID]; !ok {
		return nil, ErrNotFound
	}
	if _, ok := b.idMappingTables[membershipID][tableID]; !ok {
		return nil, ErrNotFound
	}
	return map[string]any{"mappedJobIdentifier": uuid.NewString()}, nil
}

// ---- IdNamespaceAssociation ----

func (b *InMemoryBackend) CreateIdNamespaceAssociation(
	membershipID, name, description string,
	inputReferenceConfig map[string]any,
	idMappingConfig map[string]any,
	tags map[string]string,
) (*IdNamespaceAssociation, error) {
	b.mu.Lock("CreateIdNamespaceAssociation")
	defer b.mu.Unlock()
	mem, ok := b.memberships[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.idNamespaceAssociations[membershipID] == nil {
		b.idNamespaceAssociations[membershipID] = make(map[string]*IdNamespaceAssociation)
	}
	id := uuid.NewString()
	ts := now()
	collab := b.collaborations[mem.CollaborationIdentifier]
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	assoc := &IdNamespaceAssociation{
		IdNamespaceAssociationIdentifier: id,
		Arn:                              b.idNamespaceAssocARN(membershipID, id),
		CollaborationArn:                 collabArn,
		CollaborationIdentifier:          mem.CollaborationIdentifier,
		MembershipArn:                    mem.Arn,
		MembershipIdentifier:             membershipID,
		Name:                             name,
		Description:                      description,
		InputReferenceConfig:             inputReferenceConfig,
		IdMappingConfig:                  idMappingConfig,
		CreateTime:                       ts,
		UpdateTime:                       ts,
		Tags:                             tags,
	}
	b.idNamespaceAssociations[membershipID][id] = assoc
	if len(tags) > 0 {
		b.tagsByArn[assoc.Arn] = maps.Clone(tags)
	}
	return assoc, nil
}

func (b *InMemoryBackend) GetIdNamespaceAssociation(
	membershipID, assocID string,
) (*IdNamespaceAssociation, error) {
	b.mu.RLock("GetIdNamespaceAssociation")
	defer b.mu.RUnlock()
	assocs, ok := b.idNamespaceAssociations[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	assoc, ok := assocs[assocID]
	if !ok {
		return nil, ErrNotFound
	}
	return assoc, nil
}

func (b *InMemoryBackend) ListIdNamespaceAssociations(
	membershipID, maxResults, nextToken string,
) ([]*IdNamespaceAssociationSummary, string, error) {
	b.mu.RLock("ListIdNamespaceAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.memberships[membershipID]; !ok {
		return nil, "", ErrNotFound
	}
	var items []*IdNamespaceAssociationSummary
	for _, a := range b.idNamespaceAssociations[membershipID] {
		items = append(items, &IdNamespaceAssociationSummary{
			IdNamespaceAssociationIdentifier: a.IdNamespaceAssociationIdentifier,
			Arn:                              a.Arn,
			CollaborationArn:                 a.CollaborationArn,
			CollaborationIdentifier:          a.CollaborationIdentifier,
			MembershipArn:                    a.MembershipArn,
			MembershipIdentifier:             a.MembershipIdentifier,
			Name:                             a.Name,
			CreateTime:                       a.CreateTime,
			UpdateTime:                       a.UpdateTime,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].IdNamespaceAssociationIdentifier < items[j].IdNamespaceAssociationIdentifier
	})
	page, next := paginate(items, maxResults, nextToken)
	return page, next, nil
}

func (b *InMemoryBackend) UpdateIdNamespaceAssociation(
	membershipID, assocID, description string,
	idMappingConfig map[string]any,
) (*IdNamespaceAssociation, error) {
	b.mu.Lock("UpdateIdNamespaceAssociation")
	defer b.mu.Unlock()
	assocs, ok := b.idNamespaceAssociations[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	assoc, ok := assocs[assocID]
	if !ok {
		return nil, ErrNotFound
	}
	if description != "" {
		assoc.Description = description
	}
	if idMappingConfig != nil {
		assoc.IdMappingConfig = idMappingConfig
	}
	assoc.UpdateTime = now()
	return assoc, nil
}

func (b *InMemoryBackend) DeleteIdNamespaceAssociation(membershipID, assocID string) error {
	b.mu.Lock("DeleteIdNamespaceAssociation")
	defer b.mu.Unlock()
	assocs, ok := b.idNamespaceAssociations[membershipID]
	if !ok {
		return ErrNotFound
	}
	assoc, ok := assocs[assocID]
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, assoc.Arn)
	delete(assocs, assocID)
	return nil
}

func (b *InMemoryBackend) GetCollaborationIdNamespaceAssociation(
	collaborationID, assocID string,
) (*IdNamespaceAssociation, error) {
	b.mu.RLock("GetCollaborationIdNamespaceAssociation")
	defer b.mu.RUnlock()
	for _, assocs := range b.idNamespaceAssociations {
		for _, a := range assocs {
			if a.CollaborationIdentifier == collaborationID &&
				a.IdNamespaceAssociationIdentifier == assocID {
				return a, nil
			}
		}
	}
	return nil, ErrNotFound
}

func (b *InMemoryBackend) ListCollaborationIdNamespaceAssociations(
	collaborationID, maxResults, nextToken string,
) ([]*IdNamespaceAssociationSummary, string, error) {
	b.mu.RLock("ListCollaborationIdNamespaceAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations[collaborationID]; !ok {
		return nil, "", ErrNotFound
	}
	var items []*IdNamespaceAssociationSummary
	for _, assocs := range b.idNamespaceAssociations {
		for _, a := range assocs {
			if a.CollaborationIdentifier == collaborationID {
				items = append(items, &IdNamespaceAssociationSummary{
					IdNamespaceAssociationIdentifier: a.IdNamespaceAssociationIdentifier,
					Arn:                              a.Arn,
					CollaborationArn:                 a.CollaborationArn,
					CollaborationIdentifier:          a.CollaborationIdentifier,
					MembershipArn:                    a.MembershipArn,
					MembershipIdentifier:             a.MembershipIdentifier,
					Name:                             a.Name,
					CreateTime:                       a.CreateTime,
					UpdateTime:                       a.UpdateTime,
				})
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].IdNamespaceAssociationIdentifier < items[j].IdNamespaceAssociationIdentifier
	})
	page, next := paginate(items, maxResults, nextToken)
	return page, next, nil
}

// ---- ConfiguredAudienceModelAssociation ----

func (b *InMemoryBackend) CreateConfiguredAudienceModelAssociation(
	membershipID, configuredAudienceModelArn, name, description string,
	manageResourcePolicies bool,
	tags map[string]string,
) (*ConfiguredAudienceModelAssociation, error) {
	b.mu.Lock("CreateConfiguredAudienceModelAssociation")
	defer b.mu.Unlock()
	mem, ok := b.memberships[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.camaAssociations[membershipID] == nil {
		b.camaAssociations[membershipID] = make(map[string]*ConfiguredAudienceModelAssociation)
	}
	id := uuid.NewString()
	ts := now()
	collab := b.collaborations[mem.CollaborationIdentifier]
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
	}
	b.camaAssociations[membershipID][id] = assoc
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
	assocs, ok := b.camaAssociations[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	assoc, ok := assocs[assocID]
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
	if _, ok := b.memberships[membershipID]; !ok {
		return nil, "", ErrNotFound
	}
	var items []*ConfiguredAudienceModelAssociationSummary
	for _, a := range b.camaAssociations[membershipID] {
		items = append(items, &ConfiguredAudienceModelAssociationSummary{
			ConfiguredAudienceModelAssociationIdentifier: a.ConfiguredAudienceModelAssociationIdentifier,
			Arn:                     a.Arn,
			CollaborationArn:        a.CollaborationArn,
			CollaborationIdentifier: a.CollaborationIdentifier,
			MembershipArn:           a.MembershipArn,
			MembershipIdentifier:    a.MembershipIdentifier,
			Name:                    a.Name,
			CreateTime:              a.CreateTime,
			UpdateTime:              a.UpdateTime,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].ConfiguredAudienceModelAssociationIdentifier < items[j].ConfiguredAudienceModelAssociationIdentifier
	})
	page, next := paginate(items, maxResults, nextToken)
	return page, next, nil
}

func (b *InMemoryBackend) UpdateConfiguredAudienceModelAssociation(
	membershipID, assocID, name, description string,
) (*ConfiguredAudienceModelAssociation, error) {
	b.mu.Lock("UpdateConfiguredAudienceModelAssociation")
	defer b.mu.Unlock()
	assocs, ok := b.camaAssociations[membershipID]
	if !ok {
		return nil, ErrNotFound
	}
	assoc, ok := assocs[assocID]
	if !ok {
		return nil, ErrNotFound
	}
	if name != "" {
		assoc.Name = name
	}
	if description != "" {
		assoc.Description = description
	}
	assoc.UpdateTime = now()
	return assoc, nil
}

func (b *InMemoryBackend) DeleteConfiguredAudienceModelAssociation(
	membershipID, assocID string,
) error {
	b.mu.Lock("DeleteConfiguredAudienceModelAssociation")
	defer b.mu.Unlock()
	assocs, ok := b.camaAssociations[membershipID]
	if !ok {
		return ErrNotFound
	}
	assoc, ok := assocs[assocID]
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, assoc.Arn)
	delete(assocs, assocID)
	return nil
}

func (b *InMemoryBackend) GetCollaborationConfiguredAudienceModelAssociation(
	collaborationID, assocID string,
) (*ConfiguredAudienceModelAssociation, error) {
	b.mu.RLock("GetCollaborationConfiguredAudienceModelAssociation")
	defer b.mu.RUnlock()
	for _, assocs := range b.camaAssociations {
		for _, a := range assocs {
			if a.CollaborationIdentifier == collaborationID &&
				a.ConfiguredAudienceModelAssociationIdentifier == assocID {
				return a, nil
			}
		}
	}
	return nil, ErrNotFound
}

func (b *InMemoryBackend) ListCollaborationConfiguredAudienceModelAssociations(
	collaborationID, maxResults, nextToken string,
) ([]*ConfiguredAudienceModelAssociationSummary, string, error) {
	b.mu.RLock("ListCollaborationConfiguredAudienceModelAssociations")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations[collaborationID]; !ok {
		return nil, "", ErrNotFound
	}
	var items []*ConfiguredAudienceModelAssociationSummary
	for _, assocs := range b.camaAssociations {
		for _, a := range assocs {
			if a.CollaborationIdentifier == collaborationID {
				items = append(items, &ConfiguredAudienceModelAssociationSummary{
					ConfiguredAudienceModelAssociationIdentifier: a.ConfiguredAudienceModelAssociationIdentifier,
					Arn:                     a.Arn,
					CollaborationArn:        a.CollaborationArn,
					CollaborationIdentifier: a.CollaborationIdentifier,
					MembershipArn:           a.MembershipArn,
					MembershipIdentifier:    a.MembershipIdentifier,
					Name:                    a.Name,
					CreateTime:              a.CreateTime,
					UpdateTime:              a.UpdateTime,
				})
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].ConfiguredAudienceModelAssociationIdentifier < items[j].ConfiguredAudienceModelAssociationIdentifier
	})
	page, next := paginate(items, maxResults, nextToken)
	return page, next, nil
}

// ---- CollaborationChangeRequest ----

func (b *InMemoryBackend) CreateCollaborationChangeRequest(
	collaborationID, changeRequestType string,
	details map[string]any,
) (*CollaborationChangeRequest, error) {
	b.mu.Lock("CreateCollaborationChangeRequest")
	defer b.mu.Unlock()
	collab, ok := b.collaborations[collaborationID]
	if !ok {
		return nil, ErrNotFound
	}
	if b.changeRequests[collaborationID] == nil {
		b.changeRequests[collaborationID] = make(map[string]*CollaborationChangeRequest)
	}
	id := uuid.NewString()
	ts := now()
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
	b.changeRequests[collaborationID][id] = req
	return req, nil
}

func (b *InMemoryBackend) GetCollaborationChangeRequest(
	collaborationID, changeRequestID string,
) (*CollaborationChangeRequest, error) {
	b.mu.RLock("GetCollaborationChangeRequest")
	defer b.mu.RUnlock()
	reqs, ok := b.changeRequests[collaborationID]
	if !ok {
		return nil, ErrNotFound
	}
	req, ok := reqs[changeRequestID]
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
	if _, ok := b.collaborations[collaborationID]; !ok {
		return nil, "", ErrNotFound
	}
	var items []*CollaborationChangeRequest
	for _, r := range b.changeRequests[collaborationID] {
		items = append(items, r)
	}
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
	reqs, ok := b.changeRequests[collaborationID]
	if !ok {
		return nil, ErrNotFound
	}
	req, ok := reqs[changeRequestID]
	if !ok {
		return nil, ErrNotFound
	}
	req.Status = status
	req.UpdateTime = now()
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
	for k, v := range tags {
		b.tagsByArn[resourceArn][k] = v
	}
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
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
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

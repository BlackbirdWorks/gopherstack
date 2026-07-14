package serverlessrepo

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// validNameRe matches AWS SAR-valid application names: alphanumeric and hyphens only.
var validNameRe = regexp.MustCompile(`^[a-zA-Z0-9\-]+$`)

// validSemanticVersionRe matches a basic semver prefix (major.minor.patch).
var validSemanticVersionRe = regexp.MustCompile(`^\d+\.\d+\.\d+`)

const (
	// templateStatusActive is the status of an active CloudFormation template.
	templateStatusActive = "ACTIVE"
	// templateExpirationHours is the number of hours before a template expires.
	templateExpirationHours = 1

	// AWS SAR field length limits.
	maxNameLength            = 140
	maxAuthorLength          = 127
	maxDescriptionLength     = 256
	maxLabelLength           = 127
	maxLabelCount            = 10
	maxSemanticVersionLength = 255
)

var (
	// ErrApplicationNotFound is returned when an application does not exist.
	ErrApplicationNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrApplicationAlreadyExists is returned when an application already exists.
	ErrApplicationAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrTemplateNotFound is returned when a CloudFormation template does not exist.
	ErrTemplateNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrVersionAlreadyExists is returned when an application version already exists.
	ErrVersionAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrValidation is returned when a request contains an invalid or missing parameter.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

// ParameterDefinition represents a CloudFormation parameter definition for an application version.
type ParameterDefinition struct {
	DefaultValue          string   `json:"defaultValue,omitempty"`
	Description           string   `json:"description,omitempty"`
	Name                  string   `json:"name"`
	Type                  string   `json:"type,omitempty"`
	ReferencedByResources []string `json:"referencedByResources"`
	AllowedValues         []string `json:"allowedValues,omitempty"`
	NoEcho                bool     `json:"noEcho,omitempty"`
}

// Application represents an AWS Serverless Application Repository application.
type Application struct {
	CreationTime      time.Time `json:"creationTime"`
	LicenseURL        string    `json:"licenseUrl,omitempty"`
	Name              string    `json:"name"`
	Description       string    `json:"description,omitempty"`
	Author            string    `json:"author,omitempty"`
	HomePageURL       string    `json:"homePageUrl,omitempty"`
	ApplicationID     string    `json:"applicationId"`
	ReadmeURL         string    `json:"readmeUrl,omitempty"`
	SpdxLicenseID     string    `json:"spdxLicenseId,omitempty"`
	SourceCodeURL     string    `json:"sourceCodeUrl,omitempty"`
	SemanticVersion   string    `json:"semanticVersion,omitempty"`
	VerifiedAuthorURL string    `json:"verifiedAuthorUrl,omitempty"`
	Labels            []string  `json:"labels,omitempty"`
	IsVerifiedAuthor  bool      `json:"isVerifiedAuthor"`
}

// ApplicationVersion represents a version of a Serverless Application Repository application.
type ApplicationVersion struct {
	CreationTime         time.Time `json:"creationTime"`
	ApplicationID        string    `json:"applicationId"`
	SemanticVersion      string    `json:"semanticVersion"`
	SourceCodeURL        string    `json:"sourceCodeUrl,omitempty"`
	SourceCodeArchiveURL string    `json:"sourceCodeArchiveUrl,omitempty"`
	TemplateURL          string    `json:"templateUrl,omitempty"`
	// AppName identifies the owning application. It exists purely so the
	// flattened store.Table[ApplicationVersion] (see store_setup.go; this
	// collection was previously nested appName -> semanticVersion -> *version)
	// can derive its composite "appName#semanticVersion" key from the value
	// alone -- SemanticVersion is unique only within an application, not
	// globally. It is not part of the Serverless Application Repository wire
	// API, hence json:"-".
	AppName              string                `json:"-"`
	ParameterDefinitions []ParameterDefinition `json:"parameterDefinitions"`
	RequiredCapabilities []string              `json:"requiredCapabilities"`
	ResourcesSupported   bool                  `json:"resourcesSupported"`
}

// CloudFormationTemplate represents a CloudFormation template for an application.
type CloudFormationTemplate struct {
	CreationTime    time.Time `json:"creationTime"`
	ExpirationTime  time.Time `json:"expirationTime"`
	ApplicationID   string    `json:"applicationId"`
	TemplateID      string    `json:"templateId"`
	SemanticVersion string    `json:"semanticVersion,omitempty"`
	Status          string    `json:"status"`
	TemplateURL     string    `json:"templateUrl,omitempty"`
	// AppName identifies the owning application. TemplateID is already
	// globally unique (it is generated as "<appName>-<unixNano>"), so it
	// remains the store.Table[CloudFormationTemplate] primary key (see
	// store_setup.go); AppName exists purely to drive the additive "byApp"
	// secondary index used for DeleteApplication's cascade delete. It is not
	// part of the Serverless Application Repository wire API, hence json:"-".
	AppName string `json:"-"`
}

// CloudFormationChangeSet represents a CloudFormation change set for an application.
type CloudFormationChangeSet struct {
	ApplicationID   string `json:"applicationId"`
	ChangeSetID     string `json:"changeSetId"`
	SemanticVersion string `json:"semanticVersion,omitempty"`
	StackID         string `json:"stackId"`
	// AppName identifies the owning application. It exists purely so the
	// flattened store.Table[CloudFormationChangeSet] (see store_setup.go;
	// this collection was previously nested appName -> changeSetID -> *cs)
	// can derive its composite "appName#changeSetID" key from the value
	// alone -- ChangeSetID is derived from a caller-supplied changeSetName/
	// stackName and is unique only within an application, not globally. It is
	// not part of the Serverless Application Repository wire API, hence
	// json:"-".
	AppName      string   `json:"-"`
	Capabilities []string `json:"capabilities,omitempty"`
	Tags         []Tag    `json:"tags,omitempty"`
}

// ApplicationPolicyStatement represents a policy statement for an application.
type ApplicationPolicyStatement struct {
	StatementID     string   `json:"statementId,omitempty"`
	Actions         []string `json:"actions"`
	Principals      []string `json:"principals"`
	PrincipalOrgIDs []string `json:"principalOrgIDs,omitempty"`
}

// ApplicationDependency represents a nested application dependency.
type ApplicationDependency struct {
	ApplicationID   string `json:"applicationId"`
	SemanticVersion string `json:"semanticVersion"`
}

// Tag represents a CloudFormation tag passed while deploying an application.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// CreateApplicationVersionOptions contains optional AWS SAR version inputs.
type CreateApplicationVersionOptions struct {
	SourceCodeURL        string
	SourceCodeArchiveURL string
	TemplateURL          string
}

// CreateCloudFormationChangeSetOptions contains optional deployment metadata.
type CreateCloudFormationChangeSetOptions struct {
	Capabilities []string
	Tags         []Tag
}

// cloneApplication returns a deep copy of a, including its Labels slice.
func cloneApplication(a *Application) *Application {
	cp := *a
	cp.Labels = cloneStringSlice(a.Labels)

	return &cp
}

// cloneVersion returns a deep copy of v, including slice fields.
func cloneVersion(v *ApplicationVersion) *ApplicationVersion {
	cp := *v
	cp.RequiredCapabilities = cloneStringSlice(v.RequiredCapabilities)
	cp.ParameterDefinitions = cloneParameterDefinitions(v.ParameterDefinitions)

	return &cp
}

func cloneTags(tags []Tag) []Tag {
	if tags == nil {
		return nil
	}

	return append([]Tag(nil), tags...)
}

func cloneChangeSet(cs *CloudFormationChangeSet) *CloudFormationChangeSet {
	cp := *cs
	cp.Capabilities = cloneStringSlice(cs.Capabilities)
	cp.Tags = cloneTags(cs.Tags)

	return &cp
}

// cloneStringSlice returns a copy of a string slice, returning nil for nil input.
func cloneStringSlice(s []string) []string {
	if s == nil {
		return nil
	}

	result := make([]string, len(s))
	copy(result, s)

	return result
}

// nonNilStringSlice returns an empty slice for nil input.
func nonNilStringSlice(s []string) []string {
	if s != nil {
		return s
	}

	return []string{}
}

// cloneParameterDefinitions returns a deep copy of a ParameterDefinition slice,
// returning an empty (non-nil) slice for nil input.
func cloneParameterDefinitions(defs []ParameterDefinition) []ParameterDefinition {
	if defs == nil {
		return []ParameterDefinition{}
	}

	out := make([]ParameterDefinition, len(defs))
	for i, d := range defs {
		out[i] = d
		out[i].AllowedValues = cloneStringSlice(d.AllowedValues)
		out[i].ReferencedByResources = cloneStringSlice(d.ReferencedByResources)
	}

	return out
}

// cloneTemplate returns a deep copy of t.
func cloneTemplate(t *CloudFormationTemplate) *CloudFormationTemplate {
	cp := *t

	return &cp
}

// clonePolicyStatement returns a deep copy of s.
func clonePolicyStatement(s *ApplicationPolicyStatement) *ApplicationPolicyStatement {
	return &ApplicationPolicyStatement{
		StatementID:     s.StatementID,
		Actions:         cloneStringSlice(s.Actions),
		Principals:      cloneStringSlice(s.Principals),
		PrincipalOrgIDs: cloneStringSlice(s.PrincipalOrgIDs),
	}
}

// clonePolicyStatements returns deep copies of all policy statements.
// Returns an empty (non-nil) slice when stmts is nil.
func clonePolicyStatements(stmts []*ApplicationPolicyStatement) []*ApplicationPolicyStatement {
	out := make([]*ApplicationPolicyStatement, len(stmts))
	for i, s := range stmts {
		out[i] = clonePolicyStatement(s)
	}

	return out
}

// InMemoryBackend is an in-memory store for Serverless Application Repository resources.
//
// applications is a "clean" store.Table (registered directly on registry --
// see store_setup.go). appVersions, cfTemplates, and cfChangeSets are "dirty"
// tables: each was previously a map nested under appName, and each gained a
// hidden AppName field purely to key (and, for cfTemplates, index) itself --
// see the doc comments on those fields in this file. Because that field is
// json:"-", persistence.go round-trips them through an ephemeral DTO
// registry instead of registry.SnapshotAll()/RestoreAll() directly.
//
// appPolicies (map[string][]*ApplicationPolicyStatement) and appDependencies
// (map[string]map[string][]*ApplicationDependency) are left as plain maps:
// their values are slices, not *T, so they do not fit store.Table's
// keyed-by-identity-value shape. appPolicies is additionally order-sensitive
// (PutApplicationPolicy replaces the slice wholesale and GetApplicationPolicy
// returns it in that same order).
type InMemoryBackend struct {
	registry     *store.Registry
	applications *store.Table[Application]

	appVersions      *store.Table[ApplicationVersion]
	appVersionsByApp *store.Index[ApplicationVersion]

	cfTemplates      *store.Table[CloudFormationTemplate]
	cfTemplatesByApp *store.Index[CloudFormationTemplate]

	cfChangeSets      *store.Table[CloudFormationChangeSet]
	cfChangeSetsByApp *store.Index[CloudFormationChangeSet]

	// appPolicies maps appName -> []*ApplicationPolicyStatement
	appPolicies map[string][]*ApplicationPolicyStatement
	// appDependencies maps appName -> semanticVersion -> dependencies.
	appDependencies map[string]map[string][]*ApplicationDependency
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
}

// NewInMemoryBackend creates a new in-memory Serverless Application Repository backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:        store.NewRegistry(),
		appPolicies:     make(map[string][]*ApplicationPolicyStatement),
		appDependencies: make(map[string]map[string][]*ApplicationDependency),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("serverlessrepo"),
	}
	registerAllTables(b)

	return b
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetTablesLocked()
	b.appPolicies = make(map[string][]*ApplicationPolicyStatement)
	b.appDependencies = make(map[string]map[string][]*ApplicationDependency)
}

// resetTablesLocked resets every store.Table-backed field: applications via
// the registry (the only "clean" table), and the three "dirty" tables
// individually since they are deliberately not registered on b.registry --
// see the InMemoryBackend doc comment. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) resetTablesLocked() {
	b.registry.ResetAll()
	b.appVersions.Reset()
	b.cfTemplates.Reset()
	b.cfChangeSets.Reset()
}

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AddApplicationInternal creates an application directly in the backend, bypassing
// validation and HTTP. Useful for seeding test state.
func (b *InMemoryBackend) AddApplicationInternal(name, description, author string) *Application {
	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	appARN := arn.Build("serverlessrepo", b.region, b.accountID, "applications/"+name)
	a := &Application{
		ApplicationID: appARN,
		Name:          name,
		Description:   description,
		Author:        author,
		CreationTime:  time.Now(),
	}
	b.applications.Put(a)

	return cloneApplication(a)
}

// AddVersionInternal creates a version directly in the backend, bypassing validation.
// Useful for seeding test state. The application must already exist.
func (b *InMemoryBackend) AddVersionInternal(appName, semanticVersion string) *ApplicationVersion {
	b.mu.Lock("AddVersionInternal")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(appName)
	if !ok {
		return nil
	}

	v := &ApplicationVersion{
		ApplicationID:        app.ApplicationID,
		SemanticVersion:      semanticVersion,
		AppName:              appName,
		CreationTime:         time.Now(),
		ParameterDefinitions: []ParameterDefinition{},
		RequiredCapabilities: []string{},
		ResourcesSupported:   true,
	}
	b.appVersions.Put(v)

	return cloneVersion(v)
}

// isValidSemanticVersion returns true if v looks like a semver string (major.minor.patch prefix)
// and does not exceed the AWS SAR maximum length.
func isValidSemanticVersion(v string) bool {
	return len(v) <= maxSemanticVersionLength && validSemanticVersionRe.MatchString(v)
}

// validateLabels checks that the label slice satisfies AWS SAR constraints.
func validateLabels(labels []string) error {
	if len(labels) > maxLabelCount {
		return fmt.Errorf("%w: at most %d labels are allowed", ErrValidation, maxLabelCount)
	}

	for i, l := range labels {
		if l == "" {
			return fmt.Errorf("%w: label %d must not be empty", ErrValidation, i)
		}

		if len(l) > maxLabelLength {
			return fmt.Errorf(
				"%w: label %d must be at most %d characters", ErrValidation, i, maxLabelLength,
			)
		}
	}

	return nil
}

// CreateApplication creates a new application.
func (b *InMemoryBackend) CreateApplication(
	name string,
	description string,
	author string,
	sourceCodeURL string,
	semanticVersion string,
	labels []string,
	homePageURL string,
	licenseURL string,
	spdxLicenseID string,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if !validNameRe.MatchString(name) {
		return nil, fmt.Errorf("%w: name must contain only alphanumeric characters and hyphens", ErrValidation)
	}

	if len(name) > maxNameLength {
		return nil, fmt.Errorf("%w: name must be at most %d characters", ErrValidation, maxNameLength)
	}

	if author == "" {
		return nil, fmt.Errorf("%w: author is required", ErrValidation)
	}

	if len(author) > maxAuthorLength {
		return nil, fmt.Errorf("%w: author must be at most %d characters", ErrValidation, maxAuthorLength)
	}

	if description == "" {
		return nil, fmt.Errorf("%w: description is required", ErrValidation)
	}

	if len(description) > maxDescriptionLength {
		return nil, fmt.Errorf("%w: description must be at most %d characters", ErrValidation, maxDescriptionLength)
	}

	if semanticVersion != "" && !isValidSemanticVersion(semanticVersion) {
		return nil, fmt.Errorf("%w: semanticVersion must be a valid semantic version (e.g. 1.0.0)", ErrValidation)
	}

	if err := validateLabels(labels); err != nil {
		return nil, err
	}

	if b.applications.Has(name) {
		return nil, fmt.Errorf("%w: application %s already exists", ErrApplicationAlreadyExists, name)
	}

	appARN := arn.Build("serverlessrepo", b.region, b.accountID, "applications/"+name)

	a := &Application{
		ApplicationID:   appARN,
		Name:            name,
		Description:     description,
		Author:          author,
		SourceCodeURL:   sourceCodeURL,
		SemanticVersion: semanticVersion,
		HomePageURL:     homePageURL,
		LicenseURL:      licenseURL,
		SpdxLicenseID:   spdxLicenseID,
		CreationTime:    time.Now(),
		Labels:          nonNilStringSlice(cloneStringSlice(labels)),
	}
	b.applications.Put(a)

	return cloneApplication(a), nil
}

// SetApplicationReadmeURL stores readme metadata supplied during application creation.
func (b *InMemoryBackend) SetApplicationReadmeURL(name, readmeURL string) (*Application, error) {
	b.mu.Lock("SetApplicationReadmeURL")
	defer b.mu.Unlock()

	a, ok := b.applications.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	a.ReadmeURL = readmeURL

	return cloneApplication(a), nil
}

// GetApplication returns an application by name.
func (b *InMemoryBackend) GetApplication(name string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	a, ok := b.applications.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	return cloneApplication(a), nil
}

// ListApplications returns all applications sorted by name.
func (b *InMemoryBackend) ListApplications() []*Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	// Table.Snapshot returns entries ordered by key ascending; the table is
	// keyed by Application.Name (see store_setup.go), which is exactly the
	// sort order this method has always returned.
	snap := b.applications.Snapshot()
	list := make([]*Application, 0, len(snap))

	for _, a := range snap {
		list = append(list, cloneApplication(a))
	}

	return list
}

// UpdateApplication updates the description or author of an existing application.
func (b *InMemoryBackend) UpdateApplication(
	name, description, author, homePageURL, readmeURL string,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	a, ok := b.applications.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	if description != "" {
		if len(description) > maxDescriptionLength {
			return nil, fmt.Errorf("%w: description must be at most %d characters", ErrValidation, maxDescriptionLength)
		}

		a.Description = description
	}

	if author != "" {
		if len(author) > maxAuthorLength {
			return nil, fmt.Errorf("%w: author must be at most %d characters", ErrValidation, maxAuthorLength)
		}

		a.Author = author
	}

	if homePageURL != "" {
		a.HomePageURL = homePageURL
	}

	if readmeURL != "" {
		a.ReadmeURL = readmeURL
	}

	return cloneApplication(a), nil
}

// UpdateApplicationLabels replaces labels supplied by UpdateApplication.
func (b *InMemoryBackend) UpdateApplicationLabels(name string, labels []string) (*Application, error) {
	b.mu.Lock("UpdateApplicationLabels")
	defer b.mu.Unlock()

	a, ok := b.applications.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	if err := validateLabels(labels); err != nil {
		return nil, err
	}

	a.Labels = nonNilStringSlice(cloneStringSlice(labels))

	return cloneApplication(a), nil
}

// DeleteApplication deletes an application by name, cascading to every
// version, CloudFormation template, and CloudFormation change set owned by
// it. The byApp index results are cloned before the delete loops since
// deleting from the underlying table mutates the index's backing slice.
func (b *InMemoryBackend) DeleteApplication(name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if !b.applications.Has(name) {
		return fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	b.applications.Delete(name)

	for _, v := range slices.Clone(b.appVersionsByApp.Get(name)) {
		b.appVersions.Delete(versionKey(v.AppName, v.SemanticVersion))
	}

	for _, t := range slices.Clone(b.cfTemplatesByApp.Get(name)) {
		b.cfTemplates.Delete(t.TemplateID)
	}

	for _, cs := range slices.Clone(b.cfChangeSetsByApp.Get(name)) {
		b.cfChangeSets.Delete(changeSetKey(cs.AppName, cs.ChangeSetID))
	}

	delete(b.appPolicies, name)
	delete(b.appDependencies, name)

	return nil
}

// CreateApplicationVersion creates a new version for an application.
func (b *InMemoryBackend) CreateApplicationVersion(
	appName string,
	semanticVersion string,
	sourceCodeURL string,
	templateURL string,
) (*ApplicationVersion, error) {
	return b.CreateApplicationVersionWithOptions(appName, semanticVersion, CreateApplicationVersionOptions{
		SourceCodeURL: sourceCodeURL,
		TemplateURL:   templateURL,
	})
}

// CreateApplicationVersionWithOptions creates a version including optional archive metadata.
func (b *InMemoryBackend) CreateApplicationVersionWithOptions(
	appName string,
	semanticVersion string,
	opts CreateApplicationVersionOptions,
) (*ApplicationVersion, error) {
	b.mu.Lock("CreateApplicationVersion")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(appName)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	if semanticVersion == "" {
		return nil, fmt.Errorf("%w: semanticVersion is required", ErrValidation)
	}

	if !isValidSemanticVersion(semanticVersion) {
		return nil, fmt.Errorf("%w: semanticVersion must be a valid semantic version (e.g. 1.0.0)", ErrValidation)
	}

	if opts.SourceCodeURL == "" && opts.SourceCodeArchiveURL == "" && opts.TemplateURL == "" {
		return nil, fmt.Errorf(
			"%w: at least one of sourceCodeUrl, sourceCodeArchiveUrl or templateUrl is required",
			ErrValidation,
		)
	}

	if b.appVersions.Has(versionKey(appName, semanticVersion)) {
		return nil, fmt.Errorf(
			"%w: version %s already exists for application %q",
			ErrVersionAlreadyExists,
			semanticVersion,
			appName,
		)
	}

	// Generate a synthetic template URL when the caller provides only a sourceCodeURL.
	resolvedTemplateURL := opts.TemplateURL
	if resolvedTemplateURL == "" && (opts.SourceCodeURL != "" || opts.SourceCodeArchiveURL != "") {
		resolvedTemplateURL = fmt.Sprintf(
			"https://s3.amazonaws.com/serverlessrepo-templates/%s/%s.template",
			appName,
			semanticVersion,
		)
	}

	v := &ApplicationVersion{
		ApplicationID:        app.ApplicationID,
		SemanticVersion:      semanticVersion,
		AppName:              appName,
		SourceCodeURL:        opts.SourceCodeURL,
		SourceCodeArchiveURL: opts.SourceCodeArchiveURL,
		TemplateURL:          resolvedTemplateURL,
		CreationTime:         time.Now(),
		ParameterDefinitions: []ParameterDefinition{},
		RequiredCapabilities: []string{},
		ResourcesSupported:   true,
	}
	b.appVersions.Put(v)

	// Track the latest created version on the application itself so GetApplication
	// returns the most recently created version by default.
	app.SemanticVersion = semanticVersion

	return cloneVersion(v), nil
}

// GetApplicationVersion returns a specific version of an application by semantic version string.
func (b *InMemoryBackend) GetApplicationVersion(appName, semanticVersion string) (*ApplicationVersion, error) {
	b.mu.RLock("GetApplicationVersion")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	v, ok := b.appVersions.Get(versionKey(appName, semanticVersion))
	if !ok {
		return nil, fmt.Errorf(
			"%w: could not find version %q for application %q",
			ErrApplicationNotFound,
			semanticVersion,
			appName,
		)
	}

	return cloneVersion(v), nil
}

// ListApplicationVersions returns all versions for an application sorted by semantic version.
func (b *InMemoryBackend) ListApplicationVersions(appName string) ([]*ApplicationVersion, error) {
	b.mu.RLock("ListApplicationVersions")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	versions := b.appVersionsByApp.Get(appName)
	list := make([]*ApplicationVersion, 0, len(versions))

	for _, v := range versions {
		list = append(list, cloneVersion(v))
	}

	// store.Index preserves insertion order, not sort order -- explicit sort
	// is required for the deterministic-by-semanticVersion result this
	// method has always returned.
	sort.Slice(list, func(i, j int) bool {
		return list[i].SemanticVersion < list[j].SemanticVersion
	})

	return list, nil
}

// CreateCloudFormationTemplate creates a new CloudFormation template for an application.
func (b *InMemoryBackend) CreateCloudFormationTemplate(
	appName, semanticVersion string,
) (*CloudFormationTemplate, error) {
	b.mu.Lock("CreateCloudFormationTemplate")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(appName)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	now := time.Now()
	templateID := fmt.Sprintf("%s-%d", appName, now.UnixNano())
	t := &CloudFormationTemplate{
		ApplicationID:   app.ApplicationID,
		TemplateID:      templateID,
		AppName:         appName,
		SemanticVersion: semanticVersion,
		Status:          templateStatusActive,
		CreationTime:    now,
		ExpirationTime:  now.Add(templateExpirationHours * time.Hour),
		TemplateURL: fmt.Sprintf(
			"https://s3.amazonaws.com/serverlessrepo-templates/%s/%s.template",
			appName,
			templateID,
		),
	}
	b.cfTemplates.Put(t)

	return cloneTemplate(t), nil
}

// GetCloudFormationTemplate returns a CloudFormation template by application name and template ID.
func (b *InMemoryBackend) GetCloudFormationTemplate(appName, templateID string) (*CloudFormationTemplate, error) {
	b.mu.RLock("GetCloudFormationTemplate")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	t, ok := b.cfTemplates.Get(templateID)
	if !ok || t.AppName != appName {
		return nil, fmt.Errorf(
			"%w: could not find template %q for application %q",
			ErrTemplateNotFound,
			templateID,
			appName,
		)
	}

	return cloneTemplate(t), nil
}

// CreateCloudFormationChangeSet creates a new CloudFormation change set for an application.
func (b *InMemoryBackend) CreateCloudFormationChangeSet(
	appName string,
	stackName string,
	changeSetName string,
	semanticVersion string,
) (*CloudFormationChangeSet, error) {
	return b.CreateCloudFormationChangeSetWithOptions(
		appName,
		stackName,
		changeSetName,
		semanticVersion,
		CreateCloudFormationChangeSetOptions{},
	)
}

// CreateCloudFormationChangeSetWithOptions creates a deployment change set with request metadata.
func (b *InMemoryBackend) CreateCloudFormationChangeSetWithOptions(
	appName string,
	stackName string,
	changeSetName string,
	semanticVersion string,
	opts CreateCloudFormationChangeSetOptions,
) (*CloudFormationChangeSet, error) {
	b.mu.Lock("CreateCloudFormationChangeSet")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(appName)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	for _, capability := range opts.Capabilities {
		if !isValidCapability(capability) {
			return nil, fmt.Errorf("%w: unsupported capability %q", ErrValidation, capability)
		}
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	csName := changeSetName
	if csName == "" {
		csName = stackName + "-" + suffix
	}

	changeSetID := arn.Build(
		"cloudformation",
		b.region,
		b.accountID,
		"changeSet/"+csName,
	)
	stackID := arn.Build(
		"cloudformation",
		b.region,
		b.accountID,
		"stack/"+stackName+"/"+suffix,
	)

	cs := &CloudFormationChangeSet{
		ApplicationID:   app.ApplicationID,
		ChangeSetID:     changeSetID,
		AppName:         appName,
		SemanticVersion: semanticVersion,
		StackID:         stackID,
		Capabilities:    cloneStringSlice(opts.Capabilities),
		Tags:            cloneTags(opts.Tags),
	}
	b.cfChangeSets.Put(cloneChangeSet(cs))

	return cloneChangeSet(cs), nil
}

// GetApplicationPolicy returns the policy statements for an application.
func (b *InMemoryBackend) GetApplicationPolicy(appName string) ([]*ApplicationPolicyStatement, error) {
	b.mu.RLock("GetApplicationPolicy")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	return clonePolicyStatements(b.appPolicies[appName]), nil
}

// PutApplicationPolicy sets the policy statements for an application.
func (b *InMemoryBackend) PutApplicationPolicy(
	appName string,
	statements []*ApplicationPolicyStatement,
) ([]*ApplicationPolicyStatement, error) {
	b.mu.Lock("PutApplicationPolicy")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	// Validate and auto-generate statementIds.
	for i, s := range statements {
		if len(s.Actions) == 0 {
			return nil, fmt.Errorf("%w: statement %d has no actions", ErrValidation, i)
		}

		for _, action := range s.Actions {
			if !isValidPolicyAction(action) {
				return nil, fmt.Errorf("%w: statement %d contains unsupported action %q", ErrValidation, i, action)
			}
		}

		if len(s.Principals) == 0 {
			return nil, fmt.Errorf("%w: statement %d has no principals", ErrValidation, i)
		}

		if s.StatementID == "" {
			statements[i].StatementID = uuid.NewString()
		}
	}

	b.appPolicies[appName] = clonePolicyStatements(statements)

	return clonePolicyStatements(b.appPolicies[appName]), nil
}

// AddApplicationDependencyInternal seeds a nested dependency for a version.
func (b *InMemoryBackend) AddApplicationDependencyInternal(
	appName, semanticVersion string,
	dependency ApplicationDependency,
) error {
	b.mu.Lock("AddApplicationDependencyInternal")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	if b.appDependencies[appName] == nil {
		b.appDependencies[appName] = make(map[string][]*ApplicationDependency)
	}

	dep := dependency
	b.appDependencies[appName][semanticVersion] = append(b.appDependencies[appName][semanticVersion], &dep)

	return nil
}

// ListApplicationDependencies returns nested application dependencies for an application version.
func (b *InMemoryBackend) ListApplicationDependencies(
	appName, semanticVersion string,
) ([]*ApplicationDependency, error) {
	b.mu.RLock("ListApplicationDependencies")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	deps := make([]*ApplicationDependency, 0)
	b.collectDependencies(appName, semanticVersion, make(map[string]struct{}), &deps)

	sort.Slice(deps, func(i, j int) bool {
		if deps[i].ApplicationID != deps[j].ApplicationID {
			return deps[i].ApplicationID < deps[j].ApplicationID
		}

		return deps[i].SemanticVersion < deps[j].SemanticVersion
	})

	return deps, nil
}

func (b *InMemoryBackend) collectDependencies(
	appName, semanticVersion string,
	seen map[string]struct{},
	deps *[]*ApplicationDependency,
) {
	for _, dependency := range b.appDependencies[appName][semanticVersion] {
		key := dependency.ApplicationID + "@" + dependency.SemanticVersion
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		cp := *dependency
		*deps = append(*deps, &cp)

		if separator := strings.LastIndex(dependency.ApplicationID, "/"); separator >= 0 {
			b.collectDependencies(dependency.ApplicationID[separator+1:], dependency.SemanticVersion, seen, deps)
		}
	}
}

// UnshareApplication removes an application from an AWS Organization.
func (b *InMemoryBackend) UnshareApplication(appName, _ string) error {
	b.mu.Lock("UnshareApplication")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	return nil
}

// validPolicyActionVariantCount is the number of case variants (PascalCase and all-lowercase)
// registered per documented policy action in [validPolicyActionsSet].
const validPolicyActionVariantCount = 2

// validPolicyActionsSet returns the set of AWS SAR application policy actions documented in
// the "Application Permissions" table of the SAR access-control guide: GetApplication,
// CreateCloudFormationChangeSet, CreateCloudFormationTemplate, ListApplicationVersions,
// ListApplicationDependencies, SearchApplications, Deploy (which implies all of the
// preceding), and UnshareApplication (used to revoke an AWS Organization share). Both the
// documented PascalCase spelling and an all-lowercase variant are accepted per each action.
func validPolicyActionsSet() map[string]struct{} {
	pascalCase := []string{
		"GetApplication",
		"CreateCloudFormationChangeSet",
		"CreateCloudFormationTemplate",
		"ListApplicationVersions",
		"ListApplicationDependencies",
		"SearchApplications",
		"Deploy",
		"UnshareApplication",
	}

	set := make(map[string]struct{}, len(pascalCase)*validPolicyActionVariantCount)
	for _, action := range pascalCase {
		set[action] = struct{}{}
		set[strings.ToLower(action)] = struct{}{}
	}

	return set
}

// isValidPolicyAction returns true if the given action is a supported SAR policy action.
// AWS SAR is case-insensitive for action names; we accept both mixed-case and lowercase variants.
func isValidPolicyAction(action string) bool {
	_, ok := validPolicyActionsSet()[action]

	return ok
}

func isValidCapability(capability string) bool {
	switch capability {
	case "CAPABILITY_IAM",
		"CAPABILITY_NAMED_IAM",
		"CAPABILITY_AUTO_EXPAND",
		"CAPABILITY_RESOURCE_POLICY":
		return true
	default:
		return false
	}
}

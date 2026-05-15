package serverlessrepo

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// validNameRe matches AWS SAR-valid application names: alphanumeric and hyphens only.
var validNameRe = regexp.MustCompile(`^[a-zA-Z0-9\-]+$`)

const (
	// templateStatusActive is the status of an active CloudFormation template.
	templateStatusActive = "ACTIVE"
	// templateExpirationHours is the number of hours before a template expires.
	templateExpirationHours = 1
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
	CreationTime    time.Time `json:"creationTime"`
	ApplicationID   string    `json:"applicationId"`
	Name            string    `json:"name"`
	Description     string    `json:"description,omitempty"`
	Author          string    `json:"author,omitempty"`
	HomePageURL     string    `json:"homePageUrl,omitempty"`
	LicenseURL      string    `json:"licenseUrl,omitempty"`
	ReadmeURL       string    `json:"readmeUrl,omitempty"`
	SpdxLicenseID   string    `json:"spdxLicenseId,omitempty"`
	SourceCodeURL   string    `json:"sourceCodeUrl,omitempty"`
	SemanticVersion string    `json:"semanticVersion,omitempty"`
	Labels          []string  `json:"labels,omitempty"`
}

// ApplicationVersion represents a version of a Serverless Application Repository application.
type ApplicationVersion struct {
	CreationTime         time.Time             `json:"creationTime"`
	ApplicationID        string                `json:"applicationId"`
	SemanticVersion      string                `json:"semanticVersion"`
	SourceCodeURL        string                `json:"sourceCodeUrl,omitempty"`
	SourceCodeArchiveURL string                `json:"sourceCodeArchiveUrl,omitempty"`
	TemplateURL          string                `json:"templateUrl,omitempty"`
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
}

// CloudFormationChangeSet represents a CloudFormation change set for an application.
type CloudFormationChangeSet struct {
	ApplicationID   string `json:"applicationId"`
	ChangeSetID     string `json:"changeSetId"`
	SemanticVersion string `json:"semanticVersion,omitempty"`
	StackID         string `json:"stackId"`
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
type InMemoryBackend struct {
	applications map[string]*Application
	// appVersions maps appName -> semanticVersion -> *ApplicationVersion
	appVersions map[string]map[string]*ApplicationVersion
	// cfTemplates maps appName -> templateID -> *CloudFormationTemplate
	cfTemplates map[string]map[string]*CloudFormationTemplate
	// cfChangeSets maps appName -> changeSetID -> *CloudFormationChangeSet
	cfChangeSets map[string]map[string]*CloudFormationChangeSet
	// appPolicies maps appName -> []*ApplicationPolicyStatement
	appPolicies map[string][]*ApplicationPolicyStatement
	mu          *lockmetrics.RWMutex
	accountID   string
	region      string
}

// NewInMemoryBackend creates a new in-memory Serverless Application Repository backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications: make(map[string]*Application),
		appVersions:  make(map[string]map[string]*ApplicationVersion),
		cfTemplates:  make(map[string]map[string]*CloudFormationTemplate),
		cfChangeSets: make(map[string]map[string]*CloudFormationChangeSet),
		appPolicies:  make(map[string][]*ApplicationPolicyStatement),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("serverlessrepo"),
	}
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.applications = make(map[string]*Application)
	b.appVersions = make(map[string]map[string]*ApplicationVersion)
	b.cfTemplates = make(map[string]map[string]*CloudFormationTemplate)
	b.cfChangeSets = make(map[string]map[string]*CloudFormationChangeSet)
	b.appPolicies = make(map[string][]*ApplicationPolicyStatement)
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
	b.applications[name] = a

	return cloneApplication(a)
}

// AddVersionInternal creates a version directly in the backend, bypassing validation.
// Useful for seeding test state. The application must already exist.
func (b *InMemoryBackend) AddVersionInternal(appName, semanticVersion string) *ApplicationVersion {
	b.mu.Lock("AddVersionInternal")
	defer b.mu.Unlock()

	app := b.applications[appName]
	if app == nil {
		return nil
	}

	if _, exists := b.appVersions[appName]; !exists {
		b.appVersions[appName] = make(map[string]*ApplicationVersion)
	}

	v := &ApplicationVersion{
		ApplicationID:        app.ApplicationID,
		SemanticVersion:      semanticVersion,
		CreationTime:         time.Now(),
		ParameterDefinitions: []ParameterDefinition{},
		RequiredCapabilities: []string{},
		ResourcesSupported:   true,
	}
	b.appVersions[appName][semanticVersion] = v

	return cloneVersion(v)
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

	if author == "" {
		return nil, fmt.Errorf("%w: author is required", ErrValidation)
	}

	if description == "" {
		return nil, fmt.Errorf("%w: description is required", ErrValidation)
	}

	if _, ok := b.applications[name]; ok {
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
	b.applications[name] = a

	return cloneApplication(a), nil
}

// GetApplication returns an application by name.
func (b *InMemoryBackend) GetApplication(name string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	a, ok := b.applications[name]
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	return cloneApplication(a), nil
}

// ListApplications returns all applications sorted by name.
func (b *InMemoryBackend) ListApplications() []*Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	list := make([]*Application, 0, len(b.applications))

	for _, a := range b.applications {
		list = append(list, cloneApplication(a))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

// UpdateApplication updates the description or author of an existing application.
func (b *InMemoryBackend) UpdateApplication(
	name, description, author, homePageURL, readmeURL string,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	a, ok := b.applications[name]
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	if description != "" {
		a.Description = description
	}

	if author != "" {
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

// DeleteApplication deletes an application by name.
func (b *InMemoryBackend) DeleteApplication(name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[name]; !ok {
		return fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, name)
	}

	delete(b.applications, name)
	delete(b.appVersions, name)
	delete(b.cfTemplates, name)
	delete(b.cfChangeSets, name)
	delete(b.appPolicies, name)

	return nil
}

// CreateApplicationVersion creates a new version for an application.
func (b *InMemoryBackend) CreateApplicationVersion(
	appName string,
	semanticVersion string,
	sourceCodeURL string,
	templateURL string,
) (*ApplicationVersion, error) {
	b.mu.Lock("CreateApplicationVersion")
	defer b.mu.Unlock()

	app, ok := b.applications[appName]
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	if sourceCodeURL == "" && templateURL == "" {
		return nil, fmt.Errorf("%w: at least one of sourceCodeUrl or templateUrl is required", ErrValidation)
	}

	if _, exists := b.appVersions[appName]; !exists {
		b.appVersions[appName] = make(map[string]*ApplicationVersion)
	}

	if _, exists := b.appVersions[appName][semanticVersion]; exists {
		return nil, fmt.Errorf(
			"%w: version %s already exists for application %q",
			ErrVersionAlreadyExists,
			semanticVersion,
			appName,
		)
	}

	// Generate a synthetic template URL when the caller provides only a sourceCodeURL.
	resolvedTemplateURL := templateURL
	if resolvedTemplateURL == "" && sourceCodeURL != "" {
		resolvedTemplateURL = fmt.Sprintf(
			"https://s3.amazonaws.com/serverlessrepo-templates/%s/%s.template",
			appName,
			semanticVersion,
		)
	}

	v := &ApplicationVersion{
		ApplicationID:        app.ApplicationID,
		SemanticVersion:      semanticVersion,
		SourceCodeURL:        sourceCodeURL,
		TemplateURL:          resolvedTemplateURL,
		CreationTime:         time.Now(),
		ParameterDefinitions: []ParameterDefinition{},
		RequiredCapabilities: []string{},
		ResourcesSupported:   true,
	}
	b.appVersions[appName][semanticVersion] = v

	return cloneVersion(v), nil
}

// GetApplicationVersion returns a specific version of an application by semantic version string.
func (b *InMemoryBackend) GetApplicationVersion(appName, semanticVersion string) (*ApplicationVersion, error) {
	b.mu.RLock("GetApplicationVersion")
	defer b.mu.RUnlock()

	if _, ok := b.applications[appName]; !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	versions, ok := b.appVersions[appName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: could not find version %q for application %q",
			ErrApplicationNotFound,
			semanticVersion,
			appName,
		)
	}

	v, ok := versions[semanticVersion]
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

	if _, ok := b.applications[appName]; !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	versions := b.appVersions[appName]
	list := make([]*ApplicationVersion, 0, len(versions))

	for _, v := range versions {
		list = append(list, cloneVersion(v))
	}

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

	app, ok := b.applications[appName]
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	if _, exists := b.cfTemplates[appName]; !exists {
		b.cfTemplates[appName] = make(map[string]*CloudFormationTemplate)
	}

	now := time.Now()
	templateID := fmt.Sprintf("%s-%d", appName, now.UnixNano())
	t := &CloudFormationTemplate{
		ApplicationID:   app.ApplicationID,
		TemplateID:      templateID,
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
	b.cfTemplates[appName][templateID] = t

	return cloneTemplate(t), nil
}

// GetCloudFormationTemplate returns a CloudFormation template by application name and template ID.
func (b *InMemoryBackend) GetCloudFormationTemplate(appName, templateID string) (*CloudFormationTemplate, error) {
	b.mu.RLock("GetCloudFormationTemplate")
	defer b.mu.RUnlock()

	if _, ok := b.applications[appName]; !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	templates, ok := b.cfTemplates[appName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: could not find template %q for application %q",
			ErrTemplateNotFound,
			templateID,
			appName,
		)
	}

	t, ok := templates[templateID]
	if !ok {
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
	b.mu.Lock("CreateCloudFormationChangeSet")
	defer b.mu.Unlock()

	app, ok := b.applications[appName]
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	if _, exists := b.cfChangeSets[appName]; !exists {
		b.cfChangeSets[appName] = make(map[string]*CloudFormationChangeSet)
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
		SemanticVersion: semanticVersion,
		StackID:         stackID,
	}
	// Store a copy to prevent shared-pointer mutations after return.
	csCopy := *cs
	b.cfChangeSets[appName][changeSetID] = &csCopy

	return cs, nil
}

// GetApplicationPolicy returns the policy statements for an application.
func (b *InMemoryBackend) GetApplicationPolicy(appName string) ([]*ApplicationPolicyStatement, error) {
	b.mu.RLock("GetApplicationPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.applications[appName]; !ok {
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

	if _, ok := b.applications[appName]; !ok {
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

// ListApplicationDependencies returns the nested application dependencies for an application.
// In this in-memory implementation dependencies are derived from the application's versions list
// (empty by default unless seeded).
func (b *InMemoryBackend) ListApplicationDependencies(appName, _ string) ([]*ApplicationDependency, error) {
	b.mu.RLock("ListApplicationDependencies")
	defer b.mu.RUnlock()

	if _, ok := b.applications[appName]; !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	return []*ApplicationDependency{}, nil
}

// UnshareApplication removes an application from an AWS Organization.
func (b *InMemoryBackend) UnshareApplication(appName, _ string) error {
	b.mu.Lock("UnshareApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[appName]; !ok {
		return fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	return nil
}

// validPolicyActions returns a set of AWS SAR application policy actions (case-sensitive map).
func validPolicyActionsSet() map[string]struct{} {
	return map[string]struct{}{
		"deploy":                  {},
		"Deploy":                  {},
		"getapplication":          {},
		"GetApplication":          {},
		"listapplicationversions": {},
		"ListApplicationVersions": {},
		"searchapplications":      {},
		"SearchApplications":      {},
	}
}

// isValidPolicyAction returns true if the given action is a supported SAR policy action.
// AWS SAR is case-insensitive for action names; we accept both mixed-case and lowercase variants.
func isValidPolicyAction(action string) bool {
	_, ok := validPolicyActionsSet()[action]

	return ok
}

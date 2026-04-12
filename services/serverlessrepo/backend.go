package serverlessrepo

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

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
)

// Application represents an AWS Serverless Application Repository application.
type Application struct {
	CreationTime    time.Time         `json:"creationTime"`
	Tags            map[string]string `json:"tags,omitempty"`
	ApplicationID   string            `json:"applicationId"`
	Name            string            `json:"name"`
	Description     string            `json:"description,omitempty"`
	Author          string            `json:"author,omitempty"`
	SourceCodeURL   string            `json:"sourceCodeUrl,omitempty"`
	SemanticVersion string            `json:"semanticVersion,omitempty"`
}

// ApplicationVersion represents a version of a Serverless Application Repository application.
type ApplicationVersion struct {
	CreationTime    time.Time `json:"creationTime"`
	ApplicationID   string    `json:"applicationId"`
	SemanticVersion string    `json:"semanticVersion"`
	SourceCodeURL   string    `json:"sourceCodeUrl,omitempty"`
	TemplateURL     string    `json:"templateUrl,omitempty"`
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

// cloneApplication returns a deep copy of a, including its Tags map.
func cloneApplication(a *Application) *Application {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

	return &cp
}

// cloneVersion returns a deep copy of v.
func cloneVersion(v *ApplicationVersion) *ApplicationVersion {
	cp := *v

	return &cp
}

// cloneTemplate returns a deep copy of t.
func cloneTemplate(t *CloudFormationTemplate) *CloudFormationTemplate {
	cp := *t

	return &cp
}

// clonePolicyStatement returns a deep copy of s.
func clonePolicyStatement(s *ApplicationPolicyStatement) *ApplicationPolicyStatement {
	cp := *s
	cp.Actions = append([]string(nil), s.Actions...)
	cp.Principals = append([]string(nil), s.Principals...)
	cp.PrincipalOrgIDs = append([]string(nil), s.PrincipalOrgIDs...)

	return &cp
}

// clonePolicyStatements returns deep copies of all policy statements.
func clonePolicyStatements(stmts []*ApplicationPolicyStatement) []*ApplicationPolicyStatement {
	if stmts == nil {
		return nil
	}

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

// CreateApplication creates a new application.
func (b *InMemoryBackend) CreateApplication(
	name string,
	description string,
	author string,
	sourceCodeURL string,
	semanticVersion string,
	tags map[string]string,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

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
		CreationTime:    time.Now(),
		Tags:            mergeTags(nil, tags),
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
func (b *InMemoryBackend) UpdateApplication(name, description, author string) (*Application, error) {
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

	v := &ApplicationVersion{
		ApplicationID:   app.ApplicationID,
		SemanticVersion: semanticVersion,
		SourceCodeURL:   sourceCodeURL,
		TemplateURL:     templateURL,
		CreationTime:    time.Now(),
	}
	b.appVersions[appName][semanticVersion] = v

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

	templateID := fmt.Sprintf("%s-%d", appName, time.Now().UnixNano())
	now := time.Now()
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
	semanticVersion string,
	_ string,
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

	changeSetID := arn.Build(
		"cloudformation",
		b.region,
		b.accountID,
		"changeSet/"+stackName+"-"+strconv.FormatInt(time.Now().UnixNano(), 10),
	)
	stackID := arn.Build(
		"cloudformation",
		b.region,
		b.accountID,
		"stack/"+stackName+"/"+strconv.FormatInt(time.Now().UnixNano(), 10),
	)

	cs := &CloudFormationChangeSet{
		ApplicationID:   app.ApplicationID,
		ChangeSetID:     changeSetID,
		SemanticVersion: semanticVersion,
		StackID:         stackID,
	}
	b.cfChangeSets[appName][changeSetID] = cs

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

// mergeTags merges new tags into existing ones, returning a new map.
func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}

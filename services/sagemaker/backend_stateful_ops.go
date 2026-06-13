package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const idByteLen = 12 // number of random bytes used when generating resource IDs

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	// ErrDomainNotFound is returned when a domain does not exist.
	ErrDomainNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrDomainAlreadyExists is returned when a domain already exists.
	ErrDomainAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrUserProfileNotFound is returned when a user profile does not exist.
	ErrUserProfileNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrUserProfileAlreadyExists is returned when a user profile already exists.
	ErrUserProfileAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrAppNotFound is returned when a SageMaker app does not exist.
	ErrAppNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrAppAlreadyExists is returned when an app already exists.
	ErrAppAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrFeatureGroupNotFound is returned when a feature group does not exist.
	ErrFeatureGroupNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrFeatureGroupAlreadyExists is returned when a feature group already exists.
	ErrFeatureGroupAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrPipelineNotFound is returned when a pipeline does not exist.
	ErrPipelineNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrPipelineAlreadyExists is returned when a pipeline already exists.
	ErrPipelineAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrPipelineExecutionNotFound is returned when a pipeline execution does not exist.
	ErrPipelineExecutionNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrExperimentNotFound is returned when an experiment does not exist.
	ErrExperimentNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrExperimentAlreadyExists is returned when an experiment already exists.
	ErrExperimentAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrTrialNotFound is returned when a trial does not exist.
	ErrTrialNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrTrialAlreadyExists is returned when a trial already exists.
	ErrTrialAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrTrialComponentNotFound is returned when a trial component does not exist.
	ErrTrialComponentNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrTrialComponentAlreadyExists is returned when a trial component already exists.
	ErrTrialComponentAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// ---------------------------------------------------------------------------
// Domain
// ---------------------------------------------------------------------------

// Domain represents a SageMaker Studio domain.
type Domain struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	DomainID         string            `json:"DomainId"`
	DomainArn        string            `json:"DomainArn"`
	DomainName       string            `json:"DomainName"`
	Status           string            `json:"Status"`
	URL              string            `json:"Url,omitempty"`
	AuthMode         string            `json:"AuthMode,omitempty"`
}

func cloneDomain(d *Domain) *Domain {
	cp := *d
	cp.Tags = maps.Clone(d.Tags)

	return &cp
}

// UserProfileKey is the composite key for user profiles.
type userProfileKey struct {
	DomainID        string
	UserProfileName string
}

// UserProfile represents a SageMaker Studio user profile.
type UserProfile struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	DomainID         string            `json:"DomainId"`
	UserProfileName  string            `json:"UserProfileName"`
	UserProfileArn   string            `json:"UserProfileArn"`
	Status           string            `json:"Status"`
}

func cloneUserProfile(up *UserProfile) *UserProfile {
	cp := *up
	cp.Tags = maps.Clone(up.Tags)

	return &cp
}

// appKey is the composite key for SageMaker apps.
type appKey struct {
	DomainID        string
	UserProfileName string
	AppType         string
	AppName         string
}

// App represents a SageMaker Studio app.
type App struct {
	CreationTime    time.Time         `json:"CreationTime"`
	Tags            map[string]string `json:"Tags,omitempty"`
	DomainID        string            `json:"DomainId"`
	UserProfileName string            `json:"UserProfileName"`
	AppType         string            `json:"AppType"`
	AppName         string            `json:"AppName"`
	AppArn          string            `json:"AppArn"`
	Status          string            `json:"Status"`
}

func cloneApp(a *App) *App {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

	return &cp
}

// ---------------------------------------------------------------------------
// Feature Group
// ---------------------------------------------------------------------------

// FeatureDefinition holds the definition of a single feature.
type FeatureDefinition struct {
	FeatureName string `json:"FeatureName"`
	FeatureType string `json:"FeatureType,omitempty"`
}

// FeatureGroup represents a SageMaker Feature Store feature group.
type FeatureGroup struct {
	CreationTime                time.Time           `json:"CreationTime"`
	Tags                        map[string]string   `json:"Tags,omitempty"`
	FeatureGroupName            string              `json:"FeatureGroupName"`
	FeatureGroupArn             string              `json:"FeatureGroupArn"`
	RecordIdentifierFeatureName string              `json:"RecordIdentifierFeatureName,omitempty"`
	EventTimeFeatureName        string              `json:"EventTimeFeatureName,omitempty"`
	FeatureGroupStatus          string              `json:"FeatureGroupStatus"`
	FeatureDefinitions          []FeatureDefinition `json:"FeatureDefinitions,omitempty"`
}

func cloneFeatureGroup(fg *FeatureGroup) *FeatureGroup {
	cp := *fg
	cp.Tags = maps.Clone(fg.Tags)
	cp.FeatureDefinitions = make([]FeatureDefinition, len(fg.FeatureDefinitions))
	copy(cp.FeatureDefinitions, fg.FeatureDefinitions)

	return &cp
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

// ParallelismConfiguration limits concurrent steps in a pipeline execution.
type ParallelismConfiguration struct {
	MaxParallelExecutionSteps int32 `json:"MaxParallelExecutionSteps,omitempty"`
}

// PipelineParameter is a name/value pair passed to StartPipelineExecution.
type PipelineParameter struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// Pipeline represents a SageMaker Pipeline.
type Pipeline struct {
	CreationTime             time.Time                 `json:"CreationTime"`
	LastModifiedTime         time.Time                 `json:"LastModifiedTime"`
	Tags                     map[string]string         `json:"Tags,omitempty"`
	ParallelismConfiguration *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
	PipelineName             string                    `json:"PipelineName"`
	PipelineArn              string                    `json:"PipelineArn"`
	PipelineStatus           string                    `json:"PipelineStatus"`
	PipelineDefinition       string                    `json:"PipelineDefinition,omitempty"`
	PipelineDisplayName      string                    `json:"PipelineDisplayName,omitempty"`
	PipelineDescription      string                    `json:"PipelineDescription,omitempty"`
	RoleArn                  string                    `json:"RoleArn,omitempty"`
}

func clonePipeline(p *Pipeline) *Pipeline {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp
}

// PipelineExecution represents a single execution of a SageMaker Pipeline.
type PipelineExecution struct {
	StartTime                    time.Time           `json:"StartTime"`
	PipelineArn                  string              `json:"PipelineArn"`
	PipelineExecutionArn         string              `json:"PipelineExecutionArn"`
	PipelineExecutionStatus      string              `json:"PipelineExecutionStatus"`
	PipelineExecutionDisplayName string              `json:"PipelineExecutionDisplayName,omitempty"`
	PipelineExecutionDescription string              `json:"PipelineExecutionDescription,omitempty"`
	FailureReason                string              `json:"FailureReason,omitempty"`
	PipelineParameters           []PipelineParameter `json:"PipelineParameters,omitempty"`
}

func clonePipelineExecution(pe *PipelineExecution) *PipelineExecution {
	cp := *pe
	cp.PipelineParameters = make([]PipelineParameter, len(pe.PipelineParameters))
	copy(cp.PipelineParameters, pe.PipelineParameters)

	return &cp
}

// ---------------------------------------------------------------------------
// Experiment
// ---------------------------------------------------------------------------

// Experiment represents a SageMaker Experiment.
type Experiment struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	ExperimentName   string            `json:"ExperimentName"`
	ExperimentArn    string            `json:"ExperimentArn"`
	DisplayName      string            `json:"DisplayName,omitempty"`
	Description      string            `json:"Description,omitempty"`
}

func cloneExperiment(e *Experiment) *Experiment {
	cp := *e
	cp.Tags = maps.Clone(e.Tags)

	return &cp
}

// Trial represents a SageMaker Trial.
type Trial struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	TrialName        string            `json:"TrialName"`
	TrialArn         string            `json:"TrialArn"`
	ExperimentName   string            `json:"ExperimentName"`
	DisplayName      string            `json:"DisplayName,omitempty"`
}

func cloneTrial(t *Trial) *Trial {
	cp := *t
	cp.Tags = maps.Clone(t.Tags)

	return &cp
}

// TrialComponent represents a SageMaker Trial Component.
type TrialComponent struct {
	CreationTime       time.Time                         `json:"CreationTime"`
	LastModifiedTime   time.Time                         `json:"LastModifiedTime"`
	StartTime          *time.Time                        `json:"StartTime,omitempty"`
	EndTime            *time.Time                        `json:"EndTime,omitempty"`
	Tags               map[string]string                 `json:"Tags,omitempty"`
	Parameters         map[string]TrialComponentValue    `json:"Parameters,omitempty"`
	InputArtifacts     map[string]TrialComponentArtifact `json:"InputArtifacts,omitempty"`
	OutputArtifacts    map[string]TrialComponentArtifact `json:"OutputArtifacts,omitempty"`
	TrialComponentName string                            `json:"TrialComponentName"`
	TrialComponentArn  string                            `json:"TrialComponentArn"`
	DisplayName        string                            `json:"DisplayName,omitempty"`
	Status             string                            `json:"Status,omitempty"`
}

// TrialComponentValue is a number or string parameter value.
type TrialComponentValue struct {
	NumberValue *float64 `json:"NumberValue,omitempty"`
	StringValue string   `json:"StringValue,omitempty"`
}

// TrialComponentArtifact is a URI/media-type artifact reference.
type TrialComponentArtifact struct {
	Value     string `json:"Value"`
	MediaType string `json:"MediaType,omitempty"`
}

func cloneTrialComponent(tc *TrialComponent) *TrialComponent {
	cp := *tc
	cp.Tags = maps.Clone(tc.Tags)
	cp.Parameters = maps.Clone(tc.Parameters)
	cp.InputArtifacts = maps.Clone(tc.InputArtifacts)
	cp.OutputArtifacts = maps.Clone(tc.OutputArtifacts)

	return &cp
}

// ---------------------------------------------------------------------------
// Backend methods — Domain
// ---------------------------------------------------------------------------

// CreateDomain creates a new SageMaker Studio domain.
func (b *InMemoryBackend) CreateDomain(
	ctx context.Context,
	name, authMode string,
	tags map[string]string,
) (*Domain, error) {
	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, d := range b.domainsStore(region) {
		if d.DomainName == name {
			return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, name)
		}
	}

	id := fmt.Sprintf("d-%s", generateID())
	domainArn := arn.Build("sagemaker", region, b.accountID, "domain/"+id)
	now := time.Now()

	d := &Domain{
		DomainID:         id,
		DomainArn:        domainArn,
		DomainName:       name,
		AuthMode:         authMode,
		Status:           statusInService,
		URL:              fmt.Sprintf("https://%s.studio.%s.sagemaker.aws", id, region),
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             mergeTags(nil, tags),
	}
	b.domainsStore(region)[id] = d

	return cloneDomain(d), nil
}

// DescribeDomain returns a domain by ID or name.
func (b *InMemoryBackend) DescribeDomain(ctx context.Context, idOrName string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if d, ok := b.domainsStore(region)[idOrName]; ok {
		return cloneDomain(d), nil
	}

	for _, d := range b.domainsStore(region) {
		if d.DomainName == idOrName {
			return cloneDomain(d), nil
		}
	}

	return nil, fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, idOrName)
}

// ListDomains returns all domains sorted by name.
func (b *InMemoryBackend) ListDomains(ctx context.Context, nextToken string) ([]*Domain, string) {
	b.mu.RLock("ListDomains")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.domainsStore(region)
	list := make([]*Domain, 0, len(store))

	for _, d := range store {
		list = append(list, cloneDomain(d))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].DomainName < list[j].DomainName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*Domain{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// DeleteDomain deletes a domain by ID or name.
func (b *InMemoryBackend) DeleteDomain(ctx context.Context, idOrName string) error {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.domainsStore(region)

	for id, d := range store {
		if id == idOrName || d.DomainName == idOrName {
			delete(store, id)

			return nil
		}
	}

	return fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, idOrName)
}

// UpdateDomain updates a domain's status.
func (b *InMemoryBackend) UpdateDomain(ctx context.Context, idOrName string) (*Domain, error) {
	b.mu.Lock("UpdateDomain")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, d := range b.domainsStore(region) {
		if d.DomainID == idOrName || d.DomainName == idOrName {
			d.LastModifiedTime = time.Now()

			return cloneDomain(d), nil
		}
	}

	return nil, fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, idOrName)
}

// ---------------------------------------------------------------------------
// Backend methods — UserProfile
// ---------------------------------------------------------------------------

// CreateUserProfile creates a new user profile in a domain.
func (b *InMemoryBackend) CreateUserProfile(
	ctx context.Context,
	domainID, name string,
	tags map[string]string,
) (*UserProfile, error) {
	b.mu.Lock("CreateUserProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := userProfileKey{DomainID: domainID, UserProfileName: name}
	if _, ok := b.userProfilesStore(region)[key]; ok {
		return nil, fmt.Errorf(
			"%w: user profile %s in domain %s already exists",
			ErrUserProfileAlreadyExists,
			name,
			domainID,
		)
	}

	upArn := arn.Build(
		"sagemaker",
		region,
		b.accountID,
		fmt.Sprintf("user-profile/%s/%s", domainID, name),
	)
	now := time.Now()

	up := &UserProfile{
		DomainID:         domainID,
		UserProfileName:  name,
		UserProfileArn:   upArn,
		Status:           statusInService,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             mergeTags(nil, tags),
	}
	b.userProfilesStore(region)[key] = up

	return cloneUserProfile(up), nil
}

// DescribeUserProfile returns a user profile.
func (b *InMemoryBackend) DescribeUserProfile(ctx context.Context, domainID, name string) (*UserProfile, error) {
	b.mu.RLock("DescribeUserProfile")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	key := userProfileKey{DomainID: domainID, UserProfileName: name}

	up, ok := b.userProfilesStore(region)[key]
	if !ok {
		return nil, fmt.Errorf(
			"%w: user profile %q in domain %q not found",
			ErrUserProfileNotFound,
			name,
			domainID,
		)
	}

	return cloneUserProfile(up), nil
}

// ListUserProfiles returns user profiles for a domain sorted by name.
//
//nolint:dupl // UserProfile and App share pagination structure but are distinct resource types
func (b *InMemoryBackend) ListUserProfiles(ctx context.Context, domainID, nextToken string) ([]*UserProfile, string) {
	b.mu.RLock("ListUserProfiles")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.userProfilesStore(region)
	list := make([]*UserProfile, 0, len(store))

	for _, up := range store {
		if domainID == "" || up.DomainID == domainID {
			list = append(list, cloneUserProfile(up))
		}
	}

	sort.Slice(
		list,
		func(i, j int) bool { return list[i].UserProfileName < list[j].UserProfileName },
	)

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*UserProfile{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// DeleteUserProfile deletes a user profile.
func (b *InMemoryBackend) DeleteUserProfile(ctx context.Context, domainID, name string) error {
	b.mu.Lock("DeleteUserProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.userProfilesStore(region)

	key := userProfileKey{DomainID: domainID, UserProfileName: name}
	if _, ok := store[key]; !ok {
		return fmt.Errorf(
			"%w: user profile %q in domain %q not found",
			ErrUserProfileNotFound,
			name,
			domainID,
		)
	}

	delete(store, key)

	return nil
}

// ---------------------------------------------------------------------------
// Backend methods — App
// ---------------------------------------------------------------------------

// CreateApp creates a new SageMaker Studio app.
func (b *InMemoryBackend) CreateApp(
	ctx context.Context,
	domainID, userProfile, appType, appName string,
	tags map[string]string,
) (*App, error) {
	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := appKey{
		DomainID:        domainID,
		UserProfileName: userProfile,
		AppType:         appType,
		AppName:         appName,
	}
	if _, ok := b.appsStore(region)[key]; ok {
		return nil, fmt.Errorf("%w: app %s already exists", ErrAppAlreadyExists, appName)
	}

	appArn := arn.Build("sagemaker", region, b.accountID,
		fmt.Sprintf("app/%s/%s/%s/%s", domainID, userProfile, appType, appName))
	now := time.Now()

	a := &App{
		DomainID:        domainID,
		UserProfileName: userProfile,
		AppType:         appType,
		AppName:         appName,
		AppArn:          appArn,
		Status:          statusInService,
		CreationTime:    now,
		Tags:            mergeTags(nil, tags),
	}
	b.appsStore(region)[key] = a

	return cloneApp(a), nil
}

// DescribeApp returns an app.
func (b *InMemoryBackend) DescribeApp(
	ctx context.Context,
	domainID, userProfile, appType, appName string,
) (*App, error) {
	b.mu.RLock("DescribeApp")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	key := appKey{
		DomainID:        domainID,
		UserProfileName: userProfile,
		AppType:         appType,
		AppName:         appName,
	}

	a, ok := b.appsStore(region)[key]
	if !ok {
		return nil, fmt.Errorf("%w: app %q not found", ErrAppNotFound, appName)
	}

	return cloneApp(a), nil
}

// ListApps returns all apps, optionally filtered by domain.
//
//nolint:dupl // App and UserProfile share pagination structure but are distinct resource types
func (b *InMemoryBackend) ListApps(ctx context.Context, domainID, nextToken string) ([]*App, string) {
	b.mu.RLock("ListApps")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.appsStore(region)
	list := make([]*App, 0, len(store))

	for _, a := range store {
		if domainID == "" || a.DomainID == domainID {
			list = append(list, cloneApp(a))
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].AppName < list[j].AppName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*App{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// DeleteApp deletes an app (marks as Deleted).
func (b *InMemoryBackend) DeleteApp(ctx context.Context, domainID, userProfile, appType, appName string) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.appsStore(region)

	key := appKey{
		DomainID:        domainID,
		UserProfileName: userProfile,
		AppType:         appType,
		AppName:         appName,
	}
	if _, ok := store[key]; !ok {
		return fmt.Errorf("%w: app %q not found", ErrAppNotFound, appName)
	}

	delete(store, key)

	return nil
}

// ---------------------------------------------------------------------------
// Backend methods — FeatureGroup
// ---------------------------------------------------------------------------

// CreateFeatureGroup creates a new feature group.
func (b *InMemoryBackend) CreateFeatureGroup(
	ctx context.Context,
	name, recordID, eventTimeFeature string,
	defs []FeatureDefinition,
	tags map[string]string,
) (*FeatureGroup, error) {
	b.mu.Lock("CreateFeatureGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.featureGroupsStore(region)[name]; ok {
		return nil, fmt.Errorf(
			"%w: feature group %s already exists",
			ErrFeatureGroupAlreadyExists,
			name,
		)
	}

	fgArn := arn.Build("sagemaker", region, b.accountID, "feature-group/"+name)
	storedDefs := make([]FeatureDefinition, len(defs))
	copy(storedDefs, defs)

	fg := &FeatureGroup{
		FeatureGroupName:            name,
		FeatureGroupArn:             fgArn,
		RecordIdentifierFeatureName: recordID,
		EventTimeFeatureName:        eventTimeFeature,
		FeatureDefinitions:          storedDefs,
		FeatureGroupStatus:          "Created",
		CreationTime:                time.Now(),
		Tags:                        mergeTags(nil, tags),
	}
	b.featureGroupsStore(region)[name] = fg

	return cloneFeatureGroup(fg), nil
}

// DescribeFeatureGroup returns a feature group by name.
func (b *InMemoryBackend) DescribeFeatureGroup(ctx context.Context, name string) (*FeatureGroup, error) {
	b.mu.RLock("DescribeFeatureGroup")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	fg, ok := b.featureGroupsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: feature group %q not found", ErrFeatureGroupNotFound, name)
	}

	return cloneFeatureGroup(fg), nil
}

// ListFeatureGroups returns all feature groups.
func (b *InMemoryBackend) ListFeatureGroups(ctx context.Context, nextToken string) ([]*FeatureGroup, string) {
	b.mu.RLock("ListFeatureGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.featureGroupsStore(region)
	list := make([]*FeatureGroup, 0, len(store))

	for _, fg := range store {
		list = append(list, cloneFeatureGroup(fg))
	}

	sort.Slice(
		list,
		func(i, j int) bool { return list[i].FeatureGroupName < list[j].FeatureGroupName },
	)

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*FeatureGroup{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// DeleteFeatureGroup deletes a feature group.
func (b *InMemoryBackend) DeleteFeatureGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteFeatureGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.featureGroupsStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: feature group %q not found", ErrFeatureGroupNotFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// Backend methods — Pipeline
// ---------------------------------------------------------------------------

// CreatePipeline creates a new pipeline.
func (b *InMemoryBackend) CreatePipeline(
	ctx context.Context,
	name, definition, roleArn string,
	tags map[string]string,
) (*Pipeline, error) {
	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelinesStore(region)[name]; ok {
		return nil, fmt.Errorf("%w: pipeline %s already exists", ErrPipelineAlreadyExists, name)
	}

	pArn := arn.Build("sagemaker", region, b.accountID, "pipeline/"+name)
	now := time.Now()

	p := &Pipeline{
		PipelineName:       name,
		PipelineArn:        pArn,
		PipelineStatus:     "Active",
		PipelineDefinition: definition,
		RoleArn:            roleArn,
		CreationTime:       now,
		LastModifiedTime:   now,
		Tags:               mergeTags(nil, tags),
	}
	b.pipelinesStore(region)[name] = p

	return clonePipeline(p), nil
}

// DescribePipeline returns a pipeline by name.
func (b *InMemoryBackend) DescribePipeline(ctx context.Context, name string) (*Pipeline, error) {
	b.mu.RLock("DescribePipeline")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, name)
	}

	return clonePipeline(p), nil
}

// ListPipelines returns all pipelines.
func (b *InMemoryBackend) ListPipelines(ctx context.Context, nextToken string) ([]*Pipeline, string) {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.pipelinesStore(region)
	list := make([]*Pipeline, 0, len(store))

	for _, p := range store {
		list = append(list, clonePipeline(p))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].PipelineName < list[j].PipelineName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*Pipeline{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// UpdatePipeline updates a pipeline definition.
func (b *InMemoryBackend) UpdatePipeline(ctx context.Context, name, definition string) (*Pipeline, error) {
	b.mu.Lock("UpdatePipeline")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, name)
	}

	if definition != "" {
		p.PipelineDefinition = definition
	}

	p.LastModifiedTime = time.Now()

	return clonePipeline(p), nil
}

// DeletePipeline deletes a pipeline.
func (b *InMemoryBackend) DeletePipeline(ctx context.Context, name string) (*Pipeline, error) {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.pipelinesStore(region)

	p, ok := store[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, name)
	}

	cp := clonePipeline(p)
	delete(store, name)

	return cp, nil
}

// StartPipelineExecution creates a pipeline execution.
func (b *InMemoryBackend) StartPipelineExecution(ctx context.Context, pipelineName string) (*PipelineExecution, error) {
	b.mu.Lock("StartPipelineExecution")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region)[pipelineName]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, pipelineName)
	}

	execID := generateID()
	execArn := p.PipelineArn + "/execution/" + execID

	pe := &PipelineExecution{
		PipelineArn:             p.PipelineArn,
		PipelineExecutionArn:    execArn,
		PipelineExecutionStatus: pipelineStatusSucceeded,
		StartTime:               time.Now(),
	}
	b.pipelineExecutionsStore(region)[execArn] = pe

	return clonePipelineExecution(pe), nil
}

// DescribePipelineExecution returns a pipeline execution.
func (b *InMemoryBackend) DescribePipelineExecution(ctx context.Context, execArn string) (*PipelineExecution, error) {
	b.mu.RLock("DescribePipelineExecution")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	pe, ok := b.pipelineExecutionsStore(region)[execArn]
	if !ok {
		return nil, fmt.Errorf(
			"%w: pipeline execution %q not found",
			ErrPipelineExecutionNotFound,
			execArn,
		)
	}

	return clonePipelineExecution(pe), nil
}

// ListPipelineExecutions returns executions for a pipeline.
func (b *InMemoryBackend) ListPipelineExecutions(
	ctx context.Context,
	pipelineName, nextToken string,
) ([]*PipelineExecution, string) {
	b.mu.RLock("ListPipelineExecutions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region)[pipelineName]
	execStore := b.pipelineExecutionsStore(region)
	list := make([]*PipelineExecution, 0, len(execStore))

	if ok {
		for _, pe := range execStore {
			if pe.PipelineArn == p.PipelineArn {
				list = append(list, clonePipelineExecution(pe))
			}
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].PipelineExecutionArn < list[j].PipelineExecutionArn
	})

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*PipelineExecution{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// ---------------------------------------------------------------------------
// Backend methods — Experiment
// ---------------------------------------------------------------------------

// CreateExperiment creates a new experiment.
func (b *InMemoryBackend) CreateExperiment(
	ctx context.Context,
	name string,
	tags map[string]string,
) (*Experiment, error) {
	b.mu.Lock("CreateExperiment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.experimentsStore(region)[name]; ok {
		return nil, fmt.Errorf("%w: experiment %s already exists", ErrExperimentAlreadyExists, name)
	}

	expArn := arn.Build("sagemaker", region, b.accountID, "experiment/"+name)
	now := time.Now()

	e := &Experiment{
		ExperimentName:   name,
		ExperimentArn:    expArn,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             mergeTags(nil, tags),
	}
	b.experimentsStore(region)[name] = e

	return cloneExperiment(e), nil
}

// DescribeExperiment returns an experiment by name.
func (b *InMemoryBackend) DescribeExperiment(ctx context.Context, name string) (*Experiment, error) {
	b.mu.RLock("DescribeExperiment")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	e, ok := b.experimentsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: experiment %q not found", ErrExperimentNotFound, name)
	}

	return cloneExperiment(e), nil
}

// ListExperiments returns all experiments.
func (b *InMemoryBackend) ListExperiments(ctx context.Context, nextToken string) ([]*Experiment, string) {
	b.mu.RLock("ListExperiments")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.experimentsStore(region)
	list := make([]*Experiment, 0, len(store))

	for _, e := range store {
		list = append(list, cloneExperiment(e))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ExperimentName < list[j].ExperimentName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*Experiment{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// DeleteExperiment deletes an experiment.
func (b *InMemoryBackend) DeleteExperiment(ctx context.Context, name string) (*Experiment, error) {
	b.mu.Lock("DeleteExperiment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.experimentsStore(region)

	e, ok := store[name]
	if !ok {
		return nil, fmt.Errorf("%w: experiment %q not found", ErrExperimentNotFound, name)
	}

	cp := cloneExperiment(e)
	delete(store, name)

	return cp, nil
}

// ---------------------------------------------------------------------------
// Backend methods — Trial
// ---------------------------------------------------------------------------

// CreateTrial creates a new trial.
func (b *InMemoryBackend) CreateTrial(
	ctx context.Context,
	name, experimentName string,
	tags map[string]string,
) (*Trial, error) {
	b.mu.Lock("CreateTrial")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.trialsStore(region)[name]; ok {
		return nil, fmt.Errorf("%w: trial %s already exists", ErrTrialAlreadyExists, name)
	}

	trialArn := arn.Build("sagemaker", region, b.accountID, "experiment-trial/"+name)
	now := time.Now()

	t := &Trial{
		TrialName:        name,
		TrialArn:         trialArn,
		ExperimentName:   experimentName,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             mergeTags(nil, tags),
	}
	b.trialsStore(region)[name] = t

	return cloneTrial(t), nil
}

// DescribeTrial returns a trial by name.
func (b *InMemoryBackend) DescribeTrial(ctx context.Context, name string) (*Trial, error) {
	b.mu.RLock("DescribeTrial")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	t, ok := b.trialsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: trial %q not found", ErrTrialNotFound, name)
	}

	return cloneTrial(t), nil
}

// ListTrials returns all trials.
func (b *InMemoryBackend) ListTrials(ctx context.Context, nextToken string) ([]*Trial, string) {
	b.mu.RLock("ListTrials")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.trialsStore(region)
	list := make([]*Trial, 0, len(store))

	for _, t := range store {
		list = append(list, cloneTrial(t))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].TrialName < list[j].TrialName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*Trial{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// DeleteTrial deletes a trial.
func (b *InMemoryBackend) DeleteTrial(ctx context.Context, name string) (*Trial, error) {
	b.mu.Lock("DeleteTrial")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.trialsStore(region)

	t, ok := store[name]
	if !ok {
		return nil, fmt.Errorf("%w: trial %q not found", ErrTrialNotFound, name)
	}

	cp := cloneTrial(t)
	delete(store, name)

	return cp, nil
}

// ---------------------------------------------------------------------------
// Backend methods — TrialComponent
// ---------------------------------------------------------------------------

// CreateTrialComponent creates a new trial component.
func (b *InMemoryBackend) CreateTrialComponent(
	ctx context.Context,
	name string,
	tags map[string]string,
) (*TrialComponent, error) {
	b.mu.Lock("CreateTrialComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.trialComponentsStore(region)[name]; ok {
		return nil, fmt.Errorf(
			"%w: trial component %s already exists",
			ErrTrialComponentAlreadyExists,
			name,
		)
	}

	tcArn := arn.Build("sagemaker", region, b.accountID, "experiment-trial-component/"+name)
	now := time.Now()

	tc := &TrialComponent{
		TrialComponentName: name,
		TrialComponentArn:  tcArn,
		CreationTime:       now,
		LastModifiedTime:   now,
		Tags:               mergeTags(nil, tags),
	}
	b.trialComponentsStore(region)[name] = tc

	return cloneTrialComponent(tc), nil
}

// DescribeTrialComponent returns a trial component by name.
func (b *InMemoryBackend) DescribeTrialComponent(ctx context.Context, name string) (*TrialComponent, error) {
	b.mu.RLock("DescribeTrialComponent")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	tc, ok := b.trialComponentsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: trial component %q not found", ErrTrialComponentNotFound, name)
	}

	return cloneTrialComponent(tc), nil
}

// DeleteTrialComponent deletes a trial component.
func (b *InMemoryBackend) DeleteTrialComponent(ctx context.Context, name string) (*TrialComponent, error) {
	b.mu.Lock("DeleteTrialComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.trialComponentsStore(region)

	tc, ok := store[name]
	if !ok {
		return nil, fmt.Errorf("%w: trial component %q not found", ErrTrialComponentNotFound, name)
	}

	cp := cloneTrialComponent(tc)
	delete(store, name)

	return cp, nil
}

// ---------------------------------------------------------------------------
// Backend methods — Update operations (gaps #19, #23, #25, #26)
// ---------------------------------------------------------------------------

// UpdateFeatureGroup mutates FeatureDefinitions on an existing feature group.
func (b *InMemoryBackend) UpdateFeatureGroup(
	ctx context.Context,
	name string,
	featureDefinitions []FeatureDefinition,
) (*FeatureGroup, error) {
	b.mu.Lock("UpdateFeatureGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	fg, ok := b.featureGroupsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: feature group %q not found", ErrFeatureGroupNotFound, name)
	}

	if len(featureDefinitions) > 0 {
		fg.FeatureDefinitions = append(fg.FeatureDefinitions, featureDefinitions...)
	}

	return cloneFeatureGroup(fg), nil
}

// UpdateExperiment mutates DisplayName and Description on an experiment.
func (b *InMemoryBackend) UpdateExperiment(
	ctx context.Context,
	name, displayName, description string,
) (*Experiment, error) {
	b.mu.Lock("UpdateExperiment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	e, ok := b.experimentsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: experiment %q not found", ErrExperimentNotFound, name)
	}

	if displayName != "" {
		e.DisplayName = displayName
	}
	if description != "" {
		e.Description = description
	}
	e.LastModifiedTime = time.Now()

	return cloneExperiment(e), nil
}

// UpdateTrial mutates DisplayName on a trial.
func (b *InMemoryBackend) UpdateTrial(ctx context.Context, name, displayName string) (*Trial, error) {
	b.mu.Lock("UpdateTrial")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	t, ok := b.trialsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: trial %q not found", ErrTrialNotFound, name)
	}

	if displayName != "" {
		t.DisplayName = displayName
	}
	t.LastModifiedTime = time.Now()

	return cloneTrial(t), nil
}

// UpdateTrialComponentOptions holds optional fields for UpdateTrialComponent.
type UpdateTrialComponentOptions struct {
	Parameters      map[string]TrialComponentValue    `json:"Parameters,omitempty"`
	InputArtifacts  map[string]TrialComponentArtifact `json:"InputArtifacts,omitempty"`
	OutputArtifacts map[string]TrialComponentArtifact `json:"OutputArtifacts,omitempty"`
	DisplayName     string                            `json:"DisplayName,omitempty"`
	Status          string                            `json:"Status,omitempty"`
}

// UpdateTrialComponent mutates DisplayName, Parameters, and Artifacts on a trial component.
func (b *InMemoryBackend) UpdateTrialComponent(
	ctx context.Context,
	name string,
	opts UpdateTrialComponentOptions,
) (*TrialComponent, error) {
	b.mu.Lock("UpdateTrialComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	tc, ok := b.trialComponentsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: trial component %q not found", ErrTrialComponentNotFound, name)
	}

	if opts.DisplayName != "" {
		tc.DisplayName = opts.DisplayName
	}
	if opts.Status != "" {
		tc.Status = opts.Status
	}
	if len(opts.Parameters) > 0 {
		if tc.Parameters == nil {
			tc.Parameters = make(map[string]TrialComponentValue)
		}
		maps.Copy(tc.Parameters, opts.Parameters)
	}
	if len(opts.InputArtifacts) > 0 {
		if tc.InputArtifacts == nil {
			tc.InputArtifacts = make(map[string]TrialComponentArtifact)
		}
		maps.Copy(tc.InputArtifacts, opts.InputArtifacts)
	}
	if len(opts.OutputArtifacts) > 0 {
		if tc.OutputArtifacts == nil {
			tc.OutputArtifacts = make(map[string]TrialComponentArtifact)
		}
		maps.Copy(tc.OutputArtifacts, opts.OutputArtifacts)
	}
	tc.LastModifiedTime = time.Now()

	return cloneTrialComponent(tc), nil
}

// CreatePipelineOptions holds full input for CreatePipeline.
type CreatePipelineOptions struct {
	Tags                     map[string]string
	ParallelismConfiguration *ParallelismConfiguration
	PipelineName             string
	PipelineDefinition       string
	PipelineDisplayName      string
	PipelineDescription      string
	RoleArn                  string
}

// CreatePipelineFull creates a pipeline with full AWS input fields.
func (b *InMemoryBackend) CreatePipelineFull(ctx context.Context, opts CreatePipelineOptions) (*Pipeline, error) {
	b.mu.Lock("CreatePipelineFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelinesStore(region)[opts.PipelineName]; ok {
		return nil, fmt.Errorf(
			"%w: pipeline %s already exists",
			ErrPipelineAlreadyExists,
			opts.PipelineName,
		)
	}

	pArn := arn.Build("sagemaker", region, b.accountID, "pipeline/"+opts.PipelineName)
	now := time.Now()

	p := &Pipeline{
		PipelineName:             opts.PipelineName,
		PipelineArn:              pArn,
		PipelineStatus:           "Active",
		PipelineDefinition:       opts.PipelineDefinition,
		PipelineDisplayName:      opts.PipelineDisplayName,
		PipelineDescription:      opts.PipelineDescription,
		RoleArn:                  opts.RoleArn,
		ParallelismConfiguration: opts.ParallelismConfiguration,
		CreationTime:             now,
		LastModifiedTime:         now,
		Tags:                     mergeTags(nil, opts.Tags),
	}
	b.pipelinesStore(region)[opts.PipelineName] = p

	return clonePipeline(p), nil
}

// UpdatePipelineFull updates a pipeline with full AWS input fields.
func (b *InMemoryBackend) UpdatePipelineFull(
	ctx context.Context,
	name, definition, displayName, description, roleArn string,
	parallelismConfig *ParallelismConfiguration,
) (*Pipeline, error) {
	b.mu.Lock("UpdatePipelineFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, name)
	}

	if definition != "" {
		p.PipelineDefinition = definition
	}
	if displayName != "" {
		p.PipelineDisplayName = displayName
	}
	if description != "" {
		p.PipelineDescription = description
	}
	if roleArn != "" {
		p.RoleArn = roleArn
	}
	if parallelismConfig != nil {
		p.ParallelismConfiguration = parallelismConfig
	}
	p.LastModifiedTime = time.Now()

	return clonePipeline(p), nil
}

// StartPipelineExecutionOptions holds full input for StartPipelineExecution.
type StartPipelineExecutionOptions struct {
	ParallelismConfiguration     *ParallelismConfiguration
	PipelineName                 string
	PipelineExecutionDisplayName string
	PipelineExecutionDescription string
	PipelineParameters           []PipelineParameter
}

// StartPipelineExecutionFull creates an execution with full AWS input fields.
func (b *InMemoryBackend) StartPipelineExecutionFull(
	ctx context.Context,
	opts StartPipelineExecutionOptions,
) (*PipelineExecution, error) {
	b.mu.Lock("StartPipelineExecutionFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region)[opts.PipelineName]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, opts.PipelineName)
	}

	execID := generateID()
	execArn := p.PipelineArn + "/execution/" + execID

	params := make([]PipelineParameter, len(opts.PipelineParameters))
	copy(params, opts.PipelineParameters)

	pe := &PipelineExecution{
		PipelineArn:                  p.PipelineArn,
		PipelineExecutionArn:         execArn,
		PipelineExecutionStatus:      pipelineStatusSucceeded,
		PipelineExecutionDisplayName: opts.PipelineExecutionDisplayName,
		PipelineExecutionDescription: opts.PipelineExecutionDescription,
		PipelineParameters:           params,
		StartTime:                    time.Now(),
	}
	b.pipelineExecutionsStore(region)[execArn] = pe

	return clonePipelineExecution(pe), nil
}

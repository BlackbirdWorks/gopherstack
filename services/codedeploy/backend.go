package codedeploy

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// simulatedDeployDuration is the simulated time for a deployment to complete.
const simulatedDeployDuration = 5 * time.Second

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ApplicationDoesNotExistException", awserr.ErrNotFound)
	// ErrDeploymentGroupNotFound is returned when a deployment group does not exist.
	ErrDeploymentGroupNotFound = awserr.New("DeploymentGroupDoesNotExistException", awserr.ErrNotFound)
	// ErrDeploymentNotFound is returned when a deployment does not exist.
	ErrDeploymentNotFound = awserr.New("DeploymentDoesNotExistException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when an application already exists.
	ErrAlreadyExists = awserr.New("ApplicationAlreadyExistsException", awserr.ErrConflict)
	// ErrDeploymentGroupAlreadyExists is returned when a deployment group already exists.
	ErrDeploymentGroupAlreadyExists = awserr.New("DeploymentGroupAlreadyExistsException", awserr.ErrConflict)
)

// Application represents an AWS CodeDeploy application.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateApplication.
type Application struct {
	CreationTime    time.Time  `json:"createTime"`
	Tags            *tags.Tags `json:"-"`
	ApplicationName string     `json:"applicationName"`
	ApplicationID   string     `json:"applicationId"`
	ComputePlatform string     `json:"computePlatform"`
	AccountID       string     `json:"-"`
	Region          string     `json:"-"`
}

// DeploymentGroup represents a CodeDeploy deployment group.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateDeploymentGroup.
type DeploymentGroup struct {
	Tags                 *tags.Tags `json:"-"`
	ApplicationName      string     `json:"applicationName"`
	DeploymentGroupName  string     `json:"deploymentGroupName"`
	DeploymentGroupID    string     `json:"deploymentGroupId"`
	ServiceRoleArn       string     `json:"serviceRoleArn"`
	DeploymentConfigName string     `json:"deploymentConfigName"`
	AccountID            string     `json:"-"`
	Region               string     `json:"-"`
}

// Deployment represents a CodeDeploy deployment.
type Deployment struct {
	CreateTime          time.Time  `json:"createTime"`
	CompleteTime        *time.Time `json:"completeTime,omitempty"`
	DeploymentID        string     `json:"deploymentId"`
	ApplicationName     string     `json:"applicationName"`
	DeploymentGroupName string     `json:"deploymentGroupName"`
	Status              string     `json:"status"`
	Creator             string     `json:"creator"`
	Description         string     `json:"description,omitempty"`
	AccountID           string     `json:"-"`
	Region              string     `json:"-"`
}

// InMemoryBackend is the in-memory store for CodeDeploy resources.
type InMemoryBackend struct {
	applications     map[string]*Application
	deploymentGroups map[string]map[string]*DeploymentGroup // appName -> dgName -> DG
	deployments      map[string]*Deployment
	mu               *lockmetrics.RWMutex
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new in-memory CodeDeploy backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications:     make(map[string]*Application),
		deploymentGroups: make(map[string]map[string]*DeploymentGroup),
		deployments:      make(map[string]*Deployment),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("codedeploy"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateApplication creates a new CodeDeploy application.
func (b *InMemoryBackend) CreateApplication(name, computePlatform string, kv map[string]string) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[name]; ok {
		return nil, fmt.Errorf("%w: application %s already exists", ErrAlreadyExists, name)
	}

	appID := uuid.NewString()
	t := tags.New("codedeploy.application." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	app := &Application{
		ApplicationName: name,
		ApplicationID:   appID,
		ComputePlatform: computePlatform,
		AccountID:       b.accountID,
		Region:          b.region,
		CreationTime:    time.Now().UTC(),
		Tags:            t,
	}
	b.applications[name] = app

	cp := *app

	return &cp, nil
}

// GetApplication returns an application by name.
func (b *InMemoryBackend) GetApplication(name string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	app, ok := b.applications[name]
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	cp := *app

	return &cp, nil
}

// ListApplications returns all application names.
func (b *InMemoryBackend) ListApplications() []string {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.applications))
	for name := range b.applications {
		names = append(names, name)
	}

	return names
}

// ListApplicationDetails returns all applications as structs.
func (b *InMemoryBackend) ListApplicationDetails() []*Application {
	b.mu.RLock("ListApplicationDetails")
	defer b.mu.RUnlock()

	list := make([]*Application, 0, len(b.applications))
	for _, app := range b.applications {
		cp := *app
		list = append(list, &cp)
	}

	return list
}

// DeleteApplication deletes an application and all its deployment groups.
func (b *InMemoryBackend) DeleteApplication(name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[name]; !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	delete(b.applications, name)
	delete(b.deploymentGroups, name)

	return nil
}

// CreateDeploymentGroup creates a deployment group for an application.
func (b *InMemoryBackend) CreateDeploymentGroup(
	appName, dgName, serviceRoleArn, deploymentConfigName string,
	kv map[string]string,
) (*DeploymentGroup, error) {
	b.mu.Lock("CreateDeploymentGroup")
	defer b.mu.Unlock()

	if _, ok := b.applications[appName]; !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	if dgs, ok := b.deploymentGroups[appName]; ok {
		if _, exists := dgs[dgName]; exists {
			return nil, fmt.Errorf("%w: deployment group %s already exists", ErrDeploymentGroupAlreadyExists, dgName)
		}
	}

	dgID := uuid.NewString()
	t := tags.New("codedeploy.dg." + appName + "." + dgName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if deploymentConfigName == "" {
		deploymentConfigName = "CodeDeployDefault.AllAtOnce"
	}

	dg := &DeploymentGroup{
		ApplicationName:      appName,
		DeploymentGroupName:  dgName,
		DeploymentGroupID:    dgID,
		ServiceRoleArn:       serviceRoleArn,
		DeploymentConfigName: deploymentConfigName,
		AccountID:            b.accountID,
		Region:               b.region,
		Tags:                 t,
	}

	if _, ok := b.deploymentGroups[appName]; !ok {
		b.deploymentGroups[appName] = make(map[string]*DeploymentGroup)
	}

	b.deploymentGroups[appName][dgName] = dg

	cp := *dg

	return &cp, nil
}

// GetDeploymentGroup returns a deployment group by application and group name.
func (b *InMemoryBackend) GetDeploymentGroup(appName, dgName string) (*DeploymentGroup, error) {
	b.mu.RLock("GetDeploymentGroup")
	defer b.mu.RUnlock()

	dgs, ok := b.deploymentGroups[appName]
	if !ok {
		return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	dg, ok := dgs[dgName]
	if !ok {
		return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	cp := *dg

	return &cp, nil
}

// ListDeploymentGroups returns all deployment group names for an application.
func (b *InMemoryBackend) ListDeploymentGroups(appName string) ([]string, error) {
	b.mu.RLock("ListDeploymentGroups")
	defer b.mu.RUnlock()

	if _, ok := b.applications[appName]; !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	dgs, ok := b.deploymentGroups[appName]
	if !ok {
		return []string{}, nil
	}

	names := make([]string, 0, len(dgs))
	for name := range dgs {
		names = append(names, name)
	}

	return names, nil
}

// ListDeploymentGroupDetails returns all deployment groups for an application.
func (b *InMemoryBackend) ListDeploymentGroupDetails(appName string) ([]*DeploymentGroup, error) {
	b.mu.RLock("ListDeploymentGroupDetails")
	defer b.mu.RUnlock()

	if _, ok := b.applications[appName]; !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	dgs, ok := b.deploymentGroups[appName]
	if !ok {
		return []*DeploymentGroup{}, nil
	}

	list := make([]*DeploymentGroup, 0, len(dgs))
	for _, dg := range dgs {
		cp := *dg
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteDeploymentGroup deletes a deployment group.
func (b *InMemoryBackend) DeleteDeploymentGroup(appName, dgName string) error {
	b.mu.Lock("DeleteDeploymentGroup")
	defer b.mu.Unlock()

	if _, ok := b.applications[appName]; !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	dgs, ok := b.deploymentGroups[appName]
	if !ok {
		return fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	if _, exists := dgs[dgName]; !exists {
		return fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	delete(dgs, dgName)

	return nil
}

// CreateDeployment creates a new deployment.
func (b *InMemoryBackend) CreateDeployment(appName, dgName, description, creator string) (*Deployment, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	if _, ok := b.applications[appName]; !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	dgs, ok := b.deploymentGroups[appName]
	if !ok {
		return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	if _, exists := dgs[dgName]; !exists {
		return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	if creator == "" {
		creator = "user"
	}

	// Use a shortened UUID for the deployment ID in the expected format.
	deployID := "d-" + uuid.NewString()[:9]
	now := time.Now().UTC()
	completed := now.Add(simulatedDeployDuration)

	d := &Deployment{
		DeploymentID:        deployID,
		ApplicationName:     appName,
		DeploymentGroupName: dgName,
		Status:              "Succeeded",
		Creator:             creator,
		Description:         description,
		CreateTime:          now,
		CompleteTime:        &completed,
		AccountID:           b.accountID,
		Region:              b.region,
	}
	b.deployments[deployID] = d

	cp := *d

	return &cp, nil
}

// GetDeployment returns a deployment by ID.
func (b *InMemoryBackend) GetDeployment(deploymentID string) (*Deployment, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	d, ok := b.deployments[deploymentID]
	if !ok {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	cp := *d

	return &cp, nil
}

// ListDeployments returns all deployment IDs, optionally filtered by app and group.
func (b *InMemoryBackend) ListDeployments(appName, dgName string) []string {
	b.mu.RLock("ListDeployments")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.deployments))

	for id, d := range b.deployments {
		if appName != "" && d.ApplicationName != appName {
			continue
		}

		if dgName != "" && d.DeploymentGroupName != dgName {
			continue
		}

		ids = append(ids, id)
	}

	return ids
}

// TagResource adds tags to an application by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	name := appNameFromARN(resourceARN)
	app, ok := b.applications[name]

	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	app.Tags.Merge(kv)

	return nil
}

// UntagResource removes tags from an application by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	name := appNameFromARN(resourceARN)
	app, ok := b.applications[name]

	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	app.Tags.DeleteKeys(keys)

	return nil
}

// ListTagsForResource returns the tags for an application by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	name := appNameFromARN(resourceARN)
	app, ok := b.applications[name]

	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	return app.Tags.Clone(), nil
}

// ApplicationARN builds an ARN for a CodeDeploy application.
func (b *InMemoryBackend) ApplicationARN(name string) string {
	return arn.Build("codedeploy", b.region, b.accountID, "application:"+name)
}

// appNameFromARN extracts the application name from a CodeDeploy application ARN.
// It tolerates mismatched account IDs (e.g. empty vs 000000000000 when
// the Terraform provider is configured with skip_requesting_account_id=true).
// ARN format: arn:aws:codedeploy:{region}:{account}:application:{name}.
func appNameFromARN(resourceARN string) string {
	// Split on ":" with a max of 7 parts:
	// ["arn","aws","codedeploy","{region}","{account}","application","{name}"]
	const arnParts = 7
	parts := strings.SplitN(resourceARN, ":", arnParts)

	if len(parts) == arnParts && parts[5] == "application" {
		return parts[6]
	}

	return ""
}

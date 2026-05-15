// Package codedeploy provides an in-memory implementation of the AWS CodeDeploy service.
package codedeploy

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	statusSucceeded       = "Succeeded"
	computePlatformServer = "Server"
)

// simulatedDeployDuration is the simulated time for a deployment to complete.
const simulatedDeployDuration = 5 * time.Second

// maxBatchRevisions is the maximum number of revisions accepted by BatchGetApplicationRevisions.
const maxBatchRevisions = 25

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
	// ErrDeploymentConfigNotFound is returned when a deployment config does not exist.
	ErrDeploymentConfigNotFound = awserr.New("DeploymentConfigDoesNotExistException", awserr.ErrNotFound)
	// ErrDeploymentConfigAlreadyExists is returned when a deployment config already exists.
	ErrDeploymentConfigAlreadyExists = awserr.New("DeploymentConfigAlreadyExistsException", awserr.ErrConflict)
	// ErrOnPremisesInstanceNotFound is returned when an on-premises instance does not exist.
	ErrOnPremisesInstanceNotFound = awserr.New("InstanceNameRequiredException", awserr.ErrNotFound)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("InvalidParameterValueException", awserr.ErrInvalidParameter)
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
	CreateTime           time.Time  `json:"createTime"`
	CompleteTime         *time.Time `json:"completeTime,omitempty"`
	DeploymentID         string     `json:"deploymentId"`
	ApplicationName      string     `json:"applicationName"`
	DeploymentGroupName  string     `json:"deploymentGroupName"`
	DeploymentConfigName string     `json:"deploymentConfigName"`
	Status               string     `json:"status"`
	Creator              string     `json:"creator"`
	Description          string     `json:"description,omitempty"`
	AccountID            string     `json:"-"`
	Region               string     `json:"-"`
}

// OnPremisesInstance represents an on-premises instance registered with CodeDeploy.
type OnPremisesInstance struct {
	RegisterTime   time.Time  `json:"registerTime"`
	DeregisterTime *time.Time `json:"deregisterTime,omitempty"`
	Tags           *tags.Tags `json:"-"`
	InstanceName   string     `json:"instanceName"`
	IamSessionArn  string     `json:"iamSessionArn,omitempty"`
	IamUserArn     string     `json:"iamUserArn,omitempty"`
}

// DeploymentConfig represents a CodeDeploy deployment configuration.
type DeploymentConfig struct {
	CreateTime           time.Time `json:"createTime"`
	DeploymentConfigName string    `json:"deploymentConfigName"`
	DeploymentConfigID   string    `json:"deploymentConfigId"`
	ComputePlatform      string    `json:"computePlatform"`
}

// InMemoryBackend is the in-memory store for CodeDeploy resources.
type InMemoryBackend struct {
	applications        map[string]*Application
	deploymentGroups    map[string]map[string]*DeploymentGroup // appName -> dgName -> DG
	deployments         map[string]*Deployment
	onPremisesInstances map[string]*OnPremisesInstance
	deploymentConfigs   map[string]*DeploymentConfig
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
}

// NewInMemoryBackend creates a new in-memory CodeDeploy backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications:        make(map[string]*Application),
		deploymentGroups:    make(map[string]map[string]*DeploymentGroup),
		deployments:         make(map[string]*Deployment),
		onPremisesInstances: make(map[string]*OnPremisesInstance),
		deploymentConfigs:   make(map[string]*DeploymentConfig),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("codedeploy"),
	}
}

// Reset clears all state, returning the backend to a fresh empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, app := range b.applications {
		if app.Tags != nil {
			app.Tags.Close()
		}
	}

	for _, dgs := range b.deploymentGroups {
		for _, dg := range dgs {
			if dg.Tags != nil {
				dg.Tags.Close()
			}
		}
	}

	for _, inst := range b.onPremisesInstances {
		if inst.Tags != nil {
			inst.Tags.Close()
		}
	}

	b.applications = make(map[string]*Application)
	b.deploymentGroups = make(map[string]map[string]*DeploymentGroup)
	b.deployments = make(map[string]*Deployment)
	b.onPremisesInstances = make(map[string]*OnPremisesInstance)
	b.deploymentConfigs = make(map[string]*DeploymentConfig)
}

// ensureTags returns the given tags if non-nil, or creates a new tags.Tags with the given key.
func ensureTags(existing *tags.Tags, key string) *tags.Tags {
	if existing != nil {
		return existing
	}

	return tags.New(key)
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

// ListApplications returns all application names in sorted order.
func (b *InMemoryBackend) ListApplications() []string {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.applications))
	for name := range b.applications {
		names = append(names, name)
	}

	sort.Strings(names)

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

	app, ok := b.applications[name]
	if !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	app.Tags.Close()
	for _, dg := range b.deploymentGroups[name] {
		dg.Tags.Close()
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

// ListDeploymentGroups returns all deployment group names for an application in sorted order.
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

	sort.Strings(names)

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

	dg, exists := dgs[dgName]
	if !exists {
		return fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	dg.Tags.Close()
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

	dg, exists := dgs[dgName]
	if !exists {
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
		DeploymentID:         deployID,
		ApplicationName:      appName,
		DeploymentGroupName:  dgName,
		DeploymentConfigName: dg.DeploymentConfigName,
		Status:               statusSucceeded,
		Creator:              creator,
		Description:          description,
		CreateTime:           now,
		CompleteTime:         &completed,
		AccountID:            b.accountID,
		Region:               b.region,
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

// ListDeployments returns all deployment IDs in sorted order, optionally filtered by app and group.
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

	sort.Strings(ids)

	return ids
}

// TagResource adds tags to a resource (application or deployment group) by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.findResourceTagsLocked(resourceARN)
	if err != nil {
		return err
	}

	t.Merge(kv)

	return nil
}

// UntagResource removes tags from a resource (application or deployment group) by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.findResourceTagsLocked(resourceARN)
	if err != nil {
		return err
	}

	t.DeleteKeys(keys)

	return nil
}

// ListTagsForResource returns the tags for a resource (application or deployment group) by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.findResourceTagsLocked(resourceARN)
	if err != nil {
		return nil, err
	}

	return t.Clone(), nil
}

// findResourceTagsLocked looks up the tags.Tags for a resource ARN.
// Supports application ARNs (arn:…:application:{name}) and deployment group ARNs
// (arn:…:deploymentgroup:{appName}/{groupName}).
// The caller must hold at least a read lock on b.mu before calling this method.
func (b *InMemoryBackend) findResourceTagsLocked(resourceARN string) (*tags.Tags, error) {
	const arnParts = 7

	parts := strings.SplitN(resourceARN, ":", arnParts)
	if len(parts) != arnParts {
		return nil, fmt.Errorf("%w: invalid ARN %s", ErrNotFound, resourceARN)
	}

	resourceType := parts[5]
	resourceID := parts[6]

	switch resourceType {
	case "application":
		app, ok := b.applications[resourceID]
		if !ok {
			return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, resourceID)
		}

		return app.Tags, nil

	case "deploymentgroup":
		// deploymentgroup resource ID is "{appName}/{groupName}"
		appName, dgName, ok := strings.Cut(resourceID, "/")
		if !ok {
			return nil, fmt.Errorf("%w: invalid deployment group ARN %s", ErrNotFound, resourceARN)
		}

		dgs, ok := b.deploymentGroups[appName]
		if !ok {
			return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
		}

		dg, ok := dgs[dgName]
		if !ok {
			return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
		}

		return dg.Tags, nil

	default:
		return nil, fmt.Errorf("%w: unsupported resource type %s", ErrNotFound, resourceType)
	}
}

// ApplicationARN builds an ARN for a CodeDeploy application.
func (b *InMemoryBackend) ApplicationARN(name string) string {
	return arn.Build("codedeploy", b.region, b.accountID, "application:"+name)
}

// DeploymentGroupARN builds an ARN for a CodeDeploy deployment group.
func (b *InMemoryBackend) DeploymentGroupARN(appName, dgName string) string {
	return arn.Build("codedeploy", b.region, b.accountID, "deploymentgroup:"+appName+"/"+dgName)
}

// DeploymentConfigARN builds an ARN for a CodeDeploy deployment configuration.
func (b *InMemoryBackend) DeploymentConfigARN(name string) string {
	return arn.Build("codedeploy", b.region, b.accountID, "deploymentconfig:"+name)
}

// validComputePlatforms lists the accepted CodeDeploy compute platforms.
func validComputePlatforms() map[string]struct{} {
	return map[string]struct{}{
		computePlatformServer: {},
		"Lambda":              {},
		"ECS":                 {},
	}
}

// AddTagsToOnPremisesInstances adds tags to on-premises instances, registering them if needed.
func (b *InMemoryBackend) AddTagsToOnPremisesInstances(instanceNames []string, kv map[string]string) error {
	b.mu.Lock("AddTagsToOnPremisesInstances")
	defer b.mu.Unlock()

	for _, name := range instanceNames {
		inst, ok := b.onPremisesInstances[name]
		if !ok {
			t := tags.New("codedeploy.onprem." + name + ".tags")
			inst = &OnPremisesInstance{
				InstanceName: name,
				RegisterTime: time.Now().UTC(),
				Tags:         t,
			}
			b.onPremisesInstances[name] = inst
		}

		inst.Tags.Merge(kv)
	}

	return nil
}

// BatchGetApplicationRevisions validates that the application exists.
// It accepts up to maxBatchRevisions revisions per AWS spec.
func (b *InMemoryBackend) BatchGetApplicationRevisions(appName string, count int) (string, error) {
	b.mu.RLock("BatchGetApplicationRevisions")
	defer b.mu.RUnlock()

	if _, ok := b.applications[appName]; !ok {
		return "", fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	if count > maxBatchRevisions {
		return "", fmt.Errorf("%w: at most %d revisions can be requested at once, got %d",
			ErrValidation, maxBatchRevisions, count)
	}

	return appName, nil
}

// BatchGetApplications returns application structs for the given names.
// Names that do not exist are silently omitted (AWS behavior).
func (b *InMemoryBackend) BatchGetApplications(names []string) []*Application {
	b.mu.RLock("BatchGetApplications")
	defer b.mu.RUnlock()

	result := make([]*Application, 0, len(names))

	for _, name := range names {
		app, ok := b.applications[name]
		if !ok {
			continue
		}

		cp := *app
		result = append(result, &cp)
	}

	return result
}

// BatchGetDeploymentGroups returns deployment group info for the given names under an app.
// Groups that do not exist are silently omitted.
func (b *InMemoryBackend) BatchGetDeploymentGroups(appName string, dgNames []string) ([]*DeploymentGroup, error) {
	b.mu.RLock("BatchGetDeploymentGroups")
	defer b.mu.RUnlock()

	if _, ok := b.applications[appName]; !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	dgs := b.deploymentGroups[appName]
	result := make([]*DeploymentGroup, 0, len(dgNames))

	for _, name := range dgNames {
		dg, ok := dgs[name]
		if !ok {
			continue
		}

		cp := *dg
		result = append(result, &cp)
	}

	return result, nil
}

// BatchGetDeploymentInstances returns stub instance summaries for the given instance IDs.
// This is a deprecated operation; returns empty summaries for found deployments.
func (b *InMemoryBackend) BatchGetDeploymentInstances(
	deploymentID string,
	instanceIDs []string,
) ([]InstanceSummaryItem, string) {
	b.mu.RLock("BatchGetDeploymentInstances")
	defer b.mu.RUnlock()

	d, ok := b.deployments[deploymentID]
	if !ok {
		return nil, fmt.Sprintf("deployment %s not found", deploymentID)
	}

	result := make([]InstanceSummaryItem, 0, len(instanceIDs))

	for _, id := range instanceIDs {
		result = append(result, InstanceSummaryItem{
			DeploymentID: d.DeploymentID,
			InstanceID:   id,
			Status:       statusSucceeded,
		})
	}

	return result, ""
}

// BatchGetDeploymentTargets returns stub deployment targets for the given target IDs.
func (b *InMemoryBackend) BatchGetDeploymentTargets(
	deploymentID string,
	targetIDs []string,
) ([]*DeploymentTargetItem, error) {
	b.mu.RLock("BatchGetDeploymentTargets")
	defer b.mu.RUnlock()

	if _, ok := b.deployments[deploymentID]; !ok {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	result := make([]*DeploymentTargetItem, 0, len(targetIDs))

	for _, id := range targetIDs {
		result = append(result, &DeploymentTargetItem{
			DeploymentID: deploymentID,
			TargetID:     id,
			Status:       statusSucceeded,
			TargetType:   "instanceTarget",
		})
	}

	return result, nil
}

// BatchGetDeployments returns deployment structs for the given IDs.
// Deployment IDs that do not exist are silently omitted.
// Shallow copies are returned, consistent with GetDeployment. The pointer field
// CompleteTime shares the same [time.Time] value, which is immutable (value semantics).
func (b *InMemoryBackend) BatchGetDeployments(deploymentIDs []string) []*Deployment {
	b.mu.RLock("BatchGetDeployments")
	defer b.mu.RUnlock()

	result := make([]*Deployment, 0, len(deploymentIDs))

	for _, id := range deploymentIDs {
		d, ok := b.deployments[id]
		if !ok {
			continue
		}

		cp := *d
		result = append(result, &cp)
	}

	return result
}

// BatchGetOnPremisesInstances returns on-premises instance info for the given names.
// Names that do not exist are silently omitted.
// Shallow copies are returned. The Tags field pointer is shared with the backend store;
// callers must not mutate tags through the returned pointer (use TagResource instead).
func (b *InMemoryBackend) BatchGetOnPremisesInstances(instanceNames []string) []*OnPremisesInstance {
	b.mu.RLock("BatchGetOnPremisesInstances")
	defer b.mu.RUnlock()

	result := make([]*OnPremisesInstance, 0, len(instanceNames))

	for _, name := range instanceNames {
		inst, ok := b.onPremisesInstances[name]
		if !ok {
			continue
		}

		cp := *inst
		result = append(result, &cp)
	}

	return result
}

// ContinueDeployment marks a blue/green deployment as continuing past the wait point.
// In this in-memory implementation the deployment status is unchanged.
func (b *InMemoryBackend) ContinueDeployment(deploymentID string) error {
	b.mu.Lock("ContinueDeployment")
	defer b.mu.Unlock()

	if _, ok := b.deployments[deploymentID]; !ok {
		return fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	return nil
}

// CreateDeploymentConfig creates a named deployment configuration.
func (b *InMemoryBackend) CreateDeploymentConfig(name, computePlatform string) (*DeploymentConfig, error) {
	b.mu.Lock("CreateDeploymentConfig")
	defer b.mu.Unlock()

	if _, ok := b.deploymentConfigs[name]; ok {
		return nil, fmt.Errorf("%w: deployment config %s already exists", ErrDeploymentConfigAlreadyExists, name)
	}

	if computePlatform == "" {
		computePlatform = computePlatformServer
	}

	if _, ok := validComputePlatforms()[computePlatform]; !ok {
		return nil, fmt.Errorf("%w: invalid computePlatform %q, must be Server, Lambda, or ECS",
			ErrValidation, computePlatform)
	}

	cfg := &DeploymentConfig{
		DeploymentConfigName: name,
		DeploymentConfigID:   uuid.NewString(),
		ComputePlatform:      computePlatform,
		CreateTime:           time.Now().UTC(),
	}
	b.deploymentConfigs[name] = cfg

	cp := *cfg

	return &cp, nil
}

// InstanceSummaryItem is a simplified deployment instance summary used by BatchGetDeploymentInstances.
type InstanceSummaryItem struct {
	DeploymentID string `json:"deploymentId"`
	InstanceID   string `json:"instanceId"`
	Status       string `json:"status"`
}

// DeploymentTargetItem is a simplified deployment target used by BatchGetDeploymentTargets.
type DeploymentTargetItem struct {
	DeploymentID string `json:"deploymentId"`
	TargetID     string `json:"targetId"`
	Status       string `json:"status"`
	TargetType   string `json:"targetType"`
}

// AddApplicationInternal adds an application directly to the backend without validation.
// Used for test seeding only.
func (b *InMemoryBackend) AddApplicationInternal(app *Application) {
	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	app.Tags = ensureTags(app.Tags, "codedeploy.application."+app.ApplicationName+".tags")

	if app.ApplicationID == "" {
		app.ApplicationID = uuid.NewString()
	}

	if app.CreationTime.IsZero() {
		app.CreationTime = time.Now().UTC()
	}

	b.applications[app.ApplicationName] = app
}

// AddDeploymentGroupInternal adds a deployment group directly to the backend without validation.
// Used for test seeding only.
func (b *InMemoryBackend) AddDeploymentGroupInternal(dg *DeploymentGroup) {
	b.mu.Lock("AddDeploymentGroupInternal")
	defer b.mu.Unlock()

	dg.Tags = ensureTags(dg.Tags, "codedeploy.dg."+dg.ApplicationName+"."+dg.DeploymentGroupName+".tags")

	if dg.DeploymentGroupID == "" {
		dg.DeploymentGroupID = uuid.NewString()
	}

	if _, ok := b.deploymentGroups[dg.ApplicationName]; !ok {
		b.deploymentGroups[dg.ApplicationName] = make(map[string]*DeploymentGroup)
	}

	b.deploymentGroups[dg.ApplicationName][dg.DeploymentGroupName] = dg
}

// AddDeploymentInternal adds a deployment directly to the backend without validation.
// Used for test seeding only.
func (b *InMemoryBackend) AddDeploymentInternal(d *Deployment) {
	b.mu.Lock("AddDeploymentInternal")
	defer b.mu.Unlock()

	if d.DeploymentID == "" {
		d.DeploymentID = "d-" + uuid.NewString()[:9]
	}

	if d.CreateTime.IsZero() {
		d.CreateTime = time.Now().UTC()
	}

	b.deployments[d.DeploymentID] = d
}

// AddOnPremisesInstanceInternal adds an on-premises instance directly to the backend.
// Used for test seeding only.
func (b *InMemoryBackend) AddOnPremisesInstanceInternal(inst *OnPremisesInstance) {
	b.mu.Lock("AddOnPremisesInstanceInternal")
	defer b.mu.Unlock()

	inst.Tags = ensureTags(inst.Tags, "codedeploy.onprem."+inst.InstanceName+".tags")

	if inst.RegisterTime.IsZero() {
		inst.RegisterTime = time.Now().UTC()
	}

	b.onPremisesInstances[inst.InstanceName] = inst
}

// UpdateApplication renames a CodeDeploy application.
func (b *InMemoryBackend) UpdateApplication(name, newName string) error {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
	if !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	if newName == "" || newName == name {
		return nil
	}

	if _, exists := b.applications[newName]; exists {
		return fmt.Errorf("%w: application %s already exists", ErrAlreadyExists, newName)
	}

	app.ApplicationName = newName
	b.applications[newName] = app
	delete(b.applications, name)

	if dgs, ok2 := b.deploymentGroups[name]; ok2 {
		b.deploymentGroups[newName] = dgs
		delete(b.deploymentGroups, name)
	}

	return nil
}

// StopDeployment marks a deployment as Stopped.
func (b *InMemoryBackend) StopDeployment(deploymentID string) error {
	b.mu.Lock("StopDeployment")
	defer b.mu.Unlock()

	d, ok := b.deployments[deploymentID]
	if !ok {
		return fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	d.Status = "Stopped"

	return nil
}

// GetDeploymentConfig returns a deployment configuration by name.
func (b *InMemoryBackend) GetDeploymentConfig(name string) (*DeploymentConfig, error) {
	b.mu.RLock("GetDeploymentConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.deploymentConfigs[name]
	if !ok {
		return nil, fmt.Errorf("%w: deployment config %s not found", ErrDeploymentConfigNotFound, name)
	}

	cp := *cfg

	return &cp, nil
}

// ListDeploymentConfigs returns all deployment config names in sorted order.
func (b *InMemoryBackend) ListDeploymentConfigs() []string {
	b.mu.RLock("ListDeploymentConfigs")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.deploymentConfigs))
	for name := range b.deploymentConfigs {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// DeleteDeploymentConfig deletes a deployment configuration by name.
func (b *InMemoryBackend) DeleteDeploymentConfig(name string) error {
	b.mu.Lock("DeleteDeploymentConfig")
	defer b.mu.Unlock()

	if _, ok := b.deploymentConfigs[name]; !ok {
		return fmt.Errorf("%w: deployment config %s not found", ErrDeploymentConfigNotFound, name)
	}

	delete(b.deploymentConfigs, name)

	return nil
}

// RemoveTagsFromOnPremisesInstances removes tag keys from the given on-premises instances.
func (b *InMemoryBackend) RemoveTagsFromOnPremisesInstances(instanceNames []string, keys []string) error {
	b.mu.Lock("RemoveTagsFromOnPremisesInstances")
	defer b.mu.Unlock()

	for _, name := range instanceNames {
		inst, ok := b.onPremisesInstances[name]
		if !ok {
			continue
		}

		inst.Tags.DeleteKeys(keys)
	}

	return nil
}

// RegisterOnPremisesInstance registers a new on-premises instance.
func (b *InMemoryBackend) RegisterOnPremisesInstance(name, iamSessionArn, iamUserArn string) {
	b.mu.Lock("RegisterOnPremisesInstance")
	defer b.mu.Unlock()

	if _, ok := b.onPremisesInstances[name]; ok {
		return
	}

	t := tags.New("codedeploy.onprem." + name + ".tags")
	b.onPremisesInstances[name] = &OnPremisesInstance{
		InstanceName:  name,
		IamSessionArn: iamSessionArn,
		IamUserArn:    iamUserArn,
		RegisterTime:  time.Now().UTC(),
		Tags:          t,
	}
}

// DeregisterOnPremisesInstance marks an on-premises instance as deregistered.
func (b *InMemoryBackend) DeregisterOnPremisesInstance(name string) error {
	b.mu.Lock("DeregisterOnPremisesInstance")
	defer b.mu.Unlock()

	inst, ok := b.onPremisesInstances[name]
	if !ok {
		return fmt.Errorf("%w: instance %s not found", ErrOnPremisesInstanceNotFound, name)
	}

	now := time.Now().UTC()
	inst.DeregisterTime = &now

	return nil
}

// GetOnPremisesInstance returns an on-premises instance by name.
func (b *InMemoryBackend) GetOnPremisesInstance(name string) (*OnPremisesInstance, error) {
	b.mu.RLock("GetOnPremisesInstance")
	defer b.mu.RUnlock()

	inst, ok := b.onPremisesInstances[name]
	if !ok {
		return nil, fmt.Errorf("%w: instance %s not found", ErrOnPremisesInstanceNotFound, name)
	}

	cp := *inst

	return &cp, nil
}

// ListOnPremisesInstances returns instance names, optionally filtered by registration status.
func (b *InMemoryBackend) ListOnPremisesInstances(registrationStatus string) []string {
	b.mu.RLock("ListOnPremisesInstances")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.onPremisesInstances))
	for name, inst := range b.onPremisesInstances {
		if registrationStatus == "Deregistered" && inst.DeregisterTime == nil {
			continue
		}
		if registrationStatus == "Registered" && inst.DeregisterTime != nil {
			continue
		}
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// AddDeploymentConfigInternal adds a deployment config directly to the backend without validation.
// Used for test seeding only.
func (b *InMemoryBackend) AddDeploymentConfigInternal(cfg *DeploymentConfig) {
	b.mu.Lock("AddDeploymentConfigInternal")
	defer b.mu.Unlock()

	if cfg.DeploymentConfigID == "" {
		cfg.DeploymentConfigID = uuid.NewString()
	}

	if cfg.CreateTime.IsZero() {
		cfg.CreateTime = time.Now().UTC()
	}

	b.deploymentConfigs[cfg.DeploymentConfigName] = cfg
}

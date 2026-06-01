package opsworks

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	errResourceNotFound = "ResourceNotFoundException"
	errValidation       = "ValidationException"

	instanceStatusStopped  = "stopped"
	instanceStatusStarting = "starting"
	instanceStatusStopping = "stopping"
	instanceStatusOnline   = "online"

	deploymentStatusRunning    = "running"
	deploymentStatusSuccessful = "successful"

	commandStatusSuccessful = "successful"

	maxTagsPerPage = 200
)

var (
	// ErrStackNotFound is returned when a stack does not exist.
	ErrStackNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrLayerNotFound is returned when a layer does not exist.
	ErrLayerNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrInstanceNotFound is returned when an instance does not exist.
	ErrInstanceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAppNotFound is returned when an app does not exist.
	ErrAppNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDeploymentNotFound is returned when a deployment does not exist.
	ErrDeploymentNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCommandNotFound is returned when a command does not exist.
	ErrCommandNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
)

// storedStack holds a stack with all fields.
type storedStack struct {
	CreatedAt                 time.Time         `json:"createdAt"`
	Tags                      map[string]string `json:"tags"`
	StackID                   string            `json:"stackId"`
	Arn                       string            `json:"arn"`
	Name                      string            `json:"name"`
	Region                    string            `json:"region"`
	DefaultInstanceProfileArn string            `json:"defaultInstanceProfileArn"`
	ServiceRoleArn            string            `json:"serviceRoleArn"`
	Status                    string            `json:"status"`
}

func (s *storedStack) toStack() *Stack {
	tags := make(map[string]string, len(s.Tags))
	maps.Copy(tags, s.Tags)

	return &Stack{
		CreatedAt:                 s.CreatedAt,
		Tags:                      tags,
		StackID:                   s.StackID,
		Arn:                       s.Arn,
		Name:                      s.Name,
		Region:                    s.Region,
		DefaultInstanceProfileArn: s.DefaultInstanceProfileArn,
		ServiceRoleArn:            s.ServiceRoleArn,
		Status:                    s.Status,
	}
}

// storedLayer holds a layer with all fields.
type storedLayer struct {
	CreatedAt time.Time `json:"createdAt"`
	StackID   string    `json:"stackId"`
	LayerID   string    `json:"layerId"`
	Arn       string    `json:"arn"`
	Type      string    `json:"type"`
	Name      string    `json:"name"`
	Shortname string    `json:"shortname"`
}

func (l *storedLayer) toLayer() *Layer {
	return &Layer{
		CreatedAt: l.CreatedAt,
		StackID:   l.StackID,
		LayerID:   l.LayerID,
		Arn:       l.Arn,
		Type:      l.Type,
		Name:      l.Name,
		Shortname: l.Shortname,
	}
}

// storedInstance holds an instance with all fields.
type storedInstance struct {
	CreatedAt    time.Time `json:"createdAt"`
	StackID      string    `json:"stackId"`
	LayerID      string    `json:"layerId"`
	InstanceID   string    `json:"instanceId"`
	Arn          string    `json:"arn"`
	Hostname     string    `json:"hostname"`
	InstanceType string    `json:"instanceType"`
	Status       string    `json:"status"`
}

func (i *storedInstance) toInstance() *Instance {
	return &Instance{
		CreatedAt:    i.CreatedAt,
		StackID:      i.StackID,
		LayerID:      i.LayerID,
		InstanceID:   i.InstanceID,
		Arn:          i.Arn,
		Hostname:     i.Hostname,
		InstanceType: i.InstanceType,
		Status:       i.Status,
	}
}

// storedApp holds an app with all fields.
type storedApp struct {
	CreatedAt time.Time `json:"createdAt"`
	StackID   string    `json:"stackId"`
	AppID     string    `json:"appId"`
	Arn       string    `json:"arn"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
}

func (a *storedApp) toApp() *App {
	return &App{
		CreatedAt: a.CreatedAt,
		StackID:   a.StackID,
		AppID:     a.AppID,
		Arn:       a.Arn,
		Name:      a.Name,
		Type:      a.Type,
	}
}

// storedDeployment holds a deployment with all fields.
type storedDeployment struct {
	CreatedAt    time.Time `json:"createdAt"`
	CompletedAt  time.Time `json:"completedAt"`
	StackID      string    `json:"stackId"`
	AppID        string    `json:"appId"`
	DeploymentID string    `json:"deploymentId"`
	Command      string    `json:"command"`
	Status       string    `json:"status"`
	Duration     int32     `json:"duration"`
}

func (d *storedDeployment) toDeployment() *Deployment {
	return &Deployment{
		CreatedAt:    d.CreatedAt,
		CompletedAt:  d.CompletedAt,
		StackID:      d.StackID,
		AppID:        d.AppID,
		DeploymentID: d.DeploymentID,
		Command:      d.Command,
		Status:       d.Status,
		Duration:     d.Duration,
	}
}

// storedCommand holds a command with all fields.
type storedCommand struct {
	CreatedAt      time.Time `json:"createdAt"`
	AcknowledgedAt time.Time `json:"acknowledgedAt"`
	CompletedAt    time.Time `json:"completedAt"`
	DeploymentID   string    `json:"deploymentId"`
	InstanceID     string    `json:"instanceId"`
	CommandID      string    `json:"commandId"`
	Type           string    `json:"type"`
	Status         string    `json:"status"`
	LogURL         string    `json:"logUrl"`
	ExitCode       int32     `json:"exitCode"`
}

func (c *storedCommand) toCommand() *Command {
	return &Command{
		CreatedAt:      c.CreatedAt,
		AcknowledgedAt: c.AcknowledgedAt,
		CompletedAt:    c.CompletedAt,
		DeploymentID:   c.DeploymentID,
		InstanceID:     c.InstanceID,
		CommandID:      c.CommandID,
		Type:           c.Type,
		Status:         c.Status,
		LogURL:         c.LogURL,
		ExitCode:       c.ExitCode,
	}
}

// snapshot holds serializable backend state.
type snapshot struct {
	Stacks      map[string]*storedStack      `json:"stacks"`
	Layers      map[string]*storedLayer      `json:"layers"`
	Instances   map[string]*storedInstance   `json:"instances"`
	Apps        map[string]*storedApp        `json:"apps"`
	Deployments map[string]*storedDeployment `json:"deployments"`
	Commands    map[string]*storedCommand    `json:"commands"`
	Tags        map[string]map[string]string `json:"tags"`
}

// InMemoryBackend is an in-memory OpsWorks backend.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	stacks      map[string]*storedStack
	layers      map[string]*storedLayer
	instances   map[string]*storedInstance
	apps        map[string]*storedApp
	deployments map[string]*storedDeployment
	commands    map[string]*storedCommand
	tags        map[string]map[string]string
	accountID   string
	region      string
}

// NewInMemoryBackend creates a new in-memory OpsWorks backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:          lockmetrics.New("opsworks"),
		stacks:      make(map[string]*storedStack),
		layers:      make(map[string]*storedLayer),
		instances:   make(map[string]*storedInstance),
		apps:        make(map[string]*storedApp),
		deployments: make(map[string]*storedDeployment),
		commands:    make(map[string]*storedCommand),
		tags:        make(map[string]map[string]string),
		accountID:   accountID,
		region:      region,
	}
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.stacks = make(map[string]*storedStack)
	b.layers = make(map[string]*storedLayer)
	b.instances = make(map[string]*storedInstance)
	b.apps = make(map[string]*storedApp)
	b.deployments = make(map[string]*storedDeployment)
	b.commands = make(map[string]*storedCommand)
	b.tags = make(map[string]map[string]string)
}

// Snapshot serializes the backend state.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(snapshot{
		Stacks:      b.stacks,
		Layers:      b.layers,
		Instances:   b.instances,
		Apps:        b.apps,
		Deployments: b.deployments,
		Commands:    b.commands,
		Tags:        b.tags,
	})

	return data
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.stacks = s.Stacks
	b.layers = s.Layers
	b.instances = s.Instances
	b.apps = s.Apps
	b.deployments = s.Deployments
	b.commands = s.Commands
	b.tags = s.Tags

	return nil
}

func (b *InMemoryBackend) stackARN(stackID string) string {
	return fmt.Sprintf("arn:aws:opsworks:%s:%s:stack/%s", b.region, b.accountID, stackID)
}

func (b *InMemoryBackend) layerARN(layerID string) string {
	return fmt.Sprintf("arn:aws:opsworks:%s:%s:layer/%s", b.region, b.accountID, layerID)
}

func (b *InMemoryBackend) instanceARN(instanceID string) string {
	return fmt.Sprintf("arn:aws:opsworks:%s:%s:instance/%s", b.region, b.accountID, instanceID)
}

func (b *InMemoryBackend) appARN(appID string) string {
	return fmt.Sprintf("arn:aws:opsworks:%s:%s:app/%s", b.region, b.accountID, appID)
}

// CreateStack creates a new OpsWorks stack.
func (b *InMemoryBackend) CreateStack(
	name, region, defaultInstanceProfileArn, serviceRoleArn string,
) (*Stack, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateStack")
	defer b.mu.Unlock()

	id := uuid.NewString()
	now := time.Now().UTC()
	arn := b.stackARN(id)

	s := &storedStack{
		CreatedAt:                 now,
		Tags:                      make(map[string]string),
		StackID:                   id,
		Arn:                       arn,
		Name:                      name,
		Region:                    region,
		DefaultInstanceProfileArn: defaultInstanceProfileArn,
		ServiceRoleArn:            serviceRoleArn,
		Status:                    "running",
	}
	b.stacks[id] = s

	return s.toStack(), nil
}

// DescribeStacks returns stacks, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeStacks(stackIDs []string) ([]*Stack, error) {
	b.mu.RLock("DescribeStacks")
	defer b.mu.RUnlock()

	if len(stackIDs) > 0 {
		result := make([]*Stack, 0, len(stackIDs))
		for _, id := range stackIDs {
			s, ok := b.stacks[id]
			if !ok {
				return nil, ErrStackNotFound
			}
			result = append(result, s.toStack())
		}

		return result, nil
	}

	result := make([]*Stack, 0, len(b.stacks))
	for _, s := range b.stacks {
		result = append(result, s.toStack())
	}

	return result, nil
}

// UpdateStack updates a stack's name.
func (b *InMemoryBackend) UpdateStack(stackID, name string) error {
	b.mu.Lock("UpdateStack")
	defer b.mu.Unlock()

	s, ok := b.stacks[stackID]
	if !ok {
		return ErrStackNotFound
	}

	if name != "" {
		s.Name = name
	}

	return nil
}

// DeleteStack deletes a stack.
func (b *InMemoryBackend) DeleteStack(stackID string) error {
	b.mu.Lock("DeleteStack")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return ErrStackNotFound
	}

	delete(b.stacks, stackID)

	return nil
}

// CreateLayer creates a new layer in a stack.
func (b *InMemoryBackend) CreateLayer(stackID, layerType, name, shortname string) (*Layer, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateLayer")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return nil, ErrStackNotFound
	}

	id := uuid.NewString()
	now := time.Now().UTC()

	l := &storedLayer{
		CreatedAt: now,
		StackID:   stackID,
		LayerID:   id,
		Arn:       b.layerARN(id),
		Type:      layerType,
		Name:      name,
		Shortname: shortname,
	}
	b.layers[id] = l

	return l.toLayer(), nil
}

// DescribeLayers returns layers filtered by stack and/or layer IDs.
func (b *InMemoryBackend) DescribeLayers(stackID string, layerIDs []string) ([]*Layer, error) {
	b.mu.RLock("DescribeLayers")
	defer b.mu.RUnlock()

	if len(layerIDs) > 0 {
		result := make([]*Layer, 0, len(layerIDs))
		for _, id := range layerIDs {
			l, ok := b.layers[id]
			if !ok {
				return nil, ErrLayerNotFound
			}
			result = append(result, l.toLayer())
		}

		return result, nil
	}

	result := make([]*Layer, 0)
	for _, l := range b.layers {
		if stackID == "" || l.StackID == stackID {
			result = append(result, l.toLayer())
		}
	}

	return result, nil
}

// UpdateLayer updates a layer's name.
func (b *InMemoryBackend) UpdateLayer(layerID, name string) error {
	b.mu.Lock("UpdateLayer")
	defer b.mu.Unlock()

	l, ok := b.layers[layerID]
	if !ok {
		return ErrLayerNotFound
	}

	if name != "" {
		l.Name = name
	}

	return nil
}

// DeleteLayer deletes a layer.
func (b *InMemoryBackend) DeleteLayer(layerID string) error {
	b.mu.Lock("DeleteLayer")
	defer b.mu.Unlock()

	if _, ok := b.layers[layerID]; !ok {
		return ErrLayerNotFound
	}

	delete(b.layers, layerID)

	return nil
}

// CreateInstance creates a new instance in a stack/layer.
func (b *InMemoryBackend) CreateInstance(stackID, layerID, instanceType string) (*Instance, error) {
	b.mu.Lock("CreateInstance")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return nil, ErrStackNotFound
	}

	id := uuid.NewString()
	now := time.Now().UTC()

	hostname := fmt.Sprintf("gopherstack%d", len(b.instances)+1)
	i := &storedInstance{
		CreatedAt:    now,
		StackID:      stackID,
		LayerID:      layerID,
		InstanceID:   id,
		Arn:          b.instanceARN(id),
		Hostname:     hostname,
		InstanceType: instanceType,
		Status:       instanceStatusStopped,
	}
	b.instances[id] = i

	return i.toInstance(), nil
}

// DescribeInstances returns instances filtered by stack, layer, or IDs.
func (b *InMemoryBackend) DescribeInstances(stackID, layerID string, instanceIDs []string) ([]*Instance, error) {
	b.mu.RLock("DescribeInstances")
	defer b.mu.RUnlock()

	if len(instanceIDs) > 0 {
		result := make([]*Instance, 0, len(instanceIDs))
		for _, id := range instanceIDs {
			i, ok := b.instances[id]
			if !ok {
				return nil, ErrInstanceNotFound
			}
			result = append(result, i.toInstance())
		}

		return result, nil
	}

	result := make([]*Instance, 0)
	for _, i := range b.instances {
		if stackID != "" && i.StackID != stackID {
			continue
		}
		if layerID != "" && i.LayerID != layerID {
			continue
		}
		result = append(result, i.toInstance())
	}

	return result, nil
}

// UpdateInstance updates an instance's hostname.
func (b *InMemoryBackend) UpdateInstance(instanceID, hostname string) error {
	b.mu.Lock("UpdateInstance")
	defer b.mu.Unlock()

	i, ok := b.instances[instanceID]
	if !ok {
		return ErrInstanceNotFound
	}

	if hostname != "" {
		i.Hostname = hostname
	}

	return nil
}

// DeleteInstance deletes an instance.
func (b *InMemoryBackend) DeleteInstance(instanceID string) error {
	b.mu.Lock("DeleteInstance")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceID]; !ok {
		return ErrInstanceNotFound
	}

	delete(b.instances, instanceID)

	return nil
}

// StartInstance transitions an instance to starting state.
func (b *InMemoryBackend) StartInstance(instanceID string) error {
	b.mu.Lock("StartInstance")
	defer b.mu.Unlock()

	i, ok := b.instances[instanceID]
	if !ok {
		return ErrInstanceNotFound
	}

	i.Status = instanceStatusStarting

	return nil
}

// StopInstance transitions an instance to stopping state.
func (b *InMemoryBackend) StopInstance(instanceID string) error {
	b.mu.Lock("StopInstance")
	defer b.mu.Unlock()

	i, ok := b.instances[instanceID]
	if !ok {
		return ErrInstanceNotFound
	}

	i.Status = instanceStatusStopping

	return nil
}

// RebootInstance transitions an instance to starting state.
func (b *InMemoryBackend) RebootInstance(instanceID string) error {
	b.mu.Lock("RebootInstance")
	defer b.mu.Unlock()

	i, ok := b.instances[instanceID]
	if !ok {
		return ErrInstanceNotFound
	}

	i.Status = instanceStatusStarting

	return nil
}

// CreateApp creates a new app in a stack.
func (b *InMemoryBackend) CreateApp(stackID, name, appType string) (*App, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return nil, ErrStackNotFound
	}

	id := uuid.NewString()
	now := time.Now().UTC()

	a := &storedApp{
		CreatedAt: now,
		StackID:   stackID,
		AppID:     id,
		Arn:       b.appARN(id),
		Name:      name,
		Type:      appType,
	}
	b.apps[id] = a

	return a.toApp(), nil
}

// DescribeApps returns apps filtered by stack and/or app IDs.
func (b *InMemoryBackend) DescribeApps(stackID string, appIDs []string) ([]*App, error) {
	b.mu.RLock("DescribeApps")
	defer b.mu.RUnlock()

	if len(appIDs) > 0 {
		result := make([]*App, 0, len(appIDs))
		for _, id := range appIDs {
			a, ok := b.apps[id]
			if !ok {
				return nil, ErrAppNotFound
			}
			result = append(result, a.toApp())
		}

		return result, nil
	}

	result := make([]*App, 0)
	for _, a := range b.apps {
		if stackID == "" || a.StackID == stackID {
			result = append(result, a.toApp())
		}
	}

	return result, nil
}

// UpdateApp updates an app's name.
func (b *InMemoryBackend) UpdateApp(appID, name string) error {
	b.mu.Lock("UpdateApp")
	defer b.mu.Unlock()

	a, ok := b.apps[appID]
	if !ok {
		return ErrAppNotFound
	}

	if name != "" {
		a.Name = name
	}

	return nil
}

// DeleteApp deletes an app.
func (b *InMemoryBackend) DeleteApp(appID string) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return ErrAppNotFound
	}

	delete(b.apps, appID)

	return nil
}

// CreateDeployment creates a new deployment.
func (b *InMemoryBackend) CreateDeployment(stackID, appID, command string) (*Deployment, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return nil, ErrStackNotFound
	}

	id := uuid.NewString()
	now := time.Now().UTC()

	d := &storedDeployment{
		CreatedAt:    now,
		CompletedAt:  now,
		StackID:      stackID,
		AppID:        appID,
		DeploymentID: id,
		Command:      command,
		Status:       deploymentStatusSuccessful,
		Duration:     1,
	}
	b.deployments[id] = d

	cmdID := uuid.NewString()
	cmd := &storedCommand{
		CreatedAt:      now,
		AcknowledgedAt: now,
		CompletedAt:    now,
		DeploymentID:   id,
		InstanceID:     "",
		CommandID:      cmdID,
		Type:           command,
		Status:         commandStatusSuccessful,
		ExitCode:       0,
	}
	b.commands[cmdID] = cmd

	return d.toDeployment(), nil
}

// DescribeDeployments returns deployments filtered by stack, app, or IDs.
func (b *InMemoryBackend) DescribeDeployments(stackID, appID string, deploymentIDs []string) ([]*Deployment, error) {
	b.mu.RLock("DescribeDeployments")
	defer b.mu.RUnlock()

	if len(deploymentIDs) > 0 {
		result := make([]*Deployment, 0, len(deploymentIDs))
		for _, id := range deploymentIDs {
			d, ok := b.deployments[id]
			if !ok {
				return nil, ErrDeploymentNotFound
			}
			result = append(result, d.toDeployment())
		}

		return result, nil
	}

	result := make([]*Deployment, 0)
	for _, d := range b.deployments {
		if stackID != "" && d.StackID != stackID {
			continue
		}
		if appID != "" && d.AppID != appID {
			continue
		}
		result = append(result, d.toDeployment())
	}

	return result, nil
}

// DescribeCommands returns commands filtered by deployment, instance, or IDs.
func (b *InMemoryBackend) DescribeCommands(deploymentID, instanceID string, commandIDs []string) ([]*Command, error) {
	b.mu.RLock("DescribeCommands")
	defer b.mu.RUnlock()

	if len(commandIDs) > 0 {
		result := make([]*Command, 0, len(commandIDs))
		for _, id := range commandIDs {
			c, ok := b.commands[id]
			if !ok {
				return nil, ErrCommandNotFound
			}
			result = append(result, c.toCommand())
		}

		return result, nil
	}

	result := make([]*Command, 0)
	for _, c := range b.commands {
		if deploymentID != "" && c.DeploymentID != deploymentID {
			continue
		}
		if instanceID != "" && c.InstanceID != instanceID {
			continue
		}
		result = append(result, c.toCommand())
	}

	return result, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrStackNotFound
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrStackNotFound
	}

	existing := b.tags[resourceARN]
	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTags lists tags for a resource with pagination.
func (b *InMemoryBackend) ListTags(
	resourceARN string,
	maxResults int32,
	nextToken string,
) (map[string]string, string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	if !b.resourceExists(resourceARN) {
		return nil, "", ErrStackNotFound
	}

	tags := make(map[string]string)
	maps.Copy(tags, b.tags[resourceARN])

	return tags, "", nil
}

// resourceExists checks if a resource ARN refers to a known resource.
func (b *InMemoryBackend) resourceExists(arn string) bool {
	if strings.Contains(arn, ":stack/") {
		id := arnSuffix(arn)
		_, ok := b.stacks[id]

		return ok
	}
	if strings.Contains(arn, ":layer/") {
		id := arnSuffix(arn)
		_, ok := b.layers[id]

		return ok
	}
	if strings.Contains(arn, ":instance/") {
		id := arnSuffix(arn)
		_, ok := b.instances[id]

		return ok
	}
	if strings.Contains(arn, ":app/") {
		id := arnSuffix(arn)
		_, ok := b.apps[id]

		return ok
	}

	return false
}

func arnSuffix(arn string) string {
	parts := strings.Split(arn, "/")
	if len(parts) == 0 {
		return ""
	}

	return parts[len(parts)-1]
}

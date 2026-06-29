package opsworks

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const (
	errResourceNotFound = "ResourceNotFoundException"
	errValidation       = "ValidationException"

	configManagerChef = "Chef"
	osTypeLinux       = "Linux"

	instanceStatusStopped  = "stopped"
	instanceStatusStarting = "starting"
	instanceStatusStopping = "stopping"
	instanceStatusOnline   = "online"

	deploymentStatusRunning    = "running"
	deploymentStatusSuccessful = "successful"

	commandStatusSuccessful = "successful"

	volumeStatusRegistered = "registered"

	ecsClusterStatusRegistered = "registered"
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
	// ErrUserProfileNotFound is returned when a user profile does not exist.
	ErrUserProfileNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrElasticLBNotFound is returned when an ELB is not found.
	ErrElasticLBNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrElasticIPNotFound is returned when an elastic IP is not found.
	ErrElasticIPNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrVolumeNotFound is returned when a volume is not found.
	ErrVolumeNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrRdsDBInstanceNotFound is returned when an RDS DB instance is not found.
	ErrRdsDBInstanceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrEcsClusterNotFound is returned when an ECS cluster is not found.
	ErrEcsClusterNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
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
	Registered   bool      `json:"registered"`
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
		Registered:   i.Registered,
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

// storedUserProfile holds a user profile.
type storedUserProfile struct {
	IamUserArn          string `json:"iamUserArn"`
	Name                string `json:"name"`
	SSHUsername         string `json:"sshUsername"`
	SSHPublicKey        string `json:"sshPublicKey"`
	AllowSelfManagement bool   `json:"allowSelfManagement"`
}

func (u *storedUserProfile) toUserProfile() *UserProfile {
	return &UserProfile{
		IamUserArn:          u.IamUserArn,
		Name:                u.Name,
		SSHUsername:         u.SSHUsername,
		SSHPublicKey:        u.SSHPublicKey,
		AllowSelfManagement: u.AllowSelfManagement,
	}
}

// storedElasticLoadBalancer represents an attached ELB.
type storedElasticLoadBalancer struct {
	ElasticLoadBalancerName string `json:"elasticLoadBalancerName"`
	Region                  string `json:"region"`
	DNSName                 string `json:"dnsName"`
	StackID                 string `json:"stackId"`
	LayerID                 string `json:"layerId"`
}

func (e *storedElasticLoadBalancer) toElasticLoadBalancer() *ElasticLoadBalancer {
	return &ElasticLoadBalancer{
		ElasticLoadBalancerName: e.ElasticLoadBalancerName,
		Region:                  e.Region,
		DNSName:                 e.DNSName,
		StackID:                 e.StackID,
		LayerID:                 e.LayerID,
	}
}

// storedElasticIP represents a registered elastic IP.
type storedElasticIP struct {
	IP         string `json:"ip"`
	Domain     string `json:"domain"`
	Name       string `json:"name"`
	Region     string `json:"region"`
	InstanceID string `json:"instanceId"`
}

func (e *storedElasticIP) toElasticIP() *ElasticIP {
	return &ElasticIP{
		IP:         e.IP,
		Domain:     e.Domain,
		Name:       e.Name,
		Region:     e.Region,
		InstanceID: e.InstanceID,
	}
}

// storedVolume represents a registered volume.
type storedVolume struct {
	RegisteredAt time.Time `json:"registeredAt"`
	VolumeID     string    `json:"volumeId"`
	Ec2VolumeID  string    `json:"ec2VolumeId"`
	StackID      string    `json:"stackId"`
	InstanceID   string    `json:"instanceId"`
	Name         string    `json:"name"`
	MountPoint   string    `json:"mountPoint"`
	Region       string    `json:"region"`
	Status       string    `json:"status"`
	Size         int32     `json:"size"`
}

func (v *storedVolume) toVolume() *Volume {
	return &Volume{
		RegisteredAt: v.RegisteredAt,
		VolumeID:     v.VolumeID,
		Ec2VolumeID:  v.Ec2VolumeID,
		StackID:      v.StackID,
		InstanceID:   v.InstanceID,
		Name:         v.Name,
		MountPoint:   v.MountPoint,
		Region:       v.Region,
		Status:       v.Status,
		Size:         v.Size,
	}
}

// storedRdsDBInstance represents a registered RDS DB instance.
type storedRdsDBInstance struct {
	RdsDBInstanceArn     string `json:"rdsDbInstanceArn"`
	DBInstanceIdentifier string `json:"dbInstanceIdentifier"`
	DBUser               string `json:"dbUser"`
	StackID              string `json:"stackId"`
	Region               string `json:"region"`
	Address              string `json:"address"`
}

func (r *storedRdsDBInstance) toRdsDBInstance() *RdsDBInstance {
	return &RdsDBInstance{
		RdsDBInstanceArn:     r.RdsDBInstanceArn,
		DBInstanceIdentifier: r.DBInstanceIdentifier,
		DBUser:               r.DBUser,
		StackID:              r.StackID,
		Region:               r.Region,
		Address:              r.Address,
	}
}

// storedEcsCluster represents a registered ECS cluster.
type storedEcsCluster struct {
	RegisteredAt   time.Time `json:"registeredAt"`
	EcsClusterArn  string    `json:"ecsClusterArn"`
	EcsClusterName string    `json:"ecsClusterName"`
	StackID        string    `json:"stackId"`
	Status         string    `json:"status"`
}

func (e *storedEcsCluster) toEcsCluster() *EcsCluster {
	return &EcsCluster{
		RegisteredAt:   e.RegisteredAt,
		EcsClusterArn:  e.EcsClusterArn,
		EcsClusterName: e.EcsClusterName,
		StackID:        e.StackID,
		Status:         e.Status,
	}
}

// storedPermission represents OpsWorks stack permissions.
type storedPermission struct {
	StackID    string `json:"stackId"`
	IamUserArn string `json:"iamUserArn"`
	Level      string `json:"level"`
	AllowSSH   bool   `json:"allowSsh"`
	AllowSudo  bool   `json:"allowSudo"`
}

func (p *storedPermission) toPermission() *Permission {
	return &Permission{
		StackID:    p.StackID,
		IamUserArn: p.IamUserArn,
		Level:      p.Level,
		AllowSSH:   p.AllowSSH,
		AllowSudo:  p.AllowSudo,
	}
}

// storedTimeBasedAutoScaling represents time-based auto scaling for an instance.
type storedTimeBasedAutoScaling struct {
	AutoScalingSchedule *AutoScalingSchedule `json:"autoScalingSchedule"`
	InstanceID          string               `json:"instanceId"`
}

func (t *storedTimeBasedAutoScaling) toTimeBasedAutoScaling() *TimeBasedAutoScaling {
	return &TimeBasedAutoScaling{
		AutoScalingSchedule: t.AutoScalingSchedule,
		InstanceID:          t.InstanceID,
	}
}

// storedLoadBasedAutoScaling represents load-based auto scaling for a layer.
type storedLoadBasedAutoScaling struct {
	UpScaling   *ScalingParameters `json:"upScaling"`
	DownScaling *ScalingParameters `json:"downScaling"`
	LayerID     string             `json:"layerId"`
	Enable      bool               `json:"enable"`
}

func (l *storedLoadBasedAutoScaling) toLoadBasedAutoScaling() *LoadBasedAutoScaling {
	return &LoadBasedAutoScaling{
		UpScaling:   l.UpScaling,
		DownScaling: l.DownScaling,
		LayerID:     l.LayerID,
		Enable:      l.Enable,
	}
}

// snapshot holds serializable backend state.
type snapshot struct {
	Stacks             map[string]*storedStack                `json:"stacks"`
	Layers             map[string]*storedLayer                `json:"layers"`
	Instances          map[string]*storedInstance             `json:"instances"`
	Apps               map[string]*storedApp                  `json:"apps"`
	Deployments        map[string]*storedDeployment           `json:"deployments"`
	Commands           map[string]*storedCommand              `json:"commands"`
	Tags               map[string]map[string]string           `json:"tags"`
	UserProfiles       map[string]*storedUserProfile          `json:"userProfiles"`
	ElasticLBs         map[string]*storedElasticLoadBalancer  `json:"elasticLBs"`
	ElasticIPs         map[string]*storedElasticIP            `json:"elasticIps"`
	Volumes            map[string]*storedVolume               `json:"volumes"`
	RdsDBInstances     map[string]*storedRdsDBInstance        `json:"rdsDbInstances"`
	EcsClusters        map[string]*storedEcsCluster           `json:"ecsClusters"`
	Permissions        map[string]*storedPermission           `json:"permissions"`
	TimeBasedAutoScale map[string]*storedTimeBasedAutoScaling `json:"timeBasedAutoScale"`
	LoadBasedAutoScale map[string]*storedLoadBasedAutoScaling `json:"loadBasedAutoScale"`
}

// InMemoryBackend is an in-memory OpsWorks backend.
type InMemoryBackend struct {
	mu                 *lockmetrics.RWMutex
	stacks             map[string]*storedStack
	layers             map[string]*storedLayer
	instances          map[string]*storedInstance
	apps               map[string]*storedApp
	deployments        map[string]*storedDeployment
	commands           map[string]*storedCommand
	tags               map[string]map[string]string
	userProfiles       map[string]*storedUserProfile
	elasticLBs         map[string]*storedElasticLoadBalancer
	elasticIPs         map[string]*storedElasticIP
	volumes            map[string]*storedVolume
	rdsDBInstances     map[string]*storedRdsDBInstance
	ecsClusters        map[string]*storedEcsCluster
	permissions        map[string]*storedPermission
	timeBasedAutoScale map[string]*storedTimeBasedAutoScaling
	loadBasedAutoScale map[string]*storedLoadBasedAutoScaling
	accountID          string
	region             string
}

// NewInMemoryBackend creates a new in-memory OpsWorks backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                 lockmetrics.New("opsworks"),
		stacks:             make(map[string]*storedStack),
		layers:             make(map[string]*storedLayer),
		instances:          make(map[string]*storedInstance),
		apps:               make(map[string]*storedApp),
		deployments:        make(map[string]*storedDeployment),
		commands:           make(map[string]*storedCommand),
		tags:               make(map[string]map[string]string),
		userProfiles:       make(map[string]*storedUserProfile),
		elasticLBs:         make(map[string]*storedElasticLoadBalancer),
		elasticIPs:         make(map[string]*storedElasticIP),
		volumes:            make(map[string]*storedVolume),
		rdsDBInstances:     make(map[string]*storedRdsDBInstance),
		ecsClusters:        make(map[string]*storedEcsCluster),
		permissions:        make(map[string]*storedPermission),
		timeBasedAutoScale: make(map[string]*storedTimeBasedAutoScaling),
		loadBasedAutoScale: make(map[string]*storedLoadBasedAutoScaling),
		accountID:          accountID,
		region:             region,
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
	b.userProfiles = make(map[string]*storedUserProfile)
	b.elasticLBs = make(map[string]*storedElasticLoadBalancer)
	b.elasticIPs = make(map[string]*storedElasticIP)
	b.volumes = make(map[string]*storedVolume)
	b.rdsDBInstances = make(map[string]*storedRdsDBInstance)
	b.ecsClusters = make(map[string]*storedEcsCluster)
	b.permissions = make(map[string]*storedPermission)
	b.timeBasedAutoScale = make(map[string]*storedTimeBasedAutoScaling)
	b.loadBasedAutoScale = make(map[string]*storedLoadBasedAutoScaling)
}

// Snapshot serializes the backend state.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "opsworks", snapshot{
		Stacks:             b.stacks,
		Layers:             b.layers,
		Instances:          b.instances,
		Apps:               b.apps,
		Deployments:        b.deployments,
		Commands:           b.commands,
		Tags:               b.tags,
		UserProfiles:       b.userProfiles,
		ElasticLBs:         b.elasticLBs,
		ElasticIPs:         b.elasticIPs,
		Volumes:            b.volumes,
		RdsDBInstances:     b.rdsDBInstances,
		EcsClusters:        b.ecsClusters,
		Permissions:        b.permissions,
		TimeBasedAutoScale: b.timeBasedAutoScale,
		LoadBasedAutoScale: b.loadBasedAutoScale,
	})
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var s snapshot
	if err := persistence.UnmarshalSnapshot(ctx, "opsworks", data, &s); err != nil {
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
	b.userProfiles = s.UserProfiles
	b.elasticLBs = s.ElasticLBs
	b.elasticIPs = s.ElasticIPs
	b.volumes = s.Volumes
	b.rdsDBInstances = s.RdsDBInstances
	b.ecsClusters = s.EcsClusters
	b.permissions = s.Permissions
	b.timeBasedAutoScale = s.TimeBasedAutoScale
	b.loadBasedAutoScale = s.LoadBasedAutoScale

	return nil
}

func (b *InMemoryBackend) stackARN(stackID string) string {
	return arn.Build("opsworks", b.region, b.accountID, fmt.Sprintf("stack/%s", stackID))
}

func (b *InMemoryBackend) layerARN(layerID string) string {
	return arn.Build("opsworks", b.region, b.accountID, fmt.Sprintf("layer/%s", layerID))
}

func (b *InMemoryBackend) instanceARN(instanceID string) string {
	return arn.Build("opsworks", b.region, b.accountID, fmt.Sprintf("instance/%s", instanceID))
}

func (b *InMemoryBackend) appARN(appID string) string {
	return arn.Build("opsworks", b.region, b.accountID, fmt.Sprintf("app/%s", appID))
}

func permissionKey(stackID, iamUserArn string) string {
	return stackID + ":" + iamUserArn
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
	stackArn := b.stackARN(id)

	s := &storedStack{
		CreatedAt:                 now,
		Tags:                      make(map[string]string),
		StackID:                   id,
		Arn:                       stackArn,
		Name:                      name,
		Region:                    region,
		DefaultInstanceProfileArn: defaultInstanceProfileArn,
		ServiceRoleArn:            serviceRoleArn,
		Status:                    "running",
	}
	b.stacks[id] = s

	return s.toStack(), nil
}

// CloneStack creates a new stack that is a copy of the source stack.
func (b *InMemoryBackend) CloneStack(sourceStackID, name, region string) (*Stack, error) {
	b.mu.Lock("CloneStack")
	defer b.mu.Unlock()

	src, ok := b.stacks[sourceStackID]
	if !ok {
		return nil, ErrStackNotFound
	}

	cloneName := name
	if cloneName == "" {
		cloneName = src.Name + "-clone"
	}

	cloneRegion := region
	if cloneRegion == "" {
		cloneRegion = src.Region
	}

	id := uuid.NewString()
	now := time.Now().UTC()

	s := &storedStack{
		CreatedAt:                 now,
		Tags:                      make(map[string]string),
		StackID:                   id,
		Arn:                       b.stackARN(id),
		Name:                      cloneName,
		Region:                    cloneRegion,
		DefaultInstanceProfileArn: src.DefaultInstanceProfileArn,
		ServiceRoleArn:            src.ServiceRoleArn,
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

// deleteStackResources removes layers, instances, apps, and deployments for a stack (caller holds lock).
func (b *InMemoryBackend) deleteStackResources(stackID string) {
	for id, l := range b.layers {
		if l.StackID == stackID {
			delete(b.layers, id)
		}
	}
	for id, i := range b.instances {
		if i.StackID == stackID {
			delete(b.instances, id)
		}
	}
	for id, a := range b.apps {
		if a.StackID == stackID {
			delete(b.apps, id)
		}
	}
	for id, d := range b.deployments {
		if d.StackID == stackID {
			delete(b.deployments, id)
		}
	}
}

// deleteStackAssociations removes permissions, volumes, RDS instances, and ECS clusters for a stack
// (caller holds lock).
func (b *InMemoryBackend) deleteStackAssociations(stackID string) {
	for k, p := range b.permissions {
		if p.StackID == stackID {
			delete(b.permissions, k)
		}
	}
	for k, v := range b.volumes {
		if v.StackID == stackID {
			delete(b.volumes, k)
		}
	}
	for k, r := range b.rdsDBInstances {
		if r.StackID == stackID {
			delete(b.rdsDBInstances, k)
		}
	}
	for k, e := range b.ecsClusters {
		if e.StackID == stackID {
			delete(b.ecsClusters, k)
		}
	}
}

// deleteStackChildren removes all resources belonging to a stack (caller holds lock).
func (b *InMemoryBackend) deleteStackChildren(stackID string) {
	b.deleteStackResources(stackID)
	b.deleteStackAssociations(stackID)
}

// DeleteStack deletes a stack and all its child resources.
func (b *InMemoryBackend) DeleteStack(stackID string) error {
	b.mu.Lock("DeleteStack")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return ErrStackNotFound
	}

	b.deleteStackChildren(stackID)

	stackArn := b.stackARN(stackID)
	delete(b.tags, stackArn)
	delete(b.stacks, stackID)

	return nil
}

// StartStack transitions all instances in a stack to starting state.
func (b *InMemoryBackend) StartStack(stackID string) error {
	b.mu.Lock("StartStack")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return ErrStackNotFound
	}

	for _, i := range b.instances {
		if i.StackID == stackID && i.Status == instanceStatusStopped {
			i.Status = instanceStatusStarting
		}
	}

	return nil
}

// StopStack transitions all instances in a stack to stopping state.
func (b *InMemoryBackend) StopStack(stackID string) error {
	b.mu.Lock("StopStack")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return ErrStackNotFound
	}

	for _, i := range b.instances {
		if i.StackID == stackID && i.Status == instanceStatusOnline {
			i.Status = instanceStatusStopping
		}
	}

	return nil
}

// GetHostnameSuggestion returns a suggested hostname for a new instance.
func (b *InMemoryBackend) GetHostnameSuggestion(stackID, _ string) (string, error) {
	b.mu.RLock("GetHostnameSuggestion")
	defer b.mu.RUnlock()

	if _, ok := b.stacks[stackID]; !ok {
		return "", ErrStackNotFound
	}

	suffix := uuid.NewString()[:8]

	return fmt.Sprintf("gopherstack-%s", suffix), nil
}

// DescribeStackSummary returns summary counts for a stack.
func (b *InMemoryBackend) DescribeStackSummary(stackID string) (*StackSummary, error) {
	b.mu.RLock("DescribeStackSummary")
	defer b.mu.RUnlock()

	s, ok := b.stacks[stackID]
	if !ok {
		return nil, ErrStackNotFound
	}

	counts := &InstancesCount{}
	for _, i := range b.instances {
		if i.StackID != stackID {
			continue
		}
		counts.Total++
		switch i.Status {
		case instanceStatusOnline:
			counts.Online++
		case instanceStatusStopped:
			counts.Stopped++
		case instanceStatusStarting:
			counts.Starting++
		case instanceStatusStopping:
			counts.Stopping++
		}
	}

	var layerCount, appCount, deploymentCount int32
	for _, l := range b.layers {
		if l.StackID == stackID {
			layerCount++
		}
	}
	for _, a := range b.apps {
		if a.StackID == stackID {
			appCount++
		}
	}
	for _, d := range b.deployments {
		if d.StackID == stackID {
			deploymentCount++
		}
	}

	return &StackSummary{
		StackID:          stackID,
		Arn:              s.Arn,
		Name:             s.Name,
		InstancesCount:   counts,
		LayersCount:      layerCount,
		AppsCount:        appCount,
		DeploymentsCount: deploymentCount,
	}, nil
}

// DescribeStackProvisioningParameters returns provisioning parameters for a stack.
func (b *InMemoryBackend) DescribeStackProvisioningParameters(stackID string) (map[string]string, string, error) {
	b.mu.RLock("DescribeStackProvisioningParameters")
	defer b.mu.RUnlock()

	s, ok := b.stacks[stackID]
	if !ok {
		return nil, "", ErrStackNotFound
	}

	params := map[string]string{
		"AgentInstallerUrl": fmt.Sprintf(
			"https://opsworks-instance-agent.s3.amazonaws.com/latest/install/%s",
			b.region,
		),
	}

	return params, s.Arn, nil
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
	// Use a UUID-derived suffix to avoid length-based race conditions.
	hostname := fmt.Sprintf("gopherstack-%s", id[:8])

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

// RegisterInstance registers an on-premises instance with a stack.
func (b *InMemoryBackend) RegisterInstance(stackID, hostname string) (string, error) {
	b.mu.Lock("RegisterInstance")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return "", ErrStackNotFound
	}

	id := uuid.NewString()
	now := time.Now().UTC()

	h := hostname
	if h == "" {
		h = fmt.Sprintf("registered-%s", id[:8])
	}

	i := &storedInstance{
		CreatedAt:  now,
		StackID:    stackID,
		InstanceID: id,
		Arn:        b.instanceARN(id),
		Hostname:   h,
		Status:     instanceStatusStopped,
		Registered: true,
	}
	b.instances[id] = i

	return id, nil
}

// DeregisterInstance deregisters an instance from OpsWorks.
func (b *InMemoryBackend) DeregisterInstance(instanceID string) error {
	b.mu.Lock("DeregisterInstance")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceID]; !ok {
		return ErrInstanceNotFound
	}

	delete(b.instances, instanceID)

	return nil
}

// AssignInstance assigns an existing EC2 instance to a layer.
func (b *InMemoryBackend) AssignInstance(instanceID string, layerIDs []string) error {
	b.mu.Lock("AssignInstance")
	defer b.mu.Unlock()

	i, ok := b.instances[instanceID]
	if !ok {
		return ErrInstanceNotFound
	}

	if len(layerIDs) > 0 {
		i.LayerID = layerIDs[0]
	}

	return nil
}

// UnassignInstance removes an instance from its layer.
func (b *InMemoryBackend) UnassignInstance(instanceID string) error {
	b.mu.Lock("UnassignInstance")
	defer b.mu.Unlock()

	i, ok := b.instances[instanceID]
	if !ok {
		return ErrInstanceNotFound
	}

	i.LayerID = ""

	return nil
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
	completedAt := now.Add(time.Second)

	d := &storedDeployment{
		CreatedAt:    now,
		CompletedAt:  completedAt,
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
		CompletedAt:    completedAt,
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

// ListTags lists tags for a resource with pagination support.
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

	allTags := b.tags[resourceARN]

	// Build a sorted list of keys for deterministic pagination.
	keys := make([]string, 0, len(allTags))
	for k := range allTags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Determine start index from nextToken.
	startIdx := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				startIdx = i

				break
			}
		}
	}

	// Apply maxResults limit.
	limit := len(keys) - startIdx
	if maxResults > 0 && int(maxResults) < limit {
		limit = int(maxResults)
	}

	result := make(map[string]string, limit)
	for i := startIdx; i < startIdx+limit; i++ {
		result[keys[i]] = allTags[keys[i]]
	}

	// Compute next token.
	outToken := ""
	if startIdx+limit < len(keys) {
		outToken = keys[startIdx+limit]
	}

	return result, outToken, nil
}

// CreateUserProfile creates an OpsWorks IAM user profile.
func (b *InMemoryBackend) CreateUserProfile(
	iamUserArn, sshUsername, sshPublicKey string,
	allowSelfManagement bool,
) (*UserProfile, error) {
	if iamUserArn == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateUserProfile")
	defer b.mu.Unlock()

	name := iamUserArn
	if idx := strings.LastIndex(iamUserArn, "/"); idx >= 0 {
		name = iamUserArn[idx+1:]
	}

	u := &storedUserProfile{
		IamUserArn:          iamUserArn,
		Name:                name,
		SSHUsername:         sshUsername,
		SSHPublicKey:        sshPublicKey,
		AllowSelfManagement: allowSelfManagement,
	}
	b.userProfiles[iamUserArn] = u

	return u.toUserProfile(), nil
}

// DeleteUserProfile removes a user profile.
func (b *InMemoryBackend) DeleteUserProfile(iamUserArn string) error {
	b.mu.Lock("DeleteUserProfile")
	defer b.mu.Unlock()

	if _, ok := b.userProfiles[iamUserArn]; !ok {
		return ErrUserProfileNotFound
	}

	delete(b.userProfiles, iamUserArn)

	return nil
}

// DescribeUserProfiles returns user profiles optionally filtered by ARN.
func (b *InMemoryBackend) DescribeUserProfiles(iamUserArns []string) ([]*UserProfile, error) {
	b.mu.RLock("DescribeUserProfiles")
	defer b.mu.RUnlock()

	if len(iamUserArns) > 0 {
		result := make([]*UserProfile, 0, len(iamUserArns))
		for _, arn := range iamUserArns {
			u, ok := b.userProfiles[arn]
			if !ok {
				return nil, ErrUserProfileNotFound
			}
			result = append(result, u.toUserProfile())
		}

		return result, nil
	}

	result := make([]*UserProfile, 0, len(b.userProfiles))
	for _, u := range b.userProfiles {
		result = append(result, u.toUserProfile())
	}

	return result, nil
}

// UpdateUserProfile updates a user profile's SSH settings.
func (b *InMemoryBackend) UpdateUserProfile(iamUserArn, sshUsername, sshPublicKey string) error {
	b.mu.Lock("UpdateUserProfile")
	defer b.mu.Unlock()

	u, ok := b.userProfiles[iamUserArn]
	if !ok {
		return ErrUserProfileNotFound
	}

	if sshUsername != "" {
		u.SSHUsername = sshUsername
	}
	if sshPublicKey != "" {
		u.SSHPublicKey = sshPublicKey
	}

	return nil
}

// DescribeMyUserProfile returns a placeholder profile for the current user.
func (b *InMemoryBackend) DescribeMyUserProfile() (*UserProfile, error) {
	b.mu.RLock("DescribeMyUserProfile")
	defer b.mu.RUnlock()

	// Return a synthetic "my" profile backed by the account.
	return &UserProfile{
		IamUserArn:          fmt.Sprintf("arn:aws:iam::%s:user/opsworks-user", b.accountID),
		Name:                "opsworks-user",
		SSHUsername:         "opsworks",
		AllowSelfManagement: false,
	}, nil
}

// UpdateMyUserProfile updates the SSH public key for the current user.
func (b *InMemoryBackend) UpdateMyUserProfile(_ string) error {
	return nil
}

// AttachElasticLoadBalancer attaches an ELB to a layer.
func (b *InMemoryBackend) AttachElasticLoadBalancer(elbName, layerID string) error {
	b.mu.Lock("AttachElasticLoadBalancer")
	defer b.mu.Unlock()

	l, ok := b.layers[layerID]
	if !ok {
		return ErrLayerNotFound
	}

	b.elasticLBs[elbName] = &storedElasticLoadBalancer{
		ElasticLoadBalancerName: elbName,
		Region:                  b.region,
		DNSName:                 fmt.Sprintf("%s.%s.elb.amazonaws.com", elbName, b.region),
		StackID:                 l.StackID,
		LayerID:                 layerID,
	}

	return nil
}

// DetachElasticLoadBalancer detaches an ELB from a layer.
func (b *InMemoryBackend) DetachElasticLoadBalancer(elbName, _ string) error {
	b.mu.Lock("DetachElasticLoadBalancer")
	defer b.mu.Unlock()

	if _, ok := b.elasticLBs[elbName]; !ok {
		return ErrElasticLBNotFound
	}

	delete(b.elasticLBs, elbName)

	return nil
}

// DescribeElasticLoadBalancers returns ELBs optionally filtered by stack/layer.
func (b *InMemoryBackend) DescribeElasticLoadBalancers(stackID, _ string) ([]*ElasticLoadBalancer, error) {
	b.mu.RLock("DescribeElasticLoadBalancers")
	defer b.mu.RUnlock()

	result := make([]*ElasticLoadBalancer, 0)
	for _, e := range b.elasticLBs {
		if stackID != "" && e.StackID != stackID {
			continue
		}
		result = append(result, e.toElasticLoadBalancer())
	}

	return result, nil
}

// RegisterElasticIP registers an elastic IP address.
func (b *InMemoryBackend) RegisterElasticIP(elasticIP, region string) (*ElasticIP, error) {
	if elasticIP == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("RegisterElasticIP")
	defer b.mu.Unlock()

	r := region
	if r == "" {
		r = b.region
	}

	e := &storedElasticIP{
		IP:     elasticIP,
		Region: r,
		Domain: "vpc",
	}
	b.elasticIPs[elasticIP] = e

	return e.toElasticIP(), nil
}

// DeregisterElasticIP removes a registered elastic IP.
func (b *InMemoryBackend) DeregisterElasticIP(elasticIP string) error {
	b.mu.Lock("DeregisterElasticIP")
	defer b.mu.Unlock()

	if _, ok := b.elasticIPs[elasticIP]; !ok {
		return ErrElasticIPNotFound
	}

	delete(b.elasticIPs, elasticIP)

	return nil
}

// AssociateElasticIP associates an elastic IP with an instance.
func (b *InMemoryBackend) AssociateElasticIP(elasticIP, instanceID string) error {
	b.mu.Lock("AssociateElasticIP")
	defer b.mu.Unlock()

	e, ok := b.elasticIPs[elasticIP]
	if !ok {
		return ErrElasticIPNotFound
	}

	if _, exists := b.instances[instanceID]; !exists {
		return ErrInstanceNotFound
	}

	e.InstanceID = instanceID

	return nil
}

// DisassociateElasticIP removes an elastic IP's instance association.
func (b *InMemoryBackend) DisassociateElasticIP(elasticIP string) error {
	b.mu.Lock("DisassociateElasticIP")
	defer b.mu.Unlock()

	e, ok := b.elasticIPs[elasticIP]
	if !ok {
		return ErrElasticIPNotFound
	}

	e.InstanceID = ""

	return nil
}

// DescribeElasticIps returns elastic IPs optionally filtered by instance or IP list.
func (b *InMemoryBackend) DescribeElasticIps(instanceID string, ips []string) ([]*ElasticIP, error) {
	b.mu.RLock("DescribeElasticIps")
	defer b.mu.RUnlock()

	if len(ips) > 0 {
		result := make([]*ElasticIP, 0, len(ips))
		for _, ip := range ips {
			e, ok := b.elasticIPs[ip]
			if !ok {
				return nil, ErrElasticIPNotFound
			}
			result = append(result, e.toElasticIP())
		}

		return result, nil
	}

	result := make([]*ElasticIP, 0)
	for _, e := range b.elasticIPs {
		if instanceID != "" && e.InstanceID != instanceID {
			continue
		}
		result = append(result, e.toElasticIP())
	}

	return result, nil
}

// UpdateElasticIP updates the name of a registered elastic IP.
func (b *InMemoryBackend) UpdateElasticIP(elasticIP, name string) error {
	b.mu.Lock("UpdateElasticIP")
	defer b.mu.Unlock()

	e, ok := b.elasticIPs[elasticIP]
	if !ok {
		return ErrElasticIPNotFound
	}

	e.Name = name

	return nil
}

// RegisterVolume registers an EBS volume with a stack.
func (b *InMemoryBackend) RegisterVolume(ec2VolumeID, stackID string) (string, error) {
	b.mu.Lock("RegisterVolume")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return "", ErrStackNotFound
	}

	id := uuid.NewString()
	v := &storedVolume{
		RegisteredAt: time.Now().UTC(),
		VolumeID:     id,
		Ec2VolumeID:  ec2VolumeID,
		StackID:      stackID,
		Status:       volumeStatusRegistered,
		Region:       b.region,
	}
	b.volumes[id] = v

	return id, nil
}

// DeregisterVolume removes a registered volume.
func (b *InMemoryBackend) DeregisterVolume(volumeID string) error {
	b.mu.Lock("DeregisterVolume")
	defer b.mu.Unlock()

	if _, ok := b.volumes[volumeID]; !ok {
		return ErrVolumeNotFound
	}

	delete(b.volumes, volumeID)

	return nil
}

// AssignVolume assigns a registered volume to an instance.
func (b *InMemoryBackend) AssignVolume(volumeID, instanceID string) error {
	b.mu.Lock("AssignVolume")
	defer b.mu.Unlock()

	v, ok := b.volumes[volumeID]
	if !ok {
		return ErrVolumeNotFound
	}

	if _, exists := b.instances[instanceID]; !exists {
		return ErrInstanceNotFound
	}

	v.InstanceID = instanceID

	return nil
}

// UnassignVolume removes a volume's instance assignment.
func (b *InMemoryBackend) UnassignVolume(volumeID string) error {
	b.mu.Lock("UnassignVolume")
	defer b.mu.Unlock()

	v, ok := b.volumes[volumeID]
	if !ok {
		return ErrVolumeNotFound
	}

	v.InstanceID = ""

	return nil
}

// DescribeVolumes returns volumes filtered by instance, RAID array, or IDs.
func (b *InMemoryBackend) DescribeVolumes(instanceID, _ string, volumeIDs []string) ([]*Volume, error) {
	b.mu.RLock("DescribeVolumes")
	defer b.mu.RUnlock()

	if len(volumeIDs) > 0 {
		result := make([]*Volume, 0, len(volumeIDs))
		for _, id := range volumeIDs {
			v, ok := b.volumes[id]
			if !ok {
				return nil, ErrVolumeNotFound
			}
			result = append(result, v.toVolume())
		}

		return result, nil
	}

	result := make([]*Volume, 0)
	for _, v := range b.volumes {
		if instanceID != "" && v.InstanceID != instanceID {
			continue
		}
		result = append(result, v.toVolume())
	}

	return result, nil
}

// UpdateVolume updates a volume's name and mount point.
func (b *InMemoryBackend) UpdateVolume(volumeID, name, mountPoint string) error {
	b.mu.Lock("UpdateVolume")
	defer b.mu.Unlock()

	v, ok := b.volumes[volumeID]
	if !ok {
		return ErrVolumeNotFound
	}

	if name != "" {
		v.Name = name
	}
	if mountPoint != "" {
		v.MountPoint = mountPoint
	}

	return nil
}

// RegisterRdsDBInstance registers an RDS DB instance with a stack.
func (b *InMemoryBackend) RegisterRdsDBInstance(stackID, rdsDBInstanceArn, dbUser, _ string) error {
	if rdsDBInstanceArn == "" {
		return ErrValidation
	}

	b.mu.Lock("RegisterRdsDBInstance")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return ErrStackNotFound
	}

	// Extract a readable identifier from the ARN.
	id := rdsDBInstanceArn
	if idx := strings.LastIndex(rdsDBInstanceArn, ":"); idx >= 0 {
		id = rdsDBInstanceArn[idx+1:]
	}

	b.rdsDBInstances[rdsDBInstanceArn] = &storedRdsDBInstance{
		RdsDBInstanceArn:     rdsDBInstanceArn,
		DBInstanceIdentifier: id,
		DBUser:               dbUser,
		StackID:              stackID,
		Region:               b.region,
		Address:              fmt.Sprintf("%s.%s.rds.amazonaws.com", id, b.region),
	}

	return nil
}

// DeregisterRdsDBInstance removes a registered RDS DB instance.
func (b *InMemoryBackend) DeregisterRdsDBInstance(rdsDBInstanceArn string) error {
	b.mu.Lock("DeregisterRdsDBInstance")
	defer b.mu.Unlock()

	if _, ok := b.rdsDBInstances[rdsDBInstanceArn]; !ok {
		return ErrRdsDBInstanceNotFound
	}

	delete(b.rdsDBInstances, rdsDBInstanceArn)

	return nil
}

// DescribeRdsDBInstances returns RDS DB instances filtered by stack or ARN list.
func (b *InMemoryBackend) DescribeRdsDBInstances(stackID string, rdsDBInstanceArns []string) ([]*RdsDBInstance, error) {
	b.mu.RLock("DescribeRdsDBInstances")
	defer b.mu.RUnlock()

	if len(rdsDBInstanceArns) > 0 {
		result := make([]*RdsDBInstance, 0, len(rdsDBInstanceArns))
		for _, rArn := range rdsDBInstanceArns {
			r, ok := b.rdsDBInstances[rArn]
			if !ok {
				return nil, ErrRdsDBInstanceNotFound
			}
			result = append(result, r.toRdsDBInstance())
		}

		return result, nil
	}

	result := make([]*RdsDBInstance, 0)
	for _, r := range b.rdsDBInstances {
		if stackID != "" && r.StackID != stackID {
			continue
		}
		result = append(result, r.toRdsDBInstance())
	}

	return result, nil
}

// UpdateRdsDBInstance updates the DB user/password for a registered RDS instance.
func (b *InMemoryBackend) UpdateRdsDBInstance(rdsDBInstanceArn, dbUser, _ string) error {
	b.mu.Lock("UpdateRdsDBInstance")
	defer b.mu.Unlock()

	r, ok := b.rdsDBInstances[rdsDBInstanceArn]
	if !ok {
		return ErrRdsDBInstanceNotFound
	}

	if dbUser != "" {
		r.DBUser = dbUser
	}

	return nil
}

// RegisterEcsCluster registers an ECS cluster with a stack.
func (b *InMemoryBackend) RegisterEcsCluster(ecsClusterArn, stackID string) (string, error) {
	if ecsClusterArn == "" {
		return "", ErrValidation
	}

	b.mu.Lock("RegisterEcsCluster")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return "", ErrStackNotFound
	}

	name := ecsClusterArn
	if idx := strings.LastIndex(ecsClusterArn, "/"); idx >= 0 {
		name = ecsClusterArn[idx+1:]
	}

	b.ecsClusters[ecsClusterArn] = &storedEcsCluster{
		RegisteredAt:   time.Now().UTC(),
		EcsClusterArn:  ecsClusterArn,
		EcsClusterName: name,
		StackID:        stackID,
		Status:         ecsClusterStatusRegistered,
	}

	return ecsClusterArn, nil
}

// DeregisterEcsCluster removes a registered ECS cluster.
func (b *InMemoryBackend) DeregisterEcsCluster(ecsClusterArn string) error {
	b.mu.Lock("DeregisterEcsCluster")
	defer b.mu.Unlock()

	if _, ok := b.ecsClusters[ecsClusterArn]; !ok {
		return ErrEcsClusterNotFound
	}

	delete(b.ecsClusters, ecsClusterArn)

	return nil
}

// DescribeEcsClusters returns ECS clusters filtered by stack or ARN list.
func (b *InMemoryBackend) DescribeEcsClusters(stackID string, ecsClusterArns []string) ([]*EcsCluster, error) {
	b.mu.RLock("DescribeEcsClusters")
	defer b.mu.RUnlock()

	if len(ecsClusterArns) > 0 {
		result := make([]*EcsCluster, 0, len(ecsClusterArns))
		for _, clusterArn := range ecsClusterArns {
			e, ok := b.ecsClusters[clusterArn]
			if !ok {
				return nil, ErrEcsClusterNotFound
			}
			result = append(result, e.toEcsCluster())
		}

		return result, nil
	}

	result := make([]*EcsCluster, 0)
	for _, e := range b.ecsClusters {
		if stackID != "" && e.StackID != stackID {
			continue
		}
		result = append(result, e.toEcsCluster())
	}

	return result, nil
}

// SetPermission sets stack permissions for an IAM user.
func (b *InMemoryBackend) SetPermission(stackID, iamUserArn, level string, allowSSH, allowSudo bool) error {
	b.mu.Lock("SetPermission")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackID]; !ok {
		return ErrStackNotFound
	}

	key := permissionKey(stackID, iamUserArn)
	b.permissions[key] = &storedPermission{
		StackID:    stackID,
		IamUserArn: iamUserArn,
		Level:      level,
		AllowSSH:   allowSSH,
		AllowSudo:  allowSudo,
	}

	return nil
}

// DescribePermissions returns permissions optionally filtered by stack and user.
func (b *InMemoryBackend) DescribePermissions(stackID, iamUserArn string) ([]*Permission, error) {
	b.mu.RLock("DescribePermissions")
	defer b.mu.RUnlock()

	if stackID != "" && iamUserArn != "" {
		key := permissionKey(stackID, iamUserArn)
		p, ok := b.permissions[key]
		if !ok {
			return []*Permission{}, nil
		}

		return []*Permission{p.toPermission()}, nil
	}

	result := make([]*Permission, 0)
	for _, p := range b.permissions {
		if stackID != "" && p.StackID != stackID {
			continue
		}
		if iamUserArn != "" && p.IamUserArn != iamUserArn {
			continue
		}
		result = append(result, p.toPermission())
	}

	return result, nil
}

// SetTimeBasedAutoScaling sets the time-based auto-scaling schedule for an instance.
func (b *InMemoryBackend) SetTimeBasedAutoScaling(instanceID string, schedule *AutoScalingSchedule) error {
	b.mu.Lock("SetTimeBasedAutoScaling")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceID]; !ok {
		return ErrInstanceNotFound
	}

	b.timeBasedAutoScale[instanceID] = &storedTimeBasedAutoScaling{
		AutoScalingSchedule: schedule,
		InstanceID:          instanceID,
	}

	return nil
}

// DescribeTimeBasedAutoScaling returns time-based auto-scaling config for instances.
func (b *InMemoryBackend) DescribeTimeBasedAutoScaling(instanceIDs []string) ([]*TimeBasedAutoScaling, error) {
	b.mu.RLock("DescribeTimeBasedAutoScaling")
	defer b.mu.RUnlock()

	result := make([]*TimeBasedAutoScaling, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		t, ok := b.timeBasedAutoScale[id]
		if !ok {
			// Return a record with empty schedule if not configured.
			result = append(result, &TimeBasedAutoScaling{
				AutoScalingSchedule: &AutoScalingSchedule{},
				InstanceID:          id,
			})

			continue
		}
		result = append(result, t.toTimeBasedAutoScaling())
	}

	return result, nil
}

// SetLoadBasedAutoScaling sets load-based auto-scaling config for a layer.
func (b *InMemoryBackend) SetLoadBasedAutoScaling(
	layerID string,
	enable bool,
	upScaling, downScaling *ScalingParameters,
) error {
	b.mu.Lock("SetLoadBasedAutoScaling")
	defer b.mu.Unlock()

	if _, ok := b.layers[layerID]; !ok {
		return ErrLayerNotFound
	}

	b.loadBasedAutoScale[layerID] = &storedLoadBasedAutoScaling{
		UpScaling:   upScaling,
		DownScaling: downScaling,
		LayerID:     layerID,
		Enable:      enable,
	}

	return nil
}

// DescribeLoadBasedAutoScaling returns load-based auto-scaling config for layers.
func (b *InMemoryBackend) DescribeLoadBasedAutoScaling(layerIDs []string) ([]*LoadBasedAutoScaling, error) {
	b.mu.RLock("DescribeLoadBasedAutoScaling")
	defer b.mu.RUnlock()

	result := make([]*LoadBasedAutoScaling, 0, len(layerIDs))
	for _, id := range layerIDs {
		l, ok := b.loadBasedAutoScale[id]
		if !ok {
			result = append(result, &LoadBasedAutoScaling{LayerID: id})

			continue
		}
		result = append(result, l.toLoadBasedAutoScaling())
	}

	return result, nil
}

// GrantAccess returns temporary SSH credentials for an instance.
func (b *InMemoryBackend) GrantAccess(instanceID string, validForInMinutes int32) (*TemporaryCredential, error) {
	b.mu.RLock("GrantAccess")
	defer b.mu.RUnlock()

	if _, ok := b.instances[instanceID]; !ok {
		return nil, ErrInstanceNotFound
	}

	mins := validForInMinutes
	if mins <= 0 {
		mins = 60
	}

	return &TemporaryCredential{
		InstanceID:        instanceID,
		Username:          "opsworks",
		Password:          uuid.NewString(),
		ValidForInMinutes: mins,
	}, nil
}

// DescribeServiceErrors returns service errors (always empty in mock).
func (b *InMemoryBackend) DescribeServiceErrors(
	stackID, _ string,
	_ []string,
) ([]map[string]any, error) {
	b.mu.RLock("DescribeServiceErrors")
	defer b.mu.RUnlock()

	if stackID != "" {
		if _, ok := b.stacks[stackID]; !ok {
			return nil, ErrStackNotFound
		}
	}

	return []map[string]any{}, nil
}

// DescribeRaidArrays returns RAID arrays (always empty in mock).
func (b *InMemoryBackend) DescribeRaidArrays(
	_, _ string,
	_ []string,
) ([]map[string]any, error) {
	b.mu.RLock("DescribeRaidArrays")
	defer b.mu.RUnlock()

	return []map[string]any{}, nil
}

// DescribeAgentVersions returns a static list of supported OpsWorks agent versions.
func (b *InMemoryBackend) DescribeAgentVersions(stackID string) ([]*AgentVersion, error) {
	b.mu.RLock("DescribeAgentVersions")
	defer b.mu.RUnlock()

	if stackID != "" {
		if _, ok := b.stacks[stackID]; !ok {
			return nil, ErrStackNotFound
		}
	}

	return []*AgentVersion{
		{
			ConfigurationManager: &ConfigurationManager{
				Name:    configManagerChef,
				Version: "12",
			},
			Version: "4000-20161221135000",
		},
		{
			ConfigurationManager: &ConfigurationManager{
				Name:    configManagerChef,
				Version: "11.10",
			},
			Version: "4000-20161221135000",
		},
	}, nil
}

// DescribeOperatingSystems returns a static list of supported OpsWorks operating systems.
func (b *InMemoryBackend) DescribeOperatingSystems() ([]*OperatingSystem, error) {
	return []*OperatingSystem{
		{
			ConfigurationManagers: []*ConfigurationManager{
				{Name: configManagerChef, Version: "12"},
				{Name: configManagerChef, Version: "11.10"},
			},
			ID:              "AmazonLinux2",
			Name:            "Amazon Linux 2",
			Type:            osTypeLinux,
			ReportedVersion: "2",
			Supported:       true,
		},
		{
			ConfigurationManagers: []*ConfigurationManager{
				{Name: configManagerChef, Version: "12"},
			},
			ID:              "Ubuntu18.04",
			Name:            "Ubuntu 18.04 LTS",
			Type:            osTypeLinux,
			ReportedVersion: "18.04",
			Supported:       true,
		},
		{
			ConfigurationManagers: []*ConfigurationManager{
				{Name: configManagerChef, Version: "12"},
			},
			ID:              "CentOS7",
			Name:            "CentOS Linux 7",
			Type:            osTypeLinux,
			ReportedVersion: "7",
			Supported:       true,
		},
		{
			ConfigurationManagers: []*ConfigurationManager{
				{Name: configManagerChef, Version: "12.2"},
			},
			ID:              "MicrosoftWindowsServer2019",
			Name:            "Microsoft Windows Server 2019",
			Type:            "Windows",
			ReportedVersion: "2019",
			Supported:       true,
		},
	}, nil
}

// resourceExists checks if a resource ARN refers to a known resource.
func (b *InMemoryBackend) resourceExists(resourceArn string) bool {
	if strings.Contains(resourceArn, ":stack/") {
		id := arnSuffix(resourceArn)
		_, ok := b.stacks[id]

		return ok
	}
	if strings.Contains(resourceArn, ":layer/") {
		id := arnSuffix(resourceArn)
		_, ok := b.layers[id]

		return ok
	}
	if strings.Contains(resourceArn, ":instance/") {
		id := arnSuffix(resourceArn)
		_, ok := b.instances[id]

		return ok
	}
	if strings.Contains(resourceArn, ":app/") {
		id := arnSuffix(resourceArn)
		_, ok := b.apps[id]

		return ok
	}

	return false
}

func arnSuffix(resourceArn string) string {
	parts := strings.Split(resourceArn, "/")
	if len(parts) == 0 {
		return ""
	}

	return parts[len(parts)-1]
}

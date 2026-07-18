package opsworks

import (
	"maps"
	"time"
)

const (
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

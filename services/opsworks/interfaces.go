package opsworks

import (
	"context"
	"time"
)

// StorageBackend is the interface for OpsWorks storage operations.
type StorageBackend interface {
	// Stack operations
	CreateStack(name, region, defaultInstanceProfileArn, serviceRoleArn string) (*Stack, error)
	DescribeStacks(stackIDs []string) ([]*Stack, error)
	UpdateStack(stackID, name string) error
	DeleteStack(stackID string) error

	// Layer operations
	CreateLayer(stackID, layerType, name, shortname string) (*Layer, error)
	DescribeLayers(stackID string, layerIDs []string) ([]*Layer, error)
	UpdateLayer(layerID, name string) error
	DeleteLayer(layerID string) error

	// Instance operations
	CreateInstance(stackID, layerID, instanceType string) (*Instance, error)
	DescribeInstances(stackID, layerID string, instanceIDs []string) ([]*Instance, error)
	UpdateInstance(instanceID, hostname string) error
	DeleteInstance(instanceID string) error
	StartInstance(instanceID string) error
	StopInstance(instanceID string) error
	RebootInstance(instanceID string) error

	// App operations
	CreateApp(stackID, name, appType string) (*App, error)
	DescribeApps(stackID string, appIDs []string) ([]*App, error)
	UpdateApp(appID, name string) error
	DeleteApp(appID string) error

	// Deployment operations
	CreateDeployment(stackID, appID, command string) (*Deployment, error)
	DescribeDeployments(stackID, appID string, deploymentIDs []string) ([]*Deployment, error)

	// Command operations
	DescribeCommands(deploymentID, instanceID string, commandIDs []string) ([]*Command, error)

	// Tag operations
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTags(resourceARN string, maxResults int32, nextToken string) (map[string]string, string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Stack represents an OpsWorks stack.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Stack struct {
	CreatedAt                 time.Time
	Tags                      map[string]string
	StackID                   string
	Arn                       string
	Name                      string
	Region                    string
	DefaultInstanceProfileArn string
	ServiceRoleArn            string
	Status                    string
}

// Layer represents an OpsWorks layer.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Layer struct {
	CreatedAt time.Time
	StackID   string
	LayerID   string
	Arn       string
	Type      string
	Name      string
	Shortname string
}

// Instance represents an OpsWorks instance.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Instance struct {
	CreatedAt    time.Time
	StackID      string
	LayerID      string
	InstanceID   string
	Arn          string
	Hostname     string
	InstanceType string
	Status       string
}

// App represents an OpsWorks app.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type App struct {
	CreatedAt time.Time
	StackID   string
	AppID     string
	Arn       string
	Name      string
	Type      string
}

// Deployment represents an OpsWorks deployment.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Deployment struct {
	CreatedAt    time.Time
	CompletedAt  time.Time
	StackID      string
	AppID        string
	DeploymentID string
	Command      string
	Status       string
	Duration     int32
}

// Command represents an OpsWorks command.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Command struct {
	CreatedAt      time.Time
	AcknowledgedAt time.Time
	CompletedAt    time.Time
	DeploymentID   string
	InstanceID     string
	CommandID      string
	Type           string
	Status         string
	LogURL         string
	ExitCode       int32
}

var _ StorageBackend = (*InMemoryBackend)(nil)

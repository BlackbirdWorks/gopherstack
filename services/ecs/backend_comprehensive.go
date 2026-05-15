package ecs

// ServiceConnectClientAlias represents an alias for a service connect client.
type ServiceConnectClientAlias struct {
	DNSName string `json:"dnsName,omitempty"`
	Port    int    `json:"port"`
}

// ServiceConnectService represents a service in a service connect configuration.
type ServiceConnectService struct {
	PortName      string                      `json:"portName"`
	DiscoveryName string                      `json:"discoveryName,omitempty"`
	ClientAliases []ServiceConnectClientAlias `json:"clientAliases,omitempty"`
}

// ServiceConnectConfiguration represents the service connect configuration for a service.
type ServiceConnectConfiguration struct {
	Namespace string                  `json:"namespace,omitempty"`
	Services  []ServiceConnectService `json:"services,omitempty"`
	Enabled   bool                    `json:"enabled"`
}

// ContainerOverride represents a container override for a task.
type ContainerOverride struct {
	CPU         *int              `json:"cpu,omitempty"`
	Memory      *int              `json:"memory,omitempty"`
	Name        string            `json:"name"`
	Command     []string          `json:"command,omitempty"`
	Environment []KeyValuePair    `json:"environment,omitempty"`
	Secrets     []SecretReference `json:"secrets,omitempty"`
}

// TaskOverride represents overrides for a task run.
type TaskOverride struct {
	TaskRoleArn        string              `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn   string              `json:"executionRoleArn,omitempty"`
	CPU                string              `json:"cpu,omitempty"`
	Memory             string              `json:"memory,omitempty"`
	ContainerOverrides []ContainerOverride `json:"containerOverrides,omitempty"`
}

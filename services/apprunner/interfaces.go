package apprunner

import (
	"context"
	"time"
)

// StorageBackend is the interface for App Runner storage operations.
type StorageBackend interface {
	CreateService(params CreateServiceParams) (*Service, error)
	DescribeService(serviceArn string) (*Service, error)
	UpdateService(params UpdateServiceParams) (*Service, error)
	DeleteService(serviceArn string) (*Service, error)
	ListServices(maxResults int32, nextToken string) ([]*ServiceSummary, string, error)
	PauseService(serviceArn string) (*Service, error)
	ResumeService(serviceArn string) (*Service, error)
	ListOperations(serviceArn string, maxResults int32, nextToken string) ([]*OperationSummary, string, error)
	StartDeployment(serviceArn string) (string, error)

	CreateAutoScalingConfiguration(
		name string,
		maxConcurrency, maxSize, minSize int32,
		tags map[string]string,
	) (*AutoScalingConfiguration, error)
	DescribeAutoScalingConfiguration(arn string) (*AutoScalingConfiguration, error)
	DeleteAutoScalingConfiguration(arn string) (*AutoScalingConfiguration, error)
	ListAutoScalingConfigurations(
		nameFilter string,
		latestOnly bool,
		maxResults int32,
		nextToken string,
	) ([]*AutoScalingConfigurationSummary, string, error)
	UpdateDefaultAutoScalingConfiguration(arn string) (*AutoScalingConfiguration, error)
	ListServicesForAutoScalingConfiguration(arn string, maxResults int32, nextToken string) ([]string, string, error)

	CreateConnection(name, providerType string, tags map[string]string) (*Connection, error)
	DeleteConnection(arn string) (*Connection, error)
	ListConnections(nameFilter string, maxResults int32, nextToken string) ([]*ConnectionSummary, string, error)

	CreateObservabilityConfiguration(
		name, tracingVendor string,
		tags map[string]string,
	) (*ObservabilityConfiguration, error)
	DescribeObservabilityConfiguration(arn string) (*ObservabilityConfiguration, error)
	DeleteObservabilityConfiguration(arn string) (*ObservabilityConfiguration, error)
	ListObservabilityConfigurations(
		nameFilter string,
		latestOnly bool,
		maxResults int32,
		nextToken string,
	) ([]*ObservabilityConfigurationSummary, string, error)

	CreateVpcConnector(name string, subnets, securityGroups []string, tags map[string]string) (*VpcConnector, error)
	DescribeVpcConnector(arn string) (*VpcConnector, error)
	DeleteVpcConnector(arn string) (*VpcConnector, error)
	ListVpcConnectors(maxResults int32, nextToken string) ([]*VpcConnector, string, error)

	CreateVpcIngressConnection(
		name, serviceArn, vpcID, vpcEndpointID string,
		tags map[string]string,
	) (*VpcIngressConnection, error)
	DescribeVpcIngressConnection(arn string) (*VpcIngressConnection, error)
	DeleteVpcIngressConnection(arn string) (*VpcIngressConnection, error)
	ListVpcIngressConnections(
		serviceArnFilter, vpcEndpointIDFilter string,
		maxResults int32,
		nextToken string,
	) ([]*VpcIngressConnectionSummary, string, error)
	UpdateVpcIngressConnection(arn, vpcID, vpcEndpointID string) (*VpcIngressConnection, error)

	AssociateCustomDomain(serviceArn, domainName string, enableWWW bool) (*CustomDomain, error)
	DisassociateCustomDomain(serviceArn, domainName string) (*CustomDomain, error)
	DescribeCustomDomains(
		serviceArn string,
		maxResults int32,
		nextToken string,
	) ([]*CustomDomain, string, string, error)

	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, keys []string) error
	ListTagsForResource(resourceArn string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// InstanceConfig mirrors types.InstanceConfiguration: the runtime
// configuration of instances (scaling units) of a service. It's shared
// between CreateServiceParams/UpdateServiceParams (as input) and Service (as
// output) since the shape is identical either direction.
type InstanceConfig struct {
	CPU             string `json:"cpu"`
	Memory          string `json:"memory"`
	InstanceRoleArn string `json:"instanceRoleArn"`
}

// ImageSource mirrors types.ImageRepository (+ its nested ImageConfiguration,
// flattened for simplicity since App Runner allows only one of
// ImageRepository/CodeRepository per service).
type ImageSource struct {
	RuntimeEnvironmentVariables map[string]string `json:"runtimeEnvironmentVariables,omitempty"`
	RuntimeEnvironmentSecrets   map[string]string `json:"runtimeEnvironmentSecrets,omitempty"`
	ImageIdentifier             string            `json:"imageIdentifier"`
	ImageRepositoryType         string            `json:"imageRepositoryType"`
	Port                        string            `json:"port"`
	StartCommand                string            `json:"startCommand"`
}

// CodeSource mirrors types.CodeRepository (+ its nested CodeConfiguration/
// CodeConfigurationValues/SourceCodeVersion, flattened).
type CodeSource struct {
	RuntimeEnvironmentVariables map[string]string `json:"runtimeEnvironmentVariables,omitempty"`
	RuntimeEnvironmentSecrets   map[string]string `json:"runtimeEnvironmentSecrets,omitempty"`
	RepositoryURL               string            `json:"repositoryUrl"`
	SourceCodeVersionType       string            `json:"sourceCodeVersionType"`
	SourceCodeVersionValue      string            `json:"sourceCodeVersionValue"`
	SourceDirectory             string            `json:"sourceDirectory"`
	ConfigurationSource         string            `json:"configurationSource"`
	Runtime                     string            `json:"runtime"`
	BuildCommand                string            `json:"buildCommand"`
	StartCommand                string            `json:"startCommand"`
	Port                        string            `json:"port"`
}

// SourceConfig mirrors types.SourceConfiguration: the source deployed to a
// service (exactly one of ImageRepository/CodeRepository) plus the sibling
// AuthenticationConfiguration/AutoDeploymentsEnabled fields.
type SourceConfig struct {
	ImageRepository        *ImageSource `json:"imageRepository,omitempty"`
	CodeRepository         *CodeSource  `json:"codeRepository,omitempty"`
	AutoDeploymentsEnabled *bool        `json:"autoDeploymentsEnabled,omitempty"`
	AccessRoleArn          string       `json:"accessRoleArn,omitempty"`
	ConnectionArn          string       `json:"connectionArn,omitempty"`
}

// NetworkConfig mirrors types.NetworkConfiguration. IsPubliclyAccessible is a
// pointer so CreateService/UpdateService can distinguish "not specified"
// (apply App Runner's default) from an explicit false; once a Service is
// constructed the backend guarantees the pointer is non-nil.
type NetworkConfig struct {
	EgressType            string `json:"egressType"`
	EgressVpcConnectorArn string `json:"egressVpcConnectorArn,omitempty"`
	IsPubliclyAccessible  *bool  `json:"isPubliclyAccessible,omitempty"`
	IPAddressType         string `json:"ipAddressType"`
}

// HealthCheckConfig mirrors types.HealthCheckConfiguration.
type HealthCheckConfig struct {
	Protocol           string `json:"protocol"`
	Path               string `json:"path"`
	Interval           int32  `json:"interval"`
	Timeout            int32  `json:"timeout"`
	HealthyThreshold   int32  `json:"healthyThreshold"`
	UnhealthyThreshold int32  `json:"unhealthyThreshold"`
}

// ServiceObservability mirrors types.ServiceObservabilityConfiguration.
type ServiceObservability struct {
	ConfigurationArn string `json:"configurationArn,omitempty"`
	Enabled          bool   `json:"enabled"`
}

// CreateServiceParams groups CreateService's backend inputs. A nil Network/
// HealthCheck/Observability means "apply App Runner's default", matching the
// real API's behavior when those optional request members are omitted.
type CreateServiceParams struct {
	Source                      SourceConfig
	Network                     *NetworkConfig
	HealthCheck                 *HealthCheckConfig
	Observability               *ServiceObservability
	Tags                        map[string]string
	Instance                    InstanceConfig
	Name                        string
	AutoScalingConfigurationArn string
	EncryptionKmsKey            string
}

// UpdateServiceParams groups UpdateService's backend inputs. A nil field (or
// a nil Source.ImageRepository/CodeRepository) means "leave unchanged",
// matching UpdateServiceInput's optional members.
type UpdateServiceParams struct {
	Instance                    *InstanceConfig
	Source                      *SourceConfig
	Network                     *NetworkConfig
	HealthCheck                 *HealthCheckConfig
	Observability               *ServiceObservability
	ServiceArn                  string
	AutoScalingConfigurationArn string
}

// AutoScalingConfiguration represents an App Runner auto scaling configuration.
// CreatedAt is first to reduce GC pointer bytes.
type AutoScalingConfiguration struct {
	CreatedAt                        time.Time
	DeletedAt                        time.Time
	AutoScalingConfigurationArn      string
	AutoScalingConfigurationName     string
	Status                           string
	AutoScalingConfigurationRevision int32
	MaxConcurrency                   int32
	MaxSize                          int32
	MinSize                          int32
	IsDefault                        bool
	HasAssociatedService             bool
	Latest                           bool
}

// AutoScalingConfigurationSummary is a summary entry for list responses.
type AutoScalingConfigurationSummary struct {
	CreatedAt                        time.Time
	AutoScalingConfigurationArn      string
	AutoScalingConfigurationName     string
	Status                           string
	AutoScalingConfigurationRevision int32
	IsDefault                        bool
	HasAssociatedService             bool
}

// Connection represents an App Runner connection resource.
// CreatedAt is first to reduce GC pointer bytes.
type Connection struct {
	CreatedAt      time.Time
	ConnectionArn  string
	ConnectionName string
	ProviderType   string
	Status         string
}

// ConnectionSummary is a summary entry for list responses.
type ConnectionSummary struct {
	CreatedAt      time.Time
	ConnectionArn  string
	ConnectionName string
	ProviderType   string
	Status         string
}

// ObservabilityConfiguration represents an App Runner observability configuration.
// CreatedAt is first to reduce GC pointer bytes.
type ObservabilityConfiguration struct {
	CreatedAt                          time.Time
	DeletedAt                          time.Time
	ObservabilityConfigurationArn      string
	ObservabilityConfigurationName     string
	Status                             string
	TracingVendor                      string
	ObservabilityConfigurationRevision int32
	Latest                             bool
}

// ObservabilityConfigurationSummary is a summary entry for list responses.
type ObservabilityConfigurationSummary struct {
	CreatedAt                          time.Time
	ObservabilityConfigurationArn      string
	ObservabilityConfigurationName     string
	Status                             string
	ObservabilityConfigurationRevision int32
	Latest                             bool
}

// VpcConnector represents an App Runner VPC connector resource.
// CreatedAt is first to reduce GC pointer bytes.
type VpcConnector struct {
	CreatedAt            time.Time
	DeletedAt            time.Time
	VpcConnectorArn      string
	VpcConnectorName     string
	Status               string
	SecurityGroups       []string
	Subnets              []string
	VpcConnectorRevision int32
}

// VpcIngressConnection represents an App Runner VPC ingress connection resource.
// CreatedAt is first to reduce GC pointer bytes.
type VpcIngressConnection struct {
	CreatedAt                time.Time
	DeletedAt                time.Time
	VpcIngressConnectionArn  string
	VpcIngressConnectionName string
	ServiceArn               string
	AccountID                string
	DomainName               string
	VpcID                    string
	VpcEndpointID            string
	Status                   string
}

// VpcIngressConnectionSummary is a summary entry for list responses.
type VpcIngressConnectionSummary struct {
	VpcIngressConnectionArn string
	ServiceArn              string
}

// CustomDomain represents a custom domain associated with an App Runner service.
type CustomDomain struct {
	DomainName         string
	Status             string
	EnableWWWSubdomain bool
}

// Service represents an App Runner service with full details.
// CreatedAt is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type Service struct {
	Source                      SourceConfig
	CreatedAt                   time.Time
	UpdatedAt                   time.Time
	DeletedAt                   time.Time
	Network                     NetworkConfig
	Instance                    InstanceConfig
	Observability               ServiceObservability
	Status                      string
	ServiceURL                  string
	ServiceName                 string
	AutoScalingConfigurationArn string
	ServiceID                   string
	EncryptionKmsKey            string
	ServiceArn                  string
	HealthCheck                 HealthCheckConfig
}

// ServiceSummary is a service entry in a list response.
// CreatedAt is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type ServiceSummary struct {
	CreatedAt   time.Time
	UpdatedAt   time.Time
	ServiceArn  string
	ServiceID   string
	ServiceName string
	ServiceURL  string
	Status      string
}

// OperationSummary is an operation entry in a list response.
// StartedAt is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type OperationSummary struct {
	StartedAt time.Time
	EndedAt   time.Time
	UpdatedAt time.Time
	ID        string
	Type      string
	Status    string
	TargetArn string
}

var _ StorageBackend = (*InMemoryBackend)(nil)

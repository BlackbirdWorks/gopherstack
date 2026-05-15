package ecs

import (
	"fmt"
	"slices"
	"strings"
)

// ---- Issue #1: LoadBalancer ----

// LoadBalancer wires ECS tasks to ALB/NLB target groups.
type LoadBalancer struct {
	TargetGroupArn   string `json:"targetGroupArn,omitempty"`
	LoadBalancerName string `json:"loadBalancerName,omitempty"`
	ContainerName    string `json:"containerName,omitempty"`
	ContainerPort    int    `json:"containerPort,omitempty"`
}

// ---- Issue #3: DeploymentController ----

const (
	deploymentControllerRolling    = "ROLLING"
	deploymentControllerExternal   = "EXTERNAL"
	deploymentControllerCodeDeploy = "CODE_DEPLOY"
)

// DeploymentController specifies the deployment controller type for a service.
type DeploymentController struct {
	Type string `json:"type"`
}

// ---- Issue #4: LogConfiguration ----

// LogConfiguration specifies the logging driver and options for a container.
type LogConfiguration struct {
	LogDriver     string            `json:"logDriver"`
	Options       map[string]string `json:"options,omitempty"`
	SecretOptions []SecretOption    `json:"secretOptions,omitempty"`
}

// SecretOption is a secret key/value pair for log driver authentication.
type SecretOption struct {
	Name      string `json:"name"`
	ValueFrom string `json:"valueFrom"`
}

// FirelensConfiguration specifies FireLens routing for container logs.
type FirelensConfiguration struct {
	Options map[string]string `json:"options,omitempty"`
	Type    string            `json:"type"`
}

// ---- Issue #6: Secrets ----

// SecretReference resolves a SecretsManager ARN or SSM path into an env var.
type SecretReference struct {
	Name      string `json:"name"`
	ValueFrom string `json:"valueFrom"`
}

// ---- Issue #7: Volumes ----

// EFSVolumeConfiguration holds EFS-specific volume options.
type EFSVolumeConfiguration struct {
	AuthorizationConfig *EFSAuthorizationConfig `json:"authorizationConfig,omitempty"`
	FileSystemID        string                  `json:"fileSystemId"`
	RootDirectory       string                  `json:"rootDirectory,omitempty"`
	TransitEncryption   string                  `json:"transitEncryption,omitempty"`
}

// EFSAuthorizationConfig holds EFS authorization settings.
type EFSAuthorizationConfig struct {
	AccessPointID string `json:"accessPointId,omitempty"`
	IAM           string `json:"iam,omitempty"`
}

// HostVolumeProperties holds host-bind volume path.
type HostVolumeProperties struct {
	SourcePath string `json:"sourcePath,omitempty"`
}

// Volume defines a storage volume for a task definition.
type Volume struct {
	Host                   *HostVolumeProperties   `json:"host,omitempty"`
	EFSVolumeConfiguration *EFSVolumeConfiguration `json:"efsVolumeConfiguration,omitempty"`
	Name                   string                  `json:"name"`
	ConfiguredAtLaunch     bool                    `json:"configuredAtLaunch,omitempty"`
}

// MountPoint maps a volume into a container.
type MountPoint struct {
	SourceVolume  string `json:"sourceVolume,omitempty"`
	ContainerPath string `json:"containerPath,omitempty"`
	ReadOnly      bool   `json:"readOnly,omitempty"`
}

// VolumeFrom specifies a container whose volumes to mount.
type VolumeFrom struct {
	SourceContainer string `json:"sourceContainer,omitempty"`
	ReadOnly        bool   `json:"readOnly,omitempty"`
}

// ---- Issue #8: NetworkConfiguration ----

const (
	assignPublicIPEnabled  = "ENABLED"
	assignPublicIPDisabled = "DISABLED"
)

// AwsvpcConfiguration holds awsvpc mode network settings.
type AwsvpcConfiguration struct {
	AssignPublicIp string   `json:"assignPublicIp,omitempty"` //nolint:revive,stylecheck // AWS API field name
	Subnets        []string `json:"subnets"`
	SecurityGroups []string `json:"securityGroups,omitempty"`
}

// NetworkConfiguration holds the network settings for a task or service.
type NetworkConfiguration struct {
	AwsvpcConfiguration *AwsvpcConfiguration `json:"awsvpcConfiguration,omitempty"`
}

// ---- Issue #10: HealthCheck ----

// HealthCheck configures the container health check.
type HealthCheck struct {
	Command     []string `json:"command"`
	Interval    int      `json:"interval,omitempty"`
	Timeout     int      `json:"timeout,omitempty"`
	Retries     int      `json:"retries,omitempty"`
	StartPeriod int      `json:"startPeriod,omitempty"`
}

// ---- Issue #11: RepositoryCredentials ----

// RepositoryCredentials references a SecretsManager secret for private registry pulls.
type RepositoryCredentials struct {
	CredentialsParameter string `json:"credentialsParameter"`
}

// ---- Issue #13: ServiceRegistry ----

// ServiceRegistry wires tasks into a CloudMap namespace for service discovery.
type ServiceRegistry struct {
	RegistryArn   string `json:"registryArn,omitempty"`
	ContainerName string `json:"containerName,omitempty"`
	Port          int    `json:"port,omitempty"`
	ContainerPort int    `json:"containerPort,omitempty"`
}

// ---- Issue #9: Fargate CPU/memory validation ----

// fargateCPUMemoryPairs returns valid Fargate CPU/memory combinations per the AWS docs.
// Keys are CPU values (vCPU units as strings); values are allowed memory values (MiB as strings).
func fargateCPUMemoryPairs() map[string][]string {
	return map[string][]string{
		"256":  {"512", "1024", "2048"},
		"512":  {"1024", "2048", "3072", "4096"},
		"1024": {"2048", "3072", "4096", "5120", "6144", "7168", "8192"},
		"2048": {
			"4096", "5120", "6144", "7168", "8192", "9216", "10240", "11264",
			"12288", "13312", "14336", "15360", "16384",
		},
		"4096": {
			"8192", "9216", "10240", "11264", "12288", "13312", "14336", "15360",
			"16384", "17408", "18432", "19456", "20480", "21504", "22528", "23552",
			"24576", "25600", "26624", "27648", "28672", "29696", "30720",
		},
		"8192": {
			"16384", "20480", "24576", "28672", "32768", "36864", "40960", "45056",
			"49152", "53248", "57344", "61440",
		},
		"16384": {
			"32768", "40960", "49152", "57344", "65536", "73728", "81920", "90112",
			"98304", "106496", "114688", "122880",
		},
	}
}

// validateFargateCPUMemory returns an error if the cpu/memory combination is
// invalid for Fargate. It is a no-op when either value is empty (EC2 launch type).
func validateFargateCPUMemory(cpu, memory string) error {
	if cpu == "" || memory == "" {
		return nil
	}

	pairs := fargateCPUMemoryPairs()
	validMemories, ok := pairs[cpu]

	if !ok {
		return fmt.Errorf(
			"%w: invalid Fargate CPU value %q; valid values: 256, 512, 1024, 2048, 4096, 8192, 16384",
			ErrInvalidParameter, cpu,
		)
	}

	if slices.Contains(validMemories, memory) {
		return nil
	}

	return fmt.Errorf(
		"%w: invalid Fargate memory %q for CPU %q; valid values: %s",
		ErrInvalidParameter, memory, cpu, strings.Join(validMemories, ", "),
	)
}

// ---- Issue #16: PlatformVersion validation ----

const (
	platformVersionLatest = "LATEST"
	platformVersion140    = "1.4.0"
	platformVersion130    = "1.3.0"
	platformVersion120    = "1.2.0"
	platformVersion110    = "1.1.0"
	platformVersion100    = "1.0.0"
)

// isValidFargatePlatformVersion reports whether pv is a known Fargate platform version.
func isValidFargatePlatformVersion(pv string) bool {
	switch pv {
	case platformVersionLatest,
		platformVersion140,
		platformVersion130,
		platformVersion120,
		platformVersion110,
		platformVersion100:
		return true
	}

	return false
}

// validatePlatformVersion returns an error if the platform version is non-empty
// and not recognized.
func validatePlatformVersion(pv string) error {
	if pv == "" {
		return nil
	}

	upper := strings.ToUpper(pv)
	if isValidFargatePlatformVersion(upper) || isValidFargatePlatformVersion(pv) {
		return nil
	}

	return fmt.Errorf(
		"%w: unknown Fargate platform version %q; valid values: LATEST, 1.4.0, 1.3.0, 1.2.0, 1.1.0, 1.0.0",
		ErrInvalidParameter, pv,
	)
}

// ---- Issue #12: PropagateTags constants ----

const (
	propagateTagsTaskDefinition = "TASK_DEFINITION"
	propagateTagsService        = "SERVICE"
	propagateTagsNone           = "NONE"
)

// ---- Issue #3: DeploymentController validation ----

// validateDeploymentController returns an error for unsupported controller types.
func validateDeploymentController(dc *DeploymentController) error {
	if dc == nil {
		return nil
	}

	switch strings.ToUpper(dc.Type) {
	case deploymentControllerRolling, "":
		return nil
	case deploymentControllerExternal:
		return nil
	case deploymentControllerCodeDeploy:
		return fmt.Errorf(
			"%w: CODE_DEPLOY deployment controller is not supported; use ROLLING or EXTERNAL",
			ErrInvalidParameter,
		)
	default:
		return fmt.Errorf(
			"%w: unknown deployment controller type %q; valid values: ROLLING, EXTERNAL",
			ErrInvalidParameter, dc.Type,
		)
	}
}

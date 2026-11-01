package ec2

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// Compute is the optional pluggable hook that backs EC2 instances with a real
// runtime (such as a Docker container). When the InMemoryBackend is configured
// with a Compute provider via WithCompute, lifecycle methods (RunInstances,
// TerminateInstances, StartInstances, StopInstances) call into the provider
// after updating in-memory state, allowing real workloads to be created and
// torn down alongside the simulated instance metadata.
//
// Implementations must be safe for concurrent use.
type Compute interface {
	// Launch starts the underlying compute resource for a freshly created
	// instance. The returned LaunchResult is merged back onto the Instance
	// (PrivateIP, PublicIPAddress, PublicDNSName, ProviderID, SSHPort).
	Launch(ctx context.Context, req LaunchRequest) (LaunchResult, error)

	// Terminate removes the underlying compute resource. It is invoked from
	// TerminateInstances and must be idempotent (no error if the resource is
	// already gone).
	Terminate(ctx context.Context, instanceID, providerID string) error

	// Start resumes a previously stopped resource (analogous to EC2 StartInstances).
	Start(ctx context.Context, instanceID, providerID string) error

	// Stop pauses the underlying resource (analogous to EC2 StopInstances).
	Stop(ctx context.Context, instanceID, providerID string) error
}

// LaunchRequest describes a new instance the Compute provider should back.
type LaunchRequest struct {
	// Admit, when set, makes the provider front every published port with a relay that admits only clients
	// the instance's live security groups allow.
	Admit func(proto string, port int) sgfilter.Admitter `json:"-"`
	// InstanceID is the synthetic EC2 instance ID (i-...).
	InstanceID string `json:"instanceID,omitempty"`
	// ImageID is the AMI ID requested by the caller; the provider may ignore it.
	ImageID string `json:"imageID,omitempty"`
	// InstanceType is the requested EC2 instance type (e.g. t3.micro).
	InstanceType string `json:"instanceType,omitempty"`
	// KeyName is the EC2 key pair name (may be empty).
	KeyName string `json:"keyName,omitempty"`
	// AuthorizedKey is the OpenSSH-format public key of the key pair, written to ~ec2-user/.ssh/authorized_keys.
	AuthorizedKey string `json:"authorizedKey,omitempty"`
	// UserData is the raw (already base64-decoded) user data, if any.
	UserData string `json:"userData,omitempty"`
	// MetadataEndpoint is the URL of this instance's metadata service, reachable from the
	// container; empty when no listener is wired.
	MetadataEndpoint string `json:"metadataEndpoint,omitempty"`
	// VPCID and VPCCIDR identify the VPC network the instance joins.
	VPCID   string `json:"vpcID,omitempty"`
	VPCCIDR string `json:"vpcCIDR,omitempty"`
	// PrivateIP is the address allocated from the instance's subnet.
	PrivateIP string `json:"privateIP,omitempty"`
	// DockerImage is the Docker image bound to ImageID; empty means the provider's configured default.
	DockerImage string `json:"dockerImage,omitempty"`
	// Volumes are the EBS volumes attached at launch (root volume excluded).
	Volumes []VolumeAttachSpec `json:"volumes,omitempty"`
	// PublishPorts are the ports to publish: those opened to the world, or every TCP rule port when enforcing.
	PublishPorts []PortSpec `json:"publishPorts,omitempty"`
}

// PortSpec is a container port to publish on the host.
type PortSpec struct {
	Protocol string `json:"protocol"`
	Port     int    `json:"port"`
}

// PublishedPort is a container port reachable on the host.
type PublishedPort struct {
	Protocol      string `json:"protocol"`
	HostIP        string `json:"hostIp"`
	ContainerPort int    `json:"containerPort"`
	HostPort      int    `json:"hostPort"`
}

// LaunchResult is what the Compute provider returns after creating a backing
// resource. Empty string fields are ignored (in-memory defaults are kept).
type LaunchResult struct {
	// NetworkNote reports a VPC addressing mismatch (empty when the VPC CIDR was honoured).
	NetworkNote string `json:"networkNote,omitempty"`
	// ProviderID is the provider-specific opaque identifier (Docker container ID).
	ProviderID string `json:"providerID,omitempty"`
	// PrivateIP overrides the in-memory allocated private IP.
	PrivateIP string `json:"privateIP,omitempty"`
	// PublicIPAddress overrides the in-memory allocated public IP, if any.
	PublicIPAddress string `json:"publicIPAddress,omitempty"`
	// PublicDNSName overrides the in-memory generated public DNS name, if any.
	PublicDNSName string `json:"publicDNSName,omitempty"`
	// PublishedPorts are the security-group ports mapped to the host.
	PublishedPorts []PublishedPort `json:"publishedPorts,omitempty"`
	// SSHPort is the host TCP port that maps to the container's sshd. 0 means
	// no host-side mapping was configured.
	SSHPort int `json:"sshPort,omitempty"`
}

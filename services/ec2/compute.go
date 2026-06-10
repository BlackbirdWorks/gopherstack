package ec2

import "context"

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
	// InstanceID is the synthetic EC2 instance ID (i-...).
	InstanceID string `json:"instanceID,omitempty"`
	// ImageID is the AMI ID requested by the caller. The Compute provider may
	// use this to pick a Docker image but is also free to ignore it and use
	// a configured default image.
	ImageID string `json:"imageID,omitempty"`
	// InstanceType is the requested EC2 instance type (e.g. t3.micro).
	InstanceType string `json:"instanceType,omitempty"`
	// KeyName is the EC2 key pair name (may be empty).
	KeyName string `json:"keyName,omitempty"`
	// AuthorizedKey is the OpenSSH-format public key derived from the named
	// EC2 key pair. When non-empty the provider should write it to the
	// container's ~ec2-user/.ssh/authorized_keys so the demo can ssh in.
	AuthorizedKey string `json:"authorizedKey,omitempty"`
	// UserData is the raw (already base64-decoded) user data, if any.
	UserData string `json:"userData,omitempty"`
}

// LaunchResult is what the Compute provider returns after creating a backing
// resource. Empty string fields are ignored (in-memory defaults are kept).
type LaunchResult struct {
	// ProviderID is the provider-specific opaque identifier (Docker container ID).
	ProviderID string `json:"providerID,omitempty"`
	// PrivateIP overrides the in-memory allocated private IP.
	PrivateIP string `json:"privateIP,omitempty"`
	// PublicIPAddress overrides the in-memory allocated public IP, if any.
	PublicIPAddress string `json:"publicIPAddress,omitempty"`
	// PublicDNSName overrides the in-memory generated public DNS name, if any.
	PublicDNSName string `json:"publicDNSName,omitempty"`
	// SSHPort is the host TCP port that maps to the container's sshd. 0 means
	// no host-side mapping was configured.
	SSHPort int `json:"sshPort,omitempty"`
}

package transfer

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"maps"
	"net/http"
	"sort"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	protocolSFTP = "SFTP"
	protocolFTPS = "FTPS"
)

var (
	// ErrServerNotFound is returned when a Transfer server is not found.
	ErrServerNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrUserNotFound is returned when a Transfer user is not found.
	ErrUserNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrUserAlreadyExists is returned when a Transfer user already exists.
	ErrUserAlreadyExists = awserr.New("ResourceExistsException", awserr.ErrConflict)
	// ErrInvalidProtocol is returned when an unsupported protocol is specified.
	ErrInvalidProtocol = awserr.New(
		"InvalidRequestException: unsupported protocol",
		awserr.ErrInvalidParameter,
	)
	// ErrServerStateConflict is returned when a state transition is invalid.
	ErrServerStateConflict = awserr.New(
		"ConflictException: server is already in the requested state",
		awserr.ErrConflict,
	)
	// ErrAccessNotFound is returned when a Transfer access is not found.
	ErrAccessNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAccessAlreadyExists is returned when an access with the same ExternalId already exists.
	ErrAccessAlreadyExists = awserr.New("ResourceExistsException", awserr.ErrConflict)
	// ErrAgreementNotFound is returned when a Transfer agreement is not found.
	ErrAgreementNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrConnectorNotFound is returned when a Transfer connector is not found.
	ErrConnectorNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProfileNotFound is returned when a Transfer profile is not found.
	ErrProfileNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrWebAppNotFound is returned when a Transfer web app is not found.
	ErrWebAppNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrWorkflowNotFound is returned when a Transfer workflow is not found.
	ErrWorkflowNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrCertificateNotFound is returned when a Transfer certificate is not found.
	ErrCertificateNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrHostKeyNotFound is returned when a Transfer host key is not found.
	ErrHostKeyNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrSSHPublicKeyNotFound is returned when a Transfer SSH public key is not found.
	ErrSSHPublicKeyNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when a required parameter is missing or invalid.
	ErrValidation = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
)

// Server state constants.
const (
	serverStatusOnline      = "ONLINE"
	serverStatusOffline     = "OFFLINE"
	serverStatusStarting    = "STARTING"
	serverStatusStopping    = "STOPPING"
	serverStatusStartFailed = "START_FAILED"
	serverStatusStopFailed  = "STOP_FAILED"
)

// IdentityProviderType constants.
const (
	identityProviderServiceManaged   = "SERVICE_MANAGED"
	identityProviderAPIGateway       = "API_GATEWAY"
	identityProviderDirectoryService = "AWS_DIRECTORY_SERVICE"
	identityProviderLambda           = "AWS_LAMBDA"
)

// EndpointType constants.
const (
	endpointTypePublic      = "PUBLIC"
	endpointTypeVPC         = "VPC"
	endpointTypeVPCEndpoint = "VPC_ENDPOINT"
)

// HomeDirectoryType constants.
const (
	homeDirectoryTypePath    = "PATH"
	homeDirectoryTypeLogical = "LOGICAL"
)

// Profile type constants.
const (
	profileTypeLocal   = "LOCAL"
	profileTypePartner = "PARTNER"
)

// Agreement status constants.
const (
	agreementStatusActive   = "ACTIVE"
	agreementStatusInactive = "INACTIVE"
	defaultHostKeyType      = "ssh-rsa"
	sshKeyTypeEd25519       = "ssh-ed25519"
	sshKeyTypeECDSAP256     = "ecdsa-sha2-nistp256"
	sshKeyTypeECDSAP384     = "ecdsa-sha2-nistp384"
	sshKeyTypeECDSAP521     = "ecdsa-sha2-nistp521"
)

// Workflow step state status constants (SendWorkflowStepState).
const (
	workflowStepStatusComplete  = "COMPLETE"
	workflowStepStatusException = "EXCEPTION"
)

// IdentityProviderDetails holds identity provider configuration for a Transfer server.
type IdentityProviderDetails struct {
	URL                       string `json:"url,omitempty"`
	InvocationRole            string `json:"invocation_role,omitempty"`
	DirectoryID               string `json:"directory_id,omitempty"`
	Function                  string `json:"function,omitempty"`
	SftpAuthenticationMethods string `json:"sftp_authentication_methods,omitempty"`
}

// EndpointDetails holds VPC endpoint configuration for a Transfer server.
type EndpointDetails struct {
	VpcEndpointID        string   `json:"vpc_endpoint_id,omitempty"`
	VpcID                string   `json:"vpc_id,omitempty"`
	AddressAllocationIDs []string `json:"address_allocation_ids,omitempty"`
	SubnetIDs            []string `json:"subnet_ids,omitempty"`
	SecurityGroupIDs     []string `json:"security_group_ids,omitempty"`
}

// ProtocolDetails holds protocol-specific configuration for a Transfer server.
type ProtocolDetails struct {
	PassiveIP                string   `json:"passive_ip,omitempty"`
	TLSSessionResumptionMode string   `json:"tls_session_resumption_mode,omitempty"`
	SetStatOption            string   `json:"set_stat_option,omitempty"`
	As2Transports            []string `json:"as2_transports,omitempty"`
}

// WorkflowDetail holds a single workflow ID + execution role pair.
type WorkflowDetail struct {
	WorkflowID    string `json:"workflow_id"`
	ExecutionRole string `json:"execution_role"`
}

// WorkflowDetails holds workflow trigger configuration for a Transfer server.
type WorkflowDetails struct {
	OnUpload        []WorkflowDetail `json:"on_upload,omitempty"`
	OnPartialUpload []WorkflowDetail `json:"on_partial_upload,omitempty"`
}

// S3StorageOptions holds S3 storage configuration for a Transfer server.
type S3StorageOptions struct {
	DirectoryListingOptimization string `json:"directory_listing_optimization,omitempty"`
}

// Server represents an AWS Transfer Family server.
type Server struct {
	IdentityProviderDetails       *IdentityProviderDetails `json:"identity_provider_details,omitempty"`
	EndpointDetails               *EndpointDetails         `json:"endpoint_details,omitempty"`
	ProtocolDetails               *ProtocolDetails         `json:"protocol_details,omitempty"`
	WorkflowDetails               *WorkflowDetails         `json:"workflow_details,omitempty"`
	S3StorageOptions              *S3StorageOptions        `json:"s3_storage_options,omitempty"`
	CreatedAt                     time.Time                `json:"created_at"`
	Tags                          map[string]string        `json:"tags"`
	ServerID                      string                   `json:"server_id"`
	State                         string                   `json:"state"`
	Endpoint                      string                   `json:"endpoint"`
	Domain                        string                   `json:"domain"`
	Region                        string                   `json:"region"`
	AccountID                     string                   `json:"account_id"`
	IdentityProviderType          string                   `json:"identity_provider_type,omitempty"`
	EndpointType                  string                   `json:"endpoint_type,omitempty"`
	LoggingRole                   string                   `json:"logging_role,omitempty"`
	PreAuthenticationLoginBanner  string                   `json:"pre_authentication_login_banner,omitempty"`
	PostAuthenticationLoginBanner string                   `json:"post_authentication_login_banner,omitempty"`
	HostKey                       string                   `json:"host_key,omitempty"`
	Certificate                   string                   `json:"certificate,omitempty"`
	SecurityPolicyName            string                   `json:"security_policy_name,omitempty"`
	IPAddressType                 string                   `json:"ip_address_type,omitempty"`
	StructuredLogDestinations     []string                 `json:"structured_log_destinations,omitempty"`
	Protocols                     []string                 `json:"protocols"`
}

// serverARN builds the ARN for a Transfer server.
func serverARN(accountID, region, serverID string) string {
	return arn.Build("transfer", region, accountID, "server/"+serverID)
}

// cloneServer returns a deep copy of a Server.
func cloneServer(s *Server) *Server {
	cp := *s
	cp.Tags = make(map[string]string, len(s.Tags))
	maps.Copy(cp.Tags, s.Tags)

	cp.Protocols = make([]string, len(s.Protocols))
	copy(cp.Protocols, s.Protocols)

	if s.StructuredLogDestinations != nil {
		cp.StructuredLogDestinations = make([]string, len(s.StructuredLogDestinations))
		copy(cp.StructuredLogDestinations, s.StructuredLogDestinations)
	}

	if s.IdentityProviderDetails != nil {
		ipd := *s.IdentityProviderDetails
		cp.IdentityProviderDetails = &ipd
	}

	if s.EndpointDetails != nil {
		ed := *s.EndpointDetails
		if s.EndpointDetails.AddressAllocationIDs != nil {
			ed.AddressAllocationIDs = make([]string, len(s.EndpointDetails.AddressAllocationIDs))
			copy(ed.AddressAllocationIDs, s.EndpointDetails.AddressAllocationIDs)
		}
		if s.EndpointDetails.SubnetIDs != nil {
			ed.SubnetIDs = make([]string, len(s.EndpointDetails.SubnetIDs))
			copy(ed.SubnetIDs, s.EndpointDetails.SubnetIDs)
		}
		if s.EndpointDetails.SecurityGroupIDs != nil {
			ed.SecurityGroupIDs = make([]string, len(s.EndpointDetails.SecurityGroupIDs))
			copy(ed.SecurityGroupIDs, s.EndpointDetails.SecurityGroupIDs)
		}
		cp.EndpointDetails = &ed
	}

	if s.ProtocolDetails != nil {
		pd := *s.ProtocolDetails
		if s.ProtocolDetails.As2Transports != nil {
			pd.As2Transports = make([]string, len(s.ProtocolDetails.As2Transports))
			copy(pd.As2Transports, s.ProtocolDetails.As2Transports)
		}
		cp.ProtocolDetails = &pd
	}

	if s.WorkflowDetails != nil {
		wd := WorkflowDetails{
			OnUpload:        cloneWorkflowDetails(s.WorkflowDetails.OnUpload),
			OnPartialUpload: cloneWorkflowDetails(s.WorkflowDetails.OnPartialUpload),
		}
		cp.WorkflowDetails = &wd
	}

	if s.S3StorageOptions != nil {
		so := *s.S3StorageOptions
		cp.S3StorageOptions = &so
	}

	return &cp
}

func cloneWorkflowDetails(wds []WorkflowDetail) []WorkflowDetail {
	if wds == nil {
		return nil
	}
	out := make([]WorkflowDetail, len(wds))
	copy(out, wds)

	return out
}

// PosixProfile holds POSIX profile configuration for a Transfer user or access.
type PosixProfile struct {
	SecondaryGids []int64 `json:"secondary_gids,omitempty"`
	UID           int64   `json:"uid"`
	GID           int64   `json:"gid"`
}

// HomeDirectoryMapEntry holds a single logical directory mapping.
type HomeDirectoryMapEntry struct {
	Entry  string `json:"entry"`
	Target string `json:"target"`
	Type   string `json:"type,omitempty"`
}

// User represents a user on an AWS Transfer Family server.
type User struct {
	CreatedAt             time.Time               `json:"created_at"`
	PosixProfile          *PosixProfile           `json:"posix_profile,omitempty"`
	Tags                  map[string]string       `json:"tags"`
	UserName              string                  `json:"user_name"`
	ServerID              string                  `json:"server_id"`
	HomeDir               string                  `json:"home_dir"`
	Role                  string                  `json:"role"`
	AccountID             string                  `json:"account_id"`
	Region                string                  `json:"region"`
	HomeDirectoryType     string                  `json:"home_directory_type,omitempty"`
	Policy                string                  `json:"policy,omitempty"`
	HomeDirectoryMappings []HomeDirectoryMapEntry `json:"home_directory_mappings,omitempty"`
}

// userARN builds the ARN for a Transfer user.
// AWS format: arn:aws:transfer:<region>:<account>:user/<serverId>/<userName>.
func userARN(accountID, region, serverID, userName string) string {
	return arn.Build("transfer", region, accountID, "user/"+serverID+"/"+userName)
}

// cloneUser returns a deep copy of a User.
func cloneUser(u *User) *User {
	cp := *u
	cp.Tags = make(map[string]string, len(u.Tags))
	maps.Copy(cp.Tags, u.Tags)

	if u.PosixProfile != nil {
		pp := *u.PosixProfile
		if u.PosixProfile.SecondaryGids != nil {
			pp.SecondaryGids = make([]int64, len(u.PosixProfile.SecondaryGids))
			copy(pp.SecondaryGids, u.PosixProfile.SecondaryGids)
		}
		cp.PosixProfile = &pp
	}

	if u.HomeDirectoryMappings != nil {
		cp.HomeDirectoryMappings = make([]HomeDirectoryMapEntry, len(u.HomeDirectoryMappings))
		copy(cp.HomeDirectoryMappings, u.HomeDirectoryMappings)
	}

	return &cp
}

// Access represents an AWS Transfer access policy entry for a server.
type Access struct {
	CreatedAt             time.Time               `json:"created_at"`
	PosixProfile          *PosixProfile           `json:"posix_profile,omitempty"`
	Tags                  map[string]string       `json:"tags"`
	ExternalID            string                  `json:"external_id"`
	ServerID              string                  `json:"server_id"`
	Role                  string                  `json:"role"`
	HomeDir               string                  `json:"home_dir"`
	AccountID             string                  `json:"account_id"`
	Region                string                  `json:"region"`
	HomeDirectoryType     string                  `json:"home_directory_type,omitempty"`
	Policy                string                  `json:"policy,omitempty"`
	HomeDirectoryMappings []HomeDirectoryMapEntry `json:"home_directory_mappings,omitempty"`
}

// cloneAccess returns a deep copy of an Access.
func cloneAccess(a *Access) *Access {
	cp := *a
	cp.Tags = make(map[string]string, len(a.Tags))
	maps.Copy(cp.Tags, a.Tags)

	if a.PosixProfile != nil {
		pp := *a.PosixProfile
		if a.PosixProfile.SecondaryGids != nil {
			pp.SecondaryGids = make([]int64, len(a.PosixProfile.SecondaryGids))
			copy(pp.SecondaryGids, a.PosixProfile.SecondaryGids)
		}
		cp.PosixProfile = &pp
	}

	if a.HomeDirectoryMappings != nil {
		cp.HomeDirectoryMappings = make([]HomeDirectoryMapEntry, len(a.HomeDirectoryMappings))
		copy(cp.HomeDirectoryMappings, a.HomeDirectoryMappings)
	}

	return &cp
}

// Agreement represents an AWS Transfer AS2 agreement.
type Agreement struct {
	CreatedAt        time.Time         `json:"created_at"`
	Tags             map[string]string `json:"tags"`
	AgreementID      string            `json:"agreement_id"`
	ServerID         string            `json:"server_id"`
	Description      string            `json:"description"`
	LocalProfileID   string            `json:"local_profile_id"`
	PartnerProfileID string            `json:"partner_profile_id"`
	BaseDirectory    string            `json:"base_directory"`
	AccessRole       string            `json:"access_role"`
	AccountID        string            `json:"account_id"`
	Region           string            `json:"region"`
	Status           string            `json:"status"`
}

// cloneAgreement returns a deep copy of an Agreement.
func cloneAgreement(a *Agreement) *Agreement {
	cp := *a
	cp.Tags = make(map[string]string, len(a.Tags))
	maps.Copy(cp.Tags, a.Tags)

	return &cp
}

// ConnectorSftpConfig holds SFTP-specific connector configuration.
type ConnectorSftpConfig struct {
	UserSecretID    string   `json:"user_secret_id,omitempty"`
	TrustedHostKeys []string `json:"trusted_host_keys,omitempty"`
}

// ConnectorAs2Config holds AS2-specific connector configuration.
type ConnectorAs2Config struct {
	LocalProfileID      string `json:"local_profile_id,omitempty"`
	PartnerProfileID    string `json:"partner_profile_id,omitempty"`
	SigningAlgorithm    string `json:"signing_algorithm,omitempty"`
	EncryptionAlgorithm string `json:"encryption_algorithm,omitempty"`
	MdnSigningAlgorithm string `json:"mdn_signing_algorithm,omitempty"`
	MdnResponse         string `json:"mdn_response,omitempty"`
	MessageSubject      string `json:"message_subject,omitempty"`
	Compression         string `json:"compression,omitempty"`
}

// Connector represents an AWS Transfer connector used to initiate file transfers.
type Connector struct {
	SftpConfig         *ConnectorSftpConfig `json:"sftp_config,omitempty"`
	As2Config          *ConnectorAs2Config  `json:"as2_config,omitempty"`
	CreatedAt          time.Time            `json:"created_at"`
	Tags               map[string]string    `json:"tags"`
	ConnectorID        string               `json:"connector_id"`
	URL                string               `json:"url"`
	AccessRole         string               `json:"access_role"`
	AccountID          string               `json:"account_id"`
	Region             string               `json:"region"`
	LoggingRole        string               `json:"logging_role,omitempty"`
	SecurityPolicyName string               `json:"security_policy_name,omitempty"`
}

// FileTransferResult stores state for a file transfer operation started via StartFileTransfer.
type FileTransferResult struct {
	CreatedAt   time.Time `json:"created_at"`
	TransferID  string    `json:"transfer_id"`
	ConnectorID string    `json:"connector_id"`
	Status      string    `json:"status"`
	Files       []string  `json:"files,omitempty"`
}

// AsyncOperationRecord stores state for async connector operations (directory listing, delete, move).
type AsyncOperationRecord struct {
	CreatedAt   time.Time `json:"created_at"`
	ID          string    `json:"id"`
	ConnectorID string    `json:"connector_id"`
	Status      string    `json:"status"`
	Type        string    `json:"type"`
}

// cloneConnector returns a deep copy of a Connector.
func cloneConnector(c *Connector) *Connector {
	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	if c.SftpConfig != nil {
		sc := *c.SftpConfig
		if c.SftpConfig.TrustedHostKeys != nil {
			sc.TrustedHostKeys = make([]string, len(c.SftpConfig.TrustedHostKeys))
			copy(sc.TrustedHostKeys, c.SftpConfig.TrustedHostKeys)
		}
		cp.SftpConfig = &sc
	}

	if c.As2Config != nil {
		ac := *c.As2Config
		cp.As2Config = &ac
	}

	return &cp
}

// Profile represents an AWS Transfer AS2 profile.
type Profile struct {
	CreatedAt   time.Time         `json:"created_at"`
	Tags        map[string]string `json:"tags"`
	ProfileID   string            `json:"profile_id"`
	ProfileType string            `json:"profile_type"`
	As2ID       string            `json:"as2_id"`
	AccountID   string            `json:"account_id"`
	Region      string            `json:"region"`
}

// cloneProfile returns a deep copy of a Profile.
func cloneProfile(p *Profile) *Profile {
	cp := *p
	cp.Tags = make(map[string]string, len(p.Tags))
	maps.Copy(cp.Tags, p.Tags)

	return &cp
}

// WebAppIdentityProviderDetails holds identity provider configuration for a web app.
type WebAppIdentityProviderDetails struct {
	IdentityProviderType string `json:"identity_provider_type,omitempty"`
	InstanceArn          string `json:"instance_arn,omitempty"`
	Role                 string `json:"role,omitempty"`
	URL                  string `json:"url,omitempty"`
	Directory            string `json:"directory,omitempty"`
	Function             string `json:"function,omitempty"`
}

// WebApp represents an AWS Transfer web application.
type WebApp struct {
	IdentityProviderDetails *WebAppIdentityProviderDetails `json:"identity_provider_details,omitempty"`
	CreatedAt               time.Time                      `json:"created_at"`
	Tags                    map[string]string              `json:"tags"`
	WebAppID                string                         `json:"web_app_id"`
	AccountID               string                         `json:"account_id"`
	Region                  string                         `json:"region"`
}

// cloneWebApp returns a deep copy of a WebApp.
func cloneWebApp(w *WebApp) *WebApp {
	cp := *w
	cp.Tags = make(map[string]string, len(w.Tags))
	maps.Copy(cp.Tags, w.Tags)

	if w.IdentityProviderDetails != nil {
		ipd := *w.IdentityProviderDetails
		cp.IdentityProviderDetails = &ipd
	}

	return &cp
}

// CopyStepDetails holds details for a Copy workflow step.
type CopyStepDetails struct {
	DestinationFileLocation map[string]any `json:"destination_file_location,omitempty"`
	Name                    string         `json:"name,omitempty"`
	SourceFileLocation      string         `json:"source_file_location,omitempty"`
	OverwriteExisting       string         `json:"overwrite_existing,omitempty"`
}

// CustomStepDetails holds details for a Custom workflow step.
type CustomStepDetails struct {
	Name               string `json:"name,omitempty"`
	Target             string `json:"target,omitempty"`
	SourceFileLocation string `json:"source_file_location,omitempty"`
	Timeout            int32  `json:"timeout,omitempty"`
}

// DeleteStepDetails holds details for a Delete workflow step.
type DeleteStepDetails struct {
	Name               string `json:"name,omitempty"`
	SourceFileLocation string `json:"source_file_location,omitempty"`
}

// TagStepDetails holds details for a Tag workflow step.
type TagStepDetails struct {
	Name               string           `json:"name,omitempty"`
	SourceFileLocation string           `json:"source_file_location,omitempty"`
	Tags               []map[string]any `json:"tags,omitempty"`
}

// DecryptStepDetails holds details for a Decrypt workflow step.
type DecryptStepDetails struct {
	DestinationFileLocation map[string]any `json:"destination_file_location,omitempty"`
	Name                    string         `json:"name,omitempty"`
	Type                    string         `json:"type,omitempty"`
	SourceFileLocation      string         `json:"source_file_location,omitempty"`
	OverwriteExisting       string         `json:"overwrite_existing,omitempty"`
}

// WorkflowStep represents a single step in an AWS Transfer workflow.
type WorkflowStep struct {
	CopyStepDetails    *CopyStepDetails    `json:"copy_step_details,omitempty"`
	CustomStepDetails  *CustomStepDetails  `json:"custom_step_details,omitempty"`
	DeleteStepDetails  *DeleteStepDetails  `json:"delete_step_details,omitempty"`
	TagStepDetails     *TagStepDetails     `json:"tag_step_details,omitempty"`
	DecryptStepDetails *DecryptStepDetails `json:"decrypt_step_details,omitempty"`
	Type               string              `json:"type"`
}

// Workflow represents an AWS Transfer workflow for file processing.
type Workflow struct {
	CreatedAt        time.Time         `json:"created_at"`
	Tags             map[string]string `json:"tags"`
	WorkflowID       string            `json:"workflow_id"`
	Description      string            `json:"description"`
	AccountID        string            `json:"account_id"`
	Region           string            `json:"region"`
	Steps            []WorkflowStep    `json:"steps,omitempty"`
	OnExceptionSteps []WorkflowStep    `json:"on_exception_steps,omitempty"`
}

// cloneWorkflowSteps returns a deep copy of a WorkflowStep slice.
func cloneWorkflowSteps(steps []WorkflowStep) []WorkflowStep {
	if steps == nil {
		return nil
	}

	out := make([]WorkflowStep, len(steps))
	for i, s := range steps {
		out[i] = WorkflowStep{Type: s.Type}
		if s.CopyStepDetails != nil {
			cp := *s.CopyStepDetails
			out[i].CopyStepDetails = &cp
		}
		if s.CustomStepDetails != nil {
			cp := *s.CustomStepDetails
			out[i].CustomStepDetails = &cp
		}
		if s.DeleteStepDetails != nil {
			cp := *s.DeleteStepDetails
			out[i].DeleteStepDetails = &cp
		}
		if s.TagStepDetails != nil {
			cp := *s.TagStepDetails
			out[i].TagStepDetails = &cp
		}
		if s.DecryptStepDetails != nil {
			cp := *s.DecryptStepDetails
			out[i].DecryptStepDetails = &cp
		}
	}

	return out
}

// cloneWorkflow returns a deep copy of a Workflow.
func cloneWorkflow(w *Workflow) *Workflow {
	cp := *w
	cp.Tags = make(map[string]string, len(w.Tags))
	maps.Copy(cp.Tags, w.Tags)
	cp.Steps = cloneWorkflowSteps(w.Steps)
	cp.OnExceptionSteps = cloneWorkflowSteps(w.OnExceptionSteps)

	return &cp
}

// Certificate represents an imported AWS Transfer certificate.
type Certificate struct {
	NotBeforeDate time.Time         `json:"not_before_date,omitzero"`
	NotAfterDate  time.Time         `json:"not_after_date,omitzero"`
	ActiveDate    time.Time         `json:"active_date,omitzero"`
	InactiveDate  time.Time         `json:"inactive_date,omitzero"`
	CreatedAt     time.Time         `json:"created_at"`
	Tags          map[string]string `json:"tags"`
	CertificateID string            `json:"certificate_id"`
	Description   string            `json:"description"`
	Usage         string            `json:"usage"`
	Body          string            `json:"body"`
	Status        string            `json:"status"`
	AccountID     string            `json:"account_id"`
	Region        string            `json:"region"`
}

// HostKey represents an SSH host key associated with a Transfer server.
type HostKey struct {
	CreatedAt   time.Time         `json:"created_at"`
	Tags        map[string]string `json:"tags"`
	HostKeyID   string            `json:"host_key_id"`
	ServerID    string            `json:"server_id"`
	Description string            `json:"description"`
	Fingerprint string            `json:"fingerprint,omitempty"`
	Type        string            `json:"type"`
	Value       string            `json:"value"`
	AccountID   string            `json:"account_id"`
	Region      string            `json:"region"`
}

// cloneHostKey returns a deep copy of a HostKey.
func cloneHostKey(h *HostKey) *HostKey {
	cp := *h
	cp.Tags = make(map[string]string, len(h.Tags))
	maps.Copy(cp.Tags, h.Tags)

	return &cp
}

// SSHPublicKey represents an SSH public key attached to a Transfer user.
type SSHPublicKey struct {
	DateImported     time.Time `json:"date_imported"`
	SSHPublicKeyID   string    `json:"ssh_public_key_id"`
	SSHPublicKeyBody string    `json:"ssh_public_key_body"`
	Fingerprint      string    `json:"fingerprint,omitempty"`
	KeyType          string    `json:"key_type,omitempty"`
	UserName         string    `json:"user_name"`
	ServerID         string    `json:"server_id"`
}

// Execution represents the lifecycle of a workflow execution.
type Execution struct {
	InitialFileLocation map[string]any    `json:"initial_file_location,omitempty"`
	CurrentStep         *WorkflowStep     `json:"current_step,omitempty"`
	PendingTokens       map[string]string `json:"pending_tokens,omitempty"` // token -> stepName
	CreatedAt           time.Time         `json:"created_at"`
	ExecutionID         string            `json:"execution_id"`
	WorkflowID          string            `json:"workflow_id"`
	Status              string            `json:"status"` // "IN_PROGRESS", "COMPLETED", "EXCEPTION", "HANDLING_EXCEPTION"
}

// WebAppCustomization holds per-web-app branding customization.
type WebAppCustomization struct {
	WebAppID    string
	Title       string
	LogoFile    string
	FaviconFile string
}

// InMemoryBackend is the in-memory store for Transfer resources.
type InMemoryBackend struct {
	servers              map[string]*Server
	users                map[string]map[string]*User      // serverID -> userName -> User
	accesses             map[string]map[string]*Access    // serverID -> externalID -> Access
	agreements           map[string]map[string]*Agreement // serverID -> agreementID -> Agreement
	connectors           map[string]*Connector
	profiles             map[string]*Profile
	webApps              map[string]*WebApp
	webAppCustomizations map[string]*WebAppCustomization // webAppID -> customization
	workflows            map[string]*Workflow
	certificates         map[string]*Certificate
	hostKeys             map[string]map[string]*HostKey                 // serverID -> hostKeyID -> HostKey
	sshPublicKeys        map[string]map[string]map[string]*SSHPublicKey // serverID -> userName -> keyID -> SSHPublicKey
	// sshKeyBodies indexes normalized SSH key bodies for O(1) duplicate detection.
	sshKeyBodies    map[string]map[string]map[string]struct{} // serverID -> userName -> normalizedBody -> {}
	executions      map[string]map[string]*Execution          // workflowID -> executionID -> Execution
	tagsStore       map[string]map[string]string              // arn -> tags
	transferRecords map[string]*FileTransferResult            // transferID -> FileTransferResult
	asyncOperations map[string]*AsyncOperationRecord          // operationID -> AsyncOperationRecord
	mu              *lockmetrics.RWMutex
	work            *worker.Group
	accountID       string
	region          string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		servers:              make(map[string]*Server),
		users:                make(map[string]map[string]*User),
		accesses:             make(map[string]map[string]*Access),
		agreements:           make(map[string]map[string]*Agreement),
		connectors:           make(map[string]*Connector),
		profiles:             make(map[string]*Profile),
		webApps:              make(map[string]*WebApp),
		webAppCustomizations: make(map[string]*WebAppCustomization),
		workflows:            make(map[string]*Workflow),
		certificates:         make(map[string]*Certificate),
		hostKeys:             make(map[string]map[string]*HostKey),
		sshPublicKeys:        make(map[string]map[string]map[string]*SSHPublicKey),
		sshKeyBodies:         make(map[string]map[string]map[string]struct{}),
		executions:           make(map[string]map[string]*Execution),
		tagsStore:            make(map[string]map[string]string),
		transferRecords:      make(map[string]*FileTransferResult),
		asyncOperations:      make(map[string]*AsyncOperationRecord),
		accountID:            accountID,
		region:               region,
		mu:                   lockmetrics.New("transfer"),
		work:                 worker.NewGroup("transfer"),
	}
}

// Close stops all scheduled server state-transition timers so none outlives the
// backend. It is safe to call multiple times.
func (b *InMemoryBackend) Close() { b.work.Stop() }

// CreateServerInput holds all optional fields for CreateServer.
type CreateServerInput struct {
	IdentityProviderDetails       *IdentityProviderDetails
	EndpointDetails               *EndpointDetails
	ProtocolDetails               *ProtocolDetails
	WorkflowDetails               *WorkflowDetails
	S3StorageOptions              *S3StorageOptions
	Protocols                     []string
	Tags                          map[string]string
	IdentityProviderType          string
	EndpointType                  string
	LoggingRole                   string
	PreAuthenticationLoginBanner  string
	PostAuthenticationLoginBanner string
	HostKey                       string
	Certificate                   string
	Domain                        string
	SecurityPolicyName            string
	IPAddressType                 string
	StructuredLogDestinations     []string
}

// CreateServer creates a new Transfer Family server.
func (b *InMemoryBackend) CreateServer(
	protocols []string,
	tags map[string]string,
) (*Server, error) {
	return b.CreateServerFull(&CreateServerInput{
		Protocols: protocols,
		Tags:      tags,
	})
}

// CreateServerFull creates a new Transfer Family server with full configuration.
// validateAndDefaultServerProtocols validates protocols and returns the default if empty.
func validateAndDefaultServerProtocols(protocols []string) ([]string, error) {
	if len(protocols) == 0 {
		return []string{protocolSFTP}, nil
	}

	for _, p := range protocols {
		switch p {
		case protocolSFTP, "FTP", "FTPS", "AS2":
			// valid
		default:
			return nil, fmt.Errorf("%w: %s", ErrInvalidProtocol, p)
		}
	}

	return protocols, nil
}

// validateAndDefaultIdentityProviderType validates and defaults the identity provider type.
func validateAndDefaultIdentityProviderType(t string) (string, error) {
	if t == "" {
		return identityProviderServiceManaged, nil
	}

	switch t {
	case identityProviderServiceManaged, identityProviderAPIGateway,
		identityProviderDirectoryService, identityProviderLambda:
		return t, nil
	default:
		return "", fmt.Errorf(
			"%w: IdentityProviderType must be one of SERVICE_MANAGED, API_GATEWAY,"+
				" AWS_DIRECTORY_SERVICE, AWS_LAMBDA, got %q",
			ErrValidation, t,
		)
	}
}

// validateAndDefaultDomain validates and defaults the domain.
func validateAndDefaultDomain(domain string) (string, error) {
	if domain == "" {
		return "S3", nil
	}

	switch domain {
	case "S3", "EFS":
		return domain, nil
	default:
		return "", fmt.Errorf("%w: Domain must be S3 or EFS, got %q", ErrValidation, domain)
	}
}

// validateAndDefaultEndpointType validates and defaults the endpoint type.
func validateAndDefaultEndpointType(t string) (string, error) {
	if t == "" {
		return endpointTypePublic, nil
	}

	switch t {
	case endpointTypePublic, endpointTypeVPC, endpointTypeVPCEndpoint:
		return t, nil
	default:
		return "", fmt.Errorf(
			"%w: EndpointType must be PUBLIC, VPC, or VPC_ENDPOINT, got %q",
			ErrValidation, t,
		)
	}
}

// validateProtocolDetails validates the protocol details TLS mode.
func validateProtocolDetails(pd *ProtocolDetails) error {
	if pd == nil || pd.TLSSessionResumptionMode == "" {
		return nil
	}

	switch pd.TLSSessionResumptionMode {
	case "DISABLED", "ENABLED", "ENFORCED":
		return nil
	default:
		return fmt.Errorf(
			"%w: TlsSessionResumptionMode must be DISABLED, ENABLED, or ENFORCED, got %q",
			ErrValidation, pd.TLSSessionResumptionMode,
		)
	}
}

func (b *InMemoryBackend) CreateServerFull(in *CreateServerInput) (*Server, error) {
	b.mu.Lock("CreateServer")
	defer b.mu.Unlock()

	serverID := "s-" + uuid.NewString()[:20]

	protocols, err := validateAndDefaultServerProtocols(in.Protocols)
	if err != nil {
		return nil, err
	}

	identityProviderType, err := validateAndDefaultIdentityProviderType(in.IdentityProviderType)
	if err != nil {
		return nil, err
	}

	domain, err := validateAndDefaultDomain(in.Domain)
	if err != nil {
		return nil, err
	}

	endpointType, err := validateAndDefaultEndpointType(in.EndpointType)
	if err != nil {
		return nil, err
	}

	if pdErr := validateProtocolDetails(in.ProtocolDetails); pdErr != nil {
		return nil, pdErr
	}

	merged := make(map[string]string, len(in.Tags))
	maps.Copy(merged, in.Tags)

	s := &Server{
		ServerID:                      serverID,
		State:                         serverStatusOnline,
		Endpoint:                      fmt.Sprintf("%s.server.transfer.%s.amazonaws.com", serverID, b.region),
		Protocols:                     protocols,
		Domain:                        domain,
		IdentityProviderType:          identityProviderType,
		EndpointType:                  endpointType,
		LoggingRole:                   in.LoggingRole,
		PreAuthenticationLoginBanner:  in.PreAuthenticationLoginBanner,
		PostAuthenticationLoginBanner: in.PostAuthenticationLoginBanner,
		HostKey:                       in.HostKey,
		Certificate:                   in.Certificate,
		SecurityPolicyName:            in.SecurityPolicyName,
		IPAddressType:                 in.IPAddressType,
		IdentityProviderDetails:       in.IdentityProviderDetails,
		EndpointDetails:               in.EndpointDetails,
		ProtocolDetails:               in.ProtocolDetails,
		WorkflowDetails:               in.WorkflowDetails,
		S3StorageOptions:              in.S3StorageOptions,
		StructuredLogDestinations:     in.StructuredLogDestinations,
		CreatedAt:                     time.Now(),
		Tags:                          merged,
		AccountID:                     b.accountID,
		Region:                        b.region,
	}
	b.servers[serverID] = s
	b.users[serverID] = make(map[string]*User)

	return cloneServer(s), nil
}

// DescribeServer returns the server with the given ID.
func (b *InMemoryBackend) DescribeServer(serverID string) (*Server, error) {
	b.mu.RLock("DescribeServer")
	defer b.mu.RUnlock()

	s, ok := b.servers[serverID]
	if !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	return cloneServer(s), nil
}

// ServerUserCount returns the number of users on the given server.
// Returns 0 if the server does not exist.
func (b *InMemoryBackend) ServerUserCount(serverID string) int {
	b.mu.RLock("ServerUserCount")
	defer b.mu.RUnlock()

	return len(b.users[serverID])
}

// ListServers returns all servers sorted by ServerId (ascending, deterministic).
func (b *InMemoryBackend) ListServers() []Server {
	b.mu.RLock("ListServers")
	defer b.mu.RUnlock()

	out := make([]Server, 0, len(b.servers))
	for _, s := range b.servers {
		out = append(out, *cloneServer(s))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ServerID < out[j].ServerID
	})

	return out
}

// ErrServerOnline is returned when an operation requires the server to be OFFLINE.
var ErrServerOnline = awserr.New(
	"ConflictException: server must be offline to be deleted",
	awserr.ErrConflict,
)

// DeleteServer removes a server and all of its associated resources (users, accesses, agreements,
// SSH keys, and host keys). The server must be OFFLINE; returns ErrServerOnline otherwise.
func (b *InMemoryBackend) DeleteServer(serverID string) error {
	b.mu.Lock("DeleteServer")
	defer b.mu.Unlock()

	s, ok := b.servers[serverID]
	if !ok {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	// AWS requires the server to be OFFLINE before deletion.
	// STOPPING is also accepted — server is already transitioning offline.
	if s.State == serverStatusOnline || s.State == serverStatusStarting {
		return fmt.Errorf("%w: server %s is in state %s", ErrServerOnline, serverID, s.State)
	}

	delete(b.servers, serverID)
	delete(b.users, serverID)
	delete(b.accesses, serverID)
	delete(b.agreements, serverID)
	delete(b.sshPublicKeys, serverID)
	delete(b.sshKeyBodies, serverID)
	delete(b.hostKeys, serverID)

	return nil
}

// startServerTransitionDelay is the async delay before a server reaches ONLINE/OFFLINE state.
const startServerTransitionDelay = 100 * time.Millisecond

// StartServer transitions a server to ONLINE state (via STARTING).
// Calling StartServer on an already-ONLINE server is idempotent (no-op, no error).
func (b *InMemoryBackend) StartServer(serverID string) error {
	b.mu.Lock("StartServer")
	defer b.mu.Unlock()

	s, ok := b.servers[serverID]
	if !ok {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	// Idempotent: already ONLINE is a no-op.
	if s.State == serverStatusOnline {
		return nil
	}

	// Set to STARTING, then transition to ONLINE asynchronously.
	s.State = serverStatusStarting

	b.work.After("StartServerTransition", startServerTransitionDelay, func() {
		b.mu.Lock("StartServer-async")
		defer b.mu.Unlock()

		if sv, found := b.servers[serverID]; found && sv.State == serverStatusStarting {
			sv.State = serverStatusOnline
		}
	})

	return nil
}

// StopServer transitions a server to OFFLINE state (via STOPPING).
// Calling StopServer on an already-OFFLINE server is idempotent (no-op, no error).
func (b *InMemoryBackend) StopServer(serverID string) error {
	b.mu.Lock("StopServer")
	defer b.mu.Unlock()

	s, ok := b.servers[serverID]
	if !ok {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	// Idempotent: already OFFLINE is a no-op.
	if s.State == serverStatusOffline {
		return nil
	}

	// Set to STOPPING, then transition to OFFLINE asynchronously.
	s.State = serverStatusStopping

	b.work.After("StopServerTransition", startServerTransitionDelay, func() {
		b.mu.Lock("StopServer-async")
		defer b.mu.Unlock()

		if sv, found := b.servers[serverID]; found && sv.State == serverStatusStopping {
			sv.State = serverStatusOffline
		}
	})

	return nil
}

// UpdateServerInput holds all optional fields for UpdateServer.
type UpdateServerInput struct {
	IdentityProviderDetails       *IdentityProviderDetails
	EndpointDetails               *EndpointDetails
	ProtocolDetails               *ProtocolDetails
	WorkflowDetails               *WorkflowDetails
	S3StorageOptions              *S3StorageOptions
	SecurityPolicyName            string
	ServerID                      string
	Certificate                   string
	EndpointType                  string
	HostKey                       string
	LoggingRole                   string
	PreAuthenticationLoginBanner  string
	PostAuthenticationLoginBanner string
	IPAddressType                 string
	StructuredLogDestinations     []string
	Protocols                     []string
	SetLoggingRole                bool
	SetIdentityProviderDetails    bool
	SetCertificate                bool
	SetPreAuthBanner              bool
	SetPostAuthBanner             bool
	SetSecurityPolicyName         bool
	SetIPAddressType              bool
	SetEndpointType               bool
	SetEndpointDetails            bool
	SetProtocolDetails            bool
	SetWorkflowDetails            bool
	SetS3StorageOptions           bool
	SetStructuredLogDestinations  bool
	SetHostKey                    bool
}

// UpdateServer updates mutable fields on an existing server.
func (b *InMemoryBackend) UpdateServer(serverID string, protocols []string) (*Server, error) {
	return b.UpdateServerFull(&UpdateServerInput{
		ServerID:  serverID,
		Protocols: protocols,
	})
}

// applyServerStringFields applies optional string updates to a server.
func applyServerStringFields(s *Server, in *UpdateServerInput) {
	if len(in.Protocols) > 0 {
		s.Protocols = in.Protocols
	}

	if in.SetCertificate {
		s.Certificate = in.Certificate
	}

	if in.SetEndpointType && in.EndpointType != "" {
		s.EndpointType = in.EndpointType
	}

	if in.SetLoggingRole {
		s.LoggingRole = in.LoggingRole
	}

	if in.SetPreAuthBanner {
		s.PreAuthenticationLoginBanner = in.PreAuthenticationLoginBanner
	}

	if in.SetPostAuthBanner {
		s.PostAuthenticationLoginBanner = in.PostAuthenticationLoginBanner
	}

	if in.SetSecurityPolicyName {
		s.SecurityPolicyName = in.SecurityPolicyName
	}

	if in.SetIPAddressType {
		s.IPAddressType = in.IPAddressType
	}

	if in.SetHostKey {
		s.HostKey = in.HostKey
	}
}

// applyServerStructFields applies optional struct updates to a server.
func applyServerStructFields(s *Server, in *UpdateServerInput) {
	if in.SetIdentityProviderDetails {
		s.IdentityProviderDetails = in.IdentityProviderDetails
	}

	if in.SetEndpointDetails {
		s.EndpointDetails = in.EndpointDetails
	}

	if in.SetProtocolDetails {
		s.ProtocolDetails = in.ProtocolDetails
	}

	if in.SetWorkflowDetails {
		s.WorkflowDetails = in.WorkflowDetails
	}

	if in.SetS3StorageOptions {
		s.S3StorageOptions = in.S3StorageOptions
	}

	if in.SetStructuredLogDestinations {
		s.StructuredLogDestinations = in.StructuredLogDestinations
	}
}

// UpdateServerFull updates all mutable fields on an existing server.
func (b *InMemoryBackend) UpdateServerFull(in *UpdateServerInput) (*Server, error) {
	b.mu.Lock("UpdateServer")
	defer b.mu.Unlock()

	s, ok := b.servers[in.ServerID]
	if !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, in.ServerID)
	}

	applyServerStringFields(s, in)
	applyServerStructFields(s, in)

	return cloneServer(s), nil
}

// CreateUserInput holds all optional fields for CreateUser.
type CreateUserInput struct {
	PosixProfile          *PosixProfile
	Tags                  map[string]string
	ServerID              string
	UserName              string
	HomeDir               string
	Role                  string
	HomeDirectoryType     string
	Policy                string
	HomeDirectoryMappings []HomeDirectoryMapEntry
}

// CreateUser creates a user on the given server.
func (b *InMemoryBackend) CreateUser(
	serverID, userName, homeDir, role string,
	tags map[string]string,
) (*User, error) {
	return b.CreateUserFull(&CreateUserInput{
		ServerID: serverID,
		UserName: userName,
		HomeDir:  homeDir,
		Role:     role,
		Tags:     tags,
	})
}

// CreateUserFull creates a user on the given server with full configuration.
func (b *InMemoryBackend) CreateUserFull(in *CreateUserInput) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if _, ok := b.servers[in.ServerID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, in.ServerID)
	}

	if _, ok := b.users[in.ServerID][in.UserName]; ok {
		return nil, fmt.Errorf(
			"%w: user %s already exists on server %s",
			ErrUserAlreadyExists,
			in.UserName,
			in.ServerID,
		)
	}

	// Validate HomeDirectoryType.
	homeDirectoryType := in.HomeDirectoryType
	if homeDirectoryType == "" {
		homeDirectoryType = homeDirectoryTypePath
	} else {
		switch homeDirectoryType {
		case homeDirectoryTypePath, homeDirectoryTypeLogical:
			// valid
		default:
			return nil, fmt.Errorf(
				"%w: HomeDirectoryType must be PATH or LOGICAL, got %q",
				ErrValidation,
				homeDirectoryType,
			)
		}
	}

	merged := make(map[string]string, len(in.Tags))
	maps.Copy(merged, in.Tags)

	u := &User{
		UserName:              in.UserName,
		ServerID:              in.ServerID,
		HomeDir:               in.HomeDir,
		Role:                  in.Role,
		HomeDirectoryType:     homeDirectoryType,
		HomeDirectoryMappings: in.HomeDirectoryMappings,
		Policy:                in.Policy,
		PosixProfile:          in.PosixProfile,
		CreatedAt:             time.Now(),
		Tags:                  merged,
		AccountID:             b.accountID,
		Region:                b.region,
	}
	b.users[in.ServerID][in.UserName] = u

	return cloneUser(u), nil
}

// DescribeUser returns the user with the given name on the given server.
func (b *InMemoryBackend) DescribeUser(serverID, userName string) (*User, error) {
	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	users, ok := b.users[serverID]
	if !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	u, ok := users[userName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: user %s not found on server %s",
			ErrUserNotFound,
			userName,
			serverID,
		)
	}

	return cloneUser(u), nil
}

// ListUsers returns all users on a server sorted by username.
func (b *InMemoryBackend) ListUsers(serverID string) ([]User, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	users, ok := b.users[serverID]
	if !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	out := make([]User, 0, len(users))
	for _, u := range users {
		out = append(out, *cloneUser(u))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].UserName < out[j].UserName
	})

	return out, nil
}

// DeleteUser removes a user from the given server and also deletes all SSH public keys for that user.
func (b *InMemoryBackend) DeleteUser(serverID, userName string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	users, ok := b.users[serverID]
	if !ok {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	if _, exists := users[userName]; !exists {
		return fmt.Errorf("%w: user %s not found on server %s", ErrUserNotFound, userName, serverID)
	}

	delete(users, userName)

	// Delete all SSH public keys for this user.
	if serverKeys, exists := b.sshPublicKeys[serverID]; exists {
		delete(serverKeys, userName)
	}

	if serverBodies, exists := b.sshKeyBodies[serverID]; exists {
		delete(serverBodies, userName)
	}

	return nil
}

// UpdateUserInput holds all optional fields for UpdateUser.
type UpdateUserInput struct {
	PosixProfile             *PosixProfile
	ServerID                 string
	UserName                 string
	HomeDir                  string
	Role                     string
	HomeDirectoryType        string
	Policy                   string
	HomeDirectoryMappings    []HomeDirectoryMapEntry
	SetPosixProfile          bool
	SetHomeDirectoryMappings bool
	SetPolicy                bool
	SetHomeDirectoryType     bool
}

// UpdateUser updates mutable fields on a user.
func (b *InMemoryBackend) UpdateUser(serverID, userName, homeDir, role string) (*User, error) {
	return b.UpdateUserFull(&UpdateUserInput{
		ServerID: serverID,
		UserName: userName,
		HomeDir:  homeDir,
		Role:     role,
	})
}

// UpdateUserFull updates all mutable fields on a user.
func (b *InMemoryBackend) UpdateUserFull(in *UpdateUserInput) (*User, error) {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	users, ok := b.users[in.ServerID]
	if !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, in.ServerID)
	}

	u, ok := users[in.UserName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: user %s not found on server %s",
			ErrUserNotFound,
			in.UserName,
			in.ServerID,
		)
	}

	if in.HomeDir != "" {
		u.HomeDir = in.HomeDir
	}

	if in.Role != "" {
		u.Role = in.Role
	}

	if in.SetPosixProfile {
		u.PosixProfile = in.PosixProfile
	}

	if in.SetHomeDirectoryMappings {
		u.HomeDirectoryMappings = in.HomeDirectoryMappings
	}

	if in.SetPolicy {
		u.Policy = in.Policy
	}

	if in.SetHomeDirectoryType && in.HomeDirectoryType != "" {
		switch in.HomeDirectoryType {
		case homeDirectoryTypePath, homeDirectoryTypeLogical:
			u.HomeDirectoryType = in.HomeDirectoryType
		default:
			return nil, fmt.Errorf(
				"%w: HomeDirectoryType must be PATH or LOGICAL, got %q",
				ErrValidation,
				in.HomeDirectoryType,
			)
		}
	}

	return cloneUser(u), nil
}

// AccountID returns the AWS account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// ListSSHPublicKeys returns all SSH public keys for a user on a server.
func (b *InMemoryBackend) ListSSHPublicKeys(serverID, userName string) []*SSHPublicKey {
	b.mu.RLock("ListSSHPublicKeys")
	defer b.mu.RUnlock()

	userMap, ok := b.sshPublicKeys[serverID]
	if !ok {
		return nil
	}

	keyMap, ok := userMap[userName]
	if !ok {
		return nil
	}

	keys := make([]*SSHPublicKey, 0, len(keyMap))
	for _, k := range keyMap {
		cp := *k
		keys = append(keys, &cp)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].SSHPublicKeyID < keys[j].SSHPublicKeyID
	})

	return keys
}

// Reset clears all stored resources, returning the backend to a clean state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.servers = make(map[string]*Server)
	b.users = make(map[string]map[string]*User)
	b.accesses = make(map[string]map[string]*Access)
	b.agreements = make(map[string]map[string]*Agreement)
	b.connectors = make(map[string]*Connector)
	b.profiles = make(map[string]*Profile)
	b.webApps = make(map[string]*WebApp)
	b.webAppCustomizations = make(map[string]*WebAppCustomization)
	b.workflows = make(map[string]*Workflow)
	b.certificates = make(map[string]*Certificate)
	b.hostKeys = make(map[string]map[string]*HostKey)
	b.sshPublicKeys = make(map[string]map[string]map[string]*SSHPublicKey)
	b.sshKeyBodies = make(map[string]map[string]map[string]struct{})
	b.executions = make(map[string]map[string]*Execution)
	b.tagsStore = make(map[string]map[string]string)
	b.transferRecords = make(map[string]*FileTransferResult)
	b.asyncOperations = make(map[string]*AsyncOperationRecord)
}

// CreateAccessInput holds all optional fields for CreateAccess.
type CreateAccessInput struct {
	PosixProfile          *PosixProfile
	Tags                  map[string]string
	ServerID              string
	ExternalID            string
	Role                  string
	HomeDir               string
	HomeDirectoryType     string
	Policy                string
	HomeDirectoryMappings []HomeDirectoryMapEntry
}

// CreateAccess creates an access policy entry on an existing server.
func (b *InMemoryBackend) CreateAccess(
	serverID, externalID, role, homeDir string,
	tags map[string]string,
) (*Access, error) {
	return b.CreateAccessFull(&CreateAccessInput{
		ServerID:   serverID,
		ExternalID: externalID,
		Role:       role,
		HomeDir:    homeDir,
		Tags:       tags,
	})
}

// CreateAccessFull creates an access entry with full configuration.
func (b *InMemoryBackend) CreateAccessFull(in *CreateAccessInput) (*Access, error) {
	b.mu.Lock("CreateAccess")
	defer b.mu.Unlock()

	if _, ok := b.servers[in.ServerID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, in.ServerID)
	}

	if _, ok := b.accesses[in.ServerID]; !ok {
		b.accesses[in.ServerID] = make(map[string]*Access)
	}

	// ExternalId must be unique per server.
	if _, exists := b.accesses[in.ServerID][in.ExternalID]; exists {
		return nil, fmt.Errorf(
			"%w: access with ExternalId %s already exists on server %s",
			ErrAccessAlreadyExists,
			in.ExternalID,
			in.ServerID,
		)
	}

	merged := make(map[string]string, len(in.Tags))
	maps.Copy(merged, in.Tags)

	homeDirectoryType := in.HomeDirectoryType
	if homeDirectoryType == "" {
		homeDirectoryType = homeDirectoryTypePath
	}

	a := &Access{
		ExternalID:            in.ExternalID,
		ServerID:              in.ServerID,
		Role:                  in.Role,
		HomeDir:               in.HomeDir,
		HomeDirectoryType:     homeDirectoryType,
		HomeDirectoryMappings: in.HomeDirectoryMappings,
		Policy:                in.Policy,
		PosixProfile:          in.PosixProfile,
		CreatedAt:             time.Now(),
		Tags:                  merged,
		AccountID:             b.accountID,
		Region:                b.region,
	}
	b.accesses[in.ServerID][in.ExternalID] = a

	return cloneAccess(a), nil
}

// DeleteAccess removes an access entry from a server.
func (b *InMemoryBackend) DeleteAccess(serverID, externalID string) error {
	b.mu.Lock("DeleteAccess")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	serverAccesses, ok := b.accesses[serverID]
	if !ok {
		return fmt.Errorf(
			"%w: access %s not found on server %s",
			ErrAccessNotFound,
			externalID,
			serverID,
		)
	}

	if _, exists := serverAccesses[externalID]; !exists {
		return fmt.Errorf(
			"%w: access %s not found on server %s",
			ErrAccessNotFound,
			externalID,
			serverID,
		)
	}

	delete(serverAccesses, externalID)

	return nil
}

// CreateAgreement creates an AS2 agreement on an existing server.
func (b *InMemoryBackend) CreateAgreement(
	serverID, description, localProfileID, partnerProfileID, baseDirectory, accessRole string,
	tags map[string]string,
) (*Agreement, error) {
	return b.CreateAgreementFull(
		serverID,
		description,
		localProfileID,
		partnerProfileID,
		baseDirectory,
		accessRole,
		agreementStatusActive,
		tags,
	)
}

// CreateAgreementFull creates an AS2 agreement with an explicit initial status.
func (b *InMemoryBackend) CreateAgreementFull(
	serverID, description, localProfileID, partnerProfileID, baseDirectory, accessRole, status string,
	tags map[string]string,
) (*Agreement, error) {
	b.mu.Lock("CreateAgreement")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	// Validate status.
	if status == "" {
		status = agreementStatusActive
	}

	switch status {
	case agreementStatusActive, agreementStatusInactive:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: Status must be ACTIVE or INACTIVE, got %q",
			ErrValidation,
			status,
		)
	}

	if _, ok := b.agreements[serverID]; !ok {
		b.agreements[serverID] = make(map[string]*Agreement)
	}

	agreementID := "a-" + uuid.NewString()[:20]

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	ag := &Agreement{
		AgreementID:      agreementID,
		ServerID:         serverID,
		Description:      description,
		LocalProfileID:   localProfileID,
		PartnerProfileID: partnerProfileID,
		BaseDirectory:    baseDirectory,
		AccessRole:       accessRole,
		Status:           status,
		CreatedAt:        time.Now(),
		Tags:             merged,
		AccountID:        b.accountID,
		Region:           b.region,
	}
	b.agreements[serverID][agreementID] = ag

	return cloneAgreement(ag), nil
}

// DeleteAgreement removes an AS2 agreement from a server.
func (b *InMemoryBackend) DeleteAgreement(serverID, agreementID string) error {
	b.mu.Lock("DeleteAgreement")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	serverAgreements, ok := b.agreements[serverID]
	if !ok {
		return fmt.Errorf(
			"%w: agreement %s not found on server %s",
			ErrAgreementNotFound,
			agreementID,
			serverID,
		)
	}

	if _, exists := serverAgreements[agreementID]; !exists {
		return fmt.Errorf(
			"%w: agreement %s not found on server %s",
			ErrAgreementNotFound,
			agreementID,
			serverID,
		)
	}

	delete(serverAgreements, agreementID)

	return nil
}

// CreateConnectorInput holds all fields for CreateConnector.
type CreateConnectorInput struct {
	SftpConfig         *ConnectorSftpConfig
	As2Config          *ConnectorAs2Config
	Tags               map[string]string
	URL                string
	AccessRole         string
	LoggingRole        string
	SecurityPolicyName string
}

// CreateConnector creates a Transfer connector. URL is required.
func (b *InMemoryBackend) CreateConnector(
	url, accessRole string,
	sftpConfig *ConnectorSftpConfig,
	as2Config *ConnectorAs2Config,
	tags map[string]string,
) (*Connector, error) {
	return b.CreateConnectorFull(&CreateConnectorInput{
		URL:        url,
		AccessRole: accessRole,
		SftpConfig: sftpConfig,
		As2Config:  as2Config,
		Tags:       tags,
	})
}

// CreateConnectorFull creates a Transfer connector with full configuration.
func (b *InMemoryBackend) CreateConnectorFull(in *CreateConnectorInput) (*Connector, error) {
	if in.URL == "" {
		return nil, fmt.Errorf("%w: Url is required", ErrValidation)
	}

	b.mu.Lock("CreateConnector")
	defer b.mu.Unlock()

	connectorID := "c-" + uuid.NewString()[:20]

	merged := make(map[string]string, len(in.Tags))
	maps.Copy(merged, in.Tags)

	c := &Connector{
		ConnectorID:        connectorID,
		URL:                in.URL,
		AccessRole:         in.AccessRole,
		SftpConfig:         in.SftpConfig,
		As2Config:          in.As2Config,
		LoggingRole:        in.LoggingRole,
		SecurityPolicyName: in.SecurityPolicyName,
		CreatedAt:          time.Now(),
		Tags:               merged,
		AccountID:          b.accountID,
		Region:             b.region,
	}
	b.connectors[connectorID] = c

	return cloneConnector(c), nil
}

// DeleteConnector removes a connector by ID.
func (b *InMemoryBackend) DeleteConnector(connectorID string) error {
	b.mu.Lock("DeleteConnector")
	defer b.mu.Unlock()

	if _, ok := b.connectors[connectorID]; !ok {
		return fmt.Errorf("%w: connector %s not found", ErrConnectorNotFound, connectorID)
	}

	delete(b.connectors, connectorID)

	return nil
}

// CreateProfile creates an AS2 profile. ProfileType must be LOCAL or PARTNER.
func (b *InMemoryBackend) CreateProfile(
	profileType, as2ID string,
	tags map[string]string,
) (*Profile, error) {
	switch profileType {
	case profileTypeLocal, profileTypePartner:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: ProfileType must be LOCAL or PARTNER, got %q",
			ErrValidation,
			profileType,
		)
	}

	b.mu.Lock("CreateProfile")
	defer b.mu.Unlock()

	profileID := "p-" + uuid.NewString()[:20]

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	p := &Profile{
		ProfileID:   profileID,
		ProfileType: profileType,
		As2ID:       as2ID,
		CreatedAt:   time.Now(),
		Tags:        merged,
		AccountID:   b.accountID,
		Region:      b.region,
	}
	b.profiles[profileID] = p

	return cloneProfile(p), nil
}

// CreateWebApp creates a Transfer web application.
func (b *InMemoryBackend) CreateWebApp(tags map[string]string) (*WebApp, error) {
	b.mu.Lock("CreateWebApp")
	defer b.mu.Unlock()

	webAppID := "webapp-" + uuid.NewString()[:20]

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	w := &WebApp{
		WebAppID:  webAppID,
		CreatedAt: time.Now(),
		Tags:      merged,
		AccountID: b.accountID,
		Region:    b.region,
	}
	b.webApps[webAppID] = w

	return cloneWebApp(w), nil
}

// CreateWorkflow creates a Transfer workflow.
func (b *InMemoryBackend) CreateWorkflow(
	description string,
	steps []WorkflowStep,
	onExceptionSteps []WorkflowStep,
	tags map[string]string,
) (*Workflow, error) {
	b.mu.Lock("CreateWorkflow")
	defer b.mu.Unlock()

	workflowID := "w-" + uuid.NewString()[:20]

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	wf := &Workflow{
		WorkflowID:       workflowID,
		Description:      description,
		Steps:            cloneWorkflowSteps(steps),
		OnExceptionSteps: cloneWorkflowSteps(onExceptionSteps),
		CreatedAt:        time.Now(),
		Tags:             merged,
		AccountID:        b.accountID,
		Region:           b.region,
	}
	b.workflows[workflowID] = wf

	return cloneWorkflow(wf), nil
}

// DeleteCertificate removes a certificate by ID.
func (b *InMemoryBackend) DeleteCertificate(certificateID string) error {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	if _, ok := b.certificates[certificateID]; !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrCertificateNotFound, certificateID)
	}

	delete(b.certificates, certificateID)

	return nil
}

// DescribeAccess returns an access entry from a server.
func (b *InMemoryBackend) DescribeAccess(serverID, externalID string) (*Access, error) {
	b.mu.RLock("DescribeAccess")
	defer b.mu.RUnlock()

	serverAccesses, ok := b.accesses[serverID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: access %s not found on server %s",
			ErrAccessNotFound,
			externalID,
			serverID,
		)
	}

	a, ok := serverAccesses[externalID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: access %s not found on server %s",
			ErrAccessNotFound,
			externalID,
			serverID,
		)
	}

	return cloneAccess(a), nil
}

// ListAccesses returns all accesses on a server sorted by externalID.
func (b *InMemoryBackend) ListAccesses(serverID string) ([]*Access, error) {
	b.mu.RLock("ListAccesses")
	defer b.mu.RUnlock()

	if _, ok := b.servers[serverID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	serverAccesses := b.accesses[serverID]
	out := make([]*Access, 0, len(serverAccesses))

	for _, a := range serverAccesses {
		out = append(out, cloneAccess(a))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ExternalID < out[j].ExternalID
	})

	return out, nil
}

// UpdateAccess updates mutable fields on an access entry.
func (b *InMemoryBackend) UpdateAccess(
	serverID, externalID, role, homeDir string,
) (*Access, error) {
	b.mu.Lock("UpdateAccess")
	defer b.mu.Unlock()

	serverAccesses, ok := b.accesses[serverID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: access %s not found on server %s",
			ErrAccessNotFound,
			externalID,
			serverID,
		)
	}

	a, ok := serverAccesses[externalID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: access %s not found on server %s",
			ErrAccessNotFound,
			externalID,
			serverID,
		)
	}

	if role != "" {
		a.Role = role
	}

	if homeDir != "" {
		a.HomeDir = homeDir
	}

	return cloneAccess(a), nil
}

// DescribeAgreement returns an agreement from a server.
func (b *InMemoryBackend) DescribeAgreement(serverID, agreementID string) (*Agreement, error) {
	b.mu.RLock("DescribeAgreement")
	defer b.mu.RUnlock()

	serverAgreements, ok := b.agreements[serverID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: agreement %s not found on server %s",
			ErrAgreementNotFound,
			agreementID,
			serverID,
		)
	}

	ag, ok := serverAgreements[agreementID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: agreement %s not found on server %s",
			ErrAgreementNotFound,
			agreementID,
			serverID,
		)
	}

	return cloneAgreement(ag), nil
}

// ListAgreements returns all agreements on a server sorted by agreementID.
func (b *InMemoryBackend) ListAgreements(serverID string) ([]*Agreement, error) {
	b.mu.RLock("ListAgreements")
	defer b.mu.RUnlock()

	if _, ok := b.servers[serverID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	serverAgreements := b.agreements[serverID]
	out := make([]*Agreement, 0, len(serverAgreements))

	for _, ag := range serverAgreements {
		out = append(out, cloneAgreement(ag))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].AgreementID < out[j].AgreementID
	})

	return out, nil
}

// UpdateAgreement updates mutable fields on an agreement.
func (b *InMemoryBackend) UpdateAgreement(
	serverID, agreementID, description, status string,
) (*Agreement, error) {
	b.mu.Lock("UpdateAgreement")
	defer b.mu.Unlock()

	serverAgreements, ok := b.agreements[serverID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: agreement %s not found on server %s",
			ErrAgreementNotFound,
			agreementID,
			serverID,
		)
	}

	ag, ok := serverAgreements[agreementID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: agreement %s not found on server %s",
			ErrAgreementNotFound,
			agreementID,
			serverID,
		)
	}

	if description != "" {
		ag.Description = description
	}

	if status != "" {
		switch status {
		case agreementStatusActive, agreementStatusInactive:
			// valid
		default:
			return nil, fmt.Errorf(
				"%w: Status must be ACTIVE or INACTIVE, got %q",
				ErrValidation,
				status,
			)
		}

		ag.Status = status
	}

	return cloneAgreement(ag), nil
}

// DescribeConnector returns a connector by ID.
func (b *InMemoryBackend) DescribeConnector(connectorID string) (*Connector, error) {
	b.mu.RLock("DescribeConnector")
	defer b.mu.RUnlock()

	c, ok := b.connectors[connectorID]
	if !ok {
		return nil, fmt.Errorf("%w: connector %s not found", ErrConnectorNotFound, connectorID)
	}

	return cloneConnector(c), nil
}

// ListConnectors returns all connectors sorted by connectorID.
func (b *InMemoryBackend) ListConnectors() []*Connector {
	b.mu.RLock("ListConnectors")
	defer b.mu.RUnlock()

	out := make([]*Connector, 0, len(b.connectors))

	for _, c := range b.connectors {
		out = append(out, cloneConnector(c))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ConnectorID < out[j].ConnectorID
	})

	return out
}

// UpdateConnectorInput holds all optional fields for UpdateConnector.
type UpdateConnectorInput struct {
	SftpConfig            *ConnectorSftpConfig
	As2Config             *ConnectorAs2Config
	ConnectorID           string
	URL                   string
	AccessRole            string
	LoggingRole           string
	SecurityPolicyName    string
	SetLoggingRole        bool
	SetSecurityPolicyName bool
}

// UpdateConnector updates mutable fields on a connector.
func (b *InMemoryBackend) UpdateConnector(
	connectorID, url, accessRole string,
	sftpConfig *ConnectorSftpConfig,
	as2Config *ConnectorAs2Config,
) (*Connector, error) {
	return b.UpdateConnectorFull(&UpdateConnectorInput{
		ConnectorID: connectorID,
		URL:         url,
		AccessRole:  accessRole,
		SftpConfig:  sftpConfig,
		As2Config:   as2Config,
	})
}

// UpdateConnectorFull updates all mutable fields on a connector.
func (b *InMemoryBackend) UpdateConnectorFull(in *UpdateConnectorInput) (*Connector, error) {
	b.mu.Lock("UpdateConnector")
	defer b.mu.Unlock()

	c, ok := b.connectors[in.ConnectorID]
	if !ok {
		return nil, fmt.Errorf("%w: connector %s not found", ErrConnectorNotFound, in.ConnectorID)
	}

	if in.URL != "" {
		c.URL = in.URL
	}

	if in.AccessRole != "" {
		c.AccessRole = in.AccessRole
	}

	if in.SftpConfig != nil {
		c.SftpConfig = in.SftpConfig
	}

	if in.As2Config != nil {
		c.As2Config = in.As2Config
	}

	if in.SetLoggingRole {
		c.LoggingRole = in.LoggingRole
	}

	if in.SetSecurityPolicyName {
		c.SecurityPolicyName = in.SecurityPolicyName
	}

	return cloneConnector(c), nil
}

// DeleteProfile removes a profile by ID.
func (b *InMemoryBackend) DeleteProfile(profileID string) error {
	b.mu.Lock("DeleteProfile")
	defer b.mu.Unlock()

	if _, ok := b.profiles[profileID]; !ok {
		return fmt.Errorf("%w: profile %s not found", ErrProfileNotFound, profileID)
	}

	delete(b.profiles, profileID)

	return nil
}

// DescribeProfile returns a profile by ID.
func (b *InMemoryBackend) DescribeProfile(profileID string) (*Profile, error) {
	b.mu.RLock("DescribeProfile")
	defer b.mu.RUnlock()

	p, ok := b.profiles[profileID]
	if !ok {
		return nil, fmt.Errorf("%w: profile %s not found", ErrProfileNotFound, profileID)
	}

	return cloneProfile(p), nil
}

// ListProfiles returns all profiles sorted by profileID.
func (b *InMemoryBackend) ListProfiles() []*Profile {
	b.mu.RLock("ListProfiles")
	defer b.mu.RUnlock()

	out := make([]*Profile, 0, len(b.profiles))

	for _, p := range b.profiles {
		out = append(out, cloneProfile(p))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ProfileID < out[j].ProfileID
	})

	return out
}

// UpdateProfile updates mutable fields on a profile.
func (b *InMemoryBackend) UpdateProfile(profileID, as2ID string) (*Profile, error) {
	b.mu.Lock("UpdateProfile")
	defer b.mu.Unlock()

	p, ok := b.profiles[profileID]
	if !ok {
		return nil, fmt.Errorf("%w: profile %s not found", ErrProfileNotFound, profileID)
	}

	if as2ID != "" {
		p.As2ID = as2ID
	}

	return cloneProfile(p), nil
}

// DeleteWebApp removes a web app by ID.
func (b *InMemoryBackend) DeleteWebApp(webAppID string) error {
	b.mu.Lock("DeleteWebApp")
	defer b.mu.Unlock()

	if _, ok := b.webApps[webAppID]; !ok {
		return fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	delete(b.webApps, webAppID)

	return nil
}

// DescribeWebApp returns a web app by ID.
func (b *InMemoryBackend) DescribeWebApp(webAppID string) (*WebApp, error) {
	b.mu.RLock("DescribeWebApp")
	defer b.mu.RUnlock()

	w, ok := b.webApps[webAppID]
	if !ok {
		return nil, fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	return cloneWebApp(w), nil
}

// ListWebApps returns all web apps sorted by webAppID.
func (b *InMemoryBackend) ListWebApps() []*WebApp {
	b.mu.RLock("ListWebApps")
	defer b.mu.RUnlock()

	out := make([]*WebApp, 0, len(b.webApps))

	for _, w := range b.webApps {
		out = append(out, cloneWebApp(w))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].WebAppID < out[j].WebAppID
	})

	return out
}

// UpdateWebApp updates mutable fields on a web app.
func (b *InMemoryBackend) UpdateWebApp(
	webAppID string,
	identityProviderDetails *WebAppIdentityProviderDetails,
) (*WebApp, error) {
	b.mu.Lock("UpdateWebApp")
	defer b.mu.Unlock()

	w, ok := b.webApps[webAppID]
	if !ok {
		return nil, fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	if identityProviderDetails != nil {
		w.IdentityProviderDetails = identityProviderDetails
	}

	return cloneWebApp(w), nil
}

// DescribeWebAppCustomization returns the customization for a web app.
// Returns empty customization (not an error) when none has been set.
func (b *InMemoryBackend) DescribeWebAppCustomization(webAppID string) (*WebAppCustomization, error) {
	b.mu.RLock("DescribeWebAppCustomization")
	defer b.mu.RUnlock()

	if _, ok := b.webApps[webAppID]; !ok {
		return nil, fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	if c, ok := b.webAppCustomizations[webAppID]; ok {
		cp := *c

		return &cp, nil
	}

	return &WebAppCustomization{WebAppID: webAppID}, nil
}

// UpdateWebAppCustomization sets or overwrites the customization for a web app.
func (b *InMemoryBackend) UpdateWebAppCustomization(
	webAppID, title, logoFile, faviconFile string,
) (*WebAppCustomization, error) {
	b.mu.Lock("UpdateWebAppCustomization")
	defer b.mu.Unlock()

	if _, ok := b.webApps[webAppID]; !ok {
		return nil, fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	c := &WebAppCustomization{
		WebAppID:    webAppID,
		Title:       title,
		LogoFile:    logoFile,
		FaviconFile: faviconFile,
	}
	b.webAppCustomizations[webAppID] = c

	cp := *c

	return &cp, nil
}

// DeleteWebAppCustomization clears the customization for a web app.
func (b *InMemoryBackend) DeleteWebAppCustomization(webAppID string) error {
	b.mu.Lock("DeleteWebAppCustomization")
	defer b.mu.Unlock()

	if _, ok := b.webApps[webAppID]; !ok {
		return fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	delete(b.webAppCustomizations, webAppID)

	return nil
}

// DeleteWorkflow removes a workflow by ID.
func (b *InMemoryBackend) DeleteWorkflow(workflowID string) error {
	b.mu.Lock("DeleteWorkflow")
	defer b.mu.Unlock()

	if _, ok := b.workflows[workflowID]; !ok {
		return fmt.Errorf("%w: workflow %s not found", ErrWorkflowNotFound, workflowID)
	}

	delete(b.workflows, workflowID)

	return nil
}

// DescribeWorkflow returns a workflow by ID.
func (b *InMemoryBackend) DescribeWorkflow(workflowID string) (*Workflow, error) {
	b.mu.RLock("DescribeWorkflow")
	defer b.mu.RUnlock()

	wf, ok := b.workflows[workflowID]
	if !ok {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrWorkflowNotFound, workflowID)
	}

	return cloneWorkflow(wf), nil
}

// ListWorkflows returns all workflows sorted by workflowID.
func (b *InMemoryBackend) ListWorkflows() []*Workflow {
	b.mu.RLock("ListWorkflows")
	defer b.mu.RUnlock()

	out := make([]*Workflow, 0, len(b.workflows))

	for _, wf := range b.workflows {
		out = append(out, cloneWorkflow(wf))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].WorkflowID < out[j].WorkflowID
	})

	return out
}

// ImportCertificate imports a certificate.
// notBefore and notAfter are optional; zero values use defaults (now and +1 year).
// If body is a valid PEM certificate, NotBefore/NotAfter are extracted from it.
func (b *InMemoryBackend) ImportCertificate(
	usage, body, description string,
	notBefore, notAfter time.Time,
	tags map[string]string,
) (*Certificate, error) {
	b.mu.Lock("ImportCertificate")
	defer b.mu.Unlock()

	// Try to parse PEM if provided.
	if body != "" {
		block, _ := pem.Decode([]byte(body))
		if block == nil {
			return nil, fmt.Errorf(
				"%w: certificate body is not a valid PEM block",
				ErrValidation,
			)
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: failed to parse certificate: %w",
				ErrValidation,
				err,
			)
		}

		// Override notBefore/notAfter from certificate.
		notBefore = cert.NotBefore
		notAfter = cert.NotAfter
	}

	certID := "cert-" + uuid.NewString()[:20]

	now := time.Now()
	if notBefore.IsZero() {
		notBefore = now
	}

	if notAfter.IsZero() {
		notAfter = now.AddDate(1, 0, 0)
	}

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	c := &Certificate{
		CertificateID: certID,
		Usage:         usage,
		Body:          body,
		Description:   description,
		Status:        agreementStatusActive,
		NotBeforeDate: notBefore,
		NotAfterDate:  notAfter,
		CreatedAt:     now,
		Tags:          merged,
		AccountID:     b.accountID,
		Region:        b.region,
	}
	b.certificates[certID] = c

	cp := *c
	cp.Tags = make(map[string]string, len(merged))
	maps.Copy(cp.Tags, merged)

	return &cp, nil
}

// StartFileFileTransferResult persists a transfer record and returns the transferID.
func (b *InMemoryBackend) StartFileFileTransferResult(connectorID string, files []string) string {
	b.mu.Lock("StartFileFileTransferResult")
	defer b.mu.Unlock()

	transferID := uuid.NewString()

	b.transferRecords[transferID] = &FileTransferResult{
		TransferID:  transferID,
		ConnectorID: connectorID,
		Status:      "QUEUED",
		Files:       files,
		CreatedAt:   time.Now(),
	}

	return transferID
}

// ListFileFileTransferResults returns all transfer records for a connector.
func (b *InMemoryBackend) ListFileFileTransferResults(connectorID string) []*FileTransferResult {
	b.mu.RLock("ListFileFileTransferResults")
	defer b.mu.RUnlock()

	var out []*FileTransferResult

	for _, r := range b.transferRecords {
		if connectorID == "" || r.ConnectorID == connectorID {
			cp := *r
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TransferID < out[j].TransferID
	})

	return out
}

// StartAsyncOperationRecord persists an async operation record and returns the operationID.
func (b *InMemoryBackend) StartAsyncOperationRecord(connectorID, opType string) string {
	b.mu.Lock("StartAsyncOperationRecord")
	defer b.mu.Unlock()

	opID := uuid.NewString()

	b.asyncOperations[opID] = &AsyncOperationRecord{
		ID:          opID,
		ConnectorID: connectorID,
		Status:      "QUEUED",
		Type:        opType,
		CreatedAt:   time.Now(),
	}

	return opID
}

// HTTP status code constants for TestIdentityProvider.
const (
	httpStatusOK           = http.StatusOK
	httpStatusBadRequest   = http.StatusBadRequest
	httpStatusUnauthorized = http.StatusUnauthorized
)

// TestIdentityProvider tests the identity provider for a server.
func (b *InMemoryBackend) TestIdentityProvider(serverID, userName string) (int, string) {
	b.mu.RLock("TestIdentityProvider")
	defer b.mu.RUnlock()

	s, found := b.servers[serverID]
	if !found {
		return httpStatusBadRequest, "server not found"
	}

	if s.IdentityProviderType != identityProviderServiceManaged && s.IdentityProviderType != "" {
		return httpStatusOK, "No validation for this provider type"
	}

	// SERVICE_MANAGED: check if user exists.
	if users, hasUsers := b.users[serverID]; hasUsers {
		if _, hasUser := users[userName]; hasUser {
			return httpStatusOK, ""
		}
	}

	return httpStatusUnauthorized, "user not found"
}

// SendWorkflowStepStateRecord advances an execution by matching a token.
// Status must be "COMPLETE" or "EXCEPTION" per AWS API contract.
func (b *InMemoryBackend) SendWorkflowStepStateRecord(workflowID, executionID, token, status string) error {
	// Validate status before acquiring the lock.
	switch status {
	case workflowStepStatusComplete, workflowStepStatusException:
		// valid
	default:
		return fmt.Errorf(
			"%w: Status must be COMPLETE or EXCEPTION, got %q",
			ErrValidation,
			status,
		)
	}

	b.mu.Lock("SendWorkflowStepState")
	defer b.mu.Unlock()

	execs, ok := b.executions[workflowID]
	if !ok {
		return fmt.Errorf("%w: workflow %s not found", ErrWorkflowNotFound, workflowID)
	}

	e, ok := execs[executionID]
	if !ok {
		return fmt.Errorf("%w: execution %s not found", ErrWorkflowNotFound, executionID)
	}

	if e.PendingTokens == nil {
		e.PendingTokens = make(map[string]string)
	}

	// Mark token as processed.
	e.PendingTokens[token] = status

	switch status {
	case workflowStepStatusComplete:
		e.Status = "COMPLETED"
	case workflowStepStatusException:
		e.Status = workflowStepStatusException
	}

	return nil
}

// DescribeCertificate returns a certificate by ID.
func (b *InMemoryBackend) DescribeCertificate(certificateID string) (*Certificate, error) {
	b.mu.RLock("DescribeCertificate")
	defer b.mu.RUnlock()

	c, ok := b.certificates[certificateID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: certificate %s not found",
			ErrCertificateNotFound,
			certificateID,
		)
	}

	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	return &cp, nil
}

// ListCertificates returns all certificates sorted by certificateID.
func (b *InMemoryBackend) ListCertificates() []*Certificate {
	b.mu.RLock("ListCertificates")
	defer b.mu.RUnlock()

	out := make([]*Certificate, 0, len(b.certificates))

	for _, c := range b.certificates {
		cp := *c
		cp.Tags = make(map[string]string, len(c.Tags))
		maps.Copy(cp.Tags, c.Tags)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CertificateID < out[j].CertificateID
	})

	return out
}

// UpdateCertificate updates mutable fields on a certificate.
func (b *InMemoryBackend) UpdateCertificate(
	certificateID, description string,
) (*Certificate, error) {
	b.mu.Lock("UpdateCertificate")
	defer b.mu.Unlock()

	c, ok := b.certificates[certificateID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: certificate %s not found",
			ErrCertificateNotFound,
			certificateID,
		)
	}

	if description != "" {
		c.Description = description
	}

	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	return &cp, nil
}

// ImportHostKey imports a host key onto a server.
func (b *InMemoryBackend) ImportHostKey(
	serverID, hostKeyBody, description string,
	tags map[string]string,
) (*HostKey, error) {
	b.mu.Lock("ImportHostKey")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	if _, ok := b.hostKeys[serverID]; !ok {
		b.hostKeys[serverID] = make(map[string]*HostKey)
	}

	hostKeyID := "hostkey-" + uuid.NewString()[:8]

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	fp, _ := computeSSHKeyFingerprintAndType(hostKeyBody)

	hk := &HostKey{
		HostKeyID:   hostKeyID,
		ServerID:    serverID,
		Description: description,
		Fingerprint: fp,
		Value:       hostKeyBody,
		Type:        detectHostKeyType(hostKeyBody),
		CreatedAt:   time.Now(),
		Tags:        merged,
		AccountID:   b.accountID,
		Region:      b.region,
	}
	b.hostKeys[serverID][hostKeyID] = hk

	return cloneHostKey(hk), nil
}

// DeleteHostKey removes a host key from a server.
func (b *InMemoryBackend) DeleteHostKey(serverID, hostKeyID string) error {
	b.mu.Lock("DeleteHostKey")
	defer b.mu.Unlock()

	serverKeys, ok := b.hostKeys[serverID]
	if !ok {
		return fmt.Errorf(
			"%w: host key %s not found on server %s",
			ErrHostKeyNotFound,
			hostKeyID,
			serverID,
		)
	}

	if _, exists := serverKeys[hostKeyID]; !exists {
		return fmt.Errorf(
			"%w: host key %s not found on server %s",
			ErrHostKeyNotFound,
			hostKeyID,
			serverID,
		)
	}

	delete(serverKeys, hostKeyID)

	return nil
}

// DescribeHostKey returns a host key from a server.
func (b *InMemoryBackend) DescribeHostKey(serverID, hostKeyID string) (*HostKey, error) {
	b.mu.RLock("DescribeHostKey")
	defer b.mu.RUnlock()

	serverKeys, ok := b.hostKeys[serverID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: host key %s not found on server %s",
			ErrHostKeyNotFound,
			hostKeyID,
			serverID,
		)
	}

	hk, ok := serverKeys[hostKeyID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: host key %s not found on server %s",
			ErrHostKeyNotFound,
			hostKeyID,
			serverID,
		)
	}

	return cloneHostKey(hk), nil
}

// ListHostKeys returns all host keys on a server sorted by hostKeyID.
func (b *InMemoryBackend) ListHostKeys(serverID string) ([]*HostKey, error) {
	b.mu.RLock("ListHostKeys")
	defer b.mu.RUnlock()

	if _, ok := b.servers[serverID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	serverKeys := b.hostKeys[serverID]
	out := make([]*HostKey, 0, len(serverKeys))

	for _, hk := range serverKeys {
		out = append(out, cloneHostKey(hk))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].HostKeyID < out[j].HostKeyID
	})

	return out, nil
}

// UpdateHostKey updates mutable fields on a host key.
func (b *InMemoryBackend) UpdateHostKey(serverID, hostKeyID, description string) (*HostKey, error) {
	b.mu.Lock("UpdateHostKey")
	defer b.mu.Unlock()

	serverKeys, ok := b.hostKeys[serverID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: host key %s not found on server %s",
			ErrHostKeyNotFound,
			hostKeyID,
			serverID,
		)
	}

	hk, ok := serverKeys[hostKeyID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: host key %s not found on server %s",
			ErrHostKeyNotFound,
			hostKeyID,
			serverID,
		)
	}

	if description != "" {
		hk.Description = description
	}

	return cloneHostKey(hk), nil
}

// ErrSSHPublicKeyDuplicate is returned when an SSH public key body already exists for the user.
var ErrSSHPublicKeyDuplicate = awserr.New("ResourceExistsException", awserr.ErrConflict)

// ImportSSHPublicKey imports an SSH public key for a user on a server.
func (b *InMemoryBackend) ImportSSHPublicKey(
	serverID, userName, sshPublicKeyBody string,
) (*SSHPublicKey, error) {
	b.mu.Lock("ImportSshPublicKey")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	if _, ok := b.sshPublicKeys[serverID]; !ok {
		b.sshPublicKeys[serverID] = make(map[string]map[string]*SSHPublicKey)
	}

	if _, ok := b.sshPublicKeys[serverID][userName]; !ok {
		b.sshPublicKeys[serverID][userName] = make(map[string]*SSHPublicKey)
	}

	// Lazily initialize the body index for this server/user.
	if _, ok := b.sshKeyBodies[serverID]; !ok {
		b.sshKeyBodies[serverID] = make(map[string]map[string]struct{})
	}

	if _, ok := b.sshKeyBodies[serverID][userName]; !ok {
		b.sshKeyBodies[serverID][userName] = make(map[string]struct{})
	}

	// AWS limits each user to 50 SSH public keys.
	const maxSSHPublicKeysPerUser = 50
	if len(b.sshPublicKeys[serverID][userName]) >= maxSSHPublicKeysPerUser {
		return nil, fmt.Errorf(
			"%w: user %s on server %s has reached the maximum of %d SSH public keys",
			ErrValidation,
			userName,
			serverID,
			maxSSHPublicKeysPerUser,
		)
	}

	// Check for duplicate key body using O(1) index.
	normalizedBody := strings.TrimSpace(sshPublicKeyBody)
	if _, dup := b.sshKeyBodies[serverID][userName][normalizedBody]; dup {
		return nil, fmt.Errorf(
			"%w: SSH public key body already exists for user %s on server %s",
			ErrSSHPublicKeyDuplicate,
			userName,
			serverID,
		)
	}

	keyID := "key-" + uuid.NewString()[:8]
	fingerprint, keyType := computeSSHKeyFingerprintAndType(sshPublicKeyBody)

	k := &SSHPublicKey{
		SSHPublicKeyID:   keyID,
		SSHPublicKeyBody: sshPublicKeyBody,
		Fingerprint:      fingerprint,
		KeyType:          keyType,
		UserName:         userName,
		ServerID:         serverID,
		DateImported:     time.Now(),
	}
	b.sshPublicKeys[serverID][userName][keyID] = k
	b.sshKeyBodies[serverID][userName][normalizedBody] = struct{}{}

	return &SSHPublicKey{
		SSHPublicKeyID:   k.SSHPublicKeyID,
		SSHPublicKeyBody: k.SSHPublicKeyBody,
		Fingerprint:      k.Fingerprint,
		KeyType:          k.KeyType,
		UserName:         k.UserName,
		ServerID:         k.ServerID,
		DateImported:     k.DateImported,
	}, nil
}

// DeleteSSHPublicKey removes an SSH public key from a user on a server.
func (b *InMemoryBackend) DeleteSSHPublicKey(serverID, userName, sshPublicKeyID string) error {
	b.mu.Lock("DeleteSshPublicKey")
	defer b.mu.Unlock()

	serverKeys, ok := b.sshPublicKeys[serverID]
	if !ok {
		return fmt.Errorf("%w: SSH key %s not found", ErrSSHPublicKeyNotFound, sshPublicKeyID)
	}

	userKeys, ok := serverKeys[userName]
	if !ok {
		return fmt.Errorf("%w: SSH key %s not found", ErrSSHPublicKeyNotFound, sshPublicKeyID)
	}

	k, exists := userKeys[sshPublicKeyID]
	if !exists {
		return fmt.Errorf("%w: SSH key %s not found", ErrSSHPublicKeyNotFound, sshPublicKeyID)
	}

	if bodyIndex := b.sshKeyBodies[serverID][userName]; bodyIndex != nil {
		delete(bodyIndex, strings.TrimSpace(k.SSHPublicKeyBody))
	}

	delete(userKeys, sshPublicKeyID)

	return nil
}

// TagResource applies tags to a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.tagsStore[resourceARN]; !ok {
		b.tagsStore[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tagsStore[resourceARN], tags)

	return nil
}

// UntagResource removes tag keys from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if existing, ok := b.tagsStore[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(existing, k)
		}
	}

	return nil
}

// ListTagsForResource returns tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.tagsStore[resourceARN]
	if !ok {
		return make(map[string]string)
	}

	out := make(map[string]string, len(existing))
	maps.Copy(out, existing)

	return out
}

// AddCertificateInternal seeds a certificate for testing purposes.
func (b *InMemoryBackend) AddCertificateInternal(certID string) {
	b.mu.Lock("AddCertificateInternal")
	defer b.mu.Unlock()

	b.certificates[certID] = &Certificate{
		CertificateID: certID,
		CreatedAt:     time.Now(),
		Tags:          make(map[string]string),
		AccountID:     b.accountID,
		Region:        b.region,
	}
}

// AddServerInternal seeds a server for testing purposes.
func (b *InMemoryBackend) AddServerInternal(serverID string) {
	b.mu.Lock("AddServerInternal")
	defer b.mu.Unlock()

	b.servers[serverID] = &Server{
		ServerID:  serverID,
		State:     serverStatusOnline,
		Protocols: []string{protocolSFTP},
		Domain:    "S3",
		Endpoint:  fmt.Sprintf("%s.server.transfer.%s.amazonaws.com", serverID, b.region),
		CreatedAt: time.Now(),
		Tags:      make(map[string]string),
		AccountID: b.accountID,
		Region:    b.region,
	}
	b.users[serverID] = make(map[string]*User)
}

// AddConnectorInternal seeds a connector for testing purposes.
func (b *InMemoryBackend) AddConnectorInternal(connectorID, url string) {
	b.mu.Lock("AddConnectorInternal")
	defer b.mu.Unlock()

	b.connectors[connectorID] = &Connector{
		ConnectorID: connectorID,
		URL:         url,
		CreatedAt:   time.Now(),
		Tags:        make(map[string]string),
		AccountID:   b.accountID,
		Region:      b.region,
	}
}

// AddProfileInternal seeds a profile for testing purposes.
func (b *InMemoryBackend) AddProfileInternal(profileID, profileType string) {
	b.mu.Lock("AddProfileInternal")
	defer b.mu.Unlock()

	b.profiles[profileID] = &Profile{
		ProfileID:   profileID,
		ProfileType: profileType,
		CreatedAt:   time.Now(),
		Tags:        make(map[string]string),
		AccountID:   b.accountID,
		Region:      b.region,
	}
}

// AddWebAppInternal seeds a web app for testing purposes.
func (b *InMemoryBackend) AddWebAppInternal(webAppID string) {
	b.mu.Lock("AddWebAppInternal")
	defer b.mu.Unlock()

	b.webApps[webAppID] = &WebApp{
		WebAppID:  webAppID,
		CreatedAt: time.Now(),
		Tags:      make(map[string]string),
		AccountID: b.accountID,
		Region:    b.region,
	}
}

// AddWorkflowInternal seeds a workflow for testing purposes.
func (b *InMemoryBackend) AddWorkflowInternal(workflowID string) {
	b.mu.Lock("AddWorkflowInternal")
	defer b.mu.Unlock()

	b.workflows[workflowID] = &Workflow{
		WorkflowID: workflowID,
		CreatedAt:  time.Now(),
		Tags:       make(map[string]string),
		AccountID:  b.accountID,
		Region:     b.region,
	}
}

// CreateExecution creates a new execution for a workflow.
func (b *InMemoryBackend) CreateExecution(workflowID string) (*Execution, error) {
	b.mu.Lock("CreateExecution")
	defer b.mu.Unlock()

	if _, ok := b.workflows[workflowID]; !ok {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrWorkflowNotFound, workflowID)
	}

	if _, ok := b.executions[workflowID]; !ok {
		b.executions[workflowID] = make(map[string]*Execution)
	}

	executionID := "exec-" + uuid.NewString()[:8]

	e := &Execution{
		ExecutionID: executionID,
		WorkflowID:  workflowID,
		Status:      "IN_PROGRESS",
		CreatedAt:   time.Now(),
	}
	b.executions[workflowID][executionID] = e

	cp := *e

	return &cp, nil
}

// DescribeExecution returns an execution for a workflow.
func (b *InMemoryBackend) DescribeExecution(workflowID, executionID string) (*Execution, error) {
	b.mu.RLock("DescribeExecution")
	defer b.mu.RUnlock()

	workflowExecs, ok := b.executions[workflowID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: execution %s not found for workflow %s",
			ErrWorkflowNotFound,
			executionID,
			workflowID,
		)
	}

	e, ok := workflowExecs[executionID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: execution %s not found for workflow %s",
			ErrWorkflowNotFound,
			executionID,
			workflowID,
		)
	}

	cp := *e

	return &cp, nil
}

// ListExecutions returns all executions for a workflow sorted by executionID.
func (b *InMemoryBackend) ListExecutions(workflowID string) ([]*Execution, error) {
	b.mu.RLock("ListExecutions")
	defer b.mu.RUnlock()

	if _, ok := b.workflows[workflowID]; !ok {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrWorkflowNotFound, workflowID)
	}

	workflowExecs := b.executions[workflowID]
	out := make([]*Execution, 0, len(workflowExecs))

	for _, e := range workflowExecs {
		cp := *e
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ExecutionID < out[j].ExecutionID
	})

	return out, nil
}

// computeSSHKeyFingerprintAndType computes the SHA256 fingerprint and detects the key type.
// Uses golang.org/x/crypto/ssh when possible for accurate fingerprint; falls back to manual SHA256.
func computeSSHKeyFingerprintAndType(keyBody string) (string, string) {
	// Try golang.org/x/crypto/ssh first for accurate fingerprint.
	pk, _, _, _, err := ssh.ParseAuthorizedKey([]byte(keyBody))
	if err == nil {
		return ssh.FingerprintSHA256(pk), pk.Type()
	}

	// Fallback: manual computation.
	parts := strings.Fields(keyBody)
	const minSSHKeyParts = 2
	if len(parts) < minSSHKeyParts {
		return "", ""
	}

	decoded, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", ""
	}

	sum := sha256.Sum256(decoded)
	fp := "SHA256:" + base64.StdEncoding.EncodeToString(sum[:])

	// Detect type from prefix.
	switch parts[0] {
	case defaultHostKeyType, sshKeyTypeECDSAP256, sshKeyTypeECDSAP384, sshKeyTypeECDSAP521, sshKeyTypeEd25519:
		return fp, parts[0]
	default:
		return fp, ""
	}
}

// detectHostKeyType returns the host key type string from the key body prefix.
func detectHostKeyType(hostKeyBody string) string {
	prefix := strings.Fields(hostKeyBody)
	if len(prefix) == 0 {
		return defaultHostKeyType
	}

	switch prefix[0] {
	case defaultHostKeyType:
		return defaultHostKeyType
	case sshKeyTypeECDSAP256, sshKeyTypeECDSAP384, sshKeyTypeECDSAP521:
		return prefix[0]
	case sshKeyTypeEd25519:
		return sshKeyTypeEd25519
	default:
		return defaultHostKeyType
	}
}

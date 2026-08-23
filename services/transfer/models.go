package transfer

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	protocolSFTP = "SFTP"
	protocolFTPS = "FTPS"
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

// SendWorkflowStepState's Status is types.CustomStepStatus
// (transfer@v1.75.4 api_op_SendWorkflowStepState.go); its only real values
// are SUCCESS/FAILURE. This backend previously accepted the non-existent
// "COMPLETE"/"EXCEPTION" instead, so no real client (which can only send
// SUCCESS or FAILURE) could ever pass validation here.
const (
	workflowStepStatusSuccess = "SUCCESS"
	workflowStepStatusFailure = "FAILURE"
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
	IPAddressType      string               `json:"ip_address_type,omitempty"`
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
	CreatedAt      time.Time         `json:"created_at"`
	Tags           map[string]string `json:"tags"`
	ProfileID      string            `json:"profile_id"`
	ProfileType    string            `json:"profile_type"`
	As2ID          string            `json:"as2_id"`
	AccountID      string            `json:"account_id"`
	Region         string            `json:"region"`
	CertificateIDs []string          `json:"certificate_ids,omitempty"`
}

// cloneProfile returns a deep copy of a Profile.
func cloneProfile(p *Profile) *Profile {
	cp := *p
	cp.Tags = make(map[string]string, len(p.Tags))
	maps.Copy(cp.Tags, p.Tags)

	if p.CertificateIDs != nil {
		cp.CertificateIDs = append([]string(nil), p.CertificateIDs...)
	}

	return &cp
}

// WebAppIdentityCenterConfig holds IAM Identity Center configuration for a web app's
// identity provider. Real AWS web apps support ONLY IdentityCenterConfig as an
// identity provider (unlike Transfer servers, which additionally support
// SERVICE_MANAGED / AWS_DIRECTORY_SERVICE / AWS_LAMBDA / API_GATEWAY).
type WebAppIdentityCenterConfig struct {
	// ApplicationArn is assigned automatically by AWS when the web app is created;
	// it is not settable by the caller.
	ApplicationArn string `json:"application_arn,omitempty"`
	InstanceArn    string `json:"instance_arn,omitempty"`
	Role           string `json:"role,omitempty"`
}

// WebAppVpcConfig holds the VPC configuration for a web app endpoint hosted within a VPC.
type WebAppVpcConfig struct {
	VpcID            string   `json:"vpc_id,omitempty"`
	VpcEndpointID    string   `json:"vpc_endpoint_id,omitempty"`
	IPAddressType    string   `json:"ip_address_type,omitempty"`
	SecurityGroupIDs []string `json:"security_group_ids,omitempty"`
	SubnetIDs        []string `json:"subnet_ids,omitempty"`
}

// WebApp represents an AWS Transfer web application.
type WebApp struct {
	CreatedAt            time.Time                   `json:"created_at"`
	IdentityCenterConfig *WebAppIdentityCenterConfig `json:"identity_center_config,omitempty"`
	VpcConfig            *WebAppVpcConfig            `json:"vpc_config,omitempty"`
	Tags                 map[string]string           `json:"tags"`
	WebAppID             string                      `json:"web_app_id"`
	AccessEndpoint       string                      `json:"access_endpoint,omitempty"`
	WebAppEndpoint       string                      `json:"web_app_endpoint,omitempty"`
	WebAppEndpointPolicy string                      `json:"web_app_endpoint_policy,omitempty"`
	AccountID            string                      `json:"account_id"`
	Region               string                      `json:"region"`
	WebAppUnits          int32                       `json:"web_app_units,omitempty"`
}

// cloneWebApp returns a deep copy of a WebApp.
func cloneWebApp(w *WebApp) *WebApp {
	cp := *w
	cp.Tags = make(map[string]string, len(w.Tags))
	maps.Copy(cp.Tags, w.Tags)

	if w.IdentityCenterConfig != nil {
		icc := *w.IdentityCenterConfig
		cp.IdentityCenterConfig = &icc
	}

	if w.VpcConfig != nil {
		vc := *w.VpcConfig
		vc.SecurityGroupIDs = append([]string(nil), w.VpcConfig.SecurityGroupIDs...)
		vc.SubnetIDs = append([]string(nil), w.VpcConfig.SubnetIDs...)
		cp.VpcConfig = &vc
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
	NotBeforeDate    time.Time         `json:"not_before_date,omitzero"`
	NotAfterDate     time.Time         `json:"not_after_date,omitzero"`
	ActiveDate       time.Time         `json:"active_date,omitzero"`
	InactiveDate     time.Time         `json:"inactive_date,omitzero"`
	CreatedAt        time.Time         `json:"created_at"`
	Tags             map[string]string `json:"tags"`
	CertificateID    string            `json:"certificate_id"`
	Description      string            `json:"description"`
	Usage            string            `json:"usage"`
	Body             string            `json:"body"`
	CertificateChain string            `json:"certificate_chain,omitempty"`
	Serial           string            `json:"serial,omitempty"`
	Status           string            `json:"status"`
	AccountID        string            `json:"account_id"`
	Region           string            `json:"region"`
	HasPrivateKey    bool              `json:"has_private_key,omitempty"`
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

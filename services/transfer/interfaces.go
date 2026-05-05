package transfer

// StorageBackend defines the interface for Transfer backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	AccountID() string
	Region() string
	Reset()
	CreateServer(protocols []string, tags map[string]string) (*Server, error)
	DescribeServer(serverID string) (*Server, error)
	ListServers() []Server
	StartServer(serverID string) error
	StopServer(serverID string) error
	DeleteServer(serverID string) error
	UpdateServer(serverID string, protocols []string) (*Server, error)
	CreateUser(serverID, userName, homeDir, role string, tags map[string]string) (*User, error)
	DescribeUser(serverID, userName string) (*User, error)
	ListUsers(serverID string) ([]User, error)
	DeleteUser(serverID, userName string) error
	UpdateUser(serverID, userName, homeDir, role string) (*User, error)
	CreateAccess(serverID, externalID, role, homeDir string, tags map[string]string) (*Access, error)
	DeleteAccess(serverID, externalID string) error
	DescribeAccess(serverID, externalID string) (*Access, error)
	ListAccesses(serverID string) ([]*Access, error)
	UpdateAccess(serverID, externalID, role, homeDir string) (*Access, error)
	CreateAgreement(
		serverID, description, localProfileID, partnerProfileID, baseDirectory, accessRole string,
		tags map[string]string,
	) (*Agreement, error)
	DeleteAgreement(serverID, agreementID string) error
	DescribeAgreement(serverID, agreementID string) (*Agreement, error)
	ListAgreements(serverID string) ([]*Agreement, error)
	UpdateAgreement(serverID, agreementID, description, status string) (*Agreement, error)
	CreateConnector(url, accessRole string, tags map[string]string) (*Connector, error)
	DeleteConnector(connectorID string) error
	DescribeConnector(connectorID string) (*Connector, error)
	ListConnectors() []*Connector
	UpdateConnector(connectorID, url, accessRole string) (*Connector, error)
	CreateProfile(profileType, as2ID string, tags map[string]string) (*Profile, error)
	DeleteProfile(profileID string) error
	DescribeProfile(profileID string) (*Profile, error)
	ListProfiles() []*Profile
	UpdateProfile(profileID, as2ID string) (*Profile, error)
	CreateWebApp(tags map[string]string) (*WebApp, error)
	DeleteWebApp(webAppID string) error
	DescribeWebApp(webAppID string) (*WebApp, error)
	ListWebApps() []*WebApp
	UpdateWebApp(webAppID string) (*WebApp, error)
	CreateWorkflow(description string, tags map[string]string) (*Workflow, error)
	DeleteWorkflow(workflowID string) error
	DescribeWorkflow(workflowID string) (*Workflow, error)
	ListWorkflows() []*Workflow
	DeleteCertificate(certificateID string) error
	ImportCertificate(usage, body, description string, tags map[string]string) (*Certificate, error)
	DescribeCertificate(certificateID string) (*Certificate, error)
	ListCertificates() []*Certificate
	UpdateCertificate(certificateID, description string) (*Certificate, error)
	ImportHostKey(serverID, hostKeyBody, description string, tags map[string]string) (*HostKey, error)
	DeleteHostKey(serverID, hostKeyID string) error
	DescribeHostKey(serverID, hostKeyID string) (*HostKey, error)
	ListHostKeys(serverID string) ([]*HostKey, error)
	UpdateHostKey(serverID, hostKeyID, description string) (*HostKey, error)
	ImportSshPublicKey(serverID, userName, sshPublicKeyBody string) (*SshPublicKey, error)
	DeleteSshPublicKey(serverID, userName, sshPublicKeyID string) error
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) map[string]string
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

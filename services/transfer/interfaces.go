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
	CreateAgreement(
		serverID, description, localProfileID, partnerProfileID, baseDirectory, accessRole string,
		tags map[string]string,
	) (*Agreement, error)
	DeleteAgreement(serverID, agreementID string) error
	CreateConnector(url, accessRole string, tags map[string]string) (*Connector, error)
	DeleteConnector(connectorID string) error
	CreateProfile(profileType, as2ID string, tags map[string]string) (*Profile, error)
	CreateWebApp(tags map[string]string) (*WebApp, error)
	CreateWorkflow(description string, tags map[string]string) (*Workflow, error)
	DeleteCertificate(certificateID string) error
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

package transfer

// StorageBackend defines the interface for Transfer backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
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
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

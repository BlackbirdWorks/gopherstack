package transfer

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrServerNotFound is returned when a Transfer server is not found.
	ErrServerNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrUserNotFound is returned when a Transfer user is not found.
	ErrUserNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrUserAlreadyExists is returned when a Transfer user already exists.
	ErrUserAlreadyExists = awserr.New("ResourceExistsException", awserr.ErrConflict)
)

// Server represents an AWS Transfer Family server.
type Server struct {
	CreatedAt time.Time
	Tags      map[string]string
	ServerID  string
	State     string
	Endpoint  string
	Domain    string
	Region    string
	AccountID string
	Protocols []string
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

	return &cp
}

// User represents a user on an AWS Transfer Family server.
type User struct {
	CreatedAt time.Time
	Tags      map[string]string
	UserName  string
	ServerID  string
	HomeDir   string
	Role      string
	AccountID string
	Region    string
}

// userARN builds the ARN for a Transfer user.
func userARN(accountID, region, serverID, userName string) string {
	return arn.Build("transfer", region, accountID, "server/"+serverID+"/user/"+userName)
}

// cloneUser returns a deep copy of a User.
func cloneUser(u *User) *User {
	cp := *u
	cp.Tags = make(map[string]string, len(u.Tags))
	maps.Copy(cp.Tags, u.Tags)

	return &cp
}

// InMemoryBackend is the in-memory store for Transfer resources.
type InMemoryBackend struct {
	servers   map[string]*Server
	users     map[string]map[string]*User // serverID -> userName -> User
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		servers:   make(map[string]*Server),
		users:     make(map[string]map[string]*User),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("transfer"),
	}
}

// CreateServer creates a new Transfer Family server.
func (b *InMemoryBackend) CreateServer(protocols []string, tags map[string]string) (*Server, error) {
	b.mu.Lock("CreateServer")
	defer b.mu.Unlock()

	serverID := "s-" + uuid.NewString()[:20]

	if len(protocols) == 0 {
		protocols = []string{"SFTP"}
	}

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	s := &Server{
		ServerID:  serverID,
		State:     "ONLINE",
		Protocols: protocols,
		Domain:    "S3",
		CreatedAt: time.Now(),
		Tags:      merged,
		AccountID: b.accountID,
		Region:    b.region,
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

// ListServers returns all servers sorted by creation time (newest first).
func (b *InMemoryBackend) ListServers() []Server {
	b.mu.RLock("ListServers")
	defer b.mu.RUnlock()

	out := make([]Server, 0, len(b.servers))
	for _, s := range b.servers {
		out = append(out, *cloneServer(s))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	return out
}

// DeleteServer removes a server and its users by ID.
func (b *InMemoryBackend) DeleteServer(serverID string) error {
	b.mu.Lock("DeleteServer")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	delete(b.servers, serverID)
	delete(b.users, serverID)

	return nil
}

// StartServer transitions a server to ONLINE state.
func (b *InMemoryBackend) StartServer(serverID string) error {
	b.mu.Lock("StartServer")
	defer b.mu.Unlock()

	s, ok := b.servers[serverID]
	if !ok {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	s.State = "ONLINE"

	return nil
}

// StopServer transitions a server to OFFLINE state.
func (b *InMemoryBackend) StopServer(serverID string) error {
	b.mu.Lock("StopServer")
	defer b.mu.Unlock()

	s, ok := b.servers[serverID]
	if !ok {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	s.State = "OFFLINE"

	return nil
}

// UpdateServer updates mutable fields on an existing server.
func (b *InMemoryBackend) UpdateServer(serverID string, protocols []string) (*Server, error) {
	b.mu.Lock("UpdateServer")
	defer b.mu.Unlock()

	s, ok := b.servers[serverID]
	if !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	if len(protocols) > 0 {
		s.Protocols = protocols
	}

	return cloneServer(s), nil
}

// CreateUser creates a user on the given server.
func (b *InMemoryBackend) CreateUser(serverID, userName, homeDir, role string, tags map[string]string) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	if _, ok := b.users[serverID][userName]; ok {
		return nil, fmt.Errorf("%w: user %s already exists on server %s", ErrUserAlreadyExists, userName, serverID)
	}

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	u := &User{
		UserName:  userName,
		ServerID:  serverID,
		HomeDir:   homeDir,
		Role:      role,
		CreatedAt: time.Now(),
		Tags:      merged,
		AccountID: b.accountID,
		Region:    b.region,
	}
	b.users[serverID][userName] = u

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
		return nil, fmt.Errorf("%w: user %s not found on server %s", ErrUserNotFound, userName, serverID)
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

// DeleteUser removes a user from the given server.
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

	return nil
}

// UpdateUser updates mutable fields on a user.
func (b *InMemoryBackend) UpdateUser(serverID, userName, homeDir, role string) (*User, error) {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	users, ok := b.users[serverID]
	if !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	u, ok := users[userName]
	if !ok {
		return nil, fmt.Errorf("%w: user %s not found on server %s", ErrUserNotFound, userName, serverID)
	}

	if homeDir != "" {
		u.HomeDir = homeDir
	}

	if role != "" {
		u.Role = role
	}

	return cloneUser(u), nil
}

// serverARNForServer builds the ARN for the given server.
func (b *InMemoryBackend) serverARNForServer(s *Server) string {
	return serverARN(s.AccountID, s.Region, s.ServerID)
}

// userARNForUser builds the ARN for the given user.
func (b *InMemoryBackend) userARNForUser(u *User) string {
	return userARN(u.AccountID, u.Region, u.ServerID, u.UserName)
}

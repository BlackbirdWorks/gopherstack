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

const (
	protocolSFTP = "SFTP"
)

var (
	// ErrServerNotFound is returned when a Transfer server is not found.
	ErrServerNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrUserNotFound is returned when a Transfer user is not found.
	ErrUserNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrUserAlreadyExists is returned when a Transfer user already exists.
	ErrUserAlreadyExists = awserr.New("ResourceExistsException", awserr.ErrConflict)
	// ErrInvalidProtocol is returned when an unsupported protocol is specified.
	ErrInvalidProtocol = awserr.New("InvalidRequestException: unsupported protocol", awserr.ErrInvalidParameter)
	// ErrServerStateConflict is returned when a state transition is invalid.
	ErrServerStateConflict = awserr.New(
		"ConflictException: server is already in the requested state",
		awserr.ErrConflict,
	)
	// ErrAccessNotFound is returned when a Transfer access is not found.
	ErrAccessNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
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
	// ErrValidation is returned when a required parameter is missing or invalid.
	ErrValidation = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
)

// Server state constants.
const (
	serverStatusOnline  = "ONLINE"
	serverStatusOffline = "OFFLINE"
)

// Profile type constants.
const (
	profileTypeLocal   = "LOCAL"
	profileTypePartner = "PARTNER"
)

// Agreement status constants.
const (
	agreementStatusActive = "ACTIVE"
)

// Server represents an AWS Transfer Family server.
type Server struct {
	CreatedAt time.Time         `json:"created_at"`
	Tags      map[string]string `json:"tags"`
	ServerID  string            `json:"server_id"`
	State     string            `json:"state"`
	Endpoint  string            `json:"endpoint"`
	Domain    string            `json:"domain"`
	Region    string            `json:"region"`
	AccountID string            `json:"account_id"`
	Protocols []string          `json:"protocols"`
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
	CreatedAt time.Time         `json:"created_at"`
	Tags      map[string]string `json:"tags"`
	UserName  string            `json:"user_name"`
	ServerID  string            `json:"server_id"`
	HomeDir   string            `json:"home_dir"`
	Role      string            `json:"role"`
	AccountID string            `json:"account_id"`
	Region    string            `json:"region"`
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

// Access represents an AWS Transfer access policy entry for a server.
type Access struct {
	CreatedAt  time.Time         `json:"created_at"`
	Tags       map[string]string `json:"tags"`
	ExternalID string            `json:"external_id"`
	ServerID   string            `json:"server_id"`
	Role       string            `json:"role"`
	HomeDir    string            `json:"home_dir"`
	AccountID  string            `json:"account_id"`
	Region     string            `json:"region"`
}

// cloneAccess returns a deep copy of an Access.
func cloneAccess(a *Access) *Access {
	cp := *a
	cp.Tags = make(map[string]string, len(a.Tags))
	maps.Copy(cp.Tags, a.Tags)

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

// Connector represents an AWS Transfer connector used to initiate file transfers.
type Connector struct {
	CreatedAt   time.Time         `json:"created_at"`
	Tags        map[string]string `json:"tags"`
	ConnectorID string            `json:"connector_id"`
	URL         string            `json:"url"`
	AccessRole  string            `json:"access_role"`
	AccountID   string            `json:"account_id"`
	Region      string            `json:"region"`
}

// cloneConnector returns a deep copy of a Connector.
func cloneConnector(c *Connector) *Connector {
	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

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

// WebApp represents an AWS Transfer web application.
type WebApp struct {
	CreatedAt time.Time         `json:"created_at"`
	Tags      map[string]string `json:"tags"`
	WebAppID  string            `json:"web_app_id"`
	AccountID string            `json:"account_id"`
	Region    string            `json:"region"`
}

// cloneWebApp returns a deep copy of a WebApp.
func cloneWebApp(w *WebApp) *WebApp {
	cp := *w
	cp.Tags = make(map[string]string, len(w.Tags))
	maps.Copy(cp.Tags, w.Tags)

	return &cp
}

// Workflow represents an AWS Transfer workflow for file processing.
type Workflow struct {
	CreatedAt   time.Time         `json:"created_at"`
	Tags        map[string]string `json:"tags"`
	WorkflowID  string            `json:"workflow_id"`
	Description string            `json:"description"`
	AccountID   string            `json:"account_id"`
	Region      string            `json:"region"`
}

// cloneWorkflow returns a deep copy of a Workflow.
func cloneWorkflow(w *Workflow) *Workflow {
	cp := *w
	cp.Tags = make(map[string]string, len(w.Tags))
	maps.Copy(cp.Tags, w.Tags)

	return &cp
}

// Certificate represents an imported AWS Transfer certificate.
type Certificate struct {
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

// SshPublicKey represents an SSH public key attached to a Transfer user.
type SshPublicKey struct {
	DateImported     time.Time `json:"date_imported"`
	SshPublicKeyID   string    `json:"ssh_public_key_id"`
	SshPublicKeyBody string    `json:"ssh_public_key_body"`
	UserName         string    `json:"user_name"`
	ServerID         string    `json:"server_id"`
}

// InMemoryBackend is the in-memory store for Transfer resources.
type InMemoryBackend struct {
	servers       map[string]*Server
	users         map[string]map[string]*User                          // serverID -> userName -> User
	accesses      map[string]map[string]*Access                        // serverID -> externalID -> Access
	agreements    map[string]map[string]*Agreement                     // serverID -> agreementID -> Agreement
	connectors    map[string]*Connector
	profiles      map[string]*Profile
	webApps       map[string]*WebApp
	workflows     map[string]*Workflow
	certificates  map[string]*Certificate
	hostKeys      map[string]map[string]*HostKey                       // serverID -> hostKeyID -> HostKey
	sshPublicKeys map[string]map[string]map[string]*SshPublicKey       // serverID -> userName -> keyID -> SshPublicKey
	tagsStore     map[string]map[string]string                         // arn -> tags
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		servers:       make(map[string]*Server),
		users:         make(map[string]map[string]*User),
		accesses:      make(map[string]map[string]*Access),
		agreements:    make(map[string]map[string]*Agreement),
		connectors:    make(map[string]*Connector),
		profiles:      make(map[string]*Profile),
		webApps:       make(map[string]*WebApp),
		workflows:     make(map[string]*Workflow),
		certificates:  make(map[string]*Certificate),
		hostKeys:      make(map[string]map[string]*HostKey),
		sshPublicKeys: make(map[string]map[string]map[string]*SshPublicKey),
		tagsStore:     make(map[string]map[string]string),
		accountID:     accountID,
		region:        region,
		mu:            lockmetrics.New("transfer"),
	}
}

// CreateServer creates a new Transfer Family server.
func (b *InMemoryBackend) CreateServer(protocols []string, tags map[string]string) (*Server, error) {
	b.mu.Lock("CreateServer")
	defer b.mu.Unlock()

	serverID := "s-" + uuid.NewString()[:20]

	if len(protocols) == 0 {
		protocols = []string{protocolSFTP}
	}

	for _, p := range protocols {
		switch p {
		case protocolSFTP, "FTP", "FTPS", "AS2":
			// valid
		default:
			return nil, fmt.Errorf("%w: %s", ErrInvalidProtocol, p)
		}
	}

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	s := &Server{
		ServerID:  serverID,
		State:     serverStatusOnline,
		Endpoint:  fmt.Sprintf("%s.server.transfer.%s.amazonaws.com", serverID, b.region),
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

// DeleteServer removes a server and all of its associated resources (users, accesses, agreements).
func (b *InMemoryBackend) DeleteServer(serverID string) error {
	b.mu.Lock("DeleteServer")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	delete(b.servers, serverID)
	delete(b.users, serverID)
	delete(b.accesses, serverID)
	delete(b.agreements, serverID)

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

	s.State = serverStatusOnline

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

	s.State = serverStatusOffline

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

// AccountID returns the AWS account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

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
	b.workflows = make(map[string]*Workflow)
	b.certificates = make(map[string]*Certificate)
	b.hostKeys = make(map[string]map[string]*HostKey)
	b.sshPublicKeys = make(map[string]map[string]map[string]*SshPublicKey)
	b.tagsStore = make(map[string]map[string]string)
}

// CreateAccess creates an access policy entry on an existing server.
func (b *InMemoryBackend) CreateAccess(
	serverID, externalID, role, homeDir string,
	tags map[string]string,
) (*Access, error) {
	b.mu.Lock("CreateAccess")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	if _, ok := b.accesses[serverID]; !ok {
		b.accesses[serverID] = make(map[string]*Access)
	}

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	a := &Access{
		ExternalID: externalID,
		ServerID:   serverID,
		Role:       role,
		HomeDir:    homeDir,
		CreatedAt:  time.Now(),
		Tags:       merged,
		AccountID:  b.accountID,
		Region:     b.region,
	}
	b.accesses[serverID][externalID] = a

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
		return fmt.Errorf("%w: access %s not found on server %s", ErrAccessNotFound, externalID, serverID)
	}

	if _, exists := serverAccesses[externalID]; !exists {
		return fmt.Errorf("%w: access %s not found on server %s", ErrAccessNotFound, externalID, serverID)
	}

	delete(serverAccesses, externalID)

	return nil
}

// CreateAgreement creates an AS2 agreement on an existing server.
func (b *InMemoryBackend) CreateAgreement(
	serverID, description, localProfileID, partnerProfileID, baseDirectory, accessRole string,
	tags map[string]string,
) (*Agreement, error) {
	b.mu.Lock("CreateAgreement")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
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
		Status:           agreementStatusActive,
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
		return fmt.Errorf("%w: agreement %s not found on server %s", ErrAgreementNotFound, agreementID, serverID)
	}

	if _, exists := serverAgreements[agreementID]; !exists {
		return fmt.Errorf("%w: agreement %s not found on server %s", ErrAgreementNotFound, agreementID, serverID)
	}

	delete(serverAgreements, agreementID)

	return nil
}

// CreateConnector creates a Transfer connector. URL is required.
func (b *InMemoryBackend) CreateConnector(url, accessRole string, tags map[string]string) (*Connector, error) {
	if url == "" {
		return nil, fmt.Errorf("%w: Url is required", ErrValidation)
	}

	b.mu.Lock("CreateConnector")
	defer b.mu.Unlock()

	connectorID := "c-" + uuid.NewString()[:20]

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	c := &Connector{
		ConnectorID: connectorID,
		URL:         url,
		AccessRole:  accessRole,
		CreatedAt:   time.Now(),
		Tags:        merged,
		AccountID:   b.accountID,
		Region:      b.region,
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
func (b *InMemoryBackend) CreateProfile(profileType, as2ID string, tags map[string]string) (*Profile, error) {
	switch profileType {
	case profileTypeLocal, profileTypePartner:
		// valid
	default:
		return nil, fmt.Errorf("%w: ProfileType must be LOCAL or PARTNER, got %q", ErrValidation, profileType)
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
func (b *InMemoryBackend) CreateWorkflow(description string, tags map[string]string) (*Workflow, error) {
	b.mu.Lock("CreateWorkflow")
	defer b.mu.Unlock()

	workflowID := "w-" + uuid.NewString()[:20]

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	wf := &Workflow{
		WorkflowID:  workflowID,
		Description: description,
		CreatedAt:   time.Now(),
		Tags:        merged,
		AccountID:   b.accountID,
		Region:      b.region,
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
		return nil, fmt.Errorf("%w: access %s not found on server %s", ErrAccessNotFound, externalID, serverID)
	}

	a, ok := serverAccesses[externalID]
	if !ok {
		return nil, fmt.Errorf("%w: access %s not found on server %s", ErrAccessNotFound, externalID, serverID)
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
func (b *InMemoryBackend) UpdateAccess(serverID, externalID, role, homeDir string) (*Access, error) {
	b.mu.Lock("UpdateAccess")
	defer b.mu.Unlock()

	serverAccesses, ok := b.accesses[serverID]
	if !ok {
		return nil, fmt.Errorf("%w: access %s not found on server %s", ErrAccessNotFound, externalID, serverID)
	}

	a, ok := serverAccesses[externalID]
	if !ok {
		return nil, fmt.Errorf("%w: access %s not found on server %s", ErrAccessNotFound, externalID, serverID)
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
		return nil, fmt.Errorf("%w: agreement %s not found on server %s", ErrAgreementNotFound, agreementID, serverID)
	}

	ag, ok := serverAgreements[agreementID]
	if !ok {
		return nil, fmt.Errorf("%w: agreement %s not found on server %s", ErrAgreementNotFound, agreementID, serverID)
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
func (b *InMemoryBackend) UpdateAgreement(serverID, agreementID, description, status string) (*Agreement, error) {
	b.mu.Lock("UpdateAgreement")
	defer b.mu.Unlock()

	serverAgreements, ok := b.agreements[serverID]
	if !ok {
		return nil, fmt.Errorf("%w: agreement %s not found on server %s", ErrAgreementNotFound, agreementID, serverID)
	}

	ag, ok := serverAgreements[agreementID]
	if !ok {
		return nil, fmt.Errorf("%w: agreement %s not found on server %s", ErrAgreementNotFound, agreementID, serverID)
	}

	if description != "" {
		ag.Description = description
	}

	if status != "" {
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

// UpdateConnector updates mutable fields on a connector.
func (b *InMemoryBackend) UpdateConnector(connectorID, url, accessRole string) (*Connector, error) {
	b.mu.Lock("UpdateConnector")
	defer b.mu.Unlock()

	c, ok := b.connectors[connectorID]
	if !ok {
		return nil, fmt.Errorf("%w: connector %s not found", ErrConnectorNotFound, connectorID)
	}

	if url != "" {
		c.URL = url
	}

	if accessRole != "" {
		c.AccessRole = accessRole
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

// UpdateWebApp updates mutable fields on a web app (currently a no-op placeholder).
func (b *InMemoryBackend) UpdateWebApp(webAppID string) (*WebApp, error) {
	b.mu.Lock("UpdateWebApp")
	defer b.mu.Unlock()

	w, ok := b.webApps[webAppID]
	if !ok {
		return nil, fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	return cloneWebApp(w), nil
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
func (b *InMemoryBackend) ImportCertificate(usage, body, description string, tags map[string]string) (*Certificate, error) {
	b.mu.Lock("ImportCertificate")
	defer b.mu.Unlock()

	certID := "cert-" + uuid.NewString()[:20]

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	c := &Certificate{
		CertificateID: certID,
		Usage:         usage,
		Body:          body,
		Description:   description,
		Status:        "ACTIVE",
		CreatedAt:     time.Now(),
		Tags:          merged,
		AccountID:     b.accountID,
		Region:        b.region,
	}
	b.certificates[certID] = c

	return &Certificate{
		CertificateID: c.CertificateID,
		Usage:         c.Usage,
		Body:          c.Body,
		Description:   c.Description,
		Status:        c.Status,
		CreatedAt:     c.CreatedAt,
		Tags:          merged,
		AccountID:     c.AccountID,
		Region:        c.Region,
	}, nil
}

// DescribeCertificate returns a certificate by ID.
func (b *InMemoryBackend) DescribeCertificate(certificateID string) (*Certificate, error) {
	b.mu.RLock("DescribeCertificate")
	defer b.mu.RUnlock()

	c, ok := b.certificates[certificateID]
	if !ok {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertificateNotFound, certificateID)
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
func (b *InMemoryBackend) UpdateCertificate(certificateID, description string) (*Certificate, error) {
	b.mu.Lock("UpdateCertificate")
	defer b.mu.Unlock()

	c, ok := b.certificates[certificateID]
	if !ok {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertificateNotFound, certificateID)
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
func (b *InMemoryBackend) ImportHostKey(serverID, hostKeyBody, description string, tags map[string]string) (*HostKey, error) {
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

	hk := &HostKey{
		HostKeyID:   hostKeyID,
		ServerID:    serverID,
		Description: description,
		Value:       hostKeyBody,
		Type:        "ssh-rsa",
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
		return fmt.Errorf("%w: host key %s not found on server %s", ErrServerNotFound, hostKeyID, serverID)
	}

	if _, exists := serverKeys[hostKeyID]; !exists {
		return fmt.Errorf("%w: host key %s not found on server %s", ErrServerNotFound, hostKeyID, serverID)
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
		return nil, fmt.Errorf("%w: host key %s not found on server %s", ErrServerNotFound, hostKeyID, serverID)
	}

	hk, ok := serverKeys[hostKeyID]
	if !ok {
		return nil, fmt.Errorf("%w: host key %s not found on server %s", ErrServerNotFound, hostKeyID, serverID)
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
		return nil, fmt.Errorf("%w: host key %s not found on server %s", ErrServerNotFound, hostKeyID, serverID)
	}

	hk, ok := serverKeys[hostKeyID]
	if !ok {
		return nil, fmt.Errorf("%w: host key %s not found on server %s", ErrServerNotFound, hostKeyID, serverID)
	}

	if description != "" {
		hk.Description = description
	}

	return cloneHostKey(hk), nil
}

// ImportSshPublicKey imports an SSH public key for a user on a server.
func (b *InMemoryBackend) ImportSshPublicKey(serverID, userName, sshPublicKeyBody string) (*SshPublicKey, error) {
	b.mu.Lock("ImportSshPublicKey")
	defer b.mu.Unlock()

	if _, ok := b.servers[serverID]; !ok {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	if _, ok := b.sshPublicKeys[serverID]; !ok {
		b.sshPublicKeys[serverID] = make(map[string]map[string]*SshPublicKey)
	}

	if _, ok := b.sshPublicKeys[serverID][userName]; !ok {
		b.sshPublicKeys[serverID][userName] = make(map[string]*SshPublicKey)
	}

	keyID := "key-" + uuid.NewString()[:8]

	k := &SshPublicKey{
		SshPublicKeyID:   keyID,
		SshPublicKeyBody: sshPublicKeyBody,
		UserName:         userName,
		ServerID:         serverID,
		DateImported:     time.Now(),
	}
	b.sshPublicKeys[serverID][userName][keyID] = k

	return &SshPublicKey{
		SshPublicKeyID:   k.SshPublicKeyID,
		SshPublicKeyBody: k.SshPublicKeyBody,
		UserName:         k.UserName,
		ServerID:         k.ServerID,
		DateImported:     k.DateImported,
	}, nil
}

// DeleteSshPublicKey removes an SSH public key from a user on a server.
func (b *InMemoryBackend) DeleteSshPublicKey(serverID, userName, sshPublicKeyID string) error {
	b.mu.Lock("DeleteSshPublicKey")
	defer b.mu.Unlock()

	serverKeys, ok := b.sshPublicKeys[serverID]
	if !ok {
		return fmt.Errorf("%w: SSH key %s not found", ErrUserNotFound, sshPublicKeyID)
	}

	userKeys, ok := serverKeys[userName]
	if !ok {
		return fmt.Errorf("%w: SSH key %s not found", ErrUserNotFound, sshPublicKeyID)
	}

	if _, exists := userKeys[sshPublicKeyID]; !exists {
		return fmt.Errorf("%w: SSH key %s not found", ErrUserNotFound, sshPublicKeyID)
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

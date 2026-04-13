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

// Access represents an AWS Transfer access policy entry for a server.
type Access struct {
	CreatedAt  time.Time
	Tags       map[string]string
	ExternalID string
	ServerID   string
	Role       string
	HomeDir    string
	AccountID  string
	Region     string
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
	CreatedAt        time.Time
	Tags             map[string]string
	AgreementID      string
	ServerID         string
	Description      string
	LocalProfileID   string
	PartnerProfileID string
	BaseDirectory    string
	AccessRole       string
	AccountID        string
	Region           string
	Status           string
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
	CreatedAt   time.Time
	Tags        map[string]string
	ConnectorID string
	URL         string
	AccessRole  string
	AccountID   string
	Region      string
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
	CreatedAt   time.Time
	Tags        map[string]string
	ProfileID   string
	ProfileType string
	As2ID       string
	AccountID   string
	Region      string
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
	CreatedAt time.Time
	Tags      map[string]string
	WebAppID  string
	AccountID string
	Region    string
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
	CreatedAt   time.Time
	Tags        map[string]string
	WorkflowID  string
	Description string
	AccountID   string
	Region      string
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
	CreatedAt     time.Time
	Tags          map[string]string
	CertificateID string
	AccountID     string
	Region        string
}

// InMemoryBackend is the in-memory store for Transfer resources.
type InMemoryBackend struct {
	servers      map[string]*Server
	users        map[string]map[string]*User      // serverID -> userName -> User
	accesses     map[string]map[string]*Access    // serverID -> externalID -> Access
	agreements   map[string]map[string]*Agreement // serverID -> agreementID -> Agreement
	connectors   map[string]*Connector
	profiles     map[string]*Profile
	webApps      map[string]*WebApp
	workflows    map[string]*Workflow
	certificates map[string]*Certificate
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		servers:      make(map[string]*Server),
		users:        make(map[string]map[string]*User),
		accesses:     make(map[string]map[string]*Access),
		agreements:   make(map[string]map[string]*Agreement),
		connectors:   make(map[string]*Connector),
		profiles:     make(map[string]*Profile),
		webApps:      make(map[string]*WebApp),
		workflows:    make(map[string]*Workflow),
		certificates: make(map[string]*Certificate),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("transfer"),
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

	for _, p := range protocols {
		switch p {
		case "SFTP", "FTP", "FTPS", "AS2":
			// valid
		default:
			return nil, fmt.Errorf("%w: %s", ErrInvalidProtocol, p)
		}
	}

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	s := &Server{
		ServerID:  serverID,
		State:     "ONLINE",
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
// The operation is idempotent: calling it on an already ONLINE server succeeds.
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
// The operation is idempotent: calling it on an already OFFLINE server succeeds.
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
		Status:           "ACTIVE",
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

// CreateConnector creates a Transfer connector.
func (b *InMemoryBackend) CreateConnector(url, accessRole string, tags map[string]string) (*Connector, error) {
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

// CreateProfile creates an AS2 profile.
func (b *InMemoryBackend) CreateProfile(profileType, as2ID string, tags map[string]string) (*Profile, error) {
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

package ssoadmin

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusSucceeded   = "SUCCEEDED"
	statusInProgress  = "IN_PROGRESS"
	statusFailed      = "FAILED"
	appProviderCustom = "arn:aws:sso::aws:applicationProvider/custom"
)

const (
	identityStoreIDPrefixLen = 8
	identityStoreIDMaxLen    = 12
	uuidShortLen             = 8

	// Instance/application status constants.
	instanceStatusActive           = "ACTIVE"
	instanceStatusCreateInProgress = "CREATE_IN_PROGRESS"
	instanceStatusDeleteInProgress = "DELETE_IN_PROGRESS"
	appStatusEnabled               = "ENABLED"
	appStatusDisabled              = "DISABLED"

	// ABAC configuration status.
	abacStatusEnabled            = "ENABLED"
	abacStatusCreationInProgress = "CREATION_IN_PROGRESS"
	abacStatusCreationFailed     = "CREATION_FAILED"

	// Default session duration for new permission sets.
	defaultSessionDuration = "PT1H"

	// Session duration limits in minutes.
	minutesPerHour    = 60
	minSessionMinutes = 60  // 1 hour
	maxSessionMinutes = 720 // 12 hours

	// AWS tag limits.
	maxTagsPerResource = 50
	maxTagKeyLen       = 128
	maxTagValueLen     = 256

	// AWS permission set name length limit.
	maxPermissionSetNameLen = 32

	// CustomerManagedPolicyReference limits.
	maxCMPRNameLen = 128
	maxCMPRPathLen = 512

	// AccessControlAttribute limits per AWS spec.
	maxACAAttributes    = 50
	maxACAKeyLen        = 128
	maxACASourceItems   = 10
	maxACASourceItemLen = 256

	// TrustedTokenIssuer type — only OIDC_JWT is supported by AWS.
	ttiTypeOIDCJWT = "OIDC_JWT"

	// ttiDefaultType is kept as an alias for backward compat in seeding code.
	ttiDefaultType = ttiTypeOIDCJWT

	// OIDC JWT configuration.
	jwksRetrievalOpenIDDiscovery = "OPEN_ID_DISCOVERY"
	maxIssuerURLLen              = 512

	// Region scope type for SSO instance regions.
	regionScopeTypeAllRegions = "ALL_REGIONS"

	// Valid principal types for account assignments.
	principalTypeUser  = "USER"
	principalTypeGroup = "GROUP"

	// Valid target types for ProvisionPermissionSet.
	targetTypeAWSAccount             = "AWS_ACCOUNT"
	targetTypeAllProvisionedAccounts = "ALL_PROVISIONED_ACCOUNTS"

	// Valid authentication method types.
	authMethodTypeIAM = "IAM"

	// Application portal visibility.
	portalVisibilityEnabled  = "ENABLED"
	portalVisibilityDisabled = "DISABLED"

	// Application sign-in origin.
	signInOriginApplication    = "APPLICATION"
	signInOriginIdentityCenter = "IDENTITY_CENTER"

	// Federation protocol constants.
	federationProtocolSAML = "SAML"
)

// Compiled validation regexes.
var (
	// permissionSetNameRe matches valid permission set names per AWS spec.
	permissionSetNameRe = regexp.MustCompile(`^[\w+=,.@-]+$`)

	// cmprNameRe matches valid CustomerManagedPolicyReference names per AWS spec.
	cmprNameRe = regexp.MustCompile(`^[\w+=,.@-]+$`)

	// acaKeyRe matches valid AccessControlAttribute keys per AWS spec.
	acaKeyRe = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]+$`)
)

var (
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)

	ErrInstanceNotFound                = errors.New("ResourceNotFoundException")
	ErrPermissionSetNotFound           = errors.New("ResourceNotFoundException")
	ErrPermissionSetAlreadyExists      = errors.New("ConflictException")
	ErrPermissionSetHasAssignments     = errors.New("ConflictException")
	ErrAssignmentNotFound              = errors.New("ResourceNotFoundException")
	ErrRequestNotFound                 = errors.New("ResourceNotFoundException")
	ErrApplicationNotFound             = errors.New("ResourceNotFoundException")
	ErrApplicationAlreadyExists        = errors.New("ConflictException")
	ErrTrustedTokenIssuerNotFound      = errors.New("ResourceNotFoundException")
	ErrTrustedTokenIssuerAlreadyExists = errors.New("ConflictException")
	ErrAccessScopeNotFound             = errors.New("ResourceNotFoundException")
	ErrAuthMethodNotFound              = errors.New("ResourceNotFoundException")
	ErrGrantNotFound                   = errors.New("ResourceNotFoundException")
	ErrACAAlreadyExists                = errors.New("ConflictException")
	ErrPermissionsBoundaryNotFound     = errors.New("ResourceNotFoundException")
	ErrTooManyTagsException            = errors.New("TooManyTagsException")
)

// Instance represents an AWS SSO instance.
type Instance struct {
	CreatedDate     time.Time         `json:"CreatedDate"`
	Tags            map[string]string `json:"Tags"`
	IdentityStoreID string            `json:"IdentityStoreId"`
	InstanceArn     string            `json:"InstanceArn"`
	Name            string            `json:"Name"`
	OwnerAccountID  string            `json:"OwnerAccountId"`
	Status          string            `json:"Status"`
}

// PermissionSet represents an AWS SSO permission set.
type PermissionSet struct {
	CreatedDate      time.Time         `json:"CreatedDate"`
	Tags             map[string]string `json:"Tags"`
	PermissionSetArn string            `json:"PermissionSetArn"`
	InstanceArn      string            `json:"InstanceArn"`
	Name             string            `json:"Name"`
	Description      string            `json:"Description"`
	SessionDuration  string            `json:"SessionDuration"`
	RelayState       string            `json:"RelayState"`
	InlinePolicy     string            `json:"InlinePolicy"`
	ManagedPolicies  []ManagedPolicy   `json:"ManagedPolicies"`
}

// ManagedPolicy represents an IAM managed policy attached to a permission set.
type ManagedPolicy struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

// AccountAssignment represents an assignment of a permission set to a principal in an account.
type AccountAssignment struct {
	AccountID        string `json:"AccountId"`
	PermissionSetArn string `json:"PermissionSetArn"`
	PrincipalID      string `json:"PrincipalId"`
	PrincipalType    string `json:"PrincipalType"`
}

// ProvisioningStatus represents the status of an async provisioning/assignment request.
type ProvisioningStatus struct {
	CreatedDate      time.Time `json:"CreatedDate"`
	RequestID        string    `json:"RequestId"`
	Status           string    `json:"Status"`
	FailureReason    string    `json:"FailureReason"`
	AccountID        string    `json:"AccountId,omitempty"`
	PermissionSetArn string    `json:"PermissionSetArn,omitempty"`
	TargetType       string    `json:"TargetType,omitempty"`
	PrincipalID      string    `json:"PrincipalId,omitempty"`
	PrincipalType    string    `json:"PrincipalType,omitempty"`
}

// CustomerManagedPolicyReference references a customer-managed policy.
type CustomerManagedPolicyReference struct {
	Name string `json:"Name"`
	Path string `json:"Path"`
}

// SignInOptions holds sign-in configuration for an application's portal.
type SignInOptions struct {
	Origin         string `json:"Origin"`
	ApplicationURL string `json:"ApplicationUrl"`
}

// PortalOptions holds portal configuration for an application.
type PortalOptions struct {
	Visibility    string        `json:"Visibility"`
	SignInOptions SignInOptions `json:"SignInOptions"`
}

// PermissionsBoundary is a union: either ManagedPolicyArn or CustomerManagedPolicyReference.
type PermissionsBoundary struct {
	CustomerManagedPolicyReference *CustomerManagedPolicyReference `json:"CustomerManagedPolicyReference,omitempty"`
	ManagedPolicyArn               string                          `json:"ManagedPolicyArn,omitempty"`
}

// ABACConfig holds ABAC configuration for an SSO instance with lifecycle status.
type ABACConfig struct {
	StatusReason            string                   `json:"StatusReason"`
	Status                  string                   `json:"Status"`
	AccessControlAttributes []AccessControlAttribute `json:"AccessControlAttributes"`
}

// Application represents an AWS SSO application.
type Application struct {
	CreatedDate            time.Time         `json:"CreatedDate"`
	Tags                   map[string]string `json:"Tags"`
	PortalOptions          *PortalOptions    `json:"PortalOptions,omitempty"`
	ApplicationArn         string            `json:"ApplicationArn"`
	ApplicationProviderArn string            `json:"ApplicationProviderArn"`
	Description            string            `json:"Description"`
	InstanceArn            string            `json:"InstanceArn"`
	Name                   string            `json:"Name"`
	Status                 string            `json:"Status"`
}

// ApplicationAssignment represents a principal assigned to an application.
type ApplicationAssignment struct {
	ApplicationArn string `json:"ApplicationArn"`
	PrincipalID    string `json:"PrincipalId"`
	PrincipalType  string `json:"PrincipalType"`
}

// AccessControlAttributeValue holds the source list for an attribute.
type AccessControlAttributeValue struct {
	Source []string `json:"Source"`
}

// AccessControlAttribute represents a single ABAC attribute.
type AccessControlAttribute struct {
	Key   string                      `json:"Key"`
	Value AccessControlAttributeValue `json:"Value"`
}

// ApplicationProviderDisplayData contains display metadata for an application provider.
type ApplicationProviderDisplayData struct {
	Description string `json:"Description,omitempty"`
	DisplayName string `json:"DisplayName,omitempty"`
	IconURL     string `json:"IconUrl,omitempty"`
}

// ApplicationProvider represents an SSO application provider.
type ApplicationProvider struct {
	ApplicationProviderArn string                         `json:"ApplicationProviderArn"`
	DisplayData            ApplicationProviderDisplayData `json:"DisplayData"`
	FederationProtocol     string                         `json:"FederationProtocol,omitempty"`
}

// InstanceAccessControlAttributeConfiguration holds ABAC configuration for an instance.
type InstanceAccessControlAttributeConfiguration struct {
	AccessControlAttributes []AccessControlAttribute `json:"AccessControlAttributes"`
}

// RegionMetadata represents a region associated with an SSO instance.
type RegionMetadata struct {
	Region          string `json:"Region"`
	RegionScopeType string `json:"RegionScopeType"`
}

// OidcJwtConfiguration holds OIDC JWT trusted token issuer configuration.
type OidcJwtConfiguration struct {
	IssuerURL                  string `json:"IssuerUrl"`
	ClaimAttributePath         string `json:"ClaimAttributePath"`
	IdentityStoreAttributePath string `json:"IdentityStoreAttributePath"`
	JwksRetrievalOption        string `json:"JwksRetrievalOption"`
}

// TrustedTokenIssuerConfiguration holds the issuer-type-specific configuration.
type TrustedTokenIssuerConfiguration struct {
	OidcJwtConfiguration *OidcJwtConfiguration `json:"OidcJwtConfiguration,omitempty"`
}

// TrustedTokenIssuer represents a trusted token issuer.
type TrustedTokenIssuer struct {
	Tags                            map[string]string                `json:"Tags"`
	TrustedTokenIssuerConfiguration *TrustedTokenIssuerConfiguration `json:"TrustedTokenIssuerConfiguration,omitempty"`
	TrustedTokenIssuerArn           string                           `json:"TrustedTokenIssuerArn"`
	InstanceArn                     string                           `json:"InstanceArn"`
	Name                            string                           `json:"Name"`
	TrustedTokenIssuerType          string                           `json:"TrustedTokenIssuerType"`
}

// InMemoryBackend is the in-memory backend for the SSO Admin service.
type InMemoryBackend struct {
	instances      map[string]*Instance
	permissionSets map[string]*PermissionSet
	assignments    map[string][]*AccountAssignment
	// assignmentCreationIDs maps assignmentKey|accountID|principalType|principalID → requestID for idempotency.
	assignmentCreationIDs   map[string]string
	creationStatuses        map[string]*ProvisioningStatus
	deletionStatuses        map[string]*ProvisioningStatus
	provisioningStatuses    map[string]*ProvisioningStatus
	instanceRegions         map[string][]RegionMetadata
	customerManagedPolicies map[string][]CustomerManagedPolicyReference
	applications            map[string]*Application
	applicationAssignments  map[string][]*ApplicationAssignment
	applicationScopes       map[string][]string
	// applicationAuthMethods stores per-app authentication methods: appArn → methodType → full JSON body.
	applicationAuthMethods map[string]map[string]json.RawMessage
	// applicationGrants stores per-app grants: appArn → grantType → full JSON body.
	applicationGrants       map[string]map[string]json.RawMessage
	applicationAssignConfig map[string]bool
	applicationSessions     map[string]string
	instanceACAs            map[string]*ABACConfig
	trustedTokenIssuers     map[string]*TrustedTokenIssuer
	permissionBoundaries    map[string]*PermissionsBoundary
	mu                      *lockmetrics.RWMutex
	accountID               string
	region                  string
}

// seedDefaultInstance adds the default pre-seeded instance. Must be called before concurrent use.
func (b *InMemoryBackend) seedDefaultInstance() {
	defaultID := "d-0000000001"
	identityStoreID := "d-" + b.accountID
	if len(identityStoreID) > identityStoreIDMaxLen {
		identityStoreID = identityStoreID[:identityStoreIDMaxLen]
	}
	defaultArn := "arn:aws:sso:::instance/ssoins-" + defaultID
	if b.instances[defaultArn] != nil {
		return
	}
	b.instances[defaultArn] = &Instance{
		InstanceArn:     defaultArn,
		Name:            "default",
		OwnerAccountID:  b.accountID,
		IdentityStoreID: identityStoreID,
		Status:          instanceStatusActive,
		CreatedDate:     time.Now().UTC(),
		Tags:            make(map[string]string),
	}
}

// NewInMemoryBackend creates a new in-memory SSO Admin backend with a default instance.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		instances:               make(map[string]*Instance),
		permissionSets:          make(map[string]*PermissionSet),
		assignments:             make(map[string][]*AccountAssignment),
		assignmentCreationIDs:   make(map[string]string),
		creationStatuses:        make(map[string]*ProvisioningStatus),
		deletionStatuses:        make(map[string]*ProvisioningStatus),
		provisioningStatuses:    make(map[string]*ProvisioningStatus),
		instanceRegions:         make(map[string][]RegionMetadata),
		customerManagedPolicies: make(map[string][]CustomerManagedPolicyReference),
		applications:            make(map[string]*Application),
		applicationAssignments:  make(map[string][]*ApplicationAssignment),
		applicationScopes:       make(map[string][]string),
		applicationAuthMethods:  make(map[string]map[string]json.RawMessage),
		applicationGrants:       make(map[string]map[string]json.RawMessage),
		applicationAssignConfig: make(map[string]bool),
		applicationSessions:     make(map[string]string),
		instanceACAs:            make(map[string]*ABACConfig),
		trustedTokenIssuers:     make(map[string]*TrustedTokenIssuer),
		permissionBoundaries:    make(map[string]*PermissionsBoundary),
		mu:                      lockmetrics.New("ssoadmin"),
		accountID:               accountID,
		region:                  region,
	}

	// Pre-seed a default instance to mimic AWS SSO behaviour where an instance
	// is always present once SSO is enabled.
	b.seedDefaultInstance()

	return b
}

// Reset clears all backend state and re-seeds the default instance.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.instances = make(map[string]*Instance)
	b.permissionSets = make(map[string]*PermissionSet)
	b.assignments = make(map[string][]*AccountAssignment)
	b.creationStatuses = make(map[string]*ProvisioningStatus)
	b.deletionStatuses = make(map[string]*ProvisioningStatus)
	b.provisioningStatuses = make(map[string]*ProvisioningStatus)
	b.instanceRegions = make(map[string][]RegionMetadata)
	b.assignmentCreationIDs = make(map[string]string)
	b.customerManagedPolicies = make(map[string][]CustomerManagedPolicyReference)
	b.applications = make(map[string]*Application)
	b.applicationAssignments = make(map[string][]*ApplicationAssignment)
	b.applicationScopes = make(map[string][]string)
	b.applicationAuthMethods = make(map[string]map[string]json.RawMessage)
	b.applicationGrants = make(map[string]map[string]json.RawMessage)
	b.applicationAssignConfig = make(map[string]bool)
	b.applicationSessions = make(map[string]string)
	b.instanceACAs = make(map[string]*ABACConfig)
	b.trustedTokenIssuers = make(map[string]*TrustedTokenIssuer)
	b.permissionBoundaries = make(map[string]*PermissionsBoundary)
	// Re-seed the default instance.
	b.seedDefaultInstance()
}

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateInstance creates a new SSO instance.
func (b *InMemoryBackend) CreateInstance(name, ownerAccountID, identityStoreID string) (*Instance, error) {
	b.mu.Lock("CreateInstance")
	defer b.mu.Unlock()

	id := uuid.NewString()[:uuidShortLen]
	instanceArn := "arn:aws:sso:::instance/ssoins-" + id

	if ownerAccountID == "" {
		ownerAccountID = b.accountID
	}
	if identityStoreID == "" {
		prefix := ownerAccountID
		if len(prefix) > identityStoreIDPrefixLen {
			prefix = prefix[:identityStoreIDPrefixLen]
		}
		raw := "d-" + prefix + "0000000000"
		if len(raw) > identityStoreIDMaxLen {
			raw = raw[:identityStoreIDMaxLen]
		}
		identityStoreID = raw
	}

	inst := &Instance{
		InstanceArn:     instanceArn,
		Name:            name,
		OwnerAccountID:  ownerAccountID,
		IdentityStoreID: identityStoreID,
		Status:          instanceStatusCreateInProgress,
		CreatedDate:     time.Now().UTC(),
		Tags:            make(map[string]string),
	}
	b.instances[instanceArn] = inst

	cp := *inst
	cp.Tags = make(map[string]string)

	return &cp, nil
}

// ListInstances returns all SSO instances, pruning DELETE_IN_PROGRESS entries.
func (b *InMemoryBackend) ListInstances() []*Instance {
	b.mu.Lock("ListInstances")
	defer b.mu.Unlock()

	list := make([]*Instance, 0, len(b.instances))
	for arn, inst := range b.instances {
		if inst.Status == instanceStatusDeleteInProgress {
			// Prune resources then remove instance entry.
			b.cascadeDeleteInstance(arn)
			delete(b.instances, arn)

			continue
		}
		if inst.Status == instanceStatusCreateInProgress {
			inst.Status = instanceStatusActive
		}
		cp := *inst
		list = append(list, &cp)
	}

	return list
}

// DescribeInstance returns a specific SSO instance.
// Lazily transitions CREATE_IN_PROGRESS → ACTIVE on first read.
func (b *InMemoryBackend) DescribeInstance(instanceArn string) (*Instance, error) {
	b.mu.Lock("DescribeInstance")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceArn]
	if !ok {
		return nil, ErrInstanceNotFound
	}
	if inst.Status == instanceStatusCreateInProgress {
		inst.Status = instanceStatusActive
	}

	cp := *inst
	cp.Tags = make(map[string]string, len(inst.Tags))
	maps.Copy(cp.Tags, inst.Tags)

	return &cp, nil
}

// DeleteInstance transitions an instance to DELETE_IN_PROGRESS and cascades deletion of dependent resources.
// The instance entry is retained briefly with DELETE_IN_PROGRESS status and pruned on next ListInstances.
func (b *InMemoryBackend) DeleteInstance(instanceArn string) error {
	b.mu.Lock("DeleteInstance")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceArn]
	if !ok {
		return ErrInstanceNotFound
	}
	inst.Status = instanceStatusDeleteInProgress
	b.cascadeDeleteInstance(instanceArn)

	return nil
}

// cascadeDeleteInstance removes all resources belonging to instanceArn. Must be called with mu held.
func (b *InMemoryBackend) cascadeDeleteInstance(instanceArn string) {
	for psArn, ps := range b.permissionSets {
		if ps.InstanceArn == instanceArn {
			key := assignmentKey(instanceArn, psArn)
			delete(b.assignments, key)
			delete(b.customerManagedPolicies, psArn)
			delete(b.permissionBoundaries, psArn)
			delete(b.permissionSets, psArn)
		}
	}
	delete(b.instanceACAs, instanceArn)
	delete(b.instanceRegions, instanceArn)
	for appArn, app := range b.applications {
		if app.InstanceArn == instanceArn {
			delete(b.applicationAssignments, appArn)
			delete(b.applicationScopes, appArn)
			delete(b.applicationAuthMethods, appArn)
			delete(b.applicationGrants, appArn)
			delete(b.applicationAssignConfig, appArn)
			delete(b.applicationSessions, appArn)
			delete(b.applications, appArn)
		}
	}
	for ttiArn, tti := range b.trustedTokenIssuers {
		if tti.InstanceArn == instanceArn {
			delete(b.trustedTokenIssuers, ttiArn)
		}
	}
}

// CreatePermissionSet creates a new permission set within an SSO instance.
func (b *InMemoryBackend) CreatePermissionSet(
	instanceArn, name, description, sessionDuration, relayState string,
	tags map[string]string,
) (*PermissionSet, error) {
	b.mu.Lock("CreatePermissionSet")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return nil, ErrInstanceNotFound
	}

	if err := validatePermissionSetName(name); err != nil {
		return nil, err
	}

	if err := validateSessionDuration(sessionDuration); err != nil {
		return nil, err
	}

	if len(tags) > 0 {
		if err := validateTags(tags); err != nil {
			return nil, err
		}
	}

	for _, ps := range b.permissionSets {
		if ps.InstanceArn == instanceArn && ps.Name == name {
			return nil, ErrPermissionSetAlreadyExists
		}
	}

	instanceID := instanceARNToID(instanceArn)
	id := uuid.NewString()[:uuidShortLen]
	psArn := fmt.Sprintf("arn:aws:sso:::permissionSet/%s/%s", instanceID, id)

	if sessionDuration == "" {
		sessionDuration = defaultSessionDuration
	}

	ps := &PermissionSet{
		PermissionSetArn: psArn,
		InstanceArn:      instanceArn,
		Name:             name,
		Description:      description,
		SessionDuration:  sessionDuration,
		RelayState:       relayState,
		CreatedDate:      time.Now().UTC(),
		Tags:             make(map[string]string),
	}
	maps.Copy(ps.Tags, tags)
	b.permissionSets[psArn] = ps

	cp := b.copyPermissionSet(ps)

	return cp, nil
}

// DescribePermissionSet returns a specific permission set.
func (b *InMemoryBackend) DescribePermissionSet(instanceArn, permissionSetArn string) (*PermissionSet, error) {
	b.mu.RLock("DescribePermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return nil, ErrPermissionSetNotFound
	}

	return b.copyPermissionSet(ps), nil
}

// ListPermissionSets returns all permission sets for an SSO instance.
func (b *InMemoryBackend) ListPermissionSets(instanceArn string) []*PermissionSet {
	b.mu.RLock("ListPermissionSets")
	defer b.mu.RUnlock()

	var list []*PermissionSet
	for _, ps := range b.permissionSets {
		if ps.InstanceArn == instanceArn {
			list = append(list, b.copyPermissionSet(ps))
		}
	}

	return list
}

// DeletePermissionSet removes a permission set and cascades to its assignments.
// Returns ConflictException if the permission set is still assigned to any accounts.
func (b *InMemoryBackend) DeletePermissionSet(instanceArn, permissionSetArn string) error {
	b.mu.Lock("DeletePermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}

	// AWS returns ConflictException if there are active account assignments.
	key := assignmentKey(instanceArn, permissionSetArn)
	if len(b.assignments[key]) > 0 {
		return ErrPermissionSetHasAssignments
	}

	delete(b.permissionSets, permissionSetArn)
	delete(b.permissionBoundaries, permissionSetArn)
	delete(b.customerManagedPolicies, permissionSetArn)
	delete(b.assignments, key)

	return nil
}

// UpdatePermissionSet updates a permission set's mutable fields.
func (b *InMemoryBackend) UpdatePermissionSet(
	instanceArn, permissionSetArn, description, sessionDuration, relayState string,
) error {
	b.mu.Lock("UpdatePermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	if sessionDuration != "" {
		if err := validateSessionDuration(sessionDuration); err != nil {
			return err
		}
	}
	if description != "" {
		ps.Description = description
	}
	if sessionDuration != "" {
		ps.SessionDuration = sessionDuration
	}
	if relayState != "" {
		ps.RelayState = relayState
	}

	return nil
}

// CreateAccountAssignment assigns a permission set to a principal in an account.
func (b *InMemoryBackend) CreateAccountAssignment(
	instanceArn, permissionSetArn, accountID, principalID, principalType string,
) (string, error) {
	b.mu.Lock("CreateAccountAssignment")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return "", ErrInstanceNotFound
	}
	if _, ok := b.permissionSets[permissionSetArn]; !ok {
		return "", ErrPermissionSetNotFound
	}

	assignment := &AccountAssignment{
		AccountID:        accountID,
		PermissionSetArn: permissionSetArn,
		PrincipalID:      principalID,
		PrincipalType:    principalType,
	}
	key := assignmentKey(instanceArn, permissionSetArn)

	// Build a deterministic idempotency key for this specific assignment.
	idempotencyKey := assignmentIdempotencyKey(key, accountID, principalType, principalID)

	// Idempotency: if the same assignment already exists, return the original request ID.
	if existingRequestID, exists := b.assignmentCreationIDs[idempotencyKey]; exists {
		return existingRequestID, nil
	}

	b.assignments[key] = append(b.assignments[key], assignment)

	requestID := uuid.NewString()
	b.creationStatuses[requestID] = &ProvisioningStatus{
		RequestID:        requestID,
		Status:           statusInProgress,
		CreatedDate:      time.Now().UTC(),
		AccountID:        accountID,
		PermissionSetArn: permissionSetArn,
		PrincipalID:      principalID,
		PrincipalType:    principalType,
		TargetType:       targetTypeAWSAccount,
	}
	b.assignmentCreationIDs[idempotencyKey] = requestID

	return requestID, nil
}

// DescribeAccountAssignmentCreationStatus returns the status of a creation request.
// Lazily transitions IN_PROGRESS → SUCCEEDED on first poll.
func (b *InMemoryBackend) DescribeAccountAssignmentCreationStatus(
	_ string,
	requestID string,
) (*ProvisioningStatus, error) {
	b.mu.Lock("DescribeAccountAssignmentCreationStatus")
	defer b.mu.Unlock()

	status, ok := b.creationStatuses[requestID]
	if !ok {
		return nil, ErrRequestNotFound
	}
	if status.Status == statusInProgress {
		status.Status = statusSucceeded
	}

	cp := *status

	return &cp, nil
}

// ListAccountAssignmentCreationStatus returns creation statuses sorted by creation date descending.
// filterStatus filters by status when non-empty.
func (b *InMemoryBackend) ListAccountAssignmentCreationStatus(_, filterStatus string) []*ProvisioningStatus {
	b.mu.RLock("ListAccountAssignmentCreationStatus")
	defer b.mu.RUnlock()

	result := make([]*ProvisioningStatus, 0, len(b.creationStatuses))
	for _, status := range b.creationStatuses {
		if filterStatus != "" && status.Status != filterStatus {
			continue
		}
		cp := *status
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedDate.After(result[j].CreatedDate)
	})

	return result
}

// ListAccountAssignments returns assignments for a permission set in an instance, optionally filtered by account.
func (b *InMemoryBackend) ListAccountAssignments(instanceArn, permissionSetArn, accountID string) []*AccountAssignment {
	b.mu.RLock("ListAccountAssignments")
	defer b.mu.RUnlock()

	key := assignmentKey(instanceArn, permissionSetArn)
	all := b.assignments[key]

	var result []*AccountAssignment
	for _, a := range all {
		if accountID == "" || a.AccountID == accountID {
			cp := *a
			result = append(result, &cp)
		}
	}

	return result
}

// DeleteAccountAssignment removes an account assignment.
func (b *InMemoryBackend) DeleteAccountAssignment(
	instanceArn, permissionSetArn, accountID, principalID, principalType string,
) (string, error) {
	b.mu.Lock("DeleteAccountAssignment")
	defer b.mu.Unlock()

	key := assignmentKey(instanceArn, permissionSetArn)
	all := b.assignments[key]

	found := false
	var remaining []*AccountAssignment
	for _, a := range all {
		if a.AccountID == accountID && a.PrincipalID == principalID && a.PrincipalType == principalType {
			found = true
		} else {
			remaining = append(remaining, a)
		}
	}
	if !found {
		return "", ErrAssignmentNotFound
	}
	b.assignments[key] = remaining

	// Remove the idempotency index entry and associated creation status to prevent unbounded growth.
	idempotencyKey := key + "|" + accountID + "|" + principalType + "|" + principalID
	if oldRequestID, exists := b.assignmentCreationIDs[idempotencyKey]; exists {
		delete(b.creationStatuses, oldRequestID)
	}
	delete(b.assignmentCreationIDs, idempotencyKey)

	requestID := uuid.NewString()
	b.deletionStatuses[requestID] = &ProvisioningStatus{
		RequestID:        requestID,
		Status:           statusInProgress,
		AccountID:        accountID,
		PermissionSetArn: permissionSetArn,
		PrincipalID:      principalID,
		PrincipalType:    principalType,
		TargetType:       targetTypeAWSAccount,
		CreatedDate:      time.Now().UTC(),
	}

	return requestID, nil
}

// DescribeAccountAssignmentDeletionStatus returns the status of a deletion request.
// Lazily transitions IN_PROGRESS → SUCCEEDED on first poll.
func (b *InMemoryBackend) DescribeAccountAssignmentDeletionStatus(
	_ string,
	requestID string,
) (*ProvisioningStatus, error) {
	b.mu.Lock("DescribeAccountAssignmentDeletionStatus")
	defer b.mu.Unlock()

	status, ok := b.deletionStatuses[requestID]
	if !ok {
		return nil, ErrRequestNotFound
	}
	if status.Status == statusInProgress {
		status.Status = statusSucceeded
	}

	cp := *status

	return &cp, nil
}

// ListAccountAssignmentDeletionStatus returns deletion statuses sorted by creation date descending.
// filterStatus filters by status when non-empty.
func (b *InMemoryBackend) ListAccountAssignmentDeletionStatus(_, filterStatus string) []*ProvisioningStatus {
	b.mu.RLock("ListAccountAssignmentDeletionStatus")
	defer b.mu.RUnlock()

	result := make([]*ProvisioningStatus, 0, len(b.deletionStatuses))
	for _, status := range b.deletionStatuses {
		if filterStatus != "" && status.Status != filterStatus {
			continue
		}
		cp := *status
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedDate.After(result[j].CreatedDate)
	})

	return result
}

// AttachManagedPolicyToPermissionSet attaches a managed policy to a permission set.
func (b *InMemoryBackend) AttachManagedPolicyToPermissionSet(
	instanceArn, permissionSetArn, managedPolicyArn, name string,
) error {
	b.mu.Lock("AttachManagedPolicyToPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	for _, mp := range ps.ManagedPolicies {
		if mp.Arn == managedPolicyArn {
			return nil
		}
	}
	ps.ManagedPolicies = append(ps.ManagedPolicies, ManagedPolicy{Arn: managedPolicyArn, Name: name})

	return nil
}

// DetachManagedPolicyFromPermissionSet detaches a managed policy from a permission set.
// Returns ResourceNotFoundException if the policy is not attached.
func (b *InMemoryBackend) DetachManagedPolicyFromPermissionSet(
	instanceArn, permissionSetArn, managedPolicyArn string,
) error {
	b.mu.Lock("DetachManagedPolicyFromPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	found := false
	remaining := make([]ManagedPolicy, 0, len(ps.ManagedPolicies))
	for _, mp := range ps.ManagedPolicies {
		if mp.Arn == managedPolicyArn {
			found = true
		} else {
			remaining = append(remaining, mp)
		}
	}
	if !found {
		return ErrRequestNotFound
	}
	ps.ManagedPolicies = remaining

	return nil
}

// ListManagedPoliciesInPermissionSet lists managed policies attached to a permission set.
func (b *InMemoryBackend) ListManagedPoliciesInPermissionSet(
	instanceArn, permissionSetArn string,
) ([]ManagedPolicy, error) {
	b.mu.RLock("ListManagedPoliciesInPermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return nil, ErrPermissionSetNotFound
	}
	result := make([]ManagedPolicy, len(ps.ManagedPolicies))
	copy(result, ps.ManagedPolicies)

	return result, nil
}

// PutInlinePolicyToPermissionSet sets the inline policy on a permission set.
func (b *InMemoryBackend) PutInlinePolicyToPermissionSet(instanceArn, permissionSetArn, inlinePolicy string) error {
	b.mu.Lock("PutInlinePolicyToPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	ps.InlinePolicy = inlinePolicy

	return nil
}

// GetInlinePolicyForPermissionSet returns the inline policy for a permission set.
func (b *InMemoryBackend) GetInlinePolicyForPermissionSet(instanceArn, permissionSetArn string) (string, error) {
	b.mu.RLock("GetInlinePolicyForPermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return "", ErrPermissionSetNotFound
	}

	return ps.InlinePolicy, nil
}

// DeleteInlinePolicyFromPermissionSet removes the inline policy from a permission set.
func (b *InMemoryBackend) DeleteInlinePolicyFromPermissionSet(instanceArn, permissionSetArn string) error {
	b.mu.Lock("DeleteInlinePolicyFromPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	ps.InlinePolicy = ""

	return nil
}

// PutPermissionsBoundaryToPermissionSet sets the permissions boundary on a permission set.
// boundary must have exactly one of ManagedPolicyArn or CustomerManagedPolicyReference set.
func (b *InMemoryBackend) PutPermissionsBoundaryToPermissionSet(
	instanceArn, permissionSetArn string, boundary *PermissionsBoundary,
) error {
	b.mu.Lock("PutPermissionsBoundaryToPermissionSet")
	defer b.mu.Unlock()

	if boundary == nil {
		return fmt.Errorf("%w: PermissionsBoundary is required", awserr.ErrInvalidParameter)
	}
	hasManaged := boundary.ManagedPolicyArn != ""
	hasCMPR := boundary.CustomerManagedPolicyReference != nil
	if hasManaged == hasCMPR {
		return fmt.Errorf(
			"%w: PermissionsBoundary must have exactly one of ManagedPolicyArn or CustomerManagedPolicyReference",
			awserr.ErrInvalidParameter,
		)
	}
	if hasCMPR {
		if err := validateCustomerManagedPolicyReference(
			boundary.CustomerManagedPolicyReference.Name,
			boundary.CustomerManagedPolicyReference.Path,
		); err != nil {
			return err
		}
	}

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	b.permissionBoundaries[permissionSetArn] = boundary

	return nil
}

// GetPermissionsBoundaryForPermissionSet returns the permissions boundary for a permission set.
func (b *InMemoryBackend) GetPermissionsBoundaryForPermissionSet(
	instanceArn,
	permissionSetArn string,
) (*PermissionsBoundary, error) {
	b.mu.RLock("GetPermissionsBoundaryForPermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return nil, ErrPermissionSetNotFound
	}

	boundary, ok := b.permissionBoundaries[permissionSetArn]
	if !ok {
		return nil, ErrRequestNotFound
	}

	return boundary, nil
}

// ProvisionPermissionSet initiates provisioning of a permission set.
// targetType is AWS_ACCOUNT or ALL_PROVISIONED_ACCOUNTS; targetID is required for AWS_ACCOUNT.
func (b *InMemoryBackend) ProvisionPermissionSet(
	instanceArn, permissionSetArn, targetType, targetID string,
) (string, error) {
	b.mu.Lock("ProvisionPermissionSet")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return "", ErrInstanceNotFound
	}
	if _, ok := b.permissionSets[permissionSetArn]; !ok {
		return "", ErrPermissionSetNotFound
	}

	requestID := uuid.NewString()
	b.provisioningStatuses[requestID] = &ProvisioningStatus{
		RequestID:        requestID,
		Status:           statusInProgress,
		CreatedDate:      time.Now().UTC(),
		PermissionSetArn: permissionSetArn,
		TargetType:       targetType,
		AccountID:        targetID,
	}

	return requestID, nil
}

// DescribePermissionSetProvisioningStatus returns the status of a provisioning request.
// Lazily transitions IN_PROGRESS → SUCCEEDED on first poll.
func (b *InMemoryBackend) DescribePermissionSetProvisioningStatus(
	_ string,
	provisioningRequestID string,
) (*ProvisioningStatus, error) {
	b.mu.Lock("DescribePermissionSetProvisioningStatus")
	defer b.mu.Unlock()

	status, ok := b.provisioningStatuses[provisioningRequestID]
	if !ok {
		return nil, ErrRequestNotFound
	}
	if status.Status == statusInProgress {
		status.Status = statusSucceeded
	}

	cp := *status

	return &cp, nil
}

// ListPermissionSetProvisioningStatus returns permission-set provisioning statuses sorted by date descending.
// filterStatus filters by status when non-empty.
func (b *InMemoryBackend) ListPermissionSetProvisioningStatus(_, filterStatus string) []*ProvisioningStatus {
	b.mu.RLock("ListPermissionSetProvisioningStatus")
	defer b.mu.RUnlock()

	result := make([]*ProvisioningStatus, 0, len(b.provisioningStatuses))
	for _, status := range b.provisioningStatuses {
		if filterStatus != "" && status.Status != filterStatus {
			continue
		}
		cp := *status
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedDate.After(result[j].CreatedDate)
	})

	return result
}

// validateTags validates tag keys and values against AWS limits and character rules.
func validateTags(tags map[string]string) error {
	for k, v := range tags {
		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key exceeds maximum length of %d", awserr.ErrInvalidParameter, maxTagKeyLen)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value exceeds maximum length of %d", awserr.ErrInvalidParameter, maxTagValueLen)
		}
		if strings.HasPrefix(strings.ToLower(k), "aws:") {
			return fmt.Errorf("%w: tag keys with prefix 'aws:' are reserved", awserr.ErrInvalidParameter)
		}
	}

	return nil
}

// validatePermissionSetName checks the name regex [\w+=,.@-]+ and length limit.
func validatePermissionSetName(name string) error {
	if len(name) > maxPermissionSetNameLen {
		return fmt.Errorf("%w: permission set name must not exceed %d characters",
			awserr.ErrInvalidParameter, maxPermissionSetNameLen)
	}
	if name != "" && !permissionSetNameRe.MatchString(name) {
		return fmt.Errorf("%w: permission set name contains invalid characters (allowed: [\\w+=,.@-])",
			awserr.ErrInvalidParameter)
	}

	return nil
}

// validateSessionDuration validates ISO8601 duration in range PT1H..PT12H.
// Valid examples: PT1H, PT2H, PT12H, PT1H30M. Invalid: PT13H, PT30M, P1D.
func validateSessionDuration(dur string) error {
	if dur == "" {
		return nil
	}
	if !strings.HasPrefix(dur, "PT") {
		return fmt.Errorf("%w: SessionDuration must be an ISO8601 duration starting with PT (e.g. PT1H)",
			awserr.ErrInvalidParameter)
	}
	// Parse hours and minutes from PTxHyM / PTxH / PTyM
	rest := dur[2:]
	var hours, minutes int
	if h := strings.Index(rest, "H"); h >= 0 {
		if _, err := fmt.Sscanf(rest[:h], "%d", &hours); err != nil || hours < 0 {
			return fmt.Errorf("%w: SessionDuration has invalid hours component", awserr.ErrInvalidParameter)
		}
		rest = rest[h+1:]
	}
	if m := strings.Index(rest, "M"); m >= 0 {
		if _, err := fmt.Sscanf(rest[:m], "%d", &minutes); err != nil || minutes < 0 {
			return fmt.Errorf("%w: SessionDuration has invalid minutes component", awserr.ErrInvalidParameter)
		}
		rest = rest[m+1:]
	}
	if rest != "" {
		return fmt.Errorf("%w: SessionDuration has unsupported components", awserr.ErrInvalidParameter)
	}
	totalMinutes := hours*minutesPerHour + minutes
	if totalMinutes < minSessionMinutes || totalMinutes > maxSessionMinutes {
		return fmt.Errorf("%w: SessionDuration must be between PT1H and PT12H", awserr.ErrInvalidParameter)
	}

	return nil
}

// validateCustomerManagedPolicyReference checks Name and Path per AWS spec.
func validateCustomerManagedPolicyReference(name, path string) error {
	if name == "" {
		return fmt.Errorf("%w: CustomerManagedPolicyReference.Name is required", awserr.ErrInvalidParameter)
	}
	if len(name) > maxCMPRNameLen {
		return fmt.Errorf("%w: CustomerManagedPolicyReference.Name exceeds maximum length of %d",
			awserr.ErrInvalidParameter, maxCMPRNameLen)
	}
	if !cmprNameRe.MatchString(name) {
		return fmt.Errorf("%w: CustomerManagedPolicyReference.Name contains invalid characters",
			awserr.ErrInvalidParameter)
	}
	if path != "" {
		if len(path) > maxCMPRPathLen {
			return fmt.Errorf("%w: CustomerManagedPolicyReference.Path exceeds maximum length of %d",
				awserr.ErrInvalidParameter, maxCMPRPathLen)
		}
		if !strings.HasPrefix(path, "/") {
			return fmt.Errorf("%w: CustomerManagedPolicyReference.Path must begin with '/'",
				awserr.ErrInvalidParameter)
		}
	}

	return nil
}

// validateACAAttributes validates the AccessControlAttribute list per AWS limits.
func validateACAAttributes(attrs []AccessControlAttribute) error {
	if len(attrs) > maxACAAttributes {
		return fmt.Errorf("%w: AccessControlAttributes exceeds maximum of %d",
			awserr.ErrInvalidParameter, maxACAAttributes)
	}
	for _, a := range attrs {
		if len(a.Key) > maxACAKeyLen {
			return fmt.Errorf("%w: AccessControlAttribute key exceeds maximum length of %d",
				awserr.ErrInvalidParameter, maxACAKeyLen)
		}
		if a.Key != "" && !acaKeyRe.MatchString(a.Key) {
			return fmt.Errorf("%w: AccessControlAttribute key contains invalid characters", awserr.ErrInvalidParameter)
		}
		if len(a.Value.Source) > maxACASourceItems {
			return fmt.Errorf("%w: AccessControlAttribute source list exceeds maximum of %d items",
				awserr.ErrInvalidParameter, maxACASourceItems)
		}
		if len(a.Value.Source) == 0 {
			return fmt.Errorf("%w: AccessControlAttribute source list must have at least one item",
				awserr.ErrInvalidParameter)
		}
		for _, s := range a.Value.Source {
			if len(s) > maxACASourceItemLen {
				return fmt.Errorf("%w: AccessControlAttribute source item exceeds maximum length of %d",
					awserr.ErrInvalidParameter, maxACASourceItemLen)
			}
		}
	}

	return nil
}

// validateTrustedTokenIssuerType rejects non-OIDC_JWT types.
func validateTrustedTokenIssuerType(issuerType string) error {
	if issuerType != "" && issuerType != ttiTypeOIDCJWT {
		return fmt.Errorf("%w: TrustedTokenIssuerType must be OIDC_JWT", awserr.ErrInvalidParameter)
	}

	return nil
}

// validateOIDCJWTConfig validates OIDC JWT trusted token issuer configuration when provided.
func validateOIDCJWTConfig(cfg *OidcJwtConfiguration) error {
	if cfg == nil {
		return nil
	}
	if cfg.IssuerURL == "" {
		return fmt.Errorf("%w: OidcJwtConfiguration.IssuerUrl is required", awserr.ErrInvalidParameter)
	}
	if len(cfg.IssuerURL) > maxIssuerURLLen {
		return fmt.Errorf("%w: OidcJwtConfiguration.IssuerUrl exceeds maximum length of %d",
			awserr.ErrInvalidParameter, maxIssuerURLLen)
	}
	if _, err := url.ParseRequestURI(cfg.IssuerURL); err != nil {
		return fmt.Errorf("%w: OidcJwtConfiguration.IssuerUrl must be a valid URL", awserr.ErrInvalidParameter)
	}
	if cfg.JwksRetrievalOption != "" && cfg.JwksRetrievalOption != jwksRetrievalOpenIDDiscovery {
		return fmt.Errorf("%w: OidcJwtConfiguration.JwksRetrievalOption must be OPEN_ID_DISCOVERY",
			awserr.ErrInvalidParameter)
	}

	return nil
}

// validateApplicationProviderArn validates an application provider ARN.
// Accepts AWS-managed (arn:aws:sso::aws:applicationProvider/...) or
// account-scoped (arn:aws:sso::{accountId}:applicationProvider/...) ARNs.
func validateApplicationProviderArn(arn string) error {
	if arn == "" {
		return fmt.Errorf("%w: ApplicationProviderArn is required", awserr.ErrInvalidParameter)
	}
	if !strings.HasPrefix(arn, "arn:aws:sso::") {
		return fmt.Errorf("%w: ApplicationProviderArn must be a valid SSO ARN", awserr.ErrInvalidParameter)
	}
	if !strings.Contains(arn, ":applicationProvider/") {
		return fmt.Errorf("%w: ApplicationProviderArn must reference an applicationProvider resource",
			awserr.ErrInvalidParameter)
	}

	return nil
}

// applyTagsWithLimit applies tags to an existing map, enforcing the 50-tag limit.
func applyTagsWithLimit(existing map[string]string, newTags map[string]string) error {
	if err := validateTags(newTags); err != nil {
		return err
	}
	// Count net-new keys (keys not already present).
	netNew := 0
	for k := range newTags {
		if _, exists := existing[k]; !exists {
			netNew++
		}
	}
	if len(existing)+netNew > maxTagsPerResource {
		return ErrTooManyTagsException
	}
	maps.Copy(existing, newTags)

	return nil
}

// TagResource adds tags to a resource (permission set, instance, application, or trusted token issuer).
func (b *InMemoryBackend) TagResource(instanceArn, resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if ps, ok := b.permissionSets[resourceArn]; ok && ps.InstanceArn == instanceArn {
		if ps.Tags == nil {
			ps.Tags = make(map[string]string)
		}

		return applyTagsWithLimit(ps.Tags, tags)
	}

	if inst, ok := b.instances[resourceArn]; ok {
		if inst.Tags == nil {
			inst.Tags = make(map[string]string)
		}

		return applyTagsWithLimit(inst.Tags, tags)
	}

	if app, ok := b.applications[resourceArn]; ok && app.InstanceArn == instanceArn {
		if app.Tags == nil {
			app.Tags = make(map[string]string)
		}

		return applyTagsWithLimit(app.Tags, tags)
	}

	if tti, ok := b.trustedTokenIssuers[resourceArn]; ok && tti.InstanceArn == instanceArn {
		if tti.Tags == nil {
			tti.Tags = make(map[string]string)
		}

		return applyTagsWithLimit(tti.Tags, tags)
	}

	return ErrInstanceNotFound
}

// UntagResource removes tags from a resource (permission set, instance, or application).
func (b *InMemoryBackend) UntagResource(instanceArn, resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if ps, ok := b.permissionSets[resourceArn]; ok && ps.InstanceArn == instanceArn {
		for _, k := range tagKeys {
			delete(ps.Tags, k)
		}

		return nil
	}

	if inst, ok := b.instances[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(inst.Tags, k)
		}

		return nil
	}

	if app, ok := b.applications[resourceArn]; ok && app.InstanceArn == instanceArn {
		for _, k := range tagKeys {
			delete(app.Tags, k)
		}

		return nil
	}

	if tti, ok := b.trustedTokenIssuers[resourceArn]; ok && tti.InstanceArn == instanceArn {
		for _, k := range tagKeys {
			delete(tti.Tags, k)
		}

		return nil
	}

	return ErrInstanceNotFound
}

// ListTagsForResource returns the tags on a resource (permission set, instance, or application).
func (b *InMemoryBackend) ListTagsForResource(instanceArn, resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if ps, ok := b.permissionSets[resourceArn]; ok && ps.InstanceArn == instanceArn {
		result := make(map[string]string, len(ps.Tags))
		maps.Copy(result, ps.Tags)

		return result, nil
	}

	if inst, ok := b.instances[resourceArn]; ok {
		result := make(map[string]string, len(inst.Tags))
		maps.Copy(result, inst.Tags)

		return result, nil
	}

	if app, ok := b.applications[resourceArn]; ok && app.InstanceArn == instanceArn {
		result := make(map[string]string, len(app.Tags))
		maps.Copy(result, app.Tags)

		return result, nil
	}

	if tti, ok := b.trustedTokenIssuers[resourceArn]; ok && tti.InstanceArn == instanceArn {
		result := make(map[string]string, len(tti.Tags))
		maps.Copy(result, tti.Tags)

		return result, nil
	}

	return nil, ErrInstanceNotFound
}

// copyPermissionSet returns a deep copy of a PermissionSet. Must be called with mu held.
func (b *InMemoryBackend) copyPermissionSet(ps *PermissionSet) *PermissionSet {
	cp := *ps
	cp.Tags = make(map[string]string, len(ps.Tags))
	maps.Copy(cp.Tags, ps.Tags)
	cp.ManagedPolicies = make([]ManagedPolicy, len(ps.ManagedPolicies))
	copy(cp.ManagedPolicies, ps.ManagedPolicies)

	return &cp
}

// copyApplication returns a deep copy of an Application. Must be called with mu held.
func copyApplication(app *Application) *Application {
	cp := *app
	cp.Tags = make(map[string]string, len(app.Tags))
	maps.Copy(cp.Tags, app.Tags)
	if app.PortalOptions != nil {
		po := *app.PortalOptions
		cp.PortalOptions = &po
	}

	return &cp
}

// copyAccessControlAttributes returns a deep copy of an AccessControlAttribute slice.
func copyAccessControlAttributes(attrs []AccessControlAttribute) []AccessControlAttribute {
	if attrs == nil {
		return nil
	}
	result := make([]AccessControlAttribute, len(attrs))
	for i, a := range attrs {
		result[i] = AccessControlAttribute{
			Key: a.Key,
			Value: AccessControlAttributeValue{
				Source: slices.Clone(a.Value.Source),
			},
		}
	}

	return result
}

// copyTrustedTokenIssuer returns a deep copy of a TrustedTokenIssuer. Must be called with mu held.
func copyTrustedTokenIssuer(tti *TrustedTokenIssuer) *TrustedTokenIssuer {
	cp := *tti
	cp.Tags = make(map[string]string, len(tti.Tags))
	maps.Copy(cp.Tags, tti.Tags)
	if tti.TrustedTokenIssuerConfiguration != nil {
		cfgCopy := *tti.TrustedTokenIssuerConfiguration
		if tti.TrustedTokenIssuerConfiguration.OidcJwtConfiguration != nil {
			oidcCopy := *tti.TrustedTokenIssuerConfiguration.OidcJwtConfiguration
			cfgCopy.OidcJwtConfiguration = &oidcCopy
		}
		cp.TrustedTokenIssuerConfiguration = &cfgCopy
	}

	return &cp
}

// instanceARNToID extracts the instance ID segment from an instance ARN.
// ARN format: arn:aws:sso:::instance/ssoins-<id>.
func instanceARNToID(instanceArn string) string {
	parts := strings.Split(instanceArn, "/")
	if len(parts) >= 2 { //nolint:mnd // minimum 2 parts needed for valid ARN split
		return parts[len(parts)-1]
	}

	return instanceArn
}

func assignmentKey(instanceArn, permissionSetArn string) string {
	return instanceArn + "|" + permissionSetArn
}

// assignmentIdempotencyKey builds a deterministic key for CreateAccountAssignment idempotency.
// The key encodes all fields that uniquely identify an assignment.
func assignmentIdempotencyKey(baseKey, accountID, principalType, principalID string) string {
	return baseKey + "|" + accountID + "|" + principalType + "|" + principalID
}

// AddInstanceInternal adds a pre-built Instance directly to the backend for test seeding.
// Must NOT be called concurrently with other backend methods.
func (b *InMemoryBackend) AddInstanceInternal(name string) *Instance {
	b.mu.Lock("AddInstanceInternal")
	defer b.mu.Unlock()

	id := uuid.NewString()[:uuidShortLen]
	arn := "arn:aws:sso:::instance/ssoins-" + id
	inst := &Instance{
		InstanceArn:     arn,
		Name:            name,
		OwnerAccountID:  b.accountID,
		IdentityStoreID: "d-" + b.accountID[:min(len(b.accountID), identityStoreIDPrefixLen)],
		Status:          instanceStatusActive,
		CreatedDate:     time.Now().UTC(),
		Tags:            make(map[string]string),
	}
	b.instances[arn] = inst
	cp := *inst
	cp.Tags = make(map[string]string)

	return &cp
}

// AddPermissionSetInternal adds a pre-built PermissionSet directly to the backend for test seeding.
func (b *InMemoryBackend) AddPermissionSetInternal(instanceArn, name string) *PermissionSet {
	b.mu.Lock("AddPermissionSetInternal")
	defer b.mu.Unlock()

	instanceID := instanceARNToID(instanceArn)
	id := uuid.NewString()[:uuidShortLen]
	psArn := fmt.Sprintf("arn:aws:sso:::permissionSet/%s/%s", instanceID, id)
	ps := &PermissionSet{
		PermissionSetArn: psArn,
		InstanceArn:      instanceArn,
		Name:             name,
		SessionDuration:  defaultSessionDuration,
		CreatedDate:      time.Now().UTC(),
		Tags:             make(map[string]string),
	}
	b.permissionSets[psArn] = ps

	return b.copyPermissionSet(ps)
}

// AddApplicationInternal adds a pre-built Application directly to the backend for test seeding.
func (b *InMemoryBackend) AddApplicationInternal(instanceArn, name string) *Application {
	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	id := uuid.NewString()[:uuidShortLen]
	instanceID := instanceARNToID(instanceArn)
	appArn := fmt.Sprintf("arn:aws:sso::%s:application/%s/apl-%s", b.accountID, instanceID, id)
	app := &Application{
		ApplicationArn:         appArn,
		ApplicationProviderArn: appProviderCustom,
		InstanceArn:            instanceArn,
		Name:                   name,
		Status:                 appStatusEnabled,
		CreatedDate:            time.Now().UTC(),
		Tags:                   make(map[string]string),
	}
	b.applications[appArn] = app

	return copyApplication(app)
}

// AddRegion adds a region to an SSO instance.
func (b *InMemoryBackend) AddRegion(instanceArn, regionName string) error {
	b.mu.Lock("AddRegion")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return ErrInstanceNotFound
	}
	for _, r := range b.instanceRegions[instanceArn] {
		if r.Region == regionName {
			return nil
		}
	}
	b.instanceRegions[instanceArn] = append(b.instanceRegions[instanceArn], RegionMetadata{
		Region:          regionName,
		RegionScopeType: regionScopeTypeAllRegions,
	})

	return nil
}

// AttachCustomerManagedPolicyReferenceToPermissionSet attaches a customer-managed policy reference to a permission set.
func (b *InMemoryBackend) AttachCustomerManagedPolicyReferenceToPermissionSet(
	instanceArn, permissionSetArn, name, path string,
) error {
	b.mu.Lock("AttachCustomerManagedPolicyReferenceToPermissionSet")
	defer b.mu.Unlock()

	if err := validateCustomerManagedPolicyReference(name, path); err != nil {
		return err
	}

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	for _, ref := range b.customerManagedPolicies[permissionSetArn] {
		if ref.Name == name && ref.Path == path {
			return nil
		}
	}
	b.customerManagedPolicies[permissionSetArn] = append(
		b.customerManagedPolicies[permissionSetArn],
		CustomerManagedPolicyReference{Name: name, Path: path},
	)

	return nil
}

// CreateApplication creates a new application within an SSO instance.
func (b *InMemoryBackend) CreateApplication(
	instanceArn, applicationProviderArn, name, description string,
	tags map[string]string,
	portalOptions *PortalOptions,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if err := validateApplicationProviderArn(applicationProviderArn); err != nil {
		return nil, err
	}

	if _, ok := b.instances[instanceArn]; !ok {
		return nil, ErrInstanceNotFound
	}
	for _, app := range b.applications {
		if app.InstanceArn == instanceArn && app.Name == name {
			return nil, ErrApplicationAlreadyExists
		}
	}

	if portalOptions != nil {
		if portalOptions.Visibility != "" &&
			portalOptions.Visibility != portalVisibilityEnabled &&
			portalOptions.Visibility != portalVisibilityDisabled {
			return nil, fmt.Errorf(
				"%w: PortalOptions.Visibility must be ENABLED or DISABLED",
				awserr.ErrInvalidParameter,
			)
		}
		if portalOptions.SignInOptions.Origin != "" &&
			portalOptions.SignInOptions.Origin != signInOriginApplication &&
			portalOptions.SignInOptions.Origin != signInOriginIdentityCenter {
			return nil, fmt.Errorf("%w: SignInOptions.Origin must be APPLICATION or IDENTITY_CENTER",
				awserr.ErrInvalidParameter)
		}
		if portalOptions.SignInOptions.Origin == signInOriginApplication &&
			portalOptions.SignInOptions.ApplicationURL == "" {
			return nil, fmt.Errorf("%w: SignInOptions.ApplicationUrl is required when Origin is APPLICATION",
				awserr.ErrInvalidParameter)
		}
	}

	id := uuid.NewString()[:uuidShortLen]
	instanceID := instanceARNToID(instanceArn)
	appArn := fmt.Sprintf("arn:aws:sso::%s:application/%s/apl-%s", b.accountID, instanceID, id)
	app := &Application{
		ApplicationArn:         appArn,
		ApplicationProviderArn: applicationProviderArn,
		CreatedDate:            time.Now().UTC(),
		Description:            description,
		InstanceArn:            instanceArn,
		Name:                   name,
		Status:                 appStatusEnabled,
		Tags:                   make(map[string]string),
		PortalOptions:          portalOptions,
	}
	maps.Copy(app.Tags, tags)
	b.applications[appArn] = app

	return copyApplication(app), nil
}

// DescribeApplication returns an application by ARN.
func (b *InMemoryBackend) DescribeApplication(applicationArn string) (*Application, error) {
	b.mu.RLock("DescribeApplication")
	defer b.mu.RUnlock()

	app, ok := b.applications[applicationArn]
	if !ok {
		return nil, ErrApplicationNotFound
	}

	return copyApplication(app), nil
}

// ListApplications returns applications for an instance.
func (b *InMemoryBackend) ListApplications(instanceArn string) []*Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	result := make([]*Application, 0, len(b.applications))
	for _, app := range b.applications {
		if instanceArn != "" && app.InstanceArn != instanceArn {
			continue
		}
		result = append(result, copyApplication(app))
	}

	return result
}

// UpdateApplication updates mutable fields on an application.
func (b *InMemoryBackend) UpdateApplication(
	applicationArn, name, description, status string,
	portalOptions *PortalOptions,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	if status != "" && status != appStatusEnabled && status != appStatusDisabled {
		return nil, fmt.Errorf("%w: Application Status must be ENABLED or DISABLED", awserr.ErrInvalidParameter)
	}

	app, ok := b.applications[applicationArn]
	if !ok {
		return nil, ErrApplicationNotFound
	}
	if name != "" {
		app.Name = name
	}
	if description != "" {
		app.Description = description
	}
	if status != "" {
		app.Status = status
	}
	if portalOptions != nil {
		app.PortalOptions = portalOptions
	}

	return copyApplication(app), nil
}

// DeleteApplication deletes an application.
func (b *InMemoryBackend) DeleteApplication(applicationArn string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	delete(b.applications, applicationArn)
	delete(b.applicationAssignments, applicationArn)
	delete(b.applicationScopes, applicationArn)
	delete(b.applicationAuthMethods, applicationArn)
	delete(b.applicationGrants, applicationArn)
	delete(b.applicationAssignConfig, applicationArn)
	delete(b.applicationSessions, applicationArn)

	return nil
}

// CreateApplicationAssignment assigns a principal to an application.
func (b *InMemoryBackend) CreateApplicationAssignment(applicationArn, principalID, principalType string) error {
	b.mu.Lock("CreateApplicationAssignment")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	for _, a := range b.applicationAssignments[applicationArn] {
		if a.PrincipalID == principalID && a.PrincipalType == principalType {
			return nil
		}
	}
	b.applicationAssignments[applicationArn] = append(
		b.applicationAssignments[applicationArn],
		&ApplicationAssignment{
			ApplicationArn: applicationArn,
			PrincipalID:    principalID,
			PrincipalType:  principalType,
		},
	)

	return nil
}

// DescribeApplicationAssignment returns a specific application assignment.
func (b *InMemoryBackend) DescribeApplicationAssignment(
	applicationArn,
	principalID,
	principalType string,
) (*ApplicationAssignment, error) {
	b.mu.RLock("DescribeApplicationAssignment")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return nil, ErrApplicationNotFound
	}
	for _, assignment := range b.applicationAssignments[applicationArn] {
		if assignment.PrincipalID == principalID && assignment.PrincipalType == principalType {
			cp := *assignment

			return &cp, nil
		}
	}

	return nil, ErrAssignmentNotFound
}

// ListApplicationAssignments returns assignments for an application.
func (b *InMemoryBackend) ListApplicationAssignments(applicationArn string) ([]*ApplicationAssignment, error) {
	b.mu.RLock("ListApplicationAssignments")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return nil, ErrApplicationNotFound
	}

	assignments := b.applicationAssignments[applicationArn]
	result := make([]*ApplicationAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		cp := *assignment
		result = append(result, &cp)
	}

	return result, nil
}

// DeleteApplicationAssignment removes a principal assignment from an application.
func (b *InMemoryBackend) DeleteApplicationAssignment(applicationArn, principalID, principalType string) error {
	b.mu.Lock("DeleteApplicationAssignment")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	all := b.applicationAssignments[applicationArn]
	found := false
	var remaining []*ApplicationAssignment
	for _, a := range all {
		if a.PrincipalID == principalID && a.PrincipalType == principalType {
			found = true
		} else {
			remaining = append(remaining, a)
		}
	}
	if !found {
		return ErrAssignmentNotFound
	}
	b.applicationAssignments[applicationArn] = remaining

	return nil
}

// PutApplicationAssignmentConfiguration sets assignment configuration on an application.
func (b *InMemoryBackend) PutApplicationAssignmentConfiguration(applicationArn string, assignmentRequired bool) error {
	b.mu.Lock("PutApplicationAssignmentConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	b.applicationAssignConfig[applicationArn] = assignmentRequired

	return nil
}

// DeleteApplicationAccessScope removes an access scope from an application.
func (b *InMemoryBackend) DeleteApplicationAccessScope(applicationArn, scope string) error {
	b.mu.Lock("DeleteApplicationAccessScope")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	all := b.applicationScopes[applicationArn]
	found := false
	var remaining []string
	for _, s := range all {
		if s == scope {
			found = true
		} else {
			remaining = append(remaining, s)
		}
	}
	if !found {
		return ErrAccessScopeNotFound
	}
	b.applicationScopes[applicationArn] = remaining

	return nil
}

// PutApplicationAccessScope adds an access scope to an application.
func (b *InMemoryBackend) PutApplicationAccessScope(applicationArn, scope string) error {
	b.mu.Lock("PutApplicationAccessScope")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	if slices.Contains(b.applicationScopes[applicationArn], scope) {
		return nil
	}
	b.applicationScopes[applicationArn] = append(b.applicationScopes[applicationArn], scope)

	return nil
}

// ListApplicationAccessScopes lists access scopes on an application.
func (b *InMemoryBackend) ListApplicationAccessScopes(applicationArn string) ([]string, error) {
	b.mu.RLock("ListApplicationAccessScopes")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return nil, ErrApplicationNotFound
	}

	return slices.Clone(b.applicationScopes[applicationArn]), nil
}

// AuthMethod holds a typed authentication method with its full structured body.
type AuthMethod struct {
	AuthMethodType string          `json:"AuthenticationMethodType"`
	Body           json.RawMessage `json:"AuthenticationMethod"`
}

// DeleteApplicationAuthenticationMethod removes an authentication method from an application.
func (b *InMemoryBackend) DeleteApplicationAuthenticationMethod(applicationArn, authMethodType string) error {
	b.mu.Lock("DeleteApplicationAuthenticationMethod")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	methods := b.applicationAuthMethods[applicationArn]
	if methods == nil {
		return ErrAuthMethodNotFound
	}
	if _, exists := methods[authMethodType]; !exists {
		return ErrAuthMethodNotFound
	}
	delete(methods, authMethodType)

	return nil
}

// PutApplicationAuthenticationMethod stores a typed authentication method with its body.
// authMethodType must be IAM; body is the full AuthenticationMethod JSON object.
func (b *InMemoryBackend) PutApplicationAuthenticationMethod(
	applicationArn, authMethodType string,
	body json.RawMessage,
) error {
	b.mu.Lock("PutApplicationAuthenticationMethod")
	defer b.mu.Unlock()

	if authMethodType != authMethodTypeIAM {
		return fmt.Errorf("%w: AuthenticationMethodType must be IAM", awserr.ErrInvalidParameter)
	}

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	if b.applicationAuthMethods[applicationArn] == nil {
		b.applicationAuthMethods[applicationArn] = make(map[string]json.RawMessage)
	}
	if len(body) == 0 {
		body = json.RawMessage("null")
	}
	b.applicationAuthMethods[applicationArn][authMethodType] = body

	return nil
}

// ListApplicationAuthenticationMethods lists authentication methods with their bodies.
func (b *InMemoryBackend) ListApplicationAuthenticationMethods(applicationArn string) ([]AuthMethod, error) {
	b.mu.RLock("ListApplicationAuthenticationMethods")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return nil, ErrApplicationNotFound
	}
	methods := b.applicationAuthMethods[applicationArn]
	result := make([]AuthMethod, 0, len(methods))
	for mType, body := range methods {
		bodyCopy := make(json.RawMessage, len(body))
		copy(bodyCopy, body)
		result = append(result, AuthMethod{AuthMethodType: mType, Body: bodyCopy})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].AuthMethodType < result[j].AuthMethodType })

	return result, nil
}

// GetApplicationAuthenticationMethod returns a specific auth method body.
func (b *InMemoryBackend) GetApplicationAuthenticationMethod(
	applicationArn, authMethodType string,
) (json.RawMessage, error) {
	b.mu.RLock("GetApplicationAuthenticationMethod")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return nil, ErrApplicationNotFound
	}
	methods := b.applicationAuthMethods[applicationArn]
	if methods == nil {
		return nil, ErrAuthMethodNotFound
	}
	body, exists := methods[authMethodType]
	if !exists {
		return nil, ErrAuthMethodNotFound
	}
	result := make(json.RawMessage, len(body))
	copy(result, body)

	return result, nil
}

// ApplicationGrant holds a typed grant with its structured body.
type ApplicationGrant struct {
	GrantType string          `json:"GrantType"`
	Grant     json.RawMessage `json:"Grant"`
}

// isValidGrantType reports whether grantType is an AWS-specified grant type enum value.
func isValidGrantType(grantType string) bool {
	switch grantType {
	case "authorization_code",
		"refresh_token",
		"urn:ietf:params:oauth:grant-type:jwt-bearer",
		"urn:ietf:params:oauth:grant-type:token-exchange":
		return true
	default:
		return false
	}
}

// PutApplicationGrant stores a typed grant with its full body.
func (b *InMemoryBackend) PutApplicationGrant(applicationArn, grantType string, body json.RawMessage) error {
	b.mu.Lock("PutApplicationGrant")
	defer b.mu.Unlock()

	if !isValidGrantType(grantType) {
		return fmt.Errorf("%w: GrantType must be one of authorization_code, refresh_token, jwt-bearer, token-exchange",
			awserr.ErrInvalidParameter)
	}

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	if b.applicationGrants[applicationArn] == nil {
		b.applicationGrants[applicationArn] = make(map[string]json.RawMessage)
	}
	if len(body) == 0 {
		body = json.RawMessage("null")
	}
	b.applicationGrants[applicationArn][grantType] = body

	return nil
}

// DeleteApplicationGrant removes a grant from an application.
func (b *InMemoryBackend) DeleteApplicationGrant(applicationArn, grantType string) error {
	b.mu.Lock("DeleteApplicationGrant")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	grants := b.applicationGrants[applicationArn]
	if grants == nil {
		return ErrGrantNotFound
	}
	if _, exists := grants[grantType]; !exists {
		return ErrGrantNotFound
	}
	delete(grants, grantType)

	return nil
}

// ListApplicationGrants lists grants on an application with their bodies.
func (b *InMemoryBackend) ListApplicationGrants(applicationArn string) ([]ApplicationGrant, error) {
	b.mu.RLock("ListApplicationGrants")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return nil, ErrApplicationNotFound
	}
	grants := b.applicationGrants[applicationArn]
	result := make([]ApplicationGrant, 0, len(grants))
	for gType, body := range grants {
		bodyCopy := make(json.RawMessage, len(body))
		copy(bodyCopy, body)
		result = append(result, ApplicationGrant{GrantType: gType, Grant: bodyCopy})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].GrantType < result[j].GrantType })

	return result, nil
}

// GetApplicationGrant returns a specific grant body.
func (b *InMemoryBackend) GetApplicationGrant(applicationArn, grantType string) (json.RawMessage, error) {
	b.mu.RLock("GetApplicationGrant")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return nil, ErrApplicationNotFound
	}
	grants := b.applicationGrants[applicationArn]
	if grants == nil {
		return nil, ErrGrantNotFound
	}
	body, exists := grants[grantType]
	if !exists {
		return nil, ErrGrantNotFound
	}
	result := make(json.RawMessage, len(body))
	copy(result, body)

	return result, nil
}

// PutApplicationSessionConfiguration sets session configuration on an application.
func (b *InMemoryBackend) PutApplicationSessionConfiguration(applicationArn, sessionDuration string) error {
	b.mu.Lock("PutApplicationSessionConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	b.applicationSessions[applicationArn] = sessionDuration

	return nil
}

// CreateInstanceAccessControlAttributeConfiguration creates ABAC configuration for an SSO instance.
// Returns ConflictException if a configuration already exists (use Update to modify).
func (b *InMemoryBackend) CreateInstanceAccessControlAttributeConfiguration(
	instanceArn string,
	attributes []AccessControlAttribute,
) error {
	b.mu.Lock("CreateInstanceAccessControlAttributeConfiguration")
	defer b.mu.Unlock()

	if err := validateACAAttributes(attributes); err != nil {
		return err
	}

	if _, ok := b.instances[instanceArn]; !ok {
		return ErrInstanceNotFound
	}
	if _, exists := b.instanceACAs[instanceArn]; exists {
		return ErrACAAlreadyExists
	}
	b.instanceACAs[instanceArn] = &ABACConfig{
		AccessControlAttributes: copyAccessControlAttributes(attributes),
		Status:                  abacStatusCreationInProgress,
	}

	return nil
}

// DescribeInstanceAccessControlAttributeConfiguration returns ABAC configuration for an instance.
func (b *InMemoryBackend) DescribeInstanceAccessControlAttributeConfiguration(
	instanceArn string,
) (*ABACConfig, error) {
	b.mu.Lock("DescribeInstanceAccessControlAttributeConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return nil, ErrInstanceNotFound
	}
	cfg, ok := b.instanceACAs[instanceArn]
	if !ok {
		return nil, ErrRequestNotFound
	}
	// Lazily transition CREATION_IN_PROGRESS → ENABLED on first describe.
	if cfg.Status == abacStatusCreationInProgress {
		cfg.Status = abacStatusEnabled
	}

	return &ABACConfig{
		AccessControlAttributes: copyAccessControlAttributes(cfg.AccessControlAttributes),
		Status:                  cfg.Status,
		StatusReason:            cfg.StatusReason,
	}, nil
}

// DeleteInstanceAccessControlAttributeConfiguration deletes ABAC configuration for an instance.
func (b *InMemoryBackend) DeleteInstanceAccessControlAttributeConfiguration(instanceArn string) error {
	b.mu.Lock("DeleteInstanceAccessControlAttributeConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return ErrInstanceNotFound
	}
	if _, ok := b.instanceACAs[instanceArn]; !ok {
		return ErrRequestNotFound
	}
	delete(b.instanceACAs, instanceArn)

	return nil
}

// awsApplicationProviderCatalog is the static catalog of AWS-managed SSO application providers.
//
//nolint:gochecknoglobals // read-only constant table
var awsApplicationProviderCatalog = []*ApplicationProvider{
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/custom",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "Custom SAML 2.0 application",
			Description: "Connect any SAML 2.0 compatible application",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/salesforce",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "Salesforce",
			Description: "Customer relationship management platform",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/jira-cloud",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "Jira Cloud",
			Description: "Project and issue tracking by Atlassian",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/slack",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "Slack",
			Description: "Team messaging and collaboration",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/github",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "GitHub",
			Description: "Code hosting and version control",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/datadog",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "Datadog",
			Description: "Monitoring and analytics platform",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/pagerduty",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "PagerDuty",
			Description: "Incident management platform",
		},
	},
}

// awsProvidersByARN provides O(1) lookup into the catalog.
//
//nolint:gochecknoglobals // read-only constant table, built from awsApplicationProviderCatalog
var awsProvidersByARN = func() map[string]*ApplicationProvider {
	m := make(map[string]*ApplicationProvider, len(awsApplicationProviderCatalog))
	for _, p := range awsApplicationProviderCatalog {
		m[p.ApplicationProviderArn] = p
	}

	return m
}()

// ListApplicationProviders returns the static AWS-managed provider catalog.
func (b *InMemoryBackend) ListApplicationProviders() []*ApplicationProvider {
	result := make([]*ApplicationProvider, len(awsApplicationProviderCatalog))
	for i, p := range awsApplicationProviderCatalog {
		cp := *p
		result[i] = &cp
	}

	return result
}

// DescribeApplicationProvider returns details for an application provider.
// Account-scoped custom provider ARNs (arn:aws:sso::<accountId>:applicationProvider/custom)
// are resolved to the AWS-managed custom provider entry.
func (b *InMemoryBackend) DescribeApplicationProvider(
	applicationProviderArn string,
) (*ApplicationProvider, error) {
	if p, ok := awsProvidersByARN[applicationProviderArn]; ok {
		cp := *p

		return &cp, nil
	}
	// Account-scoped ARN: try matching by provider path suffix.
	if idx := strings.LastIndex(applicationProviderArn, ":applicationProvider/"); idx >= 0 {
		suffix := applicationProviderArn[idx+len(":applicationProvider/"):]
		canonicalArn := "arn:aws:sso::aws:applicationProvider/" + suffix
		if p, ok := awsProvidersByARN[canonicalArn]; ok {
			cp := *p

			return &cp, nil
		}
	}

	return nil, ErrRequestNotFound
}

// CreateTrustedTokenIssuer creates a trusted token issuer within an SSO instance.
func (b *InMemoryBackend) CreateTrustedTokenIssuer(
	instanceArn, name, issuerType string,
	tags map[string]string,
	cfg *TrustedTokenIssuerConfiguration,
) (*TrustedTokenIssuer, error) {
	b.mu.Lock("CreateTrustedTokenIssuer")
	defer b.mu.Unlock()

	// Default to OIDC_JWT if not specified.
	if issuerType == "" {
		issuerType = ttiTypeOIDCJWT
	}
	if err := validateTrustedTokenIssuerType(issuerType); err != nil {
		return nil, err
	}
	var oidcCfg *OidcJwtConfiguration
	if cfg != nil {
		oidcCfg = cfg.OidcJwtConfiguration
	}
	if err := validateOIDCJWTConfig(oidcCfg); err != nil {
		return nil, err
	}

	if _, ok := b.instances[instanceArn]; !ok {
		return nil, ErrInstanceNotFound
	}
	for _, ti := range b.trustedTokenIssuers {
		if ti.InstanceArn == instanceArn && ti.Name == name {
			return nil, ErrTrustedTokenIssuerAlreadyExists
		}
	}

	if tags != nil {
		if err := validateTags(tags); err != nil {
			return nil, err
		}
	}

	id := uuid.NewString()[:uuidShortLen]
	instanceID := instanceARNToID(instanceArn)
	arn := fmt.Sprintf("arn:aws:sso::%s:trustedTokenIssuer/%s/tti-%s", b.accountID, instanceID, id)
	ti := &TrustedTokenIssuer{
		TrustedTokenIssuerArn:           arn,
		InstanceArn:                     instanceArn,
		Name:                            name,
		TrustedTokenIssuerType:          issuerType,
		Tags:                            make(map[string]string),
		TrustedTokenIssuerConfiguration: cfg,
	}
	if tags != nil {
		maps.Copy(ti.Tags, tags)
	}
	b.trustedTokenIssuers[arn] = ti

	return copyTrustedTokenIssuer(ti), nil
}

// DeleteTrustedTokenIssuer deletes a trusted token issuer.
func (b *InMemoryBackend) DeleteTrustedTokenIssuer(trustedTokenIssuerArn string) error {
	b.mu.Lock("DeleteTrustedTokenIssuer")
	defer b.mu.Unlock()

	if _, ok := b.trustedTokenIssuers[trustedTokenIssuerArn]; !ok {
		return ErrTrustedTokenIssuerNotFound
	}
	delete(b.trustedTokenIssuers, trustedTokenIssuerArn)

	return nil
}

// DescribeTrustedTokenIssuer returns a trusted token issuer.
func (b *InMemoryBackend) DescribeTrustedTokenIssuer(
	trustedTokenIssuerArn string,
) (*TrustedTokenIssuer, error) {
	b.mu.RLock("DescribeTrustedTokenIssuer")
	defer b.mu.RUnlock()

	issuer, ok := b.trustedTokenIssuers[trustedTokenIssuerArn]
	if !ok {
		return nil, ErrTrustedTokenIssuerNotFound
	}

	return copyTrustedTokenIssuer(issuer), nil
}

// ListTrustedTokenIssuers lists trusted token issuers for an instance.
func (b *InMemoryBackend) ListTrustedTokenIssuers(instanceArn string) []*TrustedTokenIssuer {
	b.mu.RLock("ListTrustedTokenIssuers")
	defer b.mu.RUnlock()

	result := make([]*TrustedTokenIssuer, 0, len(b.trustedTokenIssuers))
	for _, issuer := range b.trustedTokenIssuers {
		if instanceArn != "" && issuer.InstanceArn != instanceArn {
			continue
		}
		result = append(result, copyTrustedTokenIssuer(issuer))
	}

	return result
}

// UpdateTrustedTokenIssuer updates mutable trusted token issuer fields.
func (b *InMemoryBackend) UpdateTrustedTokenIssuer(
	trustedTokenIssuerArn,
	name,
	issuerType string,
	cfg *TrustedTokenIssuerConfiguration,
) (*TrustedTokenIssuer, error) {
	b.mu.Lock("UpdateTrustedTokenIssuer")
	defer b.mu.Unlock()

	if issuerType != "" {
		if err := validateTrustedTokenIssuerType(issuerType); err != nil {
			return nil, err
		}
	}
	if cfg != nil {
		if err := validateOIDCJWTConfig(cfg.OidcJwtConfiguration); err != nil {
			return nil, err
		}
	}

	issuer, ok := b.trustedTokenIssuers[trustedTokenIssuerArn]
	if !ok {
		return nil, ErrTrustedTokenIssuerNotFound
	}
	if name != "" {
		issuer.Name = name
	}
	if issuerType != "" {
		issuer.TrustedTokenIssuerType = issuerType
	}
	if cfg != nil {
		issuer.TrustedTokenIssuerConfiguration = cfg
	}

	return copyTrustedTokenIssuer(issuer), nil
}

// GetApplicationAssignmentConfiguration returns the assignment configuration for an application.
func (b *InMemoryBackend) GetApplicationAssignmentConfiguration(applicationArn string) (bool, error) {
	b.mu.RLock("GetApplicationAssignmentConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return false, ErrApplicationNotFound
	}
	required := b.applicationAssignConfig[applicationArn]

	return required, nil
}

// GetApplicationSessionConfiguration returns the session configuration for an application.
func (b *InMemoryBackend) GetApplicationSessionConfiguration(applicationArn string) (string, error) {
	b.mu.RLock("GetApplicationSessionConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return "", ErrApplicationNotFound
	}
	dur := b.applicationSessions[applicationArn]

	return dur, nil
}

// DeletePermissionsBoundaryFromPermissionSet removes the permissions boundary from a permission set.
func (b *InMemoryBackend) DeletePermissionsBoundaryFromPermissionSet(instanceArn, permissionSetArn string) error {
	b.mu.Lock("DeletePermissionsBoundaryFromPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	if _, hasBoundary := b.permissionBoundaries[permissionSetArn]; !hasBoundary {
		return ErrRequestNotFound
	}
	delete(b.permissionBoundaries, permissionSetArn)

	return nil
}

// ListCustomerManagedPolicyReferencesInPermissionSet lists customer-managed policy references in a permission set.
func (b *InMemoryBackend) ListCustomerManagedPolicyReferencesInPermissionSet(
	instanceArn,
	permissionSetArn string,
) ([]CustomerManagedPolicyReference, error) {
	b.mu.RLock("ListCustomerManagedPolicyReferencesInPermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return nil, ErrPermissionSetNotFound
	}
	refs := b.customerManagedPolicies[permissionSetArn]
	result := make([]CustomerManagedPolicyReference, len(refs))
	copy(result, refs)

	return result, nil
}

// RemoveRegion removes a region from an SSO instance.
func (b *InMemoryBackend) RemoveRegion(instanceArn, regionName string) error {
	b.mu.Lock("RemoveRegion")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return ErrInstanceNotFound
	}
	regions := b.instanceRegions[instanceArn]
	found := false
	remaining := make([]RegionMetadata, 0, len(regions))
	for _, r := range regions {
		if r.Region == regionName {
			found = true
		} else {
			remaining = append(remaining, r)
		}
	}
	if !found {
		return ErrRequestNotFound
	}
	b.instanceRegions[instanceArn] = remaining

	return nil
}

// ListRegions returns the regions associated with an SSO instance.
func (b *InMemoryBackend) ListRegions(instanceArn string) ([]RegionMetadata, error) {
	b.mu.RLock("ListRegions")
	defer b.mu.RUnlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return nil, ErrInstanceNotFound
	}
	regions := b.instanceRegions[instanceArn]
	result := make([]RegionMetadata, len(regions))
	copy(result, regions)

	return result, nil
}

// UpdateInstance updates the name of an SSO instance.
func (b *InMemoryBackend) UpdateInstance(instanceArn, name string) error {
	b.mu.Lock("UpdateInstance")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceArn]
	if !ok {
		return ErrInstanceNotFound
	}
	if name != "" {
		inst.Name = name
	}

	return nil
}

// UpdateInstanceAccessControlAttributeConfiguration updates the ABAC config for an instance.
func (b *InMemoryBackend) UpdateInstanceAccessControlAttributeConfiguration(
	instanceArn string,
	attributes []AccessControlAttribute,
) error {
	b.mu.Lock("UpdateInstanceAccessControlAttributeConfiguration")
	defer b.mu.Unlock()

	if err := validateACAAttributes(attributes); err != nil {
		return err
	}

	if _, ok := b.instances[instanceArn]; !ok {
		return ErrInstanceNotFound
	}
	existing := b.instanceACAs[instanceArn]
	status := abacStatusEnabled
	if existing != nil {
		status = existing.Status
	}
	b.instanceACAs[instanceArn] = &ABACConfig{
		AccessControlAttributes: copyAccessControlAttributes(attributes),
		Status:                  status,
	}

	return nil
}

// DetachCustomerManagedPolicyReferenceFromPermissionSet detaches a customer-managed policy reference.
func (b *InMemoryBackend) DetachCustomerManagedPolicyReferenceFromPermissionSet(
	instanceArn, permissionSetArn, name, path string,
) error {
	b.mu.Lock("DetachCustomerManagedPolicyReferenceFromPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	all := b.customerManagedPolicies[permissionSetArn]
	found := false
	remaining := make([]CustomerManagedPolicyReference, 0, len(all))
	for _, ref := range all {
		if ref.Name == name && ref.Path == path {
			found = true
		} else {
			remaining = append(remaining, ref)
		}
	}
	if !found {
		return ErrRequestNotFound
	}
	b.customerManagedPolicies[permissionSetArn] = remaining

	return nil
}

// ListPermissionSetsProvisionedToAccount returns the permission set ARNs provisioned to a specific account.
func (b *InMemoryBackend) ListPermissionSetsProvisionedToAccount(instanceArn, accountID string) []string {
	b.mu.RLock("ListPermissionSetsProvisionedToAccount")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}
	for key, assignments := range b.assignments {
		// key format: instanceArn|permissionSetArn
		if !strings.HasPrefix(key, instanceArn+"|") {
			continue
		}
		for _, a := range assignments {
			if a.AccountID == accountID {
				psArn := strings.TrimPrefix(key, instanceArn+"|")
				seen[psArn] = struct{}{}
			}
		}
	}
	result := collections.SortedKeys(seen)

	return result
}

// ListAccountAssignmentsForPrincipal returns account assignments for a specific principal.
func (b *InMemoryBackend) ListAccountAssignmentsForPrincipal(
	instanceArn, principalID, principalType string,
) []*AccountAssignment {
	b.mu.RLock("ListAccountAssignmentsForPrincipal")
	defer b.mu.RUnlock()

	var result []*AccountAssignment
	for key, assignments := range b.assignments {
		if !strings.HasPrefix(key, instanceArn+"|") {
			continue
		}
		for _, a := range assignments {
			if a.PrincipalID == principalID && a.PrincipalType == principalType {
				cp := *a
				result = append(result, &cp)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].AccountID != result[j].AccountID {
			return result[i].AccountID < result[j].AccountID
		}

		return result[i].PermissionSetArn < result[j].PermissionSetArn
	})

	return result
}

// GetApplicationAccessScope returns whether a specific access scope exists for an application.
func (b *InMemoryBackend) GetApplicationAccessScope(applicationArn, scope string) (string, error) {
	b.mu.RLock("GetApplicationAccessScope")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return "", ErrApplicationNotFound
	}
	if slices.Contains(b.applicationScopes[applicationArn], scope) {
		return scope, nil
	}

	return "", ErrAccessScopeNotFound
}

// GetApplicationAuthenticationMethod and GetApplicationGrant are implemented earlier in this file
// with json.RawMessage return types (items 20–21 of the AWS-accuracy audit).

// ListAccountsForProvisionedPermissionSet returns account IDs where a permission set has assignments.
func (b *InMemoryBackend) ListAccountsForProvisionedPermissionSet(
	instanceArn, permissionSetArn string,
) ([]string, error) {
	b.mu.RLock("ListAccountsForProvisionedPermissionSet")
	defer b.mu.RUnlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return nil, ErrInstanceNotFound
	}
	if _, ok := b.permissionSets[permissionSetArn]; !ok {
		return nil, ErrPermissionSetNotFound
	}

	key := assignmentKey(instanceArn, permissionSetArn)
	seen := map[string]struct{}{}
	for _, a := range b.assignments[key] {
		seen[a.AccountID] = struct{}{}
	}

	result := collections.SortedKeys(seen)

	return result, nil
}

// ListApplicationAssignmentsForPrincipal returns all application assignments for a specific principal.
func (b *InMemoryBackend) ListApplicationAssignmentsForPrincipal(
	instanceArn, principalID, principalType string,
) []*ApplicationAssignment {
	b.mu.RLock("ListApplicationAssignmentsForPrincipal")
	defer b.mu.RUnlock()

	var result []*ApplicationAssignment
	for appArn, app := range b.applications {
		if app.InstanceArn != instanceArn {
			continue
		}
		for _, a := range b.applicationAssignments[appArn] {
			if a.PrincipalID == principalID && a.PrincipalType == principalType {
				cp := *a
				result = append(result, &cp)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ApplicationArn < result[j].ApplicationArn
	})

	return result
}

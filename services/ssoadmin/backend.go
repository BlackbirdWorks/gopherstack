package ssoadmin

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	identityStoreIDPrefixLen = 8
	identityStoreIDMaxLen    = 12
	uuidShortLen             = 8

	// Instance/application status constants.
	instanceStatusActive = "ACTIVE"
	appStatusEnabled     = "ENABLED"

	// Default session duration for new permission sets.
	defaultSessionDuration = "PT1H"
)

var (
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)

	ErrInstanceNotFound                = errors.New("ResourceNotFoundException")
	ErrPermissionSetNotFound           = errors.New("ResourceNotFoundException")
	ErrPermissionSetAlreadyExists      = errors.New("ConflictException")
	ErrAssignmentNotFound              = errors.New("ResourceNotFoundException")
	ErrRequestNotFound                 = errors.New("ResourceNotFoundException")
	ErrApplicationNotFound             = errors.New("ResourceNotFoundException")
	ErrApplicationAlreadyExists        = errors.New("ConflictException")
	ErrTrustedTokenIssuerNotFound      = errors.New("ResourceNotFoundException")
	ErrTrustedTokenIssuerAlreadyExists = errors.New("ConflictException")
	ErrAccessScopeNotFound             = errors.New("ResourceNotFoundException")
	ErrAuthMethodNotFound              = errors.New("ResourceNotFoundException")
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

// ProvisioningStatus represents the status of an async provisioning request.
type ProvisioningStatus struct {
	CreatedDate   time.Time `json:"CreatedDate"`
	RequestID     string    `json:"RequestId"`
	Status        string    `json:"Status"`
	FailureReason string    `json:"FailureReason"`
}

// CustomerManagedPolicyReference references a customer-managed policy.
type CustomerManagedPolicyReference struct {
	Name string `json:"Name"`
	Path string `json:"Path"`
}

// Application represents an AWS SSO application.
type Application struct {
	CreatedDate            time.Time         `json:"CreatedDate"`
	Tags                   map[string]string `json:"Tags"`
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

// ApplicationProvider represents an SSO application provider.
type ApplicationProvider struct {
	ApplicationProviderArn string `json:"ApplicationProviderArn"`
	DisplayData            string `json:"DisplayData"`
}

// InstanceAccessControlAttributeConfiguration holds ABAC configuration for an instance.
type InstanceAccessControlAttributeConfiguration struct {
	AccessControlAttributes []AccessControlAttribute `json:"AccessControlAttributes"`
}

// TrustedTokenIssuer represents a trusted token issuer.
type TrustedTokenIssuer struct {
	TrustedTokenIssuerArn  string `json:"TrustedTokenIssuerArn"`
	InstanceArn            string `json:"InstanceArn"`
	Name                   string `json:"Name"`
	TrustedTokenIssuerType string `json:"TrustedTokenIssuerType"`
}

// InMemoryBackend is the in-memory backend for the SSO Admin service.
type InMemoryBackend struct {
	instances               map[string]*Instance
	permissionSets          map[string]*PermissionSet
	assignments             map[string][]*AccountAssignment
	creationStatuses        map[string]*ProvisioningStatus
	deletionStatuses        map[string]*ProvisioningStatus
	provisioningStatuses    map[string]*ProvisioningStatus
	instanceRegions         map[string][]string
	customerManagedPolicies map[string][]CustomerManagedPolicyReference
	applications            map[string]*Application
	applicationAssignments  map[string][]*ApplicationAssignment
	applicationScopes       map[string][]string
	applicationAuthMethods  map[string][]string
	applicationGrants       map[string][]string
	applicationAssignConfig map[string]bool
	applicationSessions     map[string]string
	instanceACAs            map[string]*InstanceAccessControlAttributeConfiguration
	trustedTokenIssuers     map[string]*TrustedTokenIssuer
	permissionBoundaries    map[string]string
	mu                      *lockmetrics.RWMutex
	accountID               string
	region                  string
}

// NewInMemoryBackend creates a new in-memory SSO Admin backend with a default instance.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		instances:               make(map[string]*Instance),
		permissionSets:          make(map[string]*PermissionSet),
		assignments:             make(map[string][]*AccountAssignment),
		creationStatuses:        make(map[string]*ProvisioningStatus),
		deletionStatuses:        make(map[string]*ProvisioningStatus),
		provisioningStatuses:    make(map[string]*ProvisioningStatus),
		instanceRegions:         make(map[string][]string),
		customerManagedPolicies: make(map[string][]CustomerManagedPolicyReference),
		applications:            make(map[string]*Application),
		applicationAssignments:  make(map[string][]*ApplicationAssignment),
		applicationScopes:       make(map[string][]string),
		applicationAuthMethods:  make(map[string][]string),
		applicationGrants:       make(map[string][]string),
		applicationAssignConfig: make(map[string]bool),
		applicationSessions:     make(map[string]string),
		instanceACAs:            make(map[string]*InstanceAccessControlAttributeConfiguration),
		trustedTokenIssuers:     make(map[string]*TrustedTokenIssuer),
		permissionBoundaries:    make(map[string]string),
		mu:                      lockmetrics.New("ssoadmin"),
		accountID:               accountID,
		region:                  region,
	}

	// Pre-seed a default instance to mimic AWS SSO behaviour where an instance
	// is always present once SSO is enabled.
	defaultID := "d-0000000001"
	identityStoreID := "d-" + accountID
	if len(identityStoreID) > identityStoreIDMaxLen {
		identityStoreID = identityStoreID[:identityStoreIDMaxLen]
	}
	defaultArn := "arn:aws:sso:::instance/ssoins-" + defaultID
	b.instances[defaultArn] = &Instance{
		InstanceArn:     defaultArn,
		Name:            "default",
		OwnerAccountID:  accountID,
		IdentityStoreID: identityStoreID,
		Status:          instanceStatusActive,
		CreatedDate:     time.Now().UTC(),
	}

	return b
}

// Reset clears all backend state, including the default pre-seeded instance.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.instances = make(map[string]*Instance)
	b.permissionSets = make(map[string]*PermissionSet)
	b.assignments = make(map[string][]*AccountAssignment)
	b.creationStatuses = make(map[string]*ProvisioningStatus)
	b.deletionStatuses = make(map[string]*ProvisioningStatus)
	b.provisioningStatuses = make(map[string]*ProvisioningStatus)
	b.instanceRegions = make(map[string][]string)
	b.customerManagedPolicies = make(map[string][]CustomerManagedPolicyReference)
	b.applications = make(map[string]*Application)
	b.applicationAssignments = make(map[string][]*ApplicationAssignment)
	b.applicationScopes = make(map[string][]string)
	b.applicationAuthMethods = make(map[string][]string)
	b.applicationGrants = make(map[string][]string)
	b.applicationAssignConfig = make(map[string]bool)
	b.applicationSessions = make(map[string]string)
	b.instanceACAs = make(map[string]*InstanceAccessControlAttributeConfiguration)
	b.trustedTokenIssuers = make(map[string]*TrustedTokenIssuer)
	b.permissionBoundaries = make(map[string]string)
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
		Status:          instanceStatusActive,
		CreatedDate:     time.Now().UTC(),
	}
	b.instances[instanceArn] = inst

	cp := *inst

	return &cp, nil
}

// ListInstances returns all SSO instances.
func (b *InMemoryBackend) ListInstances() []*Instance {
	b.mu.RLock("ListInstances")
	defer b.mu.RUnlock()

	list := make([]*Instance, 0, len(b.instances))
	for _, inst := range b.instances {
		cp := *inst
		list = append(list, &cp)
	}

	return list
}

// DescribeInstance returns a specific SSO instance.
func (b *InMemoryBackend) DescribeInstance(instanceArn string) (*Instance, error) {
	b.mu.RLock("DescribeInstance")
	defer b.mu.RUnlock()

	inst, ok := b.instances[instanceArn]
	if !ok {
		return nil, ErrInstanceNotFound
	}

	cp := *inst

	return &cp, nil
}

// DeleteInstance removes an SSO instance and cascades to all dependent resources.
func (b *InMemoryBackend) DeleteInstance(instanceArn string) error {
	b.mu.Lock("DeleteInstance")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return ErrInstanceNotFound
	}
	delete(b.instances, instanceArn)

	// Cascade: remove all permission sets and their assignments for this instance.
	for psArn, ps := range b.permissionSets {
		if ps.InstanceArn == instanceArn {
			key := assignmentKey(instanceArn, psArn)
			delete(b.assignments, key)
			delete(b.customerManagedPolicies, psArn)
			delete(b.permissionBoundaries, psArn)
			delete(b.permissionSets, psArn)
		}
	}

	// Cascade: remove ACA configuration and region list for this instance.
	delete(b.instanceACAs, instanceArn)
	delete(b.instanceRegions, instanceArn)

	// Cascade: remove all applications belonging to this instance.
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

	// Cascade: remove all trusted token issuers for this instance.
	for ttiArn, tti := range b.trustedTokenIssuers {
		if tti.InstanceArn == instanceArn {
			delete(b.trustedTokenIssuers, ttiArn)
		}
	}

	return nil
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

// DeletePermissionSet removes a permission set.
func (b *InMemoryBackend) DeletePermissionSet(instanceArn, permissionSetArn string) error {
	b.mu.Lock("DeletePermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	delete(b.permissionSets, permissionSetArn)
	delete(b.permissionBoundaries, permissionSetArn)

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
	b.assignments[key] = append(b.assignments[key], assignment)

	requestID := uuid.NewString()
	b.creationStatuses[requestID] = &ProvisioningStatus{
		RequestID:   requestID,
		Status:      "SUCCEEDED",
		CreatedDate: time.Now().UTC(),
	}

	return requestID, nil
}

// DescribeAccountAssignmentCreationStatus returns the status of a creation request.
func (b *InMemoryBackend) DescribeAccountAssignmentCreationStatus(
	_ string,
	requestID string,
) (*ProvisioningStatus, error) {
	b.mu.RLock("DescribeAccountAssignmentCreationStatus")
	defer b.mu.RUnlock()

	status, ok := b.creationStatuses[requestID]
	if !ok {
		return nil, ErrRequestNotFound
	}

	cp := *status

	return &cp, nil
}

// ListAccountAssignmentCreationStatus returns creation statuses.
func (b *InMemoryBackend) ListAccountAssignmentCreationStatus(_ string) []*ProvisioningStatus {
	b.mu.RLock("ListAccountAssignmentCreationStatus")
	defer b.mu.RUnlock()

	result := make([]*ProvisioningStatus, 0, len(b.creationStatuses))
	for _, status := range b.creationStatuses {
		cp := *status
		result = append(result, &cp)
	}

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

	requestID := uuid.NewString()
	b.deletionStatuses[requestID] = &ProvisioningStatus{
		RequestID:   requestID,
		Status:      "SUCCEEDED",
		CreatedDate: time.Now().UTC(),
	}

	return requestID, nil
}

// DescribeAccountAssignmentDeletionStatus returns the status of a deletion request.
func (b *InMemoryBackend) DescribeAccountAssignmentDeletionStatus(
	_ string,
	requestID string,
) (*ProvisioningStatus, error) {
	b.mu.RLock("DescribeAccountAssignmentDeletionStatus")
	defer b.mu.RUnlock()

	status, ok := b.deletionStatuses[requestID]
	if !ok {
		return nil, ErrRequestNotFound
	}

	cp := *status

	return &cp, nil
}

// ListAccountAssignmentDeletionStatus returns deletion statuses.
func (b *InMemoryBackend) ListAccountAssignmentDeletionStatus(_ string) []*ProvisioningStatus {
	b.mu.RLock("ListAccountAssignmentDeletionStatus")
	defer b.mu.RUnlock()

	result := make([]*ProvisioningStatus, 0, len(b.deletionStatuses))
	for _, status := range b.deletionStatuses {
		cp := *status
		result = append(result, &cp)
	}

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
func (b *InMemoryBackend) DetachManagedPolicyFromPermissionSet(
	instanceArn, permissionSetArn, managedPolicyArn string,
) error {
	b.mu.Lock("DetachManagedPolicyFromPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	var remaining []ManagedPolicy
	for _, mp := range ps.ManagedPolicies {
		if mp.Arn != managedPolicyArn {
			remaining = append(remaining, mp)
		}
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
func (b *InMemoryBackend) PutPermissionsBoundaryToPermissionSet(
	instanceArn, permissionSetArn, managedPolicyArn string,
) error {
	b.mu.Lock("PutPermissionsBoundaryToPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	b.permissionBoundaries[permissionSetArn] = managedPolicyArn

	return nil
}

// GetPermissionsBoundaryForPermissionSet returns the permissions boundary for a permission set.
func (b *InMemoryBackend) GetPermissionsBoundaryForPermissionSet(
	instanceArn,
	permissionSetArn string,
) (string, error) {
	b.mu.RLock("GetPermissionsBoundaryForPermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return "", ErrPermissionSetNotFound
	}

	boundary, ok := b.permissionBoundaries[permissionSetArn]
	if !ok {
		return "", ErrRequestNotFound
	}

	return boundary, nil
}

// ProvisionPermissionSet initiates provisioning of a permission set to accounts.
func (b *InMemoryBackend) ProvisionPermissionSet(instanceArn, permissionSetArn string) (string, error) {
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
		RequestID:   requestID,
		Status:      "SUCCEEDED",
		CreatedDate: time.Now().UTC(),
	}

	return requestID, nil
}

// DescribePermissionSetProvisioningStatus returns the status of a provisioning request.
func (b *InMemoryBackend) DescribePermissionSetProvisioningStatus(
	_ string,
	provisioningRequestID string,
) (*ProvisioningStatus, error) {
	b.mu.RLock("DescribePermissionSetProvisioningStatus")
	defer b.mu.RUnlock()

	status, ok := b.provisioningStatuses[provisioningRequestID]
	if !ok {
		return nil, ErrRequestNotFound
	}

	cp := *status

	return &cp, nil
}

// ListPermissionSetProvisioningStatus returns permission-set provisioning statuses.
func (b *InMemoryBackend) ListPermissionSetProvisioningStatus(_ string) []*ProvisioningStatus {
	b.mu.RLock("ListPermissionSetProvisioningStatus")
	defer b.mu.RUnlock()

	result := make([]*ProvisioningStatus, 0, len(b.provisioningStatuses))
	for _, status := range b.provisioningStatuses {
		cp := *status
		result = append(result, &cp)
	}

	return result
}

// TagResource adds tags to a resource (permission set, instance, or application).
func (b *InMemoryBackend) TagResource(instanceArn, resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if ps, ok := b.permissionSets[resourceArn]; ok && ps.InstanceArn == instanceArn {
		if ps.Tags == nil {
			ps.Tags = make(map[string]string)
		}
		maps.Copy(ps.Tags, tags)

		return nil
	}

	if inst, ok := b.instances[resourceArn]; ok {
		if inst.Tags == nil {
			inst.Tags = make(map[string]string)
		}
		maps.Copy(inst.Tags, tags)

		return nil
	}

	if app, ok := b.applications[resourceArn]; ok && app.InstanceArn == instanceArn {
		if app.Tags == nil {
			app.Tags = make(map[string]string)
		}
		maps.Copy(app.Tags, tags)

		return nil
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

	if _, ok := b.instances[resourceArn]; ok {
		return nil
	}

	if app, ok := b.applications[resourceArn]; ok && app.InstanceArn == instanceArn {
		for _, k := range tagKeys {
			delete(app.Tags, k)
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
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/custom",
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
	if slices.Contains(b.instanceRegions[instanceArn], regionName) {
		return nil
	}
	b.instanceRegions[instanceArn] = append(b.instanceRegions[instanceArn], regionName)

	return nil
}

// AttachCustomerManagedPolicyReferenceToPermissionSet attaches a customer-managed policy reference to a permission set.
func (b *InMemoryBackend) AttachCustomerManagedPolicyReferenceToPermissionSet(
	instanceArn, permissionSetArn, name, path string,
) error {
	b.mu.Lock("AttachCustomerManagedPolicyReferenceToPermissionSet")
	defer b.mu.Unlock()

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
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return nil, ErrInstanceNotFound
	}
	for _, app := range b.applications {
		if app.InstanceArn == instanceArn && app.Name == name {
			return nil, ErrApplicationAlreadyExists
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
	applicationArn,
	name,
	description,
	status string,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

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

// DeleteApplicationAuthenticationMethod removes an authentication method from an application.
func (b *InMemoryBackend) DeleteApplicationAuthenticationMethod(applicationArn, authMethodType string) error {
	b.mu.Lock("DeleteApplicationAuthenticationMethod")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	all := b.applicationAuthMethods[applicationArn]
	found := false
	var remaining []string
	for _, m := range all {
		if m == authMethodType {
			found = true
		} else {
			remaining = append(remaining, m)
		}
	}
	if !found {
		return ErrAuthMethodNotFound
	}
	b.applicationAuthMethods[applicationArn] = remaining

	return nil
}

// PutApplicationAuthenticationMethod adds an authentication method to an application.
func (b *InMemoryBackend) PutApplicationAuthenticationMethod(applicationArn, authMethodType string) error {
	b.mu.Lock("PutApplicationAuthenticationMethod")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	if slices.Contains(b.applicationAuthMethods[applicationArn], authMethodType) {
		return nil
	}
	b.applicationAuthMethods[applicationArn] = append(b.applicationAuthMethods[applicationArn], authMethodType)

	return nil
}

// ListApplicationAuthenticationMethods lists authentication methods on an application.
func (b *InMemoryBackend) ListApplicationAuthenticationMethods(applicationArn string) ([]string, error) {
	b.mu.RLock("ListApplicationAuthenticationMethods")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return nil, ErrApplicationNotFound
	}

	return slices.Clone(b.applicationAuthMethods[applicationArn]), nil
}

// PutApplicationGrant adds a grant to an application.
func (b *InMemoryBackend) PutApplicationGrant(applicationArn, grantType string) error {
	b.mu.Lock("PutApplicationGrant")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	if slices.Contains(b.applicationGrants[applicationArn], grantType) {
		return nil
	}
	b.applicationGrants[applicationArn] = append(b.applicationGrants[applicationArn], grantType)

	return nil
}

// DeleteApplicationGrant removes a grant from an application.
func (b *InMemoryBackend) DeleteApplicationGrant(applicationArn, grantType string) error {
	b.mu.Lock("DeleteApplicationGrant")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return ErrApplicationNotFound
	}
	all := b.applicationGrants[applicationArn]
	found := false
	var remaining []string
	for _, grant := range all {
		if grant == grantType {
			found = true
		} else {
			remaining = append(remaining, grant)
		}
	}
	if !found {
		return ErrRequestNotFound
	}
	b.applicationGrants[applicationArn] = remaining

	return nil
}

// ListApplicationGrants lists grants on an application.
func (b *InMemoryBackend) ListApplicationGrants(applicationArn string) ([]string, error) {
	b.mu.RLock("ListApplicationGrants")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationArn]; !ok {
		return nil, ErrApplicationNotFound
	}

	return slices.Clone(b.applicationGrants[applicationArn]), nil
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
func (b *InMemoryBackend) CreateInstanceAccessControlAttributeConfiguration(
	instanceArn string,
	attributes []AccessControlAttribute,
) error {
	b.mu.Lock("CreateInstanceAccessControlAttributeConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return ErrInstanceNotFound
	}
	b.instanceACAs[instanceArn] = &InstanceAccessControlAttributeConfiguration{
		AccessControlAttributes: copyAccessControlAttributes(attributes),
	}

	return nil
}

// DescribeInstanceAccessControlAttributeConfiguration returns ABAC configuration for an instance.
func (b *InMemoryBackend) DescribeInstanceAccessControlAttributeConfiguration(
	instanceArn string,
) (*InstanceAccessControlAttributeConfiguration, error) {
	b.mu.RLock("DescribeInstanceAccessControlAttributeConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return nil, ErrInstanceNotFound
	}
	cfg, ok := b.instanceACAs[instanceArn]
	if !ok {
		return nil, ErrRequestNotFound
	}

	return &InstanceAccessControlAttributeConfiguration{
		AccessControlAttributes: copyAccessControlAttributes(cfg.AccessControlAttributes),
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

// ListApplicationProviders returns known application providers.
func (b *InMemoryBackend) ListApplicationProviders() []*ApplicationProvider {
	b.mu.RLock("ListApplicationProviders")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{
		"arn:aws:sso::aws:applicationProvider/custom": {},
	}
	for _, app := range b.applications {
		if app.ApplicationProviderArn == "" {
			continue
		}
		seen[app.ApplicationProviderArn] = struct{}{}
	}

	result := make([]*ApplicationProvider, 0, len(seen))
	for providerArn := range seen {
		result = append(result, &ApplicationProvider{
			ApplicationProviderArn: providerArn,
			DisplayData:            "custom provider",
		})
	}

	return result
}

// DescribeApplicationProvider returns details for an application provider.
func (b *InMemoryBackend) DescribeApplicationProvider(
	applicationProviderArn string,
) (*ApplicationProvider, error) {
	providers := b.ListApplicationProviders()
	for _, provider := range providers {
		if provider.ApplicationProviderArn == applicationProviderArn {
			cp := *provider

			return &cp, nil
		}
	}

	return nil, ErrRequestNotFound
}

// CreateTrustedTokenIssuer creates a trusted token issuer within an SSO instance.
func (b *InMemoryBackend) CreateTrustedTokenIssuer(instanceArn, name, issuerType string) (*TrustedTokenIssuer, error) {
	b.mu.Lock("CreateTrustedTokenIssuer")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return nil, ErrInstanceNotFound
	}
	for _, ti := range b.trustedTokenIssuers {
		if ti.InstanceArn == instanceArn && ti.Name == name {
			return nil, ErrTrustedTokenIssuerAlreadyExists
		}
	}

	id := uuid.NewString()[:uuidShortLen]
	instanceID := instanceARNToID(instanceArn)
	arn := fmt.Sprintf("arn:aws:sso::%s:trustedTokenIssuer/%s/tti-%s", b.accountID, instanceID, id)
	ti := &TrustedTokenIssuer{
		TrustedTokenIssuerArn:  arn,
		InstanceArn:            instanceArn,
		Name:                   name,
		TrustedTokenIssuerType: issuerType,
	}
	b.trustedTokenIssuers[arn] = ti
	cp := *ti

	return &cp, nil
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
	cp := *issuer

	return &cp, nil
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
		cp := *issuer
		result = append(result, &cp)
	}

	return result
}

// UpdateTrustedTokenIssuer updates mutable trusted token issuer fields.
func (b *InMemoryBackend) UpdateTrustedTokenIssuer(
	trustedTokenIssuerArn,
	name,
	issuerType string,
) (*TrustedTokenIssuer, error) {
	b.mu.Lock("UpdateTrustedTokenIssuer")
	defer b.mu.Unlock()

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
	cp := *issuer

	return &cp, nil
}

package ssoadmin

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	identityStoreIDPrefixLen = 8
	identityStoreIDMaxLen    = 12
	uuidShortLen             = 8
)

var (
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
	CreatedDate     time.Time
	IdentityStoreID string
	InstanceArn     string
	Name            string
	OwnerAccountID  string
	Status          string
}

// PermissionSet represents an AWS SSO permission set.
type PermissionSet struct {
	CreatedDate      time.Time
	Tags             map[string]string
	PermissionSetArn string
	InstanceArn      string
	Name             string
	Description      string
	SessionDuration  string
	RelayState       string
	InlinePolicy     string
	ManagedPolicies  []ManagedPolicy
}

// ManagedPolicy represents an IAM managed policy attached to a permission set.
type ManagedPolicy struct {
	Arn  string
	Name string
}

// AccountAssignment represents an assignment of a permission set to a principal in an account.
type AccountAssignment struct {
	AccountID        string
	PermissionSetArn string
	PrincipalID      string
	PrincipalType    string
}

// ProvisioningStatus represents the status of an async provisioning request.
type ProvisioningStatus struct {
	CreatedDate   time.Time
	RequestID     string
	Status        string
	FailureReason string
}

// CustomerManagedPolicyReference references a customer-managed policy.
type CustomerManagedPolicyReference struct {
	Name string
	Path string
}

// Application represents an AWS SSO application.
type Application struct {
	CreatedDate            time.Time
	Tags                   map[string]string
	ApplicationArn         string
	ApplicationProviderArn string
	Description            string
	InstanceArn            string
	Name                   string
	Status                 string
}

// ApplicationAssignment represents a principal assigned to an application.
type ApplicationAssignment struct {
	ApplicationArn string
	PrincipalID    string
	PrincipalType  string
}

// AccessControlAttributeValue holds the source list for an attribute.
type AccessControlAttributeValue struct {
	Source []string
}

// AccessControlAttribute represents a single ABAC attribute.
type AccessControlAttribute struct {
	Key   string
	Value AccessControlAttributeValue
}

// InstanceAccessControlAttributeConfiguration holds ABAC configuration for an instance.
type InstanceAccessControlAttributeConfiguration struct {
	AccessControlAttributes []AccessControlAttribute
}

// TrustedTokenIssuer represents a trusted token issuer.
type TrustedTokenIssuer struct {
	TrustedTokenIssuerArn  string
	InstanceArn            string
	Name                   string
	TrustedTokenIssuerType string
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
	instanceACAs            map[string]*InstanceAccessControlAttributeConfiguration
	trustedTokenIssuers     map[string]*TrustedTokenIssuer
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
		instanceACAs:            make(map[string]*InstanceAccessControlAttributeConfiguration),
		trustedTokenIssuers:     make(map[string]*TrustedTokenIssuer),
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
		Status:          "ACTIVE",
		CreatedDate:     time.Now().UTC(),
	}

	return b
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
		Status:          "ACTIVE",
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

// DeleteInstance removes an SSO instance.
func (b *InMemoryBackend) DeleteInstance(instanceArn string) error {
	b.mu.Lock("DeleteInstance")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceArn]; !ok {
		return ErrInstanceNotFound
	}
	delete(b.instances, instanceArn)

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
		sessionDuration = "PT1H"
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

	return status, nil
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
			result = append(result, a)
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

	return status, nil
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

// TagResource adds tags to a resource (permission set or instance).
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

	if _, ok := b.instances[resourceArn]; ok {
		return nil
	}

	return ErrInstanceNotFound
}

// UntagResource removes tags from a resource.
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

	return ErrInstanceNotFound
}

// ListTagsForResource returns the tags on a resource.
func (b *InMemoryBackend) ListTagsForResource(instanceArn, resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if ps, ok := b.permissionSets[resourceArn]; ok && ps.InstanceArn == instanceArn {
		result := make(map[string]string, len(ps.Tags))
		maps.Copy(result, ps.Tags)

		return result, nil
	}

	if _, ok := b.instances[resourceArn]; ok {
		return map[string]string{}, nil
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
		Status:                 "ENABLED",
		Tags:                   make(map[string]string),
	}
	maps.Copy(app.Tags, tags)
	b.applications[appArn] = app

	cp := *app
	cp.Tags = make(map[string]string)
	maps.Copy(cp.Tags, app.Tags)

	return &cp, nil
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
		AccessControlAttributes: attributes,
	}

	return nil
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

package ssoadmin

import (
	"errors"
	"fmt"
	"maps"
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
	ErrInstanceNotFound           = errors.New("ResourceNotFoundException")
	ErrPermissionSetNotFound      = errors.New("ResourceNotFoundException")
	ErrPermissionSetAlreadyExists = errors.New("ConflictException")
	ErrAssignmentNotFound         = errors.New("ResourceNotFoundException")
	ErrRequestNotFound            = errors.New("ResourceNotFoundException")
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

// InMemoryBackend is the in-memory backend for the SSO Admin service.
type InMemoryBackend struct {
	instances            map[string]*Instance
	permissionSets       map[string]*PermissionSet
	assignments          map[string][]*AccountAssignment
	creationStatuses     map[string]*ProvisioningStatus
	deletionStatuses     map[string]*ProvisioningStatus
	provisioningStatuses map[string]*ProvisioningStatus
	mu                   *lockmetrics.RWMutex
	accountID            string
	region               string
}

// NewInMemoryBackend creates a new in-memory SSO Admin backend with a default instance.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		instances:            make(map[string]*Instance),
		permissionSets:       make(map[string]*PermissionSet),
		assignments:          make(map[string][]*AccountAssignment),
		creationStatuses:     make(map[string]*ProvisioningStatus),
		deletionStatuses:     make(map[string]*ProvisioningStatus),
		provisioningStatuses: make(map[string]*ProvisioningStatus),
		mu:                   lockmetrics.New("ssoadmin"),
		accountID:            accountID,
		region:               region,
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

	return inst, nil
}

// ListInstances returns all SSO instances.
func (b *InMemoryBackend) ListInstances() []*Instance {
	b.mu.RLock("ListInstances")
	defer b.mu.RUnlock()

	list := make([]*Instance, 0, len(b.instances))
	for _, inst := range b.instances {
		list = append(list, inst)
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

	return inst, nil
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

	return ps, nil
}

// DescribePermissionSet returns a specific permission set.
func (b *InMemoryBackend) DescribePermissionSet(instanceArn, permissionSetArn string) (*PermissionSet, error) {
	b.mu.RLock("DescribePermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets[permissionSetArn]
	if !ok || ps.InstanceArn != instanceArn {
		return nil, ErrPermissionSetNotFound
	}

	return ps, nil
}

// ListPermissionSets returns all permission sets for an SSO instance.
func (b *InMemoryBackend) ListPermissionSets(instanceArn string) []*PermissionSet {
	b.mu.RLock("ListPermissionSets")
	defer b.mu.RUnlock()

	var list []*PermissionSet
	for _, ps := range b.permissionSets {
		if ps.InstanceArn == instanceArn {
			list = append(list, ps)
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

	return status, nil
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

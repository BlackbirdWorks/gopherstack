package organizations

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinel errors.
var (
	// ErrOrgNotFound is returned when no organization exists.
	ErrOrgNotFound = awserr.New("AWSOrganizationsNotInUseException: organization not found", awserr.ErrNotFound)
	// ErrOrgAlreadyExists is returned when an organization already exists.
	ErrOrgAlreadyExists = awserr.New(
		"AlreadyInOrganizationException: account is already a member of an organization",
		awserr.ErrAlreadyExists,
	)
	// ErrAccountNotFound is returned when an account does not exist.
	ErrAccountNotFound = awserr.New("AccountNotFoundException: account not found", awserr.ErrNotFound)
	// ErrOUNotFound is returned when an OU does not exist.
	ErrOUNotFound = awserr.New("OrganizationalUnitNotFoundException: OU not found", awserr.ErrNotFound)
	// ErrPolicyNotFound is returned when a policy does not exist.
	ErrPolicyNotFound = awserr.New("PolicyNotFoundException: policy not found", awserr.ErrNotFound)
	// ErrPolicyTypeAlreadyEnabled is returned when a policy type is already enabled.
	ErrPolicyTypeAlreadyEnabled = awserr.New(
		"PolicyTypeAlreadyEnabledException: policy type already enabled",
		awserr.ErrConflict,
	)
	// ErrPolicyTypeNotEnabled is returned when a policy type is not enabled.
	ErrPolicyTypeNotEnabled = awserr.New(
		"PolicyTypeNotEnabledException: policy type not enabled",
		awserr.ErrConflict,
	)
	// ErrCreateAccountStatusNotFound is returned when a create-account status is not found.
	ErrCreateAccountStatusNotFound = awserr.New(
		"CreateAccountStatusNotFoundException: create account status not found",
		awserr.ErrNotFound,
	)
	// ErrDuplicatePolicyAttachment is returned when a policy is already attached.
	ErrDuplicatePolicyAttachment = awserr.New(
		"DuplicatePolicyAttachmentException: policy already attached",
		awserr.ErrConflict,
	)
	// ErrPolicyNotAttached is returned when a policy is not attached to the target.
	ErrPolicyNotAttached = awserr.New(
		"PolicyNotAttachedException: policy not attached to target",
		awserr.ErrNotFound,
	)
	// ErrInvalidInput is returned for invalid input parameters.
	ErrInvalidInput = awserr.New("InvalidInputException: invalid input", awserr.ErrInvalidParameter)
	// ErrChildNotFound is returned when a child resource is not found.
	ErrChildNotFound = awserr.New("ChildNotFoundException: child not found", awserr.ErrNotFound)
	// ErrDelegatedAdminNotFound is returned when a delegated admin is not found.
	ErrDelegatedAdminNotFound = awserr.New(
		"AccountNotRegisteredException: account is not a registered delegated administrator",
		awserr.ErrNotFound,
	)
	// ErrDelegatedAdminAlreadyExists is returned when a delegated admin already exists.
	ErrDelegatedAdminAlreadyExists = awserr.New(
		"AccountAlreadyRegisteredException: account is already a delegated administrator",
		awserr.ErrAlreadyExists,
	)
)

const (
	accountStatusActive = "ACTIVE"
	joinedMethodInvited = "INVITED"
	joinedMethodCreated = "CREATED"

	policyStatusEnabled = "ENABLED"

	createAccountStateSucceeded = "SUCCEEDED"

	// managementAccountCounter is the starting account counter (management account = 1).
	managementAccountCounter = 1

	// orgIDLen is the number of random letters in an org ID.
	orgIDLen = 10
	// rootIDLen is the number of random letters in a root ID.
	rootIDLen = 4
	// rootIDPrefixLen is the length of the "r-" prefix to strip when building OU IDs.
	rootIDPrefixLen = 2
	// ouRandomLen is the number of random chars in an OU ID suffix.
	ouRandomLen = 8
	// policyIDLen is the number of random chars in a policy ID.
	policyIDLen = 8
)

const idChars = "abcdefghijklmnopqrstuvwxyz0123456789"

func randomChars(n int) string {
	b := make([]byte, n)
	idLen := big.NewInt(int64(len(idChars)))

	for i := range b {
		idx, err := rand.Int(rand.Reader, idLen)
		if err != nil {
			// Fallback: use a fixed character on error (should never happen).
			b[i] = idChars[0]

			continue
		}

		b[i] = idChars[idx.Int64()]
	}

	return string(b)
}

func randomLetters(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	lettersLen := big.NewInt(int64(len(letters)))

	for i := range b {
		idx, err := rand.Int(rand.Reader, lettersLen)
		if err != nil {
			b[i] = letters[0]

			continue
		}

		b[i] = letters[idx.Int64()]
	}

	return string(b)
}

func newOrgID() string  { return "o-" + randomLetters(orgIDLen) }
func newRootID() string { return "r-" + randomLetters(rootIDLen) }
func newOUID(rootID string) string {
	// Strip "r-" prefix for the ou id component.
	base := rootID
	if len(base) > rootIDPrefixLen {
		base = base[rootIDPrefixLen:]
	}

	return "ou-" + base + "-" + randomChars(ouRandomLen)
}
func newPolicyID() string { return "p-" + randomChars(policyIDLen) }

// newAccountID generates a 12-digit account ID from an integer counter.
func newAccountID(counter int) string {
	return fmt.Sprintf("%012d", counter)
}

// InMemoryBackend is the in-memory storage for the Organizations service.
type InMemoryBackend struct {
	policyTargets   map[string][]string
	accountParent   map[string]string
	delegatedAdmins map[string]map[string]*DelegatedAdmin
	org             *Organization
	root            *Root
	accounts        map[string]*Account
	ous             map[string]*OrganizationalUnit
	policies        map[string]*Policy
	serviceAccess   map[string]time.Time
	createStatuses  map[string]*CreateAccountStatus
	targetPolicies  map[string][]string
	ouParent        map[string]string
	tags            map[string]map[string]string
	accountID       string
	region          string
	accountCounter  int
	statusCounter   int
	mu              sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory Organizations backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID:       accountID,
		region:          region,
		accounts:        make(map[string]*Account),
		ous:             make(map[string]*OrganizationalUnit),
		policies:        make(map[string]*Policy),
		policyTargets:   make(map[string][]string),
		targetPolicies:  make(map[string][]string),
		accountParent:   make(map[string]string),
		ouParent:        make(map[string]string),
		tags:            make(map[string]map[string]string),
		createStatuses:  make(map[string]*CreateAccountStatus),
		serviceAccess:   make(map[string]time.Time),
		delegatedAdmins: make(map[string]map[string]*DelegatedAdmin),
		accountCounter:  managementAccountCounter,
	}
}

// orgARN builds an ARN for the organization.
func (b *InMemoryBackend) orgARN(orgID string) string {
	return fmt.Sprintf("arn:aws:organizations::%s:organization/%s", b.accountID, orgID)
}

// masterAccountARN builds an ARN for the management account.
func (b *InMemoryBackend) masterAccountARN(orgID, accountID string) string {
	return fmt.Sprintf("arn:aws:organizations::%s:account/%s/%s", b.accountID, orgID, accountID)
}

// accountARN builds an ARN for an account.
func (b *InMemoryBackend) accountARN(orgID, accountID string) string {
	return fmt.Sprintf("arn:aws:organizations::%s:account/%s/%s", b.accountID, orgID, accountID)
}

// rootARN builds an ARN for the root.
func (b *InMemoryBackend) rootARN(orgID, rootID string) string {
	return fmt.Sprintf("arn:aws:organizations::%s:root/%s/%s", b.accountID, orgID, rootID)
}

// ouARN builds an ARN for an OU.
func (b *InMemoryBackend) ouARN(orgID, ouID string) string {
	return fmt.Sprintf("arn:aws:organizations::%s:ou/%s/%s", b.accountID, orgID, ouID)
}

// policyARN builds an ARN for a policy.
func (b *InMemoryBackend) policyARN(orgID, policyType, policyID string) string {
	return fmt.Sprintf("arn:aws:organizations::%s:policy/%s/%s/%s", b.accountID, orgID, policyType, policyID)
}

// -- Organization operations --

// CreateOrganization creates a new organization.
func (b *InMemoryBackend) CreateOrganization(featureSet string) (*Organization, *Root, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org != nil {
		return nil, nil, ErrOrgAlreadyExists
	}

	if featureSet == "" {
		featureSet = "ALL"
	}

	orgID := newOrgID()
	rootID := newRootID()

	b.accountCounter = managementAccountCounter
	mgmtAcctID := newAccountID(b.accountCounter)

	org := &Organization{
		ID:                 orgID,
		ARN:                b.orgARN(orgID),
		FeatureSet:         featureSet,
		MasterAccountID:    mgmtAcctID,
		MasterAccountARN:   b.masterAccountARN(orgID, mgmtAcctID),
		MasterAccountEmail: fmt.Sprintf("master@%s.example.com", mgmtAcctID),
	}

	root := &Root{
		ID:          rootID,
		ARN:         b.rootARN(orgID, rootID),
		Name:        "Root",
		PolicyTypes: []PolicyTypeSummary{},
	}

	mgmtAcct := &Account{
		ID:           mgmtAcctID,
		ARN:          b.accountARN(orgID, mgmtAcctID),
		Name:         "master",
		Email:        org.MasterAccountEmail,
		Status:       accountStatusActive,
		JoinedMethod: joinedMethodInvited,
		JoinedAt:     time.Now(),
	}

	b.org = org
	b.root = root
	b.accounts[mgmtAcctID] = mgmtAcct
	b.accountParent[mgmtAcctID] = rootID

	return org, root, nil
}

// DescribeOrganization returns the current organization.
func (b *InMemoryBackend) DescribeOrganization() (*Organization, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	return b.org, nil
}

// DeleteOrganization removes the organization.
func (b *InMemoryBackend) DeleteOrganization() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	b.org = nil
	b.root = nil
	b.accounts = make(map[string]*Account)
	b.ous = make(map[string]*OrganizationalUnit)
	b.policies = make(map[string]*Policy)
	b.policyTargets = make(map[string][]string)
	b.targetPolicies = make(map[string][]string)
	b.accountParent = make(map[string]string)
	b.ouParent = make(map[string]string)
	b.tags = make(map[string]map[string]string)
	b.createStatuses = make(map[string]*CreateAccountStatus)
	b.serviceAccess = make(map[string]time.Time)
	b.delegatedAdmins = make(map[string]map[string]*DelegatedAdmin)
	b.accountCounter = managementAccountCounter

	return nil
}

// -- Account operations --

// CreateAccount creates a new account and returns its status.
func (b *InMemoryBackend) CreateAccount(name, email string, tags []Tag) (*CreateAccountStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	b.accountCounter++
	acctID := newAccountID(b.accountCounter)

	now := time.Now()
	acct := &Account{
		ID:           acctID,
		ARN:          b.accountARN(b.org.ID, acctID),
		Name:         name,
		Email:        email,
		Status:       accountStatusActive,
		JoinedMethod: joinedMethodCreated,
		JoinedAt:     now,
	}

	b.accounts[acctID] = acct
	b.accountParent[acctID] = b.root.ID
	b.setTagsLocked(acctID, tags)

	b.statusCounter++
	statusID := fmt.Sprintf("car-%012d", b.statusCounter)

	status := &CreateAccountStatus{
		ID:                 statusID,
		AccountID:          acctID,
		AccountName:        name,
		State:              createAccountStateSucceeded,
		RequestedTimestamp: epochSeconds(now),
		CompletedTimestamp: epochSeconds(now),
	}

	b.createStatuses[statusID] = status

	return status, nil
}

// DescribeCreateAccountStatus returns the status of a CreateAccount request.
func (b *InMemoryBackend) DescribeCreateAccountStatus(requestID string) (*CreateAccountStatus, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	s, ok := b.createStatuses[requestID]
	if !ok {
		return nil, ErrCreateAccountStatusNotFound
	}

	return s, nil
}

// DescribeAccount returns an account by ID.
func (b *InMemoryBackend) DescribeAccount(accountID string) (*Account, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	a, ok := b.accounts[accountID]
	if !ok {
		return nil, ErrAccountNotFound
	}

	return a, nil
}

// ListAccounts returns all accounts in the organization.
func (b *InMemoryBackend) ListAccounts() ([]*Account, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	out := make([]*Account, 0, len(b.accounts))
	for _, a := range b.accounts {
		out = append(out, a)
	}

	return out, nil
}

// RemoveAccountFromOrganization removes an account from the organization.
func (b *InMemoryBackend) RemoveAccountFromOrganization(accountID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if accountID == b.org.MasterAccountID {
		return ErrInvalidInput
	}

	if _, ok := b.accounts[accountID]; !ok {
		return ErrAccountNotFound
	}

	delete(b.accounts, accountID)
	delete(b.accountParent, accountID)

	return nil
}

// MoveAccount moves an account from one parent to another.
func (b *InMemoryBackend) MoveAccount(accountID, sourceParentID, destParentID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if _, ok := b.accounts[accountID]; !ok {
		return ErrAccountNotFound
	}

	current := b.accountParent[accountID]
	if current != sourceParentID {
		return ErrInvalidInput
	}

	if !b.parentExists(destParentID) {
		return ErrInvalidInput
	}

	b.accountParent[accountID] = destParentID

	return nil
}

// parentExists checks if a parentID refers to the root or an existing OU.
func (b *InMemoryBackend) parentExists(parentID string) bool {
	if b.root != nil && b.root.ID == parentID {
		return true
	}

	_, ok := b.ous[parentID]

	return ok
}

// -- Root operations --

// ListRoots returns the organization roots.
func (b *InMemoryBackend) ListRoots() ([]*Root, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	return []*Root{b.root}, nil
}

// -- OU operations --

// CreateOrganizationalUnit creates a new OU under the given parent.
func (b *InMemoryBackend) CreateOrganizationalUnit(parentID, name string, tags []Tag) (*OrganizationalUnit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !b.parentExists(parentID) {
		return nil, ErrInvalidInput
	}

	ouID := newOUID(b.root.ID)
	ou := &OrganizationalUnit{
		ID:       ouID,
		ARN:      b.ouARN(b.org.ID, ouID),
		Name:     name,
		ParentID: parentID,
	}

	b.ous[ouID] = ou
	b.ouParent[ouID] = parentID
	b.setTagsLocked(ouID, tags)

	return ou, nil
}

// DescribeOrganizationalUnit returns an OU by ID.
func (b *InMemoryBackend) DescribeOrganizationalUnit(ouID string) (*OrganizationalUnit, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ou, ok := b.ous[ouID]
	if !ok {
		return nil, ErrOUNotFound
	}

	return ou, nil
}

// DeleteOrganizationalUnit removes an OU.
func (b *InMemoryBackend) DeleteOrganizationalUnit(ouID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.ous[ouID]; !ok {
		return ErrOUNotFound
	}

	delete(b.ous, ouID)
	delete(b.ouParent, ouID)

	return nil
}

// UpdateOrganizationalUnit renames an OU.
func (b *InMemoryBackend) UpdateOrganizationalUnit(ouID, name string) (*OrganizationalUnit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ou, ok := b.ous[ouID]
	if !ok {
		return nil, ErrOUNotFound
	}

	ou.Name = name

	return ou, nil
}

// ListOrganizationalUnitsForParent returns all OUs under a parent.
func (b *InMemoryBackend) ListOrganizationalUnitsForParent(parentID string) ([]*OrganizationalUnit, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !b.parentExists(parentID) {
		return nil, ErrInvalidInput
	}

	var out []*OrganizationalUnit

	for _, ou := range b.ous {
		if ou.ParentID == parentID {
			out = append(out, ou)
		}
	}

	return out, nil
}

// ListAccountsForParent returns all accounts directly under a parent.
func (b *InMemoryBackend) ListAccountsForParent(parentID string) ([]*Account, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !b.parentExists(parentID) {
		return nil, ErrInvalidInput
	}

	var out []*Account

	for acctID, pid := range b.accountParent {
		if pid == parentID {
			if a, ok := b.accounts[acctID]; ok {
				out = append(out, a)
			}
		}
	}

	return out, nil
}

// ListParents returns the parents of an account or OU.
func (b *InMemoryBackend) ListParents(childID string) ([]ParentSummary, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	// Check if childID is an account.
	if parentID, ok := b.accountParent[childID]; ok {
		parentType := b.resolveParentType(parentID)

		return []ParentSummary{{ID: parentID, Type: parentType}}, nil
	}

	// Check if childID is an OU.
	if parentID, ok := b.ouParent[childID]; ok {
		parentType := b.resolveParentType(parentID)

		return []ParentSummary{{ID: parentID, Type: parentType}}, nil
	}

	return nil, ErrChildNotFound
}

// resolveParentType returns "ROOT" or "ORGANIZATIONAL_UNIT" for a given parent ID.
func (b *InMemoryBackend) resolveParentType(parentID string) string {
	if b.root != nil && b.root.ID == parentID {
		return "ROOT"
	}

	return "ORGANIZATIONAL_UNIT"
}

// ListChildren returns children of a given type under a parent.
func (b *InMemoryBackend) ListChildren(parentID, childType string) ([]ChildSummary, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !b.parentExists(parentID) {
		return nil, ErrInvalidInput
	}

	var out []ChildSummary

	switch childType {
	case "ACCOUNT":
		for acctID, pid := range b.accountParent {
			if pid == parentID {
				out = append(out, ChildSummary{ID: acctID, Type: "ACCOUNT"})
			}
		}
	case "ORGANIZATIONAL_UNIT":
		for ouID, ou := range b.ous {
			if ou.ParentID == parentID {
				out = append(out, ChildSummary{ID: ouID, Type: "ORGANIZATIONAL_UNIT"})
			}
		}
	default:
		return nil, ErrInvalidInput
	}

	return out, nil
}

// -- Policy operations --

// CreatePolicy creates a new policy.
func (b *InMemoryBackend) CreatePolicy(name, description, content, policyType string, tags []Tag) (*Policy, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	policyID := newPolicyID()
	p := &Policy{
		PolicySummary: PolicySummary{
			ID:          policyID,
			ARN:         b.policyARN(b.org.ID, policyType, policyID),
			Name:        name,
			Description: description,
			Type:        policyType,
			AwsManaged:  false,
		},
		Content: content,
	}

	b.policies[policyID] = p
	b.policyTargets[policyID] = []string{}
	b.setTagsLocked(policyID, tags)

	return p, nil
}

// DescribePolicy returns a policy by ID.
func (b *InMemoryBackend) DescribePolicy(policyID string) (*Policy, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	p, ok := b.policies[policyID]
	if !ok {
		return nil, ErrPolicyNotFound
	}

	return p, nil
}

// UpdatePolicy updates a policy.
func (b *InMemoryBackend) UpdatePolicy(policyID, name, description, content string) (*Policy, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.policies[policyID]
	if !ok {
		return nil, ErrPolicyNotFound
	}

	if name != "" {
		p.PolicySummary.Name = name
	}

	if description != "" {
		p.PolicySummary.Description = description
	}

	if content != "" {
		p.Content = content
	}

	return p, nil
}

// DeletePolicy removes a policy.
func (b *InMemoryBackend) DeletePolicy(policyID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.policies[policyID]; !ok {
		return ErrPolicyNotFound
	}

	// Detach from all targets.
	for _, targetID := range b.policyTargets[policyID] {
		b.targetPolicies[targetID] = removeString(b.targetPolicies[targetID], policyID)
	}

	delete(b.policyTargets, policyID)
	delete(b.policies, policyID)

	return nil
}

// ListPolicies returns all policies of a given type.
func (b *InMemoryBackend) ListPolicies(filter string) ([]*Policy, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	var out []*Policy

	for _, p := range b.policies {
		if filter == "" || p.PolicySummary.Type == filter {
			out = append(out, p)
		}
	}

	return out, nil
}

// AttachPolicy attaches a policy to a target.
func (b *InMemoryBackend) AttachPolicy(policyID, targetID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.policies[policyID]; !ok {
		return ErrPolicyNotFound
	}

	targets := b.policyTargets[policyID]
	if slices.Contains(targets, targetID) {
		return ErrDuplicatePolicyAttachment
	}

	b.policyTargets[policyID] = append(targets, targetID)
	b.targetPolicies[targetID] = append(b.targetPolicies[targetID], policyID)

	return nil
}

// DetachPolicy detaches a policy from a target.
func (b *InMemoryBackend) DetachPolicy(policyID, targetID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.policies[policyID]; !ok {
		return ErrPolicyNotFound
	}

	targets := b.policyTargets[policyID]

	if !slices.Contains(targets, targetID) {
		return ErrPolicyNotAttached
	}

	b.policyTargets[policyID] = removeString(targets, targetID)
	b.targetPolicies[targetID] = removeString(b.targetPolicies[targetID], policyID)

	return nil
}

// ListPoliciesForTarget returns policies attached to a target, filtered by type.
func (b *InMemoryBackend) ListPoliciesForTarget(targetID, filter string) ([]*Policy, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	policyIDs := b.targetPolicies[targetID]

	var out []*Policy

	for _, pid := range policyIDs {
		if p, ok := b.policies[pid]; ok {
			if filter == "" || p.PolicySummary.Type == filter {
				out = append(out, p)
			}
		}
	}

	return out, nil
}

// ListTargetsForPolicy returns targets that a policy is attached to.
func (b *InMemoryBackend) ListTargetsForPolicy(policyID string) ([]PolicyTargetSummary, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.policies[policyID]; !ok {
		return nil, ErrPolicyNotFound
	}

	targetIDs := b.policyTargets[policyID]
	out := make([]PolicyTargetSummary, 0, len(targetIDs))

	for _, tid := range targetIDs {
		summary := b.resolveTargetSummary(tid)
		out = append(out, summary)
	}

	return out, nil
}

// resolveTargetSummary builds a PolicyTargetSummary for a given target ID.
func (b *InMemoryBackend) resolveTargetSummary(targetID string) PolicyTargetSummary {
	if b.root != nil && b.root.ID == targetID {
		return PolicyTargetSummary{
			TargetID: targetID,
			ARN:      b.root.ARN,
			Name:     b.root.Name,
			Type:     "ROOT",
		}
	}

	if ou, ok := b.ous[targetID]; ok {
		return PolicyTargetSummary{
			TargetID: targetID,
			ARN:      ou.ARN,
			Name:     ou.Name,
			Type:     "ORGANIZATIONAL_UNIT",
		}
	}

	if acct, ok := b.accounts[targetID]; ok {
		return PolicyTargetSummary{
			TargetID: targetID,
			ARN:      acct.ARN,
			Name:     acct.Name,
			Type:     "ACCOUNT",
		}
	}

	return PolicyTargetSummary{TargetID: targetID, Type: "ACCOUNT"}
}

// EnablePolicyType enables a policy type on the root.
func (b *InMemoryBackend) EnablePolicyType(rootID, policyType string) (*Root, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if b.root == nil || b.root.ID != rootID {
		return nil, ErrInvalidInput
	}

	for _, pt := range b.root.PolicyTypes {
		if pt.Type == policyType && pt.Status == policyStatusEnabled {
			return nil, ErrPolicyTypeAlreadyEnabled
		}
	}

	b.root.PolicyTypes = append(b.root.PolicyTypes, PolicyTypeSummary{
		Type:   policyType,
		Status: policyStatusEnabled,
	})

	return b.root, nil
}

// DisablePolicyType disables a policy type on the root.
func (b *InMemoryBackend) DisablePolicyType(rootID, policyType string) (*Root, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if b.root == nil || b.root.ID != rootID {
		return nil, ErrInvalidInput
	}

	newTypes := make([]PolicyTypeSummary, 0, len(b.root.PolicyTypes))

	found := false

	for _, pt := range b.root.PolicyTypes {
		if pt.Type == policyType {
			found = true

			continue
		}

		newTypes = append(newTypes, pt)
	}

	if !found {
		return nil, ErrPolicyTypeNotEnabled
	}

	b.root.PolicyTypes = newTypes

	return b.root, nil
}

// -- Tag operations --

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceID string, tags []Tag) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.setTagsLocked(resourceID, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceID string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	t := b.tags[resourceID]
	if t == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(t, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceID string) ([]Tag, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t := b.tags[resourceID]
	out := make([]Tag, 0, len(t))

	for k, v := range t {
		out = append(out, Tag{Key: k, Value: v})
	}

	return out, nil
}

// setTagsLocked merges tags onto a resource. Must be called with lock held.
func (b *InMemoryBackend) setTagsLocked(resourceID string, tags []Tag) {
	if len(tags) == 0 {
		return
	}

	if b.tags[resourceID] == nil {
		b.tags[resourceID] = make(map[string]string)
	}

	for _, t := range tags {
		b.tags[resourceID][t.Key] = t.Value
	}
}

// -- Service Access --

// EnableAWSServiceAccess enables a service principal for org-wide access.
func (b *InMemoryBackend) EnableAWSServiceAccess(servicePrincipal string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	b.serviceAccess[servicePrincipal] = time.Now()

	return nil
}

// DisableAWSServiceAccess disables a service principal.
func (b *InMemoryBackend) DisableAWSServiceAccess(servicePrincipal string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	delete(b.serviceAccess, servicePrincipal)

	return nil
}

// ListAWSServiceAccessForOrganization returns enabled service principals.
func (b *InMemoryBackend) ListAWSServiceAccessForOrganization() ([]EnabledServicePrincipal, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	out := make([]EnabledServicePrincipal, 0, len(b.serviceAccess))

	for sp, t := range b.serviceAccess {
		out = append(out, EnabledServicePrincipal{
			ServicePrincipal: sp,
			DateEnabled:      t,
		})
	}

	return out, nil
}

// -- Delegated Admin --

// RegisterDelegatedAdministrator registers a delegated admin for a service.
func (b *InMemoryBackend) RegisterDelegatedAdministrator(accountID, servicePrincipal string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	acct, ok := b.accounts[accountID]
	if !ok {
		return ErrAccountNotFound
	}

	if b.delegatedAdmins[servicePrincipal] == nil {
		b.delegatedAdmins[servicePrincipal] = make(map[string]*DelegatedAdmin)
	}

	if _, exists := b.delegatedAdmins[servicePrincipal][accountID]; exists {
		return ErrDelegatedAdminAlreadyExists
	}

	b.delegatedAdmins[servicePrincipal][accountID] = &DelegatedAdmin{
		AccountID:        accountID,
		ARN:              acct.ARN,
		Name:             acct.Name,
		Email:            acct.Email,
		Status:           accountStatusActive,
		JoinedMethod:     acct.JoinedMethod,
		JoinedAt:         acct.JoinedAt,
		DelegationTime:   time.Now(),
		ServicePrincipal: servicePrincipal,
	}

	return nil
}

// DeregisterDelegatedAdministrator removes a delegated admin.
func (b *InMemoryBackend) DeregisterDelegatedAdministrator(accountID, servicePrincipal string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	m := b.delegatedAdmins[servicePrincipal]
	if m == nil {
		return ErrDelegatedAdminNotFound
	}

	if _, ok := m[accountID]; !ok {
		return ErrDelegatedAdminNotFound
	}

	delete(m, accountID)

	return nil
}

// ListDelegatedAdministrators lists delegated admins, optionally filtered by service principal.
func (b *InMemoryBackend) ListDelegatedAdministrators(servicePrincipal string) ([]*DelegatedAdmin, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	var out []*DelegatedAdmin

	if servicePrincipal != "" {
		for _, da := range b.delegatedAdmins[servicePrincipal] {
			out = append(out, da)
		}

		return out, nil
	}

	for _, admins := range b.delegatedAdmins {
		for _, da := range admins {
			out = append(out, da)
		}
	}

	return out, nil
}

// -- Helpers --

// removeString returns a copy of s with all occurrences of v removed.
func removeString(s []string, v string) []string {
	return slices.DeleteFunc(slices.Clone(s), func(x string) bool { return x == v })
}

// EnsureOrgExists returns ErrOrgNotFound if no org exists (for operations that require it).
func (b *InMemoryBackend) EnsureOrgExists() error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	return nil
}

// Ensure errors are used somewhere to satisfy linter.
var _ = errors.Is(ErrOrgNotFound, awserr.ErrNotFound)

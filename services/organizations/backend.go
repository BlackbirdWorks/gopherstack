package organizations

import (
	"cmp"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	targetTypeOU      = "ORGANIZATIONAL_UNIT"
	targetTypeAccount = "ACCOUNT"

	handshakeActionInvite         = "INVITE"
	handshakeActionLeave          = "LEAVE_ORGANIZATION"
	handshakeActionEnableFeatures = "ENABLE_ALL_FEATURES"
	handshakeActionApproveAll     = "APPROVE_ALL_FEATURES"
	handshakeResourceOrg          = "ORGANIZATION"
	handshakeResourceMasterEmail  = "MASTER_EMAIL"
	handshakeResourceNotes        = "NOTES"

	// maxOUDepth is the maximum depth for organizational units (root=0, OUs=1-5).
	maxOUDepth = 5

	// accountIDLength is the expected length of an AWS account ID.
	accountIDLength = 12
)

// Sentinel errors.
var (
	// ErrOrgNotFound is returned when no organization exists.
	ErrOrgNotFound = awserr.New(
		"AWSOrganizationsNotInUseException: organization not found",
		awserr.ErrNotFound,
	)
	// ErrOrgAlreadyExists is returned when an organization already exists.
	ErrOrgAlreadyExists = awserr.New(
		"AlreadyInOrganizationException: account is already a member of an organization",
		awserr.ErrAlreadyExists,
	)
	// ErrAccountNotFound is returned when an account does not exist.
	ErrAccountNotFound = awserr.New(
		"AccountNotFoundException: account not found",
		awserr.ErrNotFound,
	)
	// ErrOUNotFound is returned when an OU does not exist.
	ErrOUNotFound = awserr.New(
		"OrganizationalUnitNotFoundException: OU not found",
		awserr.ErrNotFound,
	)
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
	// ErrPolicyLimitExceeded is returned when the maximum number of policies per target is exceeded.
	ErrPolicyLimitExceeded = awserr.New(
		"ConstraintViolationException: maximum policies per target exceeded",
		awserr.ErrConflict,
	)
	// ErrHandshakeNotFound is returned when a handshake does not exist.
	ErrHandshakeNotFound = awserr.New(
		"HandshakeNotFoundException: handshake not found",
		awserr.ErrNotFound,
	)
	// ErrHandshakeConstraintViolation is returned when a handshake state transition is invalid.
	ErrHandshakeConstraintViolation = awserr.New(
		"HandshakeConstraintViolationException: handshake is not in a valid state for this transition",
		awserr.ErrConflict,
	)
	// ErrResourcePolicyNotFound is returned when no resource policy exists.
	ErrResourcePolicyNotFound = awserr.New(
		"ResourcePolicyNotFoundException: resource policy not found",
		awserr.ErrNotFound,
	)
	// ErrEffectivePolicyNotFound is returned when no effective policy exists for a target.
	ErrEffectivePolicyNotFound = awserr.New(
		"EffectivePolicyNotFoundException: no effective policy of the given type",
		awserr.ErrNotFound,
	)
	// ErrAccountAlreadyClosed is returned when an account is already in PENDING_CLOSURE or SUSPENDED state.
	ErrAccountAlreadyClosed = awserr.New(
		"ConstraintViolationException: account is already closed or pending closure",
		awserr.ErrConflict,
	)
	// ErrOUDepthLimitExceeded is returned when creating an OU would exceed the depth limit.
	ErrOUDepthLimitExceeded = awserr.New(
		"ConstraintViolationException: OU_DEPTH_LIMIT_EXCEEDED",
		awserr.ErrConflict,
	)
	// ErrDuplicateOrganizationalUnit is returned when an OU with the same name already exists under the same parent.
	ErrDuplicateOrganizationalUnit = awserr.New(
		"DuplicateOrganizationalUnitException: duplicate OU name",
		awserr.ErrAlreadyExists,
	)
	// ErrTargetNotFound is returned when a policy attachment target is not found.
	ErrTargetNotFound = awserr.New(
		"TargetNotFoundException: target not found",
		awserr.ErrNotFound,
	)
	// ErrServiceNotEnabled is returned when registering a delegated admin for a service not enabled for org access.
	ErrServiceNotEnabled = awserr.New(
		"ConstraintViolationException: service principal not enabled for service access",
		awserr.ErrConflict,
	)
	// ErrPolicyInUse is returned when attempting to delete a policy still attached to targets.
	ErrPolicyInUse = awserr.New(
		"PolicyInUseException: policy is still attached to one or more targets",
		awserr.ErrConflict,
	)
	// ErrOrganizationNotEmpty is returned when attempting to delete an org that still has member accounts.
	ErrOrganizationNotEmpty = awserr.New(
		"OrganizationNotEmptyException: organization still contains member accounts",
		awserr.ErrConflict,
	)
	// ErrDuplicateHandshake is returned when an open handshake already exists for the same target.
	ErrDuplicateHandshake = awserr.New(
		"DuplicateHandshakeException: a handshake already exists for the specified account",
		awserr.ErrAlreadyExists,
	)
	// ErrPolicyTypeAttached is returned when disabling a policy type that still has attached policies.
	ErrPolicyTypeAttached = awserr.New(
		"ConstraintViolationException: cannot disable policy type while policies of that type are attached",
		awserr.ErrConflict,
	)
)

const (
	accountStatusActive         = "ACTIVE"
	accountStatusSuspended      = "SUSPENDED"
	accountStatusPendingClosure = "PENDING_CLOSURE"
	joinedMethodInvited         = "INVITED"
	joinedMethodCreated         = "CREATED"

	policyStatusEnabled = "ENABLED"

	createAccountStateSucceeded = "SUCCEEDED"

	handshakeStateOpen     = "OPEN"
	handshakeStateCanceled = "CANCELED"
	handshakeStateAccepted = "ACCEPTED"
	handshakeStateDeclined = "DECLINED"
	handshakeStateExpired  = "EXPIRED"

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
	// handshakeIDLen is the number of random chars in a handshake ID.
	handshakeIDLen = 8

	// govCloudAccountIDOffset is added to the account counter to generate a GovCloud account ID.
	govCloudAccountIDOffset = 1_000_000_000

	// maxPoliciesPerTarget is the AWS limit for the number of policies of the same type attached to a target.
	maxPoliciesPerTarget = 5

	// handshakeExpirationDuration is the default lifetime of a handshake (AWS default: 15 days).
	handshakeExpirationDuration = 15 * 24 * time.Hour
)

// validPolicyTypes returns the policy types supported by AWS Organizations.
func validPolicyTypes() []string {
	return []string{
		"SERVICE_CONTROL_POLICY",
		"RESOURCE_CONTROL_POLICY",
		"TAG_POLICY",
		"BACKUP_POLICY",
		"AISERVICES_OPT_OUT_POLICY",
		"CHATBOT_POLICY",
		"DECLARATIVE_POLICY_EC2",
		"SECURITYHUB_POLICY",
	}
}

const (
	idChars  = "abcdefghijklmnopqrstuvwxyz0123456789"
	hexChars = "0123456789abcdef"
)

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

// randomHex returns n random lowercase hex characters (0-9a-f).
func randomHex(n int) string {
	b := make([]byte, n)
	hexLen := big.NewInt(int64(len(hexChars)))

	for i := range b {
		idx, err := rand.Int(rand.Reader, hexLen)
		if err != nil {
			b[i] = hexChars[0]

			continue
		}

		b[i] = hexChars[idx.Int64()]
	}

	return string(b)
}

func newPolicyID() string    { return "p-" + randomHex(policyIDLen) }
func newHandshakeID() string { return "h-" + randomChars(handshakeIDLen) }
func newGovCloudAccountID(counter int) string {
	return fmt.Sprintf("%012d", counter+govCloudAccountIDOffset)
}

// newAccountID generates a 12-digit account ID from an integer counter.
func newAccountID(counter int) string {
	return fmt.Sprintf("%012d", counter)
}

// InMemoryBackend is the in-memory storage for the Organizations service.
type InMemoryBackend struct {
	serviceAccess    map[string]time.Time
	targetPolicies   map[string][]string
	delegatedAdmins  map[string]map[string]*DelegatedAdmin
	handshakes       map[string]*Handshake
	org              *Organization
	root             *Root
	resourcePolicy   *ResourcePolicy
	accounts         map[string]*Account
	ous              map[string]*OrganizationalUnit
	policies         map[string]*Policy
	accountParent    map[string]string
	policyTargets    map[string][]string
	createStatuses   map[string]*CreateAccountStatus
	ouParent         map[string]string
	tags             map[string]map[string]string
	emailToAccountID map[string]string
	mu               *lockmetrics.RWMutex
	region           string
	accountID        string
	accountCounter   int
	statusCounter    int
}

// NewInMemoryBackend creates a new in-memory Organizations backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID:        accountID,
		region:           region,
		accounts:         make(map[string]*Account),
		ous:              make(map[string]*OrganizationalUnit),
		policies:         make(map[string]*Policy),
		policyTargets:    make(map[string][]string),
		targetPolicies:   make(map[string][]string),
		accountParent:    make(map[string]string),
		ouParent:         make(map[string]string),
		tags:             make(map[string]map[string]string),
		createStatuses:   make(map[string]*CreateAccountStatus),
		serviceAccess:    make(map[string]time.Time),
		delegatedAdmins:  make(map[string]map[string]*DelegatedAdmin),
		handshakes:       make(map[string]*Handshake),
		emailToAccountID: make(map[string]string),
		accountCounter:   managementAccountCounter,
		mu:               lockmetrics.New("organizations"),
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
	return fmt.Sprintf(
		"arn:aws:organizations::%s:policy/%s/%s/%s",
		b.accountID,
		orgID,
		policyType,
		policyID,
	)
}

// resourcePolicyARN builds an ARN for the organization resource policy.
func (b *InMemoryBackend) resourcePolicyARN(orgID string) string {
	return fmt.Sprintf(
		"arn:aws:organizations::%s:resourcepolicy/%s/p-rp-default",
		b.accountID,
		orgID,
	)
}

// handshakeARN builds an ARN for a handshake.
// action should be the lowercase action string (e.g. "invite", "enable_all_features").
func (b *InMemoryBackend) handshakeARN(orgID, action, handshakeID string) string {
	return fmt.Sprintf(
		"arn:aws:organizations::%s:handshake/%s/%s/%s",
		b.accountID,
		orgID,
		strings.ToLower(action),
		handshakeID,
	)
}

// AccountID returns the configured AWS account ID.
func (b *InMemoryBackend) AccountID() string {
	b.mu.RLock("AccountID")
	defer b.mu.RUnlock()

	return b.accountID
}

// Region returns the configured AWS region.
func (b *InMemoryBackend) Region() string {
	b.mu.RLock("Region")
	defer b.mu.RUnlock()

	return b.region
}

// -- Organization operations --

// CreateOrganization creates a new organization.
func (b *InMemoryBackend) CreateOrganization(featureSet string) (*Organization, *Root, error) {
	b.mu.Lock("CreateOrganization")
	defer b.mu.Unlock()

	if b.org != nil {
		return nil, nil, ErrOrgAlreadyExists
	}

	if featureSet == "" {
		featureSet = "ALL"
	}

	if featureSet != "ALL" && featureSet != "CONSOLIDATED_BILLING" {
		return nil, nil, ErrInvalidInput
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
	b.mu.RLock("DescribeOrganization")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	org := copyOrg(b.org)

	// Populate AvailablePolicyTypes: all valid types, ENABLED for those in root.PolicyTypes.
	enabledTypes := make(map[string]bool)
	if b.root != nil {
		for _, pt := range b.root.PolicyTypes {
			if pt.Status == policyStatusEnabled {
				enabledTypes[pt.Type] = true
			}
		}
	}

	pts := make([]PolicyTypeSummary, 0, len(validPolicyTypes()))
	for _, vpt := range validPolicyTypes() {
		status := "DISABLED"
		if enabledTypes[vpt] {
			status = policyStatusEnabled
		}
		pts = append(pts, PolicyTypeSummary{Type: vpt, Status: status})
	}
	org.AvailablePolicyTypes = pts

	return org, nil
}

// DeleteOrganization removes the organization.
func (b *InMemoryBackend) DeleteOrganization() error {
	b.mu.Lock("DeleteOrganization")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	// AWS rejects deletion when member accounts (other than the management account) still exist.
	for acctID := range b.accounts {
		if acctID != b.org.MasterAccountID {
			return ErrOrganizationNotEmpty
		}
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
	b.handshakes = make(map[string]*Handshake)
	b.emailToAccountID = make(map[string]string)
	b.resourcePolicy = nil
	b.accountCounter = managementAccountCounter

	return nil
}

// -- Account operations --

// createAccountLocked creates an account and status record.
// Must be called with the write lock held.
// Returns nil if the email already exists (duplicate email).
func (b *InMemoryBackend) createAccountLocked(
	name, email, roleName, iamUserAccessToBilling string,
	acctIDFn func(counter int) string,
	govCloudID string,
	tags []Tag,
) *CreateAccountStatus {
	// Check for duplicate email.
	if b.emailToAccountID != nil {
		if _, exists := b.emailToAccountID[email]; exists {
			return nil
		}
	}

	b.accountCounter++
	acctID := acctIDFn(b.accountCounter)

	now := time.Now()
	acct := &Account{
		ID:                     acctID,
		ARN:                    b.accountARN(b.org.ID, acctID),
		Name:                   name,
		Email:                  email,
		Status:                 accountStatusActive,
		JoinedMethod:           joinedMethodCreated,
		JoinedAt:               now,
		RoleName:               roleName,
		IamUserAccessToBilling: iamUserAccessToBilling,
	}

	b.accounts[acctID] = acct
	b.accountParent[acctID] = b.root.ID
	b.setTagsLocked(acctID, tags)

	if b.emailToAccountID == nil {
		b.emailToAccountID = make(map[string]string)
	}
	b.emailToAccountID[email] = acctID

	b.statusCounter++
	statusID := fmt.Sprintf("car-%012d", b.statusCounter)

	status := &CreateAccountStatus{
		ID:                 statusID,
		AccountID:          acctID,
		GovCloudAccountID:  govCloudID,
		AccountName:        name,
		State:              createAccountStateSucceeded,
		RequestedTimestamp: epochSeconds(now),
		CompletedTimestamp: epochSeconds(now),
	}

	b.createStatuses[statusID] = status

	return status
}

// CreateAccount creates a new account and returns its status.
func (b *InMemoryBackend) CreateAccount(
	name, email, roleName, iamUserAccessToBilling string,
	tags []Tag,
) (*CreateAccountStatus, error) {
	b.mu.Lock("CreateAccount")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	status := b.createAccountLocked(name, email, roleName, iamUserAccessToBilling, newAccountID, "", tags)
	if status == nil {
		return nil, ErrInvalidInput
	}

	return status, nil
}

// DescribeCreateAccountStatus returns the status of a CreateAccount request.
func (b *InMemoryBackend) DescribeCreateAccountStatus(
	requestID string,
) (*CreateAccountStatus, error) {
	b.mu.RLock("DescribeCreateAccountStatus")
	defer b.mu.RUnlock()

	s, ok := b.createStatuses[requestID]
	if !ok {
		return nil, ErrCreateAccountStatusNotFound
	}

	return s, nil
}

// DescribeAccount returns an account by ID.
func (b *InMemoryBackend) DescribeAccount(accountID string) (*Account, error) {
	b.mu.RLock("DescribeAccount")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	a, ok := b.accounts[accountID]
	if !ok {
		return nil, ErrAccountNotFound
	}

	return copyAccount(a), nil
}
func (b *InMemoryBackend) ListAccounts() ([]*Account, error) {
	b.mu.RLock("ListAccounts")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	out := make([]*Account, 0, len(b.accounts))
	for _, a := range b.accounts {
		out = append(out, copyAccount(a))
	}

	slices.SortFunc(out, func(a, b *Account) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// RemoveAccountFromOrganization removes an account from the organization.
func (b *InMemoryBackend) RemoveAccountFromOrganization(accountID string) error {
	b.mu.Lock("RemoveAccountFromOrganization")
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

	acct := b.accounts[accountID]

	// Clean policyTargets reverse mapping: remove accountID from each policy's target list.
	for _, policyID := range b.targetPolicies[accountID] {
		b.policyTargets[policyID] = removeString(b.policyTargets[policyID], accountID)
	}

	delete(b.accounts, accountID)
	delete(b.accountParent, accountID)
	delete(b.tags, accountID)
	delete(b.targetPolicies, accountID)

	for svcPrincipal, admins := range b.delegatedAdmins {
		delete(admins, accountID)
		if len(admins) == 0 {
			delete(b.delegatedAdmins, svcPrincipal)
		}
	}

	// For INVITED accounts, generate a terminal LEAVE_ORGANIZATION handshake record.
	if b.org != nil && acct != nil && acct.JoinedMethod == joinedMethodInvited {
		now := time.Now()
		hID := newHandshakeID()
		h := &Handshake{
			ID:                  hID,
			ARN:                 b.handshakeARN(b.org.ID, handshakeActionLeave, hID),
			Action:              handshakeActionLeave,
			State:               handshakeStateAccepted,
			RequestedTimestamp:  now,
			ExpirationTimestamp: now.Add(handshakeExpirationDuration),
			Parties: []HandshakeParty{
				{ID: b.org.ID, Type: "ORGANIZATION"},
				{ID: accountID, Type: "ACCOUNT"},
			},
		}
		b.handshakes[hID] = h
	}

	return nil
}

// MoveAccount moves an account from one parent to another.
func (b *InMemoryBackend) MoveAccount(accountID, sourceParentID, destParentID string) error {
	b.mu.Lock("MoveAccount")
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

// CloseAccount marks an account as PENDING_CLOSURE.
func (b *InMemoryBackend) CloseAccount(accountID string) error {
	b.mu.Lock("CloseAccount")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	acct, ok := b.accounts[accountID]
	if !ok {
		return ErrAccountNotFound
	}

	if accountID == b.org.MasterAccountID {
		return ErrInvalidInput
	}

	if acct.Status == accountStatusPendingClosure || acct.Status == accountStatusSuspended {
		return ErrAccountAlreadyClosed
	}

	acct.Status = accountStatusPendingClosure

	return nil
}

// CreateGovCloudAccount creates a commercial account paired with a GovCloud account.
func (b *InMemoryBackend) CreateGovCloudAccount(
	name, email, roleName, iamUserAccessToBilling string,
	tags []Tag,
) (*CreateAccountStatus, error) {
	b.mu.Lock("CreateGovCloudAccount")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	// Pre-calculate the GovCloud account ID using the next counter value.
	govCloudID := newGovCloudAccountID(b.accountCounter + 1)

	status := b.createAccountLocked(name, email, roleName, iamUserAccessToBilling, newAccountID, govCloudID, tags)
	if status == nil {
		return nil, ErrInvalidInput
	}

	return status, nil
}

// parentExists checks if a parentID refers to the root or an existing OU.
func (b *InMemoryBackend) parentExists(parentID string) bool {
	if b.root != nil && b.root.ID == parentID {
		return true
	}

	_, ok := b.ous[parentID]

	return ok
}

// targetExistsLocked checks if a targetID refers to the root, an OU, or an account.
// Must be called with lock held.
func (b *InMemoryBackend) targetExistsLocked(targetID string) bool {
	if b.root != nil && b.root.ID == targetID {
		return true
	}

	if _, ok := b.ous[targetID]; ok {
		return true
	}

	if _, ok := b.accounts[targetID]; ok {
		return true
	}

	return false
}

// resourceExistsLocked checks if a resourceID refers to root, OU, account, or policy.
// Must be called with lock held.
func (b *InMemoryBackend) resourceExistsLocked(resourceID string) bool {
	if b.root != nil && b.root.ID == resourceID {
		return true
	}

	if b.org != nil && b.org.ID == resourceID {
		return true
	}

	if _, ok := b.ous[resourceID]; ok {
		return true
	}

	if _, ok := b.accounts[resourceID]; ok {
		return true
	}

	if _, ok := b.policies[resourceID]; ok {
		return true
	}

	return false
}

// -- Root operations --

// ListRoots returns the organization roots.
func (b *InMemoryBackend) ListRoots() ([]*Root, error) {
	b.mu.RLock("ListRoots")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	return []*Root{copyRoot(b.root)}, nil
}

// -- OU operations --

// ouDepthLocked computes the depth of a parent node (root = 0, direct child = 1, etc.)
// Must be called with lock held.
func (b *InMemoryBackend) ouDepthLocked(parentID string) int {
	depth := 0
	current := parentID

	for {
		if b.root != nil && current == b.root.ID {
			return depth
		}

		if parentOfCurrent, ok := b.ouParent[current]; ok {
			depth++
			current = parentOfCurrent
		} else {
			return depth
		}
	}
}

// CreateOrganizationalUnit creates a new OU under the given parent.
func (b *InMemoryBackend) CreateOrganizationalUnit(
	parentID, name string,
	tags []Tag,
) (*OrganizationalUnit, error) {
	b.mu.Lock("CreateOrganizationalUnit")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !b.parentExists(parentID) {
		return nil, ErrInvalidInput
	}

	// Depth limit: root is depth 0, OUs are depth 1-5; creating at depth 6 is rejected.
	// parentID is the parent; the new OU's depth = ouDepthLocked(parentID) + 1.
	// If the parent's depth is already maxOUDepth, the new OU would be at depth 6, which is invalid.
	if b.ouDepthLocked(parentID) >= maxOUDepth {
		return nil, ErrOUDepthLimitExceeded
	}

	// Name uniqueness: no sibling OU under the same parent may have the same name.
	for _, ou := range b.ous {
		if ou.ParentID == parentID && ou.Name == name {
			return nil, ErrDuplicateOrganizationalUnit
		}
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
	b.mu.RLock("DescribeOrganizationalUnit")
	defer b.mu.RUnlock()

	ou, ok := b.ous[ouID]
	if !ok {
		return nil, ErrOUNotFound
	}

	return copyOU(ou), nil
}

// DeleteOrganizationalUnit removes an OU.
func (b *InMemoryBackend) DeleteOrganizationalUnit(ouID string) error {
	b.mu.Lock("DeleteOrganizationalUnit")
	defer b.mu.Unlock()

	if _, ok := b.ous[ouID]; !ok {
		return ErrOUNotFound
	}

	// AWS rejects deletion of OUs that still contain accounts.
	for _, parentID := range b.accountParent {
		if parentID == ouID {
			return ErrInvalidInput
		}
	}

	// AWS rejects deletion of OUs that still contain child OUs.
	for _, parentID := range b.ouParent {
		if parentID == ouID {
			return ErrInvalidInput
		}
	}

	delete(b.ous, ouID)
	delete(b.ouParent, ouID)
	delete(b.tags, ouID)
	delete(b.targetPolicies, ouID)

	return nil
}

// UpdateOrganizationalUnit renames an OU.
func (b *InMemoryBackend) UpdateOrganizationalUnit(ouID, name string) (*OrganizationalUnit, error) {
	b.mu.Lock("UpdateOrganizationalUnit")
	defer b.mu.Unlock()

	ou, ok := b.ous[ouID]
	if !ok {
		return nil, ErrOUNotFound
	}

	// Name uniqueness: no sibling OU under the same parent may have the same name (excluding self).
	if name != "" && name != ou.Name {
		parentID := b.ouParent[ouID]
		for id, sibling := range b.ous {
			if id != ouID && sibling.ParentID == parentID && sibling.Name == name {
				return nil, ErrDuplicateOrganizationalUnit
			}
		}
	}

	ou.Name = name

	return copyOU(ou), nil
}

// ListOrganizationalUnitsForParent returns all OUs under a parent.
func (b *InMemoryBackend) ListOrganizationalUnitsForParent(
	parentID string,
) ([]*OrganizationalUnit, error) {
	b.mu.RLock("ListOrganizationalUnitsForParent")
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
			out = append(out, copyOU(ou))
		}
	}

	slices.SortFunc(out, func(a, b *OrganizationalUnit) int { return cmp.Compare(a.Name, b.Name) })

	return out, nil
}

// ListAccountsForParent returns all accounts directly under a parent.
func (b *InMemoryBackend) ListAccountsForParent(parentID string) ([]*Account, error) {
	b.mu.RLock("ListAccountsForParent")
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
				out = append(out, copyAccount(a))
			}
		}
	}

	slices.SortFunc(out, func(a, b *Account) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// ListParents returns the parents of an account or OU.
func (b *InMemoryBackend) ListParents(childID string) ([]ParentSummary, error) {
	b.mu.RLock("ListParents")
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

// resolveParentType returns "ROOT" or targetTypeOU for a given parent ID.
func (b *InMemoryBackend) resolveParentType(parentID string) string {
	if b.root != nil && b.root.ID == parentID {
		return "ROOT"
	}

	return targetTypeOU
}

// ListChildren returns children of a given type under a parent.
func (b *InMemoryBackend) ListChildren(parentID, childType string) ([]ChildSummary, error) {
	b.mu.RLock("ListChildren")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !b.parentExists(parentID) {
		return nil, ErrInvalidInput
	}

	var out []ChildSummary

	switch childType {
	case targetTypeAccount:
		for acctID, pid := range b.accountParent {
			if pid == parentID {
				out = append(out, ChildSummary{ID: acctID, Type: targetTypeAccount})
			}
		}
	case targetTypeOU:
		for ouID, ou := range b.ous {
			if ou.ParentID == parentID {
				out = append(out, ChildSummary{ID: ouID, Type: targetTypeOU})
			}
		}
	default:
		return nil, ErrInvalidInput
	}

	slices.SortFunc(out, func(a, b ChildSummary) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// -- Policy operations --

// CreatePolicy creates a new policy.
func (b *InMemoryBackend) CreatePolicy(
	name, description, content, policyType string,
	tags []Tag,
) (*Policy, error) {
	b.mu.Lock("CreatePolicy")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !slices.Contains(validPolicyTypes(), policyType) {
		return nil, ErrInvalidInput
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
	b.mu.RLock("DescribePolicy")
	defer b.mu.RUnlock()

	p, ok := b.policies[policyID]
	if !ok {
		return nil, ErrPolicyNotFound
	}

	return copyPolicy(p), nil
}

// UpdatePolicy updates a policy.
func (b *InMemoryBackend) UpdatePolicy(
	policyID, name, description, content string,
) (*Policy, error) {
	b.mu.Lock("UpdatePolicy")
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

	return copyPolicy(p), nil
}

// DeletePolicy removes a policy.
func (b *InMemoryBackend) DeletePolicy(policyID string) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	if _, ok := b.policies[policyID]; !ok {
		return ErrPolicyNotFound
	}

	// AWS rejects deletion of policies that are still attached to targets.
	if len(b.policyTargets[policyID]) > 0 {
		return ErrPolicyInUse
	}

	delete(b.policyTargets, policyID)
	delete(b.policies, policyID)
	delete(b.tags, policyID)

	return nil
}

// ListPolicies returns all policies of a given type.
// AWS requires a non-empty Filter; empty filter returns InvalidInputException.
func (b *InMemoryBackend) ListPolicies(filter string) ([]*Policy, error) {
	b.mu.RLock("ListPolicies")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if filter == "" {
		return nil, ErrInvalidInput
	}

	var out []*Policy

	for _, p := range b.policies {
		if p.PolicySummary.Type == filter {
			out = append(out, copyPolicy(p))
		}
	}

	slices.SortFunc(
		out,
		func(a, b *Policy) int { return cmp.Compare(a.PolicySummary.Name, b.PolicySummary.Name) },
	)

	return out, nil
}

// AttachPolicy attaches a policy to a target.
func (b *InMemoryBackend) AttachPolicy(policyID, targetID string) error {
	b.mu.Lock("AttachPolicy")
	defer b.mu.Unlock()

	policy, ok := b.policies[policyID]
	if !ok {
		return ErrPolicyNotFound
	}

	// Validate target exists.
	if !b.targetExistsLocked(targetID) {
		return ErrTargetNotFound
	}

	targets := b.policyTargets[policyID]
	if slices.Contains(targets, targetID) {
		return ErrDuplicatePolicyAttachment
	}

	// Enforce per-target, per-policy-type limit (AWS limit is 5).
	policyType := policy.PolicySummary.Type
	typeCount := 0

	for _, attachedPolicyID := range b.targetPolicies[targetID] {
		if p, exists := b.policies[attachedPolicyID]; exists && p.PolicySummary.Type == policyType {
			typeCount++
		}
	}

	if typeCount >= maxPoliciesPerTarget {
		return ErrPolicyLimitExceeded
	}

	b.policyTargets[policyID] = append(targets, targetID)
	b.targetPolicies[targetID] = append(b.targetPolicies[targetID], policyID)

	return nil
}

// DetachPolicy detaches a policy from a target.
func (b *InMemoryBackend) DetachPolicy(policyID, targetID string) error {
	b.mu.Lock("DetachPolicy")
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
// AWS requires a non-empty Filter; empty filter returns InvalidInputException.
func (b *InMemoryBackend) ListPoliciesForTarget(targetID, filter string) ([]*Policy, error) {
	b.mu.RLock("ListPoliciesForTarget")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if filter == "" {
		return nil, ErrInvalidInput
	}

	policyIDs := b.targetPolicies[targetID]

	var out []*Policy

	for _, pid := range policyIDs {
		if p, ok := b.policies[pid]; ok {
			if p.PolicySummary.Type == filter {
				out = append(out, copyPolicy(p))
			}
		}
	}

	return out, nil
}

// ListTargetsForPolicy returns targets that a policy is attached to.
func (b *InMemoryBackend) ListTargetsForPolicy(policyID string) ([]PolicyTargetSummary, error) {
	b.mu.RLock("ListTargetsForPolicy")
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

	slices.SortFunc(
		out,
		func(a, b PolicyTargetSummary) int { return cmp.Compare(a.TargetID, b.TargetID) },
	)

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
			Type:     targetTypeOU,
		}
	}

	if acct, ok := b.accounts[targetID]; ok {
		return PolicyTargetSummary{
			TargetID: targetID,
			ARN:      acct.ARN,
			Name:     acct.Name,
			Type:     targetTypeAccount,
		}
	}

	return PolicyTargetSummary{TargetID: targetID, Type: targetTypeAccount}
}

// EnablePolicyType enables a policy type on the root.
func (b *InMemoryBackend) EnablePolicyType(rootID, policyType string) (*Root, error) {
	b.mu.Lock("EnablePolicyType")
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

	return copyRoot(b.root), nil
}

// DisablePolicyType disables a policy type on the root.
func (b *InMemoryBackend) DisablePolicyType(rootID, policyType string) (*Root, error) {
	b.mu.Lock("DisablePolicyType")
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

	// AWS rejects disabling a policy type when policies of that type are still attached to any target.
	for policyID, targets := range b.policyTargets {
		if len(targets) > 0 {
			if p, ok := b.policies[policyID]; ok && p.PolicySummary.Type == policyType {
				return nil, ErrPolicyTypeAttached
			}
		}
	}

	b.root.PolicyTypes = newTypes

	return copyRoot(b.root), nil
}

// -- Tag operations --

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceID string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if !b.resourceExistsLocked(resourceID) {
		return ErrInvalidInput
	}

	b.setTagsLocked(resourceID, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceID string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if !b.resourceExistsLocked(resourceID) {
		return ErrInvalidInput
	}

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
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !b.resourceExistsLocked(resourceID) {
		return nil, ErrInvalidInput
	}

	t := b.tags[resourceID]
	out := make([]Tag, 0, len(t))

	for k, v := range t {
		out = append(out, Tag{Key: k, Value: v})
	}

	slices.SortFunc(out, func(a, b Tag) int { return cmp.Compare(a.Key, b.Key) })

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
	b.mu.Lock("EnableAWSServiceAccess")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	b.serviceAccess[servicePrincipal] = time.Now()

	return nil
}

// DisableAWSServiceAccess disables a service principal.
func (b *InMemoryBackend) DisableAWSServiceAccess(servicePrincipal string) error {
	b.mu.Lock("DisableAWSServiceAccess")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	delete(b.serviceAccess, servicePrincipal)

	return nil
}

// ListAWSServiceAccessForOrganization returns enabled service principals.
func (b *InMemoryBackend) ListAWSServiceAccessForOrganization() ([]EnabledServicePrincipal, error) {
	b.mu.RLock("ListAWSServiceAccessForOrganization")
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

	slices.SortFunc(out, func(a, b EnabledServicePrincipal) int {
		return cmp.Compare(a.ServicePrincipal, b.ServicePrincipal)
	})

	return out, nil
}

// -- Delegated Admin --

// RegisterDelegatedAdministrator registers a delegated admin for a service.
func (b *InMemoryBackend) RegisterDelegatedAdministrator(accountID, servicePrincipal string) error {
	b.mu.Lock("RegisterDelegatedAdministrator")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	// Reject management account.
	if accountID == b.org.MasterAccountID {
		return ErrInvalidInput
	}

	// Require service access to be enabled first.
	if _, enabled := b.serviceAccess[servicePrincipal]; !enabled {
		return ErrServiceNotEnabled
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
func (b *InMemoryBackend) DeregisterDelegatedAdministrator(
	accountID, servicePrincipal string,
) error {
	b.mu.Lock("DeregisterDelegatedAdministrator")
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
func (b *InMemoryBackend) ListDelegatedAdministrators(
	servicePrincipal string,
) ([]*DelegatedAdmin, error) {
	b.mu.RLock("ListDelegatedAdministrators")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	var out []*DelegatedAdmin

	if servicePrincipal != "" {
		for _, da := range b.delegatedAdmins[servicePrincipal] {
			out = append(out, da)
		}
	} else {
		for _, admins := range b.delegatedAdmins {
			for _, da := range admins {
				out = append(out, da)
			}
		}
	}

	slices.SortFunc(
		out,
		func(a, b *DelegatedAdmin) int { return cmp.Compare(a.AccountID, b.AccountID) },
	)

	return out, nil
}

// -- Handshake operations --

// AcceptHandshake accepts an OPEN handshake.
// For INVITE handshakes, the invited account is added to the organization.
func (b *InMemoryBackend) AcceptHandshake(handshakeID string) (*Handshake, error) {
	b.mu.Lock("AcceptHandshake")
	defer b.mu.Unlock()

	h, ok := b.handshakes[handshakeID]
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	if h.State != handshakeStateOpen {
		return nil, ErrHandshakeConstraintViolation
	}

	h.State = handshakeStateAccepted

	if h.Action == handshakeActionInvite && b.org != nil {
		for _, r := range h.Resources {
			if r.Type == targetTypeAccount {
				acctID := r.Value
				if _, exists := b.accounts[acctID]; !exists {
					now := time.Now()
					acct := &Account{
						ID:           acctID,
						ARN:          b.accountARN(b.org.ID, acctID),
						Name:         acctID,
						Email:        acctID + "@invited.example.com",
						Status:       accountStatusActive,
						JoinedMethod: joinedMethodInvited,
						JoinedAt:     now,
					}
					b.accounts[acctID] = acct
					b.accountParent[acctID] = b.root.ID
				}

				break
			}
		}
	}

	return copyHandshake(h), nil
}

// CancelHandshake cancels an OPEN handshake.
func (b *InMemoryBackend) CancelHandshake(handshakeID string) (*Handshake, error) {
	b.mu.Lock("CancelHandshake")
	defer b.mu.Unlock()

	h, ok := b.handshakes[handshakeID]
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	if h.State != handshakeStateOpen {
		return nil, ErrHandshakeConstraintViolation
	}

	h.State = handshakeStateCanceled

	return copyHandshake(h), nil
}

// DeclineHandshake declines an OPEN handshake.
func (b *InMemoryBackend) DeclineHandshake(handshakeID string) (*Handshake, error) {
	b.mu.Lock("DeclineHandshake")
	defer b.mu.Unlock()

	h, ok := b.handshakes[handshakeID]
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	if h.State != handshakeStateOpen {
		return nil, ErrHandshakeConstraintViolation
	}

	h.State = handshakeStateDeclined

	return copyHandshake(h), nil
}

// DescribeHandshake returns a handshake by ID.
func (b *InMemoryBackend) DescribeHandshake(handshakeID string) (*Handshake, error) {
	b.mu.Lock("DescribeHandshake")
	defer b.mu.Unlock()

	b.expireStaleHandshakesLocked()

	h, ok := b.handshakes[handshakeID]
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	return copyHandshake(h), nil
}

// DescribeResponsibilityTransfer returns a responsibility-transfer handshake by ID.
func (b *InMemoryBackend) DescribeResponsibilityTransfer(handshakeID string) (*Handshake, error) {
	b.mu.RLock("DescribeResponsibilityTransfer")
	defer b.mu.RUnlock()

	h, ok := b.handshakes[handshakeID]
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	return copyHandshake(h), nil
}

// AddHandshakeInternal seeds a handshake directly for testing.
// If h.ID is empty, a new ID is generated. If h.ExpirationTimestamp is zero,
// it is set to now + handshakeExpirationDuration.
func (b *InMemoryBackend) AddHandshakeInternal(h *Handshake) {
	b.mu.Lock("AddHandshakeInternal")
	defer b.mu.Unlock()

	if h.ID == "" {
		h.ID = newHandshakeID()
	}

	if h.ExpirationTimestamp.IsZero() {
		h.ExpirationTimestamp = time.Now().Add(handshakeExpirationDuration)
	}

	if h.ARN == "" && b.org != nil {
		action := h.Action
		if action == "" {
			action = handshakeActionInvite
		}

		h.ARN = b.handshakeARN(b.org.ID, action, h.ID)
	}

	b.handshakes[h.ID] = h
}

// -- ResourcePolicy operations --

// DeleteResourcePolicy removes the organization resource policy.
func (b *InMemoryBackend) DeleteResourcePolicy() error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if b.resourcePolicy == nil {
		return ErrResourcePolicyNotFound
	}

	b.resourcePolicy = nil

	return nil
}

// DescribeResourcePolicy returns the organization resource policy.
func (b *InMemoryBackend) DescribeResourcePolicy() (*ResourcePolicy, error) {
	b.mu.RLock("DescribeResourcePolicy")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if b.resourcePolicy == nil {
		return nil, ErrResourcePolicyNotFound
	}

	cp := *b.resourcePolicy

	return &cp, nil
}

// PutResourcePolicy creates or replaces the organization resource policy.
func (b *InMemoryBackend) PutResourcePolicy(content string) (*ResourcePolicy, error) {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	rpID := "p-rp-default"

	rp := &ResourcePolicy{
		ID:      rpID,
		ARN:     b.resourcePolicyARN(b.org.ID),
		Content: content,
	}

	b.resourcePolicy = rp

	cp := *rp

	return &cp, nil
}

// -- EffectivePolicy operations --

// DescribeEffectivePolicy returns the effective policy of a given type for a target.
func (b *InMemoryBackend) DescribeEffectivePolicy(
	policyType, targetID string,
) (*EffectivePolicy, error) {
	b.mu.RLock("DescribeEffectivePolicy")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if targetID == "" {
		targetID = b.org.MasterAccountID
	}

	content, policyID := b.findEffectivePolicyLocked(policyType, targetID)
	if content == "" {
		return nil, ErrEffectivePolicyNotFound
	}

	return &EffectivePolicy{
		PolicyContent:        content,
		PolicyID:             policyID,
		PolicyType:           policyType,
		TargetID:             targetID,
		LastUpdatedTimestamp: time.Now(),
	}, nil
}

// findEffectivePolicyLocked walks the full hierarchy from targetID up to root,
// collecting all policies of policyType, then merges them per AWS rules.
// For TAG_POLICY/BACKUP_POLICY/AISERVICES_OPT_OUT_POLICY: deep-merge JSON objects
// (child overrides parent). For all other types (SCP/RCP): child takes precedence
// (last attached at each level wins; root-level policies form the baseline).
// Returns (mergedContent, lastPolicyID) or ("","") if no policies found.
// Must be called with a read lock held.
func (b *InMemoryBackend) findEffectivePolicyLocked(policyType, targetID string) (string, string) {
	chain := b.collectPolicyChainLocked(policyType, targetID)
	if len(chain) == 0 {
		return "", ""
	}

	return b.mergePolicyChain(policyType, chain)
}

// effectivePolicyEntry holds a single policy in the effective-policy chain.
type effectivePolicyEntry struct {
	id      string
	content string
}

// collectPolicyChainLocked walks from targetID up to root, collecting all policies
// of policyType in order (target first, root last).
// Must be called with a read lock held.
func (b *InMemoryBackend) collectPolicyChainLocked(policyType, targetID string) []effectivePolicyEntry {
	var chain []effectivePolicyEntry

	current := targetID

	for current != "" {
		for _, pid := range b.targetPolicies[current] {
			if p, ok := b.policies[pid]; ok && p.PolicySummary.Type == policyType {
				chain = append(chain, effectivePolicyEntry{id: p.PolicySummary.ID, content: p.Content})
			}
		}

		if b.root != nil && current == b.root.ID {
			break
		}

		if parentID, ok := b.accountParent[current]; ok {
			current = parentID

			continue
		}

		if parentID, ok := b.ouParent[current]; ok {
			current = parentID

			continue
		}

		break
	}

	return chain
}

// mergePolicyChain merges a chain of policies per AWS effective-policy rules.
// For TAG_POLICY/BACKUP_POLICY/AISERVICES_OPT_OUT_POLICY: deep-merge JSON objects
// (root is base, child overrides). For all other types: child wins (first entry).
func (b *InMemoryBackend) mergePolicyChain(policyType string, chain []effectivePolicyEntry) (string, string) {
	if len(chain) == 1 {
		return chain[0].content, chain[0].id
	}

	switch policyType {
	case "TAG_POLICY", "BACKUP_POLICY", "AISERVICES_OPT_OUT_POLICY":
		return b.mergeTagStyleChain(chain)
	}

	return chain[0].content, chain[0].id
}

// mergeTagStyleChain merges TAG_POLICY-style policy chains: merge from root to target,
// child overrides parent (last write wins per key).
func (b *InMemoryBackend) mergeTagStyleChain(chain []effectivePolicyEntry) (string, string) {
	merged := make(map[string]any)

	for _, entry := range slices.Backward(chain) {
		var obj map[string]any
		if err := json.Unmarshal([]byte(entry.content), &obj); err == nil {
			mergeJSONObjects(merged, obj)
		}
	}

	if data, err := json.Marshal(merged); err == nil {
		return string(data), chain[0].id
	}

	return chain[0].content, chain[0].id
}

// expireStaleHandshakesLocked transitions OPEN handshakes past their ExpirationTimestamp to EXPIRED.
// Must be called with a write lock held.
func (b *InMemoryBackend) expireStaleHandshakesLocked() {
	now := time.Now()
	for _, h := range b.handshakes {
		if h.State == handshakeStateOpen && !h.ExpirationTimestamp.IsZero() && now.After(h.ExpirationTimestamp) {
			h.State = handshakeStateExpired
		}
	}
}

// -- Helpers --

// removeString returns a copy of s with all occurrences of v removed.
func removeString(s []string, v string) []string {
	return slices.DeleteFunc(slices.Clone(s), func(x string) bool { return x == v })
}

// mergeJSONObjects merges src into dst in-place, overwriting conflicting keys.
// Nested maps are merged recursively; all other types are overwritten.
func mergeJSONObjects(dst, src map[string]any) {
	for k, srcVal := range src {
		srcMap, srcIsMap := srcVal.(map[string]any)
		dstMap, dstIsMap := dst[k].(map[string]any)

		if srcIsMap && dstIsMap {
			mergeJSONObjects(dstMap, srcMap)

			continue
		}

		dst[k] = srcVal
	}
}

// copyOrg returns a deep copy of org (including slice fields).
func copyOrg(org *Organization) *Organization {
	cp := *org

	if org.AvailablePolicyTypes != nil {
		cp.AvailablePolicyTypes = make([]PolicyTypeSummary, len(org.AvailablePolicyTypes))
		copy(cp.AvailablePolicyTypes, org.AvailablePolicyTypes)
	}

	return &cp
}

// copyAccount returns a value copy of account (all fields are scalars).
func copyAccount(a *Account) *Account {
	cp := *a

	return &cp
}

// copyOU returns a value copy of an OrganizationalUnit (all fields are scalars).
func copyOU(ou *OrganizationalUnit) *OrganizationalUnit {
	cp := *ou

	return &cp
}

// copyPolicy returns a value copy of a Policy (all fields are scalars).
func copyPolicy(p *Policy) *Policy {
	cp := *p

	return &cp
}

// copyRoot returns a value copy of Root, including its PolicyTypes slice.
func copyRoot(r *Root) *Root {
	cp := *r

	if r.PolicyTypes != nil {
		cp.PolicyTypes = make([]PolicyTypeSummary, len(r.PolicyTypes))
		copy(cp.PolicyTypes, r.PolicyTypes)
	}

	return &cp
}

// copyHandshake returns a deep copy of a Handshake.
func copyHandshake(h *Handshake) *Handshake {
	cp := *h

	if h.Parties != nil {
		cp.Parties = make([]HandshakeParty, len(h.Parties))
		copy(cp.Parties, h.Parties)
	}

	cp.Resources = copyHandshakeResources(h.Resources)

	return &cp
}

// copyHandshakeResources returns a deep copy of a HandshakeResource slice.
func copyHandshakeResources(rs []HandshakeResource) []HandshakeResource {
	if rs == nil {
		return nil
	}

	out := make([]HandshakeResource, len(rs))

	for i, r := range rs {
		out[i] = HandshakeResource{
			Type:      r.Type,
			Value:     r.Value,
			Resources: copyHandshakeResources(r.Resources),
		}
	}

	return out
}

// EnsureOrgExists returns ErrOrgNotFound if no org exists (for operations that require it).
func (b *InMemoryBackend) EnsureOrgExists() error {
	b.mu.RLock("EnsureOrgExists")
	defer b.mu.RUnlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	return nil
}

// EnableAllFeatures creates an ENABLE_ALL_FEATURES handshake and returns it.
// Real AWS sends this handshake to all member accounts to approve the feature upgrade.
func (b *InMemoryBackend) EnableAllFeatures() (*Handshake, error) {
	b.mu.Lock("EnableAllFeatures")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	now := time.Now()
	id := newHandshakeID()
	h := &Handshake{
		ID:                  id,
		ARN:                 b.handshakeARN(b.org.ID, handshakeActionEnableFeatures, id),
		Action:              handshakeActionEnableFeatures,
		State:               handshakeStateOpen,
		RequestedTimestamp:  now,
		ExpirationTimestamp: now.Add(handshakeExpirationDuration),
		Parties: []HandshakeParty{
			{ID: b.org.MasterAccountID, Type: targetTypeAccount},
		},
		Resources: []HandshakeResource{
			{Type: handshakeResourceOrg, Value: b.org.ID},
			{Type: handshakeResourceMasterEmail, Value: b.org.MasterAccountEmail},
		},
	}

	b.handshakes[id] = h

	return copyHandshake(h), nil
}

// Ensure errors are used somewhere to satisfy linter.
var _ = errors.Is(ErrOrgNotFound, awserr.ErrNotFound)

// Reset clears all organization state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

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
	b.handshakes = make(map[string]*Handshake)
	b.emailToAccountID = make(map[string]string)
	b.resourcePolicy = nil
	b.accountCounter = managementAccountCounter
	b.statusCounter = 0
}

// -- New operations --

// InviteAccountToOrganization creates an OPEN invitation handshake targeting an account.
func (b *InMemoryBackend) InviteAccountToOrganization(
	target HandshakeParty,
	notes string,
) (*Handshake, error) {
	b.mu.Lock("InviteAccountToOrganization")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if target.ID == "" {
		return nil, ErrInvalidInput
	}

	// Validate target party.
	switch target.Type {
	case targetTypeAccount:
		if len(target.ID) != accountIDLength {
			return nil, ErrInvalidInput
		}
	case "EMAIL":
		if !strings.Contains(target.ID, "@") {
			return nil, ErrInvalidInput
		}
	default:
		if target.Type != "" {
			return nil, ErrInvalidInput
		}
	}

	// AWS rejects duplicate open invitations to the same target.
	for _, existing := range b.handshakes {
		if existing.State != handshakeStateOpen || existing.Action != handshakeActionInvite {
			continue
		}
		for _, r := range existing.Resources {
			if r.Type == targetTypeAccount && r.Value == target.ID {
				return nil, ErrDuplicateHandshake
			}
		}
	}

	now := time.Now()
	id := newHandshakeID()
	h := &Handshake{
		ID:                  id,
		ARN:                 b.handshakeARN(b.org.ID, handshakeActionInvite, id),
		Action:              handshakeActionInvite,
		State:               handshakeStateOpen,
		RequestedTimestamp:  now,
		ExpirationTimestamp: now.Add(handshakeExpirationDuration),
		Parties: []HandshakeParty{
			{ID: b.org.MasterAccountID, Type: targetTypeAccount},
			{ID: target.ID, Type: target.Type},
		},
		Resources: []HandshakeResource{
			{Type: targetTypeAccount, Value: target.ID},
			{Type: handshakeResourceOrg, Value: b.org.ID},
			{Type: handshakeResourceMasterEmail, Value: b.org.MasterAccountEmail},
		},
	}

	if notes != "" {
		h.Resources = append(h.Resources, HandshakeResource{Type: handshakeResourceNotes, Value: notes})
	}

	b.handshakes[id] = h

	return copyHandshake(h), nil
}

// LeaveOrganization removes the management account from the organization (stub: returns no error).
func (b *InMemoryBackend) LeaveOrganization() error {
	b.mu.RLock("LeaveOrganization")
	defer b.mu.RUnlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	return nil
}

// ListHandshakesForAccount returns all handshakes visible to the calling account.
// actionTypeFilter optionally restricts results to handshakes with the given Action value.
func (b *InMemoryBackend) ListHandshakesForAccount(actionTypeFilter string) ([]*Handshake, error) {
	b.mu.Lock("ListHandshakesForAccount")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	b.expireStaleHandshakesLocked()

	out := make([]*Handshake, 0, len(b.handshakes))

	for _, h := range b.handshakes {
		if actionTypeFilter == "" || h.Action == actionTypeFilter {
			out = append(out, copyHandshake(h))
		}
	}

	slices.SortFunc(out, func(a, b *Handshake) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// ListHandshakesForOrganization returns all handshakes for the organization.
// actionTypeFilter optionally restricts results to handshakes with the given Action value.
func (b *InMemoryBackend) ListHandshakesForOrganization(actionTypeFilter string) ([]*Handshake, error) {
	b.mu.Lock("ListHandshakesForOrganization")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	b.expireStaleHandshakesLocked()

	out := make([]*Handshake, 0, len(b.handshakes))

	for _, h := range b.handshakes {
		if actionTypeFilter == "" || h.Action == actionTypeFilter {
			out = append(out, copyHandshake(h))
		}
	}

	slices.SortFunc(out, func(a, b *Handshake) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// ListCreateAccountStatus returns all CreateAccount status records, optionally filtered by state.
func (b *InMemoryBackend) ListCreateAccountStatus(states []string) ([]*CreateAccountStatus, error) {
	b.mu.RLock("ListCreateAccountStatus")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	out := make([]*CreateAccountStatus, 0, len(b.createStatuses))
	for _, s := range b.createStatuses {
		if len(states) == 0 || slices.Contains(states, s.State) {
			cp := *s
			out = append(out, &cp)
		}
	}

	slices.SortFunc(out, func(a, b *CreateAccountStatus) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// ListDelegatedServicesForAccount returns service principals for which an account is a delegated admin.
func (b *InMemoryBackend) ListDelegatedServicesForAccount(
	accountID string,
) ([]DelegatedService, error) {
	b.mu.RLock("ListDelegatedServicesForAccount")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if _, ok := b.accounts[accountID]; !ok {
		return nil, ErrAccountNotFound
	}

	var out []DelegatedService

	for svc, admins := range b.delegatedAdmins {
		if da, ok := admins[accountID]; ok {
			out = append(out, DelegatedService{
				ServicePrincipal:      svc,
				DelegationEnabledDate: da.DelegationTime,
			})
		}
	}

	slices.SortFunc(
		out,
		func(a, b DelegatedService) int { return cmp.Compare(a.ServicePrincipal, b.ServicePrincipal) },
	)

	return out, nil
}

// ListEffectivePolicyValidationErrors returns validation errors for an effective policy (always empty for stub).
func (b *InMemoryBackend) ListEffectivePolicyValidationErrors(policyType, _ string) ([]any, error) {
	b.mu.RLock("ListEffectivePolicyValidationErrors")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !slices.Contains(validPolicyTypes(), policyType) {
		return nil, ErrInvalidInput
	}

	return []any{}, nil
}

// ListAccountsWithInvalidEffectivePolicy returns accounts with invalid effective policies (always empty for stub).
func (b *InMemoryBackend) ListAccountsWithInvalidEffectivePolicy(
	policyType string,
) ([]*Account, error) {
	b.mu.RLock("ListAccountsWithInvalidEffectivePolicy")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !slices.Contains(validPolicyTypes(), policyType) {
		return nil, ErrInvalidInput
	}

	return []*Account{}, nil
}

// ListInboundResponsibilityTransfers returns INVITE-type handshakes targeting this account.
func (b *InMemoryBackend) ListInboundResponsibilityTransfers() ([]*Handshake, error) {
	b.mu.RLock("ListInboundResponsibilityTransfers")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	var out []*Handshake

	for _, h := range b.handshakes {
		if h.Action == handshakeActionApproveAll ||
			h.Action == "ADD_ORGANIZATIONS_SERVICE_LINKED_ROLE" {
			out = append(out, copyHandshake(h))
		}
	}

	slices.SortFunc(out, func(a, b *Handshake) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// ListOutboundResponsibilityTransfers returns responsibility-transfer handshakes initiated by this org.
func (b *InMemoryBackend) ListOutboundResponsibilityTransfers() ([]*Handshake, error) {
	b.mu.RLock("ListOutboundResponsibilityTransfers")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	var out []*Handshake

	for _, h := range b.handshakes {
		if h.Action == handshakeActionInvite {
			out = append(out, copyHandshake(h))
		}
	}

	slices.SortFunc(out, func(a, b *Handshake) int { return cmp.Compare(a.ID, b.ID) })

	return out, nil
}

// TerminateResponsibilityTransfer terminates an OPEN responsibility-transfer handshake.
func (b *InMemoryBackend) TerminateResponsibilityTransfer(handshakeID string) (*Handshake, error) {
	b.mu.Lock("TerminateResponsibilityTransfer")
	defer b.mu.Unlock()

	h, ok := b.handshakes[handshakeID]
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	if h.State != handshakeStateOpen {
		return nil, ErrHandshakeConstraintViolation
	}

	h.State = handshakeStateCanceled

	return copyHandshake(h), nil
}

// UpdateResponsibilityTransfer accepts or declines a responsibility-transfer handshake.
func (b *InMemoryBackend) UpdateResponsibilityTransfer(
	handshakeID, action string,
) (*Handshake, error) {
	b.mu.Lock("UpdateResponsibilityTransfer")
	defer b.mu.Unlock()

	h, ok := b.handshakes[handshakeID]
	if !ok {
		return nil, ErrHandshakeNotFound
	}

	if h.State != handshakeStateOpen {
		return nil, ErrHandshakeConstraintViolation
	}

	switch action {
	case "ACCEPT":
		h.State = handshakeStateAccepted
	case "DECLINE":
		h.State = handshakeStateDeclined
	default:
		return nil, ErrInvalidInput
	}

	return copyHandshake(h), nil
}

// InviteOrganizationToTransferResponsibility creates an OPEN invitation for org-to-org responsibility transfer.
func (b *InMemoryBackend) InviteOrganizationToTransferResponsibility(
	target HandshakeParty,
	notes string,
) (*Handshake, error) {
	b.mu.Lock("InviteOrganizationToTransferResponsibility")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if target.ID == "" {
		return nil, ErrInvalidInput
	}

	now := time.Now()
	id := newHandshakeID()
	h := &Handshake{
		ID:                  id,
		ARN:                 b.handshakeARN(b.org.ID, handshakeActionApproveAll, id),
		Action:              handshakeActionApproveAll,
		State:               handshakeStateOpen,
		RequestedTimestamp:  now,
		ExpirationTimestamp: now.Add(handshakeExpirationDuration),
		Parties: []HandshakeParty{
			{ID: b.org.MasterAccountID, Type: targetTypeAccount},
			{ID: target.ID, Type: target.Type},
		},
		Resources: []HandshakeResource{
			{Type: handshakeResourceOrg, Value: b.org.ID},
		},
	}

	if notes != "" {
		h.Resources = append(h.Resources, HandshakeResource{Type: handshakeResourceNotes, Value: notes})
	}

	b.handshakes[id] = h

	return copyHandshake(h), nil
}

// AddAccountInternal seeds an account directly under the root for testing.
// Requires an organization to have been created first.
func (b *InMemoryBackend) AddAccountInternal(a *Account) {
	b.mu.Lock("AddAccountInternal")
	defer b.mu.Unlock()

	b.accounts[a.ID] = a

	if b.root != nil {
		b.accountParent[a.ID] = b.root.ID
	}
}

// AddOUInternal seeds an OU directly for testing.
// Requires an organization to have been created first.
func (b *InMemoryBackend) AddOUInternal(ou *OrganizationalUnit) {
	b.mu.Lock("AddOUInternal")
	defer b.mu.Unlock()

	b.ous[ou.ID] = ou

	if ou.ParentID != "" {
		b.ouParent[ou.ID] = ou.ParentID
	} else if b.root != nil {
		b.ouParent[ou.ID] = b.root.ID
		ou.ParentID = b.root.ID
	}
}

// AddPolicyInternal seeds a policy directly for testing.
func (b *InMemoryBackend) AddPolicyInternal(p *Policy) {
	b.mu.Lock("AddPolicyInternal")
	defer b.mu.Unlock()

	b.policies[p.PolicySummary.ID] = p

	if b.policyTargets[p.PolicySummary.ID] == nil {
		b.policyTargets[p.PolicySummary.ID] = []string{}
	}
}

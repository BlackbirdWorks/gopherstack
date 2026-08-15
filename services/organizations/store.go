package organizations

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	targetTypeOU      = "ORGANIZATIONAL_UNIT"
	targetTypeAccount = "ACCOUNT"
)

// managementAccountCounter is the starting account counter (management account = 1).
const managementAccountCounter = 1

// InMemoryBackend is the in-memory storage for the Organizations service.
//
// Phase 3.3 datalayer note: accounts, ous, policies, createStatuses,
// handshakes, and serviceAccess are store.Table[T]s registered on registry
// (see store_setup.go); delegatedAdmins is a store.Table[DelegatedAdmin]
// deliberately NOT registered on registry because its composite key relies
// on a json:"-" field (see store_setup.go's doc comment). The remaining map
// fields below (targetPolicies, accountParent, policyTargets, ouParent,
// tags, emailToAccountID, ousByParent, accountChildrenByParent) are left as
// plain maps: each holds a bare, non-*T value ([]string, string,
// map[string]string, or map[string]bool), which does not fit store.Table's
// keyed-by-*T shape.
type InMemoryBackend struct {
	registry                 *store.Registry
	serviceAccess            *store.Table[EnabledServicePrincipal]
	targetPolicies           map[string][]string
	delegatedAdmins          *store.Table[DelegatedAdmin]
	delegatedAdminsByService *store.Index[DelegatedAdmin]
	delegatedAdminsByAccount *store.Index[DelegatedAdmin]
	handshakes               *store.Table[Handshake]
	responsibilityTransfers  *store.Table[ResponsibilityTransfer]
	// responsibilityTransfersByHandshake indexes responsibilityTransfers by
	// ActiveHandshakeID, so Accept/Cancel/Decline/expire on the underlying
	// Handshake can find and re-sync the transfer's Status in O(1) -- see
	// syncResponsibilityTransferStatusLocked (handshakes.go).
	responsibilityTransfersByHandshake *store.Index[ResponsibilityTransfer]
	org                                *Organization
	root                               *Root
	resourcePolicy                     *ResourcePolicy
	accounts                           *store.Table[Account]
	ous                                *store.Table[OrganizationalUnit]
	ousByParentIdx                     *store.Index[OrganizationalUnit]
	policies                           *store.Table[Policy]
	accountParent                      map[string]string
	policyTargets                      map[string][]string
	createStatuses                     *store.Table[CreateAccountStatus]
	ouParent                           map[string]string
	tags                               map[string]map[string]string
	emailToAccountID                   map[string]string
	// ousByParent maps parentID → ouName → ouID for O(1) sibling name uniqueness
	// checks in CreateOrganizationalUnit and UpdateOrganizationalUnit.
	ousByParent map[string]map[string]string
	// accountChildrenByParent maps parentID → set of accountIDs for O(1) child
	// lookups in ListChildren and DeleteOrganizationalUnit.
	accountChildrenByParent map[string]map[string]bool
	mu                      *lockmetrics.RWMutex
	region                  string
	accountID               string
	accountCounter          int
	statusCounter           int
}

// NewInMemoryBackend creates a new in-memory Organizations backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:               accountID,
		region:                  region,
		registry:                store.NewRegistry(),
		policyTargets:           make(map[string][]string),
		targetPolicies:          make(map[string][]string),
		accountParent:           make(map[string]string),
		ouParent:                make(map[string]string),
		tags:                    make(map[string]map[string]string),
		emailToAccountID:        make(map[string]string),
		ousByParent:             make(map[string]map[string]string),
		accountChildrenByParent: make(map[string]map[string]bool),
		accountCounter:          managementAccountCounter,
		mu:                      lockmetrics.New("organizations"),
	}

	registerAllTables(b)

	return b
}

// addAccountChild records accountID as a child of parentID in the index.
// Must be called with the write lock held.
func (b *InMemoryBackend) addAccountChild(parentID, accountID string) {
	if b.accountChildrenByParent[parentID] == nil {
		b.accountChildrenByParent[parentID] = make(map[string]bool)
	}

	b.accountChildrenByParent[parentID][accountID] = true
}

// removeAccountChild removes accountID from its parent's entry in the index.
// Must be called with the write lock held.
func (b *InMemoryBackend) removeAccountChild(accountID string) {
	parentID := b.accountParent[accountID]
	if children := b.accountChildrenByParent[parentID]; children != nil {
		delete(children, accountID)
	}
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

// resetStateLocked clears all organization state except statusCounter, which
// Reset (unlike DeleteOrganization) also zeroes -- see the doc comments on
// each caller. Must be called with the write lock held.
func (b *InMemoryBackend) resetStateLocked() {
	b.org = nil
	b.root = nil
	b.registry.ResetAll()
	b.delegatedAdmins.Reset()
	b.ousByParent = make(map[string]map[string]string)
	b.accountChildrenByParent = make(map[string]map[string]bool)
	b.policyTargets = make(map[string][]string)
	b.targetPolicies = make(map[string][]string)
	b.accountParent = make(map[string]string)
	b.ouParent = make(map[string]string)
	b.tags = make(map[string]map[string]string)
	b.emailToAccountID = make(map[string]string)
	b.resourcePolicy = nil
	b.accountCounter = managementAccountCounter
}

// parentExists checks if a parentID refers to the root or an existing OU.
func (b *InMemoryBackend) parentExists(parentID string) bool {
	if b.root != nil && b.root.ID == parentID {
		return true
	}

	return b.ous.Has(parentID)
}

// targetExistsLocked checks if a targetID refers to the root, an OU, or an account.
// Must be called with lock held.
func (b *InMemoryBackend) targetExistsLocked(targetID string) bool {
	if b.root != nil && b.root.ID == targetID {
		return true
	}

	if b.ous.Has(targetID) {
		return true
	}

	if b.accounts.Has(targetID) {
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

	if b.ous.Has(resourceID) {
		return true
	}

	if b.accounts.Has(resourceID) {
		return true
	}

	if b.policies.Has(resourceID) {
		return true
	}

	return false
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

// Reset clears all organization state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetStateLocked()
	b.statusCounter = 0
}

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

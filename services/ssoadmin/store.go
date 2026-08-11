package ssoadmin

import (
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is the in-memory backend for the SSO Admin service.
type InMemoryBackend struct {
	registry                 *store.Registry
	instances                *store.Table[Instance]
	permissionSets           *store.Table[PermissionSet]
	permissionSetsByInstance *store.Index[PermissionSet]
	assignments              map[string][]*AccountAssignment
	// assignmentCreationIDs maps assignmentKey|accountID|principalType|principalID → requestID for idempotency.
	assignmentCreationIDs map[string]string
	// provisionedAt maps provisionedAtKey(instanceArn, permissionSetArn, accountID) →
	// the last time that permission set was (re-)provisioned to that account,
	// via CreateAccountAssignment (implicit) or ProvisionPermissionSet
	// (explicit). Compared against PermissionSet.ModifiedDate to answer the
	// ProvisioningStatus filter on ListPermissionSetsProvisionedToAccount/
	// ListAccountsForProvisionedPermissionSet.
	provisionedAt           map[string]time.Time
	creationStatuses        *store.Table[ProvisioningStatus]
	deletionStatuses        *store.Table[ProvisioningStatus]
	provisioningStatuses    *store.Table[ProvisioningStatus]
	instanceRegions         map[string][]RegionMetadata
	customerManagedPolicies map[string][]CustomerManagedPolicyReference
	applications            *store.Table[Application]
	applicationsByInstance  *store.Index[Application]
	applicationAssignments  map[string][]*ApplicationAssignment
	applicationScopes       map[string]map[string][]string
	// applicationAuthMethods stores per-app authentication methods: appArn → methodType → full JSON body.
	applicationAuthMethods map[string]map[string]json.RawMessage
	// applicationGrants stores per-app grants: appArn → grantType → full JSON body.
	applicationGrants             map[string]map[string]json.RawMessage
	applicationAssignConfig       map[string]bool
	applicationSessions           map[string]string
	instanceACAs                  *store.Table[ABACConfig]
	trustedTokenIssuers           *store.Table[TrustedTokenIssuer]
	trustedTokenIssuersByInstance *store.Index[TrustedTokenIssuer]
	permissionBoundaries          *store.Table[PermissionsBoundary]
	mu                            *lockmetrics.RWMutex
	accountID                     string
	region                        string
}

// seedDefaultInstance adds the default pre-seeded instance. Must be called before concurrent use.
func (b *InMemoryBackend) seedDefaultInstance() {
	defaultID := "d-0000000001"
	identityStoreID := "d-" + b.accountID
	if len(identityStoreID) > identityStoreIDMaxLen {
		identityStoreID = identityStoreID[:identityStoreIDMaxLen]
	}
	defaultArn := "arn:aws:sso:::instance/ssoins-" + defaultID
	if b.instances.Has(defaultArn) {
		return
	}
	b.instances.Put(&Instance{
		InstanceArn:     defaultArn,
		Name:            "default",
		OwnerAccountID:  b.accountID,
		IdentityStoreID: identityStoreID,
		Status:          instanceStatusActive,
		CreatedDate:     time.Now().UTC(),
		Tags:            make(map[string]string),
	})
}

// NewInMemoryBackend creates a new in-memory SSO Admin backend with a default instance.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:                store.NewRegistry(),
		assignments:             make(map[string][]*AccountAssignment),
		assignmentCreationIDs:   make(map[string]string),
		provisionedAt:           make(map[string]time.Time),
		instanceRegions:         make(map[string][]RegionMetadata),
		customerManagedPolicies: make(map[string][]CustomerManagedPolicyReference),
		applicationAssignments:  make(map[string][]*ApplicationAssignment),
		applicationScopes:       make(map[string]map[string][]string),
		applicationAuthMethods:  make(map[string]map[string]json.RawMessage),
		applicationGrants:       make(map[string]map[string]json.RawMessage),
		applicationAssignConfig: make(map[string]bool),
		applicationSessions:     make(map[string]string),
		mu:                      lockmetrics.New("ssoadmin"),
		accountID:               accountID,
		region:                  region,
	}

	registerAllTables(b)

	// Pre-seed a default instance to mimic AWS SSO behaviour where an instance
	// is always present once SSO is enabled.
	b.seedDefaultInstance()

	return b
}

// Reset clears all backend state and re-seeds the default instance.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.instanceACAs.Reset()
	b.permissionBoundaries.Reset()

	b.assignments = make(map[string][]*AccountAssignment)
	b.instanceRegions = make(map[string][]RegionMetadata)
	b.assignmentCreationIDs = make(map[string]string)
	b.provisionedAt = make(map[string]time.Time)
	b.customerManagedPolicies = make(map[string][]CustomerManagedPolicyReference)
	b.applicationAssignments = make(map[string][]*ApplicationAssignment)
	b.applicationScopes = make(map[string]map[string][]string)
	b.applicationAuthMethods = make(map[string]map[string]json.RawMessage)
	b.applicationGrants = make(map[string]map[string]json.RawMessage)
	b.applicationAssignConfig = make(map[string]bool)
	b.applicationSessions = make(map[string]string)
	// Re-seed the default instance.
	b.seedDefaultInstance()
}

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

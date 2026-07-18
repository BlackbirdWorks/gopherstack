package sesv2

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// keyStatus and keyStatusSuccess are shared response-map keys used across
// several store-layer methods (dedicated IPs, SendBulkEmail results, tenants,
// deliverability/reputation entities) that build ad-hoc map[string]any
// responses rather than typed structs.
const (
	keyStatus        = "Status"
	keyStatusSuccess = "SUCCESS"
)

const sesv2DefaultMaxItems = 100

// InMemoryBackend is an in-memory store for SES v2 email identities, emails, and configuration sets.
type InMemoryBackend struct {
	// registry holds every store.Table-backed resource field so their
	// Reset/Snapshot/Restore collapse to one call each -- see
	// store_setup.go's file doc comment for why every table here is
	// "clean" (registered directly, no DTO-registry needed).
	registry          *store.Registry
	identities        *store.Table[EmailIdentity]
	configurationSets *store.Table[ConfigurationSet]
	eventDestinations *store.Table[EventDestination]
	// eventDestinationsByConfigSet is a secondary index over
	// eventDestinations grouping by ConfigurationSetName, answering the "all
	// event destinations of configuration set X" lookups the previous
	// nested map[string]map[string]*EventDestination answered directly.
	eventDestinationsByConfigSet *store.Index[EventDestination]
	contactLists                 *store.Table[ContactList]
	contacts                     *store.Table[Contact]
	// contactsByList is a secondary index over contacts grouping by
	// ContactListName, answering the "all contacts of list X" lookups the
	// previous nested map[string]map[string]*Contact answered directly.
	contactsByList              *store.Index[Contact]
	customVerificationTemplates *store.Table[CustomVerificationEmailTemplate]
	dedicatedIPPools            *store.Table[DedicatedIPPool]
	dedicatedIPs                *store.Table[DedicatedIP]
	reputationEntities          *store.Table[ReputationEntity]
	deliverabilityTestReports   *store.Table[DeliverabilityTestReport]
	emailTemplates              *store.Table[EmailTemplate]
	exportJobs                  *store.Table[ExportJob]
	importJobs                  *store.Table[ImportJob]
	suppressedDestinations      *store.Table[SuppressedDestination]
	emailIdentityPolicies       map[string]map[string]string
	resourceTags                map[string]map[string]string
	multiRegionEndpoints        map[string]map[string]any
	tenants                     map[string]map[string]any
	tenantResources             map[string][]string
	resourceTenants             map[string][]string
	accountDetails              *AccountDetails
	mu                          *lockmetrics.RWMutex
	region                      string
	accountID                   string
	emails                      []Email
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		registry:              store.NewRegistry(),
		emailIdentityPolicies: make(map[string]map[string]string),
		resourceTags:          make(map[string]map[string]string),
		multiRegionEndpoints:  make(map[string]map[string]any),
		tenants:               make(map[string]map[string]any),
		tenantResources:       make(map[string][]string),
		resourceTenants:       make(map[string][]string),
		mu:                    lockmetrics.New("sesv2"),
		region:                config.DefaultRegion,
		accountID:             config.DefaultAccountID,
	}
	registerAllTables(b)

	return b
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given config.
func NewInMemoryBackendWithConfig(cfg *config.GlobalConfig) *InMemoryBackend {
	b := NewInMemoryBackend()
	if cfg.GetRegion() != "" {
		b.region = cfg.GetRegion()
	}

	if cfg.GetAccountID() != "" {
		b.accountID = cfg.GetAccountID()
	}

	return b
}

// Region returns the backend's AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend's AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset closes the mutex and re-initialises all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Close()
	b.mu = lockmetrics.New("sesv2")
	b.registry.ResetAll()
	b.emailIdentityPolicies = make(map[string]map[string]string)
	b.resourceTags = make(map[string]map[string]string)
	b.multiRegionEndpoints = make(map[string]map[string]any)
	b.tenants = make(map[string]map[string]any)
	b.tenantResources = make(map[string][]string)
	b.resourceTenants = make(map[string][]string)
	b.accountDetails = nil
	b.emails = nil
}

// paginateMaps applies simple nextToken/pageSize pagination to a slice of
// maps, using keyName as the cursor field. Returns the page, next token, and nil error.
func paginateMaps(
	all []map[string]any,
	nextToken string,
	pageSize int,
	keyName string,
) ([]map[string]any, string, error) {
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 100
	}

	start := 0
	if nextToken != "" {
		for i, item := range all {
			if item[keyName] == nextToken {
				start = i

				break
			}
		}
	}

	end := start + pageSize

	var next string
	if end < len(all) {
		v, ok := all[end][keyName].(string)
		if ok {
			next = v
		}
	} else {
		end = len(all)
	}

	return all[start:end], next, nil
}

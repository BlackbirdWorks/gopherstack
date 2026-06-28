package sesv2

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	DedicatedIPPools            map[string]*DedicatedIPPool                 `json:"dedicatedIPPools"`
	EmailIdentityPolicies       map[string]map[string]string                `json:"emailIdentityPolicies"`
	EventDestinations           map[string]map[string]*EventDestination     `json:"eventDestinations"`
	ContactLists                map[string]*ContactList                     `json:"contactLists"`
	Contacts                    map[string]map[string]*Contact              `json:"contacts"`
	CustomVerificationTemplates map[string]*CustomVerificationEmailTemplate `json:"customVerificationTemplates"`
	EmailTemplates              map[string]*EmailTemplate                   `json:"emailTemplates"`
	DeliverabilityTestReports   map[string]*DeliverabilityTestReport        `json:"deliverabilityTestReports"`
	ConfigurationSets           map[string]*ConfigurationSet                `json:"configurationSets"`
	ExportJobs                  map[string]*ExportJob                       `json:"exportJobs"`
	ImportJobs                  map[string]*ImportJob                       `json:"importJobs,omitempty"`
	SuppressedDestinations      map[string]*SuppressedDestination           `json:"suppressedDestinations,omitempty"`
	AccountDetails              *AccountDetails                             `json:"accountDetails,omitempty"`
	Identities                  map[string]*EmailIdentity                   `json:"identities"`
	ResourceTags                map[string]map[string]string                `json:"resourceTags"`
	MultiRegionEndpoints        map[string]map[string]any                   `json:"multiRegionEndpoints"`
	Tenants                     map[string]map[string]any                   `json:"tenants"`
	TenantResources             map[string][]string                         `json:"tenantResources"`
	ResourceTenants             map[string][]string                         `json:"resourceTenants"`
	AccountID                   string                                      `json:"accountID"`
	Region                      string                                      `json:"region"`
	Emails                      []Email                                     `json:"emails"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	ensureCoreMaps(s)
	ensureExtendedMaps(s)
}

func ensureCoreMaps(s *backendSnapshot) {
	if s.Identities == nil {
		s.Identities = make(map[string]*EmailIdentity)
	}

	if s.ConfigurationSets == nil {
		s.ConfigurationSets = make(map[string]*ConfigurationSet)
	}

	if s.EventDestinations == nil {
		s.EventDestinations = make(map[string]map[string]*EventDestination)
	}

	if s.ContactLists == nil {
		s.ContactLists = make(map[string]*ContactList)
	}

	if s.Contacts == nil {
		s.Contacts = make(map[string]map[string]*Contact)
	}

	if s.CustomVerificationTemplates == nil {
		s.CustomVerificationTemplates = make(map[string]*CustomVerificationEmailTemplate)
	}

	if s.DedicatedIPPools == nil {
		s.DedicatedIPPools = make(map[string]*DedicatedIPPool)
	}

	if s.DeliverabilityTestReports == nil {
		s.DeliverabilityTestReports = make(map[string]*DeliverabilityTestReport)
	}
}

func ensureExtendedMaps(s *backendSnapshot) {
	if s.EmailTemplates == nil {
		s.EmailTemplates = make(map[string]*EmailTemplate)
	}

	if s.ExportJobs == nil {
		s.ExportJobs = make(map[string]*ExportJob)
	}

	if s.ImportJobs == nil {
		s.ImportJobs = make(map[string]*ImportJob)
	}

	if s.SuppressedDestinations == nil {
		s.SuppressedDestinations = make(map[string]*SuppressedDestination)
	}

	if s.EmailIdentityPolicies == nil {
		s.EmailIdentityPolicies = make(map[string]map[string]string)
	}

	if s.ResourceTags == nil {
		s.ResourceTags = make(map[string]map[string]string)
	}

	if s.MultiRegionEndpoints == nil {
		s.MultiRegionEndpoints = make(map[string]map[string]any)
	}

	if s.Tenants == nil {
		s.Tenants = make(map[string]map[string]any)
	}

	if s.TenantResources == nil {
		s.TenantResources = make(map[string][]string)
	}

	if s.ResourceTenants == nil {
		s.ResourceTenants = make(map[string][]string)
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Identities:                  b.identities,
		ConfigurationSets:           b.configurationSets,
		EventDestinations:           b.eventDestinations,
		ContactLists:                b.contactLists,
		Contacts:                    b.contacts,
		CustomVerificationTemplates: b.customVerificationTemplates,
		DedicatedIPPools:            b.dedicatedIPPools,
		DeliverabilityTestReports:   b.deliverabilityTestReports,
		EmailTemplates:              b.emailTemplates,
		ExportJobs:                  b.exportJobs,
		ImportJobs:                  b.importJobs,
		SuppressedDestinations:      b.suppressedDestinations,
		AccountDetails:              b.accountDetails,
		EmailIdentityPolicies:       b.emailIdentityPolicies,
		Emails:                      b.emails,
		AccountID:                   b.accountID,
		Region:                      b.region,
		ResourceTags:                b.resourceTags,
		MultiRegionEndpoints:        b.multiRegionEndpoints,
		Tenants:                     b.tenants,
		TenantResources:             b.tenantResources,
		ResourceTenants:             b.resourceTenants,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "sesv2: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "sesv2", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.identities = snap.Identities
	b.configurationSets = snap.ConfigurationSets
	b.eventDestinations = snap.EventDestinations
	b.contactLists = snap.ContactLists
	b.contacts = snap.Contacts
	b.customVerificationTemplates = snap.CustomVerificationTemplates
	b.dedicatedIPPools = snap.DedicatedIPPools
	b.deliverabilityTestReports = snap.DeliverabilityTestReports
	b.emailTemplates = snap.EmailTemplates
	b.exportJobs = snap.ExportJobs
	b.importJobs = snap.ImportJobs
	b.suppressedDestinations = snap.SuppressedDestinations
	b.accountDetails = snap.AccountDetails
	b.emailIdentityPolicies = snap.EmailIdentityPolicies
	b.emails = snap.Emails
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.resourceTags = snap.ResourceTags
	b.multiRegionEndpoints = snap.MultiRegionEndpoints
	b.tenants = snap.Tenants
	b.tenantResources = snap.TenantResources
	b.resourceTenants = snap.ResourceTenants

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

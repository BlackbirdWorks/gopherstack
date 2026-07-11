package sesv2

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (or nested map[string]map[string]*T) resource field on
// InMemoryBackend is replaced with a *store.Table[T], following the pattern
// established by services/ec2 (commit 12e611a4, data-driven registration
// slice), services/sqs (commit 0f09d77c, DTO-registry for types that can't
// round-trip a direct JSON marshal), and services/ses (commit e9af4cc7, the
// sibling email service). See pkgs/store's package doc for the underlying
// primitive.
//
// Unlike services/ses, every convertible value type here already carries its
// own identity as a real (non-json:"-") field -- EmailIdentity.Identity,
// ConfigurationSet.Name, EventDestination.{ConfigurationSetName,Name},
// Contact.{ContactListName,EmailAddress}, etc. were already set consistently
// at every write site before this refactor. So none of the 14 tables below
// need the ses-style "dirty" DTO-registry treatment: all 14 are "clean"
// tables registered directly on b.registry, and persistence.go drives every
// one of them through b.registry.SnapshotAll() / RestoreAll().
//
// eventDestinations and contacts were previously nested maps
// (map[string]map[string]*EventDestination / map[string]map[string]*Contact)
// keyed by parent name then child name. Both are flattened into a single
// Table keyed by a composite "<parent>#<child>" key, with a secondary
// store.Index grouping by the parent name for the "all children of parent X"
// lookups the nested map used to answer directly.
//
// Left as plain maps (not store.Table-backed):
//   - emailIdentityPolicies, resourceTags (map[string]map[string]string):
//     values are plain strings, not *T, so they do not fit store.Table's
//     keyed-by-identity-value shape.
//   - multiRegionEndpoints, tenants (map[string]map[string]any): same reason
//     -- values are plain maps, not *T.
//   - tenantResources, resourceTenants (map[string][]string): slice-valued,
//     not *T.
//
// accountDetails is a single *AccountDetails pointer, not a collection, so it
// is untouched by this refactor.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func emailIdentityKeyFn(v *EmailIdentity) string { return v.Identity }

func configurationSetKeyFn(v *ConfigurationSet) string { return v.Name }

// eventDestinationKey builds the composite "<configSetName>#<destName>" key
// shared by every event destination nested under a configuration set. Access
// sites use this directly so the exact same key is used for
// Put/Get/Has/Delete.
func eventDestinationKey(configSetName, destName string) string {
	return configSetName + "#" + destName
}

func eventDestinationKeyFn(v *EventDestination) string {
	return eventDestinationKey(v.ConfigurationSetName, v.Name)
}

func contactListKeyFn(v *ContactList) string { return v.Name }

// contactKey builds the composite "<contactListName>#<emailAddress>" key
// shared by every contact nested under a contact list. Access sites use this
// directly so the exact same key is used for Put/Get/Has/Delete.
func contactKey(contactListName, emailAddress string) string {
	return contactListName + "#" + emailAddress
}

func contactKeyFn(v *Contact) string {
	return contactKey(v.ContactListName, v.EmailAddress)
}

func customVerificationTemplateKeyFn(v *CustomVerificationEmailTemplate) string {
	return v.TemplateName
}

func dedicatedIPPoolKeyFn(v *DedicatedIPPool) string { return v.PoolName }

func dedicatedIPKeyFn(v *DedicatedIP) string { return v.IP }

func reputationEntityKeyFn(v *ReputationEntity) string { return v.EntityRef }

func deliverabilityTestReportKeyFn(v *DeliverabilityTestReport) string { return v.ReportID }

func emailTemplateKeyFn(v *EmailTemplate) string { return v.TemplateName }

func exportJobKeyFn(v *ExportJob) string { return v.JobID }

func importJobKeyFn(v *ImportJob) string { return v.JobID }

func suppressedDestinationKeyFn(v *SuppressedDestination) string { return v.EmailAddress }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.identities = store.Register(b.registry, "identities", store.New(emailIdentityKeyFn))
	b.configurationSets = store.Register(b.registry, "configurationSets", store.New(configurationSetKeyFn))

	b.eventDestinations = store.Register(b.registry, "eventDestinations", store.New(eventDestinationKeyFn))
	b.eventDestinationsByConfigSet = b.eventDestinations.AddIndex(
		"byConfigSet",
		func(v *EventDestination) string { return v.ConfigurationSetName },
	)

	b.contactLists = store.Register(b.registry, "contactLists", store.New(contactListKeyFn))

	b.contacts = store.Register(b.registry, "contacts", store.New(contactKeyFn))
	b.contactsByList = b.contacts.AddIndex(
		"byContactList",
		func(v *Contact) string { return v.ContactListName },
	)

	b.customVerificationTemplates = store.Register(
		b.registry, "customVerificationTemplates", store.New(customVerificationTemplateKeyFn),
	)
	b.dedicatedIPPools = store.Register(b.registry, "dedicatedIPPools", store.New(dedicatedIPPoolKeyFn))
	b.dedicatedIPs = store.Register(b.registry, "dedicatedIPs", store.New(dedicatedIPKeyFn))
	b.reputationEntities = store.Register(b.registry, "reputationEntities", store.New(reputationEntityKeyFn))
	b.deliverabilityTestReports = store.Register(
		b.registry, "deliverabilityTestReports", store.New(deliverabilityTestReportKeyFn),
	)
	b.emailTemplates = store.Register(b.registry, "emailTemplates", store.New(emailTemplateKeyFn))
	b.exportJobs = store.Register(b.registry, "exportJobs", store.New(exportJobKeyFn))
	b.importJobs = store.Register(b.registry, "importJobs", store.New(importJobKeyFn))
	b.suppressedDestinations = store.Register(
		b.registry, "suppressedDestinations", store.New(suppressedDestinationKeyFn),
	)
}

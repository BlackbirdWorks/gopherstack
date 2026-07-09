package ses

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (or map[string]T) resource field on InMemoryBackend is
// replaced with a *store.Table[T], following the pattern established by
// services/ec2 (commit 12e611a4, data-driven registration slice) and
// services/sqs (commit 0f09d77c, DTO-registry for types with fields that
// can't round-trip through a direct JSON marshal). See pkgs/store's package
// doc for the underlying primitive.
//
// Tables split into three groups:
//
//   - "Clean" tables (templates, receiptRuleSets, receiptFilters,
//     customVerifTemplates) key off a field the value type already carries
//     (TemplateName / Name), so they are registered on b.registry here and
//     persistence.go drives them through b.registry.SnapshotAll() /
//     RestoreAll() directly.
//   - "Dirty" tables (identities, configSets, trackingOptions,
//     eventDestinations) key off a field with no natural home on the value
//     type: IdentityRecord/ConfigurationSet/TrackingOptions carried no name
//     of their own, and EventDestination carried its own Name but not its
//     parent configuration set's name. Each gained an identity-only field
//     tagged `json:"-"` purely so store.Table's keyFn can derive a key from
//     the value -- see the doc comments on those fields in backend.go. They
//     are NOT registered on b.registry: persistence.go instead builds a
//     throwaway DTO store.Registry (mirroring the services/sqs pilot) whose
//     DTO types carry the identity as a real JSON field, so Snapshot/Restore
//     never depends on a json:"-" field to round-trip correctly.
//   - emailsByID is a *store.Table[Email] but is neither "clean" nor
//     "dirty": it is a derived O(1) lookup cache over b.emails (the
//     append-ordered, TTL-swept slice that remains the source of truth), so
//     it is rebuilt from b.emails after every Restore and reset explicitly
//     alongside it rather than snapshotted independently -- see
//     persistence.go.
//
// eventDestinations additionally carries a secondary store.Index
// (eventDestinationsByConfigSet) grouping by ConfigSetName, replacing the
// direct lookup a nested map[string]map[string]*EventDestination used to
// offer for "all event destinations of configuration set X".
//
// policies (map[string]map[string]string, identity -> policyName ->
// policyDocument) is deliberately left a plain map: its values are plain
// strings, not *T, so it does not fit store.Table's keyed-by-identity-value
// shape.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func identityKeyFn(v *IdentityRecord) string { return v.Identity }

func templateKeyFn(v *EmailTemplate) string { return v.TemplateName }

func configSetKeyFn(v *ConfigurationSet) string { return v.Name }

func receiptRuleSetKeyFn(v *ReceiptRuleSet) string { return v.Name }

func receiptFilterKeyFn(v *ReceiptFilter) string { return v.Name }

func trackingOptionsKeyFn(v *TrackingOptions) string { return v.ConfigSetName }

func customVerifTemplateKeyFn(v *CustomVerificationEmailTemplate) string { return v.TemplateName }

// eventDestinationKey builds the composite "<configSetName>#<destName>" key
// shared by every event destination nested under a configuration set. Access
// sites use this directly so the exact same key is used for
// Put/Get/Has/Delete.
func eventDestinationKey(configSetName, destName string) string {
	return configSetName + "#" + destName
}

func eventDestinationKeyFn(v *EventDestination) string {
	return eventDestinationKey(v.ConfigSetName, v.Name)
}

func emailKeyFn(v *Email) string { return v.MessageID }

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. "Clean" tables are registered on
// b.registry; "dirty" tables and the emailsByID derived cache are
// constructed alongside them but deliberately NOT registered -- see the file
// doc comment above. It must be called during construction only, never on
// every Reset(): store.Register panics on a duplicate name, so runtime
// resets go through resetTablesLocked (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.templates = store.Register(b.registry, "templates", store.New(templateKeyFn))
	b.receiptRuleSets = store.Register(b.registry, "receiptRuleSets", store.New(receiptRuleSetKeyFn))
	b.receiptFilters = store.Register(b.registry, "receiptFilters", store.New(receiptFilterKeyFn))
	b.customVerifTemplates = store.Register(b.registry, "customVerifTemplates", store.New(customVerifTemplateKeyFn))

	b.identities = store.New(identityKeyFn)
	b.configSets = store.New(configSetKeyFn)
	b.trackingOptions = store.New(trackingOptionsKeyFn)
	b.eventDestinations = store.New(eventDestinationKeyFn)
	b.eventDestinationsByConfigSet = b.eventDestinations.AddIndex(
		"byConfigSet",
		func(v *EventDestination) string { return v.ConfigSetName },
	)

	b.emailsByID = store.New(emailKeyFn)
}

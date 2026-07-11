package fis

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], registered exactly once here on b.registry. See
// pkgs/store's package doc and the services/ec2 (commit 12e611a4) and
// services/ses (commit e9af4cc7) conversions this follows.
//
// All three resource tables converted here are "clean": ExperimentTemplate,
// Experiment, and TargetAccountConfiguration each already carry their own
// identity field(s) (ID / composite ExperimentTemplateID+AccountID) as real
// JSON fields, so no DTO wrapper is needed for Snapshot/Restore to round-trip
// -- persistence.go drives all three through b.registry.SnapshotAll() /
// RestoreAll() directly.
//
// templates and experiments each gain a secondary store.Index keyed by ARN,
// replacing the hand-maintained templateARNIndex / experimentARNIndex
// map[string]string fields: AddIndex keeps the ARN -> value mapping in sync
// automatically on every Put/Delete/Restore, so no manual index maintenance
// code is needed at any call site. ARN is immutable once a template or
// experiment is created (never reassigned by Update*), so it is safe to key
// an index on it -- see AddIndex's doc comment on why the index key must not
// change across a value's lifetime.
//
// targetAccountConfigs replaces the nested map[string]map[string]*T
// (templateID -> accountID -> config) with a single Table[TargetAccountConfiguration]
// keyed by the composite "<templateID>#<accountID>" string, plus a secondary
// store.Index grouping by templateID (targetAccountConfigsByTemplate) for the
// "all configs of template X" and cascade-delete-on-template-delete access
// patterns the nested map used to serve directly.
//
// The following resource fields are deliberately left as plain maps (not
// registered here):
//   - tplClientTokens, expClientTokens: map[string]string (clientToken ->
//     ID), not a map[string]*T so it does not fit store.Table's keyed-value
//     shape. Neither map is part of backendSnapshot today (idempotency
//     tokens are not persisted across a snapshot/restore cycle), so leaving
//     them as raw maps changes nothing about persistence behaviour.
//   - safetyLever: a single *SafetyLever, not a map at all -- one lever per
//     backend/account, so there is nothing to key a Table on.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func templateKeyFn(v *ExperimentTemplate) string { return v.ID }

func templateArnIndexKeyFn(v *ExperimentTemplate) string { return v.Arn }

func experimentKeyFn(v *Experiment) string { return v.ID }

func experimentArnIndexKeyFn(v *Experiment) string { return v.Arn }

// targetAccountConfigKey builds the composite "<templateID>#<accountID>" key
// shared by every target account configuration nested under a template.
// Access sites use this directly so the exact same key is used for
// Put/Get/Delete.
func targetAccountConfigKey(templateID, accountID string) string {
	return templateID + "#" + accountID
}

func targetAccountConfigKeyFn(v *TargetAccountConfiguration) string {
	return targetAccountConfigKey(v.ExperimentTemplateID, v.AccountID)
}

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time, and registers each on b.registry so
// Reset/Snapshot/Restore collapse to one Registry call. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.templates = store.Register(b.registry, "templates", store.New(templateKeyFn))
	b.templatesByArn = b.templates.AddIndex("byArn", templateArnIndexKeyFn)

	b.experiments = store.Register(b.registry, "experiments", store.New(experimentKeyFn))
	b.experimentsByArn = b.experiments.AddIndex("byArn", experimentArnIndexKeyFn)

	b.targetAccountConfigs = store.Register(
		b.registry,
		"targetAccountConfigs",
		store.New(targetAccountConfigKeyFn),
	)
	b.targetAccountConfigsByTemplate = b.targetAccountConfigs.AddIndex(
		"byTemplate",
		func(v *TargetAccountConfiguration) string { return v.ExperimentTemplateID },
	)
}

// templateByArn returns the experiment template indexed under the given ARN,
// or (nil, false) if no template has that ARN. ARN is unique per template, so
// the byArn index group has at most one member.
func (b *InMemoryBackend) templateByArn(arnStr string) (*ExperimentTemplate, bool) {
	group := b.templatesByArn.Get(arnStr)
	if len(group) == 0 {
		return nil, false
	}

	return group[0], true
}

// experimentByArn returns the experiment indexed under the given ARN, or
// (nil, false) if no experiment has that ARN. ARN is unique per experiment,
// so the byArn index group has at most one member.
func (b *InMemoryBackend) experimentByArn(arnStr string) (*Experiment, bool) {
	group := b.experimentsByArn.Get(arnStr)
	if len(group) == 0 {
		return nil, false
	}

	return group[0], true
}

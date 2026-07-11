package pipes

// Code in this file supports Phase 3.3 of the datalayer refactor: resource
// maps with a pure per-value identity are backed by *store.Table[T] instead
// of a hand-rolled map[string]*T. See pkgs/store's package doc and the
// services/ec2 (commit 12e611a4), services/sqs (commit 0f09d77c), and
// services/eventbridge (per-region lazy table registration) conversions this
// follows.
//
// # Why this looks different from ec2/sqs
//
// ec2 and sqs each model ONE region per InMemoryBackend instance, so a flat
// map[string]*store.Table[T] statically registered once at construction time
// is enough. pipes' InMemoryBackend, like eventbridge's, holds every region's
// state in one instance (region -> pipe), so each convertible resource keeps
// its existing outer region-keyed map, whose values become a
// *store.Table[T] created lazily on first access -- exactly mirroring the
// lazy map creation the hand-rolled pipesStore/enrichmentCallCountStore
// helpers did before conversion. The first time a table is created it is
// ALSO registered on b.registry under a name that encodes its region (e.g.
// "pipes/us-east-1") so that persistence.go can still drive Snapshot/Restore
// through one *store.Registry regardless of how many regions exist -- see
// preRegisterSnapshotTables in persistence.go.
//
// # What changed shape, and why
//
//   - pipeARNIndex (map[string]map[string]string, region -> arn -> name) is
//     no longer its own map: Pipe already carries its own ARN field, so it
//     becomes a store.Index[Pipe] ("byARN") attached directly to each
//     region's pipes table via AddIndex. store.Table.Put/Delete/Restore keep
//     it consistent automatically, which is also why completeDeleteTransition
//     no longer needs a separate delete(pipeARNIndex, ...) call.
//   - enrichmentCallCount (map[string]map[string]int64, region -> pipe name
//     -> count) has no identity of its own to key a store.Table[int64] by
//     (int64 is not a *V with a name field) -- it is wrapped in the
//     enrichmentCounter DTO below, whose Name field carries the pipe name so
//     store.Table has something to key by. Name IS persisted normally (see
//     enrichmentCounter's doc comment for why this differs from services/ses's
//     json:"-" identity fields).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func pipeKeyFn(v *Pipe) string    { return v.Name }
func pipeARNKeyFn(v *Pipe) string { return v.ARN }

// enrichmentCounter is the DTO backing enrichmentCounts: a per-pipe counter
// of enrichment invocations. It has no AWS-facing identity of its own, and
// (unlike Pipe.Name/Pipe.ARN) Name is not a field of any other live type
// that must stay wire-clean -- enrichmentCounter exists solely as this
// table's value type -- so Name is serialized normally (json:"name") rather
// than excluded, letting store.Table.Restore's keyFn round-trip it directly
// without a separate ephemeral DTO-registry indirection (contrast
// services/ses's ConfigurationSet/IdentityRecord, whose Name/Identity fields
// are tagged json:"-" because those live types are also used elsewhere and
// must not gain a redundant persisted field there).
type enrichmentCounter struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

func enrichmentCounterKeyFn(v *enrichmentCounter) string { return v.Name }

// pipesTable returns the *store.Table[Pipe] for region, lazily creating (and
// registering on b.registry, alongside its "byARN" secondary index) the
// first time that region is touched.
func (b *InMemoryBackend) pipesTable(region string) *store.Table[Pipe] {
	t, ok := b.pipes[region]
	if !ok {
		t = store.Register(b.registry, "pipes/"+region, store.New(pipeKeyFn))
		b.pipesByARN[region] = t.AddIndex("byARN", pipeARNKeyFn)
		b.pipes[region] = t
	}

	return t
}

// pipesByARNIndex returns the *store.Index[Pipe] backing region's ARN lookup,
// creating the underlying table (and thus the index) first if needed.
func (b *InMemoryBackend) pipesByARNIndex(region string) *store.Index[Pipe] {
	b.pipesTable(region)

	return b.pipesByARN[region]
}

// enrichmentCountsTable returns the *store.Table[enrichmentCounter] for
// region, lazily creating and registering it on b.registry the first time
// that region is touched.
func (b *InMemoryBackend) enrichmentCountsTable(region string) *store.Table[enrichmentCounter] {
	t, ok := b.enrichmentCounts[region]
	if !ok {
		t = store.Register(b.registry, "enrichmentCounts/"+region, store.New(enrichmentCounterKeyFn))
		b.enrichmentCounts[region] = t
	}

	return t
}

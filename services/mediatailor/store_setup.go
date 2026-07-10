package mediatailor

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/opsworks (data-driven, direct/composite registration -- see
// pkgs/store's package doc for the underlying primitive).
//
// storedPlaybackConfiguration, storedChannel, and storedSourceLocation are
// each keyed by their own real Name field -- flat resources with names that
// are unique service-wide -- so all three are "clean" direct registrations.
//
// storedVodSource, LiveSource, PrefetchSchedule, and Program are all
// parent-scoped: a vod/live source name is only unique within its owning
// source location, a prefetch schedule name only within its owning playback
// configuration, and a program name only within its owning channel -- every
// Describe/Update/Delete call takes the (parent, child) pair together, never
// the child name alone. That matches the composite-key rule (id unique only
// within parent, access always parent-scoped), so each keeps the
// "parent/child" composite key the original map already used, with an
// additive byParent/byChannel/byPlaybackConfig Index replacing the linear
// filter-scan the original List*/Delete* methods performed.
//
// storedFunction (a map[string]*Function keyed by FunctionID) is likewise a
// clean direct registration -- FunctionID is globally unique and looked up
// bare, with no parent scoping.
//
// Left as plain maps (not store.Table-backed): channelPolicies
// (map[string]string) and tags (map[string]map[string]string) -- their
// values are plain strings/string maps, not *T, so neither fits
// store.Table's keyed-by-identity-value shape. See persistence.go for how
// each is (or, for channelPolicies, is not) carried through Snapshot/Restore.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func playbackConfigKeyFn(v *storedPlaybackConfiguration) string { return v.Name }

func channelKeyFn(v *storedChannel) string { return v.Name }

func sourceLocationKeyFn(v *storedSourceLocation) string { return v.Name }

func vodSourceKeyFn(v *storedVodSource) string {
	return vodSourceKey(v.SourceLocationName, v.VodSourceName)
}
func vodSourceLocationKeyFn(v *storedVodSource) string { return v.SourceLocationName }

func liveSourceKeyFn(v *LiveSource) string {
	return liveSourceKey(v.SourceLocationName, v.LiveSourceName)
}
func liveSourceLocationKeyFn(v *LiveSource) string { return v.SourceLocationName }

func prefetchScheduleKeyFn(v *PrefetchSchedule) string {
	return prefetchScheduleKey(v.PlaybackConfigurationName, v.Name)
}
func prefetchSchedulePlaybackConfigKeyFn(v *PrefetchSchedule) string {
	return v.PlaybackConfigurationName
}

func programKeyFn(v *Program) string        { return programKey(v.ChannelName, v.ProgramName) }
func programChannelKeyFn(v *Program) string { return v.ChannelName }

func functionKeyFn(v *Function) string { return v.FunctionID }

// registerAllTables registers every backend resource table (and its
// secondary indexes) exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.playbackConfigurations = store.Register(b.registry, "playbackConfigurations", store.New(playbackConfigKeyFn))

	b.channels = store.Register(b.registry, "channels", store.New(channelKeyFn))

	b.sourceLocations = store.Register(b.registry, "sourceLocations", store.New(sourceLocationKeyFn))

	b.vodSources = store.Register(b.registry, "vodSources", store.New(vodSourceKeyFn))
	b.vodSourcesByLocation = b.vodSources.AddIndex("byLocation", vodSourceLocationKeyFn)

	b.liveSources = store.Register(b.registry, "liveSources", store.New(liveSourceKeyFn))
	b.liveSourcesByLocation = b.liveSources.AddIndex("byLocation", liveSourceLocationKeyFn)

	b.prefetchSchedules = store.Register(b.registry, "prefetchSchedules", store.New(prefetchScheduleKeyFn))
	b.prefetchSchedulesByConfig = b.prefetchSchedules.AddIndex("byPlaybackConfig", prefetchSchedulePlaybackConfigKeyFn)

	b.programs = store.Register(b.registry, "programs", store.New(programKeyFn))
	b.programsByChannel = b.programs.AddIndex("byChannel", programChannelKeyFn)

	b.functions = store.Register(b.registry, "functions", store.New(functionKeyFn))
}

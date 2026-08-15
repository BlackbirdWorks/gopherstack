package pinpoint

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend whose key is a pure
// function of the stored value's own fields is registered here, exactly
// once, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 (store_setup.go) / services/sqs (persistence.go) pilots
// for the pattern this follows.
//
// The following resource fields are deliberately NOT registered here and
// remain plain maps because their key is not a pure function of the stored
// value (store.Table requires one):
//   - appSettings: StoredAppSettings carries no ApplicationID/identity field
//     of its own -- it is keyed externally by app ID, the same shape of quirk
//     that keeps EC2's instanceIMDSOptions/verifiedAccess*Policies as plain
//     maps.
//   - arnIndex: values are the tagHolder interface, not a single concrete *T
//     pointer type -- store.Table is keyed on one concrete V.
//   - campaignVersions, segmentVersions, templateVersionHistory,
//     campaignActivities, journeyRuns, appEvents: slice-valued
//     (map[string][]*T / map[string][]T), not map[string]*T.
//   - sentMessages, otpCodes: counters/tokens (map[string]int /
//     map[string]string), not map[string]*T.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func appKeyFn(v *App) string                              { return v.ID }
func campaignKeyFn(v *Campaign) string                    { return v.ID }
func segmentKeyFn(v *Segment) string                      { return v.ID }
func emailTemplateKeyFn(v *EmailTemplate) string          { return v.TemplateName }
func inAppTemplateKeyFn(v *InAppTemplate) string          { return v.TemplateName }
func pushTemplateKeyFn(v *PushTemplate) string            { return v.TemplateName }
func smsTemplateKeyFn(v *SmsTemplate) string              { return v.TemplateName }
func voiceTemplateKeyFn(v *VoiceTemplate) string          { return v.TemplateName }
func exportJobKeyFn(v *ExportJob) string                  { return v.ID }
func importJobKeyFn(v *ImportJob) string                  { return v.ID }
func journeyKeyFn(v *Journey) string                      { return v.ID }
func recommenderKeyFn(v *RecommenderConfiguration) string { return v.ID }

// endpointKeyFn mirrors the historical "<appID>/<endpointID>" map key: both
// halves are already stored on the Endpoint value itself.
func endpointKeyFn(v *Endpoint) string { return v.ApplicationID + "/" + v.ID }

// eventStreamKeyFn mirrors the historical bare-appID map key.
func eventStreamKeyFn(v *EventStream) string { return v.ApplicationID }

// channelKeyFn mirrors the historical "<appID>/<channelType>" map key.
func channelKeyFn(v *Channel) string { return v.ApplicationID + "/" + v.ChannelType }

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately
// after b.registry is created), never on every Reset() -- store.Register
// panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.apps = store.Register(b.registry, "apps", store.New(appKeyFn))
	b.campaigns = store.Register(b.registry, "campaigns", store.New(campaignKeyFn))
	b.segments = store.Register(b.registry, "segments", store.New(segmentKeyFn))
	b.emailTemplates = store.Register(b.registry, "emailTemplates", store.New(emailTemplateKeyFn))
	b.inAppTemplates = store.Register(b.registry, "inAppTemplates", store.New(inAppTemplateKeyFn))
	b.pushTemplates = store.Register(b.registry, "pushTemplates", store.New(pushTemplateKeyFn))
	b.smsTemplates = store.Register(b.registry, "smsTemplates", store.New(smsTemplateKeyFn))
	b.voiceTemplates = store.Register(b.registry, "voiceTemplates", store.New(voiceTemplateKeyFn))
	b.exportJobs = store.Register(b.registry, "exportJobs", store.New(exportJobKeyFn))
	b.importJobs = store.Register(b.registry, "importJobs", store.New(importJobKeyFn))
	b.journeys = store.Register(b.registry, "journeys", store.New(journeyKeyFn))
	b.recommenders = store.Register(b.registry, "recommenders", store.New(recommenderKeyFn))
	b.endpoints = store.Register(b.registry, "endpoints", store.New(endpointKeyFn))
	b.eventStreams = store.Register(b.registry, "eventStreams", store.New(eventStreamKeyFn))
	b.channels = store.Register(b.registry, "channels", store.New(channelKeyFn))
}

package pinpoint

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Apps           map[string]*App                      `json:"apps"`
	Campaigns      map[string]*Campaign                 `json:"campaigns"`
	EmailTemplates map[string]*EmailTemplate            `json:"emailTemplates"`
	ExportJobs     map[string]*ExportJob                `json:"exportJobs"`
	ImportJobs     map[string]*ImportJob                `json:"importJobs"`
	InAppTemplates map[string]*InAppTemplate            `json:"inAppTemplates"`
	Journeys       map[string]*Journey                  `json:"journeys"`
	PushTemplates  map[string]*PushTemplate             `json:"pushTemplates"`
	Recommenders   map[string]*RecommenderConfiguration `json:"recommenders"`
	Segments       map[string]*Segment                  `json:"segments"`
	SmsTemplates   map[string]*SmsTemplate              `json:"smsTemplates"`
	AccountID      string                               `json:"accountID"`
	Region         string                               `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// Deep copies are taken before serialisation to avoid races with concurrent writes.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	apps := make(map[string]*App, len(b.apps))
	for k, v := range b.apps {
		apps[k] = cloneApp(v)
	}

	campaigns := make(map[string]*Campaign, len(b.campaigns))
	for k, v := range b.campaigns {
		campaigns[k] = cloneCampaign(v)
	}

	emailTemplates := make(map[string]*EmailTemplate, len(b.emailTemplates))
	for k, v := range b.emailTemplates {
		emailTemplates[k] = cloneEmailTemplate(v)
	}

	exportJobs := make(map[string]*ExportJob, len(b.exportJobs))
	for k, v := range b.exportJobs {
		cp := *v
		exportJobs[k] = &cp
	}

	importJobs := make(map[string]*ImportJob, len(b.importJobs))
	for k, v := range b.importJobs {
		cp := *v
		importJobs[k] = &cp
	}

	inAppTemplates := make(map[string]*InAppTemplate, len(b.inAppTemplates))
	for k, v := range b.inAppTemplates {
		inAppTemplates[k] = cloneInAppTemplate(v)
	}

	journeys := make(map[string]*Journey, len(b.journeys))
	for k, v := range b.journeys {
		journeys[k] = cloneJourney(v)
	}

	pushTemplates := make(map[string]*PushTemplate, len(b.pushTemplates))
	for k, v := range b.pushTemplates {
		pushTemplates[k] = clonePushTemplate(v)
	}

	recommenders := make(map[string]*RecommenderConfiguration, len(b.recommenders))
	for k, v := range b.recommenders {
		cp := *v
		cp.Attributes = nonNilAttrsCopy(v.Attributes)
		recommenders[k] = &cp
	}

	segments := make(map[string]*Segment, len(b.segments))
	for k, v := range b.segments {
		segments[k] = cloneSegment(v)
	}

	smsTemplates := make(map[string]*SmsTemplate, len(b.smsTemplates))
	for k, v := range b.smsTemplates {
		smsTemplates[k] = cloneSmsTemplate(v)
	}

	snap := backendSnapshot{
		Apps:           apps,
		Campaigns:      campaigns,
		EmailTemplates: emailTemplates,
		ExportJobs:     exportJobs,
		ImportJobs:     importJobs,
		InAppTemplates: inAppTemplates,
		Journeys:       journeys,
		PushTemplates:  pushTemplates,
		Recommenders:   recommenders,
		Segments:       segments,
		SmsTemplates:   smsTemplates,
		AccountID:      b.accountID,
		Region:         b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "pinpoint: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "pinpoint", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.apps = snap.Apps
	b.campaigns = snap.Campaigns
	b.emailTemplates = snap.EmailTemplates
	b.exportJobs = snap.ExportJobs
	b.importJobs = snap.ImportJobs
	b.inAppTemplates = snap.InAppTemplates
	b.journeys = snap.Journeys
	b.pushTemplates = snap.PushTemplates
	b.recommenders = snap.Recommenders
	b.segments = snap.Segments
	b.smsTemplates = snap.SmsTemplates
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild the ARN index from all restored resources.
	b.arnIndex = make(map[string]tagHolder)
	rebuildARNIndexLocked(b)

	return nil
}

// rebuildARNIndexLocked rebuilds arnIndex from all in-memory resources.
// Must be called with b.mu write lock held.
func rebuildARNIndexLocked(b *InMemoryBackend) {
	for _, v := range b.apps {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.campaigns {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.emailTemplates {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.inAppTemplates {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.journeys {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.pushTemplates {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.segments {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.smsTemplates {
		b.arnIndex[v.ARN] = v
	}
}

// ensureNonNilMaps initialises any nil maps in the snapshot to avoid nil-map panics.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Apps == nil {
		snap.Apps = make(map[string]*App)
	}

	if snap.Campaigns == nil {
		snap.Campaigns = make(map[string]*Campaign)
	}

	if snap.EmailTemplates == nil {
		snap.EmailTemplates = make(map[string]*EmailTemplate)
	}

	if snap.ExportJobs == nil {
		snap.ExportJobs = make(map[string]*ExportJob)
	}

	if snap.ImportJobs == nil {
		snap.ImportJobs = make(map[string]*ImportJob)
	}

	if snap.InAppTemplates == nil {
		snap.InAppTemplates = make(map[string]*InAppTemplate)
	}

	if snap.Journeys == nil {
		snap.Journeys = make(map[string]*Journey)
	}

	if snap.PushTemplates == nil {
		snap.PushTemplates = make(map[string]*PushTemplate)
	}

	if snap.Recommenders == nil {
		snap.Recommenders = make(map[string]*RecommenderConfiguration)
	}

	if snap.Segments == nil {
		snap.Segments = make(map[string]*Segment)
	}

	if snap.SmsTemplates == nil {
		snap.SmsTemplates = make(map[string]*SmsTemplate)
	}
}

package pinpoint

import (
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	registry               *store.Registry
	inAppTemplates         *store.Table[InAppTemplate]
	segments               *store.Table[Segment]
	campaigns              *store.Table[Campaign]
	emailTemplates         *store.Table[EmailTemplate]
	exportJobs             *store.Table[ExportJob]
	importJobs             *store.Table[ImportJob]
	arnIndex               map[string]tagHolder
	pushTemplates          *store.Table[PushTemplate]
	apps                   *store.Table[App]
	recommenders           *store.Table[RecommenderConfiguration]
	journeys               *store.Table[Journey]
	smsTemplates           *store.Table[SmsTemplate]
	voiceTemplates         *store.Table[VoiceTemplate]
	endpoints              *store.Table[Endpoint]
	eventStreams           *store.Table[EventStream]
	channels               *store.Table[Channel]
	appSettings            map[string]*StoredAppSettings
	campaignVersions       map[string][]*Campaign
	segmentVersions        map[string][]*Segment
	templateVersionHistory map[string][]templateVersionItem
	campaignActivities     map[string][]campaignActivity
	journeyRuns            map[string][]*journeyRun
	appEvents              map[string][]storedPinpointEvent
	sentMessages           map[string]int
	otpCodes               map[string]string
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

// NewInMemoryBackend creates a new Pinpoint in-memory backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		region:                 region,
		accountID:              accountID,
		mu:                     lockmetrics.New("pinpoint"),
		registry:               store.NewRegistry(),
		arnIndex:               make(map[string]tagHolder),
		appSettings:            make(map[string]*StoredAppSettings),
		campaignVersions:       make(map[string][]*Campaign),
		segmentVersions:        make(map[string][]*Segment),
		templateVersionHistory: make(map[string][]templateVersionItem),
		campaignActivities:     make(map[string][]campaignActivity),
		journeyRuns:            make(map[string][]*journeyRun),
		appEvents:              make(map[string][]storedPinpointEvent),
		sentMessages:           make(map[string]int),
		otpCodes:               make(map[string]string),
	}

	registerAllTables(b)

	return b
}

// Reset clears all stored state and returns the backend to a pristine state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()

	b.arnIndex = make(map[string]tagHolder)
	b.appSettings = make(map[string]*StoredAppSettings)
	b.campaignVersions = make(map[string][]*Campaign)
	b.segmentVersions = make(map[string][]*Segment)
	b.templateVersionHistory = make(map[string][]templateVersionItem)
	b.campaignActivities = make(map[string][]campaignActivity)
	b.journeyRuns = make(map[string][]*journeyRun)
	b.appEvents = make(map[string][]storedPinpointEvent)
	b.sentMessages = make(map[string]int)
	b.otpCodes = make(map[string]string)
}

// Region returns the configured AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the configured AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// purgeAppStateLocked releases every piece of per-application state so that
// deleting an app does not leak entries in the various app-scoped maps. The
// backend mutex must be held by the caller.
//
// App-scoped maps fall into two shapes: those keyed by a bare application ID
// (appEvents, eventStreams, otpCodes, appSettings, sentMessages) and those
// keyed by an "<appID>/<childID>" composite (endpoints, channels,
// campaignVersions, segmentVersions, campaignActivities, journeyRuns). The
// campaigns/segments/journeys maps are keyed by child ID but carry the owning
// ApplicationID, so they are filtered by that field.
func (b *InMemoryBackend) purgeAppStateLocked(appID string) {
	delete(b.appEvents, appID)
	b.eventStreams.Delete(appID)
	delete(b.otpCodes, appID)
	delete(b.appSettings, appID)
	delete(b.sentMessages, appID)

	prefix := appID + "/"
	deletePrefixed(b.campaignVersions, prefix)
	deletePrefixed(b.segmentVersions, prefix)
	deletePrefixed(b.campaignActivities, prefix)
	deletePrefixed(b.journeyRuns, prefix)

	purgeTableByAppID(b.endpoints, appID,
		func(e *Endpoint) string { return e.ApplicationID },
		func(e *Endpoint) string { return prefix + e.ID },
	)
	purgeTableByAppID(b.channels, appID,
		func(c *Channel) string { return c.ApplicationID },
		func(c *Channel) string { return prefix + c.ChannelType },
	)
	purgeTableByAppID(b.campaigns, appID,
		func(c *Campaign) string { return c.ApplicationID },
		func(c *Campaign) string { return c.ID },
	)
	purgeTableByAppID(b.segments, appID,
		func(s *Segment) string { return s.ApplicationID },
		func(s *Segment) string { return s.ID },
	)
	purgeTableByAppID(b.journeys, appID,
		func(j *Journey) string { return j.ApplicationID },
		func(j *Journey) string { return j.ID },
	)
}

// purgeTableByAppID deletes every value in t whose owning application (per
// appIDOf) matches appID, using keyOf to recover each value's own table key.
// Factoring the scan+filter+delete loop out per resource kind keeps
// purgeAppStateLocked a flat list of calls instead of one branch per
// app-scoped table, which is what keeps its cyclomatic complexity in check.
func purgeTableByAppID[V any](t *store.Table[V], appID string, appIDOf, keyOf func(*V) string) {
	for _, v := range t.All() {
		if v != nil && appIDOf(v) == appID {
			t.Delete(keyOf(v))
		}
	}
}

// deletePrefixed removes every entry from m whose key starts with prefix.
func deletePrefixed[V any](m map[string]V, prefix string) {
	for k := range m {
		if strings.HasPrefix(k, prefix) {
			delete(m, k)
		}
	}
}

// nonNilTagsCopy returns a copy of the given tags map; never returns nil.
func nonNilTagsCopy(tags map[string]string) map[string]string {
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// nonNilAttrsCopy returns a copy of the given attribute map; never returns nil.
func nonNilAttrsCopy(attrs map[string]string) map[string]string {
	return nonNilTagsCopy(attrs)
}

// nonNilStringSliceMapCopy returns a copy of a map[string][]string; never returns nil.
func nonNilStringSliceMapCopy(m map[string][]string) map[string][]string {
	cp := make(map[string][]string, len(m))

	for k, v := range m {
		if v == nil {
			cp[k] = []string{}
		} else {
			dst := make([]string, len(v))
			copy(dst, v)
			cp[k] = dst
		}
	}

	return cp
}

// nonNilFloat64MapCopy returns a copy of a map[string]float64; never returns nil.
func nonNilFloat64MapCopy(m map[string]float64) map[string]float64 {
	cp := make(map[string]float64, len(m))
	maps.Copy(cp, m)

	return cp
}

// cloneAnyMap returns a shallow copy of a map[string]any; never returns nil.
func cloneAnyMap(m map[string]any) map[string]any {
	cp := make(map[string]any, len(m))
	maps.Copy(cp, m)

	return cp
}

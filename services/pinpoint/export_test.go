package pinpoint

import "strconv"

// MaxAppEvents exposes the per-app event cap for tests.
const MaxAppEvents = maxAppEvents

// TotalPerAppEntries counts every entry across the per-application maps so a
// leak (an entry surviving DeleteApp) shows up as a non-zero delta from
// baseline in tests.
func TotalPerAppEntries(b *InMemoryBackend) int {
	b.mu.RLock("TotalPerAppEntries")
	defer b.mu.RUnlock()

	return len(b.appEvents) + len(b.eventStreams) + len(b.otpCodes) +
		len(b.appSettings) + len(b.sentMessages) +
		len(b.endpoints) + len(b.channels) +
		len(b.campaignVersions) + len(b.segmentVersions) +
		len(b.campaignActivities) + len(b.journeyRuns) +
		len(b.campaigns) + len(b.segments) + len(b.journeys)
}

// AppEventCount returns the number of retained events for an application.
func AppEventCount(b *InMemoryBackend, appID string) int {
	b.mu.RLock("AppEventCount")
	defer b.mu.RUnlock()

	return len(b.appEvents[appID])
}

// CreateCampaignForTest creates a campaign for the app using a minimal request,
// exercising the per-app campaign maps without exporting the request type.
func CreateCampaignForTest(b *InMemoryBackend, region, accountID, appID string) error {
	_, err := b.CreateCampaign(region, accountID, appID, createCampaignRequest{})

	return err
}

// CreateSegmentForTest creates a segment for the app using a minimal request.
func CreateSegmentForTest(b *InMemoryBackend, region, accountID, appID string) error {
	_, err := b.CreateSegment(region, accountID, appID, createSegmentRequest{})

	return err
}

// CreateJourneyForTest creates a journey for the app using a minimal request.
func CreateJourneyForTest(b *InMemoryBackend, region, accountID, appID string) error {
	_, err := b.CreateJourney(region, accountID, appID, createJourneyRequest{})

	return err
}

// PutEventsForTest records n events for a single endpoint of the app, exercising
// the appEvents append/cap path without exporting the request type.
func PutEventsForTest(b *InMemoryBackend, appID string, n int) error {
	events := make(map[string]eventItem, n)
	for i := range n {
		events[strconv.Itoa(i)] = eventItem{EventType: "custom"}
	}

	var req putEventsRequest
	req.EventsRequest.BatchItem = map[string]endpointEvents{
		"endpoint-1": {Events: events},
	}

	_, err := b.PutEvents(appID, req)

	return err
}

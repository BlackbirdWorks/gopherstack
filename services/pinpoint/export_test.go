package pinpoint

import "strconv"

// MaxAppEvents exposes the per-app event cap for tests.
const MaxAppEvents = maxAppEvents

// MaxTemplateVersions exposes the template version cap for tests.
const MaxTemplateVersions = maxTemplateVersions

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

// CreateCampaignIDForTest creates a campaign and returns its ID.
func CreateCampaignIDForTest(b *InMemoryBackend, region, accountID, appID string) (string, error) {
	c, err := b.CreateCampaign(region, accountID, appID, createCampaignRequest{})
	if err != nil {
		return "", err
	}

	return c.ID, nil
}

// CreateSegmentForTest creates a segment for the app using a minimal request.
func CreateSegmentForTest(b *InMemoryBackend, region, accountID, appID string) error {
	_, err := b.CreateSegment(region, accountID, appID, createSegmentRequest{})

	return err
}

// CreateSegmentIDForTest creates a segment and returns its ID.
func CreateSegmentIDForTest(b *InMemoryBackend, region, accountID, appID string) (string, error) {
	s, err := b.CreateSegment(region, accountID, appID, createSegmentRequest{})
	if err != nil {
		return "", err
	}

	return s.ID, nil
}

// CreateJourneyForTest creates a journey for the app using a minimal request.
func CreateJourneyForTest(b *InMemoryBackend, region, accountID, appID string) error {
	_, err := b.CreateJourney(region, accountID, appID, createJourneyRequest{})

	return err
}

// TemplateVersionCount returns the number of stored versions for a template.
func TemplateVersionCount(b *InMemoryBackend, templateName, templateType string) int {
	b.mu.RLock("TemplateVersionCount")
	defer b.mu.RUnlock()

	return len(b.templateVersionHistory[templateName+"/"+templateType])
}

// CampaignVersionCount returns the number of stored version entries for a campaign.
func CampaignVersionCount(b *InMemoryBackend, appID, campaignID string) int {
	b.mu.RLock("CampaignVersionCount")
	defer b.mu.RUnlock()

	return len(b.campaignVersions[appID+"/"+campaignID])
}

// SegmentVersionCount returns the number of stored version entries for a segment.
func SegmentVersionCount(b *InMemoryBackend, appID, segmentID string) int {
	b.mu.RLock("SegmentVersionCount")
	defer b.mu.RUnlock()

	return len(b.segmentVersions[appID+"/"+segmentID])
}

// CreateEmailTemplateForTest creates an email template with the given name.
func CreateEmailTemplateForTest(b *InMemoryBackend, region, accountID, templateName string) error {
	_, err := b.CreateEmailTemplate(region, accountID, templateName, createEmailTemplateRequest{})

	return err
}

// DeleteEmailTemplateForTest deletes an email template by name.
func DeleteEmailTemplateForTest(b *InMemoryBackend, templateName string) error {
	_, err := b.DeleteEmailTemplate(templateName)

	return err
}

// UpdateEmailTemplateForTest updates an email template (increments its version).
func UpdateEmailTemplateForTest(b *InMemoryBackend, templateName string) error {
	_, err := b.UpdateEmailTemplate(templateName, createEmailTemplateRequest{})

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

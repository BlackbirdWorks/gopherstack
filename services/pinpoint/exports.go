package pinpoint

// ExportedApp is a compatibility alias used by the dashboard package.
type ExportedApp = App

// ExportedCreateAppRequest aliases createAppRequest for testing.
type ExportedCreateAppRequest = createAppRequest

// ExportedCreateCampaignRequest aliases createCampaignRequest for testing/seeding.
type ExportedCreateCampaignRequest = createCampaignRequest

// ExportedCreateEmailTemplateRequest aliases createEmailTemplateRequest for testing/seeding.
type ExportedCreateEmailTemplateRequest = createEmailTemplateRequest

// ExportedCreateInAppTemplateRequest aliases createInAppTemplateRequest for testing/seeding.
type ExportedCreateInAppTemplateRequest = createInAppTemplateRequest

// ExportedCreatePushTemplateRequest aliases createPushTemplateRequest for testing/seeding.
type ExportedCreatePushTemplateRequest = createPushTemplateRequest

// ExportedCreateSmsTemplateRequest aliases createSmsTemplateRequest for testing/seeding.
type ExportedCreateSmsTemplateRequest = createSmsTemplateRequest

// ExportedCreateExportJobRequest aliases createExportJobRequest for testing/seeding.
type ExportedCreateExportJobRequest = createExportJobRequest

// ExportedCreateImportJobRequest aliases createImportJobRequest for testing/seeding.
type ExportedCreateImportJobRequest = createImportJobRequest

// ExportedCreateJourneyRequest aliases createJourneyRequest for testing/seeding.
type ExportedCreateJourneyRequest = createJourneyRequest

// ExportedCreateSegmentRequest aliases createSegmentRequest for testing/seeding.
type ExportedCreateSegmentRequest = createSegmentRequest

// ExportedCreateRecommenderConfigRequest aliases createRecommenderConfigRequest for testing/seeding.
type ExportedCreateRecommenderConfigRequest = createRecommenderConfigRequest

// ──────────────────────────────────────────────────
// Count helpers (for tests)
// ──────────────────────────────────────────────────

// AppCount returns the number of applications in the backend.
func AppCount(b *InMemoryBackend) int {
	b.mu.RLock("AppCount")
	defer b.mu.RUnlock()

	return b.apps.Len()
}

// CampaignCount returns the number of campaigns in the backend.
func CampaignCount(b *InMemoryBackend) int {
	b.mu.RLock("CampaignCount")
	defer b.mu.RUnlock()

	return b.campaigns.Len()
}

// SegmentCount returns the number of segments in the backend.
func SegmentCount(b *InMemoryBackend) int {
	b.mu.RLock("SegmentCount")
	defer b.mu.RUnlock()

	return b.segments.Len()
}

// JourneyCount returns the number of journeys in the backend.
func JourneyCount(b *InMemoryBackend) int {
	b.mu.RLock("JourneyCount")
	defer b.mu.RUnlock()

	return b.journeys.Len()
}

// EmailTemplateCount returns the number of email templates in the backend.
func EmailTemplateCount(b *InMemoryBackend) int {
	b.mu.RLock("EmailTemplateCount")
	defer b.mu.RUnlock()

	return b.emailTemplates.Len()
}

// InAppTemplateCount returns the number of in-app templates in the backend.
func InAppTemplateCount(b *InMemoryBackend) int {
	b.mu.RLock("InAppTemplateCount")
	defer b.mu.RUnlock()

	return b.inAppTemplates.Len()
}

// PushTemplateCount returns the number of push templates in the backend.
func PushTemplateCount(b *InMemoryBackend) int {
	b.mu.RLock("PushTemplateCount")
	defer b.mu.RUnlock()

	return b.pushTemplates.Len()
}

// SmsTemplateCount returns the number of SMS templates in the backend.
func SmsTemplateCount(b *InMemoryBackend) int {
	b.mu.RLock("SmsTemplateCount")
	defer b.mu.RUnlock()

	return b.smsTemplates.Len()
}

// ExportJobCount returns the number of export jobs in the backend.
func ExportJobCount(b *InMemoryBackend) int {
	b.mu.RLock("ExportJobCount")
	defer b.mu.RUnlock()

	return b.exportJobs.Len()
}

// ImportJobCount returns the number of import jobs in the backend.
func ImportJobCount(b *InMemoryBackend) int {
	b.mu.RLock("ImportJobCount")
	defer b.mu.RUnlock()

	return b.importJobs.Len()
}

// RecommenderCount returns the number of recommender configurations in the backend.
func RecommenderCount(b *InMemoryBackend) int {
	b.mu.RLock("RecommenderCount")
	defer b.mu.RUnlock()

	return b.recommenders.Len()
}

// ARNIndexSize returns the number of entries in the ARN index.
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("ARNIndexSize")
	defer b.mu.RUnlock()

	return len(b.arnIndex)
}

// HandlerOpsLen returns the number of supported operations for the given handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

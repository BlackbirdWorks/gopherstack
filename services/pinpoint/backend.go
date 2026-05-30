package pinpoint

import (
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrAppNotFound is returned when a Pinpoint application is not found.
	ErrAppNotFound = awserr.New("NotFoundException: app not found", awserr.ErrNotFound)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrAlreadyExists is returned when a resource with the same key already exists.
	ErrAlreadyExists = awserr.New("ConflictException: resource already exists", awserr.ErrConflict)
	// ErrJourneyActive is returned when attempting to modify an ACTIVE journey's activities.
	ErrJourneyActive = awserr.New(
		"BadRequestException: journey is ACTIVE and cannot be modified",
		awserr.ErrInvalidParameter,
	)
)

const (
	journeyStateActive    = "ACTIVE"
	journeyStatePaused    = "PAUSED"
	journeyStateCancelled = "CANCELLED"
	journeyStateCompleted = "COMPLETED"
	journeyStateClosed    = "CLOSED"
)

// tagHolder is implemented by all resource types that carry tags and an ARN,
// allowing the ARN index to support tag operations on any resource type.
type tagHolder interface {
	getARN() string
	getTags() map[string]string
	setTags(map[string]string)
}

// storedAppSettings holds the persisted application-level settings.
type storedAppSettings struct {
	CampaignHook        map[string]any `json:"CampaignHook"`
	Limits              map[string]any `json:"Limits"`
	QuietTime           map[string]any `json:"QuietTime"`
	CloudWatchMetrics   bool           `json:"CloudWatchMetrics"`
	EventTaggingEnabled bool           `json:"EventTaggingEnabled"`
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	inAppTemplates         map[string]*InAppTemplate
	segments               map[string]*Segment
	campaigns              map[string]*Campaign
	emailTemplates         map[string]*EmailTemplate
	exportJobs             map[string]*ExportJob
	importJobs             map[string]*ImportJob
	arnIndex               map[string]tagHolder
	pushTemplates          map[string]*PushTemplate
	apps                   map[string]*App
	recommenders           map[string]*RecommenderConfiguration
	journeys               map[string]*Journey
	smsTemplates           map[string]*SmsTemplate
	voiceTemplates         map[string]*VoiceTemplate
	endpoints              map[string]*Endpoint
	eventStreams           map[string]*EventStream
	channels               map[string]*Channel
	appSettings            map[string]*storedAppSettings
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
	return &InMemoryBackend{
		region:                 region,
		accountID:              accountID,
		mu:                     lockmetrics.New("pinpoint"),
		apps:                   make(map[string]*App),
		arnIndex:               make(map[string]tagHolder),
		campaigns:              make(map[string]*Campaign),
		channels:               make(map[string]*Channel),
		emailTemplates:         make(map[string]*EmailTemplate),
		endpoints:              make(map[string]*Endpoint),
		eventStreams:           make(map[string]*EventStream),
		exportJobs:             make(map[string]*ExportJob),
		importJobs:             make(map[string]*ImportJob),
		inAppTemplates:         make(map[string]*InAppTemplate),
		journeys:               make(map[string]*Journey),
		pushTemplates:          make(map[string]*PushTemplate),
		recommenders:           make(map[string]*RecommenderConfiguration),
		segments:               make(map[string]*Segment),
		smsTemplates:           make(map[string]*SmsTemplate),
		voiceTemplates:         make(map[string]*VoiceTemplate),
		appSettings:            make(map[string]*storedAppSettings),
		campaignVersions:       make(map[string][]*Campaign),
		segmentVersions:        make(map[string][]*Segment),
		templateVersionHistory: make(map[string][]templateVersionItem),
		campaignActivities:     make(map[string][]campaignActivity),
		journeyRuns:            make(map[string][]*journeyRun),
		appEvents:              make(map[string][]storedPinpointEvent),
		sentMessages:           make(map[string]int),
		otpCodes:               make(map[string]string),
	}
}

// Reset clears all stored state and returns the backend to a pristine state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.apps = make(map[string]*App)
	b.arnIndex = make(map[string]tagHolder)
	b.campaigns = make(map[string]*Campaign)
	b.channels = make(map[string]*Channel)
	b.emailTemplates = make(map[string]*EmailTemplate)
	b.endpoints = make(map[string]*Endpoint)
	b.eventStreams = make(map[string]*EventStream)
	b.exportJobs = make(map[string]*ExportJob)
	b.importJobs = make(map[string]*ImportJob)
	b.inAppTemplates = make(map[string]*InAppTemplate)
	b.journeys = make(map[string]*Journey)
	b.pushTemplates = make(map[string]*PushTemplate)
	b.recommenders = make(map[string]*RecommenderConfiguration)
	b.segments = make(map[string]*Segment)
	b.smsTemplates = make(map[string]*SmsTemplate)
	b.voiceTemplates = make(map[string]*VoiceTemplate)
	b.appSettings = make(map[string]*storedAppSettings)
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

// ──────────────────────────────────────────────────
// Seed helpers (for testing / demo data)
// ──────────────────────────────────────────────────

// AddAppInternal seeds an application directly without going through the HTTP layer.
func (b *InMemoryBackend) AddAppInternal(app *App) {
	b.mu.Lock("AddAppInternal")
	defer b.mu.Unlock()

	b.apps[app.ID] = app
	b.arnIndex[app.ARN] = app
}

// AddCampaignInternal seeds a campaign directly without going through the HTTP layer.
func (b *InMemoryBackend) AddCampaignInternal(c *Campaign) {
	b.mu.Lock("AddCampaignInternal")
	defer b.mu.Unlock()

	b.campaigns[c.ID] = c
	b.arnIndex[c.ARN] = c
}

// AddSegmentInternal seeds a segment directly without going through the HTTP layer.
func (b *InMemoryBackend) AddSegmentInternal(s *Segment) {
	b.mu.Lock("AddSegmentInternal")
	defer b.mu.Unlock()

	b.segments[s.ID] = s
	b.arnIndex[s.ARN] = s
}

// AddJourneyInternal seeds a journey directly without going through the HTTP layer.
func (b *InMemoryBackend) AddJourneyInternal(j *Journey) {
	b.mu.Lock("AddJourneyInternal")
	defer b.mu.Unlock()

	b.journeys[j.ID] = j
	b.arnIndex[j.ARN] = j
}

// ──────────────────────────────────────────────────
// App CRUD
// ──────────────────────────────────────────────────

// CreateApp creates a new Pinpoint application.
func (b *InMemoryBackend) CreateApp(region, accountID, name string, tags map[string]string) (*App, error) {
	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	appID := uuid.NewString()
	appARN := arn.Build("mobiletargeting", region, accountID, "apps/"+appID)

	app := &App{
		ID:           appID,
		Name:         name,
		ARN:          appARN,
		Tags:         nonNilTagsCopy(tags),
		CreationDate: nowRFC3339(),
	}

	b.apps[appID] = app
	b.arnIndex[appARN] = app

	return cloneApp(app), nil
}

// GetApp retrieves a Pinpoint application by ID.
func (b *InMemoryBackend) GetApp(appID string) (*App, error) {
	b.mu.RLock("GetApp")
	defer b.mu.RUnlock()

	app, ok := b.apps[appID]
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneApp(app), nil
}

// DeleteApp deletes a Pinpoint application by ID.
func (b *InMemoryBackend) DeleteApp(appID string) (*App, error) {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	app, ok := b.apps[appID]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.apps, appID)
	delete(b.arnIndex, app.ARN)

	return cloneApp(app), nil
}

// GetApps returns all Pinpoint applications sorted by name.
func (b *InMemoryBackend) GetApps() ([]*App, error) {
	b.mu.RLock("GetApps")
	defer b.mu.RUnlock()

	apps := make([]*App, 0, len(b.apps))

	for _, app := range b.apps {
		apps = append(apps, cloneApp(app))
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].Name < apps[j].Name
	})

	return apps, nil
}

// ──────────────────────────────────────────────────
// Tag operations (all ARN-indexed resource types)
// ──────────────────────────────────────────────────

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	res := b.arnIndex[resourceARN]
	if res == nil {
		return ErrAppNotFound
	}

	current := res.getTags()
	if current == nil {
		current = make(map[string]string)
	}

	maps.Copy(current, tags)
	res.setTags(current)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	res := b.arnIndex[resourceARN]
	if res == nil {
		return ErrAppNotFound
	}

	tags := res.getTags()

	for _, k := range tagKeys {
		delete(tags, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	res := b.arnIndex[resourceARN]
	if res == nil {
		return nil, ErrAppNotFound
	}

	return nonNilTagsCopy(res.getTags()), nil
}

// ──────────────────────────────────────────────────
// Campaign
// ──────────────────────────────────────────────────

// CreateCampaign creates a new Pinpoint campaign for an application.
func (b *InMemoryBackend) CreateCampaign(
	region, accountID, appID string,
	req createCampaignRequest,
) (*Campaign, error) {
	b.mu.Lock("CreateCampaign")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	id := uuid.NewString()
	campaignARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/campaigns/%s", appID, id))
	now := nowRFC3339()

	status := campaignStatusScheduled
	if req.IsPaused {
		status = campaignStatusPaused
	}

	c := &Campaign{
		ApplicationID:               appID,
		ARN:                         campaignARN,
		ID:                          id,
		Name:                        req.Name,
		SegmentID:                   req.SegmentID,
		SegmentVersion:              req.SegmentVersion,
		Tags:                        nonNilTagsCopy(req.Tags),
		MessageConfiguration:        cloneAnyMap(req.MessageConfiguration),
		Schedule:                    cloneAnyMap(req.Schedule),
		Hook:                        cloneAnyMap(req.Hook),
		Limits:                      cloneAnyMap(req.Limits),
		TemplateConfiguration:       cloneAnyMap(req.TemplateConfiguration),
		CustomDeliveryConfiguration: cloneAnyMap(req.CustomDeliveryConfiguration),
		TreatmentDescription:        req.TreatmentDescription,
		TreatmentName:               req.TreatmentName,
		Priority:                    req.Priority,
		IsPaused:                    req.IsPaused,
		Status:                      status,
		CreationDate:                now,
		LastModifiedDate:            now,
	}

	if req.AdditionalTreatments != nil {
		c.AdditionalTreatments = make([]map[string]any, len(req.AdditionalTreatments))
		for i, t := range req.AdditionalTreatments {
			c.AdditionalTreatments[i] = cloneAnyMap(t)
		}
	}

	c.Version = 1
	b.campaigns[id] = c
	b.arnIndex[campaignARN] = c

	// Track campaign version history.
	versionKey := appID + "/" + id
	b.campaignVersions[versionKey] = []*Campaign{cloneCampaign(c)}

	// Create an initial activity record.
	actKey := appID + "/" + id
	b.campaignActivities[actKey] = []campaignActivity{
		{ApplicationID: appID, CampaignID: id, ID: uuid.NewString()},
	}

	return cloneCampaign(c), nil
}

// ──────────────────────────────────────────────────
// Templates
// ──────────────────────────────────────────────────

// CreateEmailTemplate creates a new Pinpoint email template.
func (b *InMemoryBackend) CreateEmailTemplate(
	region, accountID, templateName string,
	req createEmailTemplateRequest,
) (*EmailTemplate, error) {
	b.mu.Lock("CreateEmailTemplate")
	defer b.mu.Unlock()

	if _, exists := b.emailTemplates[templateName]; exists {
		return nil, ErrAlreadyExists
	}

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/EMAIL", templateName))

	now := nowRFC3339()
	t := &EmailTemplate{
		ARN:                  templateARN,
		CreationDate:         now,
		DefaultSubstitutions: cloneAnyMap(req.DefaultSubstitutions),
		HTMLPart:             req.HTMLPart,
		LastModifiedDate:     now,
		RecommenderID:        req.RecommenderID,
		Subject:              req.Subject,
		Tags:                 nonNilTagsCopy(req.Tags),
		TemplateDescription:  req.TemplateDescription,
		TemplateName:         templateName,
		TextPart:             req.TextPart,
		Version:              "1",
	}

	b.emailTemplates[templateName] = t
	b.arnIndex[templateARN] = t

	// Track template version history.
	versionKey := templateName + "/EMAIL"
	b.templateVersionHistory[versionKey] = []templateVersionItem{
		{TemplateName: templateName, TemplateType: ChannelTypeEmail, TemplateVersion: "1"},
	}

	return cloneEmailTemplate(t), nil
}

// CreateInAppTemplate creates a new Pinpoint in-app template.
func (b *InMemoryBackend) CreateInAppTemplate(
	region, accountID, templateName string,
	req createInAppTemplateRequest,
) (*InAppTemplate, error) {
	b.mu.Lock("CreateInAppTemplate")
	defer b.mu.Unlock()

	if _, exists := b.inAppTemplates[templateName]; exists {
		return nil, ErrAlreadyExists
	}

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/INAPP", templateName))

	now := nowRFC3339()
	t := &InAppTemplate{
		ARN:                 templateARN,
		Content:             cloneContentSlice(req.Content),
		CreationDate:        now,
		LastModifiedDate:    now,
		Layout:              req.Layout,
		Tags:                nonNilTagsCopy(req.Tags),
		TemplateDescription: req.TemplateDescription,
		TemplateName:        templateName,
		Version:             "1",
	}

	b.inAppTemplates[templateName] = t
	b.arnIndex[templateARN] = t

	// Track template version history.
	versionKey := templateName + "/INAPP"
	b.templateVersionHistory[versionKey] = []templateVersionItem{
		{TemplateName: templateName, TemplateType: templateTypeINAPP, TemplateVersion: "1"},
	}

	return cloneInAppTemplate(t), nil
}

// CreatePushTemplate creates a new Pinpoint push notification template.
func (b *InMemoryBackend) CreatePushTemplate(
	region, accountID, templateName string,
	req createPushTemplateRequest,
) (*PushTemplate, error) {
	b.mu.Lock("CreatePushTemplate")
	defer b.mu.Unlock()

	if _, exists := b.pushTemplates[templateName]; exists {
		return nil, ErrAlreadyExists
	}

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/PUSH", templateName))

	now := nowRFC3339()
	t := &PushTemplate{
		APNS:                cloneAnyMap(req.APNS),
		ARN:                 templateARN,
		Body:                req.Body,
		CreationDate:        now,
		Default:             cloneAnyMap(req.Default),
		GCM:                 cloneAnyMap(req.GCM),
		LastModifiedDate:    now,
		Tags:                nonNilTagsCopy(req.Tags),
		TemplateDescription: req.TemplateDescription,
		TemplateName:        templateName,
		Title:               req.Title,
		Version:             "1",
	}

	b.pushTemplates[templateName] = t
	b.arnIndex[templateARN] = t

	// Track template version history.
	versionKey := templateName + "/PUSH"
	b.templateVersionHistory[versionKey] = []templateVersionItem{
		{TemplateName: templateName, TemplateType: templateTypePUSH, TemplateVersion: "1"},
	}

	return clonePushTemplate(t), nil
}

// CreateSmsTemplate creates a new Pinpoint SMS template.
func (b *InMemoryBackend) CreateSmsTemplate(
	region, accountID, templateName string,
	req createSmsTemplateRequest,
) (*SmsTemplate, error) {
	b.mu.Lock("CreateSmsTemplate")
	defer b.mu.Unlock()

	if _, exists := b.smsTemplates[templateName]; exists {
		return nil, ErrAlreadyExists
	}

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/SMS", templateName))

	now := nowRFC3339()
	t := &SmsTemplate{
		ARN:                 templateARN,
		Body:                req.Body,
		CreationDate:        now,
		LastModifiedDate:    now,
		SenderID:            req.SenderID,
		Tags:                nonNilTagsCopy(req.Tags),
		TemplateDescription: req.TemplateDescription,
		TemplateName:        templateName,
		Version:             "1",
	}

	b.smsTemplates[templateName] = t
	b.arnIndex[templateARN] = t

	// Track template version history.
	versionKey := templateName + "/SMS"
	b.templateVersionHistory[versionKey] = []templateVersionItem{
		{TemplateName: templateName, TemplateType: ChannelTypeSMS, TemplateVersion: "1"},
	}

	return cloneSmsTemplate(t), nil
}

// ──────────────────────────────────────────────────
// Jobs
// ──────────────────────────────────────────────────

// CreateExportJob creates a new Pinpoint export job for an application.
func (b *InMemoryBackend) CreateExportJob(
	region, accountID, appID string,
	req createExportJobRequest,
) (*ExportJob, error) {
	b.mu.Lock("CreateExportJob")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	id := uuid.NewString()
	jobARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/jobs/export/%s", appID, id))

	j := &ExportJob{
		ARN:           jobARN,
		ApplicationID: appID,
		ID:            id,
		RoleArn:       req.RoleArn,
		S3UrlPrefix:   req.S3UrlPrefix,
		JobStatus:     jobStatusCreated,
		CreationDate:  nowRFC3339(),
	}

	b.exportJobs[id] = j

	cp := *j

	return &cp, nil
}

// CreateImportJob creates a new Pinpoint import job for an application.
// It also materialises an associated IMPORT-type Segment so that callers can
// reference the segment by ID after the job completes (AWS behaviour).
func (b *InMemoryBackend) CreateImportJob(
	region, accountID, appID string,
	req createImportJobRequest,
) (*ImportJob, error) {
	b.mu.Lock("CreateImportJob")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	id := uuid.NewString()
	jobARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/jobs/import/%s", appID, id))
	now := nowRFC3339()

	j := &ImportJob{
		ARN:           jobARN,
		ApplicationID: appID,
		ID:            id,
		RoleArn:       req.RoleArn,
		S3Url:         req.S3Url,
		Format:        req.Format,
		JobStatus:     jobStatusCreated,
		CreationDate:  now,
	}

	b.importJobs[id] = j

	// Materialise an IMPORT-type segment so Terraform/clients can look it up.
	segName := req.SegmentName
	if segName == "" {
		segName = "import-" + id
	}

	segID := uuid.NewString()
	segARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/segments/%s", appID, segID))

	importDef := map[string]any{
		"Format":  req.Format,
		"RoleArn": req.RoleArn,
		"S3Url":   req.S3Url,
		"Size":    0,
	}

	seg := &Segment{
		ApplicationID:    appID,
		ARN:              segARN,
		ID:               segID,
		Name:             segName,
		SegmentType:      segmentTypeImport,
		ImportDefinition: importDef,
		CreationDate:     now,
		LastModifiedDate: now,
		Version:          1,
	}

	b.segments[segID] = seg
	b.arnIndex[segARN] = seg

	versionKey := appID + "/" + segID
	b.segmentVersions[versionKey] = []*Segment{cloneSegment(seg)}

	cp := *j

	return &cp, nil
}

// ──────────────────────────────────────────────────
// Journey
// ──────────────────────────────────────────────────

// CreateJourney creates a new Pinpoint journey for an application.
func (b *InMemoryBackend) CreateJourney(region, accountID, appID string, req createJourneyRequest) (*Journey, error) {
	b.mu.Lock("CreateJourney")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	id := uuid.NewString()
	journeyARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/journeys/%s", appID, id))
	now := nowRFC3339()

	j := &Journey{
		ApplicationID:          appID,
		ARN:                    journeyARN,
		ID:                     id,
		Name:                   req.Name,
		State:                  journeyStateDraft,
		Tags:                   nonNilTagsCopy(req.Tags),
		StartActivity:          req.StartActivity,
		RefreshFrequency:       req.RefreshFrequency,
		LocalTime:              req.LocalTime,
		WaitForQuietTime:       req.WaitForQuietTime,
		RefreshOnSegmentUpdate: req.RefreshOnSegmentUpdate,
		StartCondition:         cloneAnyMap(req.StartCondition),
		Schedule:               cloneAnyMap(req.Schedule),
		Limits:                 cloneAnyMap(req.Limits),
		QuietTime:              cloneAnyMap(req.QuietTime),
		OpenHours:              cloneAnyMap(req.OpenHours),
		ClosedDays:             cloneAnyMap(req.ClosedDays),
		CreationDate:           now,
		LastModifiedDate:       now,
	}

	if req.Activities != nil {
		j.Activities = make(map[string]map[string]any, len(req.Activities))
		for k, v := range req.Activities {
			j.Activities[k] = cloneAnyMap(v)
		}
	}

	b.journeys[id] = j
	b.arnIndex[journeyARN] = j

	return cloneJourney(j), nil
}

// ──────────────────────────────────────────────────
// Recommender
// ──────────────────────────────────────────────────

// CreateRecommenderConfiguration creates a new Pinpoint recommender configuration.
func (b *InMemoryBackend) CreateRecommenderConfiguration(
	req createRecommenderConfigRequest,
) (*RecommenderConfiguration, error) {
	b.mu.Lock("CreateRecommenderConfiguration")
	defer b.mu.Unlock()

	if !isValidRecommenderIDType(req.RecommendationProviderIDType) {
		return nil, ErrValidation
	}

	id := uuid.NewString()
	now := nowRFC3339()

	r := &RecommenderConfiguration{
		Attributes:                    nonNilAttrsCopy(req.Attributes),
		ID:                            id,
		Name:                          req.Name,
		Description:                   req.Description,
		RecommendationProviderIDType:  req.RecommendationProviderIDType,
		RecommendationProviderRoleARN: req.RecommendationProviderRoleArn,
		RecommendationProviderURI:     req.RecommendationProviderURI,
		RecommendationsPerMessage:     req.RecommendationsPerMessage,
		CreationDate:                  now,
		LastModifiedDate:              now,
	}

	b.recommenders[id] = r

	cp := *r
	cp.Attributes = nonNilAttrsCopy(r.Attributes)

	return &cp, nil
}

// ──────────────────────────────────────────────────
// Segment
// ──────────────────────────────────────────────────

// CreateSegment creates a new Pinpoint segment for an application.
func (b *InMemoryBackend) CreateSegment(region, accountID, appID string, req createSegmentRequest) (*Segment, error) {
	b.mu.Lock("CreateSegment")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	id := uuid.NewString()
	segmentARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/segments/%s", appID, id))

	now2 := nowRFC3339()
	segType := segmentTypeDimensional

	if len(req.ImportDefinition) > 0 {
		segType = segmentTypeImport
	}

	s := &Segment{
		ApplicationID:    appID,
		ARN:              segmentARN,
		ID:               id,
		Name:             req.Name,
		SegmentType:      segType,
		Tags:             nonNilTagsCopy(req.Tags),
		Dimensions:       cloneAnyMap(req.Dimensions),
		SegmentGroups:    cloneAnyMap(req.SegmentGroups),
		ImportDefinition: cloneAnyMap(req.ImportDefinition),
		CreationDate:     now2,
		LastModifiedDate: now2,
	}

	s.Version = 1
	b.segments[id] = s
	b.arnIndex[segmentARN] = s

	// Track segment version history.
	versionKey := appID + "/" + id
	b.segmentVersions[versionKey] = []*Segment{cloneSegment(s)}

	return cloneSegment(s), nil
}

// ──────────────────────────────────────────────────
// Clone helpers (deep-copy per resource type)
// ──────────────────────────────────────────────────

func cloneApp(a *App) *App {
	cp := *a
	cp.Tags = nonNilTagsCopy(a.Tags)

	return &cp
}

func cloneCampaign(c *Campaign) *Campaign {
	cp := *c
	cp.Tags = nonNilTagsCopy(c.Tags)
	cp.MessageConfiguration = cloneAnyMap(c.MessageConfiguration)
	cp.Schedule = cloneAnyMap(c.Schedule)
	cp.Hook = cloneAnyMap(c.Hook)
	cp.Limits = cloneAnyMap(c.Limits)
	cp.TemplateConfiguration = cloneAnyMap(c.TemplateConfiguration)
	cp.CustomDeliveryConfiguration = cloneAnyMap(c.CustomDeliveryConfiguration)

	if c.AdditionalTreatments != nil {
		cp.AdditionalTreatments = make([]map[string]any, len(c.AdditionalTreatments))
		for i, t := range c.AdditionalTreatments {
			cp.AdditionalTreatments[i] = cloneAnyMap(t)
		}
	}

	return &cp
}

func cloneEmailTemplate(t *EmailTemplate) *EmailTemplate {
	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)
	cp.DefaultSubstitutions = cloneAnyMap(t.DefaultSubstitutions)

	return &cp
}

func cloneInAppTemplate(t *InAppTemplate) *InAppTemplate {
	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)
	cp.Content = cloneContentSlice(t.Content)

	return &cp
}

func cloneContentSlice(src []map[string]any) []map[string]any {
	if src == nil {
		return nil
	}

	dst := make([]map[string]any, len(src))
	for i, m := range src {
		dst[i] = cloneAnyMap(m)
	}

	return dst
}

func clonePushTemplate(t *PushTemplate) *PushTemplate {
	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)
	cp.Default = cloneAnyMap(t.Default)
	cp.GCM = cloneAnyMap(t.GCM)
	cp.APNS = cloneAnyMap(t.APNS)

	return &cp
}

func cloneSmsTemplate(t *SmsTemplate) *SmsTemplate {
	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)

	return &cp
}

func cloneJourney(j *Journey) *Journey {
	cp := *j
	cp.Tags = nonNilTagsCopy(j.Tags)
	cp.StartCondition = cloneAnyMap(j.StartCondition)
	cp.Schedule = cloneAnyMap(j.Schedule)
	cp.Limits = cloneAnyMap(j.Limits)
	cp.QuietTime = cloneAnyMap(j.QuietTime)
	cp.OpenHours = cloneAnyMap(j.OpenHours)
	cp.ClosedDays = cloneAnyMap(j.ClosedDays)

	if j.Activities != nil {
		cp.Activities = make(map[string]map[string]any, len(j.Activities))
		for k, v := range j.Activities {
			cp.Activities[k] = cloneAnyMap(v)
		}
	}

	return &cp
}

func cloneSegment(s *Segment) *Segment {
	cp := *s
	cp.Tags = nonNilTagsCopy(s.Tags)
	cp.Dimensions = cloneAnyMap(s.Dimensions)
	cp.SegmentGroups = cloneAnyMap(s.SegmentGroups)
	cp.ImportDefinition = cloneAnyMap(s.ImportDefinition)

	return &cp
}

// ──────────────────────────────────────────────────
// Tag/attr copy helpers
// ──────────────────────────────────────────────────

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

// ──────────────────────────────────────────────────
// tagHolder implementations
// ──────────────────────────────────────────────────

// Compile-time assertions that all tag-bearing resource types implement tagHolder.
var (
	_ tagHolder = (*App)(nil)
	_ tagHolder = (*Campaign)(nil)
	_ tagHolder = (*EmailTemplate)(nil)
	_ tagHolder = (*InAppTemplate)(nil)
	_ tagHolder = (*Journey)(nil)
	_ tagHolder = (*PushTemplate)(nil)
	_ tagHolder = (*Segment)(nil)
	_ tagHolder = (*SmsTemplate)(nil)
)

func (a *App) getARN() string              { return a.ARN }
func (a *App) getTags() map[string]string  { return a.Tags }
func (a *App) setTags(t map[string]string) { a.Tags = t }

func (c *Campaign) getARN() string              { return c.ARN }
func (c *Campaign) getTags() map[string]string  { return c.Tags }
func (c *Campaign) setTags(t map[string]string) { c.Tags = t }

func (e *EmailTemplate) getARN() string              { return e.ARN }
func (e *EmailTemplate) getTags() map[string]string  { return e.Tags }
func (e *EmailTemplate) setTags(t map[string]string) { e.Tags = t }

func (i *InAppTemplate) getARN() string              { return i.ARN }
func (i *InAppTemplate) getTags() map[string]string  { return i.Tags }
func (i *InAppTemplate) setTags(t map[string]string) { i.Tags = t }

func (j *Journey) getARN() string              { return j.ARN }
func (j *Journey) getTags() map[string]string  { return j.Tags }
func (j *Journey) setTags(t map[string]string) { j.Tags = t }

func (p *PushTemplate) getARN() string              { return p.ARN }
func (p *PushTemplate) getTags() map[string]string  { return p.Tags }
func (p *PushTemplate) setTags(t map[string]string) { p.Tags = t }

func (s *Segment) getARN() string              { return s.ARN }
func (s *Segment) getTags() map[string]string  { return s.Tags }
func (s *Segment) setTags(t map[string]string) { s.Tags = t }

func (t *SmsTemplate) getARN() string              { return t.ARN }
func (t *SmsTemplate) getTags() map[string]string  { return t.Tags }
func (t *SmsTemplate) setTags(m map[string]string) { t.Tags = m }

package pinpoint

import (
	"fmt"
	"maps"
	"sort"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/google/uuid"
)

// ErrAppNotFound is returned when a Pinpoint application is not found.
var ErrAppNotFound = awserr.New("NotFoundException: app not found", awserr.ErrNotFound)

// StorageBackend is the storage interface for the Pinpoint service.
type StorageBackend interface {
	CreateApp(region, accountID, name string, tags map[string]string) (*App, error)
	GetApp(appID string) (*App, error)
	DeleteApp(appID string) (*App, error)
	GetApps() ([]*App, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	CreateCampaign(region, accountID, appID string, req createCampaignRequest) (*Campaign, error)
	CreateEmailTemplate(region, accountID, templateName string, req createEmailTemplateRequest) (*EmailTemplate, error)
	CreateExportJob(appID string, req createExportJobRequest) (*ExportJob, error)
	CreateImportJob(appID string, req createImportJobRequest) (*ImportJob, error)
	CreateInAppTemplate(region, accountID, templateName string, req createInAppTemplateRequest) (*InAppTemplate, error)
	CreateJourney(region, accountID, appID string, req createJourneyRequest) (*Journey, error)
	CreatePushTemplate(region, accountID, templateName string, req createPushTemplateRequest) (*PushTemplate, error)
	CreateRecommenderConfiguration(req createRecommenderConfigRequest) (*RecommenderConfiguration, error)
	CreateSegment(region, accountID, appID string, req createSegmentRequest) (*Segment, error)
	CreateSmsTemplate(region, accountID, templateName string, req createSmsTemplateRequest) (*SmsTemplate, error)
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	apps           map[string]*App
	campaigns      map[string]*Campaign
	emailTemplates map[string]*EmailTemplate
	exportJobs     map[string]*ExportJob
	importJobs     map[string]*ImportJob
	inAppTemplates map[string]*InAppTemplate
	journeys       map[string]*Journey
	pushTemplates  map[string]*PushTemplate
	recommenders   map[string]*RecommenderConfiguration
	segments       map[string]*Segment
	smsTemplates   map[string]*SmsTemplate
	region         string
	accountID      string
	mu             sync.RWMutex
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new Pinpoint in-memory backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		region:         region,
		accountID:      accountID,
		apps:           make(map[string]*App),
		campaigns:      make(map[string]*Campaign),
		emailTemplates: make(map[string]*EmailTemplate),
		exportJobs:     make(map[string]*ExportJob),
		importJobs:     make(map[string]*ImportJob),
		inAppTemplates: make(map[string]*InAppTemplate),
		journeys:       make(map[string]*Journey),
		pushTemplates:  make(map[string]*PushTemplate),
		recommenders:   make(map[string]*RecommenderConfiguration),
		segments:       make(map[string]*Segment),
		smsTemplates:   make(map[string]*SmsTemplate),
	}
}

// CreateApp creates a new Pinpoint application.
func (b *InMemoryBackend) CreateApp(region, accountID, name string, tags map[string]string) (*App, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	appID := uuid.NewString()
	appARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s", appID))

	appTags := make(map[string]string)
	maps.Copy(appTags, tags)

	app := &App{
		ID:           appID,
		Name:         name,
		ARN:          appARN,
		Tags:         appTags,
		CreationDate: nowRFC3339(),
	}

	b.apps[appID] = app

	cp := *app
	cp.Tags = make(map[string]string, len(app.Tags))
	maps.Copy(cp.Tags, app.Tags)

	return &cp, nil
}

// GetApp retrieves a Pinpoint application by ID.
func (b *InMemoryBackend) GetApp(appID string) (*App, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	app, ok := b.apps[appID]
	if !ok {
		return nil, ErrAppNotFound
	}

	cp := *app
	cp.Tags = make(map[string]string, len(app.Tags))
	maps.Copy(cp.Tags, app.Tags)

	return &cp, nil
}

// DeleteApp deletes a Pinpoint application by ID.
func (b *InMemoryBackend) DeleteApp(appID string) (*App, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, ok := b.apps[appID]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.apps, appID)

	cp := *app
	cp.Tags = make(map[string]string, len(app.Tags))
	maps.Copy(cp.Tags, app.Tags)

	return &cp, nil
}

// GetApps returns all Pinpoint applications sorted by name.
func (b *InMemoryBackend) GetApps() ([]*App, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	apps := make([]*App, 0, len(b.apps))

	for _, app := range b.apps {
		cp := *app
		cp.Tags = make(map[string]string, len(app.Tags))
		maps.Copy(cp.Tags, app.Tags)
		apps = append(apps, &cp)
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].Name < apps[j].Name
	})

	return apps, nil
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app := b.findByARN(resourceARN)
	if app == nil {
		return ErrAppNotFound
	}

	if app.Tags == nil {
		app.Tags = make(map[string]string)
	}

	maps.Copy(app.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app := b.findByARN(resourceARN)
	if app == nil {
		return ErrAppNotFound
	}

	for _, k := range tagKeys {
		delete(app.Tags, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	app := b.findByARN(resourceARN)
	if app == nil {
		return nil, ErrAppNotFound
	}

	result := make(map[string]string, len(app.Tags))
	maps.Copy(result, app.Tags)

	return result, nil
}

// findByARN looks up an app by its ARN. Must be called with lock held.
func (b *InMemoryBackend) findByARN(resourceARN string) *App {
	for _, app := range b.apps {
		if app.ARN == resourceARN {
			return app
		}
	}

	return nil
}

// CreateCampaign creates a new Pinpoint campaign for an application.
func (b *InMemoryBackend) CreateCampaign(
	region, accountID, appID string,
	req createCampaignRequest,
) (*Campaign, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	campaignARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/campaigns/%s", appID, id))
	now := nowRFC3339()

	tags := make(map[string]string, len(req.Tags))
	maps.Copy(tags, req.Tags)

	c := &Campaign{
		ApplicationID:    appID,
		ARN:              campaignARN,
		ID:               id,
		Name:             req.Name,
		SegmentID:        req.SegmentID,
		SegmentVersion:   req.SegmentVersion,
		Tags:             tags,
		CreationDate:     now,
		LastModifiedDate: now,
	}

	b.campaigns[id] = c

	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	return &cp, nil
}

// CreateEmailTemplate creates a new Pinpoint email template.
func (b *InMemoryBackend) CreateEmailTemplate(
	region, accountID, templateName string,
	req createEmailTemplateRequest,
) (*EmailTemplate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/EMAIL", templateName))

	tags := make(map[string]string, len(req.Tags))
	maps.Copy(tags, req.Tags)

	t := &EmailTemplate{
		ARN:          templateARN,
		TemplateName: templateName,
		Tags:         tags,
		CreationDate: nowRFC3339(),
	}

	b.emailTemplates[templateName] = t

	cp := *t
	cp.Tags = make(map[string]string, len(t.Tags))
	maps.Copy(cp.Tags, t.Tags)

	return &cp, nil
}

// CreateExportJob creates a new Pinpoint export job for an application.
func (b *InMemoryBackend) CreateExportJob(appID string, _ createExportJobRequest) (*ExportJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()

	j := &ExportJob{
		ApplicationID: appID,
		ID:            id,
		JobStatus:     "CREATED",
		CreationDate:  nowRFC3339(),
	}

	b.exportJobs[id] = j

	cp := *j

	return &cp, nil
}

// CreateImportJob creates a new Pinpoint import job for an application.
func (b *InMemoryBackend) CreateImportJob(appID string, _ createImportJobRequest) (*ImportJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()

	j := &ImportJob{
		ApplicationID: appID,
		ID:            id,
		JobStatus:     "CREATED",
		CreationDate:  nowRFC3339(),
	}

	b.importJobs[id] = j

	cp := *j

	return &cp, nil
}

// CreateInAppTemplate creates a new Pinpoint in-app template.
func (b *InMemoryBackend) CreateInAppTemplate(
	region, accountID, templateName string,
	req createInAppTemplateRequest,
) (*InAppTemplate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/INAPP", templateName))

	tags := make(map[string]string, len(req.Tags))
	maps.Copy(tags, req.Tags)

	t := &InAppTemplate{
		ARN:          templateARN,
		TemplateName: templateName,
		Tags:         tags,
		CreationDate: nowRFC3339(),
	}

	b.inAppTemplates[templateName] = t

	cp := *t
	cp.Tags = make(map[string]string, len(t.Tags))
	maps.Copy(cp.Tags, t.Tags)

	return &cp, nil
}

// CreateJourney creates a new Pinpoint journey for an application.
func (b *InMemoryBackend) CreateJourney(_, _, appID string, req createJourneyRequest) (*Journey, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	now := nowRFC3339()

	tags := make(map[string]string, len(req.Tags))
	maps.Copy(tags, req.Tags)

	j := &Journey{
		ApplicationID:    appID,
		ID:               id,
		Name:             req.Name,
		State:            "DRAFT",
		Tags:             tags,
		CreationDate:     now,
		LastModifiedDate: now,
	}

	b.journeys[id] = j

	cp := *j
	cp.Tags = make(map[string]string, len(j.Tags))
	maps.Copy(cp.Tags, j.Tags)

	return &cp, nil
}

// CreatePushTemplate creates a new Pinpoint push notification template.
func (b *InMemoryBackend) CreatePushTemplate(
	region, accountID, templateName string,
	req createPushTemplateRequest,
) (*PushTemplate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/PUSH", templateName))

	tags := make(map[string]string, len(req.Tags))
	maps.Copy(tags, req.Tags)

	t := &PushTemplate{
		ARN:          templateARN,
		TemplateName: templateName,
		Tags:         tags,
		CreationDate: nowRFC3339(),
	}

	b.pushTemplates[templateName] = t

	cp := *t
	cp.Tags = make(map[string]string, len(t.Tags))
	maps.Copy(cp.Tags, t.Tags)

	return &cp, nil
}

// CreateRecommenderConfiguration creates a new Pinpoint recommender configuration.
func (b *InMemoryBackend) CreateRecommenderConfiguration(
	req createRecommenderConfigRequest,
) (*RecommenderConfiguration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	now := nowRFC3339()

	attrs := make(map[string]string, len(req.Attributes))
	maps.Copy(attrs, req.Attributes)

	r := &RecommenderConfiguration{
		Attributes:                    attrs,
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
	cp.Attributes = make(map[string]string, len(r.Attributes))
	maps.Copy(cp.Attributes, r.Attributes)

	return &cp, nil
}

// CreateSegment creates a new Pinpoint segment for an application.
func (b *InMemoryBackend) CreateSegment(region, accountID, appID string, req createSegmentRequest) (*Segment, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	segmentARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/segments/%s", appID, id))

	tags := make(map[string]string, len(req.Tags))
	maps.Copy(tags, req.Tags)

	s := &Segment{
		ApplicationID: appID,
		ARN:           segmentARN,
		ID:            id,
		Name:          req.Name,
		SegmentType:   "DIMENSIONAL",
		Tags:          tags,
		CreationDate:  nowRFC3339(),
	}

	b.segments[id] = s

	cp := *s
	cp.Tags = make(map[string]string, len(s.Tags))
	maps.Copy(cp.Tags, s.Tags)

	return &cp, nil
}

// CreateSmsTemplate creates a new Pinpoint SMS template.
func (b *InMemoryBackend) CreateSmsTemplate(
	region, accountID, templateName string,
	req createSmsTemplateRequest,
) (*SmsTemplate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/SMS", templateName))

	tags := make(map[string]string, len(req.Tags))
	maps.Copy(tags, req.Tags)

	t := &SmsTemplate{
		ARN:          templateARN,
		TemplateName: templateName,
		Tags:         tags,
		CreationDate: nowRFC3339(),
	}

	b.smsTemplates[templateName] = t

	cp := *t
	cp.Tags = make(map[string]string, len(t.Tags))
	maps.Copy(cp.Tags, t.Tags)

	return &cp, nil
}

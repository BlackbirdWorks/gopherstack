package pinpoint

import (
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	templateTypeEmail = "email"
	templateTypeInApp = "inapp"
	templateTypePush  = "push"
	templateTypeSMS   = "sms"
	templateTypeVoice = "voice"

	statusCodeOK = 200
)

// ──────────────────────────────────────────────────
// Channel storage types
// ──────────────────────────────────────────────────

// ChannelType represents the type of a Pinpoint channel.
type ChannelType string

const (
	// ChannelTypeADM is the Amazon Device Messaging channel.
	ChannelTypeADM = "ADM"
	// ChannelTypeAPNS is the Apple Push Notification service channel.
	ChannelTypeAPNS = "APNS"
	// ChannelTypeAPNSSandbox is the APNS Sandbox channel.
	ChannelTypeAPNSSandbox = "APNS_SANDBOX"
	// ChannelTypeAPNSVoip is the APNS VoIP channel.
	ChannelTypeAPNSVoip = "APNS_VOIP"
	// ChannelTypeAPNSVoipSandbox is the APNS VoIP Sandbox channel.
	ChannelTypeAPNSVoipSandbox = "APNS_VOIP_SANDBOX"
	// ChannelTypeBaidu is the Baidu Cloud Push channel.
	ChannelTypeBaidu = "BAIDU"
	// ChannelTypeEmail is the Email channel.
	ChannelTypeEmail = "EMAIL"
	// ChannelTypeGCM is the Google Cloud Messaging / FCM channel.
	ChannelTypeGCM = "GCM"
	// ChannelTypeSMS is the SMS channel.
	ChannelTypeSMS = "SMS"
	// ChannelTypeVoice is the Voice channel.
	ChannelTypeVoice = "VOICE"
	// ChannelTypeInApp is the In-App channel.
	ChannelTypeInApp = "IN_APP"
)

// Channel represents a generic Pinpoint channel response.
type Channel struct {
	Attributes    map[string]string `json:"Attributes,omitempty"`
	ApplicationID string            `json:"ApplicationId"`
	ChannelType   string            `json:"ChannelType"`
	Enabled       bool              `json:"Enabled"`
	IsArchived    bool              `json:"IsArchived"`
}

// VoiceTemplate represents a Pinpoint voice template.
type VoiceTemplate struct {
	Tags         map[string]string `json:"tags,omitempty"`
	ARN          string            `json:"Arn,omitempty"`
	TemplateName string            `json:"TemplateName"`
	Body         string            `json:"Body,omitempty"`
	CreationDate string            `json:"CreationDate,omitempty"`
}

// Endpoint represents a Pinpoint endpoint.
type Endpoint struct {
	ApplicationID string            `json:"ApplicationId"`
	ID            string            `json:"Id"`
	ChannelType   string            `json:"ChannelType,omitempty"`
	Address       string            `json:"Address,omitempty"`
	UserID        string            `json:"UserId,omitempty"`
	Attributes    map[string]string `json:"Attributes,omitempty"`
	CreationDate  string            `json:"CreationDate,omitempty"`
}

// EventStream represents a Pinpoint event stream.
type EventStream struct {
	ApplicationID        string `json:"ApplicationId"`
	DestinationStreamArn string `json:"DestinationStreamArn"`
	RoleArn              string `json:"RoleArn"`
	LastModifiedDate     string `json:"LastModifiedDate,omitempty"`
}

// ──────────────────────────────────────────────────
// VoiceTemplate backend methods
// ──────────────────────────────────────────────────

// CreateVoiceTemplate creates a new Pinpoint voice template.
func (b *InMemoryBackend) CreateVoiceTemplate(
	region, accountID, templateName string,
	req createVoiceTemplateRequest,
) (*VoiceTemplate, error) {
	b.mu.Lock("CreateVoiceTemplate")
	defer b.mu.Unlock()

	if _, exists := b.voiceTemplates[templateName]; exists {
		return nil, ErrAlreadyExists
	}

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/VOICE", templateName))

	t := &VoiceTemplate{
		ARN:          templateARN,
		TemplateName: templateName,
		Body:         req.Body,
		Tags:         nonNilTagsCopy(req.Tags),
		CreationDate: nowRFC3339(),
	}

	b.voiceTemplates[templateName] = t

	// Track template version history.
	versionKey := templateName + "/VOICE"
	b.templateVersionHistory[versionKey] = []templateVersionItem{
		{TemplateName: templateName, TemplateType: "VOICE", TemplateVersion: "1"},
	}

	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)

	return &cp, nil
}

// GetVoiceTemplate retrieves a Pinpoint voice template by name.
func (b *InMemoryBackend) GetVoiceTemplate(templateName string) (*VoiceTemplate, error) {
	b.mu.RLock("GetVoiceTemplate")
	defer b.mu.RUnlock()

	t, ok := b.voiceTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)

	return &cp, nil
}

// UpdateVoiceTemplate updates an existing Pinpoint voice template.
func (b *InMemoryBackend) UpdateVoiceTemplate(
	templateName string,
	req createVoiceTemplateRequest,
) (*VoiceTemplate, error) {
	b.mu.Lock("UpdateVoiceTemplate")
	defer b.mu.Unlock()

	t, ok := b.voiceTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	if req.Body != "" {
		t.Body = req.Body
	}

	if req.Tags != nil {
		t.Tags = nonNilTagsCopy(req.Tags)
	}

	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)

	return &cp, nil
}

// DeleteVoiceTemplate deletes a Pinpoint voice template by name.
func (b *InMemoryBackend) DeleteVoiceTemplate(templateName string) (*VoiceTemplate, error) {
	b.mu.Lock("DeleteVoiceTemplate")
	defer b.mu.Unlock()

	t, ok := b.voiceTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.voiceTemplates, templateName)

	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)

	return &cp, nil
}

// ListTemplates returns all templates sorted by name.
func (b *InMemoryBackend) ListTemplates() ([]*templateListItem, error) {
	b.mu.RLock("ListTemplates")
	defer b.mu.RUnlock()

	totalCap := len(b.emailTemplates) + len(b.inAppTemplates) + len(b.pushTemplates) +
		len(b.smsTemplates) + len(b.voiceTemplates)
	items := make([]*templateListItem, 0, totalCap)

	for _, t := range b.emailTemplates {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: "EMAIL",
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.inAppTemplates {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: "INAPP",
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.pushTemplates {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: "PUSH",
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.smsTemplates {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: "SMS",
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.voiceTemplates {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: "VOICE",
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].TemplateName == items[j].TemplateName {
			return items[i].TemplateType < items[j].TemplateType
		}

		return items[i].TemplateName < items[j].TemplateName
	})

	return items, nil
}

// ──────────────────────────────────────────────────
// Campaign read/update backend methods
// ──────────────────────────────────────────────────

// GetCampaign retrieves a Pinpoint campaign by appID and campaignID.
func (b *InMemoryBackend) GetCampaign(appID, campaignID string) (*Campaign, error) {
	b.mu.RLock("GetCampaign")
	defer b.mu.RUnlock()

	c, ok := b.campaigns[campaignID]
	if !ok || c.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return cloneCampaign(c), nil
}

// GetCampaigns returns all campaigns for an application.
func (b *InMemoryBackend) GetCampaigns(appID string) ([]*Campaign, error) {
	b.mu.RLock("GetCampaigns")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	var campaigns []*Campaign

	for _, c := range b.campaigns {
		if c.ApplicationID == appID {
			campaigns = append(campaigns, cloneCampaign(c))
		}
	}

	sort.Slice(campaigns, func(i, j int) bool {
		return campaigns[i].Name < campaigns[j].Name
	})

	return campaigns, nil
}

// UpdateCampaign updates an existing Pinpoint campaign.
func (b *InMemoryBackend) UpdateCampaign(appID, campaignID string, req updateCampaignRequest) (*Campaign, error) {
	b.mu.Lock("UpdateCampaign")
	defer b.mu.Unlock()

	c, ok := b.campaigns[campaignID]
	if !ok || c.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	if req.Name != "" {
		c.Name = req.Name
	}

	if req.SegmentID != "" {
		c.SegmentID = req.SegmentID
	}

	c.LastModifiedDate = nowRFC3339()
	c.Version++

	// Track campaign version history.
	versionKey := appID + "/" + campaignID
	b.campaignVersions[versionKey] = append(b.campaignVersions[versionKey], cloneCampaign(c))

	return cloneCampaign(c), nil
}

// DeleteCampaign deletes a Pinpoint campaign.
func (b *InMemoryBackend) DeleteCampaign(appID, campaignID string) (*Campaign, error) {
	b.mu.Lock("DeleteCampaign")
	defer b.mu.Unlock()

	c, ok := b.campaigns[campaignID]
	if !ok || c.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	delete(b.campaigns, campaignID)
	delete(b.arnIndex, c.ARN)

	return cloneCampaign(c), nil
}

// ──────────────────────────────────────────────────
// Segment read/update backend methods
// ──────────────────────────────────────────────────

// GetSegment retrieves a Pinpoint segment by appID and segmentID.
func (b *InMemoryBackend) GetSegment(appID, segmentID string) (*Segment, error) {
	b.mu.RLock("GetSegment")
	defer b.mu.RUnlock()

	s, ok := b.segments[segmentID]
	if !ok || s.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return cloneSegment(s), nil
}

// GetSegments returns all segments for an application.
func (b *InMemoryBackend) GetSegments(appID string) ([]*Segment, error) {
	b.mu.RLock("GetSegments")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	var segments []*Segment

	for _, s := range b.segments {
		if s.ApplicationID == appID {
			segments = append(segments, cloneSegment(s))
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Name < segments[j].Name
	})

	return segments, nil
}

// UpdateSegment updates an existing Pinpoint segment.
func (b *InMemoryBackend) UpdateSegment(appID, segmentID string, req updateSegmentRequest) (*Segment, error) {
	b.mu.Lock("UpdateSegment")
	defer b.mu.Unlock()

	s, ok := b.segments[segmentID]
	if !ok || s.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	if req.Name != "" {
		s.Name = req.Name
	}

	s.Version++

	// Track segment version history.
	versionKey := appID + "/" + segmentID
	b.segmentVersions[versionKey] = append(b.segmentVersions[versionKey], cloneSegment(s))

	return cloneSegment(s), nil
}

// DeleteSegment deletes a Pinpoint segment.
func (b *InMemoryBackend) DeleteSegment(appID, segmentID string) (*Segment, error) {
	b.mu.Lock("DeleteSegment")
	defer b.mu.Unlock()

	s, ok := b.segments[segmentID]
	if !ok || s.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	delete(b.segments, segmentID)
	delete(b.arnIndex, s.ARN)

	return cloneSegment(s), nil
}

// ──────────────────────────────────────────────────
// Journey read/update backend methods
// ──────────────────────────────────────────────────

// GetJourney retrieves a Pinpoint journey by appID and journeyID.
func (b *InMemoryBackend) GetJourney(appID, journeyID string) (*Journey, error) {
	b.mu.RLock("GetJourney")
	defer b.mu.RUnlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return cloneJourney(j), nil
}

// GetJourneys returns all journeys for an application.
func (b *InMemoryBackend) GetJourneys(appID string) ([]*Journey, error) {
	b.mu.RLock("GetJourneys")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	var journeys []*Journey

	for _, j := range b.journeys {
		if j.ApplicationID == appID {
			journeys = append(journeys, cloneJourney(j))
		}
	}

	sort.Slice(journeys, func(i, j int) bool {
		return journeys[i].Name < journeys[j].Name
	})

	return journeys, nil
}

// UpdateJourney updates an existing Pinpoint journey.
func (b *InMemoryBackend) UpdateJourney(appID, journeyID string, req updateJourneyRequest) (*Journey, error) {
	b.mu.Lock("UpdateJourney")
	defer b.mu.Unlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	if req.Name != "" {
		j.Name = req.Name
	}

	j.LastModifiedDate = nowRFC3339()

	return cloneJourney(j), nil
}

// UpdateJourneyState updates the state of a Pinpoint journey.
func (b *InMemoryBackend) UpdateJourneyState(appID, journeyID, state string) (*Journey, error) {
	b.mu.Lock("UpdateJourneyState")
	defer b.mu.Unlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	j.State = state
	j.LastModifiedDate = nowRFC3339()

	// When activating, create a journey run record.
	if state == "ACTIVE" {
		runKey := appID + "/" + journeyID
		b.journeyRuns[runKey] = append(b.journeyRuns[runKey], &journeyRun{
			RunID:         uuid.NewString(),
			JourneyID:     journeyID,
			ApplicationID: appID,
			Status:        "SCHEDULED",
		})
	}

	return cloneJourney(j), nil
}

// DeleteJourney deletes a Pinpoint journey.
func (b *InMemoryBackend) DeleteJourney(appID, journeyID string) (*Journey, error) {
	b.mu.Lock("DeleteJourney")
	defer b.mu.Unlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	delete(b.journeys, journeyID)
	delete(b.arnIndex, j.ARN)

	return cloneJourney(j), nil
}

// ──────────────────────────────────────────────────
// Template read/update/delete backend methods
// ──────────────────────────────────────────────────

// GetEmailTemplate retrieves an email template by name.
func (b *InMemoryBackend) GetEmailTemplate(templateName string) (*EmailTemplate, error) {
	b.mu.RLock("GetEmailTemplate")
	defer b.mu.RUnlock()

	t, ok := b.emailTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneEmailTemplate(t), nil
}

// UpdateEmailTemplate updates an existing email template.
func (b *InMemoryBackend) UpdateEmailTemplate(
	templateName string,
	_ createEmailTemplateRequest,
) (*EmailTemplate, error) {
	b.mu.Lock("UpdateEmailTemplate")
	defer b.mu.Unlock()

	t, ok := b.emailTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneEmailTemplate(t), nil
}

// DeleteEmailTemplate deletes an email template by name.
func (b *InMemoryBackend) DeleteEmailTemplate(templateName string) (*EmailTemplate, error) {
	b.mu.Lock("DeleteEmailTemplate")
	defer b.mu.Unlock()

	t, ok := b.emailTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.emailTemplates, templateName)
	delete(b.arnIndex, t.ARN)

	return cloneEmailTemplate(t), nil
}

// GetInAppTemplate retrieves an in-app template by name.
func (b *InMemoryBackend) GetInAppTemplate(templateName string) (*InAppTemplate, error) {
	b.mu.RLock("GetInAppTemplate")
	defer b.mu.RUnlock()

	t, ok := b.inAppTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneInAppTemplate(t), nil
}

// UpdateInAppTemplate updates an existing in-app template.
func (b *InMemoryBackend) UpdateInAppTemplate(
	templateName string,
	_ createInAppTemplateRequest,
) (*InAppTemplate, error) {
	b.mu.Lock("UpdateInAppTemplate")
	defer b.mu.Unlock()

	t, ok := b.inAppTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneInAppTemplate(t), nil
}

// DeleteInAppTemplate deletes an in-app template by name.
func (b *InMemoryBackend) DeleteInAppTemplate(templateName string) (*InAppTemplate, error) {
	b.mu.Lock("DeleteInAppTemplate")
	defer b.mu.Unlock()

	t, ok := b.inAppTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.inAppTemplates, templateName)
	delete(b.arnIndex, t.ARN)

	return cloneInAppTemplate(t), nil
}

// GetPushTemplate retrieves a push notification template by name.
func (b *InMemoryBackend) GetPushTemplate(templateName string) (*PushTemplate, error) {
	b.mu.RLock("GetPushTemplate")
	defer b.mu.RUnlock()

	t, ok := b.pushTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	return clonePushTemplate(t), nil
}

// UpdatePushTemplate updates an existing push notification template.
func (b *InMemoryBackend) UpdatePushTemplate(templateName string, _ createPushTemplateRequest) (*PushTemplate, error) {
	b.mu.Lock("UpdatePushTemplate")
	defer b.mu.Unlock()

	t, ok := b.pushTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	return clonePushTemplate(t), nil
}

// DeletePushTemplate deletes a push notification template by name.
func (b *InMemoryBackend) DeletePushTemplate(templateName string) (*PushTemplate, error) {
	b.mu.Lock("DeletePushTemplate")
	defer b.mu.Unlock()

	t, ok := b.pushTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.pushTemplates, templateName)
	delete(b.arnIndex, t.ARN)

	return clonePushTemplate(t), nil
}

// GetSmsTemplate retrieves an SMS template by name.
func (b *InMemoryBackend) GetSmsTemplate(templateName string) (*SmsTemplate, error) {
	b.mu.RLock("GetSmsTemplate")
	defer b.mu.RUnlock()

	t, ok := b.smsTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneSmsTemplate(t), nil
}

// UpdateSmsTemplate updates an existing SMS template.
func (b *InMemoryBackend) UpdateSmsTemplate(templateName string, _ createSmsTemplateRequest) (*SmsTemplate, error) {
	b.mu.Lock("UpdateSmsTemplate")
	defer b.mu.Unlock()

	t, ok := b.smsTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneSmsTemplate(t), nil
}

// DeleteSmsTemplate deletes an SMS template by name.
func (b *InMemoryBackend) DeleteSmsTemplate(templateName string) (*SmsTemplate, error) {
	b.mu.Lock("DeleteSmsTemplate")
	defer b.mu.Unlock()

	t, ok := b.smsTemplates[templateName]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.smsTemplates, templateName)
	delete(b.arnIndex, t.ARN)

	return cloneSmsTemplate(t), nil
}

// ──────────────────────────────────────────────────
// Endpoint backend methods
// ──────────────────────────────────────────────────

// GetEndpoint retrieves a Pinpoint endpoint by appID and endpointID.
func (b *InMemoryBackend) GetEndpoint(appID, endpointID string) (*Endpoint, error) {
	b.mu.RLock("GetEndpoint")
	defer b.mu.RUnlock()

	key := appID + "/" + endpointID
	e, ok := b.endpoints[key]

	if !ok {
		return nil, ErrAppNotFound
	}

	cp := *e
	cp.Attributes = nonNilAttrsCopy(e.Attributes)

	return &cp, nil
}

// UpdateEndpoint creates or updates a Pinpoint endpoint.
func (b *InMemoryBackend) UpdateEndpoint(appID, endpointID string, req updateEndpointRequest) (*Endpoint, error) {
	b.mu.Lock("UpdateEndpoint")
	defer b.mu.Unlock()

	key := appID + "/" + endpointID

	e, ok := b.endpoints[key]
	if !ok {
		e = &Endpoint{
			ApplicationID: appID,
			ID:            endpointID,
			CreationDate:  nowRFC3339(),
		}
		b.endpoints[key] = e
	}

	if req.ChannelType != "" {
		e.ChannelType = req.ChannelType
	}

	if req.Address != "" {
		e.Address = req.Address
	}

	if req.User.UserID != "" {
		e.UserID = req.User.UserID
	}

	cp := *e
	cp.Attributes = nonNilAttrsCopy(e.Attributes)

	return &cp, nil
}

// DeleteEndpoint deletes a Pinpoint endpoint.
func (b *InMemoryBackend) DeleteEndpoint(appID, endpointID string) (*Endpoint, error) {
	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	key := appID + "/" + endpointID

	e, ok := b.endpoints[key]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.endpoints, key)

	cp := *e
	cp.Attributes = nonNilAttrsCopy(e.Attributes)

	return &cp, nil
}

// GetUserEndpoints retrieves all endpoints for a user in an application.
func (b *InMemoryBackend) GetUserEndpoints(appID, userID string) ([]*Endpoint, error) {
	b.mu.RLock("GetUserEndpoints")
	defer b.mu.RUnlock()

	var endpoints []*Endpoint

	for _, e := range b.endpoints {
		if e.ApplicationID == appID && e.UserID == userID {
			cp := *e
			cp.Attributes = nonNilAttrsCopy(e.Attributes)
			endpoints = append(endpoints, &cp)
		}
	}

	return endpoints, nil
}

// DeleteUserEndpoints deletes all endpoints for a user in an application.
func (b *InMemoryBackend) DeleteUserEndpoints(appID, userID string) error {
	b.mu.Lock("DeleteUserEndpoints")
	defer b.mu.Unlock()

	for key, e := range b.endpoints {
		if e.ApplicationID == appID && e.UserID == userID {
			delete(b.endpoints, key)
		}
	}

	return nil
}

// UpdateEndpointsBatch updates multiple endpoints in a single call.
func (b *InMemoryBackend) UpdateEndpointsBatch(appID string, endpoints map[string]updateEndpointRequest) error {
	b.mu.Lock("UpdateEndpointsBatch")
	defer b.mu.Unlock()

	for endpointID, req := range endpoints {
		key := appID + "/" + endpointID

		e, ok := b.endpoints[key]
		if !ok {
			e = &Endpoint{
				ApplicationID: appID,
				ID:            endpointID,
				CreationDate:  nowRFC3339(),
			}
			b.endpoints[key] = e
		}

		if req.ChannelType != "" {
			e.ChannelType = req.ChannelType
		}

		if req.Address != "" {
			e.Address = req.Address
		}
	}

	return nil
}

// ──────────────────────────────────────────────────
// EventStream backend methods
// ──────────────────────────────────────────────────

// GetEventStream retrieves the event stream for an application.
func (b *InMemoryBackend) GetEventStream(appID string) (*EventStream, error) {
	b.mu.RLock("GetEventStream")
	defer b.mu.RUnlock()

	e, ok := b.eventStreams[appID]
	if !ok {
		return nil, ErrAppNotFound
	}

	cp := *e

	return &cp, nil
}

// PutEventStream creates or updates the event stream for an application.
func (b *InMemoryBackend) PutEventStream(appID string, req putEventStreamRequest) (*EventStream, error) {
	b.mu.Lock("PutEventStream")
	defer b.mu.Unlock()

	e := &EventStream{
		ApplicationID:        appID,
		DestinationStreamArn: req.DestinationStreamArn,
		RoleArn:              req.RoleArn,
		LastModifiedDate:     nowRFC3339(),
	}

	b.eventStreams[appID] = e

	cp := *e

	return &cp, nil
}

// DeleteEventStream deletes the event stream for an application.
func (b *InMemoryBackend) DeleteEventStream(appID string) (*EventStream, error) {
	b.mu.Lock("DeleteEventStream")
	defer b.mu.Unlock()

	e, ok := b.eventStreams[appID]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.eventStreams, appID)

	cp := *e

	return &cp, nil
}

// ──────────────────────────────────────────────────
// Channel backend methods (get/update/delete stubs)
// ──────────────────────────────────────────────────

// GetChannel retrieves or synthesises a channel response for an app.
func (b *InMemoryBackend) GetChannel(appID, channelType string) *Channel {
	b.mu.RLock("GetChannel")
	defer b.mu.RUnlock()

	key := appID + "/" + channelType
	if ch, ok := b.channels[key]; ok {
		cp := *ch

		return &cp
	}

	return &Channel{
		ApplicationID: appID,
		ChannelType:   channelType,
		Enabled:       false,
		IsArchived:    false,
	}
}

// UpsertChannel creates or updates a channel for an app.
func (b *InMemoryBackend) UpsertChannel(appID, channelType string, enabled bool) *Channel {
	b.mu.Lock("UpsertChannel")
	defer b.mu.Unlock()

	key := appID + "/" + channelType
	ch := &Channel{
		ApplicationID: appID,
		ChannelType:   channelType,
		Enabled:       enabled,
	}

	b.channels[key] = ch

	cp := *ch

	return &cp
}

// DeleteChannel removes a channel for an app.
func (b *InMemoryBackend) DeleteChannel(appID, channelType string) *Channel {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	key := appID + "/" + channelType
	ch := b.channels[key]

	if ch == nil {
		ch = &Channel{ApplicationID: appID, ChannelType: channelType}
	}

	delete(b.channels, key)

	cp := *ch

	return &cp
}

// GetAllChannels returns all channels for an app.
func (b *InMemoryBackend) GetAllChannels(appID string) map[string]*Channel {
	b.mu.RLock("GetAllChannels")
	defer b.mu.RUnlock()

	result := make(map[string]*Channel)

	for key, ch := range b.channels {
		if ch.ApplicationID == appID {
			cp := *ch
			result[key] = &cp
		}
	}

	return result
}

// ──────────────────────────────────────────────────
// Recommender get/update/delete
// ──────────────────────────────────────────────────

// GetRecommenderConfiguration retrieves a recommender by ID.
func (b *InMemoryBackend) GetRecommenderConfiguration(recommenderID string) (*RecommenderConfiguration, error) {
	b.mu.RLock("GetRecommenderConfiguration")
	defer b.mu.RUnlock()

	r, ok := b.recommenders[recommenderID]
	if !ok {
		return nil, ErrAppNotFound
	}

	cp := *r
	cp.Attributes = nonNilAttrsCopy(r.Attributes)

	return &cp, nil
}

// GetRecommenderConfigurations returns all recommender configurations.
func (b *InMemoryBackend) GetRecommenderConfigurations() ([]*RecommenderConfiguration, error) {
	b.mu.RLock("GetRecommenderConfigurations")
	defer b.mu.RUnlock()

	results := make([]*RecommenderConfiguration, 0, len(b.recommenders))

	for _, r := range b.recommenders {
		cp := *r
		cp.Attributes = nonNilAttrsCopy(r.Attributes)
		results = append(results, &cp)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].ID < results[j].ID
	})

	return results, nil
}

// UpdateRecommenderConfiguration updates an existing recommender.
func (b *InMemoryBackend) UpdateRecommenderConfiguration(
	recommenderID string,
	req createRecommenderConfigRequest,
) (*RecommenderConfiguration, error) {
	b.mu.Lock("UpdateRecommenderConfiguration")
	defer b.mu.Unlock()

	r, ok := b.recommenders[recommenderID]
	if !ok {
		return nil, ErrAppNotFound
	}

	if req.Name != "" {
		r.Name = req.Name
	}

	if req.Description != "" {
		r.Description = req.Description
	}

	r.LastModifiedDate = nowRFC3339()

	cp := *r
	cp.Attributes = nonNilAttrsCopy(r.Attributes)

	return &cp, nil
}

// DeleteRecommenderConfiguration deletes a recommender by ID.
func (b *InMemoryBackend) DeleteRecommenderConfiguration(recommenderID string) (*RecommenderConfiguration, error) {
	b.mu.Lock("DeleteRecommenderConfiguration")
	defer b.mu.Unlock()

	r, ok := b.recommenders[recommenderID]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.recommenders, recommenderID)

	cp := *r
	cp.Attributes = nonNilAttrsCopy(r.Attributes)

	return &cp, nil
}

// ──────────────────────────────────────────────────
// Job read backend methods
// ──────────────────────────────────────────────────

// GetExportJob retrieves an export job by ID.
func (b *InMemoryBackend) GetExportJob(appID, jobID string) (*ExportJob, error) {
	b.mu.RLock("GetExportJob")
	defer b.mu.RUnlock()

	j, ok := b.exportJobs[jobID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	cp := *j

	return &cp, nil
}

// GetExportJobs returns all export jobs for an app.
func (b *InMemoryBackend) GetExportJobs(appID string) ([]*ExportJob, error) {
	b.mu.RLock("GetExportJobs")
	defer b.mu.RUnlock()

	var jobs []*ExportJob

	for _, j := range b.exportJobs {
		if j.ApplicationID == appID {
			cp := *j
			jobs = append(jobs, &cp)
		}
	}

	return jobs, nil
}

// GetImportJob retrieves an import job by ID.
func (b *InMemoryBackend) GetImportJob(appID, jobID string) (*ImportJob, error) {
	b.mu.RLock("GetImportJob")
	defer b.mu.RUnlock()

	j, ok := b.importJobs[jobID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	cp := *j

	return &cp, nil
}

// GetImportJobs returns all import jobs for an app.
func (b *InMemoryBackend) GetImportJobs(appID string) ([]*ImportJob, error) {
	b.mu.RLock("GetImportJobs")
	defer b.mu.RUnlock()

	var jobs []*ImportJob

	for _, j := range b.importJobs {
		if j.ApplicationID == appID {
			cp := *j
			jobs = append(jobs, &cp)
		}
	}

	return jobs, nil
}

// GetSegmentExportJobs returns all export jobs for a segment.
func (b *InMemoryBackend) GetSegmentExportJobs(appID, _ string) ([]*ExportJob, error) {
	return b.GetExportJobs(appID)
}

// GetSegmentImportJobs returns all import jobs for a segment.
func (b *InMemoryBackend) GetSegmentImportJobs(appID, _ string) ([]*ImportJob, error) {
	return b.GetImportJobs(appID)
}

// ──────────────────────────────────────────────────
// Stub analytics / messaging backend methods
// ──────────────────────────────────────────────────

// GetApplicationDateRangeKpi returns stub KPI data for an application.
func (b *InMemoryBackend) GetApplicationDateRangeKpi(appID, kpiName string) (*kpiResult, error) {
	b.mu.RLock("GetApplicationDateRangeKpi")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	return &kpiResult{
		ApplicationID: appID,
		KpiName:       kpiName,
		KpiResult:     kpiRows{Rows: []kpiRow{}},
	}, nil
}

// GetCampaignDateRangeKpi returns stub KPI data for a campaign.
func (b *InMemoryBackend) GetCampaignDateRangeKpi(appID, campaignID, kpiName string) (*kpiResult, error) {
	b.mu.RLock("GetCampaignDateRangeKpi")
	defer b.mu.RUnlock()

	c, ok := b.campaigns[campaignID]
	if !ok || c.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return &kpiResult{
		ApplicationID: appID,
		CampaignID:    campaignID,
		KpiName:       kpiName,
		KpiResult:     kpiRows{Rows: []kpiRow{}},
	}, nil
}

// GetJourneyDateRangeKpi returns stub KPI data for a journey.
func (b *InMemoryBackend) GetJourneyDateRangeKpi(appID, journeyID, kpiName string) (*kpiResult, error) {
	b.mu.RLock("GetJourneyDateRangeKpi")
	defer b.mu.RUnlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return &kpiResult{
		ApplicationID: appID,
		JourneyID:     journeyID,
		KpiName:       kpiName,
		KpiResult:     kpiRows{Rows: []kpiRow{}},
	}, nil
}

// SendMessages sends messages (stub - returns a message ID per address).
func (b *InMemoryBackend) SendMessages(appID string, req sendMessagesRequest) (*messageResponse, error) {
	b.mu.RLock("SendMessages")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	result := &messageResponse{Result: make(map[string]messageResult)}

	for addr := range req.MessageRequest.Addresses {
		result.Result[addr] = messageResult{
			DeliveryStatus: "SUCCESSFUL",
			MessageID:      uuid.NewString(),
			StatusCode:     statusCodeOK,
		}
	}

	return result, nil
}

// SendUsersMessages sends messages to users (stub).
func (b *InMemoryBackend) SendUsersMessages(appID string) (*usersMessageResponse, error) {
	b.mu.RLock("SendUsersMessages")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	return &usersMessageResponse{Result: make(map[string]map[string]messageResult)}, nil
}

// SendOTPMessage sends an OTP message (stub).
func (b *InMemoryBackend) SendOTPMessage(appID string) (*sendOTPMessageResponse, error) {
	b.mu.RLock("SendOTPMessage")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	return &sendOTPMessageResponse{
		MessageResponse: messageResponse{Result: map[string]messageResult{}},
	}, nil
}

// VerifyOTPMessage verifies an OTP (stub - always valid).
func (b *InMemoryBackend) VerifyOTPMessage(appID string) (*verifyOTPMessageResponse, error) {
	b.mu.RLock("VerifyOTPMessage")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	return &verifyOTPMessageResponse{Valid: true}, nil
}

// PutEvents records events for an application (stub - accepts and discards).
func (b *InMemoryBackend) PutEvents(_ string, _ putEventsRequest) error {
	return nil
}

// PhoneNumberValidate validates a phone number (stub - always valid).
func (b *InMemoryBackend) PhoneNumberValidate(_ string) (*phoneNumberValidateResponse, error) {
	return &phoneNumberValidateResponse{
		NumberValidateResponse: numberValidateResponse{
			Carrier:                 "Unknown",
			PhoneType:               "MOBILE",
			PhoneTypeCode:           0,
			CleansedPhoneNumberE164: "",
		},
	}, nil
}

// RemoveAttributes removes attributes from endpoints in a segment (stub).
func (b *InMemoryBackend) RemoveAttributes(appID, attributeType string) (*attributesResource, error) {
	b.mu.RLock("RemoveAttributes")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	return &attributesResource{
		ApplicationID: appID,
		AttributeType: attributeType,
		Attributes:    []string{},
	}, nil
}

// GetInAppMessages returns in-app messages for an endpoint (stub).
func (b *InMemoryBackend) GetInAppMessages(appID, _ string) (*inAppMessagesResponse, error) {
	b.mu.RLock("GetInAppMessages")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	return &inAppMessagesResponse{
		InAppMessageCampaigns: []inAppMessageCampaign{},
	}, nil
}

// GetCampaignActivities returns campaign activities (stub).
func (b *InMemoryBackend) GetCampaignActivities(appID, campaignID string) (*campaignActivitiesResponse, error) {
	b.mu.RLock("GetCampaignActivities")
	defer b.mu.RUnlock()

	c, ok := b.campaigns[campaignID]
	if !ok || c.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return &campaignActivitiesResponse{Item: []campaignActivity{}}, nil
}

// GetJourneyExecutionMetrics returns journey execution metrics (stub).
func (b *InMemoryBackend) GetJourneyExecutionMetrics(
	appID, journeyID string,
) (*journeyExecutionMetricsResponse, error) {
	b.mu.RLock("GetJourneyExecutionMetrics")
	defer b.mu.RUnlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return &journeyExecutionMetricsResponse{
		ApplicationID: appID,
		JourneyID:     journeyID,
		Metrics:       map[string]string{},
	}, nil
}

// GetJourneyExecutionActivityMetrics returns journey activity metrics (stub).
func (b *InMemoryBackend) GetJourneyExecutionActivityMetrics(
	appID, journeyID, activityID string,
) (*journeyExecutionActivityMetricsResponse, error) {
	b.mu.RLock("GetJourneyExecutionActivityMetrics")
	defer b.mu.RUnlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return &journeyExecutionActivityMetricsResponse{
		ApplicationID: appID,
		JourneyID:     journeyID,
		ActivityID:    activityID,
		Metrics:       map[string]string{},
	}, nil
}

// GetJourneyRuns returns journey runs (stub).
func (b *InMemoryBackend) GetJourneyRuns(appID, journeyID string) (*journeyRunsResponse, error) {
	b.mu.RLock("GetJourneyRuns")
	defer b.mu.RUnlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return &journeyRunsResponse{Item: []journeyRun{}}, nil
}

// GetJourneyRunExecutionMetrics returns metrics for a specific journey run (stub).
func (b *InMemoryBackend) GetJourneyRunExecutionMetrics(
	appID, journeyID, runID string,
) (*journeyRunExecutionMetricsResponse, error) {
	b.mu.RLock("GetJourneyRunExecutionMetrics")
	defer b.mu.RUnlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return &journeyRunExecutionMetricsResponse{
		ApplicationID: appID,
		JourneyID:     journeyID,
		RunID:         runID,
		Metrics:       map[string]string{},
	}, nil
}

// GetJourneyRunExecutionActivityMetrics returns metrics for a specific journey run activity (stub).
func (b *InMemoryBackend) GetJourneyRunExecutionActivityMetrics(
	appID, journeyID, runID, activityID string,
) (*journeyRunExecutionActivityMetricsResponse, error) {
	b.mu.RLock("GetJourneyRunExecutionActivityMetrics")
	defer b.mu.RUnlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return &journeyRunExecutionActivityMetricsResponse{
		ApplicationID: appID,
		JourneyID:     journeyID,
		RunID:         runID,
		ActivityID:    activityID,
		Metrics:       map[string]string{},
	}, nil
}

// GetCampaignVersion returns a specific campaign version (stub - returns current).
func (b *InMemoryBackend) GetCampaignVersion(appID, campaignID string, _ int) (*Campaign, error) {
	return b.GetCampaign(appID, campaignID)
}

// GetCampaignVersions returns campaign versions (stub - returns current as version 1).
func (b *InMemoryBackend) GetCampaignVersions(appID, campaignID string) ([]*Campaign, error) {
	c, err := b.GetCampaign(appID, campaignID)
	if err != nil {
		return nil, err
	}

	return []*Campaign{c}, nil
}

// GetSegmentVersion returns a specific segment version (stub - returns current).
func (b *InMemoryBackend) GetSegmentVersion(appID, segmentID string, _ int) (*Segment, error) {
	return b.GetSegment(appID, segmentID)
}

// GetSegmentVersions returns segment versions (stub - returns current as version 1).
func (b *InMemoryBackend) GetSegmentVersions(appID, segmentID string) ([]*Segment, error) {
	s, err := b.GetSegment(appID, segmentID)
	if err != nil {
		return nil, err
	}

	return []*Segment{s}, nil
}

// ListTemplateVersions returns template versions for a given template (stub - single version).
func (b *InMemoryBackend) ListTemplateVersions(templateName, templateType string) ([]*templateVersionItem, error) {
	b.mu.RLock("ListTemplateVersions")
	defer b.mu.RUnlock()

	var item *templateVersionItem

	switch templateType {
	case templateTypeEmail:
		if t, ok := b.emailTemplates[templateName]; ok {
			item = &templateVersionItem{
				TemplateName:    t.TemplateName,
				TemplateType:    "EMAIL",
				TemplateVersion: "1",
			}
		}
	case templateTypeInApp:
		if t, ok := b.inAppTemplates[templateName]; ok {
			item = &templateVersionItem{
				TemplateName:    t.TemplateName,
				TemplateType:    "INAPP",
				TemplateVersion: "1",
			}
		}
	case templateTypePush:
		if t, ok := b.pushTemplates[templateName]; ok {
			item = &templateVersionItem{
				TemplateName:    t.TemplateName,
				TemplateType:    "PUSH",
				TemplateVersion: "1",
			}
		}
	case templateTypeSMS:
		if t, ok := b.smsTemplates[templateName]; ok {
			item = &templateVersionItem{
				TemplateName:    t.TemplateName,
				TemplateType:    "SMS",
				TemplateVersion: "1",
			}
		}
	case templateTypeVoice:
		if t, ok := b.voiceTemplates[templateName]; ok {
			item = &templateVersionItem{
				TemplateName:    t.TemplateName,
				TemplateType:    "VOICE",
				TemplateVersion: "1",
			}
		}
	}

	if item == nil {
		return nil, ErrAppNotFound
	}

	return []*templateVersionItem{item}, nil
}

// UpdateTemplateActiveVersion is a no-op stub (templates have a single version).
func (b *InMemoryBackend) UpdateTemplateActiveVersion(_, _ string) error {
	return nil
}

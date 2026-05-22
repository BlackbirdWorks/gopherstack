package pinpoint

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	templateTypeEmail = "email"
	templateTypeInApp = "inapp"
	templateTypePush  = "push"
	templateTypeSMS   = "sms"
	templateTypeVoice = "voice"

	templateTypeINAPP = "INAPP"
	templateTypePUSH  = "PUSH"

	minPhoneLength = 10
	otpModulus     = 1000000

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
	ExtraData     map[string]any `json:"ExtraData,omitempty"`
	ApplicationID string         `json:"ApplicationId"`
	ChannelType   string         `json:"ChannelType"`
	Platform      string         `json:"Platform,omitempty"`
	CreationDate  string         `json:"CreationDate,omitempty"`
	LastModifiedDate string      `json:"LastModifiedDate,omitempty"`
	Version       int            `json:"Version,omitempty"`
	MessagesPerSecond int        `json:"MessagesPerSecond,omitempty"`
	Enabled       bool           `json:"Enabled"`
	IsArchived    bool           `json:"IsArchived"`
	HasCredential bool           `json:"HasCredential,omitempty"`
	HasTokenKey   bool           `json:"HasTokenKey,omitempty"`
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
	Attributes     map[string][]string `json:"Attributes,omitempty"`
	UserAttributes map[string][]string `json:"UserAttributes,omitempty"`
	Metrics        map[string]float64  `json:"Metrics,omitempty"`
	Demographic    map[string]any      `json:"Demographic,omitempty"`
	Location       map[string]any      `json:"Location,omitempty"`
	ApplicationID  string              `json:"ApplicationId"`
	ID             string              `json:"Id"`
	ChannelType    string              `json:"ChannelType,omitempty"`
	Address        string              `json:"Address,omitempty"`
	UserID         string              `json:"UserId,omitempty"`
	EffectiveDate  string              `json:"EffectiveDate,omitempty"`
	CreationDate   string              `json:"CreationDate,omitempty"`
	EndpointStatus string              `json:"EndpointStatus,omitempty"`
	OptOut         string              `json:"OptOut,omitempty"`
	RequestId      string              `json:"RequestId,omitempty"`
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
		{TemplateName: templateName, TemplateType: ChannelTypeVoice, TemplateVersion: "1"},
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

	versionKey := templateName + "/VOICE"
	nextVersion := strconv.Itoa(len(b.templateVersionHistory[versionKey]) + 1)
	b.templateVersionHistory[versionKey] = append(b.templateVersionHistory[versionKey], templateVersionItem{
		TemplateName:    templateName,
		TemplateType:    ChannelTypeVoice,
		TemplateVersion: nextVersion,
	})

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
			TemplateType: ChannelTypeEmail,
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.inAppTemplates {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: templateTypeINAPP,
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.pushTemplates {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: templateTypePUSH,
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.smsTemplates {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: ChannelTypeSMS,
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.voiceTemplates {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: ChannelTypeVoice,
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

	if req.SegmentVersion != 0 {
		c.SegmentVersion = req.SegmentVersion
	}

	if len(req.MessageConfiguration) > 0 {
		c.MessageConfiguration = cloneAnyMap(req.MessageConfiguration)
	}

	if len(req.Schedule) > 0 {
		c.Schedule = cloneAnyMap(req.Schedule)
	}

	if len(req.Hook) > 0 {
		c.Hook = cloneAnyMap(req.Hook)
	}

	if len(req.Limits) > 0 {
		c.Limits = cloneAnyMap(req.Limits)
	}

	if len(req.TemplateConfiguration) > 0 {
		c.TemplateConfiguration = cloneAnyMap(req.TemplateConfiguration)
	}

	if len(req.CustomDeliveryConfiguration) > 0 {
		c.CustomDeliveryConfiguration = cloneAnyMap(req.CustomDeliveryConfiguration)
	}

	if req.TreatmentDescription != "" {
		c.TreatmentDescription = req.TreatmentDescription
	}

	if req.TreatmentName != "" {
		c.TreatmentName = req.TreatmentName
	}

	if req.Priority != 0 {
		c.Priority = req.Priority
	}

	c.IsPaused = req.IsPaused

	if req.IsPaused {
		c.Status = campaignStatusPaused
	} else if c.Status == campaignStatusPaused {
		c.Status = campaignStatusScheduled
	}

	if req.AdditionalTreatments != nil {
		c.AdditionalTreatments = make([]map[string]any, len(req.AdditionalTreatments))
		for i, t := range req.AdditionalTreatments {
			c.AdditionalTreatments[i] = cloneAnyMap(t)
		}
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

	if len(req.Dimensions) > 0 {
		s.Dimensions = cloneAnyMap(req.Dimensions)
	}

	if len(req.SegmentGroups) > 0 {
		s.SegmentGroups = cloneAnyMap(req.SegmentGroups)
	}

	if len(req.ImportDefinition) > 0 {
		s.ImportDefinition = cloneAnyMap(req.ImportDefinition)
		s.SegmentType = segmentTypeImport
	}

	s.LastModifiedDate = nowRFC3339()
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

	if j.State == journeyStateActive {
		return nil, ErrJourneyActive
	}

	if req.Name != "" {
		j.Name = req.Name
	}

	if req.StartActivity != "" {
		j.StartActivity = req.StartActivity
	}

	if req.RefreshFrequency != "" {
		j.RefreshFrequency = req.RefreshFrequency
	}

	if len(req.Activities) > 0 {
		j.Activities = make(map[string]map[string]any, len(req.Activities))
		for k, v := range req.Activities {
			j.Activities[k] = cloneAnyMap(v)
		}
	}

	if len(req.StartCondition) > 0 {
		j.StartCondition = cloneAnyMap(req.StartCondition)
	}

	if len(req.Schedule) > 0 {
		j.Schedule = cloneAnyMap(req.Schedule)
	}

	if len(req.Limits) > 0 {
		j.Limits = cloneAnyMap(req.Limits)
	}

	if len(req.QuietTime) > 0 {
		j.QuietTime = cloneAnyMap(req.QuietTime)
	}

	if len(req.OpenHours) > 0 {
		j.OpenHours = cloneAnyMap(req.OpenHours)
	}

	if len(req.ClosedDays) > 0 {
		j.ClosedDays = cloneAnyMap(req.ClosedDays)
	}

	j.LocalTime = req.LocalTime
	j.WaitForQuietTime = req.WaitForQuietTime
	j.RefreshOnSegmentUpdate = req.RefreshOnSegmentUpdate
	j.LastModifiedDate = nowRFC3339()

	return cloneJourney(j), nil
}

// journeyStateTransitions maps each state to the set of valid target states.
var journeyStateTransitions = map[string][]string{
	journeyStateDraft:     {journeyStateActive, journeyStateCancelled},
	journeyStateActive:    {journeyStatePaused, journeyStateCancelled, journeyStateCompleted},
	journeyStatePaused:    {journeyStateActive, journeyStateCancelled},
	journeyStateCancelled: {},
	journeyStateCompleted: {},
	journeyStateClosed:    {},
}

// UpdateJourneyState updates the state of a Pinpoint journey.
func (b *InMemoryBackend) UpdateJourneyState(appID, journeyID, state string) (*Journey, error) {
	b.mu.Lock("UpdateJourneyState")
	defer b.mu.Unlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	allowed, exists := journeyStateTransitions[j.State]
	if !exists {
		return nil, ErrValidation
	}

	validTarget := false

	for _, s := range allowed {
		if s == state {
			validTarget = true

			break
		}
	}

	if !validTarget {
		return nil, ErrValidation
	}

	j.State = state
	j.LastModifiedDate = nowRFC3339()

	if state == journeyStateActive {
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

	versionKey := templateName + "/EMAIL"
	nextVersion := strconv.Itoa(len(b.templateVersionHistory[versionKey]) + 1)
	b.templateVersionHistory[versionKey] = append(b.templateVersionHistory[versionKey], templateVersionItem{
		TemplateName:    templateName,
		TemplateType:    ChannelTypeEmail,
		TemplateVersion: nextVersion,
	})

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

	versionKey := templateName + "/INAPP"
	nextVersion := strconv.Itoa(len(b.templateVersionHistory[versionKey]) + 1)
	b.templateVersionHistory[versionKey] = append(b.templateVersionHistory[versionKey], templateVersionItem{
		TemplateName:    templateName,
		TemplateType:    templateTypeINAPP,
		TemplateVersion: nextVersion,
	})

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

	versionKey := templateName + "/PUSH"
	nextVersion := strconv.Itoa(len(b.templateVersionHistory[versionKey]) + 1)
	b.templateVersionHistory[versionKey] = append(b.templateVersionHistory[versionKey], templateVersionItem{
		TemplateName:    templateName,
		TemplateType:    templateTypePUSH,
		TemplateVersion: nextVersion,
	})

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

	versionKey := templateName + "/SMS"
	nextVersion := strconv.Itoa(len(b.templateVersionHistory[versionKey]) + 1)
	b.templateVersionHistory[versionKey] = append(b.templateVersionHistory[versionKey], templateVersionItem{
		TemplateName:    templateName,
		TemplateType:    ChannelTypeSMS,
		TemplateVersion: nextVersion,
	})

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

	return cloneEndpoint(e), nil
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

	if len(req.User.UserAttributes) > 0 {
		e.UserAttributes = nonNilStringSliceMapCopy(req.User.UserAttributes)
	}

	if len(req.Attributes) > 0 {
		e.Attributes = nonNilStringSliceMapCopy(req.Attributes)
	}

	if len(req.Metrics) > 0 {
		e.Metrics = nonNilFloat64MapCopy(req.Metrics)
	}

	if len(req.Demographic) > 0 {
		e.Demographic = cloneAnyMap(req.Demographic)
	}

	if len(req.Location) > 0 {
		e.Location = cloneAnyMap(req.Location)
	}

	if req.EndpointStatus != "" {
		e.EndpointStatus = req.EndpointStatus
	}

	if req.OptOut != "" {
		e.OptOut = req.OptOut
	}

	if req.EffectiveDate != "" {
		e.EffectiveDate = req.EffectiveDate
	}

	if req.RequestId != "" {
		e.RequestId = req.RequestId
	}

	return cloneEndpoint(e), nil
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

	return cloneEndpoint(e), nil
}

// GetUserEndpoints retrieves all endpoints for a user in an application.
func (b *InMemoryBackend) GetUserEndpoints(appID, userID string) ([]*Endpoint, error) {
	b.mu.RLock("GetUserEndpoints")
	defer b.mu.RUnlock()

	var endpoints []*Endpoint

	for _, e := range b.endpoints {
		if e.ApplicationID == appID && e.UserID == userID {
			endpoints = append(endpoints, cloneEndpoint(e))
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

		if req.User.UserID != "" {
			e.UserID = req.User.UserID
		}

		if len(req.Attributes) > 0 {
			e.Attributes = nonNilStringSliceMapCopy(req.Attributes)
		}

		if len(req.Metrics) > 0 {
			e.Metrics = nonNilFloat64MapCopy(req.Metrics)
		}

		if len(req.Demographic) > 0 {
			e.Demographic = cloneAnyMap(req.Demographic)
		}

		if len(req.Location) > 0 {
			e.Location = cloneAnyMap(req.Location)
		}

		if req.EndpointStatus != "" {
			e.EndpointStatus = req.EndpointStatus
		}

		if req.OptOut != "" {
			e.OptOut = req.OptOut
		}
	}

	return nil
}

// cloneEndpoint returns a deep copy of an Endpoint.
func cloneEndpoint(e *Endpoint) *Endpoint {
	cp := *e
	cp.Attributes = nonNilStringSliceMapCopy(e.Attributes)
	cp.UserAttributes = nonNilStringSliceMapCopy(e.UserAttributes)
	cp.Metrics = nonNilFloat64MapCopy(e.Metrics)
	cp.Demographic = cloneAnyMap(e.Demographic)
	cp.Location = cloneAnyMap(e.Location)

	return &cp
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
		cp.ExtraData = cloneAnyMap(ch.ExtraData)

		return &cp
	}

	return &Channel{
		ApplicationID: appID,
		ChannelType:   channelType,
		Platform:      strings.ToUpper(channelType),
		Enabled:       false,
		IsArchived:    false,
	}
}

// UpsertChannel creates or updates a channel for an app with type-specific data.
func (b *InMemoryBackend) UpsertChannel(appID, channelType string, enabled bool, extra map[string]any) *Channel {
	b.mu.Lock("UpsertChannel")
	defer b.mu.Unlock()

	key := appID + "/" + channelType

	existing := b.channels[key]

	now := nowRFC3339()
	version := 1

	if existing != nil {
		version = existing.Version + 1
	}

	platform := strings.ToUpper(channelType)

	hasCredential := false
	hasTokenKey := false

	if extra != nil {
		if v, ok := extra["ApiKey"].(string); ok && v != "" {
			hasCredential = true
		}

		if v, ok := extra["BundleId"].(string); ok && v != "" {
			hasCredential = true
		}

		if v, ok := extra["Certificate"].(string); ok && v != "" {
			hasCredential = true
		}

		if v, ok := extra["ClientId"].(string); ok && v != "" {
			hasCredential = true
		}

		if v, ok := extra["FromAddress"].(string); ok && v != "" {
			hasCredential = true
		}

		if v, ok := extra["TokenKey"].(string); ok && v != "" {
			hasTokenKey = true
		}
	}

	creationDate := now

	if existing != nil && existing.CreationDate != "" {
		creationDate = existing.CreationDate
	}

	ch := &Channel{
		ApplicationID:    appID,
		ChannelType:      channelType,
		Platform:         platform,
		Enabled:          enabled,
		HasCredential:    hasCredential,
		HasTokenKey:      hasTokenKey,
		Version:          version,
		CreationDate:     creationDate,
		LastModifiedDate: now,
		ExtraData:        cloneAnyMap(extra),
	}

	b.channels[key] = ch

	cp := *ch
	cp.ExtraData = cloneAnyMap(ch.ExtraData)

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
	cp.ExtraData = cloneAnyMap(ch.ExtraData)

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
			cp.ExtraData = cloneAnyMap(ch.ExtraData)
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

// validRecommenderIDTypes are the accepted values for RecommendationProviderIdType.
var validRecommenderIDTypes = map[string]bool{
	"PINPOINT_ENDPOINT_ID": true,
	"PINPOINT_USER_ID":     true,
	"":                     true,
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

	if req.RecommendationProviderIDType != "" && !validRecommenderIDTypes[req.RecommendationProviderIDType] {
		return nil, ErrValidation
	}

	changed := false

	if req.Name != "" && req.Name != r.Name {
		r.Name = req.Name
		changed = true
	}

	if req.Description != "" && req.Description != r.Description {
		r.Description = req.Description
		changed = true
	}

	if req.RecommendationProviderIDType != "" && req.RecommendationProviderIDType != r.RecommendationProviderIDType {
		r.RecommendationProviderIDType = req.RecommendationProviderIDType
		changed = true
	}

	if req.RecommendationProviderRoleArn != "" && req.RecommendationProviderRoleArn != r.RecommendationProviderRoleARN {
		r.RecommendationProviderRoleARN = req.RecommendationProviderRoleArn
		changed = true
	}

	if req.RecommendationProviderURI != "" && req.RecommendationProviderURI != r.RecommendationProviderURI {
		r.RecommendationProviderURI = req.RecommendationProviderURI
		changed = true
	}

	if req.RecommendationsPerMessage != 0 && req.RecommendationsPerMessage != r.RecommendationsPerMessage {
		r.RecommendationsPerMessage = req.RecommendationsPerMessage
		changed = true
	}

	if len(req.Attributes) > 0 {
		newAttrs := nonNilAttrsCopy(req.Attributes)
		for k, v := range newAttrs {
			if r.Attributes[k] != v {
				changed = true

				break
			}
		}

		if changed {
			r.Attributes = newAttrs
		}
	}

	if changed {
		r.LastModifiedDate = nowRFC3339()
	}

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

// SendMessages sends messages and tracks send count.
func (b *InMemoryBackend) SendMessages(appID string, req sendMessagesRequest) (*messageResponse, error) {
	b.mu.Lock("SendMessages")
	defer b.mu.Unlock()

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
		b.sentMessages[appID]++
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

// SendOTPMessage sends an OTP message and stores the generated code.
func (b *InMemoryBackend) SendOTPMessage(appID string) (*sendOTPMessageResponse, error) {
	b.mu.Lock("SendOTPMessage")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	// Generate and store a 6-digit OTP code.
	//nolint:gosec // OTP codes are not cryptographically sensitive in mock
	code := fmt.Sprintf("%06d", rand.IntN(otpModulus))
	b.otpCodes[appID] = code

	msgID := uuid.NewString()

	return &sendOTPMessageResponse{
		MessageResponse: messageResponse{
			Result: map[string]messageResult{
				appID: {DeliveryStatus: "SUCCESSFUL", MessageID: msgID, StatusCode: statusCodeOK},
			},
		},
	}, nil
}

// VerifyOTPMessage verifies an OTP — valid if an OTP was previously sent for this app.
func (b *InMemoryBackend) VerifyOTPMessage(appID string) (*verifyOTPMessageResponse, error) {
	b.mu.RLock("VerifyOTPMessage")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	_, hasPendingOTP := b.otpCodes[appID]

	return &verifyOTPMessageResponse{Valid: hasPendingOTP}, nil
}

// PutEvents records events for an application.
func (b *InMemoryBackend) PutEvents(appID string, req putEventsRequest) error {
	b.mu.Lock("PutEvents")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return ErrAppNotFound
	}

	for _, epEvents := range req.EventsRequest.BatchItem {
		for _, ev := range epEvents.Events {
			b.appEvents[appID] = append(b.appEvents[appID], storedPinpointEvent(ev))
		}
	}

	return nil
}

// PhoneNumberValidate validates a phone number and returns a cleaned E164 response.
func (b *InMemoryBackend) PhoneNumberValidate(phoneNumber string) (*phoneNumberValidateResponse, error) {
	// Normalise to E164: strip non-digit chars, prepend + if missing.
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}

		return -1
	}, phoneNumber)

	var e164 string

	switch {
	case strings.HasPrefix(phoneNumber, "+"):
		e164 = "+" + digits
	case len(digits) == minPhoneLength:
		// Assume US number.
		e164 = "+1" + digits
	default:
		e164 = "+" + digits
	}

	return &phoneNumberValidateResponse{
		NumberValidateResponse: numberValidateResponse{
			Carrier:                 "Unknown",
			PhoneType:               "MOBILE",
			PhoneTypeCode:           0,
			CleansedPhoneNumberE164: e164,
		},
	}, nil
}

// RemoveAttributes removes attributes from endpoints and returns the updated attributesResource.
func (b *InMemoryBackend) RemoveAttributes(appID, attributeType string) (*attributesResource, error) {
	b.mu.Lock("RemoveAttributes")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	// Remove the attribute from all endpoints in this app.
	for key, e := range b.endpoints {
		if e.ApplicationID == appID {
			if e.Attributes != nil {
				delete(e.Attributes, attributeType)
				b.endpoints[key] = e
			}
		}
	}

	return &attributesResource{
		ApplicationID: appID,
		AttributeType: attributeType,
		Attributes:    []string{},
	}, nil
}

// GetInAppMessages returns in-app messages for an endpoint.
func (b *InMemoryBackend) GetInAppMessages(appID, _ string) (*inAppMessagesResponse, error) {
	b.mu.RLock("GetInAppMessages")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	// Collect in-app templates as message campaigns for this app.
	var campaigns []inAppMessageCampaign

	for _, t := range b.inAppTemplates {
		campaigns = append(campaigns, inAppMessageCampaign{CampaignID: t.TemplateName})
	}

	if campaigns == nil {
		campaigns = []inAppMessageCampaign{}
	}

	return &inAppMessagesResponse{
		InAppMessageCampaigns: campaigns,
	}, nil
}

// GetCampaignActivities returns campaign activities.
func (b *InMemoryBackend) GetCampaignActivities(appID, campaignID string) (*campaignActivitiesResponse, error) {
	b.mu.RLock("GetCampaignActivities")
	defer b.mu.RUnlock()

	c, ok := b.campaigns[campaignID]
	if !ok || c.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	actKey := appID + "/" + campaignID
	activities := b.campaignActivities[actKey]

	if activities == nil {
		activities = []campaignActivity{}
	}

	return &campaignActivitiesResponse{Item: activities}, nil
}

// GetJourneyExecutionMetrics returns journey execution metrics.
func (b *InMemoryBackend) GetJourneyExecutionMetrics(
	appID, journeyID string,
) (*journeyExecutionMetricsResponse, error) {
	b.mu.RLock("GetJourneyExecutionMetrics")
	defer b.mu.RUnlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	runKey := appID + "/" + journeyID
	runs := b.journeyRuns[runKey]

	return &journeyExecutionMetricsResponse{
		ApplicationID: appID,
		JourneyID:     journeyID,
		Metrics: map[string]string{
			"TotalRuns": strconv.Itoa(len(runs)),
		},
	}, nil
}

// GetJourneyExecutionActivityMetrics returns journey activity metrics.
func (b *InMemoryBackend) GetJourneyExecutionActivityMetrics(
	appID, journeyID, activityID string,
) (*journeyExecutionActivityMetricsResponse, error) {
	b.mu.RLock("GetJourneyExecutionActivityMetrics")
	defer b.mu.RUnlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	runKey := appID + "/" + journeyID
	runs := b.journeyRuns[runKey]

	return &journeyExecutionActivityMetricsResponse{
		ApplicationID: appID,
		JourneyID:     journeyID,
		ActivityID:    activityID,
		Metrics: map[string]string{
			"TotalRuns":  strconv.Itoa(len(runs)),
			"ActivityId": activityID,
		},
	}, nil
}

// GetJourneyRuns returns journey runs.
func (b *InMemoryBackend) GetJourneyRuns(appID, journeyID string) (*journeyRunsResponse, error) {
	b.mu.RLock("GetJourneyRuns")
	defer b.mu.RUnlock()

	j, ok := b.journeys[journeyID]
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	runKey := appID + "/" + journeyID
	storedRuns := b.journeyRuns[runKey]

	runs := make([]journeyRun, 0, len(storedRuns))
	for _, r := range storedRuns {
		runs = append(runs, *r)
	}

	return &journeyRunsResponse{Item: runs}, nil
}

// GetJourneyRunExecutionMetrics returns metrics for a specific journey run.
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
		Metrics: map[string]string{
			"RunId": runID,
		},
	}, nil
}

// GetJourneyRunExecutionActivityMetrics returns metrics for a specific journey run activity.
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
		Metrics: map[string]string{
			"RunId":      runID,
			"ActivityId": activityID,
		},
	}, nil
}

// GetCampaignVersion returns a specific campaign version.
func (b *InMemoryBackend) GetCampaignVersion(appID, campaignID string, version int) (*Campaign, error) {
	b.mu.RLock("GetCampaignVersion")
	defer b.mu.RUnlock()

	versionKey := appID + "/" + campaignID
	versions := b.campaignVersions[versionKey]

	for _, v := range versions {
		if v.Version == version {
			return cloneCampaign(v), nil
		}
	}

	// Fall back to current campaign if version not found in history.
	c, ok := b.campaigns[campaignID]
	if !ok || c.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return cloneCampaign(c), nil
}

// GetCampaignVersions returns all stored versions of a campaign.
func (b *InMemoryBackend) GetCampaignVersions(appID, campaignID string) ([]*Campaign, error) {
	b.mu.RLock("GetCampaignVersions")
	defer b.mu.RUnlock()

	if _, ok := b.campaigns[campaignID]; !ok {
		return nil, ErrAppNotFound
	}

	versionKey := appID + "/" + campaignID
	versions := b.campaignVersions[versionKey]

	result := make([]*Campaign, len(versions))
	for i, v := range versions {
		result[i] = cloneCampaign(v)
	}

	return result, nil
}

// GetSegmentVersion returns a specific segment version.
func (b *InMemoryBackend) GetSegmentVersion(appID, segmentID string, version int) (*Segment, error) {
	b.mu.RLock("GetSegmentVersion")
	defer b.mu.RUnlock()

	versionKey := appID + "/" + segmentID
	versions := b.segmentVersions[versionKey]

	for _, v := range versions {
		if v.Version == version {
			return cloneSegment(v), nil
		}
	}

	// Fall back to current segment if version not found in history.
	s, ok := b.segments[segmentID]
	if !ok || s.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return cloneSegment(s), nil
}

// GetSegmentVersions returns all stored versions of a segment.
func (b *InMemoryBackend) GetSegmentVersions(appID, segmentID string) ([]*Segment, error) {
	b.mu.RLock("GetSegmentVersions")
	defer b.mu.RUnlock()

	if _, ok := b.segments[segmentID]; !ok {
		return nil, ErrAppNotFound
	}

	versionKey := appID + "/" + segmentID
	versions := b.segmentVersions[versionKey]

	result := make([]*Segment, len(versions))
	for i, v := range versions {
		result[i] = cloneSegment(v)
	}

	return result, nil
}

// ListTemplateVersions returns stored version history for a template.
func (b *InMemoryBackend) ListTemplateVersions(templateName, templateType string) ([]*templateVersionItem, error) {
	b.mu.RLock("ListTemplateVersions")
	defer b.mu.RUnlock()

	// Normalise the template type key to uppercase for storage lookup.
	typeUpper := strings.ToUpper(templateType)
	versionKey := templateName + "/" + typeUpper

	history := b.templateVersionHistory[versionKey]
	if len(history) == 0 {
		return nil, ErrAppNotFound
	}

	result := make([]*templateVersionItem, len(history))
	for i := range history {
		cp := history[i]
		result[i] = &cp
	}

	return result, nil
}

// UpdateTemplateActiveVersion updates the active version to the latest for the given template.
func (b *InMemoryBackend) UpdateTemplateActiveVersion(templateName, templateType string) error {
	b.mu.Lock("UpdateTemplateActiveVersion")
	defer b.mu.Unlock()

	typeUpper := strings.ToUpper(templateType)
	versionKey := templateName + "/" + typeUpper

	history := b.templateVersionHistory[versionKey]
	if len(history) == 0 {
		return nil
	}

	// Mark the latest version as active (stored in version history last entry).
	// No-op needed: the last entry in history IS the active version.
	// This method exists for API compatibility.
	_ = history[len(history)-1]

	return nil
}

// ──────────────────────────────────────────────────
// Application Settings backend methods
// ──────────────────────────────────────────────────

// GetApplicationSettings retrieves the settings for a Pinpoint application.
func (b *InMemoryBackend) GetApplicationSettings(appID string) (*storedAppSettings, error) {
	b.mu.RLock("GetApplicationSettings")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	settings, ok := b.appSettings[appID]
	if !ok {
		// Return defaults when no settings have been stored yet.
		return &storedAppSettings{
			CampaignHook: map[string]any{},
			Limits:       map[string]any{},
			QuietTime:    map[string]any{},
		}, nil
	}

	cp := *settings
	cp.CampaignHook = cloneAnyMap(settings.CampaignHook)
	cp.Limits = cloneAnyMap(settings.Limits)
	cp.QuietTime = cloneAnyMap(settings.QuietTime)

	return &cp, nil
}

// UpdateApplicationSettings updates the settings for a Pinpoint application.
func (b *InMemoryBackend) UpdateApplicationSettings(
	appID string,
	settings *storedAppSettings,
) (*storedAppSettings, error) {
	b.mu.Lock("UpdateApplicationSettings")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, ErrAppNotFound
	}

	stored := &storedAppSettings{
		CampaignHook:      cloneAnyMap(settings.CampaignHook),
		Limits:            cloneAnyMap(settings.Limits),
		QuietTime:         cloneAnyMap(settings.QuietTime),
		CloudWatchMetrics: settings.CloudWatchMetrics,
	}

	b.appSettings[appID] = stored

	cp := *stored
	cp.CampaignHook = cloneAnyMap(stored.CampaignHook)
	cp.Limits = cloneAnyMap(stored.Limits)
	cp.QuietTime = cloneAnyMap(stored.QuietTime)

	return &cp, nil
}

// cloneAnyMap returns a shallow copy of a map[string]any; never returns nil.
func cloneAnyMap(m map[string]any) map[string]any {
	cp := make(map[string]any, len(m))
	maps.Copy(cp, m)

	return cp
}

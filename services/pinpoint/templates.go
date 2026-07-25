package pinpoint

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	templateTypeINAPP = "INAPP"
	templateTypePUSH  = "PUSH"

	// maxTemplateVersions caps the number of stored template versions per
	// template so Update* calls cannot grow templateVersionHistory without bound.
	maxTemplateVersions = 100
)

// CreateEmailTemplate creates a new Pinpoint email template.
func (b *InMemoryBackend) CreateEmailTemplate(
	region, accountID, templateName string,
	req createEmailTemplateRequest,
) (*EmailTemplate, error) {
	b.mu.Lock("CreateEmailTemplate")
	defer b.mu.Unlock()

	if _, exists := b.emailTemplates.Get(templateName); exists {
		return nil, ErrAlreadyExists
	}

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/EMAIL", templateName))

	now := nowRFC3339()
	t := &EmailTemplate{
		ARN:                  templateARN,
		CreationDate:         now,
		DefaultSubstitutions: req.DefaultSubstitutions,
		HTMLPart:             req.HTMLPart,
		LastModifiedDate:     now,
		RecommenderID:        req.RecommenderID,
		Subject:              req.Subject,
		Tags:                 nonNilTagsCopy(req.Tags),
		TemplateDescription:  req.TemplateDescription,
		TemplateName:         templateName,
		TemplateType:         ChannelTypeEmail,
		TextPart:             req.TextPart,
		Version:              "1",
	}

	b.emailTemplates.Put(t)
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

	if _, exists := b.inAppTemplates.Get(templateName); exists {
		return nil, ErrAlreadyExists
	}

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/INAPP", templateName))

	now := nowRFC3339()
	t := &InAppTemplate{
		ARN:                 templateARN,
		Content:             cloneContentSlice(req.Content),
		CreationDate:        now,
		CustomConfig:        nonNilTagsCopy(req.CustomConfig),
		LastModifiedDate:    now,
		Layout:              req.Layout,
		Tags:                nonNilTagsCopy(req.Tags),
		TemplateDescription: req.TemplateDescription,
		TemplateName:        templateName,
		TemplateType:        templateTypeINAPP,
		Version:             "1",
	}

	b.inAppTemplates.Put(t)
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

	if _, exists := b.pushTemplates.Get(templateName); exists {
		return nil, ErrAlreadyExists
	}

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/PUSH", templateName))

	now := nowRFC3339()
	t := &PushTemplate{
		ADM:                  cloneAnyMap(req.ADM),
		APNS:                 cloneAnyMap(req.APNS),
		ARN:                  templateARN,
		Baidu:                cloneAnyMap(req.Baidu),
		CreationDate:         now,
		Default:              cloneAnyMap(req.Default),
		DefaultSubstitutions: req.DefaultSubstitutions,
		GCM:                  cloneAnyMap(req.GCM),
		LastModifiedDate:     now,
		RecommenderID:        req.RecommenderID,
		Tags:                 nonNilTagsCopy(req.Tags),
		TemplateDescription:  req.TemplateDescription,
		TemplateName:         templateName,
		TemplateType:         templateTypePUSH,
		Version:              "1",
	}

	b.pushTemplates.Put(t)
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

	if _, exists := b.smsTemplates.Get(templateName); exists {
		return nil, ErrAlreadyExists
	}

	templateARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("templates/%s/SMS", templateName))

	now := nowRFC3339()
	t := &SmsTemplate{
		ARN:                  templateARN,
		Body:                 req.Body,
		CreationDate:         now,
		DefaultSubstitutions: req.DefaultSubstitutions,
		LastModifiedDate:     now,
		RecommenderID:        req.RecommenderID,
		Tags:                 nonNilTagsCopy(req.Tags),
		TemplateDescription:  req.TemplateDescription,
		TemplateName:         templateName,
		TemplateType:         ChannelTypeSMS,
		Version:              "1",
	}

	b.smsTemplates.Put(t)
	b.arnIndex[templateARN] = t

	// Track template version history.
	versionKey := templateName + "/SMS"
	b.templateVersionHistory[versionKey] = []templateVersionItem{
		{TemplateName: templateName, TemplateType: ChannelTypeSMS, TemplateVersion: "1"},
	}

	return cloneSmsTemplate(t), nil
}

func cloneEmailTemplate(t *EmailTemplate) *EmailTemplate {
	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)

	return &cp
}

func cloneInAppTemplate(t *InAppTemplate) *InAppTemplate {
	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)
	cp.CustomConfig = nonNilTagsCopy(t.CustomConfig)
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
	cp.ADM = cloneAnyMap(t.ADM)
	cp.Baidu = cloneAnyMap(t.Baidu)

	return &cp
}

func cloneSmsTemplate(t *SmsTemplate) *SmsTemplate {
	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)

	return &cp
}

// VoiceTemplate represents a Pinpoint voice template.
type VoiceTemplate struct {
	Tags                 map[string]string `json:"tags,omitempty"`
	ARN                  string            `json:"Arn,omitempty"`
	TemplateName         string            `json:"TemplateName"`
	TemplateType         string            `json:"TemplateType"`
	Body                 string            `json:"Body,omitempty"`
	CreationDate         string            `json:"CreationDate,omitempty"`
	DefaultSubstitutions string            `json:"DefaultSubstitutions,omitempty"`
	LanguageCode         string            `json:"LanguageCode,omitempty"`
	LastModifiedDate     string            `json:"LastModifiedDate,omitempty"`
	TemplateDescription  string            `json:"TemplateDescription,omitempty"`
	Version              string            `json:"Version,omitempty"`
	VoiceID              string            `json:"VoiceId,omitempty"`
}

// CreateVoiceTemplate creates a new Pinpoint voice template.
func (b *InMemoryBackend) CreateVoiceTemplate(
	region, accountID, templateName string,
	req createVoiceTemplateRequest,
) (*VoiceTemplate, error) {
	b.mu.Lock("CreateVoiceTemplate")
	defer b.mu.Unlock()

	if _, exists := b.voiceTemplates.Get(templateName); exists {
		return nil, ErrAlreadyExists
	}

	templateARN := arn.Build(
		"mobiletargeting",
		region,
		accountID,
		fmt.Sprintf("templates/%s/VOICE", templateName),
	)

	now := nowRFC3339()
	t := &VoiceTemplate{
		ARN:                  templateARN,
		TemplateName:         templateName,
		TemplateType:         ChannelTypeVoice,
		Body:                 req.Body,
		DefaultSubstitutions: req.DefaultSubstitutions,
		LanguageCode:         req.LanguageCode,
		TemplateDescription:  req.TemplateDescription,
		VoiceID:              req.VoiceID,
		Tags:                 nonNilTagsCopy(req.Tags),
		CreationDate:         now,
		LastModifiedDate:     now,
		Version:              "1",
	}

	b.voiceTemplates.Put(t)
	b.arnIndex[templateARN] = t

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

	t, ok := b.voiceTemplates.Get(templateName)
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

	t, ok := b.voiceTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	if req.Body != "" {
		t.Body = req.Body
	}

	if req.DefaultSubstitutions != "" {
		t.DefaultSubstitutions = req.DefaultSubstitutions
	}

	if req.LanguageCode != "" {
		t.LanguageCode = req.LanguageCode
	}

	if req.TemplateDescription != "" {
		t.TemplateDescription = req.TemplateDescription
	}

	if req.VoiceID != "" {
		t.VoiceID = req.VoiceID
	}

	if req.Tags != nil {
		t.Tags = nonNilTagsCopy(req.Tags)
	}

	versionKey := templateName + "/VOICE"
	nextVersion := strconv.Itoa(len(b.templateVersionHistory[versionKey]) + 1)
	b.templateVersionHistory[versionKey] = append(
		b.templateVersionHistory[versionKey],
		templateVersionItem{
			TemplateName:    templateName,
			TemplateType:    ChannelTypeVoice,
			TemplateVersion: nextVersion,
		},
	)

	if h := b.templateVersionHistory[versionKey]; len(h) > maxTemplateVersions {
		b.templateVersionHistory[versionKey] = h[len(h)-maxTemplateVersions:]
	}

	t.LastModifiedDate = nowRFC3339()
	t.Version = nextVersion

	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)

	return &cp, nil
}

// DeleteVoiceTemplate deletes a Pinpoint voice template by name.
func (b *InMemoryBackend) DeleteVoiceTemplate(templateName string) (*VoiceTemplate, error) {
	b.mu.Lock("DeleteVoiceTemplate")
	defer b.mu.Unlock()

	t, ok := b.voiceTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	b.voiceTemplates.Delete(templateName)
	delete(b.arnIndex, t.ARN)
	delete(b.templateVersionHistory, templateName+"/"+ChannelTypeVoice)

	cp := *t
	cp.Tags = nonNilTagsCopy(t.Tags)

	return &cp, nil
}

// ListTemplates returns all templates sorted by name.
func (b *InMemoryBackend) ListTemplates() ([]*templateListItem, error) {
	b.mu.RLock("ListTemplates")
	defer b.mu.RUnlock()

	totalCap := b.emailTemplates.Len() + b.inAppTemplates.Len() + b.pushTemplates.Len() +
		b.smsTemplates.Len() + b.voiceTemplates.Len()
	items := make([]*templateListItem, 0, totalCap)

	for _, t := range b.emailTemplates.All() {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: ChannelTypeEmail,
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.inAppTemplates.All() {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: templateTypeINAPP,
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.pushTemplates.All() {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: templateTypePUSH,
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.smsTemplates.All() {
		items = append(items, &templateListItem{
			TemplateName: t.TemplateName,
			TemplateType: ChannelTypeSMS,
			ARN:          t.ARN,
			CreationDate: t.CreationDate,
		})
	}

	for _, t := range b.voiceTemplates.All() {
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

// GetEmailTemplate retrieves an email template by name.
func (b *InMemoryBackend) GetEmailTemplate(templateName string) (*EmailTemplate, error) {
	b.mu.RLock("GetEmailTemplate")
	defer b.mu.RUnlock()

	t, ok := b.emailTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneEmailTemplate(t), nil
}

// UpdateEmailTemplate updates an existing email template.
func (b *InMemoryBackend) UpdateEmailTemplate(
	templateName string,
	req createEmailTemplateRequest,
) (*EmailTemplate, error) {
	b.mu.Lock("UpdateEmailTemplate")
	defer b.mu.Unlock()

	t, ok := b.emailTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	versionKey := templateName + "/EMAIL"
	nextVersion := strconv.Itoa(len(b.templateVersionHistory[versionKey]) + 1)
	b.templateVersionHistory[versionKey] = append(
		b.templateVersionHistory[versionKey],
		templateVersionItem{
			TemplateName:    templateName,
			TemplateType:    ChannelTypeEmail,
			TemplateVersion: nextVersion,
		},
	)

	if h := b.templateVersionHistory[versionKey]; len(h) > maxTemplateVersions {
		b.templateVersionHistory[versionKey] = h[len(h)-maxTemplateVersions:]
	}

	if req.Subject != "" {
		t.Subject = req.Subject
	}

	if req.HTMLPart != "" {
		t.HTMLPart = req.HTMLPart
	}

	if req.TextPart != "" {
		t.TextPart = req.TextPart
	}

	if req.TemplateDescription != "" {
		t.TemplateDescription = req.TemplateDescription
	}

	if req.RecommenderID != "" {
		t.RecommenderID = req.RecommenderID
	}

	if req.DefaultSubstitutions != "" {
		t.DefaultSubstitutions = req.DefaultSubstitutions
	}

	t.LastModifiedDate = nowRFC3339()
	t.Version = nextVersion

	return cloneEmailTemplate(t), nil
}

// DeleteEmailTemplate deletes an email template by name.
func (b *InMemoryBackend) DeleteEmailTemplate(templateName string) (*EmailTemplate, error) {
	b.mu.Lock("DeleteEmailTemplate")
	defer b.mu.Unlock()

	t, ok := b.emailTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	b.emailTemplates.Delete(templateName)
	delete(b.arnIndex, t.ARN)
	delete(b.templateVersionHistory, templateName+"/"+ChannelTypeEmail)

	return cloneEmailTemplate(t), nil
}

// GetInAppTemplate retrieves an in-app template by name.
func (b *InMemoryBackend) GetInAppTemplate(templateName string) (*InAppTemplate, error) {
	b.mu.RLock("GetInAppTemplate")
	defer b.mu.RUnlock()

	t, ok := b.inAppTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneInAppTemplate(t), nil
}

// UpdateInAppTemplate updates an existing in-app template.
func (b *InMemoryBackend) UpdateInAppTemplate(
	templateName string,
	req createInAppTemplateRequest,
) (*InAppTemplate, error) {
	b.mu.Lock("UpdateInAppTemplate")
	defer b.mu.Unlock()

	t, ok := b.inAppTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	versionKey := templateName + "/INAPP"
	nextVersion := strconv.Itoa(len(b.templateVersionHistory[versionKey]) + 1)
	b.templateVersionHistory[versionKey] = append(
		b.templateVersionHistory[versionKey],
		templateVersionItem{
			TemplateName:    templateName,
			TemplateType:    templateTypeINAPP,
			TemplateVersion: nextVersion,
		},
	)

	if h := b.templateVersionHistory[versionKey]; len(h) > maxTemplateVersions {
		b.templateVersionHistory[versionKey] = h[len(h)-maxTemplateVersions:]
	}

	if len(req.Content) > 0 {
		t.Content = cloneContentSlice(req.Content)
	}

	if req.Layout != "" {
		t.Layout = req.Layout
	}

	if req.TemplateDescription != "" {
		t.TemplateDescription = req.TemplateDescription
	}

	if len(req.CustomConfig) > 0 {
		t.CustomConfig = nonNilTagsCopy(req.CustomConfig)
	}

	t.LastModifiedDate = nowRFC3339()
	t.Version = nextVersion

	return cloneInAppTemplate(t), nil
}

// DeleteInAppTemplate deletes an in-app template by name.
func (b *InMemoryBackend) DeleteInAppTemplate(templateName string) (*InAppTemplate, error) {
	b.mu.Lock("DeleteInAppTemplate")
	defer b.mu.Unlock()

	t, ok := b.inAppTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	b.inAppTemplates.Delete(templateName)
	delete(b.arnIndex, t.ARN)
	delete(b.templateVersionHistory, templateName+"/"+templateTypeINAPP)

	return cloneInAppTemplate(t), nil
}

// GetPushTemplate retrieves a push notification template by name.
func (b *InMemoryBackend) GetPushTemplate(templateName string) (*PushTemplate, error) {
	b.mu.RLock("GetPushTemplate")
	defer b.mu.RUnlock()

	t, ok := b.pushTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	return clonePushTemplate(t), nil
}

// UpdatePushTemplate updates an existing push notification template.
func (b *InMemoryBackend) UpdatePushTemplate(
	templateName string,
	req createPushTemplateRequest,
) (*PushTemplate, error) {
	b.mu.Lock("UpdatePushTemplate")
	defer b.mu.Unlock()

	t, ok := b.pushTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	versionKey := templateName + "/PUSH"
	nextVersion := strconv.Itoa(len(b.templateVersionHistory[versionKey]) + 1)
	b.templateVersionHistory[versionKey] = append(
		b.templateVersionHistory[versionKey],
		templateVersionItem{
			TemplateName:    templateName,
			TemplateType:    templateTypePUSH,
			TemplateVersion: nextVersion,
		},
	)

	if h := b.templateVersionHistory[versionKey]; len(h) > maxTemplateVersions {
		b.templateVersionHistory[versionKey] = h[len(h)-maxTemplateVersions:]
	}

	applyPushTemplateUpdate(t, req)

	t.LastModifiedDate = nowRFC3339()
	t.Version = nextVersion

	return clonePushTemplate(t), nil
}

// applyPushTemplateUpdate copies every present field from req onto t. Split
// out of UpdatePushTemplate to keep that function's cyclomatic complexity
// down now that the push template surface covers five platform-override
// objects plus DefaultSubstitutions/RecommenderId.
func applyPushTemplateUpdate(t *PushTemplate, req createPushTemplateRequest) {
	if req.TemplateDescription != "" {
		t.TemplateDescription = req.TemplateDescription
	}

	if req.DefaultSubstitutions != "" {
		t.DefaultSubstitutions = req.DefaultSubstitutions
	}

	if req.RecommenderID != "" {
		t.RecommenderID = req.RecommenderID
	}

	if len(req.Default) > 0 {
		t.Default = cloneAnyMap(req.Default)
	}

	if len(req.GCM) > 0 {
		t.GCM = cloneAnyMap(req.GCM)
	}

	if len(req.APNS) > 0 {
		t.APNS = cloneAnyMap(req.APNS)
	}

	if len(req.ADM) > 0 {
		t.ADM = cloneAnyMap(req.ADM)
	}

	if len(req.Baidu) > 0 {
		t.Baidu = cloneAnyMap(req.Baidu)
	}
}

// DeletePushTemplate deletes a push notification template by name.
func (b *InMemoryBackend) DeletePushTemplate(templateName string) (*PushTemplate, error) {
	b.mu.Lock("DeletePushTemplate")
	defer b.mu.Unlock()

	t, ok := b.pushTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	b.pushTemplates.Delete(templateName)
	delete(b.arnIndex, t.ARN)
	delete(b.templateVersionHistory, templateName+"/"+templateTypePUSH)

	return clonePushTemplate(t), nil
}

// GetSmsTemplate retrieves an SMS template by name.
func (b *InMemoryBackend) GetSmsTemplate(templateName string) (*SmsTemplate, error) {
	b.mu.RLock("GetSmsTemplate")
	defer b.mu.RUnlock()

	t, ok := b.smsTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneSmsTemplate(t), nil
}

// UpdateSmsTemplate updates an existing SMS template.
func (b *InMemoryBackend) UpdateSmsTemplate(
	templateName string,
	req createSmsTemplateRequest,
) (*SmsTemplate, error) {
	b.mu.Lock("UpdateSmsTemplate")
	defer b.mu.Unlock()

	t, ok := b.smsTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	versionKey := templateName + "/SMS"
	nextVersion := strconv.Itoa(len(b.templateVersionHistory[versionKey]) + 1)
	b.templateVersionHistory[versionKey] = append(
		b.templateVersionHistory[versionKey],
		templateVersionItem{
			TemplateName:    templateName,
			TemplateType:    ChannelTypeSMS,
			TemplateVersion: nextVersion,
		},
	)

	if h := b.templateVersionHistory[versionKey]; len(h) > maxTemplateVersions {
		b.templateVersionHistory[versionKey] = h[len(h)-maxTemplateVersions:]
	}

	if req.Body != "" {
		t.Body = req.Body
	}

	if req.DefaultSubstitutions != "" {
		t.DefaultSubstitutions = req.DefaultSubstitutions
	}

	if req.RecommenderID != "" {
		t.RecommenderID = req.RecommenderID
	}

	if req.TemplateDescription != "" {
		t.TemplateDescription = req.TemplateDescription
	}

	t.LastModifiedDate = nowRFC3339()
	t.Version = nextVersion

	return cloneSmsTemplate(t), nil
}

// DeleteSmsTemplate deletes an SMS template by name.
func (b *InMemoryBackend) DeleteSmsTemplate(templateName string) (*SmsTemplate, error) {
	b.mu.Lock("DeleteSmsTemplate")
	defer b.mu.Unlock()

	t, ok := b.smsTemplates.Get(templateName)
	if !ok {
		return nil, ErrAppNotFound
	}

	b.smsTemplates.Delete(templateName)
	delete(b.arnIndex, t.ARN)
	delete(b.templateVersionHistory, templateName+"/"+ChannelTypeSMS)

	return cloneSmsTemplate(t), nil
}

// ListTemplateVersions returns stored version history for a template.
func (b *InMemoryBackend) ListTemplateVersions(
	templateName, templateType string,
) ([]*templateVersionItem, error) {
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

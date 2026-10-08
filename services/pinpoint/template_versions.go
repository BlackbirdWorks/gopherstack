package pinpoint

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const latestTemplateVersion = "latest"

type templateVersionMeta struct {
	CreationDate         string `json:"CreationDate"`
	LastModifiedDate     string `json:"LastModifiedDate"`
	DefaultSubstitutions string `json:"DefaultSubstitutions"`
	TemplateDescription  string `json:"TemplateDescription"`
}

func templateVersionKey(name, templateType string) string {
	return name + "/" + strings.ToUpper(templateType)
}

// nextTemplateVersionLocked validates an update's version selector and returns the
// version it lands on, snapshotting the outgoing latest version when a new one is cut.
func (b *InMemoryBackend) nextTemplateVersionLocked(
	templateName, templateType, requested string,
	createNewVersion bool,
	live any,
) (string, error) {
	key := templateVersionKey(templateName, templateType)
	history := b.templateVersionHistory[key]
	current := latestVersionOf(history)

	if requested != "" {
		if createNewVersion {
			return "", fmt.Errorf("%w: version cannot be combined with create-new-version", ErrValidation)
		}

		if !hasTemplateVersion(history, requested) {
			return "", fmt.Errorf("%w: template version %s", ErrAppNotFound, requested)
		}

		if requested != current {
			return "", fmt.Errorf("%w: version %s is not the latest version (%s)", ErrValidation, requested, current)
		}
	}

	if !createNewVersion {
		return current, nil
	}

	raw, err := json.Marshal(live)
	if err != nil {
		return "", fmt.Errorf("snapshot template version: %w", err)
	}

	if b.templateVersionData[key] == nil {
		b.templateVersionData[key] = make(map[string]json.RawMessage)
	}

	b.templateVersionData[key][current] = raw

	next := strconv.Itoa(versionNumber(current) + 1)
	history = append(
		history,
		templateVersionItem{TemplateName: templateName, TemplateType: templateType, TemplateVersion: next},
	)

	for len(history) > maxTemplateVersions {
		delete(b.templateVersionData[key], history[0].TemplateVersion)
		history = history[1:]
	}

	b.templateVersionHistory[key] = history

	return next, nil
}

func versionNumber(v string) int {
	n, _ := strconv.Atoi(v)

	return n
}

func latestVersionOf(history []templateVersionItem) string {
	if len(history) == 0 {
		return "1"
	}

	return history[len(history)-1].TemplateVersion
}

func hasTemplateVersion(history []templateVersionItem, version string) bool {
	for i := range history {
		if history[i].TemplateVersion == version {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) dropTemplateVersionsLocked(key string) {
	delete(b.templateVersionHistory, key)
	delete(b.templateVersionData, key)
	delete(b.templateActiveVersion, key)
}

func (b *InMemoryBackend) activeTemplateVersionLocked(key, latest string) string {
	if v, ok := b.templateActiveVersion[key]; ok {
		return v
	}

	return latest
}

// pickTemplateVersion returns the live template or a decoded older snapshot; an empty
// requested version selects the active one.
func pickTemplateVersion[T any](
	b *InMemoryBackend,
	key string,
	live *T,
	liveVersion, requested string,
) (*T, bool, error) {
	want := requested
	if want == "" {
		want = b.activeTemplateVersionLocked(key, liveVersion)
	}

	if want == liveVersion {
		return live, true, nil
	}

	raw, ok := b.templateVersionData[key][want]
	if !ok {
		return nil, false, fmt.Errorf("%w: template version %s", ErrAppNotFound, want)
	}

	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, false, fmt.Errorf("decode template version: %w", err)
	}

	return &out, false, nil
}

// GetTemplate returns the template of the given channel at version (the active one when empty).
func (b *InMemoryBackend) GetTemplate(templateName, templateType, version string) (any, error) {
	b.mu.RLock("GetTemplate")
	defer b.mu.RUnlock()

	key := templateVersionKey(templateName, templateType)

	switch strings.ToLower(templateType) {
	case templateTypeEmail:
		return getVersionedTemplate(b, key, b.emailTemplates.Get, templateName, version,
			func(t *EmailTemplate) string { return t.Version }, cloneEmailTemplate,
			func(dst, live *EmailTemplate) { dst.Tags = nonNilTagsCopy(live.Tags) })
	case templateTypeInApp:
		return getVersionedTemplate(b, key, b.inAppTemplates.Get, templateName, version,
			func(t *InAppTemplate) string { return t.Version }, cloneInAppTemplate,
			func(dst, live *InAppTemplate) { dst.Tags = nonNilTagsCopy(live.Tags) })
	case templateTypePush:
		return getVersionedTemplate(b, key, b.pushTemplates.Get, templateName, version,
			func(t *PushTemplate) string { return t.Version }, clonePushTemplate,
			func(dst, live *PushTemplate) { dst.Tags = nonNilTagsCopy(live.Tags) })
	case templateTypeSMS:
		return getVersionedTemplate(b, key, b.smsTemplates.Get, templateName, version,
			func(t *SmsTemplate) string { return t.Version }, cloneSmsTemplate,
			func(dst, live *SmsTemplate) { dst.Tags = nonNilTagsCopy(live.Tags) })
	case templateTypeVoice:
		return getVersionedTemplate(b, key, b.voiceTemplates.Get, templateName, version,
			func(t *VoiceTemplate) string { return t.Version },
			func(t *VoiceTemplate) *VoiceTemplate {
				cp := *t

				return &cp
			},
			func(dst, live *VoiceTemplate) { dst.Tags = nonNilTagsCopy(live.Tags) })
	}

	return nil, fmt.Errorf("%w: unknown template type %s", ErrAppNotFound, templateType)
}

func getVersionedTemplate[T any](
	b *InMemoryBackend,
	key string,
	get func(string) (*T, bool),
	name, requested string,
	versionOf func(*T) string,
	clone func(*T) *T,
	adoptLive func(dst, live *T),
) (any, error) {
	live, ok := get(name)
	if !ok {
		return nil, ErrAppNotFound
	}

	picked, isLive, err := pickTemplateVersion(b, key, live, versionOf(live), requested)
	if err != nil {
		return nil, err
	}

	out := clone(picked)
	if !isLive {
		adoptLive(out, live)
	}

	return out, nil
}

// DeleteTemplate deletes a template, or one version of it when version is set.
func (b *InMemoryBackend) DeleteTemplate(templateName, templateType, version string) error {
	b.mu.Lock("DeleteTemplate")
	defer b.mu.Unlock()

	if version == "" {
		return b.deleteWholeTemplateLocked(templateName, templateType)
	}

	key := templateVersionKey(templateName, templateType)
	history := b.templateVersionHistory[key]

	if !b.templateExistsLocked(templateName, templateType) {
		return ErrAppNotFound
	}

	if !hasTemplateVersion(history, version) {
		return fmt.Errorf("%w: template version %s", ErrAppNotFound, version)
	}

	if len(history) == 1 {
		return b.deleteWholeTemplateLocked(templateName, templateType)
	}

	if pinned, ok := b.templateActiveVersion[key]; ok && pinned == version {
		delete(b.templateActiveVersion, key)
	}

	latest := latestVersionOf(history)
	if version != latest {
		b.removeVersionEntryLocked(key, version)

		return nil
	}

	previous := history[len(history)-2].TemplateVersion
	raw := b.templateVersionData[key][previous]
	b.removeVersionEntryLocked(key, latest)
	delete(b.templateVersionData[key], previous)

	return b.restoreLatestLocked(templateName, templateType, raw)
}

func (b *InMemoryBackend) removeVersionEntryLocked(key, version string) {
	history := b.templateVersionHistory[key]
	kept := history[:0:0]

	for i := range history {
		if history[i].TemplateVersion != version {
			kept = append(kept, history[i])
		}
	}

	b.templateVersionHistory[key] = kept
	delete(b.templateVersionData[key], version)
}

func (b *InMemoryBackend) templateExistsLocked(name, templateType string) bool {
	switch strings.ToLower(templateType) {
	case templateTypeEmail:
		_, ok := b.emailTemplates.Get(name)

		return ok
	case templateTypeInApp:
		_, ok := b.inAppTemplates.Get(name)

		return ok
	case templateTypePush:
		_, ok := b.pushTemplates.Get(name)

		return ok
	case templateTypeSMS:
		_, ok := b.smsTemplates.Get(name)

		return ok
	case templateTypeVoice:
		_, ok := b.voiceTemplates.Get(name)

		return ok
	}

	return false
}

func (b *InMemoryBackend) deleteWholeTemplateLocked(name, templateType string) error {
	switch strings.ToLower(templateType) {
	case templateTypeEmail:
		return unlockedDelete(b, name, b.emailTemplates.Get, b.emailTemplates.Delete,
			func(t *EmailTemplate) string { return t.ARN }, ChannelTypeEmail)
	case templateTypeInApp:
		return unlockedDelete(b, name, b.inAppTemplates.Get, b.inAppTemplates.Delete,
			func(t *InAppTemplate) string { return t.ARN }, templateTypeINAPP)
	case templateTypePush:
		return unlockedDelete(b, name, b.pushTemplates.Get, b.pushTemplates.Delete,
			func(t *PushTemplate) string { return t.ARN }, templateTypePUSH)
	case templateTypeSMS:
		return unlockedDelete(b, name, b.smsTemplates.Get, b.smsTemplates.Delete,
			func(t *SmsTemplate) string { return t.ARN }, ChannelTypeSMS)
	case templateTypeVoice:
		return unlockedDelete(b, name, b.voiceTemplates.Get, b.voiceTemplates.Delete,
			func(t *VoiceTemplate) string { return t.ARN }, ChannelTypeVoice)
	}

	return fmt.Errorf("%w: unknown template type %s", ErrAppNotFound, templateType)
}

func unlockedDelete[T any](
	b *InMemoryBackend,
	name string,
	get func(string) (*T, bool),
	del func(string) bool,
	arnOf func(*T) string,
	channel string,
) error {
	t, ok := get(name)
	if !ok {
		return ErrAppNotFound
	}

	del(name)
	delete(b.arnIndex, arnOf(t))
	b.dropTemplateVersionsLocked(name + "/" + channel)

	return nil
}

// restoreLatestLocked rewinds the live template to an older snapshot, keeping its tags.
func (b *InMemoryBackend) restoreLatestLocked(name, templateType string, raw json.RawMessage) error {
	switch strings.ToLower(templateType) {
	case templateTypeEmail:
		t, _ := b.emailTemplates.Get(name)

		return restoreTemplate(t, raw, func(t *EmailTemplate) *map[string]string { return &t.Tags })
	case templateTypeInApp:
		t, _ := b.inAppTemplates.Get(name)

		return restoreTemplate(t, raw, func(t *InAppTemplate) *map[string]string { return &t.Tags })
	case templateTypePush:
		t, _ := b.pushTemplates.Get(name)

		return restoreTemplate(t, raw, func(t *PushTemplate) *map[string]string { return &t.Tags })
	case templateTypeSMS:
		t, _ := b.smsTemplates.Get(name)

		return restoreTemplate(t, raw, func(t *SmsTemplate) *map[string]string { return &t.Tags })
	case templateTypeVoice:
		t, _ := b.voiceTemplates.Get(name)

		return restoreTemplate(t, raw, func(t *VoiceTemplate) *map[string]string { return &t.Tags })
	}

	return nil
}

func restoreTemplate[T any](dst *T, raw json.RawMessage, tags func(*T) *map[string]string) error {
	var fresh T
	if err := json.Unmarshal(raw, &fresh); err != nil {
		return fmt.Errorf("restore template version: %w", err)
	}

	*tags(&fresh) = *tags(dst)
	*dst = fresh

	return nil
}

func (b *InMemoryBackend) liveTemplateJSONLocked(name, templateType string) (string, json.RawMessage, bool) {
	var (
		version string
		live    any
	)

	switch strings.ToLower(templateType) {
	case templateTypeEmail:
		if t, ok := b.emailTemplates.Get(name); ok {
			version, live = t.Version, t
		}
	case templateTypeInApp:
		if t, ok := b.inAppTemplates.Get(name); ok {
			version, live = t.Version, t
		}
	case templateTypePush:
		if t, ok := b.pushTemplates.Get(name); ok {
			version, live = t.Version, t
		}
	case templateTypeSMS:
		if t, ok := b.smsTemplates.Get(name); ok {
			version, live = t.Version, t
		}
	case templateTypeVoice:
		if t, ok := b.voiceTemplates.Get(name); ok {
			version, live = t.Version, t
		}
	}

	if live == nil {
		return "", nil, false
	}

	raw, err := json.Marshal(live)
	if err != nil {
		return "", nil, false
	}

	return version, raw, true
}

// ListTemplateVersions returns the version history of a template.
func (b *InMemoryBackend) ListTemplateVersions(
	templateName, templateType string,
) ([]*templateVersionItem, error) {
	b.mu.RLock("ListTemplateVersions")
	defer b.mu.RUnlock()

	key := templateVersionKey(templateName, templateType)

	history := b.templateVersionHistory[key]
	if len(history) == 0 {
		return nil, ErrAppNotFound
	}

	liveVersion, liveRaw, _ := b.liveTemplateJSONLocked(templateName, templateType)
	result := make([]*templateVersionItem, len(history))

	for i := range history {
		cp := history[i]
		raw := b.templateVersionData[key][cp.TemplateVersion]

		if cp.TemplateVersion == liveVersion {
			raw = liveRaw
		}

		var meta templateVersionMeta
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &meta)
		}

		cp.CreationDate = meta.CreationDate
		cp.LastModifiedDate = meta.LastModifiedDate
		cp.DefaultSubstitutions = meta.DefaultSubstitutions
		cp.TemplateDescription = meta.TemplateDescription
		result[i] = &cp
	}

	return result, nil
}

// UpdateTemplateActiveVersion makes version (an existing id, or "latest") the one Get returns by default.
func (b *InMemoryBackend) UpdateTemplateActiveVersion(templateName, templateType, version string) error {
	b.mu.Lock("UpdateTemplateActiveVersion")
	defer b.mu.Unlock()

	key := templateVersionKey(templateName, templateType)

	history := b.templateVersionHistory[key]
	if len(history) == 0 {
		return ErrAppNotFound
	}

	if version == "" || version == latestTemplateVersion {
		delete(b.templateActiveVersion, key)

		return nil
	}

	if !hasTemplateVersion(history, version) {
		return fmt.Errorf("%w: template version %s", ErrAppNotFound, version)
	}

	b.templateActiveVersion[key] = version

	return nil
}

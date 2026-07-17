package pinpoint

import (
	"maps"
)

// tagHolder is implemented by all resource types that carry tags and an ARN,
// allowing the ARN index to support tag operations on any resource type.
type tagHolder interface {
	getARN() string
	getTags() map[string]string
	setTags(map[string]string)
}

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
	_ tagHolder = (*VoiceTemplate)(nil)
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

func (t *VoiceTemplate) getARN() string              { return t.ARN }
func (t *VoiceTemplate) getTags() map[string]string  { return t.Tags }
func (t *VoiceTemplate) setTags(m map[string]string) { t.Tags = m }

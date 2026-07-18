package mediaconvert

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// AddPresetInternal inserts a preset directly into the backend.
func (b *InMemoryBackend) AddPresetInternal(p *Preset) {
	b.mu.Lock("AddPresetInternal")
	defer b.mu.Unlock()

	b.presets.Put(p)
}

// CreatePreset creates a new MediaConvert output preset.
func (b *InMemoryBackend) CreatePreset(
	name, description, category string,
	settings map[string]any,
	tags map[string]string,
) (*Preset, error) {
	b.mu.Lock("CreatePreset")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if b.presets.Has(name) {
		return nil, fmt.Errorf("%w: preset %s already exists", ErrAlreadyExists, name)
	}

	now := epochSeconds(time.Now())
	p := &Preset{
		Arn:         arn.Build("mediaconvert", b.region, b.accountID, "presets/"+name),
		Name:        name,
		Description: description,
		Category:    category,
		Settings:    deepCloneMap(settings),
		Tags:        nonNilTagsCopy(tags),
		Type:        presetCustom,
		CreatedAt:   now,
		LastUpdated: now,
	}
	b.presets.Put(p)

	if len(tags) > 0 {
		b.storeTagsLocked(p.Arn, tags)
	}

	return clonePreset(p), nil
}

// GetPreset returns a preset by name.
func (b *InMemoryBackend) GetPreset(name string) (*Preset, error) {
	b.mu.RLock("GetPreset")
	defer b.mu.RUnlock()

	p, ok := b.presets.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: preset %s not found", ErrNotFound, name)
	}

	return clonePreset(p), nil
}

// ListPresets returns all presets sorted by name.
func (b *InMemoryBackend) ListPresets() []*Preset {
	b.mu.RLock("ListPresets")
	defer b.mu.RUnlock()

	list := make([]*Preset, 0, b.presets.Len())
	for _, p := range b.presets.All() {
		list = append(list, clonePreset(p))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdatePreset updates a preset's description, category, and settings.
func (b *InMemoryBackend) UpdatePreset(name, description, category string, settings map[string]any) (*Preset, error) {
	b.mu.Lock("UpdatePreset")
	defer b.mu.Unlock()

	p, ok := b.presets.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: preset %s not found", ErrNotFound, name)
	}

	if description != "" {
		p.Description = description
	}

	if category != "" {
		p.Category = category
	}

	if settings != nil {
		p.Settings = deepCloneMap(settings)
	}

	p.LastUpdated = epochSeconds(time.Now())

	return clonePreset(p), nil
}

// DeletePreset removes a preset by name.
func (b *InMemoryBackend) DeletePreset(name string) error {
	b.mu.Lock("DeletePreset")
	defer b.mu.Unlock()

	p, ok := b.presets.Get(name)
	if !ok {
		return fmt.Errorf("%w: preset %s not found", ErrNotFound, name)
	}
	delete(b.tags, p.Arn)
	b.presets.Delete(name)

	return nil
}

func clonePreset(p *Preset) *Preset {
	cp := *p
	cp.Settings = deepCloneMap(p.Settings)
	cp.Tags = nonNilTagsCopy(p.Tags)

	return &cp
}

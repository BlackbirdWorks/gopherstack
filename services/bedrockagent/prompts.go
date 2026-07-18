package bedrockagent

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Prompt CRUD
// ---------------------------------------------------------------------------

// CreatePrompt creates a new prompt.
func (b *InMemoryBackend) CreatePrompt(ctx context.Context, cfg PromptConfig) (*Prompt, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	region := ctxRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.promptsByName[cfg.Name]; exists {
		return nil, fmt.Errorf("%w: prompt %q already exists", ErrAlreadyExists, cfg.Name)
	}

	id := b.nextID("prompt", &b.promptCounter)
	now := time.Now().UTC()

	p := &Prompt{
		PromptID:       id,
		PromptARN:      b.buildPromptARN(region, id),
		Name:           cfg.Name,
		Description:    cfg.Description,
		DefaultVariant: cfg.DefaultVariant,
		Variants:       cfg.Variants,
		Tags:           maps.Clone(cfg.Tags),
		Version:        "DRAFT",
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	b.prompts.Put(p)
	b.promptsByName[cfg.Name] = id
	b.tags[p.PromptARN] = maps.Clone(cfg.Tags)

	return promptCopy(p), nil
}

// GetPrompt returns a prompt.
func (b *InMemoryBackend) GetPrompt(_ context.Context, promptID string) (*Prompt, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	return promptCopy(p), nil
}

// UpdatePrompt updates a prompt.
func (b *InMemoryBackend) UpdatePrompt(
	_ context.Context, promptID string, cfg PromptConfig,
) (*Prompt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	if cfg.Name != "" {
		p.Name = cfg.Name
	}

	if cfg.Description != "" {
		p.Description = cfg.Description
	}

	if cfg.DefaultVariant != "" {
		p.DefaultVariant = cfg.DefaultVariant
	}

	if cfg.Variants != nil {
		p.Variants = cfg.Variants
	}

	if cfg.Tags != nil {
		p.Tags = maps.Clone(cfg.Tags)
	}

	p.UpdatedAt = time.Now().UTC()

	return promptCopy(p), nil
}

// DeletePrompt deletes a prompt.
func (b *InMemoryBackend) DeletePrompt(_ context.Context, promptID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	delete(b.promptsByName, p.Name)
	b.prompts.Delete(promptID)

	for _, pv := range slices.Clone(b.promptVersionsByPrompt.Get(promptID)) {
		b.promptVersions.Delete(promptVersionKey(pv.PromptID, pv.Version))
	}

	delete(b.promptVersionCtrs, promptID)

	return nil
}

// ListPrompts returns paginated prompt summaries.
func (b *InMemoryBackend) ListPrompts(
	_ context.Context, maxResults int, nextToken string,
) ([]*PromptSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ids := tableIDs(b.prompts.Snapshot(), func(p *Prompt) string { return p.PromptID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*PromptSummary, 0, len(ids))

	for _, id := range ids {
		p, _ := b.prompts.Get(id)
		out = append(out, &PromptSummary{
			PromptID:    p.PromptID,
			PromptARN:   p.PromptARN,
			Name:        p.Name,
			Description: p.Description,
			Version:     p.Version,
			CreatedAt:   p.CreatedAt,
			UpdatedAt:   p.UpdatedAt,
		})
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Prompt version CRUD
// ---------------------------------------------------------------------------

// CreatePromptVersion creates a versioned snapshot of a prompt.
func (b *InMemoryBackend) CreatePromptVersion(
	_ context.Context, promptID, description string,
) (*PromptVersion, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	b.promptVersionCtrs[promptID]++
	vNum := b.promptVersionCtrs[promptID]
	version := strconv.Itoa(vNum)

	pv := &PromptVersion{
		PromptID:    promptID,
		PromptARN:   p.PromptARN,
		Name:        p.Name,
		Version:     version,
		Variants:    p.Variants,
		Description: description,
		CreatedAt:   time.Now().UTC(),
	}

	b.promptVersions.Put(pv)

	return promptVersionCopy(pv), nil
}

// GetPromptVersion returns a specific prompt version. See the not-found
// precedence note on GetAgentVersion in the Agent version CRUD section above
// -- the same b.prompts.Has(promptID)-instead-of-inner-map-presence
// reasoning applies here.
func (b *InMemoryBackend) GetPromptVersion(
	_ context.Context, promptID, version string,
) (*PromptVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.prompts.Has(promptID) {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	pv, ok := b.promptVersions.Get(promptVersionKey(promptID, version))
	if !ok {
		return nil, fmt.Errorf("%w: prompt version %q not found", ErrNotFound, version)
	}

	return promptVersionCopy(pv), nil
}

// DeletePromptVersion deletes a prompt version.
func (b *InMemoryBackend) DeletePromptVersion(
	_ context.Context, promptID, version string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.prompts.Has(promptID) {
		return fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	key := promptVersionKey(promptID, version)
	if !b.promptVersions.Has(key) {
		return fmt.Errorf("%w: prompt version %q not found", ErrNotFound, version)
	}

	b.promptVersions.Delete(key)

	return nil
}

func promptCopy(p *Prompt) *Prompt {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp
}

func promptVersionCopy(pv *PromptVersion) *PromptVersion {
	cp := *pv

	return &cp
}

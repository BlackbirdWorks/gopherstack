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

	b.mu.Lock("CreatePrompt")
	defer b.mu.Unlock()

	if prior := findByClientToken(b.prompts, cfg.ClientToken,
		func(p *Prompt) string { return p.ClientToken }, func(*Prompt) bool { return true },
	); prior != nil {
		return promptCopy(prior), nil
	}

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
		Version:        "DRAFT",
		KMSKeyARN:      cfg.CustomerEncryptionKeyArn,
		ClientToken:    cfg.ClientToken,
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
	b.mu.RLock("GetPrompt")
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
	b.mu.Lock("UpdatePrompt")
	defer b.mu.Unlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	if cfg.Name != "" {
		p.Name = cfg.Name
	}

	// api_op_UpdatePrompt.go:13: omitted fields are not kept
	p.Description = cfg.Description

	if cfg.DefaultVariant != "" {
		p.DefaultVariant = cfg.DefaultVariant
	}

	if cfg.Variants != nil {
		p.Variants = cfg.Variants
	}

	if cfg.CustomerEncryptionKeyArn != "" {
		p.KMSKeyARN = cfg.CustomerEncryptionKeyArn
	}

	p.UpdatedAt = time.Now().UTC()

	return promptCopy(p), nil
}

// DeletePrompt deletes a prompt.
func (b *InMemoryBackend) DeletePrompt(_ context.Context, promptID string) error {
	b.mu.Lock("DeletePrompt")
	defer b.mu.Unlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	delete(b.promptsByName, p.Name)
	b.prompts.Delete(promptID)
	delete(b.tags, p.PromptARN)

	for _, pv := range slices.Clone(b.promptVersionsByPrompt.Get(promptID)) {
		b.promptVersions.Delete(promptVersionKey(pv.PromptID, pv.Version))
		delete(b.tags, pv.PromptARN)
	}

	delete(b.promptVersionCtrs, promptID)

	return nil
}

// ListPrompts returns paginated prompt summaries.
func (b *InMemoryBackend) ListPrompts(
	_ context.Context, promptIdentifier string, maxResults int, nextToken string,
) ([]*PromptSummary, string, error) {
	b.mu.RLock("ListPrompts")
	defer b.mu.RUnlock()

	if promptIdentifier != "" {
		return b.listPromptVersionsLocked(promptIdentifier, maxResults, nextToken)
	}

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
	_ context.Context, promptID string, cfg VersionConfig,
) (*PromptVersion, error) {
	b.mu.Lock("CreatePromptVersion")
	defer b.mu.Unlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	if prior := findByClientToken(b.promptVersions, cfg.ClientToken,
		func(v *PromptVersion) string { return v.ClientToken },
		func(v *PromptVersion) bool { return v.PromptID == promptID },
	); prior != nil {
		return promptVersionCopy(prior), nil
	}

	b.promptVersionCtrs[promptID]++
	vNum := b.promptVersionCtrs[promptID]
	version := strconv.Itoa(vNum)

	now := time.Now().UTC()
	pv := &PromptVersion{
		PromptID:    promptID,
		PromptARN:   p.PromptARN + ":" + version,
		Name:        p.Name,
		Version:     version,
		Variants:    p.Variants,
		Description: cfg.Description,

		DefaultVariant: p.DefaultVariant,
		KMSKeyARN:      p.KMSKeyARN,
		ClientToken:    cfg.ClientToken,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	b.promptVersions.Put(pv)
	b.tags[pv.PromptARN] = maps.Clone(cfg.Tags)

	return promptVersionCopy(pv), nil
}

// GetPromptVersion returns a specific prompt version. See the not-found
// precedence note on GetAgentVersion in the Agent version CRUD section above
// -- the same b.prompts.Has(promptID)-instead-of-inner-map-presence
// reasoning applies here.
func (b *InMemoryBackend) GetPromptVersion(
	_ context.Context, promptID, version string,
) (*PromptVersion, error) {
	b.mu.RLock("GetPromptVersion")
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
	b.mu.Lock("DeletePromptVersion")
	defer b.mu.Unlock()

	if !b.prompts.Has(promptID) {
		return fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	key := promptVersionKey(promptID, version)
	if !b.promptVersions.Has(key) {
		return fmt.Errorf("%w: prompt version %q not found", ErrNotFound, version)
	}

	if pv, ok := b.promptVersions.Get(key); ok {
		delete(b.tags, pv.PromptARN)
	}

	b.promptVersions.Delete(key)

	return nil
}

func promptCopy(p *Prompt) *Prompt {
	cp := *p

	return &cp
}

func promptVersionCopy(pv *PromptVersion) *PromptVersion {
	cp := *pv

	return &cp
}

// listPromptVersionsLocked lists the DRAFT and every numbered version of one prompt.
func (b *InMemoryBackend) listPromptVersionsLocked(
	promptIdentifier string, maxResults int, nextToken string,
) ([]*PromptSummary, string, error) {
	p, ok := b.prompts.Get(promptIdentifier)
	if !ok {
		p, ok = b.promptByARNLocked(promptIdentifier)
	}

	if !ok {
		return nil, "", fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptIdentifier)
	}

	all := []*PromptSummary{{
		PromptID: p.PromptID, PromptARN: p.PromptARN, Name: p.Name, Description: p.Description,
		Version: p.Version, CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt,
	}}

	versions := slices.Clone(b.promptVersionsByPrompt.Get(p.PromptID))
	slices.SortFunc(
		versions,
		func(a, c *PromptVersion) int { return versionNumber(a.Version) - versionNumber(c.Version) },
	)

	for _, pv := range versions {
		all = append(all, &PromptSummary{
			PromptID: pv.PromptID, PromptARN: pv.PromptARN, Name: pv.Name, Description: pv.Description,
			Version: pv.Version, CreatedAt: pv.CreatedAt, UpdatedAt: pv.UpdatedAt,
		})
	}

	keys := make([]string, len(all))
	byKey := make(map[string]*PromptSummary, len(all))

	for i, s := range all {
		keys[i] = fmt.Sprintf("%06d", i)
		byKey[keys[i]] = s
	}

	keys, outToken := paginate(keys, nextToken, maxResults)
	out := make([]*PromptSummary, 0, len(keys))

	for _, k := range keys {
		out = append(out, byKey[k])
	}

	return out, outToken, nil
}

func (b *InMemoryBackend) promptByARNLocked(arn string) (*Prompt, bool) {
	var found *Prompt

	b.prompts.Range(func(p *Prompt) bool {
		if p.PromptARN == arn {
			found = p

			return false
		}

		return true
	})

	return found, found != nil
}

func versionNumber(v string) int {
	n, _ := strconv.Atoi(v)

	return n
}

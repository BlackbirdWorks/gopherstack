package bedrock

import (
	"fmt"
	"sort"
	"strconv"
	"time"
)

// CreatePromptVersion creates a numbered snapshot version of a Prompt.
func (b *InMemoryBackend) CreatePromptVersion(promptID string) (*PromptVersion, error) {
	b.mu.Lock("CreatePromptVersion")
	defer b.mu.Unlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	b.promptVersionCounters[promptID]++
	ver := strconv.Itoa(b.promptVersionCounters[promptID])

	pv := &PromptVersion{
		CreatedAt: time.Now(),
		PromptID:  promptID,
		Version:   ver,
		Name:      p.Name,
	}

	b.promptVersionsStore(promptID).Put(pv)
	cp := *pv

	return &cp, nil
}

// GetPromptVersion returns a specific Prompt version.
func (b *InMemoryBackend) GetPromptVersion(promptID, version string) (*PromptVersion, error) {
	b.mu.RLock("GetPromptVersion")
	defer b.mu.RUnlock()

	versions, versionsOK := b.promptVersions[promptID]
	if !versionsOK {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	pv, verOK := versions.Get(version)
	if !verOK {
		return nil, fmt.Errorf(
			"%w: prompt version %q not found for prompt %q",
			ErrNotFound,
			version,
			promptID,
		)
	}

	cp := *pv

	return &cp, nil
}

// ListPromptVersions lists all versions for a Prompt.
func (b *InMemoryBackend) ListPromptVersions(
	promptID string,
	maxResults int,
	nextToken string,
) ([]*PromptVersion, string) {
	b.mu.RLock("ListPromptVersions")
	defer b.mu.RUnlock()

	versions := b.promptVersions[promptID]
	list := make([]*PromptVersion, 0)

	if versions != nil {
		for _, pv := range versions.All() {
			cp := *pv
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Version < list[j].Version })

	return paginate(list, maxResults, nextToken)
}

// DeletePromptVersion deletes a specific Prompt version.
func (b *InMemoryBackend) DeletePromptVersion(promptID, version string) error {
	b.mu.Lock("DeletePromptVersion")
	defer b.mu.Unlock()

	versions, versionsOK := b.promptVersions[promptID]
	if !versionsOK {
		return fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	if !versions.Has(version) {
		return fmt.Errorf(
			"%w: prompt version %q not found for prompt %q",
			ErrNotFound,
			version,
			promptID,
		)
	}

	versions.Delete(version)

	return nil
}

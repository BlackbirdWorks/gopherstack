package personalize

import (
	"fmt"
	"time"
)

// --- Campaign ---

// CreateCampaign creates a new campaign.
func (b *InMemoryBackend) CreateCampaign(
	name, solutionVersionArn string,
	minProvisionedTPS int32,
	tags map[string]string,
) (*Campaign, error) {
	b.mu.Lock("CreateCampaign")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.campaigns.Has(name) {
		return nil, fmt.Errorf("%w: campaign %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	c := &Campaign{
		CampaignArn:         b.personalizeARN("campaign", name),
		Name:                name,
		SolutionVersionArn:  solutionVersionArn,
		MinProvisionedTPS:   minProvisionedTPS,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.campaigns.Put(c)
	if len(tags) > 0 {
		b.tags[c.CampaignArn] = copyStringMap(tags)
	}

	return c, nil
}

// DescribeCampaign returns a campaign by name or ARN.
func (b *InMemoryBackend) DescribeCampaign(nameOrArn string) (*Campaign, error) {
	b.mu.RLock("DescribeCampaign")
	defer b.mu.RUnlock()

	if c := b.findCampaign(nameOrArn); c != nil {
		return c, nil
	}

	return nil, fmt.Errorf("%w: campaign %q not found", ErrNotFound, nameOrArn)
}

// UpdateCampaign updates a campaign's solution version or TPS.
func (b *InMemoryBackend) UpdateCampaign(
	nameOrArn, solutionVersionArn string,
	minProvisionedTPS int32,
) (*Campaign, error) {
	b.mu.Lock("UpdateCampaign")
	defer b.mu.Unlock()

	c := b.findCampaign(nameOrArn)
	if c == nil {
		return nil, fmt.Errorf("%w: campaign %q not found", ErrNotFound, nameOrArn)
	}
	if solutionVersionArn != "" {
		c.SolutionVersionArn = solutionVersionArn
	}
	if minProvisionedTPS > 0 {
		c.MinProvisionedTPS = minProvisionedTPS
	}
	c.LastUpdatedDateTime = time.Now().UTC()

	return c, nil
}

// DeleteCampaign removes a campaign.
func (b *InMemoryBackend) DeleteCampaign(nameOrArn string) error {
	b.mu.Lock("DeleteCampaign")
	defer b.mu.Unlock()

	c := b.findCampaign(nameOrArn)
	if c == nil {
		return fmt.Errorf("%w: campaign %q not found", ErrNotFound, nameOrArn)
	}
	b.campaigns.Delete(c.Name)
	delete(b.tags, c.CampaignArn)

	return nil
}

// ListCampaigns returns campaigns, optionally filtered by solution ARN.
func (b *InMemoryBackend) ListCampaigns(solutionArn string, maxResults int, nextToken string) ([]*Campaign, string) {
	b.mu.RLock("ListCampaigns")
	defer b.mu.RUnlock()

	all := b.campaigns.Snapshot()
	filtered := make([]*Campaign, 0, len(all))
	for _, c := range all {
		if solutionArn == "" || c.SolutionVersionArn == solutionArn {
			filtered = append(filtered, c)
		}
	}

	return paginateItems(filtered, campaignKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findCampaign(nameOrArn string) *Campaign {
	if c, ok := b.campaigns.Get(nameOrArn); ok {
		return c
	}
	for _, c := range b.campaigns.All() {
		if c.CampaignArn == nameOrArn {
			return c
		}
	}

	return nil
}

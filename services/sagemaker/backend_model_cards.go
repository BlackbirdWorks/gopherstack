package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrModelCardNotFound is returned when a model card does not exist.
var ErrModelCardNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// ModelCard
// ---------------------------------------------------------------------------

// ModelCard represents a SageMaker model card.
type ModelCard struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	ModelCardName    string            `json:"ModelCardName"`
	ModelCardArn     string            `json:"ModelCardArn"`
	ModelCardStatus  string            `json:"ModelCardStatus"`
	Content          string            `json:"Content,omitempty"`
	ModelCardVersion int               `json:"ModelCardVersion"`
}

func cloneModelCard(c *ModelCard) *ModelCard {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// CreateModelCard creates a model card.
func (b *InMemoryBackend) CreateModelCard(
	ctx context.Context,
	name, content string,
	tags map[string]string,
) (*ModelCard, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateModelCard")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", ErrValidation)
	}

	store := b.modelCardsStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: model card %q already exists", ErrValidation, name)
	}

	cardARN := arn.Build("sagemaker", region, b.accountID, "model-card/"+name)
	now := time.Now()

	c := &ModelCard{
		ModelCardName:    name,
		ModelCardArn:     cardARN,
		ModelCardStatus:  "Draft",
		ModelCardVersion: 1,
		Content:          content,
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	store.Put(c)

	return cloneModelCard(c), nil
}

// DescribeModelCard returns a model card by name.
func (b *InMemoryBackend) DescribeModelCard(ctx context.Context, name string) (*ModelCard, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeModelCard")
	defer b.mu.RUnlock()

	c, ok := b.modelCardsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	return cloneModelCard(c), nil
}

// UpdateModelCard updates a model card content and increments its version.
func (b *InMemoryBackend) UpdateModelCard(ctx context.Context, name, content string) (*ModelCard, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateModelCard")
	defer b.mu.Unlock()

	c, ok := b.modelCardsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	c.Content = content
	c.ModelCardVersion++
	c.LastModifiedTime = time.Now()

	return cloneModelCard(c), nil
}

// DeleteModelCard removes a model card by name.
func (b *InMemoryBackend) DeleteModelCard(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteModelCard")
	defer b.mu.Unlock()

	store := b.modelCardsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ListModelCards returns all model cards.
func (b *InMemoryBackend) ListModelCards(ctx context.Context, nextToken string) ([]*ModelCard, string) {
	b.mu.RLock("ListModelCards")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.modelCardsStoreRO(region),
		nextToken,
		cloneModelCard,
		func(v *ModelCard) string { return v.ModelCardName },
	)
}

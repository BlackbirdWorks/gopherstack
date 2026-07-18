package databrew

import (
	"context"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) recipeARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "recipe/"+name)
}

func (b *InMemoryBackend) CreateRecipe(
	ctx context.Context,
	name, description string,
	steps []RecipeStep,
	tags map[string]string,
) (*Recipe, error) {
	b.mu.Lock("CreateRecipe")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, ErrValidation
	}
	t := b.recipesTable(region)
	if t.Has(name) {
		return nil, ErrAlreadyExists
	}
	r := &Recipe{
		Name: name, Arn: b.recipeARN(region, name), Description: description,
		Steps: steps, Tags: maps.Clone(tags), RecipeVersion: "0.1",
		CreateDate: float64(time.Now().Unix()), LastModifiedDate: float64(time.Now().Unix()),
	}
	t.Put(r)

	return r, nil
}

func (b *InMemoryBackend) DescribeRecipe(ctx context.Context, name string) (*Recipe, error) {
	b.mu.RLock("DescribeRecipe")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	r, ok := b.recipesTable(region).Get(name)
	if !ok {
		return nil, ErrNotFound
	}
	cp := *r
	cp.Tags = maps.Clone(r.Tags)
	cp.Steps = append([]RecipeStep(nil), r.Steps...)

	return &cp, nil
}

func (b *InMemoryBackend) ListRecipes(
	ctx context.Context,
	maxResults int,
	nextToken string,
) ([]*Recipe, string) {
	b.mu.RLock("ListRecipes")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.recipesTable(region)
	keys := snapshotKeys(t, recipeKeyFn)
	pageKeys, next := paginateKeys(keys, maxResults, nextToken)
	out := make([]*Recipe, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		cp := *v
		cp.Tags = maps.Clone(v.Tags)
		cp.Steps = append([]RecipeStep(nil), v.Steps...)
		out = append(out, &cp)
	}

	return out, next
}

func (b *InMemoryBackend) PublishRecipe(ctx context.Context, name, description string) error {
	b.mu.Lock("PublishRecipe")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	r, ok := b.recipesTable(region).Get(name)
	if !ok {
		return ErrNotFound
	}
	r.RecipeVersion = "1.0"
	r.PublishedDate = float64(time.Now().Unix())
	r.PublishedBy = "admin"
	if description != "" {
		r.Description = description
	}

	return nil
}

func (b *InMemoryBackend) UpdateRecipe(
	ctx context.Context,
	name, description string,
	steps []RecipeStep,
) error {
	b.mu.Lock("UpdateRecipe")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	r, ok := b.recipesTable(region).Get(name)
	if !ok {
		return ErrNotFound
	}
	if description != "" {
		r.Description = description
	}
	r.Steps = steps
	r.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteRecipe(ctx context.Context, name string) error {
	b.mu.Lock("DeleteRecipe")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.recipesTable(region).Delete(name) {
		return ErrNotFound
	}

	return nil
}

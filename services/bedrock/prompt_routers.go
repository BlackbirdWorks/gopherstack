package bedrock

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) newPromptRouterID() string {
	b.promptRouterCounter++

	return "pr-" + strconv.Itoa(b.promptRouterCounter)
}

// CreatePromptRouter creates a new prompt router. fallbackModelArn, modelArns,
// and routingResponseQualityDiff mirror the real CreatePromptRouterInput's
// required FallbackModel/Models/RoutingCriteria fields -- gopherstack
// previously accepted only promptRouterName+tags, silently dropping all three
// and leaving every Get/List response missing them (all four are required
// response fields on GetPromptRouterOutput/PromptRouterSummary).
func (b *InMemoryBackend) CreatePromptRouter(
	name, description, fallbackModelArn string,
	modelArns []string,
	routingResponseQualityDiff float64,
	tags []Tag,
) (*PromptRouter, error) {
	b.mu.Lock("CreatePromptRouter")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: promptRouterName is required", ErrValidation)
	}

	if fallbackModelArn == "" {
		return nil, fmt.Errorf("%w: fallbackModel is required", ErrValidation)
	}

	if len(modelArns) == 0 {
		return nil, fmt.Errorf("%w: models is required", ErrValidation)
	}

	if _, exists := b.promptRoutersByName[name]; exists {
		return nil, fmt.Errorf("%w: prompt router %s already exists", ErrAlreadyExists, name)
	}

	id := b.newPromptRouterID()
	routerARN := arn.Build("bedrock", b.region, b.accountID, "prompt-router/"+id)
	now := time.Now().UTC()

	router := &PromptRouter{
		PromptRouterArn:            routerARN,
		PromptRouterName:           name,
		Description:                description,
		Status:                     statusAvailable,
		Type:                       "custom",
		FallbackModelArn:           fallbackModelArn,
		ModelArns:                  append([]string(nil), modelArns...),
		RoutingResponseQualityDiff: routingResponseQualityDiff,
		CreatedAt:                  now,
		UpdatedAt:                  now,
		Tags:                       copyTags(tags),
	}
	b.promptRouters.Put(router)
	b.promptRoutersByName[name] = routerARN
	cp := *router
	cp.Tags = copyTags(router.Tags)
	cp.ModelArns = append([]string(nil), router.ModelArns...)

	return &cp, nil
}

// GetPromptRouter returns a single prompt router by ARN.
func (b *InMemoryBackend) GetPromptRouter(routerARN string) (*PromptRouter, error) {
	b.mu.RLock("GetPromptRouter")
	defer b.mu.RUnlock()

	router, ok := b.promptRouters.Get(routerARN)
	if !ok {
		return nil, fmt.Errorf("%w: prompt router %s not found", ErrNotFound, routerARN)
	}

	cp := *router
	cp.Tags = copyTags(router.Tags)
	cp.ModelArns = append([]string(nil), router.ModelArns...)

	return &cp, nil
}

// ListPromptRouters returns prompt routers optionally filtered by type
// ("default" or "custom"), sorted and paginated.
func (b *InMemoryBackend) ListPromptRouters(typeEquals, nextToken string) ([]*PromptRouter, string) {
	b.mu.RLock("ListPromptRouters")
	defer b.mu.RUnlock()

	routers := make([]*PromptRouter, 0, b.promptRouters.Len())
	for _, r := range b.promptRouters.All() {
		if typeEquals != "" && r.Type != typeEquals {
			continue
		}

		cp := *r
		cp.Tags = copyTags(r.Tags)
		cp.ModelArns = append([]string(nil), r.ModelArns...)
		routers = append(routers, &cp)
	}

	sort.Slice(routers, func(i, k int) bool {
		return routers[i].PromptRouterName < routers[k].PromptRouterName
	})

	return paginateBedrockSlice(routers, nextToken)
}

// DeletePromptRouter removes a prompt router.
func (b *InMemoryBackend) DeletePromptRouter(routerARN string) error {
	b.mu.Lock("DeletePromptRouter")
	defer b.mu.Unlock()

	router, ok := b.promptRouters.Get(routerARN)
	if !ok {
		return fmt.Errorf("%w: prompt router %s not found", ErrNotFound, routerARN)
	}

	delete(b.promptRoutersByName, router.PromptRouterName)
	b.promptRouters.Delete(routerARN)

	return nil
}

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

// CreatePromptRouter creates a new prompt router.
func (b *InMemoryBackend) CreatePromptRouter(name string, tags []Tag) (*PromptRouter, error) {
	b.mu.Lock("CreatePromptRouter")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: promptRouterName is required", ErrValidation)
	}

	if _, exists := b.promptRoutersByName[name]; exists {
		return nil, fmt.Errorf("%w: prompt router %s already exists", ErrAlreadyExists, name)
	}

	id := b.newPromptRouterID()
	routerARN := arn.Build("bedrock", b.region, b.accountID, "prompt-router/"+id)
	now := time.Now().UTC()

	router := &PromptRouter{
		PromptRouterArn:  routerARN,
		PromptRouterName: name,
		Status:           "AVAILABLE",
		CreatedAt:        now,
		UpdatedAt:        now,
		Tags:             copyTags(tags),
	}
	b.promptRouters.Put(router)
	b.promptRoutersByName[name] = routerARN
	cp := *router
	cp.Tags = copyTags(router.Tags)

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

	return &cp, nil
}

// ListPromptRouters returns all prompt routers.
func (b *InMemoryBackend) ListPromptRouters() []*PromptRouter {
	b.mu.RLock("ListPromptRouters")
	defer b.mu.RUnlock()

	routers := make([]*PromptRouter, 0, b.promptRouters.Len())
	for _, r := range b.promptRouters.All() {
		cp := *r
		cp.Tags = copyTags(r.Tags)
		routers = append(routers, &cp)
	}

	sort.Slice(routers, func(i, k int) bool {
		return routers[i].PromptRouterName < routers[k].PromptRouterName
	})

	return routers
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

package amplify

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateApp creates a new Amplify application.
func (b *InMemoryBackend) CreateApp(
	name, description, repository, platform string,
	tagMap map[string]string,
) (*App, error) {
	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	appID := randomAppID()
	appARN := arn.Build("amplify", b.region, b.accountID, "apps/"+appID)
	now := time.Now().UTC()

	p := Platform(platform)
	if p == "" {
		p = PlatformWEB
	}

	app := &App{
		AppID:         appID,
		ARN:           appARN,
		Name:          name,
		Description:   description,
		Repository:    repository,
		Platform:      p,
		DefaultDomain: appID + ".amplifyapp.com",
		CreateTime:    now,
		UpdateTime:    now,
		Tags:          tags.FromMap("amplify.app."+appID+".tags", tagMap),
	}

	b.apps.Put(app)

	cp := *app

	return &cp, nil
}

// GetApp returns an Amplify application by ID.
func (b *InMemoryBackend) GetApp(appID string) (*App, error) {
	b.mu.RLock("GetApp")
	defer b.mu.RUnlock()

	app, ok := b.apps.Get(appID)
	if !ok {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	cp := *app

	return &cp, nil
}

// ListApps returns Amplify applications with optional pagination.
func (b *InMemoryBackend) ListApps(nextToken string, maxResults int) ([]*App, string, error) {
	b.mu.RLock("ListApps")
	defer b.mu.RUnlock()

	src := b.apps.All()
	all := make([]*App, 0, len(src))

	for _, app := range src {
		cp := *app
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].AppID < all[j].AppID })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// DeleteApp deletes an Amplify application by ID.
func (b *InMemoryBackend) DeleteApp(appID string) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	app, ok := b.apps.Get(appID)
	if !ok {
		return fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	app.Tags.Close()

	for _, branch := range slices.Clone(b.branchesByApp.Get(appID)) {
		branch.Tags.Close()
		b.branches.Delete(branchKey(appID, branch.BranchName))
	}

	b.apps.Delete(appID)

	return nil
}

// UpdateApp updates an existing Amplify application.
func (b *InMemoryBackend) UpdateApp(
	appID, name, description, repository, platform string,
) (*App, error) {
	b.mu.Lock("UpdateApp")
	defer b.mu.Unlock()

	app, ok := b.apps.Get(appID)
	if !ok {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if name != "" {
		app.Name = name
	}

	if description != "" {
		app.Description = description
	}

	if repository != "" {
		app.Repository = repository
	}

	if platform != "" {
		app.Platform = Platform(platform)
	}

	app.UpdateTime = time.Now().UTC()

	cp := *app

	return &cp, nil
}

package amplify

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateBranch creates a new branch for an Amplify application.
func (b *InMemoryBackend) CreateBranch(
	appID, branchName, description, stage string,
	enableAutoBuild bool,
	tagMap map[string]string,
) (*Branch, error) {
	b.mu.Lock("CreateBranch")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	key := branchKey(appID, branchName)
	if b.branches.Has(key) {
		return nil, fmt.Errorf("%w: branch %s already exists", ErrAlreadyExists, branchName)
	}

	branchARN := arn.Build(
		"amplify",
		b.region,
		b.accountID,
		fmt.Sprintf("apps/%s/branches/%s", appID, branchName),
	)
	now := time.Now().UTC()

	branch := &Branch{
		AppID:           appID,
		BranchName:      branchName,
		BranchARN:       branchARN,
		Description:     description,
		Stage:           Stage(stage),
		EnableAutoBuild: enableAutoBuild,
		CreateTime:      now,
		UpdateTime:      now,
		Tags:            tags.FromMap("amplify.branch."+appID+"."+branchName+".tags", tagMap),
	}

	b.branches.Put(branch)

	cp := *branch

	return &cp, nil
}

// GetBranch returns a branch for an Amplify application.
func (b *InMemoryBackend) GetBranch(appID, branchName string) (*Branch, error) {
	b.mu.RLock("GetBranch")
	defer b.mu.RUnlock()

	branch, ok := b.branches.Get(branchKey(appID, branchName))
	if !ok {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	cp := *branch

	return &cp, nil
}

// ListBranches returns branches for an Amplify application with optional pagination.
func (b *InMemoryBackend) ListBranches(
	appID, nextToken string,
	maxResults int,
) ([]*Branch, string, error) {
	b.mu.RLock("ListBranches")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	src := b.branchesByApp.Get(appID)
	all := make([]*Branch, 0, len(src))

	for _, branch := range src {
		cp := *branch
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].BranchName < all[j].BranchName })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// DeleteBranch deletes a branch from an Amplify application.
func (b *InMemoryBackend) DeleteBranch(appID, branchName string) error {
	b.mu.Lock("DeleteBranch")
	defer b.mu.Unlock()

	key := branchKey(appID, branchName)

	branch, ok := b.branches.Get(key)
	if !ok {
		return fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	branch.Tags.Close()
	b.branches.Delete(key)

	return nil
}

// UpdateBranch updates an existing Amplify branch.
func (b *InMemoryBackend) UpdateBranch(
	appID, branchName, description, stage string,
	enableAutoBuild bool,
) (*Branch, error) {
	b.mu.Lock("UpdateBranch")
	defer b.mu.Unlock()

	branch, ok := b.branches.Get(branchKey(appID, branchName))
	if !ok {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	if description != "" {
		branch.Description = description
	}

	if stage != "" {
		branch.Stage = Stage(stage)
	}

	branch.EnableAutoBuild = enableAutoBuild
	branch.UpdateTime = time.Now().UTC()

	cp := *branch

	return &cp, nil
}

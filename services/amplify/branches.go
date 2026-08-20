package amplify

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateBranch creates a new branch for an Amplify application. opts is
// optional (see BranchOptions); a nil/omitted opts applies real Amplify's
// create-time defaults for every field it covers.
func (b *InMemoryBackend) CreateBranch(
	appID, branchName, description, stage string,
	enableAutoBuild bool,
	tagMap map[string]string,
	opts ...BranchOptions,
) (*Branch, error) {
	if !isValidStage(stage) && stage != "" {
		return nil, fmt.Errorf("%w: invalid stage %q", ErrValidation, stage)
	}

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
		AppID:                appID,
		BranchName:           branchName,
		BranchARN:            branchARN,
		Description:          description,
		Stage:                Stage(stage),
		EnableAutoBuild:      enableAutoBuild,
		DisplayName:          branchName,
		TTL:                  defaultBranchTTL,
		CreateTime:           now,
		UpdateTime:           now,
		Tags:                 tags.FromMap("amplify.branch."+appID+"."+branchName+".tags", tagMap),
		EnvironmentVariables: map[string]string{},
	}

	applyBranchOptionsCreate(branch, firstBranchOptions(opts))

	b.branches.Put(branch)

	return b.branchView(branch), nil
}

// defaultBranchTTL is the content Time-To-Live real Amplify defaults new
// branches to (5 minutes) when the caller doesn't specify one.
const defaultBranchTTL = "5"

// firstBranchOptions returns opts[0] if present, else the zero value.
func firstBranchOptions(opts []BranchOptions) BranchOptions {
	if len(opts) > 0 {
		return opts[0]
	}

	return BranchOptions{}
}

// applyBranchOptionsCreate applies opts to a freshly constructed branch,
// honoring real Amplify's create-time defaults for every unset field.
func applyBranchOptionsCreate(branch *Branch, opts BranchOptions) {
	if opts.EnvironmentVariables != nil {
		branch.EnvironmentVariables = opts.EnvironmentVariables
	}

	if opts.DisplayName != nil {
		branch.DisplayName = *opts.DisplayName
	}

	if opts.TTL != nil {
		branch.TTL = *opts.TTL
	}

	branch.Framework = ptrconv.String(opts.Framework)
	branch.BasicAuthCredentials = ptrconv.String(opts.BasicAuthCredentials)
	branch.BuildSpec = ptrconv.String(opts.BuildSpec)
	branch.BackendEnvironmentARN = ptrconv.String(opts.BackendEnvironmentARN)
	branch.PullRequestEnvironmentName = ptrconv.String(opts.PullRequestEnvironmentName)
	branch.SourceBranch = ptrconv.String(opts.SourceBranch)
	branch.EnableBasicAuth = ptrconv.Bool(opts.EnableBasicAuth)
	branch.EnableNotification = ptrconv.Bool(opts.EnableNotification)
	branch.EnablePullRequestPreview = ptrconv.Bool(opts.EnablePullRequestPreview)
	branch.EnablePerformanceMode = ptrconv.Bool(opts.EnablePerformanceMode)
}

// applyBranchOptionsUpdate applies opts to an existing branch, leaving any
// field whose opts pointer is nil unchanged (real Amplify UpdateBranch
// partial-update semantics).
func applyBranchOptionsUpdate(branch *Branch, opts BranchOptions) {
	if opts.EnvironmentVariables != nil {
		branch.EnvironmentVariables = opts.EnvironmentVariables
	}

	if opts.DisplayName != nil {
		branch.DisplayName = *opts.DisplayName
	}

	if opts.Framework != nil {
		branch.Framework = *opts.Framework
	}

	if opts.TTL != nil {
		branch.TTL = *opts.TTL
	}

	if opts.BasicAuthCredentials != nil {
		branch.BasicAuthCredentials = *opts.BasicAuthCredentials
	}

	if opts.BuildSpec != nil {
		branch.BuildSpec = *opts.BuildSpec
	}

	if opts.BackendEnvironmentARN != nil {
		branch.BackendEnvironmentARN = *opts.BackendEnvironmentARN
	}

	if opts.PullRequestEnvironmentName != nil {
		branch.PullRequestEnvironmentName = *opts.PullRequestEnvironmentName
	}

	if opts.SourceBranch != nil {
		branch.SourceBranch = *opts.SourceBranch
	}

	if opts.EnableBasicAuth != nil {
		branch.EnableBasicAuth = *opts.EnableBasicAuth
	}

	if opts.EnableNotification != nil {
		branch.EnableNotification = *opts.EnableNotification
	}

	if opts.EnablePullRequestPreview != nil {
		branch.EnablePullRequestPreview = *opts.EnablePullRequestPreview
	}

	if opts.EnablePerformanceMode != nil {
		branch.EnablePerformanceMode = *opts.EnablePerformanceMode
	}
}

// branchView returns a copy of branch with computed, never-persisted fields
// (TotalNumberOfJobs, ActiveJobID) filled in. Must be called while holding
// at least a read lock.
func (b *InMemoryBackend) branchView(branch *Branch) *Branch {
	cp := *branch

	jobs := b.jobsByBranch.Get(branchKey(branch.AppID, branch.BranchName))
	cp.TotalNumberOfJobs = strconv.Itoa(len(jobs))

	var latest *Job

	for _, job := range jobs {
		if latest == nil || job.StartTime.After(latest.StartTime) {
			latest = job
		}
	}

	if latest != nil {
		cp.ActiveJobID = latest.JobID
	}

	return &cp
}

// GetBranch returns a branch for an Amplify application.
func (b *InMemoryBackend) GetBranch(appID, branchName string) (*Branch, error) {
	b.mu.RLock("GetBranch")
	defer b.mu.RUnlock()

	branch, ok := b.branches.Get(branchKey(appID, branchName))
	if !ok {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	return b.branchView(branch), nil
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
		all = append(all, b.branchView(branch))
	}

	sort.Slice(all, func(i, j int) bool { return all[i].BranchName < all[j].BranchName })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// DeleteBranch deletes a branch from an Amplify application, cascading the
// deletion to every job (and each job's artifacts) that belongs to it --
// without this, deleted branches would leave ghost job/artifact rows behind
// under a branch name ListJobs/ListArtifacts can no longer reach by any
// legitimate path once the branch itself 404s.
func (b *InMemoryBackend) DeleteBranch(appID, branchName string) (*Branch, error) {
	b.mu.Lock("DeleteBranch")
	defer b.mu.Unlock()

	key := branchKey(appID, branchName)

	branch, ok := b.branches.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	view := b.branchView(branch)

	b.deleteBranchLocked(branch)

	return view, nil
}

// UpdateBranch updates an existing Amplify branch. opts is optional (see
// BranchOptions); a nil/omitted opts leaves every field it covers unchanged.
func (b *InMemoryBackend) UpdateBranch(
	appID, branchName, description, stage string,
	enableAutoBuild bool,
	opts ...BranchOptions,
) (*Branch, error) {
	if !isValidStage(stage) && stage != "" {
		return nil, fmt.Errorf("%w: invalid stage %q", ErrValidation, stage)
	}

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

	applyBranchOptionsUpdate(branch, firstBranchOptions(opts))

	branch.UpdateTime = time.Now().UTC()

	return b.branchView(branch), nil
}

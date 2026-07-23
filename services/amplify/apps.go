package amplify

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateApp creates a new Amplify application. opts is optional (see
// AppOptions); a nil/omitted opts applies real Amplify's create-time
// defaults for every field it covers.
func (b *InMemoryBackend) CreateApp(
	name, description, repository, platform string,
	tagMap map[string]string,
	opts ...AppOptions,
) (*App, error) {
	if !isValidPlatform(platform) && platform != "" {
		return nil, fmt.Errorf("%w: invalid platform %q", ErrValidation, platform)
	}

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
		AppID:                appID,
		ARN:                  appARN,
		Name:                 name,
		Description:          description,
		Repository:           repository,
		Platform:             p,
		DefaultDomain:        appID + ".amplifyapp.com",
		CreateTime:           now,
		UpdateTime:           now,
		Tags:                 tags.FromMap("amplify.app."+appID+".tags", tagMap),
		EnvironmentVariables: map[string]string{},
		// EnableBranchAutoBuild defaults true in real Amplify unless the
		// caller opts out via AppOptions.
		EnableBranchAutoBuild: true,
	}

	applyAppOptionsCreate(app, firstAppOptions(opts))

	b.apps.Put(app)

	return b.appView(app), nil
}

// firstAppOptions returns opts[0] if present, else the zero value.
func firstAppOptions(opts []AppOptions) AppOptions {
	if len(opts) > 0 {
		return opts[0]
	}

	return AppOptions{}
}

// applyAppOptionsCreate applies opts to a freshly constructed app, honoring
// real Amplify's create-time defaults for every unset field.
func applyAppOptionsCreate(app *App, opts AppOptions) {
	if opts.EnvironmentVariables != nil {
		app.EnvironmentVariables = opts.EnvironmentVariables
	}

	app.AutoBranchCreationConfig = opts.AutoBranchCreationConfig
	app.CacheConfig = opts.CacheConfig
	app.BasicAuthCredentials = ptrconv.String(opts.BasicAuthCredentials)
	app.BuildSpec = ptrconv.String(opts.BuildSpec)
	app.CustomHeaders = ptrconv.String(opts.CustomHeaders)
	app.IAMServiceRoleArn = ptrconv.String(opts.IAMServiceRoleArn)
	app.AutoBranchCreationPatterns = opts.AutoBranchCreationPatterns
	app.CustomRules = opts.CustomRules

	if opts.EnableBranchAutoBuild != nil {
		app.EnableBranchAutoBuild = *opts.EnableBranchAutoBuild
	}

	app.EnableBasicAuth = ptrconv.Bool(opts.EnableBasicAuth)
	app.EnableAutoBranchCreation = ptrconv.Bool(opts.EnableAutoBranchCreation)
	app.EnableBranchAutoDeletion = ptrconv.Bool(opts.EnableBranchAutoDeletion)
}

// applyAppOptionsUpdate applies opts to an existing app, leaving any field
// whose opts pointer is nil unchanged (real Amplify UpdateApp partial-update
// semantics).
func applyAppOptionsUpdate(app *App, opts AppOptions) {
	if opts.EnvironmentVariables != nil {
		app.EnvironmentVariables = opts.EnvironmentVariables
	}

	if opts.AutoBranchCreationConfig != nil {
		app.AutoBranchCreationConfig = opts.AutoBranchCreationConfig
	}

	if opts.CacheConfig != nil {
		app.CacheConfig = opts.CacheConfig
	}

	if opts.BasicAuthCredentials != nil {
		app.BasicAuthCredentials = *opts.BasicAuthCredentials
	}

	if opts.BuildSpec != nil {
		app.BuildSpec = *opts.BuildSpec
	}

	if opts.CustomHeaders != nil {
		app.CustomHeaders = *opts.CustomHeaders
	}

	if opts.IAMServiceRoleArn != nil {
		app.IAMServiceRoleArn = *opts.IAMServiceRoleArn
	}

	if opts.AutoBranchCreationPatterns != nil {
		app.AutoBranchCreationPatterns = opts.AutoBranchCreationPatterns
	}

	if opts.CustomRules != nil {
		app.CustomRules = opts.CustomRules
	}

	if opts.EnableBranchAutoBuild != nil {
		app.EnableBranchAutoBuild = *opts.EnableBranchAutoBuild
	}

	if opts.EnableBasicAuth != nil {
		app.EnableBasicAuth = *opts.EnableBasicAuth
	}

	if opts.EnableAutoBranchCreation != nil {
		app.EnableAutoBranchCreation = *opts.EnableAutoBranchCreation
	}

	if opts.EnableBranchAutoDeletion != nil {
		app.EnableBranchAutoDeletion = *opts.EnableBranchAutoDeletion
	}
}

// appView returns a copy of app with computed, never-persisted fields
// (RepositoryCloneMethod, ProductionBranch) filled in. Must be called while
// holding at least a read lock.
func (b *InMemoryBackend) appView(app *App) *App {
	cp := *app
	cp.ProductionBranch = b.productionBranchFor(app.AppID)
	cp.RepositoryCloneMethod = repositoryCloneMethod(app.Repository)

	return &cp
}

// GetApp returns an Amplify application by ID.
func (b *InMemoryBackend) GetApp(appID string) (*App, error) {
	b.mu.RLock("GetApp")
	defer b.mu.RUnlock()

	app, ok := b.apps.Get(appID)
	if !ok {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	return b.appView(app), nil
}

// ListApps returns Amplify applications with optional pagination.
func (b *InMemoryBackend) ListApps(nextToken string, maxResults int) ([]*App, string, error) {
	b.mu.RLock("ListApps")
	defer b.mu.RUnlock()

	src := b.apps.All()
	all := make([]*App, 0, len(src))

	for _, app := range src {
		all = append(all, b.appView(app))
	}

	sort.Slice(all, func(i, j int) bool { return all[i].AppID < all[j].AppID })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// DeleteApp deletes an Amplify application and cascades the deletion to
// every child resource: branches (and their jobs and artifacts), domain
// associations, webhooks, and backend environments. Without this cascade,
// deleting an app would leave every child resource behind as a ghost row
// still reachable by ListBranches/ListJobs/etc. under the deleted app's ID,
// growing every table unboundedly across create/delete churn.
func (b *InMemoryBackend) DeleteApp(appID string) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	app, ok := b.apps.Get(appID)
	if !ok {
		return fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	app.Tags.Close()

	for _, branch := range slices.Clone(b.branchesByApp.Get(appID)) {
		b.deleteBranchLocked(branch)
	}

	for _, domain := range slices.Clone(b.domainsByApp.Get(appID)) {
		b.domains.Delete(domainKey(domain.AppID, domain.DomainName))
	}

	for _, wh := range slices.Clone(b.webhooksByApp.Get(appID)) {
		b.webhooks.Delete(wh.WebhookID)
	}

	for _, env := range slices.Clone(b.backendEnvironmentsByApp.Get(appID)) {
		b.backendEnvironments.Delete(backendEnvKey(env.AppID, env.EnvironmentName))
	}

	b.apps.Delete(appID)

	return nil
}

// deleteBranchLocked deletes branch and every job/artifact that belongs to
// it. Must be called while holding the write lock.
func (b *InMemoryBackend) deleteBranchLocked(branch *Branch) {
	key := branchKey(branch.AppID, branch.BranchName)

	for _, job := range slices.Clone(b.jobsByBranch.Get(key)) {
		jk := jobKey(job.AppID, job.BranchName, job.JobID)
		for _, art := range slices.Clone(b.artifactsByJob.Get(jk)) {
			b.artifacts.Delete(art.ArtifactID)
		}

		b.jobs.Delete(jk)
	}

	branch.Tags.Close()
	b.branches.Delete(key)
}

// UpdateApp updates an existing Amplify application. opts is optional (see
// AppOptions); a nil/omitted opts leaves every field it covers unchanged.
// name/description/repository/platform use the "" means unchanged"
// convention already established by the rest of this backend.
func (b *InMemoryBackend) UpdateApp(
	appID, name, description, repository, platform string,
	opts ...AppOptions,
) (*App, error) {
	if !isValidPlatform(platform) && platform != "" {
		return nil, fmt.Errorf("%w: invalid platform %q", ErrValidation, platform)
	}

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

	applyAppOptionsUpdate(app, firstAppOptions(opts))

	app.UpdateTime = time.Now().UTC()

	return b.appView(app), nil
}

// repositoryCloneMethod derives the (read-only, "for internal use") clone
// method real Amplify reports for an app: TOKEN for any app connected to a
// repository (the common case for GitHub/Bitbucket/GitLab access-token
// auth), empty for apps with no repository configured. gopherstack does not
// model the SIGV4 (CodeCommit) or SSH (GitLab/Bitbucket deploy key) cases
// since it has no notion of repository provider.
func repositoryCloneMethod(repository string) string {
	const repositoryCloneMethodToken = "TOKEN"

	if repository == "" {
		return ""
	}

	return repositoryCloneMethodToken
}

// productionBranchFor computes the ProductionBranch summary real Amplify
// reports on App/GetApp/ListApps/CreateApp/UpdateApp: the app's
// PRODUCTION-stage branch (there is normally at most one) together with its
// most recent job's outcome. Returns nil when the app has no PRODUCTION
// branch, matching real Amplify's behavior for apps that haven't designated
// one. Must be called while holding at least a read lock.
func (b *InMemoryBackend) productionBranchFor(appID string) *ProductionBranch {
	var prod *Branch

	for _, branch := range b.branchesByApp.Get(appID) {
		if branch.Stage == StageProduction {
			prod = branch

			break
		}
	}

	if prod == nil {
		return nil
	}

	pb := &ProductionBranch{BranchName: prod.BranchName}

	var latest *Job

	for _, job := range b.jobsByBranch.Get(branchKey(appID, prod.BranchName)) {
		if latest == nil || job.StartTime.After(latest.StartTime) {
			latest = job
		}
	}

	if latest != nil {
		pb.Status = string(latest.Status)
		pb.LastDeployTime = latest.StartTime
	}

	return pb
}

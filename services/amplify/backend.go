package amplify

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrNotFound is returned when a resource is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("BadRequestException", awserr.ErrAlreadyExists)
)

// StorageBackend defines the interface for Amplify storage operations.
type StorageBackend interface {
	// Snapshot and Restore implement persistence.Persistable. Handler
	// delegates to them (see persistence.go) so cli.go's generic
	// setupPersistence picks Amplify up.
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error

	CreateApp(
		name, description, repository, platform string,
		tagMap map[string]string,
	) (*App, error)
	GetApp(appID string) (*App, error)
	ListApps(nextToken string, maxResults int) ([]*App, string, error)
	DeleteApp(appID string) error
	UpdateApp(appID, name, description, repository, platform string) (*App, error)
	CreateBranch(
		appID, branchName, description, stage string,
		enableAutoBuild bool,
		tagMap map[string]string,
	) (*Branch, error)
	GetBranch(appID, branchName string) (*Branch, error)
	ListBranches(appID, nextToken string, maxResults int) ([]*Branch, string, error)
	DeleteBranch(appID, branchName string) error
	UpdateBranch(
		appID, branchName, description, stage string,
		enableAutoBuild bool,
	) (*Branch, error)
	TagResource(resourceARN string, tagMap map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	// Jobs
	StartJob(appID, branchName, jobType, commitID, commitMsg string) (*Job, error)
	StopJob(appID, branchName, jobID string) (*Job, error)
	GetJob(appID, branchName, jobID string) (*Job, error)
	ListJobs(appID, branchName, nextToken string, maxResults int) ([]*Job, string, error)
	DeleteJob(appID, branchName, jobID string) (*Job, error)
	CreateDeployment(appID, branchName string) (string, string, error)
	StartDeployment(appID, branchName, jobID, sourceURL string) (*Job, error)
	// Domains
	CreateDomainAssociation(
		appID, domainName string, subDomains []SubDomainSetting, enableAutoSubDomain bool,
	) (*DomainAssociation, error)
	UpdateDomainAssociation(
		appID, domainName string, subDomains []SubDomainSetting, enableAutoSubDomain bool,
	) (*DomainAssociation, error)
	DeleteDomainAssociation(appID, domainName string) (*DomainAssociation, error)
	GetDomainAssociation(appID, domainName string) (*DomainAssociation, error)
	ListDomainAssociations(
		appID, nextToken string,
		maxResults int,
	) ([]*DomainAssociation, string, error)
	// Webhooks
	CreateWebhook(appID, branchName, description string) (*Webhook, error)
	UpdateWebhook(webhookID, branchName, description string) (*Webhook, error)
	DeleteWebhook(webhookID string) (*Webhook, error)
	GetWebhook(webhookID string) (*Webhook, error)
	ListWebhooks(appID, nextToken string, maxResults int) ([]*Webhook, string, error)
	// Backend environments
	CreateBackendEnvironment(
		appID, environmentName, stackName, deploymentArtifacts string,
	) (*BackendEnvironment, error)
	GetBackendEnvironment(appID, environmentName string) (*BackendEnvironment, error)
	DeleteBackendEnvironment(appID, environmentName string) (*BackendEnvironment, error)
	ListBackendEnvironments(
		appID, nextToken string,
		maxResults int,
	) ([]*BackendEnvironment, string, error)
	// Logs and artifacts
	GenerateAccessLogs(appID, domainName, startTime, endTime string) (string, error)
	GetArtifactURL(artifactID string) (string, string, error)
	ListArtifacts(
		appID, _, _, nextToken string,
		maxResults int,
	) ([]*Artifact, string, error)
}

// appIDChars is the character set used to generate Amplify app IDs.
const appIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

const (
	arnResourceApps     = "apps"
	arnResourceBranches = "branches"
)

// randomAppID generates a cryptographically random 12-character alphanumeric ID.
func randomAppID() string {
	const length = 12

	b := make([]byte, length)
	charCount := uint64(len(appIDChars))

	for i := range b {
		var v [8]byte
		_, _ = rand.Read(v[:])
		b[i] = appIDChars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return string(b)
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// Every resource collection is a *store.Table[T] registered on registry (see
// store_setup.go); branches, jobs, domains, and backendEnvironments were
// previously nested per-parent maps and are now flattened to a single Table
// keyed by a composite "parent|child" string with a companion byApp/byBranch
// [store.Index] for the "all children of parent X" lookups the nested maps
// used to answer directly. webhooksByApp was a reverse-lookup map
// (map[string][]string); it is now a [store.Index] on webhooks, not its own
// Table.
type InMemoryBackend struct {
	apps                     *store.Table[App]
	branches                 *store.Table[Branch]
	branchesByApp            *store.Index[Branch]
	jobs                     *store.Table[Job]
	jobsByBranch             *store.Index[Job]
	domains                  *store.Table[DomainAssociation]
	domainsByApp             *store.Index[DomainAssociation]
	webhooks                 *store.Table[Webhook]
	webhooksByApp            *store.Index[Webhook]
	backendEnvironments      *store.Table[BackendEnvironment]
	backendEnvironmentsByApp *store.Index[BackendEnvironment]
	artifacts                *store.Table[Artifact]
	registry                 *store.Registry
	mu                       *lockmetrics.RWMutex
	accountID                string
	region                   string
}

// NewInMemoryBackend creates a new in-memory Amplify backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New("amplify"),
		accountID: accountID,
		region:    region,
	}
	registerAllTables(b)

	return b
}

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

// TagResource adds or updates tags on an Amplify resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tagMap map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.findTagsByARN(resourceARN)
	if err != nil {
		return err
	}

	t.Merge(tagMap)

	return nil
}

// UntagResource removes tags from an Amplify resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.findTagsByARN(resourceARN)
	if err != nil {
		return err
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns all tags for an Amplify resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.findTagsByARN(resourceARN)
	if err != nil {
		return nil, err
	}

	return t.Clone(), nil
}

// findTagsByARN resolves a resource ARN to its *tags.Tags. Must be called while
// holding at least a read lock; callers that modify the tags must hold a write lock.
// ARN format: arn:aws:amplify:{region}:{accountID}:apps/{appID}[/branches/{branchName}].
func (b *InMemoryBackend) findTagsByARN(resourceARN string) (*tags.Tags, error) {
	// Strip the common ARN prefix to get the resource path.
	// Expected prefix: "arn:aws:amplify:{region}:{accountID}:"
	const arnParts = 6
	parts := strings.SplitN(resourceARN, ":", arnParts)

	if len(parts) < arnParts || parts[2] != "amplify" {
		return nil, fmt.Errorf("%w: invalid Amplify ARN: %s", ErrNotFound, resourceARN)
	}

	resource := parts[5] // e.g. "apps/abc123" or "apps/abc123/branches/main"
	resourceParts := strings.Split(resource, "/")

	// apps/{appID}
	if len(resourceParts) == 2 && resourceParts[0] == arnResourceApps {
		appID := resourceParts[1]

		app, ok := b.apps.Get(appID)
		if !ok {
			return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
		}

		return app.Tags, nil
	}

	// apps/{appID}/branches/{branchName}
	if len(resourceParts) == 4 && resourceParts[0] == arnResourceApps &&
		resourceParts[2] == arnResourceBranches {
		appID := resourceParts[1]
		branchName := resourceParts[3]

		branch, ok := b.branches.Get(branchKey(appID, branchName))
		if !ok {
			return nil, fmt.Errorf(
				"%w: branch %s not found for app %s",
				ErrNotFound,
				branchName,
				appID,
			)
		}

		return branch.Tags, nil
	}

	return nil, fmt.Errorf("%w: unsupported Amplify ARN resource path: %s", ErrNotFound, resource)
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

// randomID generates a cryptographically random 12-character alphanumeric ID.
func randomID() string {
	return randomAppID()
}

// StartJob creates and starts a new deployment job for a branch.
func (b *InMemoryBackend) StartJob(
	appID, branchName, jobType, commitID, commitMsg string,
) (*Job, error) {
	b.mu.Lock("StartJob")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if !b.branches.Has(branchKey(appID, branchName)) {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	jobID := randomID()
	now := time.Now().UTC()

	jt := JobType(jobType)
	if jt == "" {
		jt = JobTypeRelease
	}

	job := &Job{
		JobID:      jobID,
		JobARN:     b.jobARN(appID, branchName, jobID),
		CommitID:   commitID,
		CommitMsg:  commitMsg,
		Status:     JobStatusRunning,
		Type:       jt,
		StartTime:  now,
		AppID:      appID,
		BranchName: branchName,
	}

	b.jobs.Put(job)

	cp := *job

	return &cp, nil
}

// jobARN builds the ARN for a deployment job. Real Amplify's JobSummary.JobArn
// is a required response field; every job created by this backend must carry
// one so the aws-sdk-go-v2 deserializer doesn't leave it nil.
func (b *InMemoryBackend) jobARN(appID, branchName, jobID string) string {
	return arn.Build(
		"amplify",
		b.region,
		b.accountID,
		fmt.Sprintf("apps/%s/branches/%s/jobs/%s", appID, branchName, jobID),
	)
}

// StopJob cancels a running job.
func (b *InMemoryBackend) StopJob(appID, branchName, jobID string) (*Job, error) {
	b.mu.Lock("StopJob")
	defer b.mu.Unlock()

	job, err := b.findJob(appID, branchName, jobID)
	if err != nil {
		return nil, err
	}

	job.Status = JobStatusCancelled
	job.EndTime = time.Now().UTC()

	cp := *job

	return &cp, nil
}

// GetJob returns a job by ID.
func (b *InMemoryBackend) GetJob(appID, branchName, jobID string) (*Job, error) {
	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	job, err := b.findJob(appID, branchName, jobID)
	if err != nil {
		return nil, err
	}

	cp := *job

	return &cp, nil
}

// ListJobs lists all jobs for a branch.
func (b *InMemoryBackend) ListJobs(
	appID, branchName, nextToken string,
	maxResults int,
) ([]*Job, string, error) {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	var all []*Job

	for _, job := range b.jobsByBranch.Get(branchKey(appID, branchName)) {
		cp := *job
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].JobID < all[j].JobID })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// DeleteJob deletes a job record.
func (b *InMemoryBackend) DeleteJob(appID, branchName, jobID string) (*Job, error) {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	job, err := b.findJob(appID, branchName, jobID)
	if err != nil {
		return nil, err
	}

	cp := *job
	b.jobs.Delete(jobKey(appID, branchName, jobID))

	return &cp, nil
}

// CreateDeployment creates a pre-signed upload URL for a manual deployment.
func (b *InMemoryBackend) CreateDeployment(appID, branchName string) (string, string, error) {
	b.mu.RLock("CreateDeployment")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return "", "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if !b.branches.Has(branchKey(appID, branchName)) {
		return "", "", fmt.Errorf(
			"%w: branch %s not found for app %s",
			ErrNotFound,
			branchName,
			appID,
		)
	}

	jobID := randomID()
	uploadURL := "https://s3.amazonaws.com/amplify-upload-" + appID + "/" + branchName + "/" + jobID + ".zip"

	return jobID, uploadURL, nil
}

// StartDeployment starts a deployment from a pre-uploaded artifact.
func (b *InMemoryBackend) StartDeployment(
	appID, branchName, jobID, sourceURL string,
) (*Job, error) {
	b.mu.Lock("StartDeployment")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if !b.branches.Has(branchKey(appID, branchName)) {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	if jobID == "" {
		jobID = randomID()
	}

	now := time.Now().UTC()

	job := &Job{
		JobID:      jobID,
		JobARN:     b.jobARN(appID, branchName, jobID),
		CommitID:   sourceURL,
		Status:     JobStatusRunning,
		Type:       JobTypeManual,
		StartTime:  now,
		AppID:      appID,
		BranchName: branchName,
	}

	b.jobs.Put(job)

	cp := *job

	return &cp, nil
}

// findJob locates a job in the jobs table. Must be called while holding a lock.
func (b *InMemoryBackend) findJob(appID, branchName, jobID string) (*Job, error) {
	job, ok := b.jobs.Get(jobKey(appID, branchName, jobID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: job %s not found for branch %s app %s",
			ErrNotFound,
			jobID,
			branchName,
			appID,
		)
	}

	return job, nil
}

// CreateDomainAssociation creates a custom domain association for an app.
func (b *InMemoryBackend) CreateDomainAssociation(
	appID, domainName string,
	subDomains []SubDomainSetting,
	enableAutoSubDomain bool,
) (*DomainAssociation, error) {
	b.mu.Lock("CreateDomainAssociation")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	key := domainKey(appID, domainName)
	if b.domains.Has(key) {
		return nil, fmt.Errorf(
			"%w: domain %s already exists for app %s",
			ErrAlreadyExists,
			domainName,
			appID,
		)
	}

	domARN := arn.Build(
		"amplify",
		b.region,
		b.accountID,
		fmt.Sprintf("apps/%s/domains/%s", appID, domainName),
	)

	subs := make([]SubDomain, 0, len(subDomains))

	for _, s := range subDomains {
		subs = append(subs, SubDomain{
			SubDomainSetting: s,
			Verified:         false,
			DNSRecord:        s.Prefix + "." + domainName + " CNAME " + appID + ".amplifyapp.com",
		})
	}

	da := &DomainAssociation{
		AppID:                            appID,
		DomainName:                       domainName,
		ARN:                              domARN,
		DomainStatus:                     DomainStatusPendingVerification,
		SubDomains:                       subs,
		EnableAutoSubDomain:              enableAutoSubDomain,
		CertificateVerificationDNSRecord: "_verify." + domainName + " CNAME _acm." + appID + ".amplifyapp.com",
	}

	b.domains.Put(da)

	cp := *da

	return &cp, nil
}

// UpdateDomainAssociation updates a domain association.
func (b *InMemoryBackend) UpdateDomainAssociation(
	appID, domainName string,
	subDomains []SubDomainSetting,
	enableAutoSubDomain bool,
) (*DomainAssociation, error) {
	b.mu.Lock("UpdateDomainAssociation")
	defer b.mu.Unlock()

	da, err := b.findDomain(appID, domainName)
	if err != nil {
		return nil, err
	}

	subs := make([]SubDomain, 0, len(subDomains))

	for _, s := range subDomains {
		subs = append(subs, SubDomain{
			SubDomainSetting: s,
			Verified:         false,
			DNSRecord:        s.Prefix + "." + domainName + " CNAME " + appID + ".amplifyapp.com",
		})
	}

	da.SubDomains = subs
	da.EnableAutoSubDomain = enableAutoSubDomain

	cp := *da

	return &cp, nil
}

// DeleteDomainAssociation deletes a domain association.
func (b *InMemoryBackend) DeleteDomainAssociation(
	appID, domainName string,
) (*DomainAssociation, error) {
	b.mu.Lock("DeleteDomainAssociation")
	defer b.mu.Unlock()

	da, err := b.findDomain(appID, domainName)
	if err != nil {
		return nil, err
	}

	cp := *da
	b.domains.Delete(domainKey(appID, domainName))

	return &cp, nil
}

// GetDomainAssociation returns a domain association.
func (b *InMemoryBackend) GetDomainAssociation(
	appID, domainName string,
) (*DomainAssociation, error) {
	b.mu.RLock("GetDomainAssociation")
	defer b.mu.RUnlock()

	da, err := b.findDomain(appID, domainName)
	if err != nil {
		return nil, err
	}

	cp := *da

	return &cp, nil
}

// ListDomainAssociations lists domain associations for an app.
func (b *InMemoryBackend) ListDomainAssociations(
	appID, nextToken string,
	maxResults int,
) ([]*DomainAssociation, string, error) {
	b.mu.RLock("ListDomainAssociations")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	var all []*DomainAssociation

	for _, da := range b.domainsByApp.Get(appID) {
		cp := *da
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].DomainName < all[j].DomainName })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// findDomain locates a domain association. Must be called while holding a lock.
func (b *InMemoryBackend) findDomain(appID, domainName string) (*DomainAssociation, error) {
	da, ok := b.domains.Get(domainKey(appID, domainName))
	if !ok {
		return nil, fmt.Errorf("%w: domain %s not found for app %s", ErrNotFound, domainName, appID)
	}

	return da, nil
}

// CreateWebhook creates a new webhook for an app branch.
func (b *InMemoryBackend) CreateWebhook(appID, branchName, description string) (*Webhook, error) {
	b.mu.Lock("CreateWebhook")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	webhookID := randomID()
	webhookARN := arn.Build(
		"amplify",
		b.region,
		b.accountID,
		fmt.Sprintf("apps/%s/webhooks/%s", appID, webhookID),
	)
	now := time.Now().UTC()

	wh := &Webhook{
		WebhookID:   webhookID,
		WebhookARN:  webhookARN,
		AppID:       appID,
		BranchName:  branchName,
		Description: description,
		WebhookURL: "https://webhooks.amplify." + b.region +
			".amazonaws.com/prod/webhooks?id=" + webhookID + "&token=" + randomID(),
		CreateTime: now,
		UpdateTime: now,
	}

	b.webhooks.Put(wh)

	cp := *wh

	return &cp, nil
}

// UpdateWebhook updates a webhook.
func (b *InMemoryBackend) UpdateWebhook(
	webhookID, branchName, description string,
) (*Webhook, error) {
	b.mu.Lock("UpdateWebhook")
	defer b.mu.Unlock()

	wh, ok := b.webhooks.Get(webhookID)
	if !ok {
		return nil, fmt.Errorf("%w: webhook %s not found", ErrNotFound, webhookID)
	}

	if branchName != "" {
		wh.BranchName = branchName
	}

	if description != "" {
		wh.Description = description
	}

	wh.UpdateTime = time.Now().UTC()

	cp := *wh

	return &cp, nil
}

// DeleteWebhook deletes a webhook.
func (b *InMemoryBackend) DeleteWebhook(webhookID string) (*Webhook, error) {
	b.mu.Lock("DeleteWebhook")
	defer b.mu.Unlock()

	wh, ok := b.webhooks.Get(webhookID)
	if !ok {
		return nil, fmt.Errorf("%w: webhook %s not found", ErrNotFound, webhookID)
	}

	cp := *wh
	b.webhooks.Delete(webhookID)

	return &cp, nil
}

// GetWebhook returns a webhook by ID.
func (b *InMemoryBackend) GetWebhook(webhookID string) (*Webhook, error) {
	b.mu.RLock("GetWebhook")
	defer b.mu.RUnlock()

	wh, ok := b.webhooks.Get(webhookID)
	if !ok {
		return nil, fmt.Errorf("%w: webhook %s not found", ErrNotFound, webhookID)
	}

	cp := *wh

	return &cp, nil
}

// ListWebhooks lists webhooks for an app.
func (b *InMemoryBackend) ListWebhooks(
	appID, nextToken string,
	maxResults int,
) ([]*Webhook, string, error) {
	b.mu.RLock("ListWebhooks")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	var all []*Webhook

	for _, wh := range b.webhooksByApp.Get(appID) {
		cp := *wh
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].WebhookID < all[j].WebhookID })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// CreateBackendEnvironment creates a new backend environment for an app.
func (b *InMemoryBackend) CreateBackendEnvironment(
	appID, environmentName, stackName, deploymentArtifacts string,
) (*BackendEnvironment, error) {
	b.mu.Lock("CreateBackendEnvironment")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	key := backendEnvKey(appID, environmentName)
	if b.backendEnvironments.Has(key) {
		return nil, fmt.Errorf(
			"%w: backend environment %s already exists for app %s",
			ErrAlreadyExists,
			environmentName,
			appID,
		)
	}

	envARN := arn.Build(
		"amplify", b.region, b.accountID,
		fmt.Sprintf("apps/%s/backendenvironments/%s", appID, environmentName),
	)
	now := time.Now().UTC()

	env := &BackendEnvironment{
		EnvironmentName:       environmentName,
		BackendEnvironmentARN: envARN,
		AppID:                 appID,
		StackName:             stackName,
		DeploymentArtifacts:   deploymentArtifacts,
		CreateTime:            now,
		UpdateTime:            now,
	}

	b.backendEnvironments.Put(env)

	cp := *env

	return &cp, nil
}

// GetBackendEnvironment returns a backend environment.
func (b *InMemoryBackend) GetBackendEnvironment(
	appID, environmentName string,
) (*BackendEnvironment, error) {
	b.mu.RLock("GetBackendEnvironment")
	defer b.mu.RUnlock()

	env, err := b.findBackendEnv(appID, environmentName)
	if err != nil {
		return nil, err
	}

	cp := *env

	return &cp, nil
}

// DeleteBackendEnvironment deletes a backend environment.
func (b *InMemoryBackend) DeleteBackendEnvironment(
	appID, environmentName string,
) (*BackendEnvironment, error) {
	b.mu.Lock("DeleteBackendEnvironment")
	defer b.mu.Unlock()

	env, err := b.findBackendEnv(appID, environmentName)
	if err != nil {
		return nil, err
	}

	cp := *env
	b.backendEnvironments.Delete(backendEnvKey(appID, environmentName))

	return &cp, nil
}

// ListBackendEnvironments lists backend environments for an app.
func (b *InMemoryBackend) ListBackendEnvironments(
	appID, nextToken string,
	maxResults int,
) ([]*BackendEnvironment, string, error) {
	b.mu.RLock("ListBackendEnvironments")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	var all []*BackendEnvironment

	for _, env := range b.backendEnvironmentsByApp.Get(appID) {
		cp := *env
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].EnvironmentName < all[j].EnvironmentName })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// findBackendEnv locates a backend environment. Must be called while holding a lock.
func (b *InMemoryBackend) findBackendEnv(
	appID, environmentName string,
) (*BackendEnvironment, error) {
	env, ok := b.backendEnvironments.Get(backendEnvKey(appID, environmentName))
	if !ok {
		return nil, fmt.Errorf(
			"%w: backend environment %s not found for app %s",
			ErrNotFound,
			environmentName,
			appID,
		)
	}

	return env, nil
}

// GenerateAccessLogs generates a presigned URL for access logs.
func (b *InMemoryBackend) GenerateAccessLogs(
	appID, domainName, startTime, endTime string,
) (string, error) {
	b.mu.RLock("GenerateAccessLogs")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	logURL := "https://s3.amazonaws.com/amplify-logs-" + appID + "/" + domainName +
		"/access-" + startTime + "-" + endTime + ".log"

	return logURL, nil
}

// GetArtifactURL returns the download URL for an artifact.
func (b *InMemoryBackend) GetArtifactURL(artifactID string) (string, string, error) {
	b.mu.RLock("GetArtifactURL")
	defer b.mu.RUnlock()

	artifact, ok := b.artifacts.Get(artifactID)
	if !ok {
		return "", "", fmt.Errorf("%w: artifact %s not found", ErrNotFound, artifactID)
	}

	url := "https://s3.amazonaws.com/amplify-artifacts/" + artifactID + "/" + artifact.ArtifactFileName

	return artifact.ArtifactType, url, nil
}

// ListArtifacts lists artifacts for a job.
func (b *InMemoryBackend) ListArtifacts(
	appID, _, _, nextToken string,
	maxResults int,
) ([]*Artifact, string, error) {
	b.mu.RLock("ListArtifacts")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	// For in-memory backend, return empty artifacts list (no actual build artifacts).
	page, token := amplifyPaginate([]*Artifact{}, nextToken, maxResults)

	return page, token, nil
}

// compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// amplifyPaginate applies token-based pagination to a sorted slice of pointers.
func amplifyPaginate[T any](all []*T, nextToken string, maxResults int) ([]*T, string) {
	const defaultLimit = 100

	startIdx := 0
	if nextToken != "" {
		if idx, err := strconv.Atoi(nextToken); err == nil && idx >= 0 {
			startIdx = idx
		}
	}

	if startIdx >= len(all) {
		return []*T{}, ""
	}

	limit := defaultLimit
	if maxResults > 0 {
		limit = maxResults
	}

	end := startIdx + limit

	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}

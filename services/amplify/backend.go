package amplify

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
	CreateApp(name, description, repository, platform string, tagMap map[string]string) (*App, error)
	GetApp(appID string) (*App, error)
	ListApps() ([]*App, error)
	DeleteApp(appID string) error
	CreateBranch(
		appID, branchName, description, stage string,
		enableAutoBuild bool,
		tagMap map[string]string,
	) (*Branch, error)
	GetBranch(appID, branchName string) (*Branch, error)
	ListBranches(appID string) ([]*Branch, error)
	DeleteBranch(appID, branchName string) error
	TagResource(resourceARN string, tagMap map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
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
type InMemoryBackend struct {
	apps      map[string]*App               // appID → app
	branches  map[string]map[string]*Branch // appID → branchName → branch
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory Amplify backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		apps:      make(map[string]*App),
		branches:  make(map[string]map[string]*Branch),
		mu:        lockmetrics.New("amplify"),
		accountID: accountID,
		region:    region,
	}
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

	b.apps[appID] = app

	cp := *app

	return &cp, nil
}

// GetApp returns an Amplify application by ID.
func (b *InMemoryBackend) GetApp(appID string) (*App, error) {
	b.mu.RLock("GetApp")
	defer b.mu.RUnlock()

	app, ok := b.apps[appID]
	if !ok {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	cp := *app

	return &cp, nil
}

// ListApps returns all Amplify applications.
func (b *InMemoryBackend) ListApps() ([]*App, error) {
	b.mu.RLock("ListApps")
	defer b.mu.RUnlock()

	out := make([]*App, 0, len(b.apps))
	for _, app := range b.apps {
		cp := *app
		out = append(out, &cp)
	}

	return out, nil
}

// DeleteApp deletes an Amplify application by ID.
func (b *InMemoryBackend) DeleteApp(appID string) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	if _, ok := b.apps[appID]; !ok {
		return fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	delete(b.apps, appID)
	delete(b.branches, appID)

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

	if _, ok := b.apps[appID]; !ok {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if b.branches[appID] == nil {
		b.branches[appID] = make(map[string]*Branch)
	}

	if _, exists := b.branches[appID][branchName]; exists {
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

	b.branches[appID][branchName] = branch

	cp := *branch

	return &cp, nil
}

// GetBranch returns a branch for an Amplify application.
func (b *InMemoryBackend) GetBranch(appID, branchName string) (*Branch, error) {
	b.mu.RLock("GetBranch")
	defer b.mu.RUnlock()

	branches, ok := b.branches[appID]
	if !ok {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	branch, ok := branches[branchName]
	if !ok {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	cp := *branch

	return &cp, nil
}

// ListBranches returns all branches for an Amplify application.
func (b *InMemoryBackend) ListBranches(appID string) ([]*Branch, error) {
	b.mu.RLock("ListBranches")
	defer b.mu.RUnlock()

	if _, ok := b.apps[appID]; !ok {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	branches := b.branches[appID]
	out := make([]*Branch, 0, len(branches))

	for _, branch := range branches {
		cp := *branch
		out = append(out, &cp)
	}

	return out, nil
}

// DeleteBranch deletes a branch from an Amplify application.
func (b *InMemoryBackend) DeleteBranch(appID, branchName string) error {
	b.mu.Lock("DeleteBranch")
	defer b.mu.Unlock()

	branches, ok := b.branches[appID]
	if !ok || branches[branchName] == nil {
		return fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	delete(branches, branchName)

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

		app, ok := b.apps[appID]
		if !ok {
			return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
		}

		return app.Tags, nil
	}

	// apps/{appID}/branches/{branchName}
	if len(resourceParts) == 4 && resourceParts[0] == arnResourceApps && resourceParts[2] == arnResourceBranches {
		appID := resourceParts[1]
		branchName := resourceParts[3]

		branches, ok := b.branches[appID]
		if !ok {
			return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
		}

		branch, ok := branches[branchName]
		if !ok {
			return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
		}

		return branch.Tags, nil
	}

	return nil, fmt.Errorf("%w: unsupported Amplify ARN resource path: %s", ErrNotFound, resource)
}

// compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

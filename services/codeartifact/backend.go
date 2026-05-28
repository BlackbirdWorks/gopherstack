package codeartifact

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// Domain represents an AWS CodeArtifact domain.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateDomain.
type Domain struct {
	CreatedTime    time.Time  `json:"createdTime"`
	Tags           *tags.Tags `json:"tags,omitempty"`
	Name           string     `json:"name"`
	ARN            string     `json:"arn"`
	EncryptionKey  string     `json:"encryptionKey,omitempty"`
	Owner          string     `json:"owner"`
	Region         string     `json:"region"`
	Status         string     `json:"status"`
	S3BucketARN    string     `json:"s3BucketArn,omitempty"`
	AssetSizeBytes int64      `json:"assetSizeBytes"`
}

// Repository represents an AWS CodeArtifact repository.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateRepository.
type Repository struct {
	CreatedTime          time.Time  `json:"createdTime"`
	Tags                 *tags.Tags `json:"tags,omitempty"`
	Name                 string     `json:"name"`
	ARN                  string     `json:"arn"`
	DomainName           string     `json:"domainName"`
	DomainOwner          string     `json:"domainOwner"`
	Description          string     `json:"description,omitempty"`
	AdministratorAccount string     `json:"administratorAccount"`
	Region               string     `json:"region"`
}

// PackageGroup represents an AWS CodeArtifact package group.
type PackageGroup struct {
	CreatedTime time.Time  `json:"createdTime"`
	Tags        *tags.Tags `json:"tags,omitempty"`
	ARN         string     `json:"arn"`
	DomainName  string     `json:"domainName"`
	DomainOwner string     `json:"domainOwner"`
	Pattern     string     `json:"pattern"`
	Description string     `json:"description,omitempty"`
	ContactInfo string     `json:"contactInfo,omitempty"`
}

// Package represents an AWS CodeArtifact package (without versions).
type Package struct {
	DomainName  string `json:"domainName"`
	DomainOwner string `json:"domainOwner"`
	Repository  string `json:"repository"`
	Format      string `json:"format"`
	Namespace   string `json:"namespace,omitempty"`
	Name        string `json:"name"`
}

// PackageVersion represents a single version of an AWS CodeArtifact package.
type PackageVersion struct {
	PublishedAt time.Time `json:"publishedAt"`
	DomainName  string    `json:"domainName"`
	Repository  string    `json:"repository"`
	Format      string    `json:"format"`
	Namespace   string    `json:"namespace,omitempty"`
	PackageName string    `json:"packageName"`
	Version     string    `json:"version"`
	Status      string    `json:"status"`
	Revision    string    `json:"revision"`
}

// ExternalConnection represents a connection of a repository to an external package source.
type ExternalConnection struct {
	ExternalConnectionName string `json:"externalConnectionName"`
	PackageFormat          string `json:"packageFormat"`
	Status                 string `json:"status"`
}

// RepositoryPermissionsPolicy represents a permissions policy attached to a repository.
type RepositoryPermissionsPolicy struct {
	Document    string `json:"document"`
	Revision    string `json:"revision"`
	ResourceARN string `json:"resourceArn"`
}

// DomainPermissionsPolicy represents a permissions policy attached to a domain.
type DomainPermissionsPolicy struct {
	Document    string `json:"document"`
	Revision    string `json:"revision"`
	ResourceARN string `json:"resourceArn"`
}

// InMemoryBackend is the in-memory store for CodeArtifact resources.
type InMemoryBackend struct {
	domains             map[string]*Domain
	repositories        map[string]*Repository                  // key: domainName/repoName
	packageGroups       map[string]*PackageGroup                // key: domainName/pattern
	packages            map[string]*Package                     // key: domainName/repoName/format/namespace/name
	packageVersions     map[string]*PackageVersion              // key: domainName/repoName/format/namespace/name/version
	externalConnections map[string][]ExternalConnection         // key: domainName/repoName
	repositoryPolicies  map[string]*RepositoryPermissionsPolicy // key: domainName/repoName
	domainPolicies      map[string]*DomainPermissionsPolicy     // key: domainName
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
}

// NewInMemoryBackend creates a new in-memory CodeArtifact backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		domains:             make(map[string]*Domain),
		repositories:        make(map[string]*Repository),
		packageGroups:       make(map[string]*PackageGroup),
		packages:            make(map[string]*Package),
		packageVersions:     make(map[string]*PackageVersion),
		externalConnections: make(map[string][]ExternalConnection),
		repositoryPolicies:  make(map[string]*RepositoryPermissionsPolicy),
		domainPolicies:      make(map[string]*DomainPermissionsPolicy),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("codeartifact"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateDomain creates a new CodeArtifact domain.
func (b *InMemoryBackend) CreateDomain(name, encryptionKey string, kv map[string]string) (*Domain, error) {
	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	if _, ok := b.domains[name]; ok {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrAlreadyExists, name)
	}

	domainARN := arn.Build("codeartifact", b.region, b.accountID, "domain/"+name)
	t := tags.New("codeartifact.domain." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	d := &Domain{
		Name:          name,
		ARN:           domainARN,
		EncryptionKey: encryptionKey,
		Owner:         b.accountID,
		Region:        b.region,
		Status:        "Active",
		S3BucketARN:   "arn:aws:s3:::assets-" + uuid.NewString()[:8],
		CreatedTime:   time.Now().UTC(),
		Tags:          t,
	}
	b.domains[name] = d
	cp := *d

	return &cp, nil
}

// DescribeDomain returns a domain by name.
func (b *InMemoryBackend) DescribeDomain(name string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	d, ok := b.domains[name]
	if !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, name)
	}
	cp := *d

	return &cp, nil
}

// ListDomains returns all domains sorted by name.
func (b *InMemoryBackend) ListDomains() []*Domain {
	b.mu.RLock("ListDomains")
	defer b.mu.RUnlock()

	list := make([]*Domain, 0, len(b.domains))
	for _, d := range b.domains {
		cp := *d
		list = append(list, &cp)
	}
	slices.SortFunc(list, func(a, b *Domain) int {
		return strings.Compare(a.Name, b.Name)
	})

	return list
}

// DeleteDomain deletes a domain by name, cascade-deleting all its repositories,
// packages, package versions, external connections, policies, and Tags.
func (b *InMemoryBackend) DeleteDomain(name string) (*Domain, error) {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	d, ok := b.domains[name]
	if !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, name)
	}
	cp := *d

	// Cascade: delete all repositories in this domain plus their dependents.
	for key, r := range b.repositories {
		if r.DomainName != name {
			continue
		}
		prefix := key + "/"
		for k := range b.packages {
			if strings.HasPrefix(k, prefix) {
				delete(b.packages, k)
			}
		}
		for k := range b.packageVersions {
			if strings.HasPrefix(k, prefix) {
				delete(b.packageVersions, k)
			}
		}
		delete(b.externalConnections, key)
		delete(b.repositoryPolicies, key)
		r.Tags.Close()
		delete(b.repositories, key)
	}

	delete(b.domainPolicies, name)
	delete(b.domains, name)
	d.Tags.Close()

	return &cp, nil
}

// Reset clears all stored resources, closing Tags on each domain, repository, and package group.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, d := range b.domains {
		d.Tags.Close()
	}
	for _, r := range b.repositories {
		r.Tags.Close()
	}
	for _, pg := range b.packageGroups {
		pg.Tags.Close()
	}

	b.domains = make(map[string]*Domain)
	b.repositories = make(map[string]*Repository)
	b.packageGroups = make(map[string]*PackageGroup)
	b.packages = make(map[string]*Package)
	b.packageVersions = make(map[string]*PackageVersion)
	b.externalConnections = make(map[string][]ExternalConnection)
	b.repositoryPolicies = make(map[string]*RepositoryPermissionsPolicy)
	b.domainPolicies = make(map[string]*DomainPermissionsPolicy)
}

// repoKey returns the map key for a repository.
func repoKey(domainName, repoName string) string {
	return domainName + "/" + repoName
}

// CreateRepository creates a new CodeArtifact repository.
func (b *InMemoryBackend) CreateRepository(
	domainName, repoName, description string,
	kv map[string]string,
) (*Repository, error) {
	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	key := repoKey(domainName, repoName)
	if _, ok := b.repositories[key]; ok {
		return nil, fmt.Errorf("%w: repository %s already exists in domain %s", ErrAlreadyExists, repoName, domainName)
	}

	repoARN := arn.Build("codeartifact", b.region, b.accountID, "repository/"+domainName+"/"+repoName)
	t := tags.New("codeartifact.repository." + key + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	r := &Repository{
		Name:                 repoName,
		ARN:                  repoARN,
		DomainName:           domainName,
		DomainOwner:          b.accountID,
		Description:          description,
		AdministratorAccount: b.accountID,
		Region:               b.region,
		CreatedTime:          time.Now().UTC(),
		Tags:                 t,
	}
	b.repositories[key] = r
	cp := *r

	return &cp, nil
}

// DescribeRepository returns a repository by domain and name.
func (b *InMemoryBackend) DescribeRepository(domainName, repoName string) (*Repository, error) {
	b.mu.RLock("DescribeRepository")
	defer b.mu.RUnlock()

	r, ok := b.repositories[repoKey(domainName, repoName)]
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}
	cp := *r

	return &cp, nil
}

// ListRepositoriesInDomain returns all repositories in a domain, sorted by name.
// Returns ErrNotFound if the domain does not exist.
func (b *InMemoryBackend) ListRepositoriesInDomain(domainName string) ([]*Repository, error) {
	b.mu.RLock("ListRepositoriesInDomain")
	defer b.mu.RUnlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	list := make([]*Repository, 0, len(b.repositories))
	for _, r := range b.repositories {
		if r.DomainName == domainName {
			cp := *r
			list = append(list, &cp)
		}
	}
	slices.SortFunc(list, func(a, b *Repository) int {
		return strings.Compare(a.Name, b.Name)
	})

	return list, nil
}

// ListRepositories returns all repositories across all domains, sorted by name.
func (b *InMemoryBackend) ListRepositories() []*Repository {
	b.mu.RLock("ListRepositories")
	defer b.mu.RUnlock()

	list := make([]*Repository, 0, len(b.repositories))
	for _, r := range b.repositories {
		cp := *r
		list = append(list, &cp)
	}
	slices.SortFunc(list, func(a, b *Repository) int {
		return strings.Compare(a.Name, b.Name)
	})

	return list
}

// DeleteRepository deletes a repository by domain and name, cascade-deleting all
// its packages, package versions, external connections, permissions policy, and Tags.
func (b *InMemoryBackend) DeleteRepository(domainName, repoName string) (*Repository, error) {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	key := repoKey(domainName, repoName)
	r, ok := b.repositories[key]
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}
	cp := *r

	prefix := key + "/"
	for k := range b.packages {
		if strings.HasPrefix(k, prefix) {
			delete(b.packages, k)
		}
	}
	for k := range b.packageVersions {
		if strings.HasPrefix(k, prefix) {
			delete(b.packageVersions, k)
		}
	}
	delete(b.externalConnections, key)
	delete(b.repositoryPolicies, key)
	delete(b.repositories, key)
	r.Tags.Close()

	return &cp, nil
}

// TagResource adds or replaces tags on a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, d := range b.domains {
		if d.ARN == resourceARN {
			d.Tags.Merge(kv)

			return nil
		}
	}
	for _, r := range b.repositories {
		if r.ARN == resourceARN {
			r.Tags.Merge(kv)

			return nil
		}
	}
	for _, pg := range b.packageGroups {
		if pg.ARN == resourceARN {
			pg.Tags.Merge(kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, d := range b.domains {
		if d.ARN == resourceARN {
			d.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}
	for _, r := range b.repositories {
		if r.ARN == resourceARN {
			r.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}
	for _, pg := range b.packageGroups {
		if pg.ARN == resourceARN {
			pg.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListTagsForResource returns tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, d := range b.domains {
		if d.ARN == resourceARN {
			return d.Tags.Clone(), nil
		}
	}
	for _, r := range b.repositories {
		if r.ARN == resourceARN {
			return r.Tags.Clone(), nil
		}
	}
	for _, pg := range b.packageGroups {
		if pg.ARN == resourceARN {
			return pg.Tags.Clone(), nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// --- Package group methods ---

// packageGroupKey returns the map key for a package group.
func packageGroupKey(domainName, pattern string) string {
	return domainName + "/" + pattern
}

// CreatePackageGroup creates a new CodeArtifact package group.
func (b *InMemoryBackend) CreatePackageGroup(
	domainName, pattern, description, contactInfo string,
	kv map[string]string,
) (*PackageGroup, error) {
	b.mu.Lock("CreatePackageGroup")
	defer b.mu.Unlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	key := packageGroupKey(domainName, pattern)
	if _, ok := b.packageGroups[key]; ok {
		return nil, fmt.Errorf(
			"%w: package group %s already exists in domain %s",
			ErrAlreadyExists,
			pattern,
			domainName,
		)
	}

	pgARN := arn.Build("codeartifact", b.region, b.accountID, "package-group/"+domainName+pattern)
	t := tags.New("codeartifact.package-group." + key + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	pg := &PackageGroup{
		ARN:         pgARN,
		DomainName:  domainName,
		DomainOwner: b.accountID,
		Pattern:     pattern,
		Description: description,
		ContactInfo: contactInfo,
		CreatedTime: time.Now().UTC(),
		Tags:        t,
	}
	b.packageGroups[key] = pg
	cp := *pg

	return &cp, nil
}

// DescribePackageGroup returns a package group by domain and pattern.
func (b *InMemoryBackend) DescribePackageGroup(domainName, pattern string) (*PackageGroup, error) {
	b.mu.RLock("DescribePackageGroup")
	defer b.mu.RUnlock()

	pg, ok := b.packageGroups[packageGroupKey(domainName, pattern)]
	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found in domain %s", ErrNotFound, pattern, domainName)
	}
	cp := *pg

	return &cp, nil
}

// DeletePackageGroup deletes a package group by domain and pattern.
func (b *InMemoryBackend) DeletePackageGroup(domainName, pattern string) (*PackageGroup, error) {
	b.mu.Lock("DeletePackageGroup")
	defer b.mu.Unlock()

	key := packageGroupKey(domainName, pattern)
	pg, ok := b.packageGroups[key]
	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found in domain %s", ErrNotFound, pattern, domainName)
	}
	cp := *pg
	delete(b.packageGroups, key)
	pg.Tags.Close()

	return &cp, nil
}

// --- Package methods ---

// packageKey returns the map key for a package.
func packageKey(domainName, repoName, format, namespace, name string) string {
	return domainName + "/" + repoName + "/" + format + "/" + namespace + "/" + name
}

// DescribePackage returns a package by domain, repository, format, namespace, and name.
// If the package does not already exist in the store, a stub entry is created on the fly so
// that callers (e.g. Terraform providers) can always retrieve metadata about packages that
// were published directly to the repository.
func (b *InMemoryBackend) DescribePackage(domainName, repoName, format, namespace, name string) (*Package, error) {
	b.mu.Lock("DescribePackage")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoKey(domainName, repoName)]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	key := packageKey(domainName, repoName, format, namespace, name)
	pkg, ok := b.packages[key]
	if !ok {
		// Auto-create a stub package entry.
		pkg = &Package{
			DomainName:  domainName,
			DomainOwner: b.accountID,
			Repository:  repoName,
			Format:      format,
			Namespace:   namespace,
			Name:        name,
		}
		b.packages[key] = pkg
	}
	cp := *pkg

	return &cp, nil
}

// DeletePackage deletes a package and all its versions from a repository.
func (b *InMemoryBackend) DeletePackage(domainName, repoName, format, namespace, name string) (*Package, error) {
	b.mu.Lock("DeletePackage")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoKey(domainName, repoName)]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	key := packageKey(domainName, repoName, format, namespace, name)
	pkg, ok := b.packages[key]
	if !ok {
		return nil, fmt.Errorf("%w: package %s not found", ErrNotFound, name)
	}
	cp := *pkg
	delete(b.packages, key)

	// Remove all associated package versions.
	prefix := key + "/"
	for k := range b.packageVersions {
		if strings.HasPrefix(k, prefix) {
			delete(b.packageVersions, k)
		}
	}

	return &cp, nil
}

// --- Package version methods ---

// packageVersionKey returns the map key for a package version.
func packageVersionKey(domainName, repoName, format, namespace, name, version string) string {
	return packageKey(domainName, repoName, format, namespace, name) + "/" + version
}

// DescribePackageVersion returns a specific version of a package.
// As with DescribePackage, stub entries are created on demand.
func (b *InMemoryBackend) DescribePackageVersion(
	domainName, repoName, format, namespace, name, version string,
) (*PackageVersion, error) {
	b.mu.Lock("DescribePackageVersion")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoKey(domainName, repoName)]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	vKey := packageVersionKey(domainName, repoName, format, namespace, name, version)
	pv, ok := b.packageVersions[vKey]
	if !ok {
		// Auto-create a stub version entry.
		pv = &PackageVersion{
			DomainName:  domainName,
			Repository:  repoName,
			Format:      format,
			Namespace:   namespace,
			PackageName: name,
			Version:     version,
			Status:      "Published",
			PublishedAt: time.Now().UTC(),
			Revision:    uuid.NewString()[:8],
		}
		b.packageVersions[vKey] = pv

		// Ensure the parent package record exists too.
		pKey := packageKey(domainName, repoName, format, namespace, name)
		if _, exists := b.packages[pKey]; !exists {
			b.packages[pKey] = &Package{
				DomainName:  domainName,
				DomainOwner: b.accountID,
				Repository:  repoName,
				Format:      format,
				Namespace:   namespace,
				Name:        name,
			}
		}
	}
	cp := *pv

	return &cp, nil
}

// DeletePackageVersions deletes specified versions of a package and returns a
// map of version→errorCode for any versions that could not be deleted.
func (b *InMemoryBackend) DeletePackageVersions(
	domainName, repoName, format, namespace, name string,
	versions []string,
) (map[string]string, error) {
	b.mu.Lock("DeletePackageVersions")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoKey(domainName, repoName)]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	failed := make(map[string]string)
	for _, v := range versions {
		vKey := packageVersionKey(domainName, repoName, format, namespace, name, v)
		if _, ok := b.packageVersions[vKey]; !ok {
			failed[v] = "RESOURCE_NOT_FOUND"

			continue
		}
		delete(b.packageVersions, vKey)
	}

	return failed, nil
}

// CopyPackageVersions copies specified package versions from a source repository
// to a destination repository in the same domain.
func (b *InMemoryBackend) CopyPackageVersions(
	domainName, srcRepo, dstRepo, format, namespace, name string,
	versions []string,
) (map[string]string, error) {
	b.mu.Lock("CopyPackageVersions")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoKey(domainName, srcRepo)]; !ok {
		return nil, fmt.Errorf("%w: source repository %s not found in domain %s", ErrNotFound, srcRepo, domainName)
	}
	if _, ok := b.repositories[repoKey(domainName, dstRepo)]; !ok {
		return nil, fmt.Errorf("%w: destination repository %s not found in domain %s", ErrNotFound, dstRepo, domainName)
	}

	failed := make(map[string]string)
	for _, v := range versions {
		srcKey := packageVersionKey(domainName, srcRepo, format, namespace, name, v)
		src, ok := b.packageVersions[srcKey]
		if !ok {
			failed[v] = "RESOURCE_NOT_FOUND"

			continue
		}
		dstKey := packageVersionKey(domainName, dstRepo, format, namespace, name, v)
		if _, exists := b.packageVersions[dstKey]; exists {
			failed[v] = "ALREADY_EXISTS"

			continue
		}
		copied := *src
		copied.Repository = dstRepo
		b.packageVersions[dstKey] = &copied
		// Ensure destination package record exists.
		dstPkgKey := packageKey(domainName, dstRepo, format, namespace, name)
		if _, exists := b.packages[dstPkgKey]; !exists {
			b.packages[dstPkgKey] = &Package{
				DomainName:  domainName,
				DomainOwner: b.accountID,
				Repository:  dstRepo,
				Format:      format,
				Namespace:   namespace,
				Name:        name,
			}
		}
	}

	return failed, nil
}

// --- External connection methods ---

// externalConnectionFormat derives the package format from a connection name.
func externalConnectionFormat(connectionName string) string {
	switch connectionName {
	case "public:npmjs":
		return "npm"
	case "public:pypi":
		return "pypi"
	case "public:maven-central", "public:maven-commonsware", "public:maven-googleandroid",
		"public:maven-gradleplugins", "public:maven-apacheorg":
		return "maven"
	case "public:nuget-org":
		return "nuget"
	case "public:crates-io":
		return "cargo"
	default:
		return "generic"
	}
}

// AssociateExternalConnection associates an external connection with a repository.
func (b *InMemoryBackend) AssociateExternalConnection(
	domainName, repoName, connectionName string,
) (*Repository, error) {
	b.mu.Lock("AssociateExternalConnection")
	defer b.mu.Unlock()

	r, ok := b.repositories[repoKey(domainName, repoName)]
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	key := repoKey(domainName, repoName)
	for _, ec := range b.externalConnections[key] {
		if ec.ExternalConnectionName == connectionName {
			return nil, fmt.Errorf("%w: external connection %s already associated", ErrAlreadyExists, connectionName)
		}
	}

	b.externalConnections[key] = append(b.externalConnections[key], ExternalConnection{
		ExternalConnectionName: connectionName,
		PackageFormat:          externalConnectionFormat(connectionName),
		Status:                 "AVAILABLE",
	})
	cp := *r

	return &cp, nil
}

// --- Repository permissions policy methods ---

// DeleteRepositoryPermissionsPolicy removes the permissions policy from a repository.
func (b *InMemoryBackend) DeleteRepositoryPermissionsPolicy(
	domainName, repoName string,
) (*RepositoryPermissionsPolicy, error) {
	b.mu.Lock("DeleteRepositoryPermissionsPolicy")
	defer b.mu.Unlock()

	if _, ok := b.repositories[repoKey(domainName, repoName)]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	key := repoKey(domainName, repoName)
	pol, ok := b.repositoryPolicies[key]
	if !ok {
		return nil, fmt.Errorf("%w: no permissions policy found for repository %s", ErrNotFound, repoName)
	}
	cp := *pol
	delete(b.repositoryPolicies, key)

	return &cp, nil
}

// PutRepositoryPermissionsPolicy stores a permissions policy for a repository.
func (b *InMemoryBackend) PutRepositoryPermissionsPolicy(
	domainName, repoName, document string,
) (*RepositoryPermissionsPolicy, error) {
	b.mu.Lock("PutRepositoryPermissionsPolicy")
	defer b.mu.Unlock()

	r, ok := b.repositories[repoKey(domainName, repoName)]
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	key := repoKey(domainName, repoName)
	pol := &RepositoryPermissionsPolicy{
		Document:    document,
		Revision:    uuid.NewString()[:8],
		ResourceARN: r.ARN,
	}
	b.repositoryPolicies[key] = pol
	cp := *pol

	return &cp, nil
}

// GetRepositoryPermissionsPolicy retrieves the permissions policy for a repository.
func (b *InMemoryBackend) GetRepositoryPermissionsPolicy(
	domainName, repoName string,
) (*RepositoryPermissionsPolicy, error) {
	b.mu.RLock("GetRepositoryPermissionsPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.repositories[repoKey(domainName, repoName)]; !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	key := repoKey(domainName, repoName)
	pol, ok := b.repositoryPolicies[key]
	if !ok {
		return nil, fmt.Errorf("%w: no permissions policy found for repository %s", ErrNotFound, repoName)
	}
	cp := *pol

	return &cp, nil
}

// --- Domain permissions policy methods ---

// GetDomainPermissionsPolicy retrieves the permissions policy for a domain.
// Returns ErrNotFound if the domain does not exist or if no policy has been set.
func (b *InMemoryBackend) GetDomainPermissionsPolicy(domainName string) (*DomainPermissionsPolicy, error) {
	b.mu.RLock("GetDomainPermissionsPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	pol, ok := b.domainPolicies[domainName]
	if !ok {
		return nil, fmt.Errorf("%w: no permissions policy found for domain %s", ErrNotFound, domainName)
	}
	cp := *pol

	return &cp, nil
}

// PutDomainPermissionsPolicy stores a permissions policy for a domain.
func (b *InMemoryBackend) PutDomainPermissionsPolicy(domainName, document string) (*DomainPermissionsPolicy, error) {
	b.mu.Lock("PutDomainPermissionsPolicy")
	defer b.mu.Unlock()

	d, ok := b.domains[domainName]
	if !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	pol := &DomainPermissionsPolicy{
		Document:    document,
		Revision:    uuid.NewString()[:8],
		ResourceARN: d.ARN,
	}
	b.domainPolicies[domainName] = pol
	cp := *pol

	return &cp, nil
}

// DeleteDomainPermissionsPolicy removes the permissions policy from a domain.
// Returns ErrNotFound if the domain does not exist or if no policy has been set.
func (b *InMemoryBackend) DeleteDomainPermissionsPolicy(domainName string) (*DomainPermissionsPolicy, error) {
	b.mu.Lock("DeleteDomainPermissionsPolicy")
	defer b.mu.Unlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	pol, ok := b.domainPolicies[domainName]
	if !ok {
		return nil, fmt.Errorf("%w: no permissions policy found for domain %s", ErrNotFound, domainName)
	}
	cp := *pol
	delete(b.domainPolicies, domainName)

	return &cp, nil
}

// DisassociateExternalConnection removes an external connection from a repository.
func (b *InMemoryBackend) DisassociateExternalConnection(
	domainName, repoName, connectionName string,
) (*Repository, error) {
	b.mu.Lock("DisassociateExternalConnection")
	defer b.mu.Unlock()

	key := repoKey(domainName, repoName)
	if _, ok := b.repositories[key]; !ok {
		return nil, fmt.Errorf("%w: repository %s/%s not found", ErrNotFound, domainName, repoName)
	}

	conns := b.externalConnections[key]
	filtered := conns[:0]

	for _, c := range conns {
		if c.ExternalConnectionName != connectionName {
			filtered = append(filtered, c)
		}
	}

	b.externalConnections[key] = filtered
	cp := *b.repositories[key]

	return &cp, nil
}

// DisposePackageVersions moves specified versions of a package to the Disposed status.
func (b *InMemoryBackend) DisposePackageVersions(
	domainName, repoName, format, namespace, name string,
	versions []string,
) (map[string]string, error) {
	b.mu.Lock("DisposePackageVersions")
	defer b.mu.Unlock()

	results := make(map[string]string, len(versions))

	for _, v := range versions {
		key := packageVersionKey(domainName, repoName, format, namespace, name, v)
		if pv, ok := b.packageVersions[key]; ok {
			pv.Status = "Disposed"
			results[v] = "SUCCESS"
		} else {
			results[v] = "NOT_FOUND"
		}
	}

	return results, nil
}

// GetAssociatedPackageGroup returns the most specific package group associated with a package.
func (b *InMemoryBackend) GetAssociatedPackageGroup(
	domainName, format, namespace, name string,
) (*PackageGroup, error) {
	b.mu.RLock("GetAssociatedPackageGroup")
	defer b.mu.RUnlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	_ = format
	_ = namespace
	_ = name

	// Return nil if no group matches (not an error per AWS API).
	return nil, nil //nolint:nilnil // AWS returns no error when no group matches
}

// ListPackageGroups returns all package groups in a domain, optionally filtered by prefix.
func (b *InMemoryBackend) ListPackageGroups(domainName, prefix string) ([]*PackageGroup, error) {
	b.mu.RLock("ListPackageGroups")
	defer b.mu.RUnlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	result := make([]*PackageGroup, 0, len(b.packageGroups))

	for _, pg := range b.packageGroups {
		if pg.DomainName != domainName {
			continue
		}

		if prefix != "" && !strings.HasPrefix(pg.Pattern, prefix) {
			continue
		}

		cp := *pg
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Pattern < result[j].Pattern
	})

	return result, nil
}

// ListSubPackageGroups returns sub-package groups of a given package group pattern.
func (b *InMemoryBackend) ListSubPackageGroups(domainName, pattern string) ([]*PackageGroup, error) {
	b.mu.RLock("ListSubPackageGroups")
	defer b.mu.RUnlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	result := make([]*PackageGroup, 0, len(b.packageGroups))

	parentRoot := strings.TrimSuffix(pattern, "*")

	for _, pg := range b.packageGroups {
		if pg.DomainName != domainName {
			continue
		}

		if pg.Pattern == pattern || !strings.HasPrefix(pg.Pattern, parentRoot) {
			continue
		}

		cp := *pg
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Pattern < result[j].Pattern
	})

	return result, nil
}

// UpdatePackageGroup updates description or contact info of a package group.
func (b *InMemoryBackend) UpdatePackageGroup(
	domainName, pattern, description, contactInfo string,
) (*PackageGroup, error) {
	b.mu.Lock("UpdatePackageGroup")
	defer b.mu.Unlock()

	key := packageGroupKey(domainName, pattern)
	pg, ok := b.packageGroups[key]

	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found", ErrNotFound, pattern)
	}

	if description != "" {
		pg.Description = description
	}

	if contactInfo != "" {
		pg.ContactInfo = contactInfo
	}

	cp := *pg

	return &cp, nil
}

// UpdatePackageGroupOriginConfiguration is a stub that accepts origin config changes.
func (b *InMemoryBackend) UpdatePackageGroupOriginConfiguration(
	domainName, pattern string,
) (*PackageGroup, error) {
	b.mu.RLock("UpdatePackageGroupOriginConfiguration")
	defer b.mu.RUnlock()

	key := packageGroupKey(domainName, pattern)
	pg, ok := b.packageGroups[key]

	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found", ErrNotFound, pattern)
	}

	cp := *pg

	return &cp, nil
}

// ListAllowedRepositoriesForGroup is a stub returning allowed repositories for a package group.
func (b *InMemoryBackend) ListAllowedRepositoriesForGroup(
	domainName, pattern string,
) ([]string, error) {
	b.mu.RLock("ListAllowedRepositoriesForGroup")
	defer b.mu.RUnlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	_ = pattern

	return []string{}, nil
}

// ListAssociatedPackages lists packages associated with a package group.
func (b *InMemoryBackend) ListAssociatedPackages(domainName, pattern string) ([]*Package, error) {
	b.mu.RLock("ListAssociatedPackages")
	defer b.mu.RUnlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	_ = pattern

	return []*Package{}, nil
}

// ListPackages lists packages in a repository.
func (b *InMemoryBackend) ListPackages(domainName, repoName, format, namespace string) ([]*Package, error) {
	b.mu.RLock("ListPackages")
	defer b.mu.RUnlock()

	key := repoKey(domainName, repoName)
	if _, ok := b.repositories[key]; !ok {
		return nil, fmt.Errorf("%w: repository %s/%s not found", ErrNotFound, domainName, repoName)
	}

	result := make([]*Package, 0, len(b.packages))

	for _, pv := range b.packageVersions {
		if pv.DomainName != domainName || pv.Repository != repoName {
			continue
		}

		if format != "" && pv.Format != format {
			continue
		}

		if namespace != "" && pv.Namespace != namespace {
			continue
		}

		// Deduplicate by package name.
		found := false

		for _, existing := range result {
			if existing.Name == pv.PackageName && existing.Format == pv.Format && existing.Namespace == pv.Namespace {
				found = true

				break
			}
		}

		if !found {
			result = append(result, &Package{
				DomainName:  domainName,
				DomainOwner: b.accountID,
				Repository:  repoName,
				Format:      pv.Format,
				Namespace:   pv.Namespace,
				Name:        pv.PackageName,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// ListPackageVersions lists all versions of a package in a repository.
func (b *InMemoryBackend) ListPackageVersions(
	domainName, repoName, format, namespace, name string,
) ([]*PackageVersion, error) {
	b.mu.RLock("ListPackageVersions")
	defer b.mu.RUnlock()

	key := repoKey(domainName, repoName)
	if _, ok := b.repositories[key]; !ok {
		return nil, fmt.Errorf("%w: repository %s/%s not found", ErrNotFound, domainName, repoName)
	}

	result := make([]*PackageVersion, 0, len(b.packageVersions))

	for _, pv := range b.packageVersions {
		if pv.DomainName != domainName || pv.Repository != repoName {
			continue
		}

		if format != "" && pv.Format != format {
			continue
		}

		if namespace != "" && pv.Namespace != namespace {
			continue
		}

		if name != "" && pv.PackageName != name {
			continue
		}

		cp := *pv
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Version < result[j].Version
	})

	return result, nil
}

// ListPackageVersionAssets is a stub returning asset list for a package version.
func (b *InMemoryBackend) ListPackageVersionAssets(
	domainName, repoName, format, namespace, name, version string,
) ([]map[string]any, error) {
	b.mu.RLock("ListPackageVersionAssets")
	defer b.mu.RUnlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)
	if _, ok := b.packageVersions[key]; !ok {
		return nil, fmt.Errorf("%w: package version not found", ErrNotFound)
	}

	return []map[string]any{}, nil
}

// ListPackageVersionDependencies is a stub returning empty dependencies.
func (b *InMemoryBackend) ListPackageVersionDependencies(
	domainName, repoName, format, namespace, name, version string,
) ([]map[string]any, error) {
	b.mu.RLock("ListPackageVersionDependencies")
	defer b.mu.RUnlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)
	if _, ok := b.packageVersions[key]; !ok {
		return nil, fmt.Errorf("%w: package version not found", ErrNotFound)
	}

	return []map[string]any{}, nil
}

// GetPackageVersionAsset is a stub that returns empty asset data.
func (b *InMemoryBackend) GetPackageVersionAsset(
	domainName, repoName, format, namespace, name, version, asset string,
) ([]byte, error) {
	b.mu.RLock("GetPackageVersionAsset")
	defer b.mu.RUnlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)
	if _, ok := b.packageVersions[key]; !ok {
		return nil, fmt.Errorf("%w: package version not found", ErrNotFound)
	}

	_ = asset

	return []byte{}, nil
}

// GetPackageVersionReadme is a stub that returns empty README content.
func (b *InMemoryBackend) GetPackageVersionReadme(
	domainName, repoName, format, namespace, name, version string,
) (string, error) {
	b.mu.RLock("GetPackageVersionReadme")
	defer b.mu.RUnlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)
	if _, ok := b.packageVersions[key]; !ok {
		return "", fmt.Errorf("%w: package version not found", ErrNotFound)
	}

	return "", nil
}

// PublishPackageVersion creates or updates a package version in the backend.
func (b *InMemoryBackend) PublishPackageVersion(
	domainName, repoName, format, namespace, name, version string,
) (*PackageVersion, error) {
	b.mu.Lock("PublishPackageVersion")
	defer b.mu.Unlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)

	if existing, ok := b.packageVersions[key]; ok {
		cp := *existing

		return &cp, nil
	}

	pv := &PackageVersion{
		DomainName:  domainName,
		Repository:  repoName,
		Format:      format,
		Namespace:   namespace,
		PackageName: name,
		Version:     version,
		Status:      "Published",
		Revision:    uuid.NewString()[:8],
		PublishedAt: time.Now().UTC(),
	}
	b.packageVersions[key] = pv

	cp := *pv

	return &cp, nil
}

// UpdatePackageVersionsStatus updates the status of specified package versions.
func (b *InMemoryBackend) UpdatePackageVersionsStatus(
	domainName, repoName, format, namespace, name, targetStatus string,
	versions []string,
) (map[string]string, error) {
	b.mu.Lock("UpdatePackageVersionsStatus")
	defer b.mu.Unlock()

	results := make(map[string]string, len(versions))

	for _, v := range versions {
		key := packageVersionKey(domainName, repoName, format, namespace, name, v)
		if pv, ok := b.packageVersions[key]; ok {
			pv.Status = targetStatus
			results[v] = "SUCCESS"
		} else {
			results[v] = "NOT_FOUND"
		}
	}

	return results, nil
}

// PutPackageOriginConfiguration is a stub accepting package origin configuration.
func (b *InMemoryBackend) PutPackageOriginConfiguration(
	domainName, repoName, format, namespace, name string,
) (*Package, error) {
	b.mu.RLock("PutPackageOriginConfiguration")
	defer b.mu.RUnlock()

	key := repoKey(domainName, repoName)
	if _, ok := b.repositories[key]; !ok {
		return nil, fmt.Errorf("%w: repository %s/%s not found", ErrNotFound, domainName, repoName)
	}

	return &Package{
		DomainName:  domainName,
		DomainOwner: b.accountID,
		Repository:  repoName,
		Format:      format,
		Namespace:   namespace,
		Name:        name,
	}, nil
}

// UpdateRepository updates repository description or upstreams.
func (b *InMemoryBackend) UpdateRepository(domainName, repoName, description string) (*Repository, error) {
	b.mu.Lock("UpdateRepository")
	defer b.mu.Unlock()

	key := repoKey(domainName, repoName)
	repo, ok := b.repositories[key]

	if !ok {
		return nil, fmt.Errorf("%w: repository %s/%s not found", ErrNotFound, domainName, repoName)
	}

	if description != "" {
		repo.Description = description
	}

	cp := *repo

	return &cp, nil
}

// --- Additional query methods ---

// CountRepositoriesInDomain returns the number of repositories in a domain.
func (b *InMemoryBackend) CountRepositoriesInDomain(domainName string) int {
	b.mu.RLock("CountRepositoriesInDomain")
	defer b.mu.RUnlock()

	count := 0
	for _, r := range b.repositories {
		if r.DomainName == domainName {
			count++
		}
	}

	return count
}

// GetExternalConnections returns a copy of the external connections for a repository.
func (b *InMemoryBackend) GetExternalConnections(domainName, repoName string) []ExternalConnection {
	b.mu.RLock("GetExternalConnections")
	defer b.mu.RUnlock()

	key := repoKey(domainName, repoName)
	conns := b.externalConnections[key]
	result := make([]ExternalConnection, len(conns))
	copy(result, conns)

	return result
}

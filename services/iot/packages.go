package iot

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// AssociateSbomWithPackageVersion associates an SBOM with a package version.
func (b *InMemoryBackend) AssociateSbomWithPackageVersion(
	input *AssociateSbomWithPackageVersionInput,
) (*AssociateSbomWithPackageVersionOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := packageVersionKey(input.PackageName, input.VersionName)
	b.packageVersionSboms[key] = input.Sbom

	result := computeSbomValidationResult(input.Sbom)
	b.sbomValidationResults[key] = []*SbomValidationResult{result}

	return &AssociateSbomWithPackageVersionOutput{
		PackageName:          input.PackageName,
		VersionName:          input.VersionName,
		Sbom:                 input.Sbom,
		SbomValidationStatus: result.ValidationResult,
	}, nil
}

// IoTPackage represents an AWS IoT software package.
//
//nolint:revive // IoTPackage is intentional to avoid conflict with the builtin package keyword
type IoTPackage struct {
	Tags               map[string]string `json:"tags,omitempty"`
	PackageARN         string            `json:"packageArn"`
	PackageName        string            `json:"packageName"`
	Description        string            `json:"description,omitempty"`
	DefaultVersionName string            `json:"defaultVersionName,omitempty"`
	CreationDate       float64           `json:"creationDate,omitempty"`
	LastModifiedDate   float64           `json:"lastModifiedDate,omitempty"`
}

func cloneIoTPackage(p *IoTPackage) *IoTPackage {
	cp := *p
	cp.Tags = make(map[string]string, len(p.Tags))
	maps.Copy(cp.Tags, p.Tags)

	return &cp
}

func (b *InMemoryBackend) packageARN(name string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("package/%s", name))
}

func (b *InMemoryBackend) CreateIoTPackage(name, description string, tags map[string]string) (*IoTPackage, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.iotPackages.Has(name) {
		return nil, fmt.Errorf("package %q already exists: %w", name, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	p := &IoTPackage{
		PackageName:      name,
		PackageARN:       b.packageARN(name),
		Description:      description,
		Tags:             make(map[string]string),
		CreationDate:     now,
		LastModifiedDate: now,
	}
	maps.Copy(p.Tags, tags)
	b.iotPackages.Put(p)
	b.putResourceTagsLocked(p.PackageARN, p.Tags)

	return cloneIoTPackage(p), nil
}

func (b *InMemoryBackend) GetIoTPackage(name string) (*IoTPackage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	p, ok := b.iotPackages.Get(name)
	if !ok {
		return nil, fmt.Errorf("package %q not found: %w", name, ErrResourceNotFound)
	}

	return cloneIoTPackage(p), nil
}

func (b *InMemoryBackend) UpdateIoTPackage(name, description, defaultVersionName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.iotPackages.Get(name)
	if !ok {
		return fmt.Errorf("package %q not found: %w", name, ErrResourceNotFound)
	}
	if description != "" {
		p.Description = description
	}
	if defaultVersionName != "" {
		p.DefaultVersionName = defaultVersionName
	}
	p.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteIoTPackage(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.iotPackages.Has(name) {
		return fmt.Errorf("package %q not found: %w", name, ErrResourceNotFound)
	}
	b.iotPackages.Delete(name)

	return nil
}

func (b *InMemoryBackend) ListIoTPackages() []*IoTPackage {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.iotPackages.Snapshot()
	out := make([]*IoTPackage, 0, len(items))
	for _, v := range items {
		out = append(out, cloneIoTPackage(v))
	}

	return out
}

// IoTPackageVersion represents a version of an AWS IoT software package.
//
//nolint:revive // IoTPackageVersion is intentional to avoid conflict with the builtin package keyword
type IoTPackageVersion struct {
	Tags              map[string]string `json:"tags,omitempty"`
	PackageVersionARN string            `json:"packageVersionArn"`
	PackageName       string            `json:"packageName"`
	VersionName       string            `json:"versionName"`
	Description       string            `json:"description,omitempty"`
	Status            string            `json:"status"`
	CreationDate      float64           `json:"creationDate,omitempty"`
	LastModifiedDate  float64           `json:"lastModifiedDate,omitempty"`
}

func cloneIoTPackageVersion(v *IoTPackageVersion) *IoTPackageVersion {
	cp := *v
	cp.Tags = make(map[string]string, len(v.Tags))
	maps.Copy(cp.Tags, v.Tags)

	return &cp
}

func (b *InMemoryBackend) packageVersionARN(packageName, versionName string) string {
	return arn.Build("iot", b.region, b.accountID,
		fmt.Sprintf("package/%s/version/%s", packageName, versionName))
}

func (b *InMemoryBackend) CreateIoTPackageVersion(
	packageName, versionName, description string,
	tags map[string]string,
) (*IoTPackageVersion, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.packageVersions2[packageName] == nil {
		b.packageVersions2[packageName] = make(map[string]*IoTPackageVersion)
	}
	if _, exists := b.packageVersions2[packageName][versionName]; exists {
		return nil, fmt.Errorf("package version %q/%q already exists: %w", packageName, versionName, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	v := &IoTPackageVersion{
		PackageVersionARN: b.packageVersionARN(packageName, versionName),
		PackageName:       packageName,
		VersionName:       versionName,
		Description:       description,
		Status:            "DRAFT",
		Tags:              make(map[string]string),
		CreationDate:      now,
		LastModifiedDate:  now,
	}
	maps.Copy(v.Tags, tags)
	b.packageVersions2[packageName][versionName] = v
	b.putResourceTagsLocked(v.PackageVersionARN, v.Tags)

	return cloneIoTPackageVersion(v), nil
}

func (b *InMemoryBackend) GetIoTPackageVersion(packageName, versionName string) (*IoTPackageVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.packageVersions2[packageName] == nil {
		return nil, fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}
	v, ok := b.packageVersions2[packageName][versionName]
	if !ok {
		return nil, fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}

	return cloneIoTPackageVersion(v), nil
}

func (b *InMemoryBackend) UpdateIoTPackageVersion(packageName, versionName, description, status string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.packageVersions2[packageName] == nil {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}
	v, ok := b.packageVersions2[packageName][versionName]
	if !ok {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}
	if description != "" {
		v.Description = description
	}
	if status != "" {
		v.Status = status
	}
	v.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteIoTPackageVersion(packageName, versionName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.packageVersions2[packageName] == nil {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}
	if _, ok := b.packageVersions2[packageName][versionName]; !ok {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}
	delete(b.packageVersions2[packageName], versionName)

	return nil
}

func (b *InMemoryBackend) ListIoTPackageVersions(packageName string) []*IoTPackageVersion {
	b.mu.RLock()
	defer b.mu.RUnlock()

	m := b.packageVersions2[packageName]
	if m == nil {
		return []*IoTPackageVersion{}
	}
	keys := collections.SortedKeys(m)
	out := make([]*IoTPackageVersion, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneIoTPackageVersion(m[k]))
	}

	return out
}

// PackageConfiguration holds the software package configuration.
type PackageConfiguration struct {
	VersionUpdateByJobsConfig map[string]any `json:"versionUpdateByJobsConfig,omitempty"`
}

func (b *InMemoryBackend) GetPackageConfiguration() *PackageConfiguration {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.packageConfig == nil {
		return &PackageConfiguration{}
	}
	cp := *b.packageConfig

	return &cp
}

func (b *InMemoryBackend) UpdatePackageConfiguration(cfg map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.packageConfig == nil {
		b.packageConfig = &PackageConfiguration{}
	}
	if cfg != nil {
		b.packageConfig.VersionUpdateByJobsConfig = cfg
	}

	return nil
}

// SbomValidationResult reports the outcome of validating one SBOM file
// associated with a package version.
type SbomValidationResult struct {
	FileName         string `json:"fileName,omitempty"`
	ValidationResult string `json:"validationResult"`
	ErrorCode        string `json:"errorCode,omitempty"`
	ErrorMessage     string `json:"errorMessage,omitempty"`
}

// computeSbomValidationResult deterministically evaluates an associated SBOM
// document: a well-formed S3 location succeeds, a missing/incomplete one
// fails with the AWS INCOMPATIBLE_FORMAT error code.
func computeSbomValidationResult(sbom *SbomDocument) *SbomValidationResult {
	if sbom == nil || sbom.S3Location == nil || sbom.S3Location.Bucket == "" || sbom.S3Location.Key == "" {
		return &SbomValidationResult{
			ValidationResult: "FAILED",
			ErrorCode:        "INCOMPATIBLE_FORMAT",
			ErrorMessage:     "SBOM file location is missing or incomplete",
		}
	}

	return &SbomValidationResult{
		FileName:         sbom.S3Location.Key,
		ValidationResult: "SUCCEEDED",
	}
}

// DisassociateSbomFromPackageVersion clears the SBOM (and its validation
// results) associated with a package version.
func (b *InMemoryBackend) DisassociateSbomFromPackageVersion(packageName, versionName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.requirePackageVersionLocked(packageName, versionName); err != nil {
		return err
	}

	key := packageVersionKey(packageName, versionName)
	delete(b.packageVersionSboms, key)
	delete(b.sbomValidationResults, key)

	return nil
}

// ListSbomValidationResults returns the stored validation results for a
// package version's associated SBOM, paginated.
func (b *InMemoryBackend) ListSbomValidationResults(
	packageName, versionName string, maxResults int32, nextToken string,
) ([]*SbomValidationResult, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.requirePackageVersionLocked(packageName, versionName); err != nil {
		return nil, "", err
	}

	src := b.sbomValidationResults[packageVersionKey(packageName, versionName)]
	out := make([]*SbomValidationResult, len(src))

	for i, r := range src {
		cp := *r
		out[i] = &cp
	}

	page, next := paginateMaps(out, searchPageSize(maxResults), searchStartOffset(nextToken))

	return page, next, nil
}

// requirePackageVersionLocked returns ErrResourceNotFound if the given
// package version does not exist (i.e. CreateIoTPackageVersion was never
// called for it). Must be called with b.mu held.
func (b *InMemoryBackend) requirePackageVersionLocked(packageName, versionName string) error {
	if b.packageVersions2[packageName] == nil {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}

	if _, ok := b.packageVersions2[packageName][versionName]; !ok {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}

	return nil
}

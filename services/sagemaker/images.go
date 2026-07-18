package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrSMImageNotFound is returned when a SageMaker image does not exist.
	ErrSMImageNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrImageHasVersions is returned when deleting an image that still has versions.
	ErrImageHasVersions = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrImageVersionNotFound is returned when an image version does not exist.
	ErrImageVersionNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
)

// ---------------------------------------------------------------------------
// SMImage
// ---------------------------------------------------------------------------

// SMImage represents a SageMaker image.
type SMImage struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	ImageName        string            `json:"ImageName"`
	ImageArn         string            `json:"ImageArn"`
	ImageStatus      string            `json:"ImageStatus"`
	Description      string            `json:"Description,omitempty"`
	DisplayName      string            `json:"DisplayName,omitempty"`
	RoleArn          string            `json:"RoleArn,omitempty"`
}

func cloneSMImage(img *SMImage) *SMImage {
	cp := *img
	cp.Tags = maps.Clone(img.Tags)

	return &cp
}

// CreateImage creates a SageMaker image.
func (b *InMemoryBackend) CreateImage(
	ctx context.Context,
	name, description, roleArn string,
	tags map[string]string,
) (*SMImage, error) {
	b.mu.Lock("CreateImage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: ImageName is required", ErrValidation)
	}

	if _, ok := b.smImagesStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: image %q already exists", ErrValidation, name)
	}

	imageARN := arn.Build("sagemaker", region, b.accountID, "image/"+name)
	now := time.Now()

	img := &SMImage{
		ImageName:        name,
		ImageArn:         imageARN,
		ImageStatus:      "CREATED",
		Description:      description,
		RoleArn:          roleArn,
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.smImagesStore(region).Put(img)

	return cloneSMImage(img), nil
}

// DescribeImage returns a SageMaker image by name.
func (b *InMemoryBackend) DescribeImage(ctx context.Context, name string) (*SMImage, error) {
	b.mu.RLock("DescribeImage")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	img, ok := b.smImagesStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, name)
	}

	return cloneSMImage(img), nil
}

// UpdateImageOptions bundles the mutable fields accepted by UpdateImage.
// Nil pointer fields are left unchanged.
type UpdateImageOptions struct {
	Description      *string
	DisplayName      *string
	RoleArn          *string
	DeleteProperties []string
}

// UpdateImage updates a SageMaker image's mutable metadata (Description,
// DisplayName, RoleArn), optionally clearing Description/DisplayName first
// via DeleteProperties.
func (b *InMemoryBackend) UpdateImage(
	ctx context.Context,
	name string,
	opts UpdateImageOptions,
) (*SMImage, error) {
	b.mu.Lock("UpdateImage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	img, ok := b.smImagesStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, name)
	}

	for _, prop := range opts.DeleteProperties {
		switch prop {
		case "Description":
			img.Description = ""
		case "DisplayName":
			img.DisplayName = ""
		default:
			return nil, fmt.Errorf("%w: DeleteProperties value %q is not supported", ErrValidation, prop)
		}
	}

	if opts.Description != nil {
		img.Description = *opts.Description
	}

	if opts.DisplayName != nil {
		img.DisplayName = *opts.DisplayName
	}

	if opts.RoleArn != nil {
		img.RoleArn = *opts.RoleArn
	}

	img.LastModifiedTime = time.Now()

	return cloneSMImage(img), nil
}

// DeleteImage removes a SageMaker image by name.
func (b *InMemoryBackend) DeleteImage(ctx context.Context, name string) error {
	b.mu.Lock("DeleteImage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.smImagesStore(region).Get(name); !ok {
		return fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, name)
	}

	// AWS rejects deletion when image versions still exist.
	if versions, ok := b.imageVersionsStore(region)[name]; ok && len(versions) > 0 {
		return fmt.Errorf("%w: image %q has versions and cannot be deleted", ErrImageHasVersions, name)
	}

	store := b.smImagesStore(region)
	store.Delete(name)

	return nil
}

// ListImages returns all images sorted by name.
func (b *InMemoryBackend) ListImages(ctx context.Context, nextToken string) ([]*SMImage, string) {
	b.mu.RLock("ListImages")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.smImagesStoreRO(region),
		nextToken,
		cloneSMImage,
		func(v *SMImage) string { return v.ImageName },
	)
}

// ---------------------------------------------------------------------------
// ImageVersion
// ---------------------------------------------------------------------------

// ImageVersion represents a version of a SageMaker image.
type ImageVersion struct {
	CreationTime       time.Time `json:"CreationTime"`
	LastModifiedTime   time.Time `json:"LastModifiedTime"`
	JobType            string    `json:"JobType,omitempty"`
	ImageArn           string    `json:"ImageArn"`
	ImageVersionArn    string    `json:"ImageVersionArn"`
	ImageVersionStatus string    `json:"ImageVersionStatus"`
	MLFramework        string    `json:"MLFramework,omitempty"`
	Processor          string    `json:"Processor,omitempty"`
	ProgrammingLang    string    `json:"ProgrammingLang,omitempty"`
	ReleaseNotes       string    `json:"ReleaseNotes,omitempty"`
	VendorGuidance     string    `json:"VendorGuidance,omitempty"`
	Aliases            []string  `json:"Aliases,omitempty"`
	Version            int       `json:"Version"`
	Horovod            bool      `json:"Horovod,omitempty"`
}

func cloneImageVersion(v *ImageVersion) *ImageVersion {
	cp := *v
	cp.Aliases = append([]string(nil), v.Aliases...)

	return &cp
}

// CreateImageVersion creates a new version for an image.
func (b *InMemoryBackend) CreateImageVersion(ctx context.Context, imageName string) (*ImageVersion, error) {
	b.mu.Lock("CreateImageVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	img, ok := b.smImagesStore(region).Get(imageName)
	if !ok {
		return nil, fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, imageName)
	}

	b.imageVersionCountsStore(region)[imageName]++
	version := b.imageVersionCountsStore(region)[imageName]

	versionARN := arn.Build(
		"sagemaker", region, b.accountID,
		"image-version/"+imageName+"/"+strconv.Itoa(version),
	)
	now := time.Now()

	iv := &ImageVersion{
		ImageArn:           img.ImageArn,
		ImageVersionArn:    versionARN,
		ImageVersionStatus: "CREATED",
		Version:            version,
		CreationTime:       now,
		LastModifiedTime:   now,
	}

	if b.imageVersionsStore(region)[imageName] == nil {
		b.imageVersionsStore(region)[imageName] = make(map[int]*ImageVersion)
	}

	b.imageVersionsStore(region)[imageName][version] = iv

	return cloneImageVersion(iv), nil
}

// DescribeImageVersion returns an image version by image name and version number.
func (b *InMemoryBackend) DescribeImageVersion(
	ctx context.Context,
	imageName string,
	version int,
) (*ImageVersion, error) {
	b.mu.RLock("DescribeImageVersion")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	versions, ok := b.imageVersionsStoreRO(region)[imageName]
	if !ok {
		return nil, fmt.Errorf("%w: no versions found for image %q", ErrImageVersionNotFound, imageName)
	}

	iv, ok := versions[version]
	if !ok {
		return nil, fmt.Errorf(
			"%w: version %d not found for image %q", ErrImageVersionNotFound, version, imageName,
		)
	}

	return cloneImageVersion(iv), nil
}

// UpdateImageVersionOptions bundles the mutable fields accepted by
// UpdateImageVersion. Nil/empty fields are left unchanged.
type UpdateImageVersionOptions struct {
	Horovod         *bool
	JobType         string
	MLFramework     string
	Processor       string
	ProgrammingLang string
	ReleaseNotes    string
	VendorGuidance  string
	AliasesToAdd    []string
	AliasesToDelete []string
}

// UpdateImageVersion updates a SageMaker image version's mutable metadata.
func (b *InMemoryBackend) UpdateImageVersion(
	ctx context.Context,
	imageName string,
	version int,
	opts UpdateImageVersionOptions,
) (*ImageVersion, error) {
	b.mu.Lock("UpdateImageVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	versions, ok := b.imageVersionsStore(region)[imageName]
	if !ok {
		return nil, fmt.Errorf("%w: no versions found for image %q", ErrImageVersionNotFound, imageName)
	}

	if version <= 0 {
		version = b.imageVersionCountsStore(region)[imageName]
	}

	iv, ok := versions[version]
	if !ok {
		return nil, fmt.Errorf(
			"%w: version %d not found for image %q", ErrImageVersionNotFound, version, imageName,
		)
	}

	applyImageVersionUpdate(iv, opts)
	iv.LastModifiedTime = time.Now()

	return cloneImageVersion(iv), nil
}

// applyImageVersionUpdate mutates iv in place per opts. Split out from
// UpdateImageVersion to keep that method's cyclomatic complexity low.
func applyImageVersionUpdate(iv *ImageVersion, opts UpdateImageVersionOptions) {
	if opts.Horovod != nil {
		iv.Horovod = *opts.Horovod
	}

	if opts.JobType != "" {
		iv.JobType = opts.JobType
	}

	if opts.MLFramework != "" {
		iv.MLFramework = opts.MLFramework
	}

	if opts.Processor != "" {
		iv.Processor = opts.Processor
	}

	if opts.ProgrammingLang != "" {
		iv.ProgrammingLang = opts.ProgrammingLang
	}

	if opts.ReleaseNotes != "" {
		iv.ReleaseNotes = opts.ReleaseNotes
	}

	if opts.VendorGuidance != "" {
		iv.VendorGuidance = opts.VendorGuidance
	}

	iv.Aliases = applyAliasChanges(iv.Aliases, opts.AliasesToAdd, opts.AliasesToDelete)
}

// applyAliasChanges returns aliases with additions appended (de-duplicated)
// and deletions removed.
func applyAliasChanges(aliases, toAdd, toDelete []string) []string {
	del := make(map[string]bool, len(toDelete))
	for _, a := range toDelete {
		del[a] = true
	}

	seen := make(map[string]bool, len(aliases)+len(toAdd))
	out := make([]string, 0, len(aliases)+len(toAdd))

	for _, a := range append(append([]string(nil), aliases...), toAdd...) {
		if del[a] || seen[a] {
			continue
		}

		seen[a] = true
		out = append(out, a)
	}

	return out
}

// DeleteImageVersion removes an image version.
func (b *InMemoryBackend) DeleteImageVersion(ctx context.Context, imageName string, version int) error {
	b.mu.Lock("DeleteImageVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	versions, ok := b.imageVersionsStore(region)[imageName]
	if !ok {
		return fmt.Errorf("%w: no versions found for image %q", ErrImageVersionNotFound, imageName)
	}

	if _, exists := versions[version]; !exists {
		return fmt.Errorf("%w: version %d not found for image %q", ErrImageVersionNotFound, version, imageName)
	}

	delete(versions, version)

	return nil
}

// ListImageVersions returns all versions for an image sorted by version number.
func (b *InMemoryBackend) ListImageVersions(
	ctx context.Context,
	imageName, nextToken string,
) ([]*ImageVersion, string) {
	b.mu.RLock("ListImageVersions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	versions := b.imageVersionsStoreRO(region)[imageName]

	nums := make([]int, 0, len(versions))
	for v := range versions {
		nums = append(nums, v)
	}

	sort.Ints(nums)

	start := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			for i, v := range nums {
				if v == n {
					start = i

					break
				}
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(nums))

	out := make([]*ImageVersion, 0, end-start)
	for _, n := range nums[start:end] {
		out = append(out, cloneImageVersion(versions[n]))
	}

	next := ""
	if end < len(nums) {
		next = strconv.Itoa(nums[end])
	}

	return out, next
}

// ---------------------------------------------------------------------------
// Image alias listing
// ---------------------------------------------------------------------------

// ListImageAliases returns the aliases attached to an image. If version is
// positive, only that version's aliases are considered; otherwise, aliases
// from every version of the image are aggregated.
func (b *InMemoryBackend) ListImageAliases(
	ctx context.Context,
	imageName string,
	version int32,
	nextToken string,
) ([]string, string, error) {
	b.mu.RLock("ListImageAliases")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.smImagesStoreRO(region).Get(imageName); !ok {
		return nil, "", fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, imageName)
	}

	versions := b.imageVersionsStoreRO(region)[imageName]

	var candidates []*ImageVersion

	if version > 0 {
		if iv, ok := versions[int(version)]; ok {
			candidates = append(candidates, iv)
		}
	} else {
		for _, iv := range versions {
			candidates = append(candidates, iv)
		}
	}

	aliases := dedupeAliases(candidates)
	sort.Strings(aliases)

	page, out := paginateSlice(aliases, nextToken, 0)

	return page, out, nil
}

// dedupeAliases flattens the Aliases of every given image version into a
// single de-duplicated slice.
func dedupeAliases(versions []*ImageVersion) []string {
	seen := map[string]bool{}
	aliases := make([]string, 0)

	for _, iv := range versions {
		for _, a := range iv.Aliases {
			if seen[a] {
				continue
			}

			seen[a] = true

			aliases = append(aliases, a)
		}
	}

	return aliases
}

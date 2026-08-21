package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
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

// SMImage represents a SageMaker image. FailureReason
// (DescribeImageOutput/types.Image, both "This member is required"-free) is
// not modeled: nothing in ImageStatus's CREATE_FAILED/UPDATE_FAILED/
// DELETE_FAILED transitions is ever reached by this backend, so there is no
// real failure to report a reason for.
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

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeImage.
func (img *SMImage) MarshalJSON() ([]byte, error) {
	type alias SMImage

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(img),
		CreationTime:     epochSeconds(img.CreationTime),
		LastModifiedTime: epochSeconds(img.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [SMImage.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (img *SMImage) UnmarshalJSON(data []byte) error {
	type alias SMImage

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(img)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	img.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	img.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateImage creates a SageMaker image.
func (b *InMemoryBackend) CreateImage(
	ctx context.Context,
	name, description, displayName, roleArn string,
	tags map[string]string,
) (*SMImage, error) {
	b.mu.Lock("CreateImage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: ImageName is required", ErrValidation)
	}

	if roleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
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
		DisplayName:      displayName,
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

// ListImagesParams bundles ListImages' filter/sort/pagination criteria
// (api_op_ListImages.go:32-64, sagemaker@v1.263.2).
type ListImagesParams struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	NameContains           string
	NextToken              string
	SortBy                 string
	SortOrder              string
	MaxResults             int32
}

// ListImages returns images matching params, sorted by params.SortBy
// (default CreationTime, per api_op_ListImages.go:57) / params.SortOrder
// (default Descending, per api_op_ListImages.go:60 — unlike most other List
// ops in this service, this one's real default sort order is Descending, not
// Ascending), capped at params.MaxResults.
func (b *InMemoryBackend) ListImages(ctx context.Context, params ListImagesParams) ([]*SMImage, string) {
	b.mu.RLock("ListImages")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	tbl := b.smImagesStoreRO(region)
	list := make([]*SMImage, 0, tbl.Len())

	for _, img := range tbl.All() {
		if !matchesImageListParams(img, params) {
			continue
		}

		list = append(list, cloneSMImage(img))
	}

	asc := strings.EqualFold(params.SortOrder, "Ascending")
	sort.Slice(list, func(i, j int) bool {
		less := imageSortLess(list[i], list[j], params.SortBy)
		if asc {
			return less
		}

		return !less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// matchesImageListParams reports whether img satisfies every filter in params.
func matchesImageListParams(img *SMImage, p ListImagesParams) bool {
	if p.NameContains != "" && !strings.Contains(img.ImageName, p.NameContains) {
		return false
	}

	if p.CreationTimeAfter != nil && !img.CreationTime.After(*p.CreationTimeAfter) {
		return false
	}

	if p.CreationTimeBefore != nil && !img.CreationTime.Before(*p.CreationTimeBefore) {
		return false
	}

	if p.LastModifiedTimeAfter != nil && !img.LastModifiedTime.After(*p.LastModifiedTimeAfter) {
		return false
	}

	if p.LastModifiedTimeBefore != nil && !img.LastModifiedTime.Before(*p.LastModifiedTimeBefore) {
		return false
	}

	return true
}

// imageSortLess orders two images by sortBy — one of ImageSortBy's real
// values (CREATION_TIME/LAST_MODIFIED_TIME/IMAGE_NAME, types/enums.go:
// 4180-4182), a different casing convention than the mixed-case "Name"/
// "CreationTime" sort keys most other List ops in this service use.
func imageSortLess(a, b *SMImage, sortBy string) bool {
	switch sortBy {
	case "IMAGE_NAME":
		if a.ImageName != b.ImageName {
			return a.ImageName < b.ImageName
		}
	case sortByLastModifiedTime:
		if !a.LastModifiedTime.Equal(b.LastModifiedTime) {
			return a.LastModifiedTime.Before(b.LastModifiedTime)
		}
	default:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	}

	return a.ImageName < b.ImageName
}

// ---------------------------------------------------------------------------
// ImageVersion
// ---------------------------------------------------------------------------

// ImageVersion represents a version of a SageMaker image. FailureReason
// (DescribeImageVersionOutput/types.ImageVersion) is not modeled for the same
// reason as SMImage's: ImageVersionStatus never reaches CREATE_FAILED/
// DELETE_FAILED here.
type ImageVersion struct {
	CreationTime       time.Time `json:"CreationTime"`
	LastModifiedTime   time.Time `json:"LastModifiedTime"`
	JobType            string    `json:"JobType,omitempty"`
	BaseImage          string    `json:"BaseImage,omitempty"`
	ContainerImage     string    `json:"ContainerImage,omitempty"`
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

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeImageVersion.
func (v *ImageVersion) MarshalJSON() ([]byte, error) {
	type alias ImageVersion

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(v),
		CreationTime:     epochSeconds(v.CreationTime),
		LastModifiedTime: epochSeconds(v.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [ImageVersion.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (v *ImageVersion) UnmarshalJSON(data []byte) error {
	type alias ImageVersion

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(v)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	v.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	v.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateImageVersionOptions bundles CreateImageVersion's optional fields
// (api_op_CreateImageVersion.go:30-100, sagemaker@v1.263.2). ClientToken is
// omitted, matching every other Create op in this service.
type CreateImageVersionOptions struct {
	Horovod         *bool
	JobType         string
	MLFramework     string
	Processor       string
	ProgrammingLang string
	ReleaseNotes    string
	VendorGuidance  string
	Aliases         []string
}

// CreateImageVersion creates a new version for an image. ContainerImage is
// set equal to BaseImage at creation: this backend has no ECR subsystem to
// resolve BaseImage to a distinct digest-pinned registry path, so the two
// coincide from the moment the version exists (they may later diverge on
// real AWS, which this backend does not model).
func (b *InMemoryBackend) CreateImageVersion(
	ctx context.Context,
	imageName, baseImage string,
	opts CreateImageVersionOptions,
) (*ImageVersion, error) {
	b.mu.Lock("CreateImageVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if baseImage == "" {
		return nil, fmt.Errorf("%w: BaseImage is required", ErrValidation)
	}

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
		BaseImage:          baseImage,
		ContainerImage:     baseImage,
		Aliases:            append([]string(nil), opts.Aliases...),
		JobType:            opts.JobType,
		MLFramework:        opts.MLFramework,
		Processor:          opts.Processor,
		ProgrammingLang:    opts.ProgrammingLang,
		ReleaseNotes:       opts.ReleaseNotes,
		VendorGuidance:     opts.VendorGuidance,
		CreationTime:       now,
		LastModifiedTime:   now,
	}

	if opts.Horovod != nil {
		iv.Horovod = *opts.Horovod
	}

	if b.imageVersionsStore(region)[imageName] == nil {
		b.imageVersionsStore(region)[imageName] = make(map[int]*ImageVersion)
	}

	b.imageVersionsStore(region)[imageName][version] = iv

	return cloneImageVersion(iv), nil
}

// resolveImageVersionNumber returns the version number identified by alias
// (searched across every version's Aliases) or, if alias is empty, version
// itself when positive. It implements no "neither given" default: callers
// decide that per their own op's documented contract —
// DescribeImageVersion/UpdateImageVersion default to the latest version,
// ListAliases aggregates every version, and DeleteImageVersion requires one
// or the other be given.
func resolveImageVersionNumber(
	versions map[int]*ImageVersion, alias string, version int,
) (int, bool, error) {
	if alias != "" {
		for n, iv := range versions {
			if slices.Contains(iv.Aliases, alias) {
				return n, true, nil
			}
		}

		return 0, true, fmt.Errorf("%w: alias %q not found", ErrImageVersionNotFound, alias)
	}

	if version > 0 {
		return version, true, nil
	}

	return 0, false, nil
}

// latestImageVersion returns the highest version number present in versions,
// or 0 if it is empty. Used as the default when DescribeImageVersion/
// UpdateImageVersion are called without a Version or Alias.
func latestImageVersion(versions map[int]*ImageVersion) int {
	latest := 0
	for v := range versions {
		if v > latest {
			latest = v
		}
	}

	return latest
}

// DescribeImageVersion returns an image version by image name, identified by
// alias or version number, defaulting to the latest version when neither is
// given (api_op_DescribeImageVersion.go:44: "If not specified, the
// latest version is described").
func (b *InMemoryBackend) DescribeImageVersion(
	ctx context.Context,
	imageName, alias string,
	version int,
) (*ImageVersion, error) {
	b.mu.RLock("DescribeImageVersion")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	versions, ok := b.imageVersionsStoreRO(region)[imageName]
	if !ok {
		return nil, fmt.Errorf("%w: no versions found for image %q", ErrImageVersionNotFound, imageName)
	}

	resolved, specified, err := resolveImageVersionNumber(versions, alias, version)
	if err != nil {
		return nil, err
	}

	if !specified {
		resolved = latestImageVersion(versions)
	}

	iv, ok := versions[resolved]
	if !ok {
		return nil, fmt.Errorf(
			"%w: version %d not found for image %q", ErrImageVersionNotFound, resolved, imageName,
		)
	}

	return cloneImageVersion(iv), nil
}

// UpdateImageVersionOptions bundles the mutable fields accepted by
// UpdateImageVersion. Nil/empty fields are left unchanged.
type UpdateImageVersionOptions struct {
	Alias           string
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

// UpdateImageVersion updates a SageMaker image version's mutable metadata,
// identified by opts.Alias or version, defaulting to the latest version when
// neither is given (same undocumented-but-established convention this
// backend already applied before opts.Alias existed).
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

	resolved, specified, err := resolveImageVersionNumber(versions, opts.Alias, version)
	if err != nil {
		return nil, err
	}

	if !specified {
		resolved = latestImageVersion(versions)
	}

	iv, ok := versions[resolved]
	if !ok {
		return nil, fmt.Errorf(
			"%w: version %d not found for image %q", ErrImageVersionNotFound, resolved, imageName,
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

// DeleteImageVersion removes an image version identified by alias or version
// number. Unlike DescribeImageVersion/UpdateImageVersion, the real op's doc
// (api_op_DeleteImageVersion.go:28-42) states no "if unspecified" default,
// so one of alias/version is required here.
func (b *InMemoryBackend) DeleteImageVersion(ctx context.Context, imageName, alias string, version int) error {
	b.mu.Lock("DeleteImageVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	versions, ok := b.imageVersionsStore(region)[imageName]
	if !ok {
		return fmt.Errorf("%w: no versions found for image %q", ErrImageVersionNotFound, imageName)
	}

	resolved, specified, err := resolveImageVersionNumber(versions, alias, version)
	if err != nil {
		return err
	}

	if !specified {
		return fmt.Errorf("%w: Version or Alias is required", ErrValidation)
	}

	if _, exists := versions[resolved]; !exists {
		return fmt.Errorf("%w: version %d not found for image %q", ErrImageVersionNotFound, resolved, imageName)
	}

	delete(versions, resolved)

	return nil
}

// ListImageVersionsParams bundles ListImageVersions' filter/sort/pagination
// criteria (api_op_ListImageVersions.go:31-65, sagemaker@v1.263.2).
type ListImageVersionsParams struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	NextToken              string
	SortBy                 string
	SortOrder              string
	MaxResults             int32
}

// ListImageVersions returns all versions for an image matching params, sorted
// by params.SortBy (default CreationTime, per api_op_ListImageVersions.go:
// 1089) / params.SortOrder (default Descending, per
// api_op_ListImageVersions.go:61 — same non-Ascending-default family as
// ListImages), capped at params.MaxResults.
func (b *InMemoryBackend) ListImageVersions(
	ctx context.Context,
	imageName string,
	params ListImageVersionsParams,
) ([]*ImageVersion, string) {
	b.mu.RLock("ListImageVersions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	versions := b.imageVersionsStoreRO(region)[imageName]

	list := make([]*ImageVersion, 0, len(versions))

	for _, iv := range versions {
		if !matchesImageVersionListParams(iv, params) {
			continue
		}

		list = append(list, cloneImageVersion(iv))
	}

	asc := strings.EqualFold(params.SortOrder, "Ascending")
	sort.Slice(list, func(i, j int) bool {
		less := imageVersionSortLess(list[i], list[j], params.SortBy)
		if asc {
			return less
		}

		return !less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// matchesImageVersionListParams reports whether iv satisfies every filter in params.
func matchesImageVersionListParams(iv *ImageVersion, p ListImageVersionsParams) bool {
	if p.CreationTimeAfter != nil && !iv.CreationTime.After(*p.CreationTimeAfter) {
		return false
	}

	if p.CreationTimeBefore != nil && !iv.CreationTime.Before(*p.CreationTimeBefore) {
		return false
	}

	if p.LastModifiedTimeAfter != nil && !iv.LastModifiedTime.After(*p.LastModifiedTimeAfter) {
		return false
	}

	if p.LastModifiedTimeBefore != nil && !iv.LastModifiedTime.Before(*p.LastModifiedTimeBefore) {
		return false
	}

	return true
}

// imageVersionSortLess orders two image versions by sortBy — one of
// ImageVersionSortBy's real values (CREATION_TIME/LAST_MODIFIED_TIME/VERSION,
// types/enums.go:4249-4251), the same all-caps-with-underscores convention as
// ImageSortBy.
func imageVersionSortLess(a, b *ImageVersion, sortBy string) bool {
	switch sortBy {
	case "VERSION":
		if a.Version != b.Version {
			return a.Version < b.Version
		}
	case sortByLastModifiedTime:
		if !a.LastModifiedTime.Equal(b.LastModifiedTime) {
			return a.LastModifiedTime.Before(b.LastModifiedTime)
		}
	default:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	}

	return a.Version < b.Version
}

// ---------------------------------------------------------------------------
// Image alias listing
// ---------------------------------------------------------------------------

// ListImageAliases returns the aliases attached to an image. If version is
// positive or alias identifies a version, only that version's aliases are
// considered; otherwise, aliases from every version of the image are
// aggregated (api_op_ListAliases.go:44-45: "If image version is not
// specified, the aliases of all versions of the image are listed").
func (b *InMemoryBackend) ListImageAliases(
	ctx context.Context,
	imageName, alias string,
	version int32,
	nextToken string,
	maxResults int32,
) ([]string, string, error) {
	b.mu.RLock("ListImageAliases")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.smImagesStoreRO(region).Get(imageName); !ok {
		return nil, "", fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, imageName)
	}

	versions := b.imageVersionsStoreRO(region)[imageName]

	resolved, specified, err := resolveImageVersionNumber(versions, alias, int(version))
	if err != nil {
		return nil, "", err
	}

	var candidates []*ImageVersion

	if specified {
		if iv, ok := versions[resolved]; ok {
			candidates = append(candidates, iv)
		}
	} else {
		for _, iv := range versions {
			candidates = append(candidates, iv)
		}
	}

	aliases := dedupeAliases(candidates)
	sort.Strings(aliases)

	page, out := paginateSlice(aliases, nextToken, maxResults)

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

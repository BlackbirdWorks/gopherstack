package ecr

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// batchDeleteImageInput is the request body for BatchDeleteImage.
type batchDeleteImageInput struct {
	RepositoryName string            `json:"repositoryName"`
	RegistryID     string            `json:"registryId,omitempty"`
	ImageIDs       []ImageIdentifier `json:"imageIds"`
}

type batchDeleteImageOutput struct {
	ImageIDs []ImageIdentifier `json:"imageIds"`
	Failures []ImageFailure    `json:"failures"`
}

func (h *Handler) handleBatchDeleteImage(
	ctx context.Context,
	in *batchDeleteImageInput,
) (*batchDeleteImageOutput, error) {
	deleted, failures, err := h.Backend.BatchDeleteImage(ctx, in.RepositoryName, in.ImageIDs)
	if err != nil {
		return nil, err
	}

	if deleted == nil {
		deleted = []ImageIdentifier{}
	}

	if failures == nil {
		failures = []ImageFailure{}
	}

	return &batchDeleteImageOutput{ImageIDs: deleted, Failures: failures}, nil
}

// imageView is the JSON representation of an image returned by PutImage and
// BatchGetImage. The real AWS ecr.types.Image shape (per
// awsAwsjson11_deserializeDocumentImage) has exactly five fields — imageId,
// imageManifest, imageManifestMediaType, registryId, repositoryName — and
// notably does NOT include imageDigest, imagePushedAt, imageStatus,
// storageClass, or imageSizeInBytes at the top level (those only appear on the
// distinct ImageDetail shape returned by DescribeImages). The digest is
// available to callers via the nested imageId.imageDigest field.
type imageView struct {
	ImageID                ImageIdentifier `json:"imageId"`
	ImageManifest          string          `json:"imageManifest,omitempty"`
	ImageManifestMediaType string          `json:"imageManifestMediaType,omitempty"`
	RegistryID             string          `json:"registryId,omitempty"`
	RepositoryName         string          `json:"repositoryName,omitempty"`
}

func toImageView(img Image) imageView {
	return imageView{
		ImageID:                img.ImageID,
		ImageManifest:          img.ImageManifest,
		ImageManifestMediaType: img.ImageManifestMediaType,
		RegistryID:             img.RegistryID,
		RepositoryName:         img.RepositoryName,
	}
}

// batchGetImageInput is the request body for BatchGetImage.
type batchGetImageInput struct {
	RepositoryName string            `json:"repositoryName"`
	RegistryID     string            `json:"registryId,omitempty"`
	ImageIDs       []ImageIdentifier `json:"imageIds"`
}

type batchGetImageOutput struct {
	Images   []imageView    `json:"images"`
	Failures []ImageFailure `json:"failures"`
}

func (h *Handler) handleBatchGetImage(
	ctx context.Context,
	in *batchGetImageInput,
) (*batchGetImageOutput, error) {
	imgs, failures, err := h.Backend.BatchGetImage(ctx, in.RepositoryName, in.ImageIDs)
	if err != nil {
		return nil, err
	}

	views := make([]imageView, 0, len(imgs))
	for _, img := range imgs {
		views = append(views, toImageView(img))
	}

	if failures == nil {
		failures = []ImageFailure{}
	}

	return &batchGetImageOutput{Images: views, Failures: failures}, nil
}

type describeImagesFilter struct {
	TagStatus string `json:"tagStatus,omitempty"`
}

type describeImagesInput struct {
	Filter         *describeImagesFilter `json:"filter,omitempty"`
	RepositoryName string                `json:"repositoryName"`
	NextToken      string                `json:"nextToken,omitempty"`
	ImageIDs       []ImageIdentifier     `json:"imageIds,omitempty"`
	MaxResults     int                   `json:"maxResults,omitempty"`
}

// imageScanFindingsSummaryView is the JSON representation of ImageDetail's
// imageScanFindingsSummary field (real AWS type: ImageScanFindingsSummary).
type imageScanFindingsSummaryView struct {
	FindingSeverityCounts        map[string]int32 `json:"findingSeverityCounts,omitempty"`
	ImageScanCompletedAt         float64          `json:"imageScanCompletedAt,omitempty"`
	VulnerabilitySourceUpdatedAt float64          `json:"vulnerabilitySourceUpdatedAt,omitempty"`
}

// imageScanStatusView is the JSON representation of ImageDetail's
// imageScanStatus field (real AWS type: ImageScanStatus).
type imageScanStatusView struct {
	Description string `json:"description,omitempty"`
	Status      string `json:"status,omitempty"`
}

type imageDetailView struct {
	ImageScanFindingsSummary *imageScanFindingsSummaryView `json:"imageScanFindingsSummary,omitempty"`
	ImageScanStatus          *imageScanStatusView          `json:"imageScanStatus,omitempty"`
	ArtifactMediaType        string                        `json:"artifactMediaType,omitempty"`
	ImageStatus              string                        `json:"imageStatus,omitempty"`
	RegistryID               string                        `json:"registryId,omitempty"`
	RepositoryName           string                        `json:"repositoryName,omitempty"`
	SubjectManifestDigest    string                        `json:"subjectManifestDigest,omitempty"`
	ImageManifestMediaType   string                        `json:"imageManifestMediaType,omitempty"`
	ImageDigest              string                        `json:"imageDigest,omitempty"`
	ImageTags                []string                      `json:"imageTags,omitempty"`
	ImagePushedAt            float64                       `json:"imagePushedAt,omitempty"`
	LastActivatedAt          float64                       `json:"lastActivatedAt,omitempty"`
	LastArchivedAt           float64                       `json:"lastArchivedAt,omitempty"`
	LastRecordedPullTime     float64                       `json:"lastRecordedPullTime,omitempty"`
	ImageSizeInBytes         int64                         `json:"imageSizeInBytes,omitempty"`
}

type describeImagesOutput struct {
	NextToken    string            `json:"nextToken,omitempty"`
	ImageDetails []imageDetailView `json:"imageDetails"`
}

// epochOrZero converts t to a Unix epoch-seconds float64, or 0 for a zero
// time.Time (which imageDetailView's omitempty tags then drop from the wire,
// matching AWS's optional-field semantics for images that were never
// archived/activated/pulled).
func epochOrZero(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}

	return float64(t.Unix())
}

// manifestArtifactFields is used to pull artifactMediaType/subjectManifestDigest
// out of a pushed image's raw manifest JSON: OCI 1.1 artifact manifests carry
// a top-level "artifactType" string and an optional "subject" descriptor
// (used for referrer relationships) whose "digest" is the subject manifest
// digest. Manifests that don't use either field (e.g. plain Docker v2
// manifests) simply decode to zero values, which toImageDetailView omits.
type manifestArtifactFields struct {
	Subject *struct {
		Digest string `json:"digest,omitempty"`
	} `json:"subject,omitempty"`
	ArtifactType string `json:"artifactType,omitempty"`
}

// parseManifestArtifactFields best-effort parses manifest for artifactType/
// subject.digest. A malformed or non-JSON manifest simply yields empty
// strings rather than an error, since DescribeImages must not fail images
// whose manifest predates strict validation.
func parseManifestArtifactFields(manifest string) (string, string) {
	var parsed manifestArtifactFields

	if err := json.Unmarshal([]byte(manifest), &parsed); err != nil {
		return "", ""
	}

	subjectDigest := ""
	if parsed.Subject != nil {
		subjectDigest = parsed.Subject.Digest
	}

	return parsed.ArtifactType, subjectDigest
}

func toImageDetailView(img Image) imageDetailView {
	// Prefer multi-tag list from DescribeImages annotation; fall back to single tag.
	tags := img.Tags
	if len(tags) == 0 && img.ImageID.ImageTag != "" {
		tags = []string{img.ImageID.ImageTag}
	}
	if tags == nil {
		tags = []string{}
	}

	artifactMediaType, subjectDigest := parseManifestArtifactFields(img.ImageManifest)

	view := imageDetailView{
		ImageDigest:            img.ImageDigest,
		ImageTags:              tags,
		ImagePushedAt:          epochOrZero(img.ImagePushedAt),
		LastActivatedAt:        epochOrZero(img.LastActivatedAt),
		LastArchivedAt:         epochOrZero(img.LastArchivedAt),
		LastRecordedPullTime:   epochOrZero(img.LastRecordedPullTime),
		ImageSizeInBytes:       img.ImageSizeInBytes,
		ImageManifestMediaType: img.ImageManifestMediaType,
		ArtifactMediaType:      artifactMediaType,
		SubjectManifestDigest:  subjectDigest,
		ImageStatus:            img.ImageStatus,
		RegistryID:             img.RegistryID,
		RepositoryName:         img.RepositoryName,
	}

	if img.ScanFindingsSummary != nil {
		view.ImageScanFindingsSummary = &imageScanFindingsSummaryView{
			FindingSeverityCounts:        img.ScanFindingsSummary.FindingSeverityCounts,
			ImageScanCompletedAt:         img.ScanFindingsSummary.ImageScanCompletedAt,
			VulnerabilitySourceUpdatedAt: img.ScanFindingsSummary.VulnerabilitySourceUpdatedAt,
		}
	}

	if img.ScanStatus != nil {
		view.ImageScanStatus = &imageScanStatusView{
			Status:      img.ScanStatus.Status,
			Description: img.ScanStatus.Description,
		}
	}

	return view
}

func (h *Handler) handleDescribeImages(
	ctx context.Context,
	in *describeImagesInput,
) (*describeImagesOutput, error) {
	imgs, err := h.Backend.DescribeImages(ctx, in.RepositoryName, in.ImageIDs)
	if err != nil {
		return nil, err
	}

	if len(in.ImageIDs) == 0 {
		imgs = filterAndPaginateImages(imgs, in.Filter, in.NextToken, in.MaxResults)
	}

	var nextToken string
	if len(in.ImageIDs) == 0 && in.MaxResults > 0 && len(imgs) > in.MaxResults {
		nextToken = base64.StdEncoding.EncodeToString([]byte(imgs[in.MaxResults].ImageDigest))
		imgs = imgs[:in.MaxResults]
	}

	details := make([]imageDetailView, 0, len(imgs))
	for _, img := range imgs {
		details = append(details, toImageDetailView(img))
	}

	return &describeImagesOutput{ImageDetails: details, NextToken: nextToken}, nil
}

func filterAndPaginateImages(imgs []Image, filter *describeImagesFilter, nextToken string, _ int) []Image {
	if filter != nil && filter.TagStatus != "" {
		filtered := imgs[:0]
		for _, img := range imgs {
			isTagged := len(img.Tags) > 0
			if passesTagFilter(isTagged, filter.TagStatus) {
				filtered = append(filtered, img)
			}
		}
		imgs = filtered
	}

	if nextToken != "" {
		decoded, decErr := base64.StdEncoding.DecodeString(nextToken)
		if decErr == nil {
			cursorKey := string(decoded)
			start := 0
			for i, img := range imgs {
				if img.ImageDigest == cursorKey {
					start = i

					break
				}
			}
			imgs = imgs[start:]
		}
	}

	return imgs
}

type listImagesFilter struct {
	TagStatus string `json:"tagStatus,omitempty"`
}

type listImagesInput struct {
	Filter         *listImagesFilter `json:"filter,omitempty"`
	RepositoryName string            `json:"repositoryName"`
	RegistryID     string            `json:"registryId,omitempty"`
	NextToken      string            `json:"nextToken,omitempty"`
	MaxResults     int               `json:"maxResults,omitempty"`
}

type listImagesOutput struct {
	NextToken string            `json:"nextToken,omitempty"`
	ImageIDs  []ImageIdentifier `json:"imageIds"`
}

func (h *Handler) handleListImages(
	ctx context.Context,
	in *listImagesInput,
) (*listImagesOutput, error) {
	tagStatusFilter := ""
	if in.Filter != nil {
		tagStatusFilter = in.Filter.TagStatus
	}

	imageIDs, err := h.Backend.ListImages(ctx, in.RepositoryName, tagStatusFilter)
	if err != nil {
		return nil, err
	}

	// Apply nextToken cursor: token is base64(digest:tag) of the first image on this page.
	if in.NextToken != "" {
		decoded, decErr := base64.StdEncoding.DecodeString(in.NextToken)
		if decErr == nil {
			cursorKey := string(decoded)
			start := 0
			for i, id := range imageIDs {
				if id.ImageDigest+":"+id.ImageTag == cursorKey {
					start = i

					break
				}
			}

			imageIDs = imageIDs[start:]
		}
	}

	// Apply maxResults page limit; emit opaque token = base64(digest:tag).
	var nextToken string
	if in.MaxResults > 0 && len(imageIDs) > in.MaxResults {
		next := imageIDs[in.MaxResults]
		nextToken = base64.StdEncoding.EncodeToString(
			[]byte(next.ImageDigest + ":" + next.ImageTag),
		)
		imageIDs = imageIDs[:in.MaxResults]
	}

	return &listImagesOutput{ImageIDs: imageIDs, NextToken: nextToken}, nil
}

type putImageInput struct {
	ImageDigest            string `json:"imageDigest,omitempty"`
	ImageManifest          string `json:"imageManifest"`
	ImageManifestMediaType string `json:"imageManifestMediaType,omitempty"`
	ImageTag               string `json:"imageTag,omitempty"`
	RepositoryName         string `json:"repositoryName"`
	RegistryID             string `json:"registryId,omitempty"`
}

type putImageOutput struct {
	Image *imageView `json:"image"`
}

func (h *Handler) handlePutImage(ctx context.Context, in *putImageInput) (*putImageOutput, error) {
	// AWS validates a caller-supplied imageDigest against the digest it computes
	// from the manifest and rejects a mismatch with ImageDigestDoesNotMatchException,
	// independent of any backend state (this is pure request validation).
	if in.ImageDigest != "" {
		sum := sha256.Sum256([]byte(in.ImageManifest))
		computed := "sha256:" + hex.EncodeToString(sum[:])

		if in.ImageDigest != computed {
			return nil, fmt.Errorf(
				"%w: manifest validation failed, digest calculated from the image manifest does"+
					" not match the provided digest",
				ErrImageDigestDoesNotMatch,
			)
		}
	}

	img, err := h.Backend.PutImage(ctx, in.RepositoryName, Image{
		ImageDigest:            in.ImageDigest,
		ImageManifest:          in.ImageManifest,
		ImageManifestMediaType: in.ImageManifestMediaType,
		ImageID: ImageIdentifier{
			ImageDigest: in.ImageDigest,
			ImageTag:    in.ImageTag,
		},
		RepositoryName: in.RepositoryName,
		RegistryID:     in.RegistryID,
	})
	if err != nil {
		return nil, err
	}

	view := toImageView(*img)

	return &putImageOutput{Image: &view}, nil
}

type putImageTagMutabilityInput struct {
	ImageTagMutability                 string                         `json:"imageTagMutability"`
	RepositoryName                     string                         `json:"repositoryName"`
	RegistryID                         string                         `json:"registryId,omitempty"`
	ImageTagMutabilityExclusionFilters []imageTagMutabilityFilterView `json:"imageTagMutabilityExclusionFilters,omitempty"`
}

type putImageTagMutabilityOutput struct {
	ImageTagMutability                 string                         `json:"imageTagMutability"`
	RepositoryName                     string                         `json:"repositoryName"`
	RegistryID                         string                         `json:"registryId"`
	ImageTagMutabilityExclusionFilters []imageTagMutabilityFilterView `json:"imageTagMutabilityExclusionFilters,omitempty"`
}

func (h *Handler) handlePutImageTagMutability(
	ctx context.Context,
	in *putImageTagMutabilityInput,
) (*putImageTagMutabilityOutput, error) {
	filters := make(
		[]ImageTagMutabilityExclusionFilter,
		0,
		len(in.ImageTagMutabilityExclusionFilters),
	)
	for _, filter := range in.ImageTagMutabilityExclusionFilters {
		filters = append(filters, ImageTagMutabilityExclusionFilter(filter))
	}

	repo, err := h.Backend.PutImageTagMutability(
		ctx,
		in.RepositoryName,
		in.ImageTagMutability,
		filters,
	)
	if err != nil {
		return nil, err
	}

	view := toRepositoryView(*repo)

	return &putImageTagMutabilityOutput{
		ImageTagMutability:                 view.ImageTagMutability,
		ImageTagMutabilityExclusionFilters: view.ImageTagMutabilityExclusionFilters,
		RepositoryName:                     view.RepositoryName,
		RegistryID:                         view.RegistryID,
	}, nil
}

type listImageReferrersInput struct {
	RepositoryName string          `json:"repositoryName"`
	SubjectID      ImageIdentifier `json:"subjectId"`
	RegistryID     string          `json:"registryId,omitempty"`
}

type listImageReferrersOutput struct {
	Referrers []ImageReferrer `json:"referrers"`
}

func (h *Handler) handleListImageReferrers(
	ctx context.Context,
	in *listImageReferrersInput,
) (*listImageReferrersOutput, error) {
	referrers, err := h.Backend.ListImageReferrers(ctx, in.RepositoryName, in.SubjectID)
	if err != nil {
		return nil, err
	}

	return &listImageReferrersOutput{Referrers: referrers}, nil
}

type updateImageStorageClassInput struct {
	ImageID            ImageIdentifier `json:"imageId"`
	RepositoryName     string          `json:"repositoryName"`
	TargetStorageClass string          `json:"targetStorageClass"`
	RegistryID         string          `json:"registryId,omitempty"`
}

func (h *Handler) handleUpdateImageStorageClass(
	ctx context.Context,
	in *updateImageStorageClassInput,
) (*ImageStorageClassResult, error) {
	return h.Backend.UpdateImageStorageClass(
		ctx,
		in.RepositoryName,
		in.ImageID,
		in.TargetStorageClass,
	)
}

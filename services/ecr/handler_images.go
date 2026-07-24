package ecr

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
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

type imageDetailView struct {
	ImageDigest            string   `json:"imageDigest,omitempty"`
	ImageManifestMediaType string   `json:"imageManifestMediaType,omitempty"`
	ImageStatus            string   `json:"imageStatus,omitempty"`
	RegistryID             string   `json:"registryId,omitempty"`
	RepositoryName         string   `json:"repositoryName,omitempty"`
	ImageTags              []string `json:"imageTags,omitempty"`
	ImagePushedAt          float64  `json:"imagePushedAt,omitempty"`
	ImageSizeInBytes       int64    `json:"imageSizeInBytes,omitempty"`
}

type describeImagesOutput struct {
	NextToken    string            `json:"nextToken,omitempty"`
	ImageDetails []imageDetailView `json:"imageDetails"`
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

	var pushedAt float64
	if !img.ImagePushedAt.IsZero() {
		pushedAt = float64(img.ImagePushedAt.Unix())
	}

	return imageDetailView{
		ImageDigest:            img.ImageDigest,
		ImageTags:              tags,
		ImagePushedAt:          pushedAt,
		ImageSizeInBytes:       img.ImageSizeInBytes,
		ImageManifestMediaType: img.ImageManifestMediaType,
		ImageStatus:            img.ImageStatus,
		RegistryID:             img.RegistryID,
		RepositoryName:         img.RepositoryName,
	}
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

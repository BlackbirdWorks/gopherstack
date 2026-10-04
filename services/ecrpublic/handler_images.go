package ecrpublic

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) buildImageOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DescribeImages":    service.WrapOp(h.handleDescribeImages),
		"DescribeImageTags": service.WrapOp(h.handleDescribeImageTags),
		"PutImage":          service.WrapOp(h.handlePutImage),
		"BatchDeleteImage":  service.WrapOp(h.handleBatchDeleteImage),
	}
}

type describeImagesInput struct {
	NextToken      string                `json:"nextToken,omitempty"`
	RegistryID     string                `json:"registryId,omitempty"`
	RepositoryName string                `json:"repositoryName"`
	ImageIDs       []ImageIdentifierWire `json:"imageIds,omitempty"`
	MaxResults     int32                 `json:"maxResults,omitempty"`
}

type describeImagesOutput struct {
	NextToken    string            `json:"nextToken,omitempty"`
	ImageDetails []ImageDetailWire `json:"imageDetails"`
}

func (h *Handler) handleDescribeImages(_ context.Context, in *describeImagesInput) (*describeImagesOutput, error) {
	ids := make([]ImageIdentifier, 0, len(in.ImageIDs))
	for _, w := range in.ImageIDs {
		ids = append(ids, imageIdentifierFromWire(w))
	}

	details, err := h.Backend.DescribeImages(in.RegistryID, in.RepositoryName, ids)
	if err != nil {
		return nil, err
	}

	out := make([]ImageDetailWire, 0, len(details))
	for _, d := range details {
		out = append(out, toImageDetailWire(d))
	}

	return &describeImagesOutput{ImageDetails: out}, nil
}

type describeImageTagsInput struct {
	NextToken      string `json:"nextToken,omitempty"`
	RegistryID     string `json:"registryId,omitempty"`
	RepositoryName string `json:"repositoryName"`
	MaxResults     int32  `json:"maxResults,omitempty"`
}

type describeImageTagsOutput struct {
	NextToken       string               `json:"nextToken,omitempty"`
	ImageTagDetails []ImageTagDetailWire `json:"imageTagDetails"`
}

func (h *Handler) handleDescribeImageTags(
	_ context.Context, in *describeImageTagsInput,
) (*describeImageTagsOutput, error) {
	details, err := h.Backend.DescribeImageTags(in.RegistryID, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	out := make([]ImageTagDetailWire, 0, len(details))
	for _, d := range details {
		out = append(out, toImageTagDetailWire(d))
	}

	return &describeImageTagsOutput{ImageTagDetails: out}, nil
}

type putImageInput struct {
	ImageDigest            string `json:"imageDigest,omitempty"`
	ImageManifest          string `json:"imageManifest"`
	ImageManifestMediaType string `json:"imageManifestMediaType,omitempty"`
	ImageTag               string `json:"imageTag,omitempty"`
	RegistryID             string `json:"registryId,omitempty"`
	RepositoryName         string `json:"repositoryName"`
}

type putImageOutput struct {
	Image ImageWire `json:"image"`
}

func (h *Handler) handlePutImage(_ context.Context, in *putImageInput) (*putImageOutput, error) {
	img, err := h.Backend.PutImage(in.RegistryID, in.RepositoryName, PutImageRequest{
		ImageDigest:            in.ImageDigest,
		ImageManifest:          in.ImageManifest,
		ImageManifestMediaType: in.ImageManifestMediaType,
		ImageTag:               in.ImageTag,
	})
	if err != nil {
		return nil, err
	}

	return &putImageOutput{
		Image: ImageWire{
			ImageID:                ImageIdentifierWire{ImageDigest: img.ImageDigest, ImageTag: in.ImageTag},
			ImageManifest:          img.ImageManifest,
			ImageManifestMediaType: img.ImageManifestMediaType,
			RegistryID:             img.RegistryID,
			RepositoryName:         img.RepositoryName,
		},
	}, nil
}

type batchDeleteImageInput struct {
	RegistryID     string                `json:"registryId,omitempty"`
	RepositoryName string                `json:"repositoryName"`
	ImageIDs       []ImageIdentifierWire `json:"imageIds"`
}

type batchDeleteImageOutput struct {
	Failures []ImageFailureWire    `json:"failures,omitempty"`
	ImageIDs []ImageIdentifierWire `json:"imageIds"`
}

func (h *Handler) handleBatchDeleteImage(
	_ context.Context, in *batchDeleteImageInput,
) (*batchDeleteImageOutput, error) {
	ids := make([]ImageIdentifier, 0, len(in.ImageIDs))
	for _, w := range in.ImageIDs {
		ids = append(ids, imageIdentifierFromWire(w))
	}

	deleted, failures, err := h.Backend.BatchDeleteImage(in.RegistryID, in.RepositoryName, ids)
	if err != nil {
		return nil, err
	}

	outIDs := make([]ImageIdentifierWire, 0, len(deleted))
	for _, id := range deleted {
		outIDs = append(outIDs, toImageIdentifierWire(id))
	}

	outFailures := make([]ImageFailureWire, 0, len(failures))
	for _, f := range failures {
		outFailures = append(outFailures, toImageFailureWire(f))
	}

	return &batchDeleteImageOutput{Failures: outFailures, ImageIDs: outIDs}, nil
}

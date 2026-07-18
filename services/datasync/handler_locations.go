package datasync

import (
	"context"
	"fmt"
)

// --- Location (S3) operations ---

type s3ConfigInput struct {
	BucketAccessRoleArn string `json:"BucketAccessRoleArn"`
}

type createLocationS3Input struct {
	S3Config       *s3ConfigInput `json:"S3Config"`
	S3BucketArn    string         `json:"S3BucketArn"`
	Subdirectory   string         `json:"Subdirectory"`
	S3StorageClass string         `json:"S3StorageClass"`
	Tags           []tagInput     `json:"Tags"`
}

type createLocationS3Output struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationS3(
	_ context.Context,
	in *createLocationS3Input,
) (*createLocationS3Output, error) {
	if in.S3BucketArn == "" {
		return nil, fmt.Errorf("%w: S3BucketArn is required", errInvalidRequest)
	}

	if in.S3Config == nil {
		return nil, fmt.Errorf("%w: S3Config is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	cfg := S3Config{BucketAccessRoleArn: in.S3Config.BucketAccessRoleArn}

	l, err := h.Backend.CreateLocationS3(in.Subdirectory, in.S3BucketArn, in.S3StorageClass, cfg, tags)
	if err != nil {
		return nil, err
	}

	return &createLocationS3Output{LocationArn: l.LocationArn}, nil
}

type describeLocationS3Input struct {
	LocationArn string `json:"LocationArn"`
}

type s3ConfigOutput struct {
	BucketAccessRoleArn string `json:"BucketAccessRoleArn"`
}

type describeLocationS3Output struct {
	S3Config       *s3ConfigOutput `json:"S3Config,omitempty"`
	LocationArn    string          `json:"LocationArn"`
	LocationURI    string          `json:"LocationUri"`
	S3BucketArn    string          `json:"S3BucketArn"`
	Subdirectory   string          `json:"Subdirectory,omitempty"`
	S3StorageClass string          `json:"S3StorageClass,omitempty"`
	CreationTime   int64           `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationS3(
	_ context.Context,
	in *describeLocationS3Input,
) (*describeLocationS3Output, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationS3(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationS3Output{
		LocationArn:    l.LocationArn,
		LocationURI:    l.LocationURI,
		S3BucketArn:    l.S3BucketArn,
		Subdirectory:   l.Subdirectory,
		S3StorageClass: l.S3StorageClass,
		CreationTime:   l.CreationTime.Unix(),
	}

	if l.S3Config.BucketAccessRoleArn != "" {
		out.S3Config = &s3ConfigOutput{BucketAccessRoleArn: l.S3Config.BucketAccessRoleArn}
	}

	return out, nil
}

type deleteLocationInput struct {
	LocationArn string `json:"LocationArn"`
}

type deleteLocationOutput struct{}

func (h *Handler) handleDeleteLocation(_ context.Context, in *deleteLocationInput) (*deleteLocationOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteLocation(in.LocationArn); err != nil {
		return nil, err
	}

	return &deleteLocationOutput{}, nil
}

type listLocationsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type locationListEntryOutput struct {
	LocationArn  string `json:"LocationArn"`
	LocationURI  string `json:"LocationUri"`
	CreationTime int64  `json:"CreationTime"`
}

type listLocationsOutput struct {
	NextToken string                    `json:"NextToken,omitempty"`
	Locations []locationListEntryOutput `json:"Locations"`
}

func (h *Handler) handleListLocations(_ context.Context, in *listLocationsInput) (*listLocationsOutput, error) {
	locations, nextToken, err := h.Backend.ListLocations(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]locationListEntryOutput, 0, len(locations))
	for _, l := range locations {
		out = append(out, locationListEntryOutput{
			LocationArn:  l.LocationArn,
			LocationURI:  l.LocationURI,
			CreationTime: l.CreationTime.Unix(),
		})
	}

	return &listLocationsOutput{Locations: out, NextToken: nextToken}, nil
}

// --- UpdateLocationS3 ---

type updateLocationS3Input struct {
	S3Config       *s3ConfigInput `json:"S3Config"`
	LocationArn    string         `json:"LocationArn"`
	Subdirectory   string         `json:"Subdirectory,omitempty"`
	S3StorageClass string         `json:"S3StorageClass,omitempty"`
}

type updateLocationS3Output struct{}

func (h *Handler) handleUpdateLocationS3(
	_ context.Context,
	in *updateLocationS3Input,
) (*updateLocationS3Output, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	var cfg S3Config
	if in.S3Config != nil {
		cfg.BucketAccessRoleArn = in.S3Config.BucketAccessRoleArn
	}

	if err := h.Backend.UpdateLocationS3(in.LocationArn, in.Subdirectory, in.S3StorageClass, cfg); err != nil {
		return nil, err
	}

	return &updateLocationS3Output{}, nil
}

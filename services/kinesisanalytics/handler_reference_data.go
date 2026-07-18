package kinesisanalytics

import (
	"context"
	"fmt"
)

func (h *Handler) handleAddApplicationReferenceDataSource(
	ctx context.Context,
	in *addApplicationReferenceDataSourceInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.ReferenceDataSource == nil {
		return nil, fmt.Errorf("%w: ReferenceDataSource is required", ErrValidation)
	}

	rds := in.ReferenceDataSource

	if rds.TableName == "" {
		return nil, fmt.Errorf("%w: ReferenceDataSource.TableName is required", ErrValidation)
	}

	if rds.S3ReferenceDataSource == nil {
		return nil, fmt.Errorf("%w: ReferenceDataSource.S3ReferenceDataSource is required", ErrValidation)
	}

	if rds.S3ReferenceDataSource.BucketARN == "" {
		return nil, fmt.Errorf("%w: S3ReferenceDataSource.BucketARN is required", ErrValidation)
	}

	if rds.S3ReferenceDataSource.FileKey == "" {
		return nil, fmt.Errorf("%w: S3ReferenceDataSource.FileKey is required", ErrValidation)
	}

	if rds.S3ReferenceDataSource.ReferenceRoleARN == "" {
		return nil, fmt.Errorf("%w: S3ReferenceDataSource.ReferenceRoleARN is required", ErrValidation)
	}

	if rds.ReferenceSchema == nil {
		return nil, fmt.Errorf("%w: ReferenceDataSource.ReferenceSchema is required", ErrValidation)
	}

	var ref ReferenceDataSourceDescription

	ref.TableName = rds.TableName
	ref.S3ReferenceDataSourceDescription = &S3ReferenceDataSourceDesc{
		BucketARN:        rds.S3ReferenceDataSource.BucketARN,
		FileKey:          rds.S3ReferenceDataSource.FileKey,
		ReferenceRoleARN: rds.S3ReferenceDataSource.ReferenceRoleARN,
	}

	schema := convertSourceSchema(rds.ReferenceSchema)
	ref.ReferenceSchema = &schema

	if err := h.Backend.AddApplicationReferenceDataSource(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, ref,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDeleteApplicationReferenceDataSource(
	ctx context.Context,
	in *deleteApplicationReferenceDataSourceInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.ReferenceID == "" {
		return nil, errReferenceID
	}

	if err := h.Backend.DeleteApplicationReferenceDataSource(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.ReferenceID,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

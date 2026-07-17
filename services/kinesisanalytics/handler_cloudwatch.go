package kinesisanalytics

import (
	"context"
	"fmt"
)

func (h *Handler) handleAddApplicationCloudWatchLoggingOption(
	ctx context.Context,
	in *addApplicationCloudWatchLoggingOptionInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.CloudWatchLoggingOption == nil {
		return nil, fmt.Errorf("%w: CloudWatchLoggingOption is required", ErrValidation)
	}

	if in.CloudWatchLoggingOption.LogStreamARN == "" {
		return nil, fmt.Errorf("%w: CloudWatchLoggingOption.LogStreamARN is required", ErrValidation)
	}

	if in.CloudWatchLoggingOption.RoleARN == "" {
		return nil, fmt.Errorf("%w: CloudWatchLoggingOption.RoleARN is required", ErrValidation)
	}

	opt := CloudWatchLoggingOptionDesc{
		LogStreamARN: in.CloudWatchLoggingOption.LogStreamARN,
		RoleARN:      in.CloudWatchLoggingOption.RoleARN,
	}

	if err := h.Backend.AddApplicationCloudWatchLoggingOption(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, opt,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDeleteApplicationCloudWatchLoggingOption(
	ctx context.Context,
	in *deleteApplicationCloudWatchLoggingOptionInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.CloudWatchLoggingOptionID == "" {
		return nil, errCWLOptionID
	}

	if err := h.Backend.DeleteApplicationCloudWatchLoggingOption(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.CloudWatchLoggingOptionID,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

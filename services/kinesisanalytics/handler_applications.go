package kinesisanalytics

import (
	"context"
	"fmt"
	"time"
)

func (h *Handler) handleCreateApplication(
	ctx context.Context,
	in *createApplicationInput,
) (*createApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	tags := make(map[string]string, len(in.Tags))

	for _, t := range in.Tags {
		tags[t.Key] = t.Value
	}

	inputs := make([]InputDescription, 0, len(in.Inputs))

	for i := range in.Inputs {
		desc, err := convertInputConfig(&in.Inputs[i])
		if err != nil {
			return nil, err
		}

		inputs = append(inputs, desc)
	}

	outputs := make([]OutputDescription, 0, len(in.Outputs))

	for i := range in.Outputs {
		desc, err := convertOutputConfig(&in.Outputs[i])
		if err != nil {
			return nil, err
		}

		outputs = append(outputs, desc)
	}

	cwlOptions := make([]CloudWatchLoggingOptionDesc, 0, len(in.CloudWatchLoggingOptions))

	for _, cwl := range in.CloudWatchLoggingOptions {
		if cwl.LogStreamARN == "" {
			return nil, fmt.Errorf("%w: CloudWatchLoggingOptions[].LogStreamARN is required", ErrValidation)
		}

		if cwl.RoleARN == "" {
			return nil, fmt.Errorf("%w: CloudWatchLoggingOptions[].RoleARN is required", ErrValidation)
		}

		cwlOptions = append(cwlOptions, CloudWatchLoggingOptionDesc{
			LogStreamARN: cwl.LogStreamARN,
			RoleARN:      cwl.RoleARN,
		})
	}

	app, err := h.Backend.CreateApplication(
		ctx,
		in.ApplicationName,
		in.ApplicationDescription,
		in.ApplicationCode,
		inputs,
		outputs,
		cwlOptions,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createApplicationOutput{
		ApplicationSummary: applicationSummary{
			ApplicationARN:    app.ApplicationARN,
			ApplicationName:   app.ApplicationName,
			ApplicationStatus: app.ApplicationStatus,
		},
	}, nil
}

func (h *Handler) handleDeleteApplication(
	ctx context.Context,
	in *deleteApplicationInput,
) (*struct{}, error) {
	ts := time.Unix(int64(in.CreateTimestamp), 0).UTC()

	if err := h.Backend.DeleteApplication(ctx, in.ApplicationName, &ts); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDescribeApplication(
	ctx context.Context,
	in *describeApplicationInput,
) (*describeApplicationOutput, error) {
	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return nil, err
	}

	return &describeApplicationOutput{
		ApplicationDetail: toApplicationDetail(app),
	}, nil
}

func (h *Handler) handleListApplications(
	ctx context.Context,
	in *listApplicationsInput,
) (*listApplicationsOutput, error) {
	apps, hasMore, err := h.Backend.ListApplications(ctx, in.ExclusiveStartApplicationName, in.Limit)
	if err != nil {
		return nil, err
	}

	summaries := make([]applicationSummary, 0, len(apps))

	for _, app := range apps {
		summaries = append(summaries, applicationSummary{
			ApplicationARN:    app.ApplicationARN,
			ApplicationName:   app.ApplicationName,
			ApplicationStatus: app.ApplicationStatus,
		})
	}

	return &listApplicationsOutput{
		ApplicationSummaries: summaries,
		HasMoreApplications:  hasMore,
	}, nil
}

func (h *Handler) handleStartApplication(
	ctx context.Context,
	in *startApplicationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if err := h.Backend.StartApplication(ctx, in.ApplicationName, in.InputConfigurations); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleStopApplication(
	ctx context.Context,
	in *stopApplicationInput,
) (*struct{}, error) {
	if err := h.Backend.StopApplication(ctx, in.ApplicationName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleUpdateApplication(
	ctx context.Context,
	in *updateApplicationInput,
) (*describeApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	app, err := h.Backend.UpdateApplication(
		ctx,
		in.ApplicationName,
		in.CurrentApplicationVersionID,
		in.ApplicationUpdate,
	)
	if err != nil {
		return nil, err
	}

	return &describeApplicationOutput{ApplicationDetail: toApplicationDetail(app)}, nil
}

// toApplicationDetail converts an Application to the API detail struct.
// Timestamps are returned as epoch seconds with sub-second precision (float64).
func toApplicationDetail(app *Application) applicationDetail {
	detail := applicationDetail{
		ApplicationARN:                      app.ApplicationARN,
		ApplicationName:                     app.ApplicationName,
		ApplicationStatus:                   app.ApplicationStatus,
		ApplicationVersionID:                app.ApplicationVersionID,
		ApplicationCode:                     app.ApplicationCode,
		ApplicationDescription:              app.ApplicationDescription,
		CloudWatchLoggingOptionDescriptions: app.CloudWatchLoggingOptions,
		InputDescriptions:                   app.Inputs,
		OutputDescriptions:                  app.Outputs,
		ReferenceDataSourceDescriptions:     app.ReferenceDataSources,
	}

	if app.CreateTimestamp != nil {
		detail.CreateTimestamp = float64(app.CreateTimestamp.UnixNano()) / nanosPerSecond
	}

	if app.LastUpdateTimestamp != nil {
		detail.LastUpdateTimestamp = float64(app.LastUpdateTimestamp.UnixNano()) / nanosPerSecond
	}

	return detail
}

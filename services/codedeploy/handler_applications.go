package codedeploy

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type createApplicationInput struct {
	ApplicationName string     `json:"applicationName"`
	ComputePlatform string     `json:"computePlatform"`
	Tags            []tagEntry `json:"tags"`
}

type createApplicationOutput struct {
	ApplicationID string `json:"applicationId"`
}

func (h *Handler) handleCreateApplication(
	_ context.Context,
	in *createApplicationInput,
) (*createApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", ErrApplicationNameRequired)
	}

	if in.ComputePlatform == "" {
		in.ComputePlatform = computePlatformServer
	}

	app, err := h.Backend.CreateApplication(in.ApplicationName, in.ComputePlatform, tagEntriesToMap(in.Tags))
	if err != nil {
		return nil, err
	}

	return &createApplicationOutput{ApplicationID: app.ApplicationID}, nil
}

type getApplicationInput struct {
	ApplicationName string `json:"applicationName"`
}

type applicationInfo struct {
	ApplicationID   string  `json:"applicationId"`
	ApplicationName string  `json:"applicationName"`
	ComputePlatform string  `json:"computePlatform"`
	CreateTime      float64 `json:"createTime"`
}

type getApplicationOutput struct {
	Application applicationInfo `json:"application"`
}

func (h *Handler) handleGetApplication(
	_ context.Context,
	in *getApplicationInput,
) (*getApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", ErrApplicationNameRequired)
	}

	app, err := h.Backend.GetApplication(in.ApplicationName)
	if err != nil {
		return nil, err
	}

	return &getApplicationOutput{
		Application: applicationInfo{
			ApplicationID:   app.ApplicationID,
			ApplicationName: app.ApplicationName,
			ComputePlatform: app.ComputePlatform,
			CreateTime:      awstime.Epoch(app.CreationTime),
		},
	}, nil
}

type listApplicationsInput struct{}

type listApplicationsOutput struct {
	Applications []string `json:"applications"`
}

func (h *Handler) handleListApplications(
	_ context.Context,
	_ *listApplicationsInput,
) (*listApplicationsOutput, error) {
	return &listApplicationsOutput{Applications: h.Backend.ListApplications()}, nil
}

type deleteApplicationInput struct {
	ApplicationName string `json:"applicationName"`
}

type deleteApplicationOutput struct{}

func (h *Handler) handleDeleteApplication(
	_ context.Context,
	in *deleteApplicationInput,
) (*deleteApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", ErrApplicationNameRequired)
	}

	if err := h.Backend.DeleteApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &deleteApplicationOutput{}, nil
}

type updateApplicationInput struct {
	ApplicationName    string `json:"applicationName"`
	NewApplicationName string `json:"newApplicationName"`
}

type updateApplicationOutput struct{}

func (h *Handler) handleUpdateApplication(
	_ context.Context,
	in *updateApplicationInput,
) (*updateApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", ErrApplicationNameRequired)
	}

	if err := h.Backend.UpdateApplication(in.ApplicationName, in.NewApplicationName); err != nil {
		return nil, err
	}

	return &updateApplicationOutput{}, nil
}

type batchGetApplicationsInput struct {
	ApplicationNames []string `json:"applicationNames"`
}

type batchGetApplicationsOutput struct {
	ApplicationsInfo []applicationInfo `json:"applicationsInfo"`
}

func (h *Handler) handleBatchGetApplications(
	_ context.Context,
	in *batchGetApplicationsInput,
) (*batchGetApplicationsOutput, error) {
	if len(in.ApplicationNames) == 0 {
		return nil, fmt.Errorf("%w: applicationNames is required", ErrApplicationNameRequired)
	}

	apps := h.Backend.BatchGetApplications(in.ApplicationNames)

	infos := make([]applicationInfo, 0, len(apps))
	for _, app := range apps {
		infos = append(infos, applicationInfo{
			ApplicationID:   app.ApplicationID,
			ApplicationName: app.ApplicationName,
			ComputePlatform: app.ComputePlatform,
			CreateTime:      awstime.Epoch(app.CreationTime),
		})
	}

	return &batchGetApplicationsOutput{ApplicationsInfo: infos}, nil
}

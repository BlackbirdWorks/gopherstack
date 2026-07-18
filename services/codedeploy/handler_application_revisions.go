package codedeploy

import (
	"context"
	"fmt"
)

type revisionInfoOutput struct {
	RevisionLocation revisionLocationInput `json:"revisionLocation"`
}

type batchGetApplicationRevisionsInput struct {
	ApplicationName string                  `json:"applicationName"`
	Revisions       []revisionLocationInput `json:"revisions"`
}

type batchGetApplicationRevisionsOutput struct {
	ApplicationName string               `json:"applicationName"`
	ErrorMessage    string               `json:"errorMessage,omitempty"`
	Revisions       []revisionInfoOutput `json:"revisions"`
}

func (h *Handler) handleBatchGetApplicationRevisions(
	_ context.Context,
	in *batchGetApplicationRevisionsInput,
) (*batchGetApplicationRevisionsOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	appName, err := h.Backend.BatchGetApplicationRevisions(in.ApplicationName, len(in.Revisions))
	if err != nil {
		return nil, err
	}

	revisions := make([]revisionInfoOutput, 0, len(in.Revisions))
	for _, r := range in.Revisions {
		revisions = append(revisions, revisionInfoOutput{RevisionLocation: r})
	}

	return &batchGetApplicationRevisionsOutput{
		ApplicationName: appName,
		Revisions:       revisions,
	}, nil
}

type registerApplicationRevisionInput struct {
	ApplicationName string                `json:"applicationName"`
	Description     string                `json:"description"`
	Revision        revisionLocationInput `json:"revision"`
}

type registerApplicationRevisionOutput struct{}

func (h *Handler) handleRegisterApplicationRevision(
	_ context.Context,
	in *registerApplicationRevisionInput,
) (*registerApplicationRevisionOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &registerApplicationRevisionOutput{}, nil
}

type getApplicationRevisionInput struct {
	ApplicationName string                `json:"applicationName"`
	Revision        revisionLocationInput `json:"revision"`
}

type getApplicationRevisionOutput struct {
	ApplicationName string                `json:"applicationName"`
	Revision        revisionLocationInput `json:"revision"`
}

func (h *Handler) handleGetApplicationRevision(
	_ context.Context,
	in *getApplicationRevisionInput,
) (*getApplicationRevisionOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &getApplicationRevisionOutput{
		ApplicationName: in.ApplicationName,
		Revision:        in.Revision,
	}, nil
}

type listApplicationRevisionsInput struct {
	ApplicationName string `json:"applicationName"`
}

type listApplicationRevisionsOutput struct {
	Revisions []revisionLocationInput `json:"revisions"`
}

func (h *Handler) handleListApplicationRevisions(
	_ context.Context,
	in *listApplicationRevisionsInput,
) (*listApplicationRevisionsOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &listApplicationRevisionsOutput{Revisions: []revisionLocationInput{}}, nil
}

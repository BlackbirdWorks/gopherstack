package codedeploy

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// genericRevisionInfoOutput is the wire format for GenericRevisionInfo.
type genericRevisionInfoOutput struct {
	Description      string   `json:"description,omitempty"`
	DeploymentGroups []string `json:"deploymentGroups,omitempty"`
	RegisterTime     float64  `json:"registerTime,omitempty"`
	FirstUsedTime    float64  `json:"firstUsedTime,omitempty"`
	LastUsedTime     float64  `json:"lastUsedTime,omitempty"`
}

// genericRevisionInfoToWire converts a backend ApplicationRevision to its
// wire GenericRevisionInfo representation.
func genericRevisionInfoToWire(rev *ApplicationRevision) *genericRevisionInfoOutput {
	if rev == nil {
		return nil
	}

	out := &genericRevisionInfoOutput{
		Description:      rev.Description,
		RegisterTime:     awstime.Epoch(rev.RegisterTime),
		DeploymentGroups: rev.DeploymentGroups,
	}

	if rev.FirstUsedTime != nil {
		out.FirstUsedTime = awstime.Epoch(*rev.FirstUsedTime)
	}

	if rev.LastUsedTime != nil {
		out.LastUsedTime = awstime.Epoch(*rev.LastUsedTime)
	}

	return out
}

type revisionInfoOutput struct {
	GenericRevisionInfo *genericRevisionInfoOutput `json:"genericRevisionInfo,omitempty"`
	RevisionLocation    revisionLocationInput      `json:"revisionLocation"`
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
		return nil, fmt.Errorf("%w: applicationName is required", ErrApplicationNameRequired)
	}

	backendRevisions := make([]RevisionLocation, 0, len(in.Revisions))
	for _, r := range in.Revisions {
		backendRevisions = append(backendRevisions, *revisionFromWire(&r))
	}

	found, err := h.Backend.BatchGetApplicationRevisions(in.ApplicationName, backendRevisions)
	if err != nil {
		return nil, err
	}

	revisions := make([]revisionInfoOutput, 0, len(in.Revisions))
	for i, r := range in.Revisions {
		entry := revisionInfoOutput{RevisionLocation: r}
		if rev, ok := found[applicationRevisionKey(in.ApplicationName, backendRevisions[i])]; ok {
			entry.GenericRevisionInfo = genericRevisionInfoToWire(rev)
		}

		revisions = append(revisions, entry)
	}

	return &batchGetApplicationRevisionsOutput{
		ApplicationName: in.ApplicationName,
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
		return nil, fmt.Errorf("%w: applicationName is required", ErrApplicationNameRequired)
	}

	if err := h.Backend.RegisterApplicationRevision(
		in.ApplicationName, *revisionFromWire(&in.Revision), in.Description,
	); err != nil {
		return nil, err
	}

	return &registerApplicationRevisionOutput{}, nil
}

type getApplicationRevisionInput struct {
	ApplicationName string                `json:"applicationName"`
	Revision        revisionLocationInput `json:"revision"`
}

type getApplicationRevisionOutput struct {
	RevisionInfo    *genericRevisionInfoOutput `json:"revisionInfo,omitempty"`
	ApplicationName string                     `json:"applicationName"`
	Revision        revisionLocationInput      `json:"revision"`
}

func (h *Handler) handleGetApplicationRevision(
	_ context.Context,
	in *getApplicationRevisionInput,
) (*getApplicationRevisionOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", ErrApplicationNameRequired)
	}

	rev, err := h.Backend.GetApplicationRevision(in.ApplicationName, *revisionFromWire(&in.Revision))
	if err != nil {
		return nil, err
	}

	return &getApplicationRevisionOutput{
		ApplicationName: in.ApplicationName,
		Revision:        in.Revision,
		RevisionInfo:    genericRevisionInfoToWire(rev),
	}, nil
}

type listApplicationRevisionsInput struct {
	ApplicationName string `json:"applicationName"`
	Deployed        string `json:"deployed"`
	S3Bucket        string `json:"s3Bucket"`
	S3KeyPrefix     string `json:"s3KeyPrefix"`
	SortBy          string `json:"sortBy"`
	SortOrder       string `json:"sortOrder"`
}

type listApplicationRevisionsOutput struct {
	Revisions []revisionLocationInput `json:"revisions"`
}

func (h *Handler) handleListApplicationRevisions(
	_ context.Context,
	in *listApplicationRevisionsInput,
) (*listApplicationRevisionsOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", ErrApplicationNameRequired)
	}

	revs, err := h.Backend.ListApplicationRevisions(in.ApplicationName, RevisionListFilter{
		Deployed:    in.Deployed,
		S3Bucket:    in.S3Bucket,
		S3KeyPrefix: in.S3KeyPrefix,
		SortBy:      in.SortBy,
		SortOrder:   in.SortOrder,
	})
	if err != nil {
		return nil, err
	}

	out := make([]revisionLocationInput, 0, len(revs))
	for _, rev := range revs {
		out = append(out, *revisionToWire(&rev.Revision))
	}

	return &listApplicationRevisionsOutput{Revisions: out}, nil
}

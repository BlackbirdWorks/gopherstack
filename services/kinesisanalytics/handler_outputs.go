package kinesisanalytics

import "context"

func (h *Handler) handleAddApplicationOutput(
	ctx context.Context,
	in *addApplicationOutputInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	desc, err := convertOutputConfig(in.Output)
	if err != nil {
		return nil, err
	}

	if addErr := h.Backend.AddApplicationOutput(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, desc,
	); addErr != nil {
		return nil, addErr
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDeleteApplicationOutput(
	ctx context.Context,
	in *deleteApplicationOutputInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.OutputID == "" {
		return nil, errOutputID
	}

	if err := h.Backend.DeleteApplicationOutput(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.OutputID,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

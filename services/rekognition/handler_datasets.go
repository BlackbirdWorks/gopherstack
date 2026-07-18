package rekognition

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) datasetOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateDataset":            service.WrapOp(h.handleCreateDataset),
		"DeleteDataset":            service.WrapOp(h.handleDeleteDataset),
		"DescribeDataset":          service.WrapOp(h.handleDescribeDataset),
		"ListDatasetEntries":       service.WrapOp(h.handleListDatasetEntries),
		"ListDatasetLabels":        service.WrapOp(h.handleListDatasetLabels),
		"UpdateDatasetEntries":     service.WrapOp(h.handleUpdateDatasetEntries),
		"DistributeDatasetEntries": service.WrapOp(h.handleDistributeDatasetEntries),
	}
}

// =============================================================================
// Datasets
// =============================================================================

type createDatasetReq struct {
	ProjectArn  string `json:"ProjectArn"`
	DatasetType string `json:"DatasetType"`
}

type createDatasetResp struct {
	DatasetArn string `json:"DatasetArn"`
}

func (h *Handler) handleCreateDataset(_ context.Context, req *createDatasetReq) (*createDatasetResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if req.DatasetType == "" {
		return nil, fmt.Errorf("%w: DatasetType is required", ErrValidation)
	}

	ds, err := h.Backend.CreateDataset(req.ProjectArn, req.DatasetType)
	if err != nil {
		return nil, err
	}

	return &createDatasetResp{DatasetArn: ds.DatasetARN}, nil
}

type deleteDatasetReq struct {
	DatasetArn string `json:"DatasetArn"`
}

func (h *Handler) handleDeleteDataset(_ context.Context, req *deleteDatasetReq) (*struct{}, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteDataset(req.DatasetArn); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeDatasetReq struct {
	DatasetArn string `json:"DatasetArn"`
}

type datasetDescription struct {
	DatasetArn           string  `json:"DatasetArn"`
	ProjectArn           string  `json:"ProjectArn"`
	DatasetType          string  `json:"DatasetType"`
	Status               string  `json:"Status"`
	StatusMessage        string  `json:"StatusMessage,omitempty"`
	CreationTimestamp    float64 `json:"CreationTimestamp"`
	LastUpdatedTimestamp float64 `json:"LastUpdatedTimestamp"`
}

type describeDatasetResp struct {
	DatasetDescription datasetDescription `json:"DatasetDescription"`
}

func (h *Handler) handleDescribeDataset(
	_ context.Context, req *describeDatasetReq,
) (*describeDatasetResp, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	ds, err := h.Backend.DescribeDataset(req.DatasetArn)
	if err != nil {
		return nil, err
	}

	return &describeDatasetResp{
		DatasetDescription: datasetDescription{
			DatasetArn:           ds.DatasetARN,
			ProjectArn:           ds.ProjectARN,
			DatasetType:          ds.DatasetType,
			Status:               ds.Status,
			StatusMessage:        ds.StatusMessage,
			CreationTimestamp:    epochSeconds(ds.CreationTimestamp),
			LastUpdatedTimestamp: epochSeconds(ds.LastUpdatedTimestamp),
		},
	}, nil
}

type listDatasetEntriesReq struct {
	DatasetArn string `json:"DatasetArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listDatasetEntriesResp struct {
	NextToken      string   `json:"NextToken,omitempty"`
	DatasetEntries []string `json:"DatasetEntries"`
}

func (h *Handler) handleListDatasetEntries(
	_ context.Context, req *listDatasetEntriesReq,
) (*listDatasetEntriesResp, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	entries, nextToken, err := h.Backend.ListDatasetEntries(req.DatasetArn, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	if entries == nil {
		entries = []string{}
	}

	return &listDatasetEntriesResp{
		DatasetEntries: entries,
		NextToken:      nextToken,
	}, nil
}

type listDatasetLabelsReq struct {
	DatasetArn string `json:"DatasetArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type datasetLabelEntry struct {
	LabelName  string `json:"LabelName"`
	EntryCount int64  `json:"EntryCount"`
}

type listDatasetLabelsResp struct {
	NextToken         string              `json:"NextToken,omitempty"`
	DatasetLabelStats []datasetLabelEntry `json:"DatasetLabelStats"`
}

func (h *Handler) handleListDatasetLabels(
	_ context.Context, req *listDatasetLabelsReq,
) (*listDatasetLabelsResp, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	labels, nextToken, err := h.Backend.ListDatasetLabels(req.DatasetArn, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	entries := make([]datasetLabelEntry, 0, len(labels))
	for _, l := range labels {
		entries = append(entries, datasetLabelEntry{
			LabelName:  l.LabelName,
			EntryCount: l.EntryCount,
		})
	}

	return &listDatasetLabelsResp{
		DatasetLabelStats: entries,
		NextToken:         nextToken,
	}, nil
}

type updateDatasetEntriesReq struct {
	DatasetArn string `json:"DatasetArn"`
	Changes    []byte `json:"Changes"`
}

func (h *Handler) handleUpdateDatasetEntries(
	_ context.Context, req *updateDatasetEntriesReq,
) (*struct{}, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	if err := h.Backend.UpdateDatasetEntries(req.DatasetArn, req.Changes); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type distributeDatasetEntriesReq struct {
	Datasets []struct {
		DatasetArn string `json:"DatasetArn"`
	} `json:"Datasets"`
}

func (h *Handler) handleDistributeDatasetEntries(
	_ context.Context, req *distributeDatasetEntriesReq,
) (*struct{}, error) {
	datasets := make([]DatasetDistribution, 0, len(req.Datasets))
	for _, d := range req.Datasets {
		datasets = append(datasets, DatasetDistribution{DatasetARN: d.DatasetArn})
	}

	if err := h.Backend.DistributeDatasetEntries(datasets); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

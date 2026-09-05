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

// datasetStatsWire mirrors types.DatasetStats (ErrorEntries/LabeledEntries/
// TotalEntries/TotalLabels). StatusMessageCode (a sibling member of
// datasetDescription, not of this type) is a disclosed gap below -- this
// backend has no status-message-code concept to source it from.
type datasetStatsWire struct {
	ErrorEntries   int64 `json:"ErrorEntries"`
	LabeledEntries int64 `json:"LabeledEntries"`
	TotalEntries   int64 `json:"TotalEntries"`
	TotalLabels    int64 `json:"TotalLabels"`
}

// datasetDescription mirrors types.DatasetDescription. DatasetArn/
// ProjectArn/DatasetType are NOT real members of this type (confirmed
// against deserializers.go's DatasetDescription switch, which has no such
// cases) -- kept here anyway as harmless extra fields a real client simply
// never sees (no sensitive data, already known to the caller from the
// request/CreateDataset). StatusMessageCode is a genuine missing member:
// this backend has no status-message-code concept, so it is omitted rather
// than fabricated.
type datasetDescription struct {
	DatasetStats         *datasetStatsWire `json:"DatasetStats,omitempty"`
	DatasetArn           string            `json:"DatasetArn"`
	ProjectArn           string            `json:"ProjectArn"`
	DatasetType          string            `json:"DatasetType"`
	Status               string            `json:"Status"`
	StatusMessage        string            `json:"StatusMessage,omitempty"`
	CreationTimestamp    float64           `json:"CreationTimestamp"`
	LastUpdatedTimestamp float64           `json:"LastUpdatedTimestamp"`
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
			DatasetStats: &datasetStatsWire{
				TotalEntries:   ds.Stats.TotalEntries,
				LabeledEntries: ds.Stats.LabeledEntries,
				TotalLabels:    ds.Stats.TotalLabels,
				ErrorEntries:   ds.Stats.ErrorEntries,
			},
		},
	}, nil
}

type listDatasetEntriesReq struct {
	HasErrors         *bool    `json:"HasErrors"`
	Labeled           *bool    `json:"Labeled"`
	DatasetArn        string   `json:"DatasetArn"`
	NextToken         string   `json:"NextToken"`
	SourceRefContains string   `json:"SourceRefContains"`
	ContainsLabels    []string `json:"ContainsLabels"`
	MaxResults        int32    `json:"MaxResults"`
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

	filter := ListDatasetEntriesFilter{
		ContainsLabels:    req.ContainsLabels,
		HasErrors:         req.HasErrors,
		Labeled:           req.Labeled,
		SourceRefContains: req.SourceRefContains,
	}

	entries, nextToken, err := h.Backend.ListDatasetEntries(req.DatasetArn, filter, req.MaxResults, req.NextToken)
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

// datasetLabelStatsWire mirrors types.DatasetLabelStats. BoundingBoxCount is
// a disclosed gap: this backend's label counts come from -metadata blocks in
// stored manifest entries (see countLabelsFromEntry) with no per-image
// bounding-box-vs-classification distinction to source it from, so it is
// omitted rather than fabricated.
type datasetLabelStatsWire struct {
	EntryCount int64 `json:"EntryCount"`
}

// datasetLabelDescriptionEntry mirrors types.DatasetLabelDescription
// exactly (LabelName, LabelStats -- confirmed against deserializers.go's
// awsAwsjson11_deserializeDocumentDatasetLabelDescription switch).
type datasetLabelDescriptionEntry struct {
	LabelStats *datasetLabelStatsWire `json:"LabelStats,omitempty"`
	LabelName  string                 `json:"LabelName"`
}

// listDatasetLabelsResp previously emitted its collection under the
// fabricated top-level key "DatasetLabelStats" with a flat per-item shape
// (LabelName/EntryCount siblings). The real ListDatasetLabelsOutput key is
// "DatasetLabelDescriptions", and EntryCount nests one level down under
// LabelStats (deserializers.go's ListDatasetLabelsOutput switch has no
// "DatasetLabelStats" case at all) -- a real typed client's
// ListDatasetLabels call silently decoded to an empty slice every time.
type listDatasetLabelsResp struct {
	NextToken                string                         `json:"NextToken,omitempty"`
	DatasetLabelDescriptions []datasetLabelDescriptionEntry `json:"DatasetLabelDescriptions"`
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

	entries := make([]datasetLabelDescriptionEntry, 0, len(labels))
	for _, l := range labels {
		entries = append(entries, datasetLabelDescriptionEntry{
			LabelName:  l.LabelName,
			LabelStats: &datasetLabelStatsWire{EntryCount: l.EntryCount},
		})
	}

	return &listDatasetLabelsResp{
		DatasetLabelDescriptions: entries,
		NextToken:                nextToken,
	}, nil
}

// datasetChangesWire mirrors types.DatasetChanges exactly: a real client
// nests the base64 manifest bytes one level down under "GroundTruth"
// (confirmed against serializers.go's awsAwsjson11_serializeDocumentDatasetChanges,
// which always wraps Changes as {"GroundTruth": <base64>}). The previous flat
// `Changes []byte` field expected "Changes" itself to hold the base64 string
// directly -- a real client's UpdateDatasetEntries call sends a JSON object
// there, which json.Unmarshal into a []byte field hard-errors on: not
// silent-empty but a total op failure for every real caller.
type datasetChangesWire struct {
	GroundTruth []byte `json:"GroundTruth"`
}

type updateDatasetEntriesReq struct {
	Changes    *datasetChangesWire `json:"Changes"`
	DatasetArn string              `json:"DatasetArn"`
}

func (h *Handler) handleUpdateDatasetEntries(
	_ context.Context, req *updateDatasetEntriesReq,
) (*struct{}, error) {
	if req.DatasetArn == "" {
		return nil, fmt.Errorf("%w: DatasetArn is required", ErrValidation)
	}

	if req.Changes == nil {
		return nil, fmt.Errorf("%w: Changes is required", ErrValidation)
	}

	if err := h.Backend.UpdateDatasetEntries(req.DatasetArn, req.Changes.GroundTruth); err != nil {
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

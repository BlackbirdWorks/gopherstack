package glue

import (
	"context"
)

// createPartitionIndexInput holds input for CreatePartitionIndex.
type createPartitionIndexInput struct {
	DatabaseName   string         `json:"DatabaseName"`
	TableName      string         `json:"TableName"`
	PartitionIndex PartitionIndex `json:"PartitionIndex"`
}

func (h *Handler) handleCreatePartitionIndex(
	_ context.Context,
	in *createPartitionIndexInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreatePartitionIndex(
		in.DatabaseName,
		in.TableName,
		in.PartitionIndex,
	)
}

// deletePartitionIndexInput holds input for DeletePartitionIndex.
type deletePartitionIndexInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	IndexName    string `json:"IndexName"`
}

func (h *Handler) handleDeletePartitionIndex(
	_ context.Context,
	in *deletePartitionIndexInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeletePartitionIndex(
		in.DatabaseName,
		in.TableName,
		in.IndexName,
	)
}

// getPartitionIndexesInput holds input for GetPartitionIndexes.
type getPartitionIndexesInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// getPartitionIndexesOutput holds the result for GetPartitionIndexes.
type getPartitionIndexesOutput struct {
	PartitionIndexDescriptorList []*PartitionIndex `json:"PartitionIndexDescriptorList"`
}

func (h *Handler) handleGetPartitionIndexes(
	_ context.Context,
	in *getPartitionIndexesInput,
) (*getPartitionIndexesOutput, error) {
	indexes, err := h.Backend.GetPartitionIndexes(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	return &getPartitionIndexesOutput{PartitionIndexDescriptorList: indexes}, nil
}

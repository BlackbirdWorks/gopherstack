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

// keySchemaElementWire mirrors types.KeySchemaElement: a partition index
// descriptor's Keys are {Name, Type} objects, not bare strings.
type keySchemaElementWire struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
}

// partitionIndexDescriptorWire mirrors types.PartitionIndexDescriptor
// (glue@v1.157.0 types/types.go:8875) -- distinct from types.PartitionIndex
// (the CreatePartitionIndex input shape), whose Keys really is []string.
// GetPartitionIndexes previously reused the input shape for its response
// too, so a real client's decode of Keys ("dt", a bare string) failed
// outright against the object shape the deserializer expects.
type partitionIndexDescriptorWire struct {
	IndexName   string                 `json:"IndexName"`
	IndexStatus string                 `json:"IndexStatus"`
	Keys        []keySchemaElementWire `json:"Keys"`
}

// getPartitionIndexesOutput holds the result for GetPartitionIndexes.
type getPartitionIndexesOutput struct {
	PartitionIndexDescriptorList []partitionIndexDescriptorWire `json:"PartitionIndexDescriptorList"`
}

func (h *Handler) handleGetPartitionIndexes(
	_ context.Context,
	in *getPartitionIndexesInput,
) (*getPartitionIndexesOutput, error) {
	indexes, err := h.Backend.GetPartitionIndexes(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	keyTypes := make(map[string]string)

	if table, tblErr := h.Backend.GetTable(in.DatabaseName, in.TableName); tblErr == nil {
		for _, col := range table.PartitionKeys {
			keyTypes[col.Name] = col.Type
		}
	}

	list := make([]partitionIndexDescriptorWire, 0, len(indexes))

	for _, idx := range indexes {
		keys := make([]keySchemaElementWire, 0, len(idx.Keys))

		for _, k := range idx.Keys {
			typ := keyTypes[k]
			if typ == "" {
				typ = "string"
			}

			keys = append(keys, keySchemaElementWire{Name: k, Type: typ})
		}

		list = append(list, partitionIndexDescriptorWire{
			IndexName:   idx.IndexName,
			IndexStatus: idx.IndexStatus,
			Keys:        keys,
		})
	}

	return &getPartitionIndexesOutput{PartitionIndexDescriptorList: list}, nil
}

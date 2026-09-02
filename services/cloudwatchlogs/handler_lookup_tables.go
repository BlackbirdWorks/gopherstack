package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type createLookupTableInput struct {
	Tags            map[string]string `json:"tags,omitempty"`
	LookupTableName string            `json:"lookupTableName"`
	TableBody       string            `json:"tableBody"`
	QueryID         string            `json:"queryId,omitempty"`
	Description     string            `json:"description,omitempty"`
	KmsKeyID        string            `json:"kmsKeyId,omitempty"`
}

func (h *Handler) handleCreateLookupTable(
	ctx context.Context,
	body []byte,
) (any, error) {
	var in createLookupTableInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	b := cwlBackend(h)
	if b == nil {
		return map[string]any{}, nil
	}

	t, err := b.CreateLookupTable(ctx, in.LookupTableName, in.TableBody, in.Description, in.KmsKeyID, in.QueryID)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		h.setTags(t.LookupTableArn, in.Tags)
	}

	return map[string]any{
		"createdAt":       t.CreatedAt,
		keyLookupTableArn: t.LookupTableArn,
	}, nil
}

type getLookupTableInput struct {
	LookupTableArn string `json:"lookupTableArn"`
}

func (h *Handler) handleGetLookupTable(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getLookupTableInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	b := cwlBackend(h)
	if b == nil {
		return map[string]any{}, nil
	}

	t, err := b.GetLookupTable(in.LookupTableArn)
	if err != nil {
		return nil, err
	}

	return lookupTableGetWireShape(t), nil
}

type updateLookupTableInput struct {
	Description    *string `json:"description,omitempty"`
	KmsKeyID       *string `json:"kmsKeyId,omitempty"`
	LookupTableArn string  `json:"lookupTableArn"`
	TableBody      string  `json:"tableBody"`
	QueryID        string  `json:"queryId,omitempty"`
}

func (h *Handler) handleUpdateLookupTable(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in updateLookupTableInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	b := cwlBackend(h)
	if b == nil {
		return map[string]any{}, nil
	}

	t, err := b.UpdateLookupTable(in.LookupTableArn, in.TableBody, in.Description, in.KmsKeyID, in.QueryID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyLastUpdatedTime: t.LastUpdatedTime,
		keyLookupTableArn:  t.LookupTableArn,
	}, nil
}

type deleteLookupTableInput struct {
	LookupTableArn string `json:"lookupTableArn"`
}

func (h *Handler) handleDeleteLookupTable(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteLookupTableInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteLookupTable(in.LookupTableArn); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type describeLookupTablesInput struct {
	LookupTableNamePrefix string `json:"lookupTableNamePrefix,omitempty"`
	NextToken             string `json:"nextToken,omitempty"`
	MaxResults            int32  `json:"maxResults,omitempty"`
}

func (h *Handler) handleDescribeLookupTables(
	ctx context.Context,
	body []byte,
) (any, error) {
	var in describeLookupTablesInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	b := cwlBackend(h)
	if b == nil {
		return map[string]any{"lookupTables": []any{}}, nil
	}

	tables, nextToken := b.DescribeLookupTables(ctx, in.LookupTableNamePrefix, in.NextToken, int(in.MaxResults))
	out := make([]map[string]any, 0, len(tables))

	for i := range tables {
		out = append(out, lookupTableMetadataWireShape(&tables[i]))
	}

	resp := map[string]any{"lookupTables": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, nil
}

// lookupTableGetWireShape maps a LookupTable to GetLookupTableOutput's shape
// (aws-sdk-go-v2 GetLookupTableOutput: description/kmsKeyId/lastUpdatedTime/
// lookupTableArn/lookupTableName/sizeBytes/tableBody -- the full content,
// unlike lookupTableMetadataWireShape).
func lookupTableGetWireShape(t *LookupTable) map[string]any {
	return map[string]any{
		"description":      t.Description,
		"kmsKeyId":         t.KmsKeyID,
		keyLastUpdatedTime: t.LastUpdatedTime,
		keyLookupTableArn:  t.LookupTableArn,
		"lookupTableName":  t.LookupTableName,
		"sizeBytes":        t.SizeBytes,
		"tableBody":        t.TableBody,
	}
}

// lookupTableMetadataWireShape maps a LookupTable to the real
// types.LookupTable shape used by DescribeLookupTablesOutput.LookupTables:
// metadata only (recordsCount/tableFields), no tableBody.
func lookupTableMetadataWireShape(t *LookupTable) map[string]any {
	return map[string]any{
		"description":      t.Description,
		"kmsKeyId":         t.KmsKeyID,
		keyLastUpdatedTime: t.LastUpdatedTime,
		keyLookupTableArn:  t.LookupTableArn,
		"lookupTableName":  t.LookupTableName,
		"recordsCount":     t.RecordsCount,
		"sizeBytes":        t.SizeBytes,
		"tableFields":      t.TableFields,
	}
}

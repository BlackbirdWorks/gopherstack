package dynamodb

import (
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

const maxVectorIndexUpdatesPerCall = 1

// validateVectorIndexConfig checks one vector index definition against the
// pinned SDK's documented rules; existing is the table's current vector indexes.
func validateVectorIndexConfig(vi models.VectorIndex, existing []models.VectorIndexDescription) error {
	if vi.IndexName == "" {
		return NewValidationException("VectorIndex IndexName is required")
	}

	for _, e := range existing {
		if e.IndexName == vi.IndexName {
			return NewValidationException("Duplicate vector index name: " + vi.IndexName)
		}
	}

	if vi.VectorAttribute == nil || vi.VectorAttribute.AttributeName == "" {
		return NewValidationException("VectorAttribute.AttributeName is required for vector index " + vi.IndexName)
	}

	if vi.Dimensions < 1 {
		return NewValidationException("Dimensions must be a positive integer for vector index " + vi.IndexName)
	}

	if !slices.Contains(vectorDistanceFunctions(), vi.DistanceFunction) {
		return NewValidationException(fmt.Sprintf(
			"Value '%s' at 'distanceFunction' failed to satisfy constraint: "+
				"Member must satisfy enum value set: [COSINE, EUCLIDEAN, DOT_PRODUCT]", vi.DistanceFunction))
	}

	if vi.Projection == nil || !slices.Contains(projectionTypes(), vi.Projection.ProjectionType) {
		return NewValidationException("Projection with a valid ProjectionType is required for vector index " +
			vi.IndexName)
	}

	return validateVectorIndexRelations(vi, existing)
}

func validateVectorIndexRelations(vi models.VectorIndex, existing []models.VectorIndexDescription) error {
	for _, e := range existing {
		if e.VectorAttribute != nil && e.VectorAttribute.AttributeName == vi.VectorAttribute.AttributeName &&
			e.Dimensions != vi.Dimensions {
			return NewValidationException(fmt.Sprintf(
				"Vector indexes on attribute %s must all use the same number of dimensions",
				vi.VectorAttribute.AttributeName))
		}
	}

	for _, el := range vi.SearchSchema {
		if el.AttributeName == "" {
			return NewValidationException("SearchSchema AttributeName is required")
		}

		if el.SearchSchemaElementType != string(types.SearchSchemaElementTypeHash) &&
			el.SearchSchemaElementType != string(types.SearchSchemaElementTypeInlineFilter) {
			return NewValidationException(fmt.Sprintf(
				"Value '%s' at 'searchSchemaElementType' failed to satisfy constraint: "+
					"Member must satisfy enum value set: [HASH, INLINE_FILTER]", el.SearchSchemaElementType))
		}
	}

	return nil
}

func vectorDistanceFunctions() []string {
	return []string{
		string(types.VectorDistanceFunctionCosine),
		string(types.VectorDistanceFunctionEuclidean),
		string(types.VectorDistanceFunctionDotProduct),
	}
}

func projectionTypes() []string {
	return []string{
		string(types.ProjectionTypeAll),
		string(types.ProjectionTypeKeysOnly),
		string(types.ProjectionTypeInclude),
	}
}

// vectorIndexDescription builds the ACTIVE stored description for a new index.
func vectorIndexDescription(tableArn string, vi models.VectorIndex) models.VectorIndexDescription {
	return models.VectorIndexDescription{
		IndexName:        vi.IndexName,
		IndexArn:         indexArn(tableArn, vi.IndexName),
		IndexStatus:      string(types.IndexStatusActive),
		DistanceFunction: vi.DistanceFunction,
		Dimensions:       vi.Dimensions,
		VectorAttribute:  vi.VectorAttribute,
		Projection:       vi.Projection,
		SearchSchema:     slices.Clone(vi.SearchSchema),
	}
}

// buildVectorIndexes validates and builds the stored descriptions for CreateTable.
func buildVectorIndexes(tableArn string, in []models.VectorIndex) ([]models.VectorIndexDescription, error) {
	out := make([]models.VectorIndexDescription, 0, len(in))

	for _, vi := range in {
		if err := validateVectorIndexConfig(vi, out); err != nil {
			return nil, err
		}

		out = append(out, vectorIndexDescription(tableArn, vi))
	}

	return out, nil
}

// applyVectorIndexUpdates applies UpdateTable's VectorIndexUpdates. Caller holds table.mu.
func applyVectorIndexUpdates(table *Table, updates []types.VectorIndexUpdate) error {
	if len(updates) > maxVectorIndexUpdatesPerCall {
		return NewValidationException("You can add or remove one vector index for each UpdateTable operation")
	}

	for _, u := range updates {
		switch {
		case u.Create != nil && u.Delete != nil:
			return NewValidationException("VectorIndexUpdate must specify exactly one of Create or Delete")
		case u.Create != nil:
			vi := models.FromSDKCreateVectorIndexAction(u.Create)
			if err := validateVectorIndexConfig(*vi, table.VectorIndexes); err != nil {
				return err
			}

			table.VectorIndexes = append(table.VectorIndexes, vectorIndexDescription(table.TableArn, *vi))
		case u.Delete != nil:
			name := stringOrEmpty(u.Delete.IndexName)

			idx := slices.IndexFunc(table.VectorIndexes, func(d models.VectorIndexDescription) bool {
				return d.IndexName == name
			})
			if idx < 0 {
				return NewResourceNotFoundException(
					"Requested resource not found: Vector index: " + name + " not found",
				)
			}

			table.VectorIndexes = slices.Delete(table.VectorIndexes, idx, idx+1)
		default:
			return NewValidationException("VectorIndexUpdate must specify Create or Delete")
		}
	}

	return nil
}

func stringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

// vectorIndexDescriptionsLive returns copies of the table's vector indexes with
// ItemCount counted from the items that hold a well-formed vector. Caller holds table.mu.
func vectorIndexDescriptionsLive(table *Table) []models.VectorIndexDescription {
	if len(table.VectorIndexes) == 0 {
		return nil
	}

	out := slices.Clone(table.VectorIndexes)

	for i := range out {
		out[i].ItemCount = 0
		out[i].IndexSizeBytes = 0

		attr := out[i].VectorAttribute.AttributeName
		for _, item := range table.Items {
			if vec, ok := itemVector(item, attr, int(out[i].Dimensions)); ok {
				out[i].ItemCount++
				out[i].IndexSizeBytes += int64(len(vec)) * float32Bytes
			}
		}
	}

	return out
}

// rebindVectorIndexes copies descriptions re-addressed to the restored table's ARN.
func rebindVectorIndexes(in []models.VectorIndexDescription, tableArn string) []models.VectorIndexDescription {
	out := slices.Clone(in)
	for i := range out {
		out[i].IndexArn = indexArn(tableArn, out[i].IndexName)
	}

	return out
}

// resolveVectorIndexOverride keeps the source indexes named in override (nil keeps all).
func resolveVectorIndexOverride(
	source []models.VectorIndexDescription,
	override []types.VectorIndex,
) ([]models.VectorIndexDescription, error) {
	if override == nil {
		return slices.Clone(source), nil
	}

	kept := make([]models.VectorIndexDescription, 0, len(override))

	for _, o := range override {
		name := stringOrEmpty(o.IndexName)

		idx := slices.IndexFunc(source, func(d models.VectorIndexDescription) bool { return d.IndexName == name })
		if idx < 0 {
			return nil, NewValidationException(
				"Vector index override " + name + " does not match a source vector index",
			)
		}

		kept = append(kept, source[idx])
	}

	return kept, nil
}

func vectorDescriptionsRLocked(table *Table) []models.VectorIndexDescription {
	table.mu.RLock("vectorDescriptions")
	defer table.mu.RUnlock()

	return vectorIndexDescriptionsLive(table)
}

package sagemaker

import (
	"fmt"
	"maps"
)

// ---------------------------------------------------------------------------
// Feature store record operations
// ---------------------------------------------------------------------------

// FeatureRecord represents a stored feature record.
type FeatureRecord struct {
	// Record maps feature name → feature value
	Record map[string]string `json:"Record"`
}

// FeatureMetadata stores metadata (description + parameters) for a feature in a group.
type FeatureMetadata struct {
	Description string            `json:"Description,omitempty"`
	Parameters  map[string]string `json:"Parameters,omitempty"`
	FeatureName string            `json:"FeatureName"`
	FeatureType string            `json:"FeatureType,omitempty"`
}

// featureRecordKey builds the map key for a feature record.
func featureRecordKey(featureGroupName, recordIDValue string) string {
	return featureGroupName + "|" + recordIDValue
}

// featureMetaKey builds the map key for feature metadata.
func featureMetaKey(featureGroupName, featureName string) string {
	return featureGroupName + "/" + featureName
}

// PutRecord stores a feature record in a feature group.
func (b *InMemoryBackend) PutRecord(
	featureGroupName string,
	record map[string]string,
) error {
	b.mu.Lock("PutRecord")
	defer b.mu.Unlock()

	fg, ok := b.featureGroups[featureGroupName]
	if !ok {
		return fmt.Errorf(
			"%w: feature group %q not found",
			ErrFeatureGroupNotFound,
			featureGroupName,
		)
	}

	recordIDValue, hasID := record[fg.RecordIdentifierFeatureName]
	if !hasID || recordIDValue == "" {
		return fmt.Errorf(
			"%w: record missing identifier feature %q",
			ErrValidation,
			fg.RecordIdentifierFeatureName,
		)
	}

	key := featureRecordKey(featureGroupName, recordIDValue)

	cp := make(map[string]string, len(record))
	maps.Copy(cp, record)

	b.featureRecords[key] = &FeatureRecord{Record: cp}

	return nil
}

// GetRecord retrieves a feature record from a feature group.
func (b *InMemoryBackend) GetRecord(
	featureGroupName, recordIDValue string,
	featureNames []string,
) (*FeatureRecord, error) {
	b.mu.RLock("GetRecord")
	defer b.mu.RUnlock()

	if _, ok := b.featureGroups[featureGroupName]; !ok {
		return nil, fmt.Errorf(
			"%w: feature group %q not found",
			ErrFeatureGroupNotFound,
			featureGroupName,
		)
	}

	key := featureRecordKey(featureGroupName, recordIDValue)

	rec, ok := b.featureRecords[key]
	if !ok {
		return nil, fmt.Errorf(
			"%w: record %q not found in feature group %q",
			ErrFeatureGroupNotFound,
			recordIDValue,
			featureGroupName,
		)
	}

	// If feature names filter is provided, return only requested features.
	if len(featureNames) == 0 {
		cp := make(map[string]string, len(rec.Record))
		maps.Copy(cp, rec.Record)

		return &FeatureRecord{Record: cp}, nil
	}

	filtered := make(map[string]string, len(featureNames))

	for _, fn := range featureNames {
		if v, found := rec.Record[fn]; found {
			filtered[fn] = v
		}
	}

	return &FeatureRecord{Record: filtered}, nil
}

// DeleteRecord deletes a feature record from a feature group.
func (b *InMemoryBackend) DeleteRecord(featureGroupName, recordIDValue string) error {
	b.mu.Lock("DeleteRecord")
	defer b.mu.Unlock()

	if _, ok := b.featureGroups[featureGroupName]; !ok {
		return fmt.Errorf(
			"%w: feature group %q not found",
			ErrFeatureGroupNotFound,
			featureGroupName,
		)
	}

	key := featureRecordKey(featureGroupName, recordIDValue)
	delete(b.featureRecords, key)

	return nil
}

// BatchGetRecordResult holds the result for a single record lookup.
type BatchGetRecordResult struct {
	FeatureGroupName              string            `json:"FeatureGroupName"`
	RecordIdentifierValueAsString string            `json:"RecordIdentifierValueAsString"`
	Record                        map[string]string `json:"Record,omitempty"`
	ErrorCode                     string            `json:"ErrorCode,omitempty"`
	ErrorMessage                  string            `json:"ErrorMessage,omitempty"`
}

// BatchGetRecord retrieves multiple feature records in a single call.
func (b *InMemoryBackend) BatchGetRecord(
	identifiers []struct {
		FeatureGroupName              string
		RecordIdentifierValueAsString string
		FeatureNames                  []string
	},
) []BatchGetRecordResult {
	b.mu.RLock("BatchGetRecord")
	defer b.mu.RUnlock()

	results := make([]BatchGetRecordResult, 0, len(identifiers))

	for _, ident := range identifiers {
		result := BatchGetRecordResult{
			FeatureGroupName:              ident.FeatureGroupName,
			RecordIdentifierValueAsString: ident.RecordIdentifierValueAsString,
		}

		fg, ok := b.featureGroups[ident.FeatureGroupName]
		if !ok {
			result.ErrorCode = "ResourceNotFoundException"
			result.ErrorMessage = "feature group " + ident.FeatureGroupName + " not found"
			results = append(results, result)

			continue
		}

		key := featureRecordKey(fg.FeatureGroupName, ident.RecordIdentifierValueAsString)

		rec, ok := b.featureRecords[key]
		if !ok {
			result.ErrorCode = "ResourceNotFoundException"
			result.ErrorMessage = "record " + ident.RecordIdentifierValueAsString + " not found"
			results = append(results, result)

			continue
		}

		if len(ident.FeatureNames) == 0 {
			cp := make(map[string]string, len(rec.Record))
			maps.Copy(cp, rec.Record)
			result.Record = cp
		} else {
			filtered := make(map[string]string, len(ident.FeatureNames))
			for _, fn := range ident.FeatureNames {
				if v, fOk := rec.Record[fn]; fOk {
					filtered[fn] = v
				}
			}

			result.Record = filtered
		}

		results = append(results, result)
	}

	return results
}

// GetFeatureMetadata returns metadata for a feature in a feature group.
func (b *InMemoryBackend) GetFeatureMetadata(
	featureGroupName, featureName string,
) (*FeatureMetadata, error) {
	b.mu.RLock("GetFeatureMetadata")
	defer b.mu.RUnlock()

	fg, ok := b.featureGroups[featureGroupName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: feature group %q not found",
			ErrFeatureGroupNotFound,
			featureGroupName,
		)
	}

	// Verify the feature exists in the group.
	featureType := ""

	for _, def := range fg.FeatureDefinitions {
		if def.FeatureName == featureName {
			featureType = def.FeatureType

			break
		}
	}

	key := featureMetaKey(featureGroupName, featureName)

	meta, ok := b.featureMetadata[key]
	if !ok {
		// Return default metadata if not explicitly set.
		return &FeatureMetadata{
			FeatureName: featureName,
			FeatureType: featureType,
		}, nil
	}

	cp := *meta
	cp.Parameters = maps.Clone(meta.Parameters)
	cp.FeatureType = featureType

	return &cp, nil
}

// UpdateFeatureMetadata updates metadata for a feature in a feature group.
func (b *InMemoryBackend) UpdateFeatureMetadata(
	featureGroupName, featureName, description string,
	parameters map[string]string,
) error {
	b.mu.Lock("UpdateFeatureMetadata")
	defer b.mu.Unlock()

	if _, ok := b.featureGroups[featureGroupName]; !ok {
		return fmt.Errorf(
			"%w: feature group %q not found",
			ErrFeatureGroupNotFound,
			featureGroupName,
		)
	}

	key := featureMetaKey(featureGroupName, featureName)

	existing, ok := b.featureMetadata[key]
	if !ok {
		existing = &FeatureMetadata{FeatureName: featureName}
	}

	if description != "" {
		existing.Description = description
	}

	if parameters != nil {
		if existing.Parameters == nil {
			existing.Parameters = make(map[string]string)
		}
		maps.Copy(existing.Parameters, parameters)
	}

	b.featureMetadata[key] = existing

	return nil
}

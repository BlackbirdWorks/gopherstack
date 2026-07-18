package timestreamwrite

import (
	"fmt"
	"regexp"
)

// AWS API limits enforced by this handler.
const (
	// maxDatabaseNameLen is the maximum length for a Timestream database name per the AWS API.
	maxDatabaseNameLen = 64
	// maxTableNameLen is the maximum length for a Timestream table name per the AWS API.
	maxTableNameLen = 256
	// maxTagKeyLen is the maximum byte length of a tag key per the AWS API.
	maxTagKeyLen = 128
	// maxTagValueLen is the maximum byte length of a tag value per the AWS API.
	maxTagValueLen = 256
	// maxTagsPerResource is the maximum number of tags allowed on a single resource per the AWS API.
	maxTagsPerResource = 200

	// minMemoryRetentionHours is the minimum allowed value for MemoryStoreRetentionPeriodInHours.
	minMemoryRetentionHours = 1
	// maxMemoryRetentionHours is the maximum allowed value for MemoryStoreRetentionPeriodInHours
	// (approximately one year).
	maxMemoryRetentionHours = 8766
	// minMagneticRetentionDays is the minimum allowed value for MagneticStoreRetentionPeriodInDays.
	minMagneticRetentionDays = 1
	// maxMagneticRetentionDays is the maximum allowed value for MagneticStoreRetentionPeriodInDays
	// (approximately 200 years).
	maxMagneticRetentionDays = 73000
	// maxDimensionsPerRecord is the maximum number of dimensions allowed per record per the AWS API.
	maxDimensionsPerRecord = 128
	// minDatabaseNameLength is the minimum length for a Timestream database name per the AWS API.
	minDatabaseNameLength = 3
)

// resourceNameRE is the allowed character set for Timestream database and table names per the
// AWS API: alphanumeric characters, hyphens, underscores, and dots.
var resourceNameRE = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

// isValidMeasureValueType reports whether v is a MeasureValueType accepted by the AWS API.
func isValidMeasureValueType(v string) bool {
	switch v {
	case "DOUBLE", "BIGINT", "BOOLEAN", "VARCHAR", "TIMESTAMP", "MULTI":
		return true
	}

	return false
}

// isValidTimeUnit reports whether v is a TimeUnit accepted by the AWS API.
func isValidTimeUnit(v string) bool {
	switch v {
	case "SECONDS", "MILLISECONDS", "MICROSECONDS", "NANOSECONDS":
		return true
	}

	return false
}

// validateDatabaseName validates a Timestream database name against AWS length and format
// constraints. The name must be 3–64 characters and contain only alphanumeric characters,
// hyphens, underscores, or dots.
func validateDatabaseName(name string) error {
	if len(name) < minDatabaseNameLength {
		return fmt.Errorf(
			"%w: DatabaseName %q must be at least %d characters long",
			errInvalidRequest, name, minDatabaseNameLength,
		)
	}

	if len(name) > maxDatabaseNameLen {
		return fmt.Errorf(
			"%w: DatabaseName %q must be at most %d characters long",
			errInvalidRequest, name, maxDatabaseNameLen,
		)
	}

	if !resourceNameRE.MatchString(name) {
		return fmt.Errorf(
			"%w: DatabaseName %q must contain only alphanumeric characters, hyphens, underscores, or dots",
			errInvalidRequest, name,
		)
	}

	return nil
}

// validateTableName validates a Timestream table name against AWS length and format constraints.
// The name must be non-empty, at most 256 characters, and contain only alphanumeric characters,
// hyphens, underscores, or dots.
func validateTableName(name string) error {
	if len(name) > maxTableNameLen {
		return fmt.Errorf(
			"%w: TableName %q must be at most %d characters long",
			errInvalidRequest, name, maxTableNameLen,
		)
	}

	if !resourceNameRE.MatchString(name) {
		return fmt.Errorf(
			"%w: TableName %q must contain only alphanumeric characters, hyphens, underscores, or dots",
			errInvalidRequest, name,
		)
	}

	return nil
}

// validateRetentionPropertiesInput validates retention period values against AWS limits.
// A nil input is treated as valid (no retention properties set).
func validateRetentionPropertiesInput(rp *retentionPropertiesInput) error {
	if rp == nil {
		return nil
	}

	if rp.MemoryStoreRetentionPeriodInHours < minMemoryRetentionHours ||
		rp.MemoryStoreRetentionPeriodInHours > maxMemoryRetentionHours {
		return fmt.Errorf(
			"%w: MemoryStoreRetentionPeriodInHours must be between %d and %d, got %d",
			errInvalidRequest,
			minMemoryRetentionHours,
			maxMemoryRetentionHours,
			rp.MemoryStoreRetentionPeriodInHours,
		)
	}

	if rp.MagneticStoreRetentionPeriodInDays < minMagneticRetentionDays ||
		rp.MagneticStoreRetentionPeriodInDays > maxMagneticRetentionDays {
		return fmt.Errorf(
			"%w: MagneticStoreRetentionPeriodInDays must be between %d and %d, got %d",
			errInvalidRequest,
			minMagneticRetentionDays,
			maxMagneticRetentionDays,
			rp.MagneticStoreRetentionPeriodInDays,
		)
	}

	return nil
}

// validateTagInputs validates a tag slice against AWS constraints: max 200 tags per resource,
// keys must be 1–128 characters and non-empty, values must be 0–256 characters.
func validateTagInputs(tags []tagInput) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: too many tags: max %d per resource, got %d",
			errInvalidRequest, maxTagsPerResource, len(tags),
		)
	}

	for _, t := range tags {
		if t.Key == "" {
			return fmt.Errorf("%w: tag key cannot be empty", errInvalidRequest)
		}

		if len(t.Key) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key %q exceeds maximum length of %d characters",
				errInvalidRequest, t.Key, maxTagKeyLen,
			)
		}

		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value for key %q exceeds maximum length of %d characters",
				errInvalidRequest, t.Key, maxTagValueLen,
			)
		}
	}

	return nil
}

// validateSchemaPartitionKeys validates the CompositePartitionKey slice against AWS rules.
// DIMENSION partition keys must specify a Name field.
// An unknown Type is rejected with a ValidationException.
func validateSchemaPartitionKeys(sc *schemaInput) error {
	if sc == nil {
		return nil
	}

	for i, pk := range sc.CompositePartitionKey {
		switch pk.Type {
		case PartitionKeyTypeDimension:
			if pk.Name == "" {
				return fmt.Errorf(
					"%w: partition key at index %d has Type %q but is missing required Name field",
					errInvalidRequest, i, PartitionKeyTypeDimension,
				)
			}
		case PartitionKeyTypeMeasure:
			// MEASURE partition keys: Name is not applicable and should not be set.
		case "":
			return fmt.Errorf(
				"%w: partition key at index %d is missing required Type field",
				errInvalidRequest, i,
			)
		default:
			return fmt.Errorf(
				"%w: partition key at index %d has unknown Type %q (must be %q or %q)",
				errInvalidRequest, i, pk.Type, PartitionKeyTypeDimension, PartitionKeyTypeMeasure,
			)
		}
	}

	return nil
}

// validateRecord validates an individual WriteRecords record against AWS constraints.
// Validation runs on the merged record (after CommonAttributes is applied).
func validateRecord(r recordInput, idx int) error {
	if r.MeasureName == "" {
		return fmt.Errorf(
			"%w: record[%d] is missing required field MeasureName",
			errInvalidRequest, idx,
		)
	}

	if r.MeasureValueType != "" && !isValidMeasureValueType(r.MeasureValueType) {
		return fmt.Errorf(
			"%w: record[%d] has invalid MeasureValueType %q; valid: DOUBLE, BIGINT, BOOLEAN, VARCHAR, TIMESTAMP, MULTI",
			errInvalidRequest,
			idx,
			r.MeasureValueType,
		)
	}

	if r.TimeUnit != "" && !isValidTimeUnit(r.TimeUnit) {
		return fmt.Errorf(
			"%w: record[%d] has invalid TimeUnit %q; valid values are SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS",
			errInvalidRequest, idx, r.TimeUnit,
		)
	}

	if len(r.Dimensions) > maxDimensionsPerRecord {
		return fmt.Errorf(
			"%w: record[%d] has %d dimensions; maximum allowed is %d",
			errInvalidRequest, idx, len(r.Dimensions), maxDimensionsPerRecord,
		)
	}

	for di, d := range r.Dimensions {
		if d.Name == "" {
			return fmt.Errorf(
				"%w: record[%d] dimension[%d] has empty Name",
				errInvalidRequest, idx, di,
			)
		}

		if d.Value == "" {
			return fmt.Errorf(
				"%w: record[%d] dimension[%d] has empty Value",
				errInvalidRequest, idx, di,
			)
		}
	}

	if r.MeasureValueType == "MULTI" {
		if len(r.MeasureValues) == 0 {
			return fmt.Errorf(
				"%w: record[%d] with MeasureValueType MULTI must have non-empty MeasureValues",
				errInvalidRequest, idx,
			)
		}

		if r.MeasureValue != "" {
			return fmt.Errorf(
				"%w: record[%d] with MeasureValueType MULTI must not set MeasureValue",
				errInvalidRequest, idx,
			)
		}
	}

	// AWS API: "Version must be 1 or greater, or you will receive a ValidationException
	// error." A negative value is unambiguously invalid input regardless of the field
	// being unset (0, defaulted to 1 downstream).
	if r.Version < 0 {
		return fmt.Errorf(
			"%w: record[%d] has Version %d; must be 1 or greater",
			errInvalidRequest, idx, r.Version,
		)
	}

	return nil
}

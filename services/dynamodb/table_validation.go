// Package dynamodb implements the AWS DynamoDB mock service.
// table_validation.go validates CreateTable/UpdateTable structural constraints:
// GSI/LSI counts, key schema shape, billing mode, and provisioned throughput.
package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

const (
	// maxGSICount is the maximum number of Global Secondary Indexes per table.
	maxGSICount = 20
	// maxLSICount is the maximum number of Local Secondary Indexes per table.
	maxLSICount = 5
)

// validateGSICount returns a LimitExceededException when the proposed GSI list
// would exceed maxGSICount (20) per table.
func validateGSICount(gsiList []models.GlobalSecondaryIndex, additions int) error {
	if len(gsiList)+additions > maxGSICount {
		return NewLimitExceededException(
			fmt.Sprintf(
				"Too many global secondary indexes; maximum is %d",
				maxGSICount,
			),
		)
	}

	return nil
}

// validateLSICount returns a LimitExceededException when the LSI list exceeds maxLSICount (5).
func validateLSICount(lsiList []models.LocalSecondaryIndex) error {
	if len(lsiList) > maxLSICount {
		return NewLimitExceededException(
			fmt.Sprintf(
				"Too many local secondary indexes; maximum is %d",
				maxLSICount,
			),
		)
	}

	return nil
}

// isOnDemandTable returns true when the table is in PAY_PER_REQUEST billing mode.
// PAY_PER_REQUEST tables are never throttled.
func isOnDemandTable(billingMode string) bool {
	return billingMode == string(types.BillingModePayPerRequest)
}

// validateCreateTableKeySchema ensures the table KeySchema has exactly one HASH
// key and at most one RANGE key. AWS rejects any other structure.
func validateCreateTableKeySchema(schema []models.KeySchemaElement) error {
	hashCount := 0
	rangeCount := 0

	for _, k := range schema {
		switch k.KeyType {
		case models.KeyTypeHash:
			hashCount++
		case models.KeyTypeRange:
			rangeCount++
		default:
			return NewValidationException(
				fmt.Sprintf("Unknown key type: %s", k.KeyType),
			)
		}
	}

	if hashCount != 1 {
		return NewValidationException(
			"One and only one hash key may be defined",
		)
	}

	if rangeCount > 1 {
		return NewValidationException(
			"No more than one range key may be defined",
		)
	}

	return nil
}

// validateGSIThroughput enforces AWS's rules on per-GSI ProvisionedThroughput:
// when the table BillingMode is PROVISIONED, every GSI must declare positive
// RCU and WCU; when PAY_PER_REQUEST, any GSI ProvisionedThroughput setting is
// rejected because GSIs inherit on-demand billing.
func validateGSIThroughput(
	gsis []types.GlobalSecondaryIndex, billingMode types.BillingMode,
) error {
	isPPR := billingMode == types.BillingModePayPerRequest
	for _, g := range gsis {
		if err := validateGSIThroughputEntry(g.ProvisionedThroughput, isPPR); err != nil {
			return err
		}
	}

	return nil
}

// validateGSIThroughputEntry validates a single GSI's ProvisionedThroughput against the
// table billing mode. isPPR is true when the table uses PAY_PER_REQUEST billing.
func validateGSIThroughputEntry(pt *types.ProvisionedThroughput, isPPR bool) error {
	if pt == nil {
		return nil
	}

	if isPPR {
		if (pt.ReadCapacityUnits != nil && *pt.ReadCapacityUnits > 0) ||
			(pt.WriteCapacityUnits != nil && *pt.WriteCapacityUnits > 0) {
			return NewValidationException(
				"One or more parameter values were invalid: " +
					"Neither ReadCapacityUnits nor WriteCapacityUnits can be specified on a GSI when BillingMode is PAY_PER_REQUEST",
			)
		}

		return nil
	}

	if pt.ReadCapacityUnits != nil && *pt.ReadCapacityUnits <= 0 {
		return NewValidationException(
			"One or more parameter values were invalid: " +
				"GSI ReadCapacityUnits must be a positive number",
		)
	}

	if pt.WriteCapacityUnits != nil && *pt.WriteCapacityUnits <= 0 {
		return NewValidationException(
			"One or more parameter values were invalid: " +
				"GSI WriteCapacityUnits must be a positive number",
		)
	}

	return nil
}

// validateProvisionedThroughput returns a ValidationException when a
// PROVISIONED table is created or updated with ReadCapacityUnits or
// WriteCapacityUnits explicitly set to 0 or negative. Nil values are allowed
// (server-side defaults). PAY_PER_REQUEST tables skip this check.
func validateProvisionedThroughput(
	pt *types.ProvisionedThroughput,
	billingMode types.BillingMode,
) error {
	if billingMode == types.BillingModePayPerRequest {
		// PAY_PER_REQUEST tables must not have explicit positive throughput.
		if pt != nil && pt.ReadCapacityUnits != nil && *pt.ReadCapacityUnits > 0 {
			return NewValidationException(
				"One or more parameter values were invalid: " +
					"Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST",
			)
		}
		if pt != nil && pt.WriteCapacityUnits != nil && *pt.WriteCapacityUnits > 0 {
			return NewValidationException(
				"One or more parameter values were invalid: " +
					"Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST",
			)
		}

		return nil
	}

	// PROVISIONED (or default): nil throughput is allowed (caller uses defaults).
	// Explicit PROVISIONED with nil throughput is an error on real AWS, but many
	// existing callers omit throughput when relying on defaults, so we only
	// validate when throughput is explicitly provided.
	if pt == nil {
		if billingMode == types.BillingModeProvisioned {
			return NewValidationException(
				"One or more parameter values were invalid: " +
					"ReadCapacityUnits and WriteCapacityUnits must be specified for tables with PROVISIONED billing mode",
			)
		}

		return nil
	}

	if pt.ReadCapacityUnits != nil && *pt.ReadCapacityUnits <= 0 {
		return NewValidationException(
			"One or more parameter values were invalid: " +
				"ReadCapacityUnits must be a positive number",
		)
	}

	if pt.WriteCapacityUnits != nil && *pt.WriteCapacityUnits <= 0 {
		return NewValidationException(
			"One or more parameter values were invalid: " +
				"WriteCapacityUnits must be a positive number",
		)
	}

	return nil
}

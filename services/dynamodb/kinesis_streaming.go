// Package dynamodb implements the AWS DynamoDB mock service.
// kinesis_streaming.go implements the Kinesis Data Streams destination family:
// Describe/Enable/Disable/UpdateKinesisStreamingDestination.
package dynamodb

import (
	"context"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// --- DescribeKinesisStreamingDestination ---

// DescribeKinesisStreamingDestination returns the Kinesis streaming destinations for a table.
func (db *InMemoryDB) DescribeKinesisStreamingDestination(
	ctx context.Context,
	input *dynamodb.DescribeKinesisStreamingDestinationInput,
) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	tableName, destinations := kinesisDestinationsRLocked(table)

	return &dynamodb.DescribeKinesisStreamingDestinationOutput{
		TableName:                     &tableName,
		KinesisDataStreamDestinations: destinations,
	}, nil
}

// kinesisDestinationsRLocked copies table.Name and the KinesisDestinations
// list under a defer-protected table.mu.RLock.
func kinesisDestinationsRLocked(table *Table) (string, []types.KinesisDataStreamDestination) {
	table.mu.RLock("DescribeKinesisStreamingDestination")
	defer table.mu.RUnlock()

	destinations := make([]types.KinesisDataStreamDestination, 0, len(table.KinesisDestinations))

	for _, dest := range table.KinesisDestinations {
		d := dest
		precision := types.ApproximateCreationDateTimePrecision(d.Precision)
		if d.Precision == "" {
			precision = types.ApproximateCreationDateTimePrecisionMillisecond
		}

		destinations = append(destinations, types.KinesisDataStreamDestination{
			StreamArn:                            &d.StreamARN,
			DestinationStatus:                    types.DestinationStatusActive,
			ApproximateCreationDateTimePrecision: precision,
		})
	}

	return table.Name, destinations
}

// --- DisableKinesisStreamingDestination ---

// DisableKinesisStreamingDestination removes a Kinesis streaming destination from a table.
func (db *InMemoryDB) DisableKinesisStreamingDestination(
	ctx context.Context,
	input *dynamodb.DisableKinesisStreamingDestinationInput,
) (*dynamodb.DisableKinesisStreamingDestinationOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	if input.StreamArn == nil || *input.StreamArn == "" {
		return nil, NewValidationException("StreamArn is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	streamARN := *input.StreamArn
	tableName := *input.TableName

	found := removeKinesisDestinationLocked(table, streamARN)
	if !found {
		return nil, &Error{
			Type:    errResourceNotFoundExceptionType,
			Message: fmt.Sprintf("Kinesis stream %s not found for table %s", streamARN, tableName),
		}
	}

	status := types.DestinationStatusDisabling

	return &dynamodb.DisableKinesisStreamingDestinationOutput{
		TableName:         &tableName,
		StreamArn:         &streamARN,
		DestinationStatus: status,
	}, nil
}

// removeKinesisDestinationLocked removes the destination with the given
// streamARN from table.KinesisDestinations under a defer-protected
// table.mu.Lock, reporting whether an entry was found and removed.
func removeKinesisDestinationLocked(table *Table, streamARN string) bool {
	table.mu.Lock("DisableKinesisStreamingDestination")
	defer table.mu.Unlock()

	for i, dest := range table.KinesisDestinations {
		if dest.StreamARN == streamARN {
			table.KinesisDestinations = append(
				table.KinesisDestinations[:i],
				table.KinesisDestinations[i+1:]...)

			return true
		}
	}

	return false
}

// --- EnableKinesisStreamingDestination ---

// EnableKinesisStreamingDestination adds a Kinesis streaming destination to a table.
func (db *InMemoryDB) EnableKinesisStreamingDestination(
	ctx context.Context,
	input *dynamodb.EnableKinesisStreamingDestinationInput,
) (*dynamodb.EnableKinesisStreamingDestinationOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	if input.StreamArn == nil || *input.StreamArn == "" {
		return nil, NewValidationException("StreamArn is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	streamARN := *input.StreamArn
	tableName := *input.TableName

	precision := ""
	if input.EnableKinesisStreamingConfiguration != nil {
		precision = string(
			input.EnableKinesisStreamingConfiguration.ApproximateCreationDateTimePrecision,
		)
	}

	addOrUpdateKinesisDestinationLocked(table, streamARN, precision)

	return &dynamodb.EnableKinesisStreamingDestinationOutput{
		TableName:         &tableName,
		StreamArn:         &streamARN,
		DestinationStatus: types.DestinationStatusEnabling,
	}, nil
}

// addOrUpdateKinesisDestinationLocked adds a new Kinesis destination or
// updates the precision of an existing one (matched by streamARN), under a
// defer-protected table.mu.Lock.
func addOrUpdateKinesisDestinationLocked(table *Table, streamARN, precision string) {
	table.mu.Lock("EnableKinesisStreamingDestination")
	defer table.mu.Unlock()

	// Idempotent: if already present update precision config, otherwise append.
	idx := slices.IndexFunc(table.KinesisDestinations, func(e KinesisDestinationEntry) bool {
		return e.StreamARN == streamARN
	})

	if idx >= 0 {
		table.KinesisDestinations[idx].Precision = precision
	} else {
		table.KinesisDestinations = append(table.KinesisDestinations, KinesisDestinationEntry{
			StreamARN: streamARN,
			Precision: precision,
		})
	}
}

// --- UpdateKinesisStreamingDestination ---

// UpdateKinesisStreamingDestination updates the precision configuration of an existing
// Kinesis streaming destination. The precision change is persisted and reflected in
// subsequent DescribeKinesisStreamingDestination calls.
func (db *InMemoryDB) UpdateKinesisStreamingDestination(
	ctx context.Context,
	input *dynamodb.UpdateKinesisStreamingDestinationInput,
) (*dynamodb.UpdateKinesisStreamingDestinationOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	if input.StreamArn == nil || *input.StreamArn == "" {
		return nil, NewValidationException("StreamArn is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	tableName := *input.TableName
	streamARN := *input.StreamArn

	var precision *string
	if input.UpdateKinesisStreamingConfiguration != nil {
		p := string(input.UpdateKinesisStreamingConfiguration.ApproximateCreationDateTimePrecision)
		precision = &p
	}

	found := updateKinesisDestinationPrecisionLocked(table, streamARN, precision)
	if !found {
		return nil, &Error{
			Type:    errResourceNotFoundExceptionType,
			Message: fmt.Sprintf("Kinesis stream %s not found for table %s", streamARN, tableName),
		}
	}

	return &dynamodb.UpdateKinesisStreamingDestinationOutput{
		TableName:         &tableName,
		StreamArn:         &streamARN,
		DestinationStatus: types.DestinationStatusActive,
	}, nil
}

// updateKinesisDestinationPrecisionLocked finds the destination matching
// streamARN and, if precision is non-nil, updates its Precision field, all
// under a defer-protected table.mu.Lock. Reports whether a matching
// destination was found.
func updateKinesisDestinationPrecisionLocked(table *Table, streamARN string, precision *string) bool {
	table.mu.Lock("UpdateKinesisStreamingDestination")
	defer table.mu.Unlock()

	idx := slices.IndexFunc(table.KinesisDestinations, func(e KinesisDestinationEntry) bool {
		return e.StreamARN == streamARN
	})

	if idx < 0 {
		return false
	}

	if precision != nil {
		table.KinesisDestinations[idx].Precision = *precision
	}

	return true
}

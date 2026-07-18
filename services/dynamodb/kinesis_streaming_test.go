package dynamodb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestUpdateKinesisStreamingDestination_TableNotFound(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)

	_, err := db.UpdateKinesisStreamingDestination(
		t.Context(),
		&sdk.UpdateKinesisStreamingDestinationInput{
			TableName: aws.String("NoTable"),
			StreamArn: aws.String("arn:aws:kinesis:us-east-1:123:stream/s"),
		},
	)
	require.Error(t, err)
}

func TestUpdateKinesisStreamingDestination_MissingStreamARN(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	createSimplePPRTable(t, db, "KinesisTable")

	_, err := db.UpdateKinesisStreamingDestination(
		t.Context(),
		&sdk.UpdateKinesisStreamingDestinationInput{
			TableName: aws.String("KinesisTable"),
			StreamArn: aws.String(""),
		},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StreamArn")
}

func TestUpdateKinesisStreamingDestination_ReturnsActive(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	createSimplePPRTable(t, db, "KinesisActiveTable")

	streamARN := "arn:aws:kinesis:us-east-1:123:stream/my-stream"

	_, err := db.EnableKinesisStreamingDestination(
		t.Context(),
		&sdk.EnableKinesisStreamingDestinationInput{
			TableName: aws.String("KinesisActiveTable"),
			StreamArn: aws.String(streamARN),
		},
	)
	require.NoError(t, err)

	out, err := db.UpdateKinesisStreamingDestination(
		t.Context(),
		&sdk.UpdateKinesisStreamingDestinationInput{
			TableName: aws.String("KinesisActiveTable"),
			StreamArn: aws.String(streamARN),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.DestinationStatusActive, out.DestinationStatus)
	assert.Equal(t, "KinesisActiveTable", aws.ToString(out.TableName))
}

func TestKinesisDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             any
		setup            func(t *testing.T, backend *dynamodb.InMemoryDB, handler *dynamodb.DynamoDBHandler)
		name             string
		action           string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:   "DescribeKinesisStreamingDestination_empty",
			action: "DescribeKinesisStreamingDestination",
			setup: func(t *testing.T, backend *dynamodb.InMemoryDB, _ *dynamodb.DynamoDBHandler) {
				t.Helper()
				createTableHelper(t, backend, "KinesisTable", "pk")
			},
			body:             map[string]any{"TableName": "KinesisTable"},
			wantStatus:       http.StatusOK,
			wantBodyContains: "KinesisDataStreamDestinations",
		},
		{
			name:             "DescribeKinesisStreamingDestination_table_not_found",
			action:           "DescribeKinesisStreamingDestination",
			body:             map[string]any{"TableName": "NoTable"},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ResourceNotFoundException",
		},
		{
			name:   "DisableKinesisStreamingDestination_success",
			action: "DisableKinesisStreamingDestination",
			setup: func(t *testing.T, backend *dynamodb.InMemoryDB, _ *dynamodb.DynamoDBHandler) {
				t.Helper()
				createTableHelper(t, backend, "KinesisDisableTable", "pk")
				backend.AddKinesisDestination(
					"KinesisDisableTable",
					"arn:aws:kinesis:us-east-1:123:stream/my-stream",
				)
			},
			body: map[string]any{
				"TableName": "KinesisDisableTable",
				"StreamArn": "arn:aws:kinesis:us-east-1:123:stream/my-stream",
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: "DISABLING",
		},
		{
			name:   "DisableKinesisStreamingDestination_stream_not_found",
			action: "DisableKinesisStreamingDestination",
			setup: func(t *testing.T, backend *dynamodb.InMemoryDB, _ *dynamodb.DynamoDBHandler) {
				t.Helper()
				createTableHelper(t, backend, "KinesisDisableTable2", "pk")
			},
			body: map[string]any{
				"TableName": "KinesisDisableTable2",
				"StreamArn": "arn:aws:kinesis:us-east-1:123:stream/no-such-stream",
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, backend, handler)
			}

			code, resp := invokeOp(t, handler, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, code)

			if tt.wantBodyContains != "" {
				bodyBytes, _ := json.Marshal(resp)
				assert.Contains(t, string(bodyBytes), tt.wantBodyContains)
			}
		})
	}
}

func TestKinesisDestinations_StatePersistence(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	createTableHelper(t, backend, "KinesisStateTable", "pk")

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
	backend.AddKinesisDestination("KinesisStateTable", streamARN)

	// Verify stream is tracked
	code, resp := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisStateTable",
	})
	require.Equal(t, http.StatusOK, code)

	destinations, ok := resp["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, destinations, 1)

	dest := destinations[0].(map[string]any)
	assert.Equal(t, streamARN, dest["StreamArn"])
	assert.Equal(t, "ACTIVE", dest["DestinationStatus"])

	// Disable the stream
	code2, resp2 := invokeOp(t, handler, "DisableKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisStateTable",
		"StreamArn": streamARN,
	})
	require.Equal(t, http.StatusOK, code2)
	assert.Equal(t, "DISABLING", resp2["DestinationStatus"])

	// Verify stream is removed
	code3, resp3 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisStateTable",
	})
	require.Equal(t, http.StatusOK, code3)

	destinations2, ok2 := resp3["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok2)
	assert.Empty(t, destinations2)
}

func TestEnableKinesisStreamingDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             any
		setup            func(t *testing.T, backend *dynamodb.InMemoryDB)
		name             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name: "success",
			setup: func(t *testing.T, backend *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, backend, "EnableKinesisTable", "pk")
			},
			body: map[string]any{
				"TableName": "EnableKinesisTable",
				"StreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: "ENABLING",
		},
		{
			name: "table_not_found",
			body: map[string]any{
				"TableName": "NoTable",
				"StreamArn": "arn:aws:kinesis:us-east-1:123:stream/s",
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ResourceNotFoundException",
		},
		{
			name:             "missing_stream_arn",
			body:             map[string]any{"TableName": "SomeTable"},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, backend)
			}

			code, resp := invokeOp(t, handler, "EnableKinesisStreamingDestination", tt.body)
			assert.Equal(t, tt.wantStatus, code)

			if tt.wantBodyContains != "" {
				bodyBytes, _ := json.Marshal(resp)
				assert.Contains(t, string(bodyBytes), tt.wantBodyContains)
			}
		})
	}
}

func TestEnableDisableKinesis_StatePersistence(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	createTableHelper(t, backend, "KinesisFullTable", "pk")

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/full-stream"

	// Enable the stream
	code, resp := invokeOp(t, handler, "EnableKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
		"StreamArn": streamARN,
	})
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ENABLING", resp["DestinationStatus"])

	// Verify it appears as ACTIVE in DescribeKinesisStreamingDestination
	code2, resp2 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
	})
	require.Equal(t, http.StatusOK, code2)

	destinations, ok := resp2["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, destinations, 1)
	assert.Equal(t, streamARN, destinations[0].(map[string]any)["StreamArn"])

	// Enable the same stream again (idempotent)
	code3, _ := invokeOp(t, handler, "EnableKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
		"StreamArn": streamARN,
	})
	require.Equal(t, http.StatusOK, code3)

	// Still only one destination
	code4, resp4 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
	})
	require.Equal(t, http.StatusOK, code4)

	destinations2, ok2 := resp4["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok2)
	assert.Len(t, destinations2, 1)

	// Disable the stream
	code5, _ := invokeOp(t, handler, "DisableKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
		"StreamArn": streamARN,
	})
	require.Equal(t, http.StatusOK, code5)

	// Verify no destinations remain
	code6, resp6 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
	})
	require.Equal(t, http.StatusOK, code6)

	destinations3, ok3 := resp6["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok3)
	assert.Empty(t, destinations3)
}

func TestKinesisPrecision_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	createTableHelper(t, backend, "PrecisionTable", "pk")

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/prec-stream"

	code, _ := invokeOp(t, handler, "EnableKinesisStreamingDestination", map[string]any{
		"TableName": "PrecisionTable",
		"StreamArn": streamARN,
		"EnableKinesisStreamingConfiguration": map[string]any{
			"ApproximateCreationDateTimePrecision": "MICROSECOND",
		},
	})
	require.Equal(t, http.StatusOK, code)

	code2, resp2 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "PrecisionTable",
	})
	require.Equal(t, http.StatusOK, code2)

	dests, ok := resp2["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, dests, 1)

	dest := dests[0].(map[string]any)
	assert.Equal(t, "MICROSECOND", dest["ApproximateCreationDateTimePrecision"])
	assert.Equal(t, streamARN, dest["StreamArn"])
}

// TestKinesisPrecision_DefaultMillisecond verifies that destinations enabled
// without an explicit precision default to MILLISECOND.

func TestKinesisPrecision_DefaultMillisecond(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	createTableHelper(t, backend, "DefaultPrecTable", "pk")

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/default-prec"

	code, _ := invokeOp(t, handler, "EnableKinesisStreamingDestination", map[string]any{
		"TableName": "DefaultPrecTable",
		"StreamArn": streamARN,
	})
	require.Equal(t, http.StatusOK, code)

	code2, resp2 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "DefaultPrecTable",
	})
	require.Equal(t, http.StatusOK, code2)

	dests, ok := resp2["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, dests, 1)

	dest := dests[0].(map[string]any)
	assert.Equal(t, "MILLISECOND", dest["ApproximateCreationDateTimePrecision"])
}

// TestUpdateKinesisPrecision verifies that UpdateKinesisStreamingDestination
// persists the new precision and DescribeKinesisStreamingDestination reflects it.

func TestUpdateKinesisPrecision(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	createTableHelper(t, backend, "UpdatePrecTable", "pk")

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/update-prec"

	_, err := backend.EnableKinesisStreamingDestination(
		t.Context(),
		buildEnableKinesisInput("UpdatePrecTable", streamARN, "MILLISECOND"),
	)
	require.NoError(t, err)

	code, resp := invokeOp(t, handler, "UpdateKinesisStreamingDestination", map[string]any{
		"TableName": "UpdatePrecTable",
		"StreamArn": streamARN,
		"UpdateKinesisStreamingConfiguration": map[string]any{
			"ApproximateCreationDateTimePrecision": "MICROSECOND",
		},
	})
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ACTIVE", resp["DestinationStatus"])

	code2, resp2 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "UpdatePrecTable",
	})
	require.Equal(t, http.StatusOK, code2)

	dests, ok := resp2["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, dests, 1)
	assert.Equal(
		t,
		"MICROSECOND",
		dests[0].(map[string]any)["ApproximateCreationDateTimePrecision"],
	)
}

// TestUpdateKinesisDestination_NotFound verifies a 404 when stream not enabled.

func TestUpdateKinesisDestination_NotFound(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	createTableHelper(t, backend, "NoStreamTable", "pk")

	code, resp := invokeOp(t, handler, "UpdateKinesisStreamingDestination", map[string]any{
		"TableName": "NoStreamTable",
		"StreamArn": "arn:aws:kinesis:us-east-1:123:stream/absent",
	})
	require.Equal(t, http.StatusBadRequest, code)
	bodyBytes, _ := json.Marshal(resp)
	assert.Contains(t, string(bodyBytes), "ResourceNotFoundException")
}

// TestUpdateGlobalTableSettings_PersistsBillingMode verifies that
// UpdateGlobalTableSettings persists the billing mode and returns it.

func buildEnableKinesisInput(
	tableName, streamARN, precision string,
) *sdk.EnableKinesisStreamingDestinationInput {
	in := &sdk.EnableKinesisStreamingDestinationInput{
		TableName: aws.String(tableName),
		StreamArn: aws.String(streamARN),
	}

	if precision != "" {
		in.EnableKinesisStreamingConfiguration = &types.EnableKinesisStreamingConfiguration{
			ApproximateCreationDateTimePrecision: types.ApproximateCreationDateTimePrecision(
				precision,
			),
		}
	}

	return in
}

package rdsdata_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsdatasdk "github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

const (
	typeHintResourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:typehint-cluster"
	typeHintSecretARN   = "arn:aws:secretsmanager:us-east-1:000000000000:secret:typehint-secret"
)

// TestExecuteStatement_TypeHint_ValidAndMalformed drives a real rdsdata
// client's ExecuteStatement with a typeHint parameter for each of the six
// documented hints (rdsdata@v1.35.4 types/enums.go's TypeHint; also
// https://docs.aws.amazon.com/rdsdataservice/latest/APIReference/API_SqlParameter.html),
// asserting a well-formed value round-trips and a malformed one 400s as
// BadRequestException naming the parameter.
func TestExecuteStatement_TypeHint_ValidAndMalformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hint    types.TypeHint
		name    string
		valid   string
		invalid string
	}{
		{name: "date", hint: types.TypeHintDate, valid: "2026-09-11", invalid: "2026/09/11"},
		{name: "decimal", hint: types.TypeHintDecimal, valid: "3.14", invalid: "not-a-number"},
		{name: "json", hint: types.TypeHintJson, valid: `{"a":1}`, invalid: `{not valid json`},
		{name: "time", hint: types.TypeHintTime, valid: "13:45:00.123", invalid: "13-45-00"},
		{
			name: "timestamp", hint: types.TypeHintTimestamp,
			valid: "2026-09-11 13:45:00", invalid: "2026-09-11T13:45:00",
		},
		{name: "uuid", hint: types.TypeHintUuid, valid: "123e4567-e89b-12d3-a456-426614174000", invalid: "not-a-uuid"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
			client := newRoundTripClient(t, rdsdata.NewHandler(backend))

			_, err := client.ExecuteStatement(t.Context(), &rdsdatasdk.ExecuteStatementInput{
				ResourceArn: aws.String(typeHintResourceARN),
				SecretArn:   aws.String(typeHintSecretARN),
				Sql:         aws.String("SELECT :v"),
				Parameters: []types.SqlParameter{
					{Name: aws.String("v"), TypeHint: tc.hint, Value: &types.FieldMemberStringValue{Value: tc.valid}},
				},
			})
			require.NoError(t, err, "well-formed %s value must be accepted", tc.hint)

			_, err = client.ExecuteStatement(t.Context(), &rdsdatasdk.ExecuteStatementInput{
				ResourceArn: aws.String(typeHintResourceARN),
				SecretArn:   aws.String(typeHintSecretARN),
				Sql:         aws.String("SELECT :v"),
				Parameters: []types.SqlParameter{
					{Name: aws.String("v"), TypeHint: tc.hint, Value: &types.FieldMemberStringValue{Value: tc.invalid}},
				},
			})
			require.Error(t, err, "malformed %s value must be rejected", tc.hint)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "BadRequestException", apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), `"v"`, "message must name the parameter")
		})
	}
}

// TestBatchExecuteStatement_TypeHint_MalformedRejected verifies malformed
// typeHint values are rejected per parameter set in BatchExecuteStatement,
// not just ExecuteStatement.
func TestBatchExecuteStatement_TypeHint_MalformedRejected(t *testing.T) {
	t.Parallel()

	backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	client := newRoundTripClient(t, rdsdata.NewHandler(backend))

	_, err := client.BatchExecuteStatement(t.Context(), &rdsdatasdk.BatchExecuteStatementInput{
		ResourceArn: aws.String(typeHintResourceARN),
		SecretArn:   aws.String(typeHintSecretARN),
		Sql:         aws.String("SELECT :v"),
		ParameterSets: [][]types.SqlParameter{
			{{
				Name: aws.String("v"), TypeHint: types.TypeHintUuid,
				Value: &types.FieldMemberStringValue{Value: "3.14"},
			}},
			{{
				Name: aws.String("v"), TypeHint: types.TypeHintUuid,
				Value: &types.FieldMemberStringValue{Value: "not-a-uuid"},
			}},
		},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "BadRequestException", apiErr.ErrorCode())
}

// TestExecuteStatement_TypeHint_NonStringValueIsNoOp verifies a typeHint on
// a non-string (or null) value is accepted unchanged, matching the doc's
// "the corresponding String parameter value..." wording -- the hint has no
// defined effect on any other Field union member.
func TestExecuteStatement_TypeHint_NonStringValueIsNoOp(t *testing.T) {
	t.Parallel()

	backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	client := newRoundTripClient(t, rdsdata.NewHandler(backend))

	_, err := client.ExecuteStatement(t.Context(), &rdsdatasdk.ExecuteStatementInput{
		ResourceArn: aws.String(typeHintResourceARN),
		SecretArn:   aws.String(typeHintSecretARN),
		Sql:         aws.String("SELECT :v"),
		Parameters: []types.SqlParameter{
			{Name: aws.String("v"), TypeHint: types.TypeHintUuid, Value: &types.FieldMemberLongValue{Value: 42}},
		},
	})
	require.NoError(t, err)
}

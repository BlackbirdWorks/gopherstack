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
	arrayParamResourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:array-param-cluster"
	arrayParamSecretARN   = "arn:aws:secretsmanager:us-east-1:000000000000:secret:array-param-secret"
)

// TestExecuteStatement_ArrayParameterRejected_RealClient drives a real
// rdsdata client's ExecuteStatement with an arrayValue parameter, verifying
// it's rejected as BadRequestException with a message matching real AWS's
// documented constraint ("Array parameters are not supported" --
// rdsdata@v1.35.4 api_op_ExecuteStatement.go's ExecuteStatementInput.
// Parameters doc comment). The exact wording a live Aurora call returns is
// not independently verifiable without one; see PARITY.md.
func TestExecuteStatement_ArrayParameterRejected_RealClient(t *testing.T) {
	t.Parallel()

	backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	client := newRoundTripClient(t, rdsdata.NewHandler(backend))

	_, err := client.ExecuteStatement(t.Context(), &rdsdatasdk.ExecuteStatementInput{
		ResourceArn: aws.String(arrayParamResourceARN),
		SecretArn:   aws.String(arrayParamSecretARN),
		Sql:         aws.String("SELECT :v"),
		Parameters: []types.SqlParameter{
			{
				Name: aws.String("v"),
				Value: &types.FieldMemberArrayValue{
					Value: &types.ArrayValueMemberStringValues{Value: []*string{aws.String("a"), aws.String("b")}},
				},
			},
		},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "BadRequestException", apiErr.ErrorCode())
	assert.Contains(t, apiErr.ErrorMessage(), "Array parameters are not supported")
	assert.Contains(t, apiErr.ErrorMessage(), `"v"`, "message must name the parameter")
}

// TestBatchExecuteStatement_ArrayParameterRejected_RealClient verifies the
// same rejection applies to BatchExecuteStatement's parameterSets.
func TestBatchExecuteStatement_ArrayParameterRejected_RealClient(t *testing.T) {
	t.Parallel()

	backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	client := newRoundTripClient(t, rdsdata.NewHandler(backend))

	_, err := client.BatchExecuteStatement(t.Context(), &rdsdatasdk.BatchExecuteStatementInput{
		ResourceArn: aws.String(arrayParamResourceARN),
		SecretArn:   aws.String(arrayParamSecretARN),
		Sql:         aws.String("SELECT :v"),
		ParameterSets: [][]types.SqlParameter{
			{
				{
					Name: aws.String("v"),
					Value: &types.FieldMemberArrayValue{
						Value: &types.ArrayValueMemberLongValues{Value: []*int64{aws.Int64(1), aws.Int64(2)}},
					},
				},
			},
		},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "BadRequestException", apiErr.ErrorCode())
	assert.Contains(t, apiErr.ErrorMessage(), "Array parameters are not supported")
}

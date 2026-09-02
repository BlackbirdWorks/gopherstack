package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestSDK_DescribeDBInstances_NonExistent_TypedError drives the real
// aws-sdk-go-v2 rds client (pinned v1.124.1, awsAwsquery/XML protocol) and
// asserts errors.As decodes the specific *types.DBInstanceNotFoundFault, not
// merely that an error occurred (unlike
// TestHandler_OversizedBodySurfacesInternalFailure in
// handler_oversized_body_test.go, which only checks smithy.APIError
// generically -- that test covers a read-failure fallback path, not a
// modelled fault, so it can't assert a concrete type).
//
// All 164 of rds@v1.124.1/deserializers.go's awsAwsquery_deserializeOpError
// functions call awsxml.GetErrorResponseComponents(errorBody, false) --
// noErrorWrapping=false selects wrappedErrorResponse (Code/Message read via
// the "Error>Code"/"Error>Message" XML path), i.e. the response body must be
// <ErrorResponse><Error><Code>...</Code><Message>...</Message></Error></ErrorResponse>
// (aws-sdk-go-v2@v1.43.4/aws/protocol/xml/error_utils.go). This matches
// TestRDSErrorCodes_FaultSuffix's raw-XML assertions in error_codes_test.go;
// this test adds the SDK-side errors.As check that file doesn't do.
func TestSDK_DescribeDBInstances_NonExistent_TypedError(t *testing.T) {
	t.Parallel()

	backend := rds.NewInMemoryBackend("000000000000", config.DefaultRegion)
	h := rds.NewHandler(backend)
	client := newTestRDSClient(t, h)

	_, err := client.DescribeDBInstances(t.Context(), &rdssdk.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var target *rdstypes.DBInstanceNotFoundFault
	require.ErrorAs(t, err, &target,
		"expected a real DBInstanceNotFoundFault from the SDK deserializer, got %T: %v", err, err)
}

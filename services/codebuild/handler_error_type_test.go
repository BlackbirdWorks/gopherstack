package codebuild_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

// TestDeleteSourceCredentials_EmptyArnSurfacesInvalidInputException drives
// DeleteSourceCredentials with Arn set to an empty (non-nil) string through a
// real SDK client. codebuild@v1.72.4's client-side validator
// (validateOpDeleteSourceCredentialsInput) only checks Arn != nil, so an
// empty string reaches the server, where handleDeleteSourceCredentials wraps
// errInvalidRequest -- the same sentinel that backs required-field checks
// across this whole package (handler_builds.go, handler_projects.go, ...).
// Before this fix, handleError's errInvalidRequest/errUnknownAction/
// json.SyntaxError/json.UnmarshalTypeError branch returned a bare message
// with no body __type field, so restjson.GetErrorInfo had nothing to read
// and the error deserialized as a generic UnknownError instead of the
// modeled InvalidInputException (gopherstack-he80).
func TestDeleteSourceCredentials_EmptyArnSurfacesInvalidInputException(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	_, err := client.DeleteSourceCredentials(t.Context(), &codebuildsdk.DeleteSourceCredentialsInput{
		Arn: aws.String(""),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())
}

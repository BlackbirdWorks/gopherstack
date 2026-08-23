package mq_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mqsdk "github.com/aws/aws-sdk-go-v2/service/mq"
	"github.com/aws/aws-sdk-go-v2/service/mq/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/mq"
)

// TestHandler_OversizedBodySurfacesInternalServerErrorException drives a
// real mq client's CreateBroker with a BrokerName large enough to push the
// request body past httputils.MaxRequestBodyBytes (a real client can
// legitimately send this; aws-sdk-go-v2's client-side validation only
// checks BrokerName is non-nil, not its length). dispatchMutating's
// readBody-failure branch previously wrote "InternalError", but
// mq@v1.39.4 types/errors.go:120 models "InternalServerErrorException"
// (ErrorFault: FaultServer) as the real service's only 5xx fault -- so a
// real client's errors.As(&types.InternalServerErrorException{}) never
// matched (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalServerErrorException(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMQClient(t, mq.NewHandler(backend))

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateBroker(t.Context(), &mqsdk.CreateBrokerInput{
		BrokerName:         aws.String(huge),
		DeploymentMode:     types.DeploymentModeSingleInstance,
		EngineType:         types.EngineTypeActivemq,
		HostInstanceType:   aws.String("mq.t3.micro"),
		PubliclyAccessible: aws.Bool(false),
	}, func(o *mqsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalServerErrorException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())

	var ise *types.InternalServerErrorException
	require.ErrorAs(t, err, &ise, "client must map this to the modeled InternalServerErrorException type")
	assert.Equal(t, smithy.FaultServer, ise.ErrorFault())
}

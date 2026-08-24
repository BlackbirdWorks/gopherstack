package bedrockagent_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/require"
)

// TestHandler_OversizedBody_TypesAsInternalServerException drives a real
// bedrockagent SDK client with a request body over
// httputils.MaxRequestBodyBytes (16MB). Handler.Handler() (handler.go) fails
// its ReadBody call before dispatch and, before the fix, wrote
// {"__type":"InternalFailure",...}: no op in bedrockagent@v1.58.4's
// deserializeOpError switch has a case for "InternalFailure" (all 75 ops
// only model "InternalServerException"), so a real client fell through to an
// untyped smithy.GenericAPIError.
func TestHandler_OversizedBody_TypesAsInternalServerException(t *testing.T) {
	t.Parallel()

	client := newTestBedrockAgentClient(t, newTestBedrockAgentHandler())

	huge := strings.Repeat("a", 17*1024*1024)

	_, err := client.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
		AgentName: aws.String(huge),
	})

	require.Error(t, err)

	var typed *types.InternalServerException

	require.ErrorAs(t, err, &typed)
}

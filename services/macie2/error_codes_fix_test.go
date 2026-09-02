package macie2_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// TestCreateAllowList_RealClient_OversizedBody drives the real macie2 SDK
// client with a request body over httputils.MaxRequestBodyBytes. The
// handler's REST router (pkgs/service.RESTRouter) hits its BadRequestBody
// path on that read failure -- found by cmd/errcodeaudit emitting
// "BadRequestException", a code macie2's SDK models nowhere (its 8
// exception types are AccessDenied/Conflict/InternalServer/
// ResourceNotFound/ServiceQuotaExceeded/Throttling/UnprocessableEntity/
// Validation). ValidationException's own doc -- "an error that occurred
// due to a syntax error in a request" -- is the correct fit.
func TestCreateAllowList_RealClient_OversizedBody(t *testing.T) {
	t.Parallel()

	backend := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	h := macie2.NewHandler(backend)
	client := newTestMacie2SDKClient(t, h)

	huge := strings.Repeat("a", int(httputils.MaxRequestBodyBytes)+1024)

	_, err := client.CreateAllowList(t.Context(), &macie2sdk.CreateAllowListInput{
		ClientToken: aws.String("tok"),
		Name:        aws.String("allow-list-oversized"),
		Criteria:    &types.AllowListCriteria{Regex: aws.String(huge)},
	})
	require.Error(t, err)

	var target *types.ValidationException

	require.ErrorAs(t, err, &target)
}

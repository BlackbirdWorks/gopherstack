package sns_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

func newErrorEnvelopeSNSClient(t *testing.T, h *sns.Handler) *snssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return snssdk.NewFromConfig(cfg, func(o *snssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestSMSSandboxPhoneNotFound_ResourceNotFound_RealClient proves that a
// missing sandbox phone number decodes as the real ResourceNotFoundException
// type, not the generic error a wrong wire code produces (gopherstack-uox6
// error-envelope sweep, errtargetaudit).
//
// DeleteSMSSandboxPhoneNumber and VerifySMSSandboxPhoneNumber both declare
// "ResourceNotFound" in their own deserializeOpError<Op> switch
// (aws-sdk-go-v2/service/sns@v1.42.4 deserializers.go) -- not "NotFound".
// ErrPhoneNumberNotFound's own sentinel text already says "ResourceNotFound"
// (errors.go), but handler_errors.go's errorCode() grouped it into the same
// case as ErrTopicNotFound/ErrSubscriptionNotFound/
// ErrPlatformApplicationNotFound/ErrEndpointNotFound, all of which return
// "NotFound" -- so both ops sent "NotFound", a code neither declares.
func TestSMSSandboxPhoneNotFound_ResourceNotFound_RealClient(t *testing.T) {
	t.Parallel()

	newClient := func(t *testing.T) *snssdk.Client {
		t.Helper()

		return newErrorEnvelopeSNSClient(t, sns.NewHandler(sns.NewInMemoryBackend()))
	}

	assertResourceNotFound := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)

		var apiErr *types.ResourceNotFoundException
		require.ErrorAs(t, err, &apiErr,
			"expected a real ResourceNotFoundException from the SDK deserializer, got: %v", err)
	}

	t.Run("delete sms sandbox phone number", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteSMSSandboxPhoneNumber(t.Context(), &snssdk.DeleteSMSSandboxPhoneNumberInput{
			PhoneNumber: aws.String("+15555550100"),
		})
		assertResourceNotFound(t, err)
	})

	t.Run("verify sms sandbox phone number", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.VerifySMSSandboxPhoneNumber(t.Context(), &snssdk.VerifySMSSandboxPhoneNumberInput{
			PhoneNumber:     aws.String("+15555550100"),
			OneTimePassword: aws.String("123456"),
		})
		assertResourceNotFound(t, err)
	})
}

// TestDeletePlatformApplication_MissingArn_Idempotent_RealClient proves that
// deleting a platform application ARN that does not exist succeeds, matching
// this SDK's sibling delete operations. DeletePlatformApplication's own
// deserializeOpError switch declares only AuthorizationError/InternalError/
// InvalidParameter -- no not-found type at all -- unlike its four sibling
// callers of the same ErrPlatformApplicationNotFound sentinel
// (GetPlatformApplicationAttributes/SetPlatformApplicationAttributes/
// CreatePlatformEndpoint/ListEndpointsByPlatformApplication), which all
// correctly declare "NotFound". DeleteEndpoint, the directly analogous
// delete operation in this same SDK module, is documented "This action is
// idempotent"; DeletePlatformApplication's declared error set matches that
// same no-not-found-type shape.
func TestDeletePlatformApplication_MissingArn_Idempotent_RealClient(t *testing.T) {
	t.Parallel()

	client := newErrorEnvelopeSNSClient(t, sns.NewHandler(sns.NewInMemoryBackend()))

	_, err := client.DeletePlatformApplication(t.Context(), &snssdk.DeletePlatformApplicationInput{
		PlatformApplicationArn: aws.String(
			"arn:aws:sns:us-east-1:000000000000:app/GCM/does-not-exist",
		),
	})
	require.NoError(t, err, "DeletePlatformApplication on a nonexistent ARN must be idempotent")
}

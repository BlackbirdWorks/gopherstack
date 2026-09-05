package pinpoint_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	pinpointtypes "github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// newDuplicateTemplateTestClient stands up the real aws-sdk-go-v2 pinpoint
// client against an httptest server running this package's Handler, matching
// the pattern in error_envelope_test.go's
// TestErrorEnvelope_GetAppNotFoundDecodesToTypedError.
func newDuplicateTemplateTestClient(t *testing.T) *pinpointsdk.Client {
	t.Helper()

	backend := pinpoint.NewInMemoryBackend("000000000000", "us-east-1")
	h := pinpoint.NewHandler(backend)

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

	return pinpointsdk.NewFromConfig(cfg, func(o *pinpointsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.RetryMaxAttempts = 1
	})
}

// TestCreateTemplate_DuplicateName_RealClient drives every Create*Template op
// through the real aws-sdk-go-v2 pinpoint client with a name that already
// exists and asserts errors.As unwraps to *types.BadRequestException -- not
// *types.ConflictException. None of these five ops' own
// deserializeOpError<Op> switches (pinpoint@v1.42.4 deserializers.go)
// declare ConflictException at all: the full set for each is
// [BadRequestException, ForbiddenException, InternalServerErrorException,
// MethodNotAllowedException, TooManyRequestsException]. UpdateJourney is the
// package's only op that legitimately declares ConflictException
// (gopherstack-uox6 sweep).
func TestCreateTemplate_DuplicateName_RealClient(t *testing.T) {
	t.Parallel()

	assertBadRequest := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)

		var conflict *pinpointtypes.ConflictException
		require.NotErrorAs(t, err, &conflict,
			"CreateXTemplate never declares ConflictException; got: %v", err)

		var badReq *pinpointtypes.BadRequestException
		require.ErrorAs(t, err, &badReq,
			"expected a real BadRequestException from the SDK deserializer, got: %v", err)
	}

	t.Run("email", func(t *testing.T) {
		t.Parallel()

		client := newDuplicateTemplateTestClient(t)
		name := aws.String("dup-email")

		_, err := client.CreateEmailTemplate(t.Context(), &pinpointsdk.CreateEmailTemplateInput{
			TemplateName:         name,
			EmailTemplateRequest: &pinpointtypes.EmailTemplateRequest{Subject: aws.String("Hello")},
		})
		require.NoError(t, err)

		_, err = client.CreateEmailTemplate(t.Context(), &pinpointsdk.CreateEmailTemplateInput{
			TemplateName:         name,
			EmailTemplateRequest: &pinpointtypes.EmailTemplateRequest{Subject: aws.String("Hello again")},
		})
		assertBadRequest(t, err)
	})

	t.Run("inapp", func(t *testing.T) {
		t.Parallel()

		client := newDuplicateTemplateTestClient(t)
		name := aws.String("dup-inapp")

		_, err := client.CreateInAppTemplate(t.Context(), &pinpointsdk.CreateInAppTemplateInput{
			TemplateName:         name,
			InAppTemplateRequest: &pinpointtypes.InAppTemplateRequest{},
		})
		require.NoError(t, err)

		_, err = client.CreateInAppTemplate(t.Context(), &pinpointsdk.CreateInAppTemplateInput{
			TemplateName:         name,
			InAppTemplateRequest: &pinpointtypes.InAppTemplateRequest{},
		})
		assertBadRequest(t, err)
	})

	t.Run("push", func(t *testing.T) {
		t.Parallel()

		client := newDuplicateTemplateTestClient(t)
		name := aws.String("dup-push")

		_, err := client.CreatePushTemplate(t.Context(), &pinpointsdk.CreatePushTemplateInput{
			TemplateName:                    name,
			PushNotificationTemplateRequest: &pinpointtypes.PushNotificationTemplateRequest{},
		})
		require.NoError(t, err)

		_, err = client.CreatePushTemplate(t.Context(), &pinpointsdk.CreatePushTemplateInput{
			TemplateName:                    name,
			PushNotificationTemplateRequest: &pinpointtypes.PushNotificationTemplateRequest{},
		})
		assertBadRequest(t, err)
	})

	t.Run("sms", func(t *testing.T) {
		t.Parallel()

		client := newDuplicateTemplateTestClient(t)
		name := aws.String("dup-sms")

		_, err := client.CreateSmsTemplate(t.Context(), &pinpointsdk.CreateSmsTemplateInput{
			TemplateName:       name,
			SMSTemplateRequest: &pinpointtypes.SMSTemplateRequest{Body: aws.String("Hi")},
		})
		require.NoError(t, err)

		_, err = client.CreateSmsTemplate(t.Context(), &pinpointsdk.CreateSmsTemplateInput{
			TemplateName:       name,
			SMSTemplateRequest: &pinpointtypes.SMSTemplateRequest{Body: aws.String("Hi again")},
		})
		assertBadRequest(t, err)
	})

	t.Run("voice", func(t *testing.T) {
		t.Parallel()

		client := newDuplicateTemplateTestClient(t)
		name := aws.String("dup-voice")

		_, err := client.CreateVoiceTemplate(t.Context(), &pinpointsdk.CreateVoiceTemplateInput{
			TemplateName:         name,
			VoiceTemplateRequest: &pinpointtypes.VoiceTemplateRequest{Body: aws.String("First")},
		})
		require.NoError(t, err)

		_, err = client.CreateVoiceTemplate(t.Context(), &pinpointsdk.CreateVoiceTemplateInput{
			TemplateName:         name,
			VoiceTemplateRequest: &pinpointtypes.VoiceTemplateRequest{Body: aws.String("Second")},
		})
		assertBadRequest(t, err)
	})
}

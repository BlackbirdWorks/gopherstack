package account_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	accountsdk "github.com/aws/aws-sdk-go-v2/service/account"
	"github.com/aws/aws-sdk-go-v2/service/account/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/account"
)

const phoneTestOTP = "123456"

func newPhoneTestClient(t *testing.T, b *account.InMemoryBackend) *accountsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(account.NewHandler(b)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return accountsdk.NewFromConfig(cfg, func(o *accountsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func putPhoneContact(t *testing.T, c *accountsdk.Client, phone string) {
	t.Helper()

	_, err := c.PutContactInformation(t.Context(), &accountsdk.PutContactInformationInput{
		ContactInformation: &types.ContactInformation{
			AddressLine1: aws.String("1 Rue Test"),
			City:         aws.String("Paris"),
			CountryCode:  aws.String("FR"),
			FullName:     aws.String("Test User"),
			PhoneNumber:  aws.String(phone),
			PostalCode:   aws.String("75001"),
		},
	})
	require.NoError(t, err)
}

func TestSDKRoundTrip_PhoneNumberVerification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *accountsdk.Client)
		name string
	}{
		{
			name: "send_without_contact_info",
			run: func(t *testing.T, c *accountsdk.Client) {
				t.Helper()

				_, err := c.SendPhoneNumberVerification(t.Context(), &accountsdk.SendPhoneNumberVerificationInput{})
				var nf *types.ResourceNotFoundException
				require.ErrorAs(t, err, &nf)
			},
		},
		{
			name: "send_then_verify",
			run: func(t *testing.T, c *accountsdk.Client) {
				t.Helper()
				putPhoneContact(t, c, "+33123456789")

				sent, err := c.SendPhoneNumberVerification(t.Context(), &accountsdk.SendPhoneNumberVerificationInput{})
				require.NoError(t, err)
				assert.Equal(t, types.PhoneNumberVerificationStatusPending, sent.Status)

				ver, err := c.VerifyPhoneNumber(
					t.Context(),
					&accountsdk.VerifyPhoneNumberInput{Otp: aws.String(phoneTestOTP)},
				)
				require.NoError(t, err)
				assert.Equal(t, types.PhoneNumberVerificationStatusVerified, ver.Status)
			},
		},
		{
			name: "wrong_otp",
			run: func(t *testing.T, c *accountsdk.Client) {
				t.Helper()
				putPhoneContact(t, c, "+33123456789")

				_, err := c.SendPhoneNumberVerification(t.Context(), &accountsdk.SendPhoneNumberVerificationInput{})
				require.NoError(t, err)

				_, err = c.VerifyPhoneNumber(t.Context(), &accountsdk.VerifyPhoneNumberInput{Otp: aws.String("000000")})
				var ve *types.ValidationException
				require.ErrorAs(t, err, &ve)
			},
		},
		{
			name: "verify_without_send",
			run: func(t *testing.T, c *accountsdk.Client) {
				t.Helper()
				putPhoneContact(t, c, "+33123456789")

				_, err := c.VerifyPhoneNumber(
					t.Context(),
					&accountsdk.VerifyPhoneNumberInput{Otp: aws.String(phoneTestOTP)},
				)
				var nf *types.ResourceNotFoundException
				require.ErrorAs(t, err, &nf)
			},
		},
		{
			name: "phone_change_invalidates_pending",
			run: func(t *testing.T, c *accountsdk.Client) {
				t.Helper()
				putPhoneContact(t, c, "+33123456789")

				_, err := c.SendPhoneNumberVerification(t.Context(), &accountsdk.SendPhoneNumberVerificationInput{})
				require.NoError(t, err)

				putPhoneContact(t, c, "+33987654321")

				_, err = c.VerifyPhoneNumber(
					t.Context(),
					&accountsdk.VerifyPhoneNumberInput{Otp: aws.String(phoneTestOTP)},
				)
				var nf *types.ResourceNotFoundException
				require.ErrorAs(t, err, &nf)
			},
		},
		{
			name: "otp_single_use",
			run: func(t *testing.T, c *accountsdk.Client) {
				t.Helper()
				putPhoneContact(t, c, "+33123456789")

				_, err := c.SendPhoneNumberVerification(t.Context(), &accountsdk.SendPhoneNumberVerificationInput{})
				require.NoError(t, err)

				_, err = c.VerifyPhoneNumber(
					t.Context(),
					&accountsdk.VerifyPhoneNumberInput{Otp: aws.String(phoneTestOTP)},
				)
				require.NoError(t, err)

				_, err = c.VerifyPhoneNumber(
					t.Context(),
					&accountsdk.VerifyPhoneNumberInput{Otp: aws.String(phoneTestOTP)},
				)
				var nf *types.ResourceNotFoundException
				require.ErrorAs(t, err, &nf)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newPhoneTestClient(t, account.NewInMemoryBackend("000000000000", "us-east-1")))
		})
	}
}

func TestPhoneNumberVerification_Persistence(t *testing.T) {
	t.Parallel()

	src := account.NewInMemoryBackend("000000000000", "us-east-1")
	c := newPhoneTestClient(t, src)
	putPhoneContact(t, c, "+33123456789")

	_, err := c.SendPhoneNumberVerification(t.Context(), &accountsdk.SendPhoneNumberVerificationInput{})
	require.NoError(t, err)

	dst := account.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	ver, err := newPhoneTestClient(t, dst).VerifyPhoneNumber(
		t.Context(), &accountsdk.VerifyPhoneNumberInput{Otp: aws.String(phoneTestOTP)},
	)
	require.NoError(t, err)
	assert.Equal(t, types.PhoneNumberVerificationStatusVerified, ver.Status)
}

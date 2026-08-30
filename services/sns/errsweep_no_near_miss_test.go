package sns_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestSDK_CreatePlatformApplication_Duplicate_TypedError drives the real
// aws-sdk-go-v2 sns client against a duplicate CreatePlatformApplication and
// asserts errors.As decodes *types.InvalidParameterException.
//
// CreatePlatformApplication's own deserializeOpError
// (sns@v1.42.4/deserializers.go:437-477) models exactly three errors --
// AuthorizationError, InternalError, InvalidParameter -- and no
// "already exists" shape exists anywhere in the pinned sns module. The
// backend previously emitted the invented code "PlatformApplicationAlreadyExists",
// which every one of those deserializers rejects into a smithy.GenericAPIError
// the typed client cannot decode into any *types.*Exception.
func TestSDK_CreatePlatformApplication_Duplicate_TypedError(t *testing.T) {
	t.Parallel()

	h := sns.NewHandler(sns.NewInMemoryBackend())
	client := newTestSNSClient(t, h)

	in := &snssdk.CreatePlatformApplicationInput{
		Name:       aws.String("MyApp"),
		Platform:   aws.String("GCM"),
		Attributes: map[string]string{"PlatformCredential": "my-api-key"},
	}

	_, err := client.CreatePlatformApplication(t.Context(), in)
	require.NoError(t, err)

	_, err = client.CreatePlatformApplication(t.Context(), in)
	require.Error(t, err)

	var target *snstypes.InvalidParameterException
	require.ErrorAs(t, err, &target,
		"expected a real InvalidParameterException from the SDK deserializer, got %T: %v", err, err)
}

// TestSDK_CreateSMSSandboxPhoneNumber_Duplicate_TypedError drives the real
// aws-sdk-go-v2 sns client against a duplicate CreateSMSSandboxPhoneNumber
// and asserts errors.As decodes *types.UserErrorException.
//
// CreateSMSSandboxPhoneNumber's own deserializeOpError
// (sns@v1.42.4/deserializers.go:676-726) models AuthorizationError,
// InternalError, InvalidParameter, OptedOut, Throttled, UserError -- no
// "already exists" shape. UserErrorException is documented as "a request
// parameter does not comply with the associated constraints", the nearest
// modelled fit for a uniqueness-constraint violation; this mapping is
// UNCONFIRMED against AWS prose docs (see PARITY.md).
func TestSDK_CreateSMSSandboxPhoneNumber_Duplicate_TypedError(t *testing.T) {
	t.Parallel()

	h := sns.NewHandler(sns.NewInMemoryBackend())
	client := newTestSNSClient(t, h)

	in := &snssdk.CreateSMSSandboxPhoneNumberInput{
		PhoneNumber: aws.String("+15005550006"),
	}

	_, err := client.CreateSMSSandboxPhoneNumber(t.Context(), in)
	require.NoError(t, err)

	_, err = client.CreateSMSSandboxPhoneNumber(t.Context(), in)
	require.Error(t, err)

	var target *snstypes.UserErrorException
	require.ErrorAs(t, err, &target,
		"expected a real UserErrorException from the SDK deserializer, got %T: %v", err, err)
}

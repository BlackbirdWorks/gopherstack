package sns_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestEmptyResultElement_RealClient covers SNS ops whose real output shape has zero
// members but whose deserializer still calls decoder.GetElement("<Op>Result")
// (sns@v1.42.4 deserializers.go, confirmed per-op). gopherstack omitted the element
// for these five, so every real SDK client failed deserialization with
// "deserialization failed: failed to decode response body ... node not found" even
// though the backend mutation succeeded. The assertion is exactly that the call
// deserializes without error -- there is nothing else to check on an empty output.
func TestEmptyResultElement_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, client *snssdk.Client) error
		name string
	}{
		{
			name: "createsmssandboxphonenumber",
			call: func(t *testing.T, client *snssdk.Client) error {
				t.Helper()

				_, err := client.CreateSMSSandboxPhoneNumber(t.Context(), &snssdk.CreateSMSSandboxPhoneNumberInput{
					PhoneNumber: aws.String("+15005550001"),
				})

				return err
			},
		},
		{
			name: "deletesmssandboxphonenumber",
			call: func(t *testing.T, client *snssdk.Client) error {
				t.Helper()

				_, err := client.CreateSMSSandboxPhoneNumber(t.Context(), &snssdk.CreateSMSSandboxPhoneNumberInput{
					PhoneNumber: aws.String("+15005550002"),
				})
				require.NoError(t, err)

				_, err = client.DeleteSMSSandboxPhoneNumber(t.Context(), &snssdk.DeleteSMSSandboxPhoneNumberInput{
					PhoneNumber: aws.String("+15005550002"),
				})

				return err
			},
		},
		{
			name: "verifysmssandboxphonenumber",
			call: func(t *testing.T, client *snssdk.Client) error {
				t.Helper()

				_, err := client.CreateSMSSandboxPhoneNumber(t.Context(), &snssdk.CreateSMSSandboxPhoneNumberInput{
					PhoneNumber: aws.String("+15005550003"),
				})
				require.NoError(t, err)

				_, err = client.VerifySMSSandboxPhoneNumber(t.Context(), &snssdk.VerifySMSSandboxPhoneNumberInput{
					PhoneNumber:     aws.String("+15005550003"),
					OneTimePassword: aws.String("123456"),
				})

				return err
			},
		},
		{
			name: "optinphonenumber",
			call: func(t *testing.T, client *snssdk.Client) error {
				t.Helper()

				_, err := client.OptInPhoneNumber(t.Context(), &snssdk.OptInPhoneNumberInput{
					PhoneNumber: aws.String("+15005550004"),
				})

				return err
			},
		},
		{
			name: "setsmsattributes",
			call: func(t *testing.T, client *snssdk.Client) error {
				t.Helper()

				_, err := client.SetSMSAttributes(t.Context(), &snssdk.SetSMSAttributesInput{
					Attributes: map[string]string{"DefaultSMSType": "Transactional"},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			h := sns.NewHandler(backend)
			client := newTestSNSClient(t, h)

			require.NoError(t, tt.call(t, client))
		})
	}
}

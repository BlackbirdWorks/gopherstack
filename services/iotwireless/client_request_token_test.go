package iotwireless_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"
	types "github.com/aws/aws-sdk-go-v2/service/iotwireless/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientRequestToken_Idempotency_RealClient(t *testing.T) {
	t.Parallel()

	createDestination := func(t *testing.T, c *iotwirelesssdk.Client, name, token string) (string, error) {
		t.Helper()

		out, err := c.CreateDestination(t.Context(), &iotwirelesssdk.CreateDestinationInput{
			Name:               aws.String(name),
			Expression:         aws.String("rule"),
			ExpressionType:     types.ExpressionTypeRuleName,
			RoleArn:            aws.String("arn:aws:iam::000000000000:role/r"),
			ClientRequestToken: aws.String(token),
		})
		if err != nil {
			return "", err
		}

		return aws.ToString(out.Arn), nil
	}

	createServiceProfile := func(t *testing.T, c *iotwirelesssdk.Client, name, token string) (string, error) {
		t.Helper()

		out, err := c.CreateServiceProfile(t.Context(), &iotwirelesssdk.CreateServiceProfileInput{
			Name:               aws.String(name),
			ClientRequestToken: aws.String(token),
		})
		if err != nil {
			return "", err
		}

		return aws.ToString(out.Id), nil
	}

	createDeviceProfile := func(t *testing.T, c *iotwirelesssdk.Client, name, token string) (string, error) {
		t.Helper()

		out, err := c.CreateDeviceProfile(t.Context(), &iotwirelesssdk.CreateDeviceProfileInput{
			Name:               aws.String(name),
			ClientRequestToken: aws.String(token),
		})
		if err != nil {
			return "", err
		}

		return aws.ToString(out.Id), nil
	}

	cases := []struct {
		create func(t *testing.T, c *iotwirelesssdk.Client, name, token string) (string, error)
		name   string
	}{
		{create: createDestination, name: "destination"},
		{create: createServiceProfile, name: "service_profile"},
		{create: createDeviceProfile, name: "device_profile"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestIoTWirelessRegistryServer(t)
			client := newTestIoTWirelessSDKClient(t, srv.URL)

			first, err := tc.create(t, client, "idem-one", "token-1")
			require.NoError(t, err)

			replay, err := tc.create(t, client, "idem-one", "token-1")
			require.NoError(t, err)
			assert.Equal(t, first, replay, "same token and parameters replay the first result")

			_, err = tc.create(t, client, "idem-two", "token-1")
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ConflictException", apiErr.ErrorCode())

			other, err := tc.create(t, client, "idem-three", "token-2")
			require.NoError(t, err)
			assert.NotEqual(t, first, other)
		})
	}
}

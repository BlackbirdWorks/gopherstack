package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_IAMAudit_GetMFADevice_NotFound verifies that GetMFADevice
// returns an error for a serial number that does not correspond to any
// virtual MFA device.
func TestIntegration_IAMAudit_GetMFADevice_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	serial := "arn:aws:iam::123456789012:mfa/nonexistent-" + uuid.NewString()[:8]

	_, err := client.GetMFADevice(ctx, &iamsdk.GetMFADeviceInput{
		SerialNumber: aws.String(serial),
	})
	require.Error(t, err, "GetMFADevice for an unknown serial number must return an error")
}

// TestIntegration_IAMAudit_GetMFADevice_Found creates a user and a virtual MFA
// device, enables it, then verifies GetMFADevice returns the real device with
// its owner and enable date.
func TestIntegration_IAMAudit_GetMFADevice_Found(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	userName := "mfa-user-" + uuid.NewString()[:8]
	deviceName := "mfa-device-" + uuid.NewString()[:8]

	_, err := client.CreateUser(ctx, &iamsdk.CreateUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteUser(ctx, &iamsdk.DeleteUserInput{UserName: aws.String(userName)})
	})

	createOut, err := client.CreateVirtualMFADevice(ctx, &iamsdk.CreateVirtualMFADeviceInput{
		VirtualMFADeviceName: aws.String(deviceName),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.VirtualMFADevice)

	serial := *createOut.VirtualMFADevice.SerialNumber

	_, err = client.EnableMFADevice(ctx, &iamsdk.EnableMFADeviceInput{
		UserName:            aws.String(userName),
		SerialNumber:        aws.String(serial),
		AuthenticationCode1: aws.String("123456"),
		AuthenticationCode2: aws.String("654321"),
	})
	require.NoError(t, err)

	getOut, err := client.GetMFADevice(ctx, &iamsdk.GetMFADeviceInput{
		SerialNumber: aws.String(serial),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.SerialNumber)
	assert.Equal(t, serial, *getOut.SerialNumber)
	assert.NotNil(t, getOut.EnableDate, "GetMFADevice must return a real EnableDate")
	require.NotNil(t, getOut.UserName)
	assert.Equal(t, userName, *getOut.UserName, "GetMFADevice must return the owning user")
}

// TestIntegration_IAMAudit_GetContextKeysForCustomPolicy verifies that a policy
// document referencing a condition context key is parsed and that key is
// returned.
func TestIntegration_IAMAudit_GetContextKeysForCustomPolicy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	policyDoc := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Action": "s3:GetObject",
				"Resource": "*",
				"Condition": {
					"StringEquals": {"aws:username": "alice"}
				}
			}
		]
	}`

	out, err := client.GetContextKeysForCustomPolicy(ctx, &iamsdk.GetContextKeysForCustomPolicyInput{
		PolicyInputList: []string{policyDoc},
	})
	require.NoError(t, err)
	assert.Contains(t, out.ContextKeyNames, "aws:username",
		"GetContextKeysForCustomPolicy must return condition context keys referenced by the policy")
}

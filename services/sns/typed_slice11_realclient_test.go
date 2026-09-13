package sns_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestTypedSlice11RealClient drives sns's typed-coverage-blind ops
// (gopherstack-n3zi slice 11) through the real aws-sdk-go-v2 client.
func TestTypedSlice11RealClient(t *testing.T) {
	t.Parallel()

	t.Run("confirm subscription", func(t *testing.T) {
		t.Parallel()

		backend := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
		client := newTestSNSClient(t, sns.NewHandler(backend))
		ctx := t.Context()

		topicOut, err := client.CreateTopic(ctx, &snssdk.CreateTopicInput{
			Name: aws.String("s11-confirm-topic"),
		})
		require.NoError(t, err)
		topicArn := aws.ToString(topicOut.TopicArn)

		_, err = client.Subscribe(ctx, &snssdk.SubscribeInput{
			TopicArn: aws.String(topicArn),
			Protocol: aws.String("email"),
			Endpoint: aws.String("user@example.com"),
		})
		require.NoError(t, err)

		attrsBefore, err := client.ListSubscriptionsByTopic(ctx, &snssdk.ListSubscriptionsByTopicInput{
			TopicArn: aws.String(topicArn),
		})
		require.NoError(t, err)
		require.Len(t, attrsBefore.Subscriptions, 1)
		// Real AWS placeholder for an unconfirmed subscription ("If the
		// subscription confirmation has not been made yet ... the
		// SubscriptionArn value contains the string 'PendingConfirmation'",
		// deserializers.go's Subscription doc), not yet the real ARN.
		require.Equal(t, "pending confirmation", aws.ToString(attrsBefore.Subscriptions[0].SubscriptionArn))

		confirmOut, err := client.ConfirmSubscription(ctx, &snssdk.ConfirmSubscriptionInput{
			TopicArn: aws.String(topicArn),
			Token:    aws.String("any-non-empty-token"),
		})
		require.NoError(t, err)
		realSubArn := aws.ToString(confirmOut.SubscriptionArn)
		assert.Contains(t, realSubArn, "s11-confirm-topic")

		attrsOut, err := client.GetSubscriptionAttributes(ctx, &snssdk.GetSubscriptionAttributesInput{
			SubscriptionArn: aws.String(realSubArn),
		})
		require.NoError(t, err)
		assert.Equal(t, "false", attrsOut.Attributes["PendingConfirmation"])
	})

	t.Run("data protection policy", func(t *testing.T) {
		t.Parallel()

		backend := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
		client := newTestSNSClient(t, sns.NewHandler(backend))
		ctx := t.Context()

		topicOut, err := client.CreateTopic(ctx, &snssdk.CreateTopicInput{
			Name: aws.String("s11-dpp-topic"),
		})
		require.NoError(t, err)
		topicArn := aws.ToString(topicOut.TopicArn)

		policy := `{"Name":"s11-policy","Version":"2021-06-01","Statement":[]}`
		_, err = client.PutDataProtectionPolicy(ctx, &snssdk.PutDataProtectionPolicyInput{
			ResourceArn:          aws.String(topicArn),
			DataProtectionPolicy: aws.String(policy),
		})
		require.NoError(t, err)

		getOut, err := client.GetDataProtectionPolicy(ctx, &snssdk.GetDataProtectionPolicyInput{
			ResourceArn: aws.String(topicArn),
		})
		require.NoError(t, err)
		assert.JSONEq(t, policy, aws.ToString(getOut.DataProtectionPolicy))
	})

	t.Run("sms attributes and sandbox", func(t *testing.T) {
		t.Parallel()

		backend := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
		client := newTestSNSClient(t, sns.NewHandler(backend))
		ctx := t.Context()

		_, err := client.SetSMSAttributes(ctx, &snssdk.SetSMSAttributesInput{
			Attributes: map[string]string{"DefaultSMSType": "Transactional"},
		})
		require.NoError(t, err)

		attrOut, err := client.GetSMSAttributes(ctx, &snssdk.GetSMSAttributesInput{
			Attributes: []string{"DefaultSMSType"},
		})
		require.NoError(t, err)
		assert.Equal(t, "Transactional", attrOut.Attributes["DefaultSMSType"])

		sandboxOut, err := client.GetSMSSandboxAccountStatus(ctx, &snssdk.GetSMSSandboxAccountStatusInput{})
		require.NoError(t, err)
		assert.True(t, sandboxOut.IsInSandbox, "new accounts default to SMS sandbox mode")

		_, err = client.CreateSMSSandboxPhoneNumber(ctx, &snssdk.CreateSMSSandboxPhoneNumberInput{
			PhoneNumber: aws.String("+15005550006"),
		})
		require.NoError(t, err)

		listOut, err := client.ListSMSSandboxPhoneNumbers(ctx, &snssdk.ListSMSSandboxPhoneNumbersInput{})
		require.NoError(t, err)
		require.Len(t, listOut.PhoneNumbers, 1)
		assert.Equal(t, "+15005550006", aws.ToString(listOut.PhoneNumbers[0].PhoneNumber))
		assert.Equal(t, "Pending", string(listOut.PhoneNumbers[0].Status))
	})

	t.Run("phone number opted out", func(t *testing.T) {
		t.Parallel()

		backend := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
		client := newTestSNSClient(t, sns.NewHandler(backend))
		ctx := t.Context()

		sns.AddOptedOutPhoneNumberForTest(backend, "+15005550001")

		checkOut, err := client.CheckIfPhoneNumberIsOptedOut(ctx, &snssdk.CheckIfPhoneNumberIsOptedOutInput{
			PhoneNumber: aws.String("+15005550001"),
		})
		require.NoError(t, err)
		assert.True(t, checkOut.IsOptedOut)

		listOut, err := client.ListPhoneNumbersOptedOut(ctx, &snssdk.ListPhoneNumbersOptedOutInput{})
		require.NoError(t, err)
		assert.Contains(t, listOut.PhoneNumbers, "+15005550001")
	})
}

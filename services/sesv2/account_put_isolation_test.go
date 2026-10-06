package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccount_PutsLeaveSendingEnabledAlone(t *testing.T) {
	t.Parallel()

	cases := []struct {
		put  func(t *testing.T, c *sesv2sdk.Client)
		name string
	}{
		{
			name: "put account details",
			put: func(t *testing.T, c *sesv2sdk.Client) {
				t.Helper()
				_, err := c.PutAccountDetails(t.Context(), &sesv2sdk.PutAccountDetailsInput{
					MailType: types.MailTypeMarketing, WebsiteURL: aws.String("https://x.example"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "put suppression attributes",
			put: func(t *testing.T, c *sesv2sdk.Client) {
				t.Helper()
				_, err := c.PutAccountSuppressionAttributes(t.Context(), &sesv2sdk.PutAccountSuppressionAttributesInput{
					SuppressedReasons: []types.SuppressionListReason{types.SuppressionListReasonBounce},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "put dedicated ip warmup",
			put: func(t *testing.T, c *sesv2sdk.Client) {
				t.Helper()
				_, err := c.PutAccountDedicatedIpWarmupAttributes(
					t.Context(),
					&sesv2sdk.PutAccountDedicatedIpWarmupAttributesInput{AutoWarmupEnabled: true},
				)
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newSESv2TestHandler(t)
			c := newSESv2SDKClient(t, h)

			tc.put(t, c)

			got, err := c.GetAccount(t.Context(), &sesv2sdk.GetAccountInput{})
			require.NoError(t, err)
			assert.True(t, got.SendingEnabled)
		})
	}
}

func TestAccount_PutDetailsKeepsOtherAttributes(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	c := newSESv2SDKClient(t, h)
	ctx := t.Context()

	_, err := c.PutAccountSendingAttributes(ctx, &sesv2sdk.PutAccountSendingAttributesInput{SendingEnabled: false})
	require.NoError(t, err)
	_, err = c.PutAccountDetails(
		ctx,
		&sesv2sdk.PutAccountDetailsInput{
			MailType:   types.MailTypeTransactional,
			WebsiteURL: aws.String("https://x.example"),
		},
	)
	require.NoError(t, err)

	got, err := c.GetAccount(ctx, &sesv2sdk.GetAccountInput{})
	require.NoError(t, err)
	assert.False(t, got.SendingEnabled)
	assert.Equal(t, types.MailTypeTransactional, got.Details.MailType)
}

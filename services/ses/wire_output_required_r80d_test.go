package ses_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestSDKRoundTrip_RequiredOutputMembers_r80d proves gopherstack-r80d's two
// findings against ses@v1.37.4's Get*Attributes ops: types.
// IdentityMailFromDomainAttributes.MailFromDomain and types.
// IdentityNotificationAttributes.{BounceTopic,ComplaintTopic,DeliveryTopic}
// are all required *string members, but gopherstack tagged them `omitempty`
// on the wire, so an identity that never configured a MailFrom domain or an
// SNS notification topic -- the default, common state after
// VerifyEmailIdentity/VerifyDomainIdentity -- decoded them as nil instead of
// a non-nil pointer to "". Unlike a non-pointer required field (where
// omitted vs present-empty decode identically), a real client can tell the
// difference on these: `aws.ToString` masks it, but the pointer itself is
// nil, which is what "This member is required" promises never happens.
func TestSDKRoundTrip_RequiredOutputMembers_r80d(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, backend *ses.InMemoryBackend, client *sessdk.Client)
		name string
	}{
		{testMailFromDomainAttributesUnconfigured, "mail from domain attributes unconfigured"},
		{testNotificationAttributesUnconfigured, "notification attributes unconfigured"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := ses.NewInMemoryBackend()
			h := ses.NewHandler(backend)
			client := newTestSESClient(t, h)

			tc.run(t, backend, client)
		})
	}
}

func testMailFromDomainAttributesUnconfigured(t *testing.T, backend *ses.InMemoryBackend, client *sessdk.Client) {
	t.Helper()
	ctx := t.Context()

	const identity = "rt-mailfrom-unconfigured@example.com"
	require.NoError(t, backend.VerifyEmailIdentity(identity))

	out, err := client.GetIdentityMailFromDomainAttributes(ctx, &sessdk.GetIdentityMailFromDomainAttributesInput{
		Identities: []string{identity},
	})
	require.NoError(t, err)
	require.Contains(t, out.MailFromDomainAttributes, identity)

	attrs := out.MailFromDomainAttributes[identity]
	if assert.NotNil(t, attrs.MailFromDomain, "required member MailFromDomain decoded nil") {
		assert.Empty(t, aws.ToString(attrs.MailFromDomain))
	}
}

func testNotificationAttributesUnconfigured(t *testing.T, backend *ses.InMemoryBackend, client *sessdk.Client) {
	t.Helper()
	ctx := t.Context()

	const identity = "rt-notif-unconfigured@example.com"
	require.NoError(t, backend.VerifyEmailIdentity(identity))

	out, err := client.GetIdentityNotificationAttributes(ctx, &sessdk.GetIdentityNotificationAttributesInput{
		Identities: []string{identity},
	})
	require.NoError(t, err)
	require.Contains(t, out.NotificationAttributes, identity)

	attrs := out.NotificationAttributes[identity]
	assert.NotNil(t, attrs.BounceTopic, "required member BounceTopic decoded nil")
	assert.NotNil(t, attrs.ComplaintTopic, "required member ComplaintTopic decoded nil")
	assert.NotNil(t, attrs.DeliveryTopic, "required member DeliveryTopic decoded nil")
}

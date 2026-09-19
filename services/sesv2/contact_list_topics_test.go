package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestContactList_Topics proves CreateContactList's Topics member
// (aws-sdk-go-v2/service/sesv2@v1.66.4 api_op_CreateContactList.go) round-trips
// through GetContactList: previously Topics was parsed nowhere, so a real
// client's Topics were silently dropped and GetContactList never echoed them
// back, even though the real GetContactListOutput carries a Topics member.
// ListContactLists' own item shape (types.ContactList) genuinely has no
// Topics member, so it must stay absent there.
func TestContactList_Topics(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	client := newSESv2SDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateContactList(ctx, &sesv2sdk.CreateContactListInput{
		ContactListName: aws.String("topics-list"),
		Topics: []sesv2types.Topic{
			{
				TopicName:                 aws.String("news"),
				DisplayName:               aws.String("Newsletter"),
				DefaultSubscriptionStatus: sesv2types.SubscriptionStatusOptIn,
			},
		},
	})
	require.NoError(t, err)

	t.Run("get echoes topics", func(t *testing.T) {
		t.Parallel()

		getOut, getErr := client.GetContactList(ctx, &sesv2sdk.GetContactListInput{
			ContactListName: aws.String("topics-list"),
		})
		require.NoError(t, getErr)
		require.Len(t, getOut.Topics, 1)
		assert.Equal(t, "news", aws.ToString(getOut.Topics[0].TopicName))
		assert.Equal(t, "Newsletter", aws.ToString(getOut.Topics[0].DisplayName))
		assert.Equal(t, sesv2types.SubscriptionStatusOptIn, getOut.Topics[0].DefaultSubscriptionStatus)
	})

	t.Run("list omits topics", func(t *testing.T) {
		t.Parallel()

		listOut, listErr := client.ListContactLists(ctx, &sesv2sdk.ListContactListsInput{})
		require.NoError(t, listErr)
		require.NotEmpty(t, listOut.ContactLists)
	})
}

// TestContact_AttributesDataAndTopicDefaultPreferences proves CreateContact's
// AttributesData member round-trips through GetContact (previously dropped
// entirely), and that TopicDefaultPreferences -- a real member of both
// GetContactOutput and ListContacts' Contact item shape -- reports the
// contact list's own topic defaults.
func TestContact_AttributesDataAndTopicDefaultPreferences(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	client := newSESv2SDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateContactList(ctx, &sesv2sdk.CreateContactListInput{
		ContactListName: aws.String("defaults-list"),
		Topics: []sesv2types.Topic{
			{
				TopicName:                 aws.String("promo"),
				DisplayName:               aws.String("Promotions"),
				DefaultSubscriptionStatus: sesv2types.SubscriptionStatusOptOut,
			},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateContact(ctx, &sesv2sdk.CreateContactInput{
		ContactListName: aws.String("defaults-list"),
		EmailAddress:    aws.String("reader@example.com"),
		AttributesData:  aws.String(`{"plan":"gold"}`),
	})
	require.NoError(t, err)

	t.Run("get echoes attributesdata and topic default preferences", func(t *testing.T) {
		t.Parallel()

		getOut, getErr := client.GetContact(ctx, &sesv2sdk.GetContactInput{
			ContactListName: aws.String("defaults-list"),
			EmailAddress:    aws.String("reader@example.com"),
		})
		require.NoError(t, getErr)
		assert.JSONEq(t, `{"plan":"gold"}`, aws.ToString(getOut.AttributesData))
		require.Len(t, getOut.TopicDefaultPreferences, 1)
		assert.Equal(t, "promo", aws.ToString(getOut.TopicDefaultPreferences[0].TopicName))
		assert.Equal(t, sesv2types.SubscriptionStatusOptOut, getOut.TopicDefaultPreferences[0].SubscriptionStatus)
	})

	t.Run("list reports topic default preferences", func(t *testing.T) {
		t.Parallel()

		listOut, listErr := client.ListContacts(ctx, &sesv2sdk.ListContactsInput{
			ContactListName: aws.String("defaults-list"),
		})
		require.NoError(t, listErr)
		require.Len(t, listOut.Contacts, 1)
		require.Len(t, listOut.Contacts[0].TopicDefaultPreferences, 1)
		assert.Equal(t, "promo", aws.ToString(listOut.Contacts[0].TopicDefaultPreferences[0].TopicName))
	})
}

package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListContacts_TopicFilter(t *testing.T) {
	t.Parallel()

	topic := func(use bool, status sesv2types.SubscriptionStatus) *sesv2types.ListContactsFilter {
		return &sesv2types.ListContactsFilter{
			FilteredStatus: status,
			TopicFilter: &sesv2types.TopicFilter{
				TopicName:                         aws.String("news"),
				UseDefaultIfPreferenceUnavailable: use,
			},
		}
	}

	tests := []struct {
		name   string
		filter *sesv2types.ListContactsFilter
		want   []string
	}{
		{"no_filter", nil, []string{"a@x.com", "b@x.com", "c@x.com"}},
		{"opt_in_explicit_only", topic(false, sesv2types.SubscriptionStatusOptIn), []string{"a@x.com"}},
		{"opt_in_with_default", topic(true, sesv2types.SubscriptionStatusOptIn), []string{"a@x.com", "c@x.com"}},
		{"opt_out_explicit_only", topic(false, sesv2types.SubscriptionStatusOptOut), []string{"b@x.com"}},
		{"status_alone_unapplied", &sesv2types.ListContactsFilter{FilteredStatus: sesv2types.SubscriptionStatusOptIn},
			[]string{"a@x.com", "b@x.com", "c@x.com"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newSESv2TestHandler(t)
			client := newSESv2SDKClient(t, h)
			ctx := t.Context()

			_, err := client.CreateContactList(ctx, &sesv2sdk.CreateContactListInput{
				ContactListName: aws.String("fl"),
				Topics: []sesv2types.Topic{{
					TopicName: aws.String("news"), DisplayName: aws.String("News"),
					DefaultSubscriptionStatus: sesv2types.SubscriptionStatusOptIn,
				}},
			})
			require.NoError(t, err)

			for email, prefs := range map[string][]sesv2types.TopicPreference{
				"a@x.com": {{TopicName: aws.String("news"), SubscriptionStatus: sesv2types.SubscriptionStatusOptIn}},
				"b@x.com": {{TopicName: aws.String("news"), SubscriptionStatus: sesv2types.SubscriptionStatusOptOut}},
				"c@x.com": nil,
			} {
				_, err = client.CreateContact(ctx, &sesv2sdk.CreateContactInput{
					ContactListName: aws.String("fl"), EmailAddress: aws.String(email), TopicPreferences: prefs,
				})
				require.NoError(t, err)
			}

			out, err := client.ListContacts(ctx, &sesv2sdk.ListContactsInput{
				ContactListName: aws.String("fl"), Filter: tt.filter,
			})
			require.NoError(t, err)

			var got []string
			for _, c := range out.Contacts {
				got = append(got, aws.ToString(c.EmailAddress))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

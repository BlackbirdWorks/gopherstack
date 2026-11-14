package connect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *contactEnv) scheduledTask(t *testing.T, prev string, at time.Time) string {
	t.Helper()

	in := &sdk.StartTaskContactInput{
		InstanceId: aws.String(e.inst), ContactFlowId: aws.String(e.flow), Name: aws.String("cb"),
		ScheduledTime: aws.Time(at),
	}
	if prev != "" {
		in.PreviousContactId = aws.String(prev)
	}

	out, err := e.f.client.StartTaskContact(t.Context(), in)
	require.NoError(t, err)

	return aws.ToString(out.ContactId)
}

func TestListAssociatedContacts(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)
	root := e.task(t)
	e.f.clock.advance(time.Second)
	child := e.scheduledTask(t, root, e.f.clock.now().Add(time.Hour))
	other := e.task(t)

	out, err := e.f.client.ListAssociatedContacts(t.Context(),
		&sdk.ListAssociatedContactsInput{InstanceId: inst, ContactId: aws.String(child)})
	require.NoError(t, err)
	require.Len(t, out.ContactSummaryList, 2)
	assert.Equal(t, root, aws.ToString(out.ContactSummaryList[0].ContactId))
	assert.Equal(t, child, aws.ToString(out.ContactSummaryList[1].ContactId))
	assert.Equal(t, root, aws.ToString(out.ContactSummaryList[1].PreviousContactId))
	assert.Equal(t, root, aws.ToString(out.ContactSummaryList[1].InitialContactId))
	assert.Equal(t, types.ChannelTask, out.ContactSummaryList[1].Channel)

	single, err := e.f.client.ListAssociatedContacts(t.Context(),
		&sdk.ListAssociatedContactsInput{InstanceId: inst, ContactId: aws.String(other)})
	require.NoError(t, err)
	assert.Len(t, single.ContactSummaryList, 1)

	_, err = e.f.client.ListAssociatedContacts(t.Context(),
		&sdk.ListAssociatedContactsInput{InstanceId: inst, ContactId: aws.String("missing")})
	assert.Equal(t, "ContactNotFoundException", apiCode(err))
}

func TestUpdateContactSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		at       func(now time.Time) time.Time
		name     string
		wantCode string
		plain    bool
	}{
		{name: "ok", at: func(n time.Time) time.Time { return n.Add(2 * time.Hour) }},
		{
			name:     "past",
			at:       func(n time.Time) time.Time { return n.Add(-time.Hour) },
			wantCode: "InvalidParameterException",
		},
		{
			name: "too far", at: func(n time.Time) time.Time { return n.Add(7 * 24 * time.Hour) },
			wantCode: "InvalidParameterException",
		},
		{
			name: "unscheduled", at: func(n time.Time) time.Time { return n.Add(time.Hour) },
			wantCode: "InvalidRequestException", plain: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			now := e.f.clock.now()
			id := e.scheduledTask(t, "", now.Add(time.Hour))

			if tt.plain {
				id = e.task(t)
			}

			_, err := e.f.client.UpdateContactSchedule(t.Context(), &sdk.UpdateContactScheduleInput{
				InstanceId: aws.String(e.inst), ContactId: aws.String(id), ScheduledTime: aws.Time(tt.at(now)),
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.True(t, e.describe(t, id).ScheduledTimestamp.Equal(tt.at(now)))
		})
	}
}

func TestDeleteContactData(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	e.emailAddress(t)

	mail, err := e.f.client.StartEmailContact(t.Context(), &sdk.StartEmailContactInput{
		InstanceId: aws.String(e.inst), DestinationEmailAddress: aws.String("support@example.com"),
		FromEmailAddress: &types.EmailAddressInfo{EmailAddress: aws.String("cust@example.org")},
		EmailMessage:     raw("help", "please"), Name: aws.String("mail"),
		AdditionalRecipients: &types.InboundAdditionalRecipients{
			CcAddresses: []types.EmailAddressInfo{{EmailAddress: aws.String("cc@example.org")}},
		},
	})
	require.NoError(t, err)

	id := aws.ToString(mail.ContactId)
	del := func(fields ...types.ContactField) error {
		_, derr := e.f.client.DeleteContactData(t.Context(), &sdk.DeleteContactDataInput{
			InstanceId: aws.String(e.inst), ContactId: aws.String(id), ContactFields: fields,
		})

		return derr
	}

	assert.Equal(t, "InvalidRequestException", apiCode(del(types.ContactFieldCustomerEndpoint)), "still active")

	e.stop(t, id)
	require.NoError(t, del(types.ContactFieldCustomerEndpoint, types.ContactFieldAdditionalEmailRecipients,
		types.ContactFieldEmailSubject))

	c := e.describe(t, id)
	assert.Nil(t, c.CustomerEndpoint)
	assert.Nil(t, c.AdditionalEmailRecipients)

	task := e.task(t)
	e.stop(t, task)

	_, err = e.f.client.DeleteContactData(t.Context(), &sdk.DeleteContactDataInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(task),
		ContactFields: []types.ContactField{types.ContactFieldEmailSubject},
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err), "email-only field on a task")
}

func TestListContactReferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		types    []types.ReferenceType
		want     []string
	}{
		{name: "url", types: []types.ReferenceType{types.ReferenceTypeUrl}, want: []string{"docs"}},
		{
			name: "several",
			types: []types.ReferenceType{
				types.ReferenceTypeUrl,
				types.ReferenceTypeNumber,
				types.ReferenceTypeEmailMessage,
			},
			want: []string{"docs", "mail", "retries"},
		},
		{name: "none match", types: []types.ReferenceType{types.ReferenceTypeDate}},
		{name: "bad type", types: []types.ReferenceType{"BOGUS"}, wantCode: "InvalidParameterException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			task, err := e.f.client.StartTaskContact(t.Context(), &sdk.StartTaskContactInput{
				InstanceId: aws.String(e.inst), ContactFlowId: aws.String(e.flow), Name: aws.String("refs"),
				References: map[string]types.Reference{
					"docs":    {Type: types.ReferenceTypeUrl, Value: aws.String("https://example.com")},
					"retries": {Type: types.ReferenceTypeNumber, Value: aws.String("3")},
					"mail": {
						Type: types.ReferenceTypeEmailMessage,
						Arn:  aws.String("arn:aws:connect:us-east-1:123456789012:instance/i/contact/c/email/1"),
					},
				},
			})
			require.NoError(t, err)

			out, err := e.f.client.ListContactReferences(t.Context(), &sdk.ListContactReferencesInput{
				InstanceId: aws.String(e.inst), ContactId: task.ContactId, ReferenceTypes: tt.types,
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)

			var got []string

			for _, r := range out.ReferenceSummaryList {
				switch v := r.(type) {
				case *types.ReferenceSummaryMemberUrl:
					got = append(got, aws.ToString(v.Value.Name))
					assert.Equal(t, "https://example.com", aws.ToString(v.Value.Value))
				case *types.ReferenceSummaryMemberNumber:
					got = append(got, aws.ToString(v.Value.Name))
				case *types.ReferenceSummaryMemberEmailMessage:
					got = append(got, aws.ToString(v.Value.Name))
					assert.Contains(t, aws.ToString(v.Value.Arn), "/email/1")
				}
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

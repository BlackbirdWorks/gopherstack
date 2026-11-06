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

func (e *contactEnv) userARN(t *testing.T, id string) string {
	t.Helper()

	out, err := e.f.client.DescribeUser(t.Context(), &sdk.DescribeUserInput{InstanceId: aws.String(e.inst), UserId: aws.String(id)})
	require.NoError(t, err)

	return aws.ToString(out.User.Arn)
}

func (e *contactEnv) notify(t *testing.T, mut func(*sdk.CreateNotificationInput)) (*sdk.CreateNotificationOutput, error) {
	t.Helper()

	in := &sdk.CreateNotificationInput{
		InstanceId: aws.String(e.inst), Content: map[string]string{"en-US": "Maintenance tonight"},
		Recipients: []string{e.userARN(t, e.user)}, Priority: types.ConfigurableNotificationPriorityHigh,
		Tags: map[string]string{"team": "ops"},
	}
	mut(in)

	return e.f.client.CreateNotification(t.Context(), in)
}

func TestNotificationLifecycle(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)

	created, err := e.notify(t, func(*sdk.CreateNotificationInput) {})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(created.NotificationArn), "/notification/"+aws.ToString(created.NotificationId))

	got, err := e.f.client.DescribeNotification(t.Context(),
		&sdk.DescribeNotificationInput{InstanceId: inst, NotificationId: created.NotificationId})
	require.NoError(t, err)
	n := got.Notification
	assert.Equal(t, "Maintenance tonight", n.Content["en-US"])
	assert.Equal(t, types.NotificationPriorityHigh, n.Priority)
	assert.Equal(t, map[string]string{"team": "ops"}, n.Tags)
	assert.Equal(t, e.f.clock.now().Add(7*24*time.Hour).Unix(), n.ExpiresAt.Unix(), "expires after a week by default")
	assert.Equal(t, "us-east-1", aws.ToString(n.LastModifiedRegion))

	e.f.clock.advance(time.Minute)

	_, err = e.f.client.UpdateNotificationContent(t.Context(), &sdk.UpdateNotificationContentInput{
		InstanceId: inst, NotificationId: created.NotificationId, Content: map[string]string{"en-US": "Maintenance moved", "de-DE": "Wartung"},
	})
	require.NoError(t, err)

	got, err = e.f.client.DescribeNotification(t.Context(),
		&sdk.DescribeNotificationInput{InstanceId: inst, NotificationId: created.NotificationId})
	require.NoError(t, err)
	assert.Len(t, got.Notification.Content, 2)
	assert.True(t, got.Notification.LastModifiedTime.After(*got.Notification.CreatedAt))

	second, err := e.notify(t, func(in *sdk.CreateNotificationInput) { in.Priority = "" })
	require.NoError(t, err)

	list, err := e.f.client.ListNotifications(t.Context(), &sdk.ListNotificationsInput{InstanceId: inst, MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, list.NotificationSummaryList, 1)
	require.NotNil(t, list.NextToken)
	assert.Equal(t, aws.ToString(second.NotificationId), aws.ToString(list.NotificationSummaryList[0].Id), "newest first")

	_, err = e.f.client.DeleteNotification(t.Context(), &sdk.DeleteNotificationInput{InstanceId: inst, NotificationId: created.NotificationId})
	require.NoError(t, err)

	_, err = e.f.client.DescribeNotification(t.Context(),
		&sdk.DescribeNotificationInput{InstanceId: inst, NotificationId: created.NotificationId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = e.f.client.DeleteNotification(t.Context(), &sdk.DeleteNotificationInput{InstanceId: inst, NotificationId: created.NotificationId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestCreateNotificationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut  func(e *contactEnv, in *sdk.CreateNotificationInput)
		name string
		code string
	}{
		{name: "no content", code: "InvalidParameterException", mut: func(_ *contactEnv, in *sdk.CreateNotificationInput) { in.Content = map[string]string{} }},
		{name: "long content", code: "InvalidParameterException", mut: func(_ *contactEnv, in *sdk.CreateNotificationInput) {
			in.Content = map[string]string{"en-US": string(make([]byte, 251))}
		}},
		{name: "blank content", code: "InvalidParameterException", mut: func(_ *contactEnv, in *sdk.CreateNotificationInput) {
			in.Content = map[string]string{"en-US": " "}
		}},
		{name: "no recipients", code: "InvalidParameterException", mut: func(_ *contactEnv, in *sdk.CreateNotificationInput) { in.Recipients = []string{} }},
		{name: "foreign recipient", code: "InvalidParameterException", mut: func(_ *contactEnv, in *sdk.CreateNotificationInput) {
			in.Recipients = []string{"arn:aws:connect:us-east-1:123456789012:instance/other/agent/x"}
		}},
		{name: "unknown user", code: "ResourceNotFoundException", mut: func(e *contactEnv, in *sdk.CreateNotificationInput) {
			in.Recipients = []string{e.instARN() + "/agent/nobody"}
		}},
		{name: "urgent is not configurable", code: "InvalidParameterException", mut: func(_ *contactEnv, in *sdk.CreateNotificationInput) {
			in.Priority = "URGENT"
		}},
		{name: "expired", code: "InvalidParameterException", mut: func(e *contactEnv, in *sdk.CreateNotificationInput) {
			in.ExpiresAt = aws.Time(e.f.clock.now().Add(-time.Hour))
		}},
		{name: "unknown instance", code: "ResourceNotFoundException", mut: func(_ *contactEnv, in *sdk.CreateNotificationInput) {
			in.InstanceId = aws.String("missing")
		}},
		{name: "instance recipient ok", mut: func(e *contactEnv, in *sdk.CreateNotificationInput) { in.Recipients = []string{e.instARN()} }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			_, err := e.notify(t, func(in *sdk.CreateNotificationInput) { tt.mut(e, in) })
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestUserNotifications(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)

	direct, err := e.notify(t, func(*sdk.CreateNotificationInput) {})
	require.NoError(t, err)

	broadcast, err := e.notify(t, func(in *sdk.CreateNotificationInput) {
		in.Recipients = []string{e.instARN()}
		in.ExpiresAt = aws.Time(e.f.clock.now().Add(time.Hour))
	})
	require.NoError(t, err)

	list := func(user string) []types.UserNotificationSummary {
		out, lerr := e.f.client.ListUserNotifications(t.Context(), &sdk.ListUserNotificationsInput{InstanceId: inst, UserId: aws.String(user)})
		require.NoError(t, lerr)

		return out.UserNotifications
	}

	mine := list(e.user)
	require.Len(t, mine, 2)

	for _, n := range mine {
		assert.Equal(t, types.NotificationStatusUnread, n.NotificationStatus)
		assert.Equal(t, types.NotificationSourceCustomer, n.Source)
		assert.Equal(t, e.user, aws.ToString(n.RecipientId))
	}

	_, err = e.f.client.UpdateUserNotificationStatus(t.Context(), &sdk.UpdateUserNotificationStatusInput{
		InstanceId: inst, UserId: aws.String(e.user), NotificationId: direct.NotificationId, Status: types.NotificationStatusRead,
	})
	require.NoError(t, err)

	statuses := map[string]types.NotificationStatus{}
	for _, n := range list(e.user) {
		statuses[aws.ToString(n.NotificationId)] = n.NotificationStatus
	}

	assert.Equal(t, types.NotificationStatusRead, statuses[aws.ToString(direct.NotificationId)])
	assert.Equal(t, types.NotificationStatusUnread, statuses[aws.ToString(broadcast.NotificationId)])

	e.f.clock.advance(2 * time.Hour)
	assert.Len(t, list(e.user), 1, "the hour-long broadcast expired")

	_, err = e.f.client.UpdateUserNotificationStatus(t.Context(), &sdk.UpdateUserNotificationStatusInput{
		InstanceId: inst, UserId: aws.String(e.user), NotificationId: direct.NotificationId, Status: "DELETED",
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = e.f.client.UpdateUserNotificationStatus(t.Context(), &sdk.UpdateUserNotificationStatusInput{
		InstanceId: inst, UserId: aws.String("nobody"), NotificationId: direct.NotificationId, Status: types.NotificationStatusHidden,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = e.f.client.ListUserNotifications(t.Context(), &sdk.ListUserNotificationsInput{InstanceId: inst, UserId: aws.String("nobody")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestUserNotificationNeedsRecipient(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	_, _, profile := e.f.defaultQueue(t, e.inst)
	other := e.f.newUser(t, e.inst, "agent2", profile)

	n, err := e.notify(t, func(*sdk.CreateNotificationInput) {})
	require.NoError(t, err)

	_, err = e.f.client.UpdateUserNotificationStatus(t.Context(), &sdk.UpdateUserNotificationStatusInput{
		InstanceId: aws.String(e.inst), UserId: other.UserId, NotificationId: n.NotificationId, Status: types.NotificationStatusRead,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "agent2 is not a recipient")

	out, err := e.f.client.ListUserNotifications(t.Context(), &sdk.ListUserNotificationsInput{InstanceId: aws.String(e.inst), UserId: other.UserId})
	require.NoError(t, err)
	assert.Empty(t, out.UserNotifications)
}

func TestSearchNotifications(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	first, err := e.notify(t, func(*sdk.CreateNotificationInput) {})
	require.NoError(t, err)

	_, err = e.notify(t, func(in *sdk.CreateNotificationInput) {
		in.Content = map[string]string{"en-US": "New release"}
		in.Tags = map[string]string{"team": "dev"}
	})
	require.NoError(t, err)

	search := func(in *sdk.SearchNotificationsInput) *sdk.SearchNotificationsOutput {
		in.InstanceId = aws.String(e.inst)
		out, serr := e.f.client.SearchNotifications(t.Context(), in)
		require.NoError(t, serr)

		return out
	}

	byContent := search(&sdk.SearchNotificationsInput{SearchCriteria: &types.NotificationSearchCriteria{
		StringCondition: &types.StringCondition{FieldName: aws.String("MessageContent"), Value: aws.String("maintenance"), ComparisonType: types.StringComparisonTypeContains},
	}})
	require.Len(t, byContent.Notifications, 1)
	assert.Equal(t, aws.ToString(first.NotificationId), aws.ToString(byContent.Notifications[0].Id))
	assert.Equal(t, e.inst, aws.ToString(byContent.Notifications[0].InstanceId))
	assert.EqualValues(t, 1, aws.ToInt64(byContent.ApproximateTotalCount))

	byID := search(&sdk.SearchNotificationsInput{SearchCriteria: &types.NotificationSearchCriteria{
		StringCondition: &types.StringCondition{FieldName: aws.String("NotificationId"), Value: first.NotificationId, ComparisonType: types.StringComparisonTypeExact},
	}})
	require.Len(t, byID.Notifications, 1)

	byTag := search(&sdk.SearchNotificationsInput{SearchFilter: &types.NotificationSearchFilter{AttributeFilter: &types.ControlPlaneAttributeFilter{
		TagCondition: &types.TagCondition{TagKey: aws.String("team"), TagValue: aws.String("dev")},
	}}})
	require.Len(t, byTag.Notifications, 1)
	assert.Equal(t, "New release", byTag.Notifications[0].Content["en-US"])

	assert.Len(t, search(&sdk.SearchNotificationsInput{}).Notifications, 2)
	assert.Len(t, search(&sdk.SearchNotificationsInput{MaxResults: aws.Int32(1)}).Notifications, 1)

	_, err = e.f.client.SearchNotifications(t.Context(), &sdk.SearchNotificationsInput{
		InstanceId: aws.String(e.inst), SearchCriteria: &types.NotificationSearchCriteria{
			StringCondition: &types.StringCondition{FieldName: aws.String("bogus"), Value: aws.String("x"), ComparisonType: types.StringComparisonTypeExact},
		},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}

func TestPushNotificationRegistration(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	chat := e.chat(t)
	pinpoint := "arn:aws:mobiletargeting:us-east-1:123456789012:apps/app1"

	create := func(mut func(*sdk.CreatePushNotificationRegistrationInput)) (*sdk.CreatePushNotificationRegistrationOutput, error) {
		in := &sdk.CreatePushNotificationRegistrationInput{
			InstanceId: aws.String(e.inst), PinpointAppArn: aws.String(pinpoint), DeviceToken: aws.String("tok"),
			DeviceType: types.DeviceTypeGcm, ContactConfiguration: &types.ContactConfiguration{
				ContactId: aws.String(chat), ParticipantRole: types.ParticipantRoleCustomer,
			},
		}
		mut(in)

		return e.f.client.CreatePushNotificationRegistration(t.Context(), in)
	}

	out, err := create(func(*sdk.CreatePushNotificationRegistrationInput) {})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(out.RegistrationId))

	del := func(contact string) error {
		_, derr := e.f.client.DeletePushNotificationRegistration(t.Context(), &sdk.DeletePushNotificationRegistrationInput{
			InstanceId: aws.String(e.inst), RegistrationId: out.RegistrationId, ContactId: aws.String(contact),
		})

		return derr
	}

	assert.Equal(t, "ResourceNotFoundException", apiCode(del(e.chat(t))), "registration belongs to another contact")
	require.NoError(t, del(chat))
	assert.Equal(t, "ResourceNotFoundException", apiCode(del(chat)))

	for name, mut := range map[string]func(*sdk.CreatePushNotificationRegistrationInput){
		"agent role": func(in *sdk.CreatePushNotificationRegistrationInput) {
			in.ContactConfiguration.ParticipantRole = types.ParticipantRoleAgent
		},
		"device type": func(in *sdk.CreatePushNotificationRegistrationInput) { in.DeviceType = "SMS" },
		"pinpoint arn": func(in *sdk.CreatePushNotificationRegistrationInput) {
			in.PinpointAppArn = aws.String("arn:aws:sns:us-east-1:123456789012:topic")
		},
		"task contact": func(in *sdk.CreatePushNotificationRegistrationInput) {
			in.ContactConfiguration.ContactId = aws.String(e.task(t))
		},
		"unknown contact": func(in *sdk.CreatePushNotificationRegistrationInput) {
			in.ContactConfiguration.ContactId = aws.String("nope")
		},
	} {
		_, err = create(mut)
		assert.Contains(t, []string{"InvalidParameterException", "InvalidRequestException", "ResourceNotFoundException"}, apiCode(err), name)
		assert.NotEmpty(t, apiCode(err), name)
	}
}

func TestNotificationsSurviveRestore(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	created, err := e.notify(t, func(*sdk.CreateNotificationInput) {})
	require.NoError(t, err)

	_, err = e.f.client.UpdateUserNotificationStatus(t.Context(), &sdk.UpdateUserNotificationStatusInput{
		InstanceId: aws.String(e.inst), UserId: aws.String(e.user), NotificationId: created.NotificationId, Status: types.NotificationStatusHidden,
	})
	require.NoError(t, err)

	r := e.f.restored(t)
	out, err := r.client.ListUserNotifications(t.Context(), &sdk.ListUserNotificationsInput{InstanceId: aws.String(e.inst), UserId: aws.String(e.user)})
	require.NoError(t, err)
	require.Len(t, out.UserNotifications, 1)
	assert.Equal(t, types.NotificationStatusHidden, out.UserNotifications[0].NotificationStatus)

	tags, err := r.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: created.NotificationArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "ops"}, tags.Tags)
}

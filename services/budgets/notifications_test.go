package budgets_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bsdk "github.com/aws/aws-sdk-go-v2/service/budgets"
	"github.com/aws/aws-sdk-go-v2/service/budgets/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const topicARN = "arn:aws:sns:us-east-1:123456789012:alerts"

func pctNotification(threshold float64) *types.Notification {
	return &types.Notification{
		NotificationType: types.NotificationTypeActual, ComparisonOperator: types.ComparisonOperatorGreaterThan,
		Threshold: threshold, ThresholdType: types.ThresholdTypePercentage,
	}
}

func email(addr string) types.Subscriber {
	return types.Subscriber{Address: aws.String(addr), SubscriptionType: types.SubscriptionTypeEmail}
}

func TestNotificationAndSubscriberCRUD(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	acct, name := aws.String(testAccount), aws.String("n1")
	createBudget(t, f, "n1", "100")

	_, err := f.client.CreateNotification(ctx, &bsdk.CreateNotificationInput{
		AccountId: acct, BudgetName: name, Notification: pctNotification(80),
		Subscribers: []types.Subscriber{email("a@example.com")},
	})
	require.NoError(t, err)

	_, err = f.client.CreateNotification(ctx, &bsdk.CreateNotificationInput{
		AccountId: acct, BudgetName: name, Notification: pctNotification(80),
		Subscribers: []types.Subscriber{email("a@example.com")},
	})
	assert.Equal(t, "DuplicateRecordException", apiCode(err))

	_, err = f.client.CreateSubscriber(ctx, &bsdk.CreateSubscriberInput{
		AccountId: acct, BudgetName: name, Notification: pctNotification(80),
		Subscriber: &types.Subscriber{Address: aws.String(topicARN), SubscriptionType: types.SubscriptionTypeSns},
	})
	require.NoError(t, err)

	_, err = f.client.CreateSubscriber(ctx, &bsdk.CreateSubscriberInput{
		AccountId: acct, BudgetName: name, Notification: pctNotification(80),
		Subscriber: &types.Subscriber{Address: aws.String("not-a-topic"), SubscriptionType: types.SubscriptionTypeSns},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	subs, err := f.client.DescribeSubscribersForNotification(ctx, &bsdk.DescribeSubscribersForNotificationInput{
		AccountId: acct, BudgetName: name, Notification: pctNotification(80),
	})
	require.NoError(t, err)
	assert.Len(t, subs.Subscribers, 2)

	newSub := email("b@example.com")
	_, err = f.client.UpdateSubscriber(ctx, &bsdk.UpdateSubscriberInput{
		AccountId:    acct,
		BudgetName:   name,
		Notification: pctNotification(80),
		OldSubscriber: &types.Subscriber{
			Address:          aws.String("a@example.com"),
			SubscriptionType: types.SubscriptionTypeEmail,
		},
		NewSubscriber: &newSub,
	})
	require.NoError(t, err)

	for _, addr := range []string{"b@example.com", topicARN} {
		st := types.SubscriptionTypeEmail
		if addr == topicARN {
			st = types.SubscriptionTypeSns
		}

		_, err = f.client.DeleteSubscriber(ctx, &bsdk.DeleteSubscriberInput{
			AccountId: acct, BudgetName: name, Notification: pctNotification(80),
			Subscriber: &types.Subscriber{Address: aws.String(addr), SubscriptionType: st},
		})

		if addr == topicARN {
			assert.Equal(t, "InvalidParameterException", apiCode(err), "last subscriber is kept")
		} else {
			require.NoError(t, err)
		}
	}

	_, err = f.client.UpdateNotification(ctx, &bsdk.UpdateNotificationInput{
		AccountId: acct, BudgetName: name, OldNotification: pctNotification(80), NewNotification: pctNotification(90),
	})
	require.NoError(t, err)

	got, err := f.client.DescribeNotificationsForBudget(ctx, &bsdk.DescribeNotificationsForBudgetInput{
		AccountId: acct, BudgetName: name,
	})
	require.NoError(t, err)
	require.Len(t, got.Notifications, 1)
	assert.InDelta(t, 90, got.Notifications[0].Threshold, 0)

	all, err := f.client.DescribeBudgetNotificationsForAccount(
		ctx,
		&bsdk.DescribeBudgetNotificationsForAccountInput{AccountId: acct},
	)
	require.NoError(t, err)
	assert.Len(t, all.BudgetNotificationsForAccount, 1)

	_, err = f.client.DeleteNotification(ctx, &bsdk.DeleteNotificationInput{
		AccountId: acct, BudgetName: name, Notification: pctNotification(90),
	})
	require.NoError(t, err)

	_, err = f.client.DeleteNotification(ctx, &bsdk.DeleteNotificationInput{
		AccountId: acct, BudgetName: name, Notification: pctNotification(90),
	})
	assert.Equal(t, "NotFoundException", apiCode(err))
}

func TestNotificationLimit(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	createBudget(t, f, "lim", "100")

	var err error

	for i := range 6 {
		_, err = f.client.CreateNotification(t.Context(), &bsdk.CreateNotificationInput{
			AccountId: aws.String(testAccount), BudgetName: aws.String("lim"),
			Notification: pctNotification(float64(10 + i)), Subscribers: []types.Subscriber{email("a@example.com")},
		})
	}

	assert.Equal(t, "CreationLimitExceededException", apiCode(err))
}

func TestNotificationFiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantState types.NotificationState
		spend     float64
		threshold float64
		wantSent  int
	}{
		{name: "below", spend: 50, threshold: 80, wantSent: 0, wantState: types.NotificationStateOk},
		{name: "crossed", spend: 90, threshold: 80, wantSent: 1, wantState: types.NotificationStateAlarm},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.spend.set(tc.spend)

			_, err := f.client.CreateBudget(t.Context(), &bsdk.CreateBudgetInput{
				AccountId: aws.String(testAccount), Budget: costBudget("fire", "100"),
				NotificationsWithSubscribers: []types.NotificationWithSubscribers{{
					Notification: pctNotification(tc.threshold),
					Subscribers: []types.Subscriber{
						{Address: aws.String(topicARN), SubscriptionType: types.SubscriptionTypeSns},
					},
				}},
			})
			require.NoError(t, err)

			got, err := f.client.DescribeNotificationsForBudget(t.Context(), &bsdk.DescribeNotificationsForBudgetInput{
				AccountId: aws.String(testAccount), BudgetName: aws.String("fire"),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantState, got.Notifications[0].NotificationState)
			assert.Equal(t, tc.wantSent, f.notifier.count())

			f.backend.EvaluateAll(t.Context())
			assert.Equal(t, tc.wantSent, f.notifier.count(), "no re-fire without a state change")
		})
	}
}

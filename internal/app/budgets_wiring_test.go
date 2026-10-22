package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	budgetsbackend "github.com/blackbirdworks/gopherstack/services/budgets"
	cebackend "github.com/blackbirdworks/gopherstack/services/ce"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

func TestBudgetsExecutorIAM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantLen  int
		reverse  bool
		wantFail bool
	}{
		{name: "apply", wantLen: 1},
		{name: "reverse", reverse: true, wantLen: 0},
		{name: "missing role", wantFail: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := iambackend.NewInMemoryBackend()
			pol, err := b.CreatePolicy(
				"deny-all",
				"/",
				`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
			)
			require.NoError(t, err)

			role := "worker"
			if tc.wantFail {
				role = "ghost"
			} else {
				_, err = b.CreateRole(role, "/", `{"Version":"2012-10-17","Statement":[]}`, "")
				require.NoError(t, err)

				if tc.reverse {
					require.NoError(t, b.AttachRolePolicy(role, pol.Arn))
				}
			}

			x := &budgetsExecutor{iam: iambackend.NewHandler(b)}
			err = x.Run(t.Context(), "123456789012", budgetsbackend.Action{
				ActionType:       "APPLY_IAM_POLICY",
				ExecutionRoleArn: "arn:aws:iam::123456789012:role/" + role,
				Definition: &budgetsbackend.Definition{IamActionDefinition: &budgetsbackend.IamActionDefinition{
					PolicyArn: pol.Arn, Roles: []string{role},
				}},
			}, tc.reverse)

			if tc.wantFail {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			attached, err := b.ListAttachedRolePolicies(role)
			require.NoError(t, err)
			assert.Len(t, attached, tc.wantLen)
		})
	}
}

func TestBudgetsExecutorWithoutBackend(t *testing.T) {
	t.Parallel()

	for _, typ := range []string{"APPLY_IAM_POLICY", "APPLY_SCP_POLICY", "RUN_SSM_DOCUMENTS", "BOGUS"} {
		t.Run(typ, func(t *testing.T) {
			t.Parallel()

			err := (&budgetsExecutor{}).Run(t.Context(), "123456789012", budgetsbackend.Action{
				ActionType: typ,
				Definition: &budgetsbackend.Definition{
					IamActionDefinition: &budgetsbackend.IamActionDefinition{},
					ScpActionDefinition: &budgetsbackend.ScpActionDefinition{},
					SsmActionDefinition: &budgetsbackend.SsmActionDefinition{},
				},
			}, false)
			require.Error(t, err)
		})
	}
}

func TestBudgetsNotifierSNS(t *testing.T) {
	t.Parallel()

	sns := snsbackend.NewInMemoryBackend()
	topic, err := sns.CreateTopic("alerts", nil)
	require.NoError(t, err)

	n := &budgetsNotifier{sns: sns}
	err = n.Notify(t.Context(), budgetsbackend.Delivery{
		Subject: "s", Message: "m",
		Subscribers: []budgetsbackend.Subscriber{{Address: topic.TopicArn, SubscriptionType: "SNS"}},
	})
	require.NoError(t, err)

	err = n.Notify(t.Context(), budgetsbackend.Delivery{
		Subscribers: []budgetsbackend.Subscriber{{Address: topic.TopicArn + "x", SubscriptionType: "SNS"}},
	})
	require.Error(t, err)

	err = n.Notify(t.Context(), budgetsbackend.Delivery{
		Subscribers: []budgetsbackend.Subscriber{{Address: "a@example.com", SubscriptionType: "EMAIL"}},
	})
	require.Error(t, err, "no SES backend wired")
}

func TestBudgetsCESpend(t *testing.T) {
	t.Parallel()

	ce := cebackend.NewInMemoryBackend("123456789012", "us-east-1")
	now := time.Now().UTC()

	got, ok := budgetsCESpend{ce: ce}.Spend(now.AddDate(0, 0, -5), now, nil, "UnblendedCost")
	assert.True(t, ok)
	assert.Positive(t, got)
}

func TestBudgetsExecutorChecksExecutionRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleArn string
		wantErr bool
	}{
		{name: "exists", roleArn: "arn:aws:iam::123456789012:role/exec"},
		{name: "pathed", roleArn: "arn:aws:iam::123456789012:role/svc/exec"},
		{name: "missing", roleArn: "arn:aws:iam::123456789012:role/ghost", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := iambackend.NewInMemoryBackend()
			_, err := b.CreateRole("exec", "/", `{"Version":"2012-10-17","Statement":[]}`, "")
			require.NoError(t, err)

			x := &budgetsExecutor{iam: iambackend.NewHandler(b)}
			err = x.Run(t.Context(), "123456789012", budgetsbackend.Action{
				ActionType:       "APPLY_IAM_POLICY",
				ExecutionRoleArn: tc.roleArn,
				Definition: &budgetsbackend.Definition{
					IamActionDefinition: &budgetsbackend.IamActionDefinition{},
				},
			}, false)

			if tc.wantErr {
				require.ErrorIs(t, err, errBudgetsRole)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestBudgetsNotifierSNSRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home", region: "us-east-1"},
		{name: "other", region: "eu-west-1"},
		{name: "asia", region: "ap-southeast-2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sns := snsbackend.NewInMemoryBackend()
			topic, err := sns.CreateTopicInRegion("alerts", tc.region, nil)
			require.NoError(t, err)
			assert.Contains(t, topic.TopicArn, ":"+tc.region+":")

			n := &budgetsNotifier{sns: sns}
			require.NoError(t, n.Notify(t.Context(), budgetsbackend.Delivery{
				Subject: "s", Message: "m",
				Subscribers: []budgetsbackend.Subscriber{{Address: topic.TopicArn, SubscriptionType: "SNS"}},
			}))
		})
	}

	n := &budgetsNotifier{sns: snsbackend.NewInMemoryBackend()}
	err := n.Notify(t.Context(), budgetsbackend.Delivery{
		Subscribers: []budgetsbackend.Subscriber{{Address: "not-an-arn", SubscriptionType: "SNS"}},
	})
	require.ErrorIs(t, err, errBudgetsTopicARN)
}

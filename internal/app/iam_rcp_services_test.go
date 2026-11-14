package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const openQueuePolicy = `{"Version":"2012-10-17","Statement":` +
	`[{"Effect":"Allow","Principal":"*","Action":"sqs:*","Resource":"*"}]}`

func (f *scpFixture) sqs(t *testing.T, c scpCaller) *sqs.Client {
	t.Helper()

	return sqs.NewFromConfig(scpConfig(t, f.srv.URL, c.key, c.secret, "us-east-1", c.account))
}

func (f *scpFixture) queueFor(t *testing.T, owner scpCaller, name string) string {
	t.Helper()

	c := f.sqs(t, owner)
	out, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String(name)})
	require.NoError(t, err)

	_, err = c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl:   out.QueueUrl,
		Attributes: map[string]string{"Policy": openQueuePolicy},
	})
	require.NoError(t, err)

	return aws.ToString(out.QueueUrl)
}

func TestRCPEnforcementSQS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *scpFixture, member scpCaller)
		name string
	}{
		{
			name: "outsider_denied_peer_and_owner_allowed",
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				url := f.queueFor(t, member, "rcp-q")
				outsider := f.adminUser(t, outsideAccount, "outsider")
				send := func(c scpCaller) error {
					_, err := f.sqs(t, c).SendMessage(t.Context(), &sqs.SendMessageInput{
						QueueUrl: aws.String(url), MessageBody: aws.String("m"),
					})

					return err
				}

				if err := send(outsider); err != nil {
					assert.NotContains(t, err.Error(), "resource control policy", "no RCP attached yet")
				}

				f.enableRCP(t)
				f.attach(t, f.createRCP(t, `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*",`+
					`"Action":"sqs:*","Resource":"*","Condition":{"StringNotEqualsIfExists":`+
					`{"aws:PrincipalOrgID":"`+f.orgID(t)+`"}}}]}`), f.rootID)

				err := send(outsider)
				require.Error(t, err)
				assert.Contains(t, accessDeniedMessage(t, err), "explicit deny in a resource control policy")

				require.NoError(t, send(member))

				peer := f.adminUser(t, f.accountID, "peer")
				require.NoError(t, send(peer))
			},
		},
		{
			name: "management_owned_queue_exempt",
			run: func(t *testing.T, f *scpFixture, _ scpCaller) {
				t.Helper()

				mgmt := f.adminUser(t, "", "mgmt-admin")
				url := f.queueFor(t, mgmt, "mgmt-q")

				f.enableRCP(t)
				f.attach(t, f.createRCP(t, `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*",`+
					`"Action":"sqs:*","Resource":"*"}]}`), f.rootID)

				_, err := f.sqs(t, mgmt).SendMessage(t.Context(), &sqs.SendMessageInput{
					QueueUrl: aws.String(url), MessageBody: aws.String("m"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "member_create_queue_governed_by_member_rcp",
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				f.enableRCP(t)
				f.attach(t, f.createRCP(t, `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*",`+
					`"Action":"sqs:CreateQueue","Resource":"*"}]}`), f.ouID)

				_, err := f.sqs(t, member).CreateQueue(t.Context(),
					&sqs.CreateQueueInput{QueueName: aws.String("denied-q")})
				require.Error(t, err)
				assert.Contains(t, accessDeniedMessage(t, err), "resource control policy")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newSCPFixture(t, true)
			tt.run(t, f, f.adminUser(t, f.accountID, "member-admin"))
		})
	}
}

func TestRCPEnforcementSTS(t *testing.T) {
	t.Parallel()

	f := newSCPFixture(t, true)
	member := f.adminUser(t, f.accountID, "member-admin")
	outsider := f.adminUser(t, outsideAccount, "outsider")

	const trust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},` +
		`"Action":"sts:AssumeRole"}]}`

	iamc := iam.NewFromConfig(scpConfig(t, f.srv.URL, member.key, member.secret, "us-east-1", member.account))
	role, err := iamc.CreateRole(t.Context(), &iam.CreateRoleInput{
		RoleName: aws.String("target"), AssumeRolePolicyDocument: aws.String(trust),
	})
	require.NoError(t, err)

	assume := func(c scpCaller) error {
		_, e := sts.NewFromConfig(scpConfig(t, f.srv.URL, c.key, c.secret, "us-east-1", c.account)).
			AssumeRole(t.Context(), &sts.AssumeRoleInput{
				RoleArn: role.Role.Arn, RoleSessionName: aws.String("sess"),
			})

		return e
	}

	require.NoError(t, assume(outsider), "trust policy allows any principal")

	f.enableRCP(t)
	f.attach(t, f.createRCP(t, `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*",`+
		`"Action":"sts:AssumeRole","Resource":"*","Condition":{"StringNotEqualsIfExists":`+
		`{"aws:PrincipalOrgID":"`+f.orgID(t)+`"}}}]}`), f.rootID)

	err = assume(outsider)
	require.Error(t, err)
	assert.Contains(t, accessDeniedMessage(t, err), "explicit deny in a resource control policy")

	require.NoError(t, assume(member))
}

func TestSimulatePrincipalPolicyOrganizations(t *testing.T) {
	t.Parallel()

	const denyDelete = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:DeleteBucket","Resource":"*"}]}`

	tests := []struct {
		account         string
		action          string
		wantDecision    string
		name            string
		scpAttached     bool
		wantDetail      bool
		wantAllowedByOr bool
	}{
		{name: "member_scp_denies", account: "member", action: "s3:DeleteBucket", scpAttached: true,
			wantDetail: true, wantAllowedByOr: false, wantDecision: "explicitDeny"},
		{name: "member_scp_allows_other_action", account: "member", action: "s3:GetObject", scpAttached: true,
			wantDetail: true, wantAllowedByOr: true, wantDecision: "allowed"},
		{name: "management_account_has_no_detail", account: "mgmt", action: "s3:DeleteBucket", scpAttached: true,
			wantDetail: false, wantDecision: "allowed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newSCPFixture(t, true)
			acct := f.accountID

			if tt.account == "mgmt" {
				acct = "000000000000"
			}

			callerAccount := acct
			if tt.account == "mgmt" {
				callerAccount = ""
			}

			caller := f.adminUser(t, callerAccount, "sim-user")
			if tt.scpAttached {
				f.attach(t, f.createSCP(t, "deny-delete", denyDelete), f.ouID)
			}

			c := iam.NewFromConfig(scpConfig(t, f.srv.URL, caller.key, caller.secret, "us-east-1", caller.account))
			out, err := c.SimulatePrincipalPolicy(t.Context(), &iam.SimulatePrincipalPolicyInput{
				PolicySourceArn: aws.String("arn:aws:iam::" + acct + ":user/sim-user"),
				ActionNames:     []string{tt.action},
			})
			require.NoError(t, err)
			require.Len(t, out.EvaluationResults, 1)

			res := out.EvaluationResults[0]
			assert.Equal(t, tt.wantDecision, string(res.EvalDecision))

			if !tt.wantDetail {
				assert.Nil(t, res.OrganizationsDecisionDetail)

				return
			}

			require.NotNil(t, res.OrganizationsDecisionDetail)
			assert.Equal(t, tt.wantAllowedByOr, res.OrganizationsDecisionDetail.AllowedByOrganizations)
		})
	}
}

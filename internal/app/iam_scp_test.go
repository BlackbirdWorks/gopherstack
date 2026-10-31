package app

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	orgtypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	scpFullAccessID = "p-FullAWSAccess"
	scpAdminPolicy  = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
)

type scpFixture struct {
	srv       *httptest.Server
	iamBk     *iambackend.InMemoryBackend
	rec       *iampolicystream.Recorder
	orgs      *organizations.Client
	rootID    string
	ouID      string
	accountID string
}

func newSCPServer(
	t *testing.T, enforceSCPs bool,
) (*httptest.Server, *iampolicystream.Recorder, *iambackend.InMemoryBackend) {
	t.Helper()

	rec := iampolicystream.New(200)
	log := buildLogger("")
	cli := CLI{
		AccountID: "000000000000", Region: "us-east-1", EnforceIAM: true, IAMEnforceSCPs: enforceSCPs,
		iamStream: rec,
	}
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	iamBk, _ := findIAMBackend(services).(*iambackend.InMemoryBackend)
	require.NotNil(t, iamBk)

	return srv, rec, iamBk
}

func scpConfig(t *testing.T, url, key, secret, region, account string) aws.Config {
	t.Helper()

	cfg := awsConfigFor(t, url, key, secret)
	cfg.Region = region
	cfg.HTTPClient = testclient.New(t)

	if account != "" {
		cfg.APIOptions = append(cfg.APIOptions, smithyhttp.AddHeaderValue(awsmeta.AccountHeader, account))
	}

	return cfg
}

func newSCPFixture(t *testing.T, enforceSCPs bool) *scpFixture {
	t.Helper()

	srv, rec, iamBk := newSCPServer(t, enforceSCPs)
	orgs := organizations.NewFromConfig(scpConfig(t, srv.URL, "test", "test", "us-east-1", ""))

	_, err := orgs.CreateOrganization(t.Context(), &organizations.CreateOrganizationInput{
		FeatureSet: orgtypes.OrganizationFeatureSetAll,
	})
	require.NoError(t, err)

	roots, err := orgs.ListRoots(t.Context(), &organizations.ListRootsInput{})
	require.NoError(t, err)
	require.Len(t, roots.Roots, 1)

	f := &scpFixture{srv: srv, iamBk: iamBk, rec: rec, orgs: orgs, rootID: aws.ToString(roots.Roots[0].Id)}

	ou, err := orgs.CreateOrganizationalUnit(t.Context(), &organizations.CreateOrganizationalUnitInput{
		ParentId: aws.String(f.rootID), Name: aws.String("workloads"),
	})
	require.NoError(t, err)

	f.ouID = aws.ToString(ou.OrganizationalUnit.Id)

	st, err := orgs.CreateAccount(t.Context(), &organizations.CreateAccountInput{
		AccountName: aws.String("member"), Email: aws.String("member@example.com"),
	})
	require.NoError(t, err)

	f.accountID = aws.ToString(st.CreateAccountStatus.AccountId)
	require.NotEmpty(t, f.accountID)

	_, err = orgs.MoveAccount(t.Context(), &organizations.MoveAccountInput{
		AccountId: aws.String(
			f.accountID,
		),
		SourceParentId:      aws.String(f.rootID),
		DestinationParentId: aws.String(f.ouID),
	})
	require.NoError(t, err)

	return f
}

type scpCaller struct{ key, secret, account string }

func (f *scpFixture) adminUser(t *testing.T, account, name string) scpCaller {
	t.Helper()

	bk := f.iamBk.ForAccount(account)

	_, err := bk.CreateUser(name, "/", "")
	require.NoError(t, err)
	require.NoError(t, bk.PutUserPolicy(name, "admin", scpAdminPolicy))

	k, err := bk.CreateAccessKey(name)
	require.NoError(t, err)

	return scpCaller{k.AccessKeyID, k.SecretAccessKey, account}
}

func (f *scpFixture) createSCP(t *testing.T, name, doc string) string {
	t.Helper()

	out, err := f.orgs.CreatePolicy(t.Context(), &organizations.CreatePolicyInput{
		Name: aws.String(name), Description: aws.String(name), Content: aws.String(doc),
		Type: orgtypes.PolicyTypeServiceControlPolicy,
	})
	require.NoError(t, err)

	return aws.ToString(out.Policy.PolicySummary.Id)
}

func (f *scpFixture) attach(t *testing.T, policyID, target string) {
	t.Helper()

	_, err := f.orgs.AttachPolicy(t.Context(), &organizations.AttachPolicyInput{
		PolicyId: aws.String(policyID), TargetId: aws.String(target),
	})
	require.NoError(t, err)
}

func (f *scpFixture) detach(t *testing.T, policyID, target string) {
	t.Helper()

	_, err := f.orgs.DetachPolicy(t.Context(), &organizations.DetachPolicyInput{
		PolicyId: aws.String(policyID), TargetId: aws.String(target),
	})
	require.NoError(t, err)
}

func (f *scpFixture) s3(t *testing.T, c scpCaller) *s3.Client {
	t.Helper()

	return s3.NewFromConfig(scpConfig(t, f.srv.URL, c.key, c.secret, "us-east-1", c.account),
		func(o *s3.Options) { o.UsePathStyle = true })
}

func accessDeniedMessage(t *testing.T, err error) string {
	t.Helper()

	var ae smithy.APIError
	require.ErrorAs(t, err, &ae)
	require.Contains(t, []string{"AccessDenied", "AccessDeniedException"}, ae.ErrorCode())

	return ae.ErrorMessage()
}

func TestSCPEnforcement(t *testing.T) {
	t.Parallel()

	const denyDelete = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:DeleteBucket","Resource":"*"}]}`

	tests := []struct {
		run  func(t *testing.T, f *scpFixture, member scpCaller)
		name string
		off  bool
	}{
		{
			name: "explicit_deny_blocks_admin_member",
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				pid := f.createSCP(t, "deny-delete", denyDelete)
				f.attach(t, pid, f.ouID)

				c := f.s3(t, member)
				_, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("scp-b")})
				require.NoError(t, err)

				_, err = c.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String("scp-b")})
				require.Error(t, err)

				msg := accessDeniedMessage(t, err)
				assert.Contains(t, msg, "is not authorized to perform: s3:DeleteBucket")
				assert.Contains(t, msg, "with an explicit deny in a service control policy: arn:aws:organizations::")
				assert.Contains(t, msg, pid)

				var denied *iampolicystream.Event

				for _, ev := range f.rec.Snapshot(func(iampolicystream.Event) bool { return true }) {
					if ev.Decision == iampolicystream.DecisionDeny {
						denied = &ev
					}
				}

				require.NotNil(t, denied)
				assert.Equal(t, iampolicystream.ReasonServiceControl, denied.Reason)
				assert.Equal(t, iampolicystream.DenyExplicit, denied.DenyType)
				require.NotNil(t, denied.Policy)
				assert.Equal(t, iampolicystream.PolicySCP, denied.Policy.Type)
				assert.Contains(t, denied.Policy.Arn, pid)
			},
		},
		{
			name: "management_account_exempt",
			run: func(t *testing.T, f *scpFixture, _ scpCaller) {
				t.Helper()

				pid := f.createSCP(t, "deny-delete", denyDelete)
				f.attach(t, pid, f.rootID)

				mgmt := f.adminUser(t, "", "mgmt-admin")
				c := f.s3(t, mgmt)
				_, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("mgmt-b")})
				require.NoError(t, err)

				_, err = c.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String("mgmt-b")})
				require.NoError(t, err)
			},
		},
		{
			name: "allow_list_without_full_access_denies_others",
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				allow := f.createSCP(t, "ec2-only", `{"Version":"2012-10-17","Statement":[`+
					`{"Effect":"Allow","Action":"ec2:*","Resource":"*"}]}`)
				f.attach(t, allow, f.ouID)
				f.detach(t, scpFullAccessID, f.ouID)

				_, err := f.s3(t, member).CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("x-b")})
				require.Error(t, err)
				assert.Contains(t, accessDeniedMessage(t, err),
					"because no service control policy allows the s3:CreateBucket action")
			},
		},
		{
			name: "allow_list_still_permits_listed_actions",
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				allow := f.createSCP(t, "s3-only", `{"Version":"2012-10-17","Statement":[`+
					`{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
				f.attach(t, allow, f.ouID)
				f.detach(t, scpFullAccessID, f.ouID)

				_, err := f.s3(t, member).CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("ok-b")})
				require.NoError(t, err)
			},
		},
		{
			name: "requested_region_condition",
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				pid := f.createSCP(t, "region-lock", `{"Version":"2012-10-17","Statement":[{"Effect":"Deny",`+
					`"Action":"sqs:*","Resource":"*","Condition":{"StringNotEquals":`+
					`{"aws:RequestedRegion":"us-east-1"}}}]}`)
				f.attach(t, pid, f.rootID)

				east := sqs.NewFromConfig(
					scpConfig(t, f.srv.URL, member.key, member.secret, "us-east-1", member.account),
				)
				_, err := east.ListQueues(t.Context(), &sqs.ListQueuesInput{})
				require.NoError(t, err)

				west := sqs.NewFromConfig(
					scpConfig(t, f.srv.URL, member.key, member.secret, "us-west-2", member.account),
				)
				_, err = west.ListQueues(t.Context(), &sqs.ListQueuesInput{})
				require.Error(t, err)
				assert.Contains(t, accessDeniedMessage(t, err), "explicit deny in a service control policy")
			},
		},
		{
			name: "policy_type_disabled_has_no_effect",
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				_, err := f.orgs.DisablePolicyType(t.Context(), &organizations.DisablePolicyTypeInput{
					RootId: aws.String(f.rootID), PolicyType: orgtypes.PolicyTypeServiceControlPolicy,
				})
				require.NoError(t, err)

				_, err = f.s3(t, member).CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("off-b")})
				require.NoError(t, err)
			},
		},
		{
			name: "flag_off_has_no_effect",
			off:  true,
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				pid := f.createSCP(t, "deny-delete", denyDelete)
				f.attach(t, pid, f.ouID)

				c := f.s3(t, member)
				_, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("flag-b")})
				require.NoError(t, err)

				_, err = c.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String("flag-b")})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newSCPFixture(t, !tt.off)
			tt.run(t, f, f.adminUser(t, f.accountID, "member-admin"))
		})
	}
}

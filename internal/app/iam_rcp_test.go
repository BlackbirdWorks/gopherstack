package app

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	orgtypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
)

const (
	rcpFullAccessID = "p-RCPFullAWSAccess"
	outsideAccount  = "222222222222"
)

func (f *scpFixture) enableRCP(t *testing.T) {
	t.Helper()

	_, err := f.orgs.EnablePolicyType(t.Context(), &organizations.EnablePolicyTypeInput{
		RootId: aws.String(f.rootID), PolicyType: orgtypes.PolicyTypeResourceControlPolicy,
	})
	require.NoError(t, err)
}

func (f *scpFixture) createRCP(t *testing.T, name, doc string) string {
	t.Helper()

	out, err := f.orgs.CreatePolicy(t.Context(), &organizations.CreatePolicyInput{
		Name: aws.String(name), Description: aws.String(name), Content: aws.String(doc),
		Type: orgtypes.PolicyTypeResourceControlPolicy,
	})
	require.NoError(t, err)

	return aws.ToString(out.Policy.PolicySummary.Id)
}

func (f *scpFixture) orgID(t *testing.T) string {
	t.Helper()

	out, err := f.orgs.DescribeOrganization(t.Context(), &organizations.DescribeOrganizationInput{})
	require.NoError(t, err)

	return aws.ToString(out.Organization.Id)
}

func orgPerimeterRCP(orgID string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:*","Resource":"*",` +
		`"Condition":{"StringNotEqualsIfExists":{"aws:PrincipalOrgID":"` + orgID + `"}}}]}`
}

func (f *scpFixture) bucketWithObject(t *testing.T, owner scpCaller, bucket string) {
	t.Helper()

	c := f.s3(t, owner)
	_, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	_, err = c.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), Body: strings.NewReader("v"),
	})
	require.NoError(t, err)
}

func TestRCPEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *scpFixture, member scpCaller)
		name string
	}{
		{
			name: "outside_account_denied_in_org_allowed",
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				f.bucketWithObject(t, member, "rcp-b")
				outsider := f.adminUser(t, outsideAccount, "outsider")
				peer := f.adminUser(t, f.accountID, "peer")

				_, err := f.s3(t, outsider).GetObject(t.Context(), &s3.GetObjectInput{
					Bucket: aws.String("rcp-b"), Key: aws.String("k"),
				})
				require.NoError(t, err, "no RCP attached yet")

				f.enableRCP(t)
				pid := f.createRCP(t, "perimeter", orgPerimeterRCP(f.orgID(t)))
				f.attach(t, pid, f.rootID)

				_, err = f.s3(t, outsider).GetObject(t.Context(), &s3.GetObjectInput{
					Bucket: aws.String("rcp-b"), Key: aws.String("k"),
				})
				require.Error(t, err)

				msg := accessDeniedMessage(t, err)
				assert.Contains(t, msg, "is not authorized to perform: s3:GetObject")
				assert.Contains(t, msg, "with an explicit deny in a resource control policy: arn:aws:organizations::")
				assert.Contains(t, msg, "/resource_control_policy/"+pid)

				_, err = f.s3(t, peer).GetObject(t.Context(), &s3.GetObjectInput{
					Bucket: aws.String("rcp-b"), Key: aws.String("k"),
				})
				require.NoError(t, err)

				_, err = f.s3(t, member).GetObject(t.Context(), &s3.GetObjectInput{
					Bucket: aws.String("rcp-b"), Key: aws.String("k"),
				})
				require.NoError(t, err)

				var denied *iampolicystream.Event

				for _, ev := range f.rec.Snapshot(func(iampolicystream.Event) bool { return true }) {
					if ev.Decision == iampolicystream.DecisionDeny {
						denied = &ev
					}
				}

				require.NotNil(t, denied)
				assert.Equal(t, iampolicystream.ReasonResourceControl, denied.Reason)
				assert.Equal(t, iampolicystream.DenyExplicit, denied.DenyType)
				require.NotNil(t, denied.Policy)
				assert.Equal(t, iampolicystream.PolicyRCP, denied.Policy.Type)
				assert.Contains(t, denied.Policy.Arn, pid)
			},
		},
		{
			name: "management_account_resources_exempt",
			run: func(t *testing.T, f *scpFixture, _ scpCaller) {
				t.Helper()

				f.enableRCP(t)
				f.attach(t, f.createRCP(t, "perimeter", orgPerimeterRCP(f.orgID(t))), f.rootID)

				mgmt := f.adminUser(t, "", "mgmt-admin")
				f.bucketWithObject(t, mgmt, "mgmt-b")

				outsider := f.adminUser(t, outsideAccount, "outsider")
				_, err := f.s3(t, outsider).GetObject(t.Context(), &s3.GetObjectInput{
					Bucket: aws.String("mgmt-b"), Key: aws.String("k"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "rcp_type_disabled_has_no_effect",
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				f.bucketWithObject(t, member, "off-b")
				f.enableRCP(t)
				pid := f.createRCP(t, "perimeter", orgPerimeterRCP(f.orgID(t)))
				f.attach(t, pid, f.rootID)

				_, err := f.orgs.DisablePolicyType(t.Context(), &organizations.DisablePolicyTypeInput{
					RootId: aws.String(f.rootID), PolicyType: orgtypes.PolicyTypeResourceControlPolicy,
				})
				require.NoError(t, err)

				outsider := f.adminUser(t, outsideAccount, "outsider")
				_, err = f.s3(t, outsider).GetObject(t.Context(), &s3.GetObjectInput{
					Bucket: aws.String("off-b"), Key: aws.String("k"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "unsupported_service_not_evaluated",
			run: func(t *testing.T, f *scpFixture, member scpCaller) {
				t.Helper()

				f.enableRCP(t)
				f.attach(t, f.createRCP(t, "perimeter", orgPerimeterRCP(f.orgID(t))), f.rootID)
				f.bucketWithObject(t, member, "svc-b")

				_, err := f.s3(t, member).ListBuckets(t.Context(), &s3.ListBucketsInput{})
				require.NoError(t, err)
			},
		},
		{
			name: "default_rcp_attached_and_not_detachable",
			run: func(t *testing.T, f *scpFixture, _ scpCaller) {
				t.Helper()

				f.enableRCP(t)

				for _, target := range []string{f.rootID, f.ouID, f.accountID} {
					out, err := f.orgs.ListPoliciesForTarget(t.Context(), &organizations.ListPoliciesForTargetInput{
						TargetId: aws.String(target), Filter: orgtypes.PolicyTypeResourceControlPolicy,
					})
					require.NoError(t, err)
					require.Len(t, out.Policies, 1)
					assert.Equal(t, rcpFullAccessID, aws.ToString(out.Policies[0].Id))
				}

				_, err := f.orgs.DetachPolicy(t.Context(), &organizations.DetachPolicyInput{
					PolicyId: aws.String(rcpFullAccessID), TargetId: aws.String(f.ouID),
				})
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidInputException")
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

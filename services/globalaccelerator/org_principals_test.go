package globalaccelerator_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gasdk "github.com/aws/aws-sdk-go-v2/service/globalaccelerator"
	gatypes "github.com/aws/aws-sdk-go-v2/service/globalaccelerator/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

type fakeOrgDirectory struct {
	orgs map[string][]string
	ous  map[string][]string
}

func (f fakeOrgDirectory) OrganizationAccountIDs(id string) []string { return f.orgs[id] }
func (f fakeOrgDirectory) OUAccountIDs(id string) []string           { return f.ous[id] }

func TestCrossAccountAttachmentOrgPrincipals(t *testing.T) {
	t.Parallel()

	const (
		orgARN = "arn:aws:organizations::111122223333:organization/o-abc1234567"
		ouARN  = "arn:aws:organizations::111122223333:ou/o-abc1234567/ou-ab12-cdef3456"
	)

	dir := fakeOrgDirectory{
		orgs: map[string][]string{"o-abc1234567": {awsmeta.DefaultAccount}},
		ous:  map[string][]string{"ou-ab12-cdef3456": {"555566667777"}},
	}

	tests := []struct {
		name      string
		principal string
		wantOwner bool
	}{
		{name: "organization_member", principal: orgARN, wantOwner: true},
		{name: "ou_without_caller", principal: ouARN, wantOwner: false},
		{
			name:      "unknown_org",
			principal: "arn:aws:organizations::111122223333:organization/o-zzzzzzzzzz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetOrgDirectory(dir)
			ctx := t.Context()

			_, err := f.client.CreateCrossAccountAttachment(ctx, &gasdk.CreateCrossAccountAttachmentInput{
				Name:       aws.String("share"),
				Principals: []string{tt.principal},
				Resources: []gatypes.Resource{{
					EndpointId: aws.String(
						"arn:aws:elasticloadbalancing:us-west-2:111122223333:loadbalancer/app/x/abc",
					),
					Region: aws.String("us-west-2"),
				}},
			})
			require.NoError(t, err)

			got, err := f.client.ListCrossAccountResourceAccounts(ctx, &gasdk.ListCrossAccountResourceAccountsInput{})
			require.NoError(t, err)

			if tt.wantOwner {
				assert.Equal(t, []string{awsmeta.DefaultAccount}, got.ResourceOwnerAwsAccountIds)
			} else {
				assert.Empty(t, got.ResourceOwnerAwsAccountIds)
			}
		})
	}
}

package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_NetworkAclAssociationIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		unknown bool
	}{
		{name: "replace by association id"},
		{name: "unknown association id", unknown: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newMiscClient(t)

			vpc, err := b.CreateVpc("10.60.0.0/16", "")
			require.NoError(t, err)
			subnet, err := b.CreateSubnet(vpc.ID, "10.60.1.0/24", "us-east-1a")
			require.NoError(t, err)
			acl, err := b.CreateNetworkACL(vpc.ID)
			require.NoError(t, err)

			before, err := client.DescribeNetworkAcls(t.Context(), &ec2sdk.DescribeNetworkAclsInput{
				Filters: tailFilter("association.subnet-id", subnet.ID),
			})
			require.NoError(t, err)
			require.Len(t, before.NetworkAcls, 1)
			require.Len(t, before.NetworkAcls[0].Associations, 1)

			oldID := aws.ToString(before.NetworkAcls[0].Associations[0].NetworkAclAssociationId)
			assert.Contains(t, oldID, "aclassoc-")
			assert.NotEqual(t, subnet.ID, oldID)
			assert.Equal(t, subnet.ID, aws.ToString(before.NetworkAcls[0].Associations[0].SubnetId))

			ref := oldID
			if tt.unknown {
				ref = "aclassoc-doesnotexist00"
			}

			out, err := client.ReplaceNetworkAclAssociation(t.Context(), &ec2sdk.ReplaceNetworkAclAssociationInput{
				AssociationId: aws.String(ref), NetworkAclId: aws.String(acl.ID),
			})
			if tt.unknown {
				require.ErrorContains(t, err, "InvalidAssociationID.NotFound")

				return
			}

			require.NoError(t, err)

			newID := aws.ToString(out.NewAssociationId)
			assert.Contains(t, newID, "aclassoc-")
			assert.NotEqual(t, oldID, newID, "moving to another ACL issues a new association ID")

			after, err := client.DescribeNetworkAcls(t.Context(), &ec2sdk.DescribeNetworkAclsInput{
				Filters: tailFilter("association.association-id", newID),
			})
			require.NoError(t, err)
			require.Len(t, after.NetworkAcls, 1)
			assert.Equal(t, acl.ID, aws.ToString(after.NetworkAcls[0].NetworkAclId))
			assert.Equal(t, newID, aws.ToString(after.NetworkAcls[0].Associations[0].NetworkAclAssociationId))

			stale, err := client.DescribeNetworkAcls(t.Context(), &ec2sdk.DescribeNetworkAclsInput{
				Filters: tailFilter("association.association-id", oldID),
			})
			require.NoError(t, err)
			assert.Empty(t, stale.NetworkAcls)
		})
	}
}

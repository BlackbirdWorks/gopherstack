package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/require"
)

func TestAssociateVpcCidrBlock_Restrictions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		primary string
		add     string
		wantErr bool
	}{
		{name: "10 with 172", primary: "10.0.0.0/16", add: "172.16.0.0/16", wantErr: true},
		{name: "10 with 192", primary: "10.0.0.0/16", add: "192.168.0.0/16", wantErr: true},
		{name: "10 with 198.19", primary: "10.0.0.0/16", add: "198.19.0.0/16", wantErr: true},
		{name: "10 with 10", primary: "10.0.0.0/16", add: "10.2.0.0/16"},
		{name: "10 with public", primary: "10.0.0.0/16", add: "44.0.0.0/16"},
		{name: "10 with cgnat", primary: "10.0.0.0/16", add: "100.64.0.0/16"},
		{name: "10.0/15 then 10.0/16", primary: "10.0.0.0/15", add: "10.0.0.0/16", wantErr: true},
		{name: "172 with 172.31", primary: "172.16.0.0/16", add: "172.31.0.0/16", wantErr: true},
		{name: "172 with 172", primary: "172.16.0.0/16", add: "172.17.0.0/16"},
		{name: "192 with 10", primary: "192.168.0.0/16", add: "10.1.0.0/16", wantErr: true},
		{name: "public with 10", primary: "44.0.0.0/16", add: "10.1.0.0/16", wantErr: true},
		{name: "public with public", primary: "44.0.0.0/16", add: "45.0.0.0/16"},
		{name: "cgnat with 198.19", primary: "100.64.0.0/16", add: "198.19.0.0/16", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestBackendAndClient(t)

			vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String(tc.primary)})
			require.NoError(t, err)

			_, err = client.AssociateVpcCidrBlock(t.Context(), &ec2sdk.AssociateVpcCidrBlockInput{
				VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String(tc.add),
			})
			if tc.wantErr {
				require.ErrorContains(t, err, "InvalidVpc.Range")

				return
			}

			require.NoError(t, err)
		})
	}
}

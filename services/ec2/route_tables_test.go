package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouteTableOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "create", op: "create", wantErr: false},
		{name: "create_bad_vpc", op: "create_bad_vpc", wantErr: true},
		{name: "describe_all", op: "describe_all", wantErr: false},
		{name: "delete", op: "delete", wantErr: false},
		{name: "delete_nonexistent", op: "delete_nonexistent", wantErr: true},
		{name: "create_route", op: "create_route", wantErr: false},
		{name: "delete_route", op: "delete_route", wantErr: false},
		{name: "delete_route_not_found", op: "delete_route_not_found", wantErr: true},
		{name: "associate_disassociate", op: "associate_disassociate", wantErr: false},
		{name: "associate_bad_rt", op: "associate_bad_rt", wantErr: true},
		{name: "associate_bad_subnet", op: "associate_bad_subnet", wantErr: true},
		{name: "disassociate_nonexistent", op: "disassociate_nonexistent", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				assert.NotEmpty(t, rt.ID)
				assert.Equal(t, "vpc-default", rt.VPCID)

			case "create_bad_vpc":
				_, err := b.CreateRouteTable("vpc-nonexistent")
				require.Error(t, err)

			case "describe_all":
				_, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				rts := b.DescribeRouteTables(nil)
				assert.NotEmpty(t, rts)

			case "delete":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				err = b.DeleteRouteTable(rt.ID)
				require.NoError(t, err)

			case "delete_nonexistent":
				err := b.DeleteRouteTable("rtb-nonexistent")
				require.Error(t, err)

			case "create_route":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				err = b.CreateRoute(rt.ID, "0.0.0.0/0", "igw-123", "")
				require.NoError(t, err)
				rts := b.DescribeRouteTables([]string{rt.ID})
				require.Len(t, rts, 1)
				assert.Len(t, rts[0].Routes, 1)

			case "delete_route":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				err = b.CreateRoute(rt.ID, "0.0.0.0/0", "igw-123", "")
				require.NoError(t, err)
				err = b.DeleteRoute(rt.ID, "0.0.0.0/0")
				require.NoError(t, err)
				rts := b.DescribeRouteTables([]string{rt.ID})
				require.Len(t, rts, 1)
				assert.Empty(t, rts[0].Routes)

			case "delete_route_not_found":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				err = b.DeleteRoute(rt.ID, "10.0.0.0/8")
				require.Error(t, err)

			case "associate_disassociate":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				assocID, err := b.AssociateRouteTable(rt.ID, "subnet-default")
				require.NoError(t, err)
				assert.NotEmpty(t, assocID)
				err = b.DisassociateRouteTable(assocID)
				require.NoError(t, err)

			case "associate_bad_rt":
				_, err := b.AssociateRouteTable("rtb-nonexistent", "subnet-default")
				require.Error(t, err)

			case "associate_bad_subnet":
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				_, err = b.AssociateRouteTable(rt.ID, "subnet-nonexistent")
				require.Error(t, err)

			case "disassociate_nonexistent":
				err := b.DisassociateRouteTable("rtbassoc-nonexistent")
				require.Error(t, err)
			}
		})
	}
}

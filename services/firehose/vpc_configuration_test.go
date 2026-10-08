package firehose_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/firehose"
)

type fakeSiblings struct{ ec2 service.Registerable }

func (f *fakeSiblings) GetEC2Handler() service.Registerable { return f.ec2 }

func TestCreateDeliveryStream_VpcConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		subnets func(a, b string) []string
		sgs     func(sg string) []string
		name    string
		wantErr bool
	}{
		{
			name:    "resolves vpc id",
			subnets: func(a, _ string) []string { return []string{a} },
			sgs:     func(g string) []string { return []string{g} },
		},
		{
			name:    "unknown subnet",
			subnets: func(string, string) []string { return []string{"subnet-nope"} },
			sgs:     func(g string) []string { return []string{g} },
			wantErr: true,
		},
		{
			name:    "unknown group",
			subnets: func(a, _ string) []string { return []string{a} },
			sgs:     func(string) []string { return []string{"sg-nope"} },
			wantErr: true,
		},
		{
			name:    "mixed vpcs",
			subnets: func(a, b string) []string { return []string{a, b} },
			sgs:     func(g string) []string { return []string{g} },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ec2Bk := ec2backend.NewInMemoryBackend("000000000000", flushRegion)
			vpc1, err := ec2Bk.CreateVpc("10.1.0.0/16", "")
			require.NoError(t, err)
			vpc2, err := ec2Bk.CreateVpc("10.2.0.0/16", "")
			require.NoError(t, err)
			sub1, err := ec2Bk.CreateSubnet(vpc1.ID, "10.1.0.0/24", flushRegion+"a")
			require.NoError(t, err)
			sub2, err := ec2Bk.CreateSubnet(vpc2.ID, "10.2.0.0/24", flushRegion+"a")
			require.NoError(t, err)
			sg, err := ec2Bk.CreateSecurityGroup("fh", "fh", vpc1.ID)
			require.NoError(t, err)

			b := firehose.NewInMemoryBackend("000000000000", flushRegion)
			b.SetAppConfig(&fakeSiblings{ec2: ec2backend.NewHandler(ec2Bk)})

			ds, err := b.CreateDeliveryStream(t.Context(), firehose.CreateDeliveryStreamInput{
				Name: "vpc-stream",
				OpenSearchDestination: &firehose.OpenSearchDestinationDescription{
					DomainARN: "arn:aws:es:us-east-1:000000000000:domain/d",
					VpcConfigurationDescription: &firehose.VpcConfigurationDescription{
						RoleARN:          "arn:aws:iam::000000000000:role/r",
						SubnetIDs:        tt.subnets(sub1.ID, sub2.ID),
						SecurityGroupIDs: tt.sgs(sg.ID),
					},
				},
			})
			if tt.wantErr {
				require.ErrorIs(t, err, firehose.ErrValidation)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, vpc1.ID, ds.OpenSearchDestination.VpcConfigurationDescription.VpcID)
		})
	}
}

package vpclattice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	vpclatticetypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

func TestServiceNetworkResourceAssociation_PrivateDNSEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		customDomain  string
		wantPrivateDN string
		privateDNS    bool
	}{
		{
			name:          "enabled_with_domain",
			customDomain:  "db.example.com",
			privateDNS:    true,
			wantPrivateDN: "db.example.com",
		},
		{name: "disabled", customDomain: "db.example.com"},
		{name: "enabled_without_domain", privateDNS: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
			ctx := t.Context()

			sn, err := client.CreateServiceNetwork(
				ctx,
				&vpclatticesdk.CreateServiceNetworkInput{Name: aws.String("sn")},
			)
			require.NoError(t, err)

			rcIn := &vpclatticesdk.CreateResourceConfigurationInput{
				Name: aws.String("rc"), Type: vpclatticetypes.ResourceConfigurationTypeSingle,
			}
			if tt.customDomain != "" {
				rcIn.CustomDomainName = aws.String(tt.customDomain)
			}

			rc, err := client.CreateResourceConfiguration(ctx, rcIn)
			require.NoError(t, err)

			created, err := client.CreateServiceNetworkResourceAssociation(
				ctx, &vpclatticesdk.CreateServiceNetworkResourceAssociationInput{
					ServiceNetworkIdentifier:        sn.Id,
					ResourceConfigurationIdentifier: rc.Id,
					PrivateDnsEnabled:               aws.Bool(tt.privateDNS),
				},
			)
			require.NoError(t, err)

			got, err := client.GetServiceNetworkResourceAssociation(
				ctx, &vpclatticesdk.GetServiceNetworkResourceAssociationInput{
					ServiceNetworkResourceAssociationIdentifier: created.Id,
				},
			)
			require.NoError(t, err)

			listed, err := client.ListServiceNetworkResourceAssociations(
				ctx, &vpclatticesdk.ListServiceNetworkResourceAssociationsInput{
					ServiceNetworkIdentifier: sn.Id,
				},
			)
			require.NoError(t, err)
			require.Len(t, listed.Items, 1)

			if tt.wantPrivateDN == "" {
				assert.Nil(t, got.PrivateDnsEntry)
				assert.Nil(t, listed.Items[0].PrivateDnsEntry)

				return
			}

			require.NotNil(t, got.PrivateDnsEntry)
			assert.Equal(t, tt.wantPrivateDN, aws.ToString(got.PrivateDnsEntry.DomainName))
			assert.NotEmpty(t, aws.ToString(got.PrivateDnsEntry.HostedZoneId))
			require.NotNil(t, listed.Items[0].PrivateDnsEntry)
			assert.Equal(t, got.PrivateDnsEntry.HostedZoneId, listed.Items[0].PrivateDnsEntry.HostedZoneId)
		})
	}
}

package vpclattice_test

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	vpclatticetypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

type fakeEndpointDirectory struct {
	byServiceNetwork map[string][]vpclattice.VpcEndpointRef
	byResourceConfig map[string][]vpclattice.VpcEndpointRef
	disassociated    []string
	mu               sync.Mutex
}

func (f *fakeEndpointDirectory) ServiceNetworkEndpoints(_, arn string) []vpclattice.VpcEndpointRef {
	return f.byServiceNetwork[arn]
}

func (f *fakeEndpointDirectory) ResourceConfigurationEndpoints(_, arn string) []vpclattice.VpcEndpointRef {
	return f.byResourceConfig[arn]
}

func (f *fakeEndpointDirectory) DisassociateResourceConfiguration(_, endpointID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disassociated = append(f.disassociated, endpointID)

	return nil
}

func TestEndpointAssociations(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name         string
		filterOwner  string
		filterID     string
		wantEndpoint []string
	}{
		{name: "all", wantEndpoint: []string{"vpce-aaa", "vpce-bbb"}},
		{name: "by_endpoint", filterID: "vpce-bbb", wantEndpoint: []string{"vpce-bbb"}},
		{name: "by_owner", filterOwner: "111111111111", wantEndpoint: []string{"vpce-aaa"}},
		{name: "no_match", filterID: "vpce-zzz"},
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

			rc, err := client.CreateResourceConfiguration(ctx, &vpclatticesdk.CreateResourceConfigurationInput{
				Name: aws.String("rc"), Type: vpclatticetypes.ResourceConfigurationTypeArn,
				ResourceConfigurationDefinition: &vpclatticetypes.ResourceConfigurationDefinitionMemberArnResource{
					Value: vpclatticetypes.ArnResource{Arn: aws.String("arn:aws:rds:us-east-1:000000000000:db:mydb")},
				},
			})
			require.NoError(t, err)

			eps := []vpclattice.VpcEndpointRef{
				{ID: "vpce-aaa", VpcID: "vpc-1", OwnerID: "111111111111", State: "available", CreatedAt: created},
				{ID: "vpce-bbb", VpcID: "vpc-2", OwnerID: "222222222222", State: "available", CreatedAt: created},
			}
			dir := &fakeEndpointDirectory{
				byServiceNetwork: map[string][]vpclattice.VpcEndpointRef{aws.ToString(sn.Arn): eps},
				byResourceConfig: map[string][]vpclattice.VpcEndpointRef{aws.ToString(rc.Arn): eps},
			}
			backend.SetEndpointDirectory(dir)

			listed, err := client.ListResourceEndpointAssociations(
				ctx,
				&vpclatticesdk.ListResourceEndpointAssociationsInput{
					ResourceConfigurationIdentifier: rc.Id,
					VpcEndpointId:                   nonEmpty(tt.filterID),
					VpcEndpointOwner:                nonEmpty(tt.filterOwner),
				},
			)
			require.NoError(t, err)

			var got []string
			for _, a := range listed.Items {
				got = append(got, aws.ToString(a.VpcEndpointId))
				assert.Equal(t, aws.ToString(rc.Arn), aws.ToString(a.ResourceConfigurationArn))
				assert.Equal(t, created, aws.ToTime(a.CreatedAt))
			}

			assert.Equal(t, tt.wantEndpoint, got)

			snList, err := client.ListServiceNetworkVpcEndpointAssociations(
				ctx, &vpclatticesdk.ListServiceNetworkVpcEndpointAssociationsInput{ServiceNetworkIdentifier: sn.Id},
			)
			require.NoError(t, err)
			require.Len(t, snList.Items, 2)
			assert.Equal(t, "vpc-1", aws.ToString(snList.Items[0].VpcId))
			assert.Equal(t, aws.ToString(sn.Arn), aws.ToString(snList.Items[0].ServiceNetworkArn))

			if len(listed.Items) == 0 {
				return
			}

			deleted, err := client.DeleteResourceEndpointAssociation(
				ctx,
				&vpclatticesdk.DeleteResourceEndpointAssociationInput{
					ResourceEndpointAssociationIdentifier: listed.Items[0].Id,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(listed.Items[0].VpcEndpointId), aws.ToString(deleted.VpcEndpointId))
			assert.Equal(t, []string{tt.wantEndpoint[0]}, dir.disassociated)
		})
	}
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

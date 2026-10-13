package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice"
	dstypes "github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkMailDeleteOrganizationDeletesDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		deleteDirectory bool
	}{
		{name: "delete_directory", deleteDirectory: true},
		{name: "keep_directory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			ds := directoryservice.NewFromConfig(fx.cfg)
			wm := workmail.NewFromConfig(fx.cfg)

			dir, err := ds.CreateDirectory(t.Context(), &directoryservice.CreateDirectoryInput{
				Name: aws.String(
					"corp.example.com",
				),
				Password: aws.String("Passw0rd!Passw0rd"),
				Size:     dstypes.DirectorySizeSmall,
				VpcSettings: &dstypes.DirectoryVpcSettings{
					VpcId: aws.String("vpc-12345678"), SubnetIds: []string{"subnet-1", "subnet-2"},
				},
			})
			require.NoError(t, err)

			org, err := wm.CreateOrganization(t.Context(), &workmail.CreateOrganizationInput{
				Alias: aws.String("wm-org"), DirectoryId: dir.DirectoryId,
			})
			require.NoError(t, err)

			_, err = wm.DeleteOrganization(t.Context(), &workmail.DeleteOrganizationInput{
				OrganizationId: org.OrganizationId, DeleteDirectory: tt.deleteDirectory,
			})
			require.NoError(t, err)

			got, err := ds.DescribeDirectories(t.Context(), &directoryservice.DescribeDirectoriesInput{})
			require.NoError(t, err)

			if tt.deleteDirectory {
				assert.Empty(t, got.DirectoryDescriptions)
			} else {
				assert.Len(t, got.DirectoryDescriptions, 1)
			}
		})
	}
}

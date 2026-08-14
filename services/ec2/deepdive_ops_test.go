package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackendDeepDiveOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		scenario string
	}{
		{name: "create_image_and_usage_report", scenario: "image"},
		{name: "create_and_describe_launch_templates", scenario: "launch_template"},
		{name: "create_and_describe_vpc_endpoints", scenario: "vpc_endpoint"},
		{name: "describe_network_acls", scenario: "network_acl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.scenario {
			case "image":
				instances, err := b.RunInstances("ami-base", "t3.micro", "", 1)
				require.NoError(t, err)
				require.Len(t, instances, 1)

				image, err := b.CreateImage(instances[0].ID, "custom-image", "test image")
				require.NoError(t, err)
				require.NotEmpty(t, image.ImageID)

				images := b.DescribeImages()
				assert.Condition(t, func() bool {
					for _, got := range images {
						if got.ImageID == image.ImageID {
							return true
						}
					}

					return false
				})

				reports := b.DescribeImageUsageReports()
				assert.Condition(t, func() bool {
					for _, got := range reports {
						if got.ImageID == image.ImageID {
							return true
						}
					}

					return false
				})

			case "launch_template":
				template, err := b.CreateLaunchTemplate("web-template", "ami-123", "t3.small", nil)
				require.NoError(t, err)
				require.NotEmpty(t, template.ID)

				all := b.DescribeLaunchTemplates(nil)
				require.Len(t, all, 1)
				assert.Equal(t, "web-template", all[0].Name)

				filtered := b.DescribeLaunchTemplates([]string{"web-template"})
				require.Len(t, filtered, 1)
				assert.Equal(t, template.ID, filtered[0].ID)

			case "vpc_endpoint":
				endpoint, err := b.CreateVpcEndpoint(
					"vpc-default",
					"com.amazonaws.us-east-1.s3",
					"Interface",
					[]string{"subnet-default"},
				)
				require.NoError(t, err)
				require.NotEmpty(t, endpoint.ID)

				endpoints := b.DescribeVpcEndpoints([]string{endpoint.ID})
				require.Len(t, endpoints, 1)
				assert.Equal(t, endpoint.ID, endpoints[0].ID)
				assert.Equal(t, "vpc-default", endpoints[0].VPCID)

			case "network_acl":
				acls := b.DescribeNetworkAcls([]string{"vpc-default"})
				require.Len(t, acls, 1)
				assert.Equal(t, "acl-default-vpc-default", acls[0].ID)
				assert.True(t, acls[0].IsDefault)
			}
		})
	}
}

package terraform_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appstreammega "github.com/aws/aws-sdk-go-v2/service/appstream"
	fsxmega "github.com/aws/aws-sdk-go-v2/service/fsx"
	guarddutymega "github.com/aws/aws-sdk-go-v2/service/guardduty"
	securityhubmega "github.com/aws/aws-sdk-go-v2/service/securityhub"
	wafmega "github.com/aws/aws-sdk-go-v2/service/waf"
	workspacesmega "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parityMegaProviderBlock renders a provider block that routes the §H services to gopherstack.
// These endpoints are not part of the shared providerBlock, so each §H Terraform fixture below
// uses this provider via tfTestCase.providerFn.
func parityMegaProviderBlock(addr string) string {
	return fmt.Sprintf(`terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.0"
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = false
  s3_use_path_style           = true

  endpoints {
    appstream   = %[1]q
    ec2         = %[1]q
    fsx         = %[1]q
    guardduty   = %[1]q
    securityhub = %[1]q
    sts         = %[1]q
    waf         = %[1]q
    workspaces  = %[1]q
  }
}
`, addr)
}

// megaConfig builds an AWS SDK config pointed at the shared gopherstack container.
func megaConfig(t *testing.T) aws.Config {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return cfg
}

// TestTerraform_GuardDuty provisions a detector via Terraform and verifies it is enabled.
func TestTerraform_GuardDuty(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "guardduty/success",
			providerFn: parityMegaProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				client := guarddutymega.NewFromConfig(megaConfig(t), func(o *guarddutymega.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				out, err := client.ListDetectors(ctx, &guarddutymega.ListDetectorsInput{})
				require.NoError(t, err, "ListDetectors should succeed after terraform apply")
				require.NotEmpty(t, out.DetectorIds, "a detector should exist after apply")

				det, err := client.GetDetector(ctx, &guarddutymega.GetDetectorInput{
					DetectorId: aws.String(out.DetectorIds[0]),
				})
				require.NoError(t, err, "GetDetector should succeed")
				assert.Equal(t, "ENABLED", string(det.Status))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// TestTerraform_SecurityHub provisions a Security Hub account and verifies the hub is enabled.
func TestTerraform_SecurityHub(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "securityhub/success",
			providerFn: parityMegaProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				client := securityhubmega.NewFromConfig(megaConfig(t), func(o *securityhubmega.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				out, err := client.DescribeHub(ctx, &securityhubmega.DescribeHubInput{})
				require.NoError(t, err, "DescribeHub should succeed after terraform apply")
				assert.NotEmpty(t, aws.ToString(out.HubArn), "hub ARN should be set when enabled")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// TestTerraform_WorkSpacesIpGroup provisions an IP access control group via Terraform and
// verifies the configured CIDR rules round-trip.
func TestTerraform_WorkSpacesIpGroup(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "ipgroup",
			fixture:    "workspaces/ipgroup",
			providerFn: parityMegaProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"GroupName": "tf-ipgroup-" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := workspacesmega.NewFromConfig(megaConfig(t), func(o *workspacesmega.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				name := vars["GroupName"].(string)
				out, err := client.DescribeIpGroups(ctx, &workspacesmega.DescribeIpGroupsInput{})
				require.NoError(t, err, "DescribeIpGroups should succeed after terraform apply")

				found := false
				for _, g := range out.Result {
					if aws.ToString(g.GroupName) == name {
						found = true
						assert.GreaterOrEqual(t, len(g.UserRules), 2, "both CIDR rules should be present")

						break
					}
				}

				assert.True(t, found, "IP group %q should exist after apply", name)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// TestTerraform_AppStreamStack provisions an AppStream stack via Terraform and verifies it exists.
func TestTerraform_AppStreamStack(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "stack",
			fixture:    "appstream/stack",
			providerFn: parityMegaProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"StackName": "tf-stack-" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := appstreammega.NewFromConfig(megaConfig(t), func(o *appstreammega.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				name := vars["StackName"].(string)
				out, err := client.DescribeStacks(ctx, &appstreammega.DescribeStacksInput{
					Names: []string{name},
				})
				require.NoError(t, err, "DescribeStacks should succeed after terraform apply")
				require.Len(t, out.Stacks, 1)
				assert.Equal(t, name, aws.ToString(out.Stacks[0].Name))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// TestTerraform_WAFClassic provisions a classic WAF IPSet + Rule via Terraform and verifies the
// IPSet descriptors round-trip.
func TestTerraform_WAFClassic(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "ipset_rule",
			fixture:    "waf/ipset",
			providerFn: parityMegaProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				suffix := uuid.NewString()[:8]

				return map[string]any{
					"IPSetName":  "tfipset" + suffix,
					"RuleName":   "tfrule" + suffix,
					"MetricName": "tfmetric" + suffix,
				}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				client := wafmega.NewFromConfig(megaConfig(t), func(o *wafmega.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				out, err := client.ListIPSets(ctx, &wafmega.ListIPSetsInput{})
				require.NoError(t, err, "ListIPSets should succeed after terraform apply")
				require.NotEmpty(t, out.IPSets, "at least one IPSet should exist after apply")

				get, err := client.GetIPSet(ctx, &wafmega.GetIPSetInput{
					IPSetId: out.IPSets[0].IPSetId,
				})
				require.NoError(t, err, "GetIPSet should succeed")
				require.NotNil(t, get.IPSet)
				assert.NotEmpty(t, get.IPSet.IPSetDescriptors, "IPSet descriptors should be present")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// TestTerraform_FSxLustre provisions a VPC, subnet, and Lustre file system via Terraform and
// verifies the file system's storage capacity.
func TestTerraform_FSxLustre(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "lustre",
			fixture:    "fsx/lustre",
			providerFn: parityMegaProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				vars := vpcCIDRVars(t)
				vars["Name"] = "tf-fsx-" + uuid.NewString()[:8]

				return vars
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				client := fsxmega.NewFromConfig(megaConfig(t), func(o *fsxmega.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				out, err := client.DescribeFileSystems(ctx, &fsxmega.DescribeFileSystemsInput{})
				require.NoError(t, err, "DescribeFileSystems should succeed after terraform apply")
				require.NotEmpty(t, out.FileSystems, "a Lustre file system should exist after apply")
				assert.Equal(t, int32(1200), aws.ToInt32(out.FileSystems[0].StorageCapacity))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

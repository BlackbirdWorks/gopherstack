package terraform_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsvc "github.com/aws/aws-sdk-go-v2/service/iam"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch10 provisions IAM identity/credential resources
// (group, group membership/policy/attachment, user policy/attachment,
// access key, login profile, SSH key, service-specific credential, virtual
// MFA device, account alias/password policy, OIDC/SAML providers,
// service-linked role) and S3 bucket sub-resource configuration (accelerate,
// ACL, analytics, CORS, intelligent-tiering, inventory, lifecycle, metrics,
// object, request payment) via Terraform and verifies each through its own
// SDK client's Get/List path.
func TestTerraform_MegaBatch10(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-10",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				cfg := megaConfig(t)

				iamClient := iamsvc.NewFromConfig(cfg, func(o *iamsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				profOut, err := iamClient.GetInstanceProfile(ctx, &iamsvc.GetInstanceProfileInput{
					InstanceProfileName: aws.String("mega-batch-10-instance-profile"),
				})
				require.NoError(t, err, "GetInstanceProfile should succeed")
				require.Len(t, profOut.InstanceProfile.Roles, 1)
				assert.Equal(t, "mega-batch-10-role", aws.ToString(profOut.InstanceProfile.Roles[0].RoleName))

				groupOut, err := iamClient.GetGroup(ctx, &iamsvc.GetGroupInput{
					GroupName: aws.String("mega-batch-10-group"),
				})
				require.NoError(t, err, "GetGroup should succeed")
				require.Len(t, groupOut.Users, 1, "group membership should list the user")
				assert.Equal(t, "mega-batch-10-user", aws.ToString(groupOut.Users[0].UserName))

				groupPolicyOut, err := iamClient.GetGroupPolicy(ctx, &iamsvc.GetGroupPolicyInput{
					GroupName:  aws.String("mega-batch-10-group"),
					PolicyName: aws.String("mega-batch-10-group-policy"),
				})
				require.NoError(t, err, "GetGroupPolicy should succeed")
				assert.NotEmpty(t, aws.ToString(groupPolicyOut.PolicyDocument))

				attachedGroupPoliciesOut, err := iamClient.ListAttachedGroupPolicies(
					ctx,
					&iamsvc.ListAttachedGroupPoliciesInput{
						GroupName: aws.String("mega-batch-10-group"),
					},
				)
				require.NoError(t, err, "ListAttachedGroupPolicies should succeed")
				require.Len(t, attachedGroupPoliciesOut.AttachedPolicies, 1)
				assert.Equal(
					t,
					"mega-batch-10-policy",
					aws.ToString(attachedGroupPoliciesOut.AttachedPolicies[0].PolicyName),
				)

				userPolicyOut, err := iamClient.GetUserPolicy(ctx, &iamsvc.GetUserPolicyInput{
					UserName:   aws.String("mega-batch-10-user"),
					PolicyName: aws.String("mega-batch-10-user-policy"),
				})
				require.NoError(t, err, "GetUserPolicy should succeed")
				assert.NotEmpty(t, aws.ToString(userPolicyOut.PolicyDocument))

				attachedUserPoliciesOut, err := iamClient.ListAttachedUserPolicies(
					ctx,
					&iamsvc.ListAttachedUserPoliciesInput{
						UserName: aws.String("mega-batch-10-user"),
					},
				)
				require.NoError(t, err, "ListAttachedUserPolicies should succeed")
				require.Len(t, attachedUserPoliciesOut.AttachedPolicies, 1)
				assert.Equal(
					t,
					"mega-batch-10-policy",
					aws.ToString(attachedUserPoliciesOut.AttachedPolicies[0].PolicyName),
				)

				accessKeysOut, err := iamClient.ListAccessKeys(ctx, &iamsvc.ListAccessKeysInput{
					UserName: aws.String("mega-batch-10-user"),
				})
				require.NoError(t, err, "ListAccessKeys should succeed")
				require.Len(t, accessKeysOut.AccessKeyMetadata, 1)

				_, err = iamClient.GetLoginProfile(ctx, &iamsvc.GetLoginProfileInput{
					UserName: aws.String("mega-batch-10-user"),
				})
				require.NoError(t, err, "GetLoginProfile should succeed")

				sshKeysOut, err := iamClient.ListSSHPublicKeys(ctx, &iamsvc.ListSSHPublicKeysInput{
					UserName: aws.String("mega-batch-10-user"),
				})
				require.NoError(t, err, "ListSSHPublicKeys should succeed")
				require.Len(t, sshKeysOut.SSHPublicKeys, 1)

				credsOut, err := iamClient.ListServiceSpecificCredentials(
					ctx,
					&iamsvc.ListServiceSpecificCredentialsInput{
						UserName:    aws.String("mega-batch-10-user"),
						ServiceName: aws.String("cassandra.amazonaws.com"),
					},
				)
				require.NoError(t, err, "ListServiceSpecificCredentials should succeed")
				require.Len(t, credsOut.ServiceSpecificCredentials, 1)

				mfaOut, err := iamClient.ListVirtualMFADevices(ctx, &iamsvc.ListVirtualMFADevicesInput{})
				require.NoError(t, err, "ListVirtualMFADevices should succeed")

				var foundMFA bool

				for _, d := range mfaOut.VirtualMFADevices {
					if aws.ToString(d.SerialNumber) != "" &&
						strings.HasSuffix(aws.ToString(d.SerialNumber), "mega-batch-10-mfa") {
						foundMFA = true
					}
				}

				assert.True(t, foundMFA, "virtual MFA device mega-batch-10-mfa should be listed")

				aliasOut, err := iamClient.ListAccountAliases(ctx, &iamsvc.ListAccountAliasesInput{})
				require.NoError(t, err, "ListAccountAliases should succeed")
				require.Contains(t, aliasOut.AccountAliases, "mega-batch-10-alias")

				passPolicyOut, err := iamClient.GetAccountPasswordPolicy(ctx, &iamsvc.GetAccountPasswordPolicyInput{})
				require.NoError(t, err, "GetAccountPasswordPolicy should succeed")
				require.NotNil(t, passPolicyOut.PasswordPolicy)
				assert.EqualValues(t, 12, aws.ToInt32(passPolicyOut.PasswordPolicy.MinimumPasswordLength))

				oidcOut, err := iamClient.ListOpenIDConnectProviders(ctx, &iamsvc.ListOpenIDConnectProvidersInput{})
				require.NoError(t, err, "ListOpenIDConnectProviders should succeed")

				var oidcARN string

				for _, p := range oidcOut.OpenIDConnectProviderList {
					arnStr := aws.ToString(p.Arn)
					if strings.HasSuffix(arnStr, "mega-batch-10.oidc.example.com") {
						oidcARN = arnStr
					}
				}

				require.NotEmpty(t, oidcARN, "OIDC provider should be listed")

				samlOut, err := iamClient.ListSAMLProviders(ctx, &iamsvc.ListSAMLProvidersInput{})
				require.NoError(t, err, "ListSAMLProviders should succeed")

				var foundSAML bool

				for _, p := range samlOut.SAMLProviderList {
					if strings.HasSuffix(aws.ToString(p.Arn), "saml-provider/mega-batch-10-saml") {
						foundSAML = true
					}
				}

				assert.True(t, foundSAML, "SAML provider mega-batch-10-saml should be listed")

				roleOut, err := iamClient.GetRole(ctx, &iamsvc.GetRoleInput{
					RoleName: aws.String("AWSServiceRoleForElasticbeanstalk"),
				})
				require.NoError(t, err, "GetRole should find the service-linked role")
				assert.Equal(t, "/aws-service-role/elasticbeanstalk.amazonaws.com/", aws.ToString(roleOut.Role.Path))

				s3Client := s3svc.NewFromConfig(cfg, func(o *s3svc.Options) {
					o.UsePathStyle = true
					o.BaseEndpoint = aws.String(endpoint)
				})

				const bucket = "mega-batch-10-bucket"

				accelOut, err := s3Client.GetBucketAccelerateConfiguration(
					ctx,
					&s3svc.GetBucketAccelerateConfigurationInput{
						Bucket: aws.String(bucket),
					},
				)
				require.NoError(t, err, "GetBucketAccelerateConfiguration should succeed")
				assert.Equal(t, "Enabled", string(accelOut.Status))

				_, err = s3Client.GetBucketAcl(ctx, &s3svc.GetBucketAclInput{Bucket: aws.String(bucket)})
				require.NoError(t, err, "GetBucketAcl should succeed")

				analyticsOut, err := s3Client.GetBucketAnalyticsConfiguration(
					ctx,
					&s3svc.GetBucketAnalyticsConfigurationInput{
						Bucket: aws.String(bucket),
						Id:     aws.String("mega-batch-10-analytics"),
					},
				)
				require.NoError(t, err, "GetBucketAnalyticsConfiguration should succeed")
				require.NotNil(t, analyticsOut.AnalyticsConfiguration)

				corsOut, err := s3Client.GetBucketCors(ctx, &s3svc.GetBucketCorsInput{Bucket: aws.String(bucket)})
				require.NoError(t, err, "GetBucketCors should succeed")
				require.Len(t, corsOut.CORSRules, 1)
				assert.Equal(t, []string{"https://example.com"}, corsOut.CORSRules[0].AllowedOrigins)

				tieringOut, err := s3Client.GetBucketIntelligentTieringConfiguration(
					ctx,
					&s3svc.GetBucketIntelligentTieringConfigurationInput{
						Bucket: aws.String(bucket),
						Id:     aws.String("mega-batch-10-tiering"),
					},
				)
				require.NoError(t, err, "GetBucketIntelligentTieringConfiguration should succeed")
				require.NotNil(t, tieringOut.IntelligentTieringConfiguration)

				invOut, err := s3Client.GetBucketInventoryConfiguration(
					ctx,
					&s3svc.GetBucketInventoryConfigurationInput{
						Bucket: aws.String(bucket),
						Id:     aws.String("mega-batch-10-inventory"),
					},
				)
				require.NoError(t, err, "GetBucketInventoryConfiguration should succeed")
				require.NotNil(t, invOut.InventoryConfiguration)

				lifecycleOut, err := s3Client.GetBucketLifecycleConfiguration(
					ctx,
					&s3svc.GetBucketLifecycleConfigurationInput{
						Bucket: aws.String(bucket),
					},
				)
				require.NoError(t, err, "GetBucketLifecycleConfiguration should succeed")
				require.Len(t, lifecycleOut.Rules, 1)
				assert.Equal(t, "expire", aws.ToString(lifecycleOut.Rules[0].ID))

				metricOut, err := s3Client.GetBucketMetricsConfiguration(ctx, &s3svc.GetBucketMetricsConfigurationInput{
					Bucket: aws.String(bucket),
					Id:     aws.String("mega-batch-10-metric"),
				})
				require.NoError(t, err, "GetBucketMetricsConfiguration should succeed")
				require.NotNil(t, metricOut.MetricsConfiguration)

				objOut, err := s3Client.GetObject(ctx, &s3svc.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String("mega-batch-10.txt"),
				})
				require.NoError(t, err, "GetObject should succeed")

				body, err := io.ReadAll(objOut.Body)
				require.NoError(t, err)
				assert.Equal(t, "mega batch ten", string(body))

				payOut, err := s3Client.GetBucketRequestPayment(ctx, &s3svc.GetBucketRequestPaymentInput{
					Bucket: aws.String(bucket),
				})
				require.NoError(t, err, "GetBucketRequestPayment should succeed")
				assert.Equal(t, "Requester", string(payOut.Payer))
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

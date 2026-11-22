package app

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
)

const limitTestTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

const limitTestPolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`

// priorLimitCases covers the quotas enforced before the generic SetQuotaLimiter wiring, so a refactor of
// enforceServiceQuotas that drops one fails here.
func priorLimitCases() []limitCase {
	iamHandler := func(*testing.T) service.Registerable {
		return iambackend.NewHandler(iambackend.NewInMemoryBackendWithConfig(quotaTestAccount))
	}
	iamClient := func(e limitEnv) *iam.Client {
		return iam.NewFromConfig(e.cfg, func(o *iam.Options) { o.BaseEndpoint = e.endpoint() })
	}

	return []limitCase{
		{
			name: "iam roles", svc: "iam", quota: "L-F4A5425F", code: "LimitExceeded", limit: 2,
			handler: iamHandler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := iamClient(e).CreateRole(ctx, &iam.CreateRoleInput{
					RoleName: aws.String(name("r", i)), AssumeRolePolicyDocument: aws.String(limitTestTrust),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := iamClient(e).DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: aws.String(name("r", i))})

				return err
			},
		},
		{
			name: "iam users", svc: "iam", quota: "L-F55AF5E4", code: "LimitExceeded", limit: 2,
			handler: iamHandler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := iamClient(e).CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(name("u", i))})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := iamClient(e).DeleteUser(ctx, &iam.DeleteUserInput{UserName: aws.String(name("u", i))})

				return err
			},
		},
		{
			name: "iam customer managed policies", svc: "iam", quota: "L-0DA4ABF3", code: "LimitExceeded", limit: 2,
			handler: iamHandler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := iamClient(e).CreatePolicy(ctx, &iam.CreatePolicyInput{
					PolicyName: aws.String(name("p", i)), PolicyDocument: aws.String(limitTestPolicyDoc),
				})

				return err
			},
		},
		{
			name: "iam instance profiles", svc: "iam", quota: "L-E95E4862", code: "LimitExceeded", limit: 2,
			handler: iamHandler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := iamClient(e).CreateInstanceProfile(ctx, &iam.CreateInstanceProfileInput{
					InstanceProfileName: aws.String(name("ip", i)),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := iamClient(e).DeleteInstanceProfile(ctx, &iam.DeleteInstanceProfileInput{
					InstanceProfileName: aws.String(name("ip", i)),
				})

				return err
			},
		},
		{
			name: "eks clusters", svc: "eks", quota: "L-1194D53C", code: "ResourceLimitExceededException", limit: 2,
			handler: func(t *testing.T) service.Registerable {
				t.Helper()

				bk := eksbackend.NewInMemoryBackend(t.Context(), quotaTestAccount, quotaTestRegion)

				return eksbackend.NewHandler(bk)
			},
			create: func(ctx context.Context, e limitEnv, i int) error {
				c := eks.NewFromConfig(e.cfg, func(o *eks.Options) { o.BaseEndpoint = e.endpoint() })
				_, err := c.CreateCluster(ctx, &eks.CreateClusterInput{
					Name: aws.String(name("c", i)), RoleArn: aws.String(limitTestRole),
					ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				c := eks.NewFromConfig(e.cfg, func(o *eks.Options) { o.BaseEndpoint = e.endpoint() })
				_, err := c.DeleteCluster(ctx, &eks.DeleteClusterInput{Name: aws.String(name("c", i))})

				return err
			},
		},
	}
}

func TestQuotaLimitsPreviouslyEnforcedServices(t *testing.T) {
	t.Parallel()

	runLimitCases(t, priorLimitCases())
}

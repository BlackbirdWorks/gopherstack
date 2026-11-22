package app

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	fhtypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	asbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
)

func moreLimitCases() []limitCase {
	asHandler := func(*testing.T) service.Registerable {
		return asbackend.NewHandler(asbackend.NewInMemoryBackendWithConfig(quotaTestAccount, quotaTestRegion))
	}
	asClient := func(e limitEnv) *autoscaling.Client {
		return autoscaling.NewFromConfig(e.cfg, func(o *autoscaling.Options) { o.BaseEndpoint = e.endpoint() })
	}

	return []limitCase{
		{
			name: "auto scaling groups", svc: "autoscaling", quota: "L-CDE20ADC",
			code: "LimitExceeded", limit: 2,
			handler: asHandler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := asClient(e).CreateAutoScalingGroup(ctx, &autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: aws.String(name("g", i)), MinSize: aws.Int32(0), MaxSize: aws.Int32(1),
					AvailabilityZones:       []string{"us-east-1a"},
					LaunchConfigurationName: aws.String("lc"),
				})

				return err
			},
			prepare: func(ctx context.Context, e limitEnv) error {
				_, err := asClient(e).CreateLaunchConfiguration(ctx, &autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: aws.String("lc"), ImageId: aws.String("ami-12345678"),
					InstanceType: aws.String("t3.micro"),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := asClient(e).DeleteAutoScalingGroup(ctx, &autoscaling.DeleteAutoScalingGroupInput{
					AutoScalingGroupName: aws.String(name("g", i)),
				})

				return err
			},
		},
		{
			name: "launch configurations", svc: "autoscaling", quota: "L-6B80B8FA",
			code: "LimitExceeded", limit: 2,
			handler: asHandler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := asClient(e).CreateLaunchConfiguration(ctx, &autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: aws.String(name("lc", i)), ImageId: aws.String("ami-12345678"),
					InstanceType: aws.String("t3.micro"),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := asClient(e).DeleteLaunchConfiguration(ctx, &autoscaling.DeleteLaunchConfigurationInput{
					LaunchConfigurationName: aws.String(name("lc", i)),
				})

				return err
			},
		},
		{
			name: "file systems", svc: "elasticfilesystem", quota: "L-848C634D",
			code: "FileSystemLimitExceeded", limit: 2,
			handler: func(*testing.T) service.Registerable {
				return efsbackend.NewHandler(efsbackend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion))
			},
			create: func(ctx context.Context, e limitEnv, i int) error {
				c := efs.NewFromConfig(e.cfg, func(o *efs.Options) { o.BaseEndpoint = e.endpoint() })
				_, err := c.CreateFileSystem(ctx, &efs.CreateFileSystemInput{CreationToken: aws.String(name("t", i))})

				return err
			},
		},
		{
			name: "delivery streams", svc: "firehose", quota: "L-14BB0BE7",
			code: "LimitExceededException", limit: 2,
			handler: func(*testing.T) service.Registerable {
				return firehosebackend.NewHandler(firehosebackend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion))
			},
			create: func(ctx context.Context, e limitEnv, i int) error {
				c := firehose.NewFromConfig(e.cfg, func(o *firehose.Options) { o.BaseEndpoint = e.endpoint() })
				_, err := c.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
					DeliveryStreamName: aws.String(name("s", i)),
					ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{
						BucketARN: aws.String("arn:aws:s3:::b"), RoleARN: aws.String(limitTestRole),
					},
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				c := firehose.NewFromConfig(e.cfg, func(o *firehose.Options) { o.BaseEndpoint = e.endpoint() })
				_, err := c.DeleteDeliveryStream(
					ctx,
					&firehose.DeleteDeliveryStreamInput{DeliveryStreamName: aws.String(name("s", i))},
				)

				return err
			},
		},
		{
			name: "identity pools", svc: "cognito-identity", quota: "L-8692CE1C",
			code: "LimitExceededException", limit: 2,
			handler: func(*testing.T) service.Registerable {
				return cognitoidentitybackend.NewHandler(
					cognitoidentitybackend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion), quotaTestRegion)
			},
			create: func(ctx context.Context, e limitEnv, i int) error {
				c := cognitoidentity.NewFromConfig(
					e.cfg,
					func(o *cognitoidentity.Options) { o.BaseEndpoint = e.endpoint() },
				)
				_, err := c.CreateIdentityPool(ctx, &cognitoidentity.CreateIdentityPoolInput{
					IdentityPoolName: aws.String(name("p", i)), AllowUnauthenticatedIdentities: true,
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := cognitoidentity.NewFromConfig(
					e.cfg,
					func(o *cognitoidentity.Options) { o.BaseEndpoint = e.endpoint() },
				)

				list, err := c.ListIdentityPools(
					ctx,
					&cognitoidentity.ListIdentityPoolsInput{MaxResults: aws.Int32(10)},
				)
				if err != nil {
					return err
				}

				_, err = c.DeleteIdentityPool(
					ctx,
					&cognitoidentity.DeleteIdentityPoolInput{IdentityPoolId: list.IdentityPools[0].IdentityPoolId},
				)

				return err
			},
		},
		{
			name: "user pools", svc: "cognito-idp", quota: "L-66E6DF30",
			code: "LimitExceededException", limit: 2,
			handler: func(*testing.T) service.Registerable {
				return cognitoidpbackend.NewHandler(
					cognitoidpbackend.NewInMemoryBackend(
						quotaTestAccount,
						quotaTestRegion,
						"http://localhost",
					),
					quotaTestRegion,
				)
			},
			create: func(ctx context.Context, e limitEnv, i int) error {
				c := cognitoidentityprovider.NewFromConfig(
					e.cfg,
					func(o *cognitoidentityprovider.Options) { o.BaseEndpoint = e.endpoint() },
				)
				_, err := c.CreateUserPool(
					ctx,
					&cognitoidentityprovider.CreateUserPoolInput{PoolName: aws.String(name("u", i))},
				)

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := cognitoidentityprovider.NewFromConfig(
					e.cfg,
					func(o *cognitoidentityprovider.Options) { o.BaseEndpoint = e.endpoint() },
				)

				list, err := c.ListUserPools(
					ctx,
					&cognitoidentityprovider.ListUserPoolsInput{MaxResults: aws.Int32(10)},
				)
				if err != nil {
					return err
				}

				_, err = c.DeleteUserPool(
					ctx,
					&cognitoidentityprovider.DeleteUserPoolInput{UserPoolId: list.UserPools[0].Id},
				)

				return err
			},
		},
	}
}

func TestQuotaLimitsMoreServices(t *testing.T) {
	t.Parallel()

	runLimitCases(t, moreLimitCases())
}

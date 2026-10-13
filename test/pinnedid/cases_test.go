package pinnedid_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigwv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	smsdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var errNoTask = errors.New("no task started")

// created is what a create call reports: the resource's observable ID, a
// handle for tag operations, and any tag keys echoed in the create response.
type created struct {
	id, handle string
	createTags []string
}

type pinCase struct {
	create       func(ctx context.Context, c *clients, id, n string) (created, error)
	listTags     func(ctx context.Context, c *clients, handle string) ([]string, error)
	retag        func(ctx context.Context, c *clients, handle string) error
	name         string
	valid        string
	invalid      string
	invalidCode  string
	conflictCode string
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}

func lastSegment(s, sep string) string { return s[strings.LastIndex(s, sep)+len(sep):] }

const keep = "env"

func mapTags(id string) map[string]string {
	return map[string]string{tags.PinnedIDKey: id, keep: "t"}
}

func ec2Spec(resourceType ec2types.ResourceType, id string) []ec2types.TagSpecification {
	return []ec2types.TagSpecification{
		{
			ResourceType: resourceType,
			Tags: []ec2types.Tag{
				{Key: aws.String(tags.PinnedIDKey), Value: aws.String(id)},
				{Key: aws.String(keep), Value: aws.String("t")},
			},
		},
	}
}

func ec2ListTags(ctx context.Context, c *clients, id string) ([]string, error) {
	out, err := c.ec2.DescribeTags(ctx, &ec2sdk.DescribeTagsInput{
		Filters: []ec2types.Filter{{Name: aws.String("resource-id"), Values: []string{id}}},
	})
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(out.Tags))
	for _, t := range out.Tags {
		keys = append(keys, aws.ToString(t.Key))
	}

	return keys, nil
}

func ec2Retag(ctx context.Context, c *clients, id string) error {
	_, err := c.ec2.CreateTags(ctx, &ec2sdk.CreateTagsInput{
		Resources: []string{id},
		Tags:      []ec2types.Tag{{Key: aws.String(tags.PinnedIDKey), Value: aws.String("x")}},
	})

	return err
}

func ec2TagKeys(ts []ec2types.Tag) []string {
	keys := make([]string, 0, len(ts))
	for _, t := range ts {
		keys = append(keys, aws.ToString(t.Key))
	}

	return keys
}

func allCases() []pinCase {
	return append(append(append(computeCases(), identityCases()...), edgeCases()...), dataCases()...)
}

func computeCases() []pinCase {
	return []pinCase{
		{
			name: "ec2-vpc", valid: "vpc-0a1b2c3d4e5f60718", invalid: "vpc-XYZ", invalidCode: "InvalidParameterValue",
			conflictCode: "InvalidParameterValue",
			create: func(ctx context.Context, c *clients, id, _ string) (created, error) {
				out, err := c.ec2.CreateVpc(ctx, &ec2sdk.CreateVpcInput{
					CidrBlock: aws.String("10.9.0.0/16"), TagSpecifications: ec2Spec(ec2types.ResourceTypeVpc, id),
				})
				if err != nil {
					return created{}, err
				}

				return created{
					id:         aws.ToString(out.Vpc.VpcId),
					handle:     aws.ToString(out.Vpc.VpcId),
					createTags: ec2TagKeys(out.Vpc.Tags),
				}, nil
			},
			listTags: ec2ListTags, retag: ec2Retag,
		},
		{
			name:         "ec2-subnet",
			valid:        "subnet-0a1b2c3d4e5f60718",
			invalid:      "subnet-nope",
			invalidCode:  "InvalidParameterValue",
			conflictCode: "InvalidParameterValue",
			create: func(ctx context.Context, c *clients, id, _ string) (created, error) {
				vpc, err := c.ec2.CreateVpc(ctx, &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.8.0.0/16")})
				if err != nil {
					return created{}, err
				}

				out, err := c.ec2.CreateSubnet(ctx, &ec2sdk.CreateSubnetInput{
					VpcId:             vpc.Vpc.VpcId,
					CidrBlock:         aws.String("10.8.1.0/24"),
					TagSpecifications: ec2Spec(ec2types.ResourceTypeSubnet, id),
				})
				if err != nil {
					return created{}, err
				}

				return created{
					id:         aws.ToString(out.Subnet.SubnetId),
					handle:     aws.ToString(out.Subnet.SubnetId),
					createTags: ec2TagKeys(out.Subnet.Tags),
				}, nil
			},
			listTags: ec2ListTags,
			retag:    ec2Retag,
		},
		{
			name:         "ec2-security-group",
			valid:        "sg-0a1b2c3d4e5f60718",
			invalid:      "sg-zzz",
			invalidCode:  "InvalidParameterValue",
			conflictCode: "InvalidGroup.Duplicate",
			create: func(ctx context.Context, c *clients, id, n string) (created, error) {
				out, err := c.ec2.CreateSecurityGroup(ctx, &ec2sdk.CreateSecurityGroupInput{
					GroupName: aws.String(
						"pin-" + n,
					),
					Description:       aws.String("pin"),
					TagSpecifications: ec2Spec(ec2types.ResourceTypeSecurityGroup, id),
				})
				if err != nil {
					return created{}, err
				}

				return created{
					id:         aws.ToString(out.GroupId),
					handle:     aws.ToString(out.GroupId),
					createTags: ec2TagKeys(out.Tags),
				}, nil
			},
			listTags: ec2ListTags,
			retag:    ec2Retag,
		},
		{
			name:         "ec2-instance",
			valid:        "i-0a1b2c3d4e5f60718",
			invalid:      "i-nothex",
			invalidCode:  "InvalidParameterValue",
			conflictCode: "InvalidParameterValue",
			create: func(ctx context.Context, c *clients, id, _ string) (created, error) {
				out, err := c.ec2.RunInstances(ctx, &ec2sdk.RunInstancesInput{
					ImageId:      aws.String("ami-12345678"),
					InstanceType: ec2types.InstanceTypeT3Micro,
					MinCount: aws.Int32(
						1,
					),
					MaxCount:          aws.Int32(1),
					TagSpecifications: ec2Spec(ec2types.ResourceTypeInstance, id),
				})
				if err != nil {
					return created{}, err
				}

				inst := out.Instances[0]

				return created{
					id:         aws.ToString(inst.InstanceId),
					handle:     aws.ToString(inst.InstanceId),
					createTags: ec2TagKeys(inst.Tags),
				}, nil
			},
			listTags: ec2ListTags,
			retag:    ec2Retag,
		},
		{
			name:         "ecs-task",
			valid:        "0a1b2c3d4e5f60718293a4b5c6d7e8f9",
			invalid:      "not-a-task-id",
			invalidCode:  "InvalidParameterException",
			conflictCode: "ResourceInUseException",
			create: func(ctx context.Context, c *clients, id, _ string) (created, error) {
				if _, err := c.ecs.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
					Family: aws.String("pin"),
					ContainerDefinitions: []ecstypes.ContainerDefinition{
						{Name: aws.String("c"), Image: aws.String("busybox"), Memory: aws.Int32(64)},
					},
				}); err != nil {
					return created{}, err
				}

				out, err := c.ecs.RunTask(ctx, &ecssdk.RunTaskInput{
					TaskDefinition: aws.String("pin"),
					LaunchType:     ecstypes.LaunchTypeFargate,
					Tags: []ecstypes.Tag{
						{Key: aws.String(tags.PinnedIDKey), Value: aws.String(id)},
						{Key: aws.String(keep), Value: aws.String("t")},
					},
				})
				if err != nil {
					return created{}, err
				}

				if len(out.Tasks) == 0 {
					return created{}, fmt.Errorf("%w: %v", errNoTask, out.Failures)
				}

				arn := aws.ToString(out.Tasks[0].TaskArn)
				keys := make([]string, 0)
				for _, t := range out.Tasks[0].Tags {
					keys = append(keys, aws.ToString(t.Key))
				}

				return created{id: lastSegment(arn, "/"), handle: arn, createTags: keys}, nil
			},
			listTags: func(ctx context.Context, c *clients, arn string) ([]string, error) {
				out, err := c.ecs.ListTagsForResource(
					ctx,
					&ecssdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
				)
				if err != nil {
					return nil, err
				}

				keys := make([]string, 0)
				for _, t := range out.Tags {
					keys = append(keys, aws.ToString(t.Key))
				}

				return keys, nil
			},
			retag: func(ctx context.Context, c *clients, arn string) error {
				_, err := c.ecs.TagResource(ctx, &ecssdk.TagResourceInput{
					ResourceArn: aws.String(
						arn,
					),
					Tags: []ecstypes.Tag{{Key: aws.String(tags.PinnedIDKey), Value: aws.String("x")}},
				})

				return err
			},
		},
	}
}

func identityCases() []pinCase {
	return []pinCase{
		{
			name: "kms-key", valid: "11111111-2222-3333-4444-555555555555", invalid: "not-a-uuid",
			invalidCode: "ValidationException", conflictCode: "AlreadyExistsException",
			create: func(ctx context.Context, c *clients, id, _ string) (created, error) {
				out, err := c.kms.CreateKey(ctx, &kmssdk.CreateKeyInput{
					Tags: []kmstypes.Tag{
						{TagKey: aws.String(tags.PinnedIDKey), TagValue: aws.String(id)},
						{TagKey: aws.String(keep), TagValue: aws.String("t")},
					},
				})
				if err != nil {
					return created{}, err
				}

				kid := aws.ToString(out.KeyMetadata.KeyId)

				return created{id: kid, handle: kid}, nil
			},
			listTags: func(ctx context.Context, c *clients, keyID string) ([]string, error) {
				out, err := c.kms.ListResourceTags(ctx, &kmssdk.ListResourceTagsInput{KeyId: aws.String(keyID)})
				if err != nil {
					return nil, err
				}

				keys := make([]string, 0)
				for _, t := range out.Tags {
					keys = append(keys, aws.ToString(t.TagKey))
				}

				return keys, nil
			},
			retag: func(ctx context.Context, c *clients, keyID string) error {
				_, err := c.kms.TagResource(ctx, &kmssdk.TagResourceInput{
					KeyId: aws.String(
						keyID,
					),
					Tags: []kmstypes.Tag{{TagKey: aws.String(tags.PinnedIDKey), TagValue: aws.String("x")}},
				})

				return err
			},
		},
		{
			name: "cognito-user-pool", valid: "us-east-1_PinnedA1", invalid: "bad_id",
			invalidCode: "InvalidParameterException", conflictCode: "InvalidParameterException",
			create: func(ctx context.Context, c *clients, id, n string) (created, error) {
				out, err := c.cognito.CreateUserPool(ctx, &cognitoidentityprovider.CreateUserPoolInput{
					PoolName: aws.String("pool-" + n), UserPoolTags: mapTags(id),
				})
				if err != nil {
					return created{}, err
				}

				return created{
					id:         aws.ToString(out.UserPool.Id),
					handle:     aws.ToString(out.UserPool.Arn),
					createTags: keysOf(out.UserPool.UserPoolTags),
				}, nil
			},
			listTags: func(ctx context.Context, c *clients, arn string) ([]string, error) {
				out, err := c.cognito.ListTagsForResource(
					ctx,
					&cognitoidentityprovider.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
				)
				if err != nil {
					return nil, err
				}

				return keysOf(out.Tags), nil
			},
			retag: func(ctx context.Context, c *clients, arn string) error {
				_, err := c.cognito.TagResource(ctx, &cognitoidentityprovider.TagResourceInput{
					ResourceArn: aws.String(arn), Tags: map[string]string{tags.PinnedIDKey: "x"},
				})

				return err
			},
		},
		{
			name:        "secretsmanager-arn-suffix",
			valid:       "AbC123",
			invalid:     "toolong-suffix",
			invalidCode: "InvalidParameterException",
			create: func(ctx context.Context, c *clients, id, n string) (created, error) {
				out, err := c.sm.CreateSecret(ctx, &smsdk.CreateSecretInput{
					Name: aws.String("pin-" + n), SecretString: aws.String("v"),
					Tags: []smtypes.Tag{
						{Key: aws.String(tags.PinnedIDKey), Value: aws.String(id)},
						{Key: aws.String(keep), Value: aws.String("t")},
					},
				})
				if err != nil {
					return created{}, err
				}

				arn := aws.ToString(out.ARN)

				return created{id: lastSegment(arn, "-"), handle: arn}, nil
			},
			listTags: func(ctx context.Context, c *clients, arn string) ([]string, error) {
				out, err := c.sm.DescribeSecret(ctx, &smsdk.DescribeSecretInput{SecretId: aws.String(arn)})
				if err != nil {
					return nil, err
				}

				keys := make([]string, 0)
				for _, t := range out.Tags {
					keys = append(keys, aws.ToString(t.Key))
				}

				return keys, nil
			},
			retag: func(ctx context.Context, c *clients, arn string) error {
				_, err := c.sm.TagResource(ctx, &smsdk.TagResourceInput{
					SecretId: aws.String(
						arn,
					),
					Tags: []smtypes.Tag{{Key: aws.String(tags.PinnedIDKey), Value: aws.String("x")}},
				})

				return err
			},
		},
	}
}

func edgeCases() []pinCase {
	return []pinCase{
		{
			name: "apigateway-rest-api", valid: "abcde12345", invalid: "UPPER", invalidCode: "BadRequestException",
			conflictCode: "ConflictException",
			create: func(ctx context.Context, c *clients, id, n string) (created, error) {
				out, err := c.apigw.CreateRestApi(
					ctx,
					&apigateway.CreateRestApiInput{Name: aws.String("api-" + n), Tags: mapTags(id)},
				)
				if err != nil {
					return created{}, err
				}

				return created{
					id: aws.ToString(out.Id), handle: "arn:aws:apigateway:us-east-1::/restapis/" + aws.ToString(out.Id),
					createTags: keysOf(out.Tags),
				}, nil
			},
			listTags: func(ctx context.Context, c *clients, arn string) ([]string, error) {
				out, err := c.apigw.GetTags(ctx, &apigateway.GetTagsInput{ResourceArn: aws.String(arn)})
				if err != nil {
					return nil, err
				}

				return keysOf(out.Tags), nil
			},
			retag: func(ctx context.Context, c *clients, arn string) error {
				_, err := c.apigw.TagResource(ctx, &apigateway.TagResourceInput{
					ResourceArn: aws.String(arn), Tags: map[string]string{tags.PinnedIDKey: "x"},
				})

				return err
			},
		},
		{
			name: "apigatewayv2-api", valid: "vwxyz98765", invalid: "bad id!", invalidCode: "BadRequestException",
			conflictCode: "ConflictException",
			create: func(ctx context.Context, c *clients, id, n string) (created, error) {
				out, err := c.apigwv2.CreateApi(ctx, &apigatewayv2.CreateApiInput{
					Name: aws.String("api-" + n), ProtocolType: apigwv2types.ProtocolTypeHttp, Tags: mapTags(id),
				})
				if err != nil {
					return created{}, err
				}

				return created{
					id: aws.ToString(
						out.ApiId,
					),
					handle:     "arn:aws:apigateway:us-east-1::/apis/" + aws.ToString(out.ApiId),
					createTags: keysOf(out.Tags),
				}, nil
			},
			listTags: func(ctx context.Context, c *clients, arn string) ([]string, error) {
				out, err := c.apigwv2.GetTags(ctx, &apigatewayv2.GetTagsInput{ResourceArn: aws.String(arn)})
				if err != nil {
					return nil, err
				}

				return keysOf(out.Tags), nil
			},
			retag: func(ctx context.Context, c *clients, arn string) error {
				_, err := c.apigwv2.TagResource(ctx, &apigatewayv2.TagResourceInput{
					ResourceArn: aws.String(arn), Tags: map[string]string{tags.PinnedIDKey: "x"},
				})

				return err
			},
		},
		{
			name: "efs-file-system", valid: "fs-0a1b2c3d", invalid: "fs-NOPE", invalidCode: "ValidationException",
			conflictCode: "FileSystemAlreadyExists",
			create: func(ctx context.Context, c *clients, id, n string) (created, error) {
				out, err := c.efs.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
					CreationToken: aws.String("tok-" + n),
					Tags: []efstypes.Tag{
						{Key: aws.String(tags.PinnedIDKey), Value: aws.String(id)},
						{Key: aws.String(keep), Value: aws.String("t")},
					},
				})
				if err != nil {
					return created{}, err
				}

				keys := make([]string, 0)
				for _, t := range out.Tags {
					keys = append(keys, aws.ToString(t.Key))
				}

				return created{
					id:         aws.ToString(out.FileSystemId),
					handle:     aws.ToString(out.FileSystemId),
					createTags: keys,
				}, nil
			},
			listTags: func(ctx context.Context, c *clients, fsID string) ([]string, error) {
				out, err := c.efs.ListTagsForResource(
					ctx,
					&efssdk.ListTagsForResourceInput{ResourceId: aws.String(fsID)},
				)
				if err != nil {
					return nil, err
				}

				keys := make([]string, 0)
				for _, t := range out.Tags {
					keys = append(keys, aws.ToString(t.Key))
				}

				return keys, nil
			},
			retag: func(ctx context.Context, c *clients, fsID string) error {
				_, err := c.efs.TagResource(ctx, &efssdk.TagResourceInput{
					ResourceId: aws.String(
						fsID,
					),
					Tags: []efstypes.Tag{{Key: aws.String(tags.PinnedIDKey), Value: aws.String("x")}},
				})

				return err
			},
		},
	}
}

func cfConfig(n string) *cftypes.DistributionConfig {
	return &cftypes.DistributionConfig{
		CallerReference: aws.String("ref-" + n),
		Comment:         aws.String("pin"),
		Enabled:         aws.Bool(true),
		Origins: &cftypes.Origins{
			Quantity: aws.Int32(1),
			Items: []cftypes.Origin{{
				Id: aws.String("o"), DomainName: aws.String("example.com"),
				S3OriginConfig: &cftypes.S3OriginConfig{OriginAccessIdentity: aws.String("")},
			}},
		},
		DefaultCacheBehavior: &cftypes.DefaultCacheBehavior{
			ViewerProtocolPolicy: cftypes.ViewerProtocolPolicyAllowAll,
			TargetOriginId:       aws.String("o"),
			CachePolicyId:        aws.String("658327ea-f89d-4fab-a63d-7e88639e58f6"),
		},
	}
}

func dataCases() []pinCase {
	return []pinCase{
		{
			name: "cloudfront-distribution", valid: "EABCDEFGHIJKLM", invalid: "e-bad", invalidCode: "InvalidArgument",
			conflictCode: "DistributionAlreadyExists",
			create: func(ctx context.Context, c *clients, id, n string) (created, error) {
				out, err := c.cf.CreateDistributionWithTags(ctx, &cfsdk.CreateDistributionWithTagsInput{
					DistributionConfigWithTags: &cftypes.DistributionConfigWithTags{
						DistributionConfig: cfConfig(n),
						Tags: &cftypes.Tags{Items: []cftypes.Tag{
							{Key: aws.String(tags.PinnedIDKey), Value: aws.String(id)},
							{Key: aws.String(keep), Value: aws.String("t")},
						}},
					},
				})
				if err != nil {
					return created{}, err
				}

				return created{id: aws.ToString(out.Distribution.Id), handle: aws.ToString(out.Distribution.ARN)}, nil
			},
			listTags: func(ctx context.Context, c *clients, arn string) ([]string, error) {
				out, err := c.cf.ListTagsForResource(ctx, &cfsdk.ListTagsForResourceInput{Resource: aws.String(arn)})
				if err != nil {
					return nil, err
				}

				keys := make([]string, 0)
				for _, t := range out.Tags.Items {
					keys = append(keys, aws.ToString(t.Key))
				}

				return keys, nil
			},
			retag: func(ctx context.Context, c *clients, arn string) error {
				_, err := c.cf.TagResource(ctx, &cfsdk.TagResourceInput{
					Resource: aws.String(arn),
					Tags: &cftypes.Tags{
						Items: []cftypes.Tag{{Key: aws.String(tags.PinnedIDKey), Value: aws.String("x")}},
					},
				})

				return err
			},
		},
		{
			name: "elbv2-load-balancer", valid: "0a1b2c3d4e5f6071", invalid: "short", invalidCode: "ValidationError",
			conflictCode: "DuplicateLoadBalancerName",
			create: func(ctx context.Context, c *clients, id, n string) (created, error) {
				out, err := c.elbv2.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
					Name: aws.String("lb-" + n),
					Tags: []elbv2types.Tag{
						{Key: aws.String(tags.PinnedIDKey), Value: aws.String(id)},
						{Key: aws.String(keep), Value: aws.String("t")},
					},
				})
				if err != nil {
					return created{}, err
				}

				arn := aws.ToString(out.LoadBalancers[0].LoadBalancerArn)

				return created{id: lastSegment(arn, "/"), handle: arn}, nil
			},
			listTags: func(ctx context.Context, c *clients, arn string) ([]string, error) {
				out, err := c.elbv2.DescribeTags(ctx, &elbv2sdk.DescribeTagsInput{ResourceArns: []string{arn}})
				if err != nil {
					return nil, err
				}

				keys := make([]string, 0)
				for _, td := range out.TagDescriptions {
					for _, t := range td.Tags {
						keys = append(keys, aws.ToString(t.Key))
					}
				}

				return keys, nil
			},
			retag: func(ctx context.Context, c *clients, arn string) error {
				_, err := c.elbv2.AddTags(ctx, &elbv2sdk.AddTagsInput{
					ResourceArns: []string{
						arn,
					},
					Tags: []elbv2types.Tag{{Key: aws.String(tags.PinnedIDKey), Value: aws.String("x")}},
				})

				return err
			},
		},
		{
			name:         "rds-dbi-resource-id",
			valid:        "db-ABCDEFGHIJKLMNOPQRSTUVWXYZ",
			invalid:      "db-lower",
			invalidCode:  "InvalidParameterValue",
			conflictCode: "DBInstanceAlreadyExists",
			create: func(ctx context.Context, c *clients, id, n string) (created, error) {
				out, err := c.rds.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String(
						"db" + n,
					),
					Engine:          aws.String("postgres"),
					DBInstanceClass: aws.String("db.t3.micro"),
					MasterUsername: aws.String(
						"admin",
					),
					MasterUserPassword: aws.String("password123"),
					AllocatedStorage:   aws.Int32(20),
					Tags: []rdstypes.Tag{
						{Key: aws.String(tags.PinnedIDKey), Value: aws.String(id)},
						{Key: aws.String(keep), Value: aws.String("t")},
					},
				})
				if err != nil {
					return created{}, err
				}

				return created{
					id:     aws.ToString(out.DBInstance.DbiResourceId),
					handle: aws.ToString(out.DBInstance.DBInstanceArn),
				}, nil
			},
			listTags: func(ctx context.Context, c *clients, arn string) ([]string, error) {
				out, err := c.rds.ListTagsForResource(
					ctx,
					&rdssdk.ListTagsForResourceInput{ResourceName: aws.String(arn)},
				)
				if err != nil {
					return nil, err
				}

				keys := make([]string, 0)
				for _, t := range out.TagList {
					keys = append(keys, aws.ToString(t.Key))
				}

				return keys, nil
			},
			retag: func(ctx context.Context, c *clients, arn string) error {
				_, err := c.rds.AddTagsToResource(ctx, &rdssdk.AddTagsToResourceInput{
					ResourceName: aws.String(
						arn,
					),
					Tags: []rdstypes.Tag{{Key: aws.String(tags.PinnedIDKey), Value: aws.String("x")}},
				})

				return err
			},
		},
	}
}

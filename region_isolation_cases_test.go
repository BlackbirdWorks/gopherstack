package main

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigatewaytypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigwv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	astypes "github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/backup"
	backuptypes "github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/codecommit"
	codecommittypes "github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	cognitoidentitytypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentity/types"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitotypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/aws/aws-sdk-go-v2/service/docdb"
	docdbtypes "github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	elb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	kafkatypes "github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/aws/aws-sdk-go-v2/service/memorydb"
	memorydbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/aws/aws-sdk-go-v2/service/mq"
	mqtypes "github.com/aws/aws-sdk-go-v2/service/mq/types"
	"github.com/aws/aws-sdk-go-v2/service/neptune"
	neptunetypes "github.com/aws/aws-sdk-go-v2/service/neptune/types"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchtypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/aws/aws-sdk-go-v2/service/pipes"
	pipestypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	redshifttypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	route53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/s3control"
	s3controltypes "github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	sdtypes "github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	sestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/aws-sdk-go-v2/service/wafv2"
	wafv2types "github.com/aws/aws-sdk-go-v2/service/wafv2/types"
)

const regionAccount = "000000000000"

func strs[T any](in []T, f func(T) *string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, aws.ToString(f(v)))
	}

	return out
}

func tail(in []string, sep string) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = v[strings.LastIndex(v, sep)+1:]
	}

	return out
}

func regionIsolationCases() []regionCase {
	return []regionCase{
		ssmCase(),
		cloudwatchlogsCase(),
		memorydbCase(),
		s3controlCase(),
		lightsailCase(),
		sqsCase(),
		snsCase(),
		secretsmanagerCase(),
		eventbridgeCase(),
		kinesisCase(),
		ecsCase(),
		dynamodbCase(),
		ec2Case(),
		ecrCase(),
		glueCase(),
		athenaCase(),
		backupCase(),
		iotCase(),
		codecommitCase(),
		s3Case(),
		iamCase(),
		route53Case(),
		stepfunctionsCase(),
		kmsCase(),
		cloudwatchCase(),
		apigatewayCase(),
		apigatewayv2Case(),
		cognitoidpCase(),
		efsCase(),
		firehoseCase(),
		servicediscoveryCase(),
		elasticacheCase(),
		sesv2Case(),
		autoscalingCase(),
		cloudformationCase(),
		elbv2Case(),
		codedeployCase(),
		lambdaCase(),
		elbCase(),
		rdsCase(),
		docdbCase(),
		neptuneCase(),
		opensearchCase(),
		eksCase(),
		kafkaCase(),
		mqCase(),
		redshiftCase(),
		schedulerCase(),
		pipesCase(),
		appsyncCase(),
		cognitoidentityCase(),
		sesCase(),
		wafv2Case(),
	}
}

func ssmCase() regionCase {
	return regionCase{
		name: "ssm",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := ssm.NewFromConfig(cfg).PutParameter(ctx, &ssm.PutParameterInput{
				Name: aws.String("/" + name), Value: aws.String("v"), Type: ssmtypes.ParameterTypeString,
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := ssm.NewFromConfig(cfg).DescribeParameters(ctx, &ssm.DescribeParametersInput{})
			if err != nil {
				return nil, err
			}

			names := strs(out.Parameters, func(p ssmtypes.ParameterMetadata) *string { return p.Name })

			return tail(names, "/"), nil
		},
	}
}

func cloudwatchlogsCase() regionCase {
	return regionCase{
		name: "cloudwatchlogs",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := cloudwatchlogs.NewFromConfig(cfg).CreateLogGroup(
				ctx, &cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := cloudwatchlogs.NewFromConfig(cfg).DescribeLogGroups(
				ctx, &cloudwatchlogs.DescribeLogGroupsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.LogGroups, func(g cwltypes.LogGroup) *string { return g.LogGroupName }), nil
		},
	}
}

func memorydbCase() regionCase {
	return regionCase{
		name: "memorydb",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := memorydb.NewFromConfig(cfg).
				CreateACL(ctx, &memorydb.CreateACLInput{ACLName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := memorydb.NewFromConfig(cfg).DescribeACLs(ctx, &memorydb.DescribeACLsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.ACLs, func(a memorydbtypes.ACL) *string { return a.Name }), nil
		},
	}
}

func s3controlCase() regionCase {
	return regionCase{
		name: "s3control",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := s3control.NewFromConfig(cfg).CreateAccessPoint(ctx, &s3control.CreateAccessPointInput{
				AccountId: aws.String(regionAccount), Name: aws.String(name), Bucket: aws.String("b-" + name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := s3control.NewFromConfig(cfg).ListAccessPoints(
				ctx, &s3control.ListAccessPointsInput{AccountId: aws.String(regionAccount)})
			if err != nil {
				return nil, err
			}

			return strs(out.AccessPointList, func(a s3controltypes.AccessPoint) *string { return a.Name }), nil
		},
	}
}

func lightsailCase() regionCase {
	return regionCase{
		name: "lightsail",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := lightsail.NewFromConfig(cfg).CreateKeyPair(
				ctx, &lightsail.CreateKeyPairInput{KeyPairName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := lightsail.NewFromConfig(cfg).GetKeyPairs(ctx, &lightsail.GetKeyPairsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.KeyPairs, func(k lightsailtypes.KeyPair) *string { return k.Name }), nil
		},
	}
}

func sqsCase() regionCase {
	return regionCase{
		name: "sqs",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := sqs.NewFromConfig(cfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := sqs.NewFromConfig(cfg).ListQueues(ctx, &sqs.ListQueuesInput{})

			return tail(out.QueueUrls, "/"), err
		},
	}
}

func snsCase() regionCase {
	return regionCase{
		name: "sns",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := sns.NewFromConfig(cfg).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := sns.NewFromConfig(cfg).ListTopics(ctx, &sns.ListTopicsInput{})
			if err != nil {
				return nil, err
			}

			return tail(strs(out.Topics, func(t snstypes.Topic) *string { return t.TopicArn }), ":"), nil
		},
	}
}

func secretsmanagerCase() regionCase {
	return regionCase{
		name: "secretsmanager",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := secretsmanager.NewFromConfig(cfg).CreateSecret(
				ctx, &secretsmanager.CreateSecretInput{Name: aws.String(name), SecretString: aws.String("v")})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := secretsmanager.NewFromConfig(cfg).ListSecrets(ctx, &secretsmanager.ListSecretsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.SecretList, func(s smtypes.SecretListEntry) *string { return s.Name }), nil
		},
	}
}

func eventbridgeCase() regionCase {
	return regionCase{
		name: "eventbridge",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := eventbridge.NewFromConfig(cfg).CreateEventBus(
				ctx, &eventbridge.CreateEventBusInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := eventbridge.NewFromConfig(cfg).ListEventBuses(ctx, &eventbridge.ListEventBusesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.EventBuses, func(b ebtypes.EventBus) *string { return b.Name }), nil
		},
	}
}

func kinesisCase() regionCase {
	return regionCase{
		name: "kinesis",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := kinesis.NewFromConfig(cfg).CreateStream(
				ctx, &kinesis.CreateStreamInput{StreamName: aws.String(name), ShardCount: aws.Int32(1)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := kinesis.NewFromConfig(cfg).ListStreams(ctx, &kinesis.ListStreamsInput{})

			return out.StreamNames, err
		},
	}
}

func ecsCase() regionCase {
	return regionCase{
		name: "ecs",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := ecs.NewFromConfig(cfg).
				CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := ecs.NewFromConfig(cfg).ListClusters(ctx, &ecs.ListClustersInput{})

			return tail(out.ClusterArns, "/"), err
		},
	}
}

func dynamodbCase() regionCase {
	return regionCase{
		name: "dynamodb",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := dynamodb.NewFromConfig(cfg).CreateTable(ctx, &dynamodb.CreateTableInput{
				TableName:   aws.String(name),
				BillingMode: "PAY_PER_REQUEST",
				AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
					{AttributeName: aws.String("k"), AttributeType: "S"},
				},
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("k"), KeyType: "HASH"},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := dynamodb.NewFromConfig(cfg).ListTables(ctx, &dynamodb.ListTablesInput{})

			return out.TableNames, err
		},
	}
}

func ec2Case() regionCase {
	return regionCase{
		name: "ec2",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := ec2.NewFromConfig(cfg).CreateKeyPair(ctx, &ec2.CreateKeyPairInput{KeyName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := ec2.NewFromConfig(cfg).DescribeKeyPairs(ctx, &ec2.DescribeKeyPairsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.KeyPairs, func(k ec2types.KeyPairInfo) *string { return k.KeyName }), nil
		},
	}
}

func ecrCase() regionCase {
	return regionCase{
		name: "ecr",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := ecr.NewFromConfig(cfg).CreateRepository(
				ctx, &ecr.CreateRepositoryInput{RepositoryName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := ecr.NewFromConfig(cfg).DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Repositories, func(r ecrtypes.Repository) *string { return r.RepositoryName }), nil
		},
	}
}

func glueCase() regionCase {
	return regionCase{
		name: "glue",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := glue.NewFromConfig(cfg).CreateDatabase(ctx, &glue.CreateDatabaseInput{
				DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String(name)},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := glue.NewFromConfig(cfg).GetDatabases(ctx, &glue.GetDatabasesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.DatabaseList, func(d gluetypes.Database) *string { return d.Name }), nil
		},
	}
}

func athenaCase() regionCase {
	return regionCase{
		name: "athena",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := athena.NewFromConfig(cfg).
				CreateWorkGroup(ctx, &athena.CreateWorkGroupInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := athena.NewFromConfig(cfg).ListWorkGroups(ctx, &athena.ListWorkGroupsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.WorkGroups, func(w athenatypes.WorkGroupSummary) *string { return w.Name }), nil
		},
	}
}

func backupCase() regionCase {
	return regionCase{
		name: "backup",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := backup.NewFromConfig(cfg).CreateBackupVault(
				ctx, &backup.CreateBackupVaultInput{BackupVaultName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := backup.NewFromConfig(cfg).ListBackupVaults(ctx, &backup.ListBackupVaultsInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.BackupVaultList,
				func(v backuptypes.BackupVaultListMember) *string { return v.BackupVaultName },
			), nil
		},
	}
}

func iotCase() regionCase {
	return regionCase{
		name: "iot",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := iot.NewFromConfig(cfg).CreateThing(ctx, &iot.CreateThingInput{ThingName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := iot.NewFromConfig(cfg).ListThings(ctx, &iot.ListThingsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Things, func(t iottypes.ThingAttribute) *string { return t.ThingName }), nil
		},
	}
}

func codecommitCase() regionCase {
	return regionCase{
		name: "codecommit",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := codecommit.NewFromConfig(cfg).CreateRepository(
				ctx, &codecommit.CreateRepositoryInput{RepositoryName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := codecommit.NewFromConfig(cfg).ListRepositories(ctx, &codecommit.ListRepositoriesInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.Repositories,
				func(r codecommittypes.RepositoryNameIdPair) *string { return r.RepositoryName },
			), nil
		},
	}
}

func s3Case() regionCase {
	return regionCase{
		name: "s3", uniqueNames: true,
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }).
				CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(name),
					CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
						LocationConstraint: s3types.BucketLocationConstraint(cfg.Region),
					},
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }).ListBuckets(
				ctx, &s3.ListBucketsInput{BucketRegion: aws.String(cfg.Region)})
			if err != nil {
				return nil, err
			}

			return strs(out.Buckets, func(b s3types.Bucket) *string { return b.Name }), nil
		},
	}
}

func iamCase() regionCase {
	return regionCase{
		name: "iam", global: true,
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := iam.NewFromConfig(cfg).CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := iam.NewFromConfig(cfg).ListUsers(ctx, &iam.ListUsersInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Users, func(u iamtypes.User) *string { return u.UserName }), nil
		},
	}
}

func route53Case() regionCase {
	return regionCase{
		name: "route53", global: true,
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := route53.NewFromConfig(cfg).CreateHostedZone(ctx, &route53.CreateHostedZoneInput{
				Name: aws.String(name + ".example.com"), CallerReference: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := route53.NewFromConfig(cfg).ListHostedZones(ctx, &route53.ListHostedZonesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.HostedZones, func(z route53types.HostedZone) *string { return z.Name }), nil
		},
	}
}

func stepfunctionsCase() regionCase {
	return regionCase{
		name: "stepfunctions",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := sfn.NewFromConfig(cfg).CreateStateMachine(ctx, &sfn.CreateStateMachineInput{
				Name:       aws.String(name),
				Definition: aws.String(`{"StartAt":"p","States":{"p":{"Type":"Pass","End":true}}}`),
				RoleArn:    aws.String("arn:aws:iam::000000000000:role/r"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := sfn.NewFromConfig(cfg).ListStateMachines(ctx, &sfn.ListStateMachinesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.StateMachines, func(m sfntypes.StateMachineListItem) *string { return m.Name }), nil
		},
	}
}

func kmsCase() regionCase {
	return regionCase{
		name: "kms",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			c := kms.NewFromConfig(cfg)

			k, err := c.CreateKey(ctx, &kms.CreateKeyInput{})
			if err != nil {
				return err
			}

			_, err = c.CreateAlias(ctx, &kms.CreateAliasInput{
				AliasName: aws.String("alias/" + name), TargetKeyId: k.KeyMetadata.KeyId,
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := kms.NewFromConfig(cfg).ListAliases(ctx, &kms.ListAliasesInput{})
			if err != nil {
				return nil, err
			}

			return tail(strs(out.Aliases, func(a kmstypes.AliasListEntry) *string { return a.AliasName }), "/"), nil
		},
	}
}

func cloudwatchCase() regionCase {
	return regionCase{
		name: "cloudwatch",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := cloudwatch.NewFromConfig(cfg).PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{
				AlarmName: aws.String(name), MetricName: aws.String("m"), Namespace: aws.String("n"),
				Statistic: "Average", Period: aws.Int32(60), EvaluationPeriods: aws.Int32(1),
				Threshold: aws.Float64(1), ComparisonOperator: "GreaterThanThreshold",
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := cloudwatch.NewFromConfig(cfg).DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.MetricAlarms, func(a cwtypes.MetricAlarm) *string { return a.AlarmName }), nil
		},
	}
}

func apigatewayCase() regionCase {
	return regionCase{
		name: "apigateway",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := apigateway.NewFromConfig(cfg).
				CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := apigateway.NewFromConfig(cfg).GetRestApis(ctx, &apigateway.GetRestApisInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Items, func(a apigatewaytypes.RestApi) *string { return a.Name }), nil
		},
	}
}

func apigatewayv2Case() regionCase {
	return regionCase{
		name: "apigatewayv2",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := apigatewayv2.NewFromConfig(cfg).CreateApi(ctx, &apigatewayv2.CreateApiInput{
				Name: aws.String(name), ProtocolType: "HTTP",
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := apigatewayv2.NewFromConfig(cfg).GetApis(ctx, &apigatewayv2.GetApisInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Items, func(a apigwv2types.Api) *string { return a.Name }), nil
		},
	}
}

func cognitoidpCase() regionCase {
	return regionCase{
		name: "cognitoidp",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := cognitoidentityprovider.NewFromConfig(cfg).CreateUserPool(
				ctx, &cognitoidentityprovider.CreateUserPoolInput{PoolName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := cognitoidentityprovider.NewFromConfig(cfg).ListUserPools(
				ctx, &cognitoidentityprovider.ListUserPoolsInput{MaxResults: aws.Int32(60)})
			if err != nil {
				return nil, err
			}

			return strs(out.UserPools, func(p cognitotypes.UserPoolDescriptionType) *string { return p.Name }), nil
		},
	}
}

func efsCase() regionCase {
	return regionCase{
		name: "efs",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := efs.NewFromConfig(cfg).CreateFileSystem(ctx, &efs.CreateFileSystemInput{
				CreationToken: aws.String(name),
				Tags:          []efstypes.Tag{{Key: aws.String("Name"), Value: aws.String(name)}},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := efs.NewFromConfig(cfg).DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.FileSystems,
				func(f efstypes.FileSystemDescription) *string { return f.CreationToken },
			), nil
		},
	}
}

func firehoseCase() regionCase {
	return regionCase{
		name: "firehose",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := firehose.NewFromConfig(cfg).CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
				DeliveryStreamName: aws.String(name),
				ExtendedS3DestinationConfiguration: &firehosetypes.ExtendedS3DestinationConfiguration{
					BucketARN: aws.String(
						"arn:aws:s3:::b",
					),
					RoleARN: aws.String("arn:aws:iam::000000000000:role/r"),
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := firehose.NewFromConfig(cfg).ListDeliveryStreams(ctx, &firehose.ListDeliveryStreamsInput{})

			return out.DeliveryStreamNames, err
		},
	}
}

func servicediscoveryCase() regionCase {
	return regionCase{
		name: "servicediscovery",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := servicediscovery.NewFromConfig(cfg).CreatePrivateDnsNamespace(
				ctx, &servicediscovery.CreatePrivateDnsNamespaceInput{Name: aws.String(name + ".local"), Vpc: aws.String("vpc-1")})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := servicediscovery.NewFromConfig(cfg).
				ListNamespaces(ctx, &servicediscovery.ListNamespacesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Namespaces, func(n sdtypes.NamespaceSummary) *string {
				return aws.String(strings.TrimSuffix(aws.ToString(n.Name), ".local"))
			}), nil
		},
	}
}

func elasticacheCase() regionCase {
	return regionCase{
		name: "elasticache",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := elasticache.NewFromConfig(cfg).
				CreateCacheSubnetGroup(ctx, &elasticache.CreateCacheSubnetGroupInput{
					CacheSubnetGroupName: aws.String(name), CacheSubnetGroupDescription: aws.String("d"),
					SubnetIds: []string{"subnet-1"},
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := elasticache.NewFromConfig(cfg).DescribeCacheSubnetGroups(
				ctx, &elasticache.DescribeCacheSubnetGroupsInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.CacheSubnetGroups,
				func(g elasticachetypes.CacheSubnetGroup) *string { return g.CacheSubnetGroupName },
			), nil
		},
	}
}

func sesv2Case() regionCase {
	return regionCase{
		name: "sesv2",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := sesv2.NewFromConfig(cfg).CreateEmailTemplate(ctx, &sesv2.CreateEmailTemplateInput{
				TemplateName:    aws.String(name),
				TemplateContent: &sesv2types.EmailTemplateContent{Subject: aws.String("s"), Text: aws.String("t")},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := sesv2.NewFromConfig(cfg).ListEmailTemplates(ctx, &sesv2.ListEmailTemplatesInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.TemplatesMetadata,
				func(m sesv2types.EmailTemplateMetadata) *string { return m.TemplateName },
			), nil
		},
	}
}

func autoscalingCase() regionCase {
	return regionCase{
		name: "autoscaling",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := autoscaling.NewFromConfig(cfg).CreateLaunchConfiguration(
				ctx, &autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: aws.String(name),
					ImageId:                 aws.String("ami-1"),
					InstanceType:            aws.String("t3.micro"),
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := autoscaling.NewFromConfig(cfg).DescribeLaunchConfigurations(
				ctx, &autoscaling.DescribeLaunchConfigurationsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.LaunchConfigurations, func(l astypes.LaunchConfiguration) *string {
				return l.LaunchConfigurationName
			}), nil
		},
	}
}

const cfnIsolationTemplate = `{"Resources":{"P":{"Type":"AWS::SSM::Parameter",` +
	`"Properties":{"Type":"String","Value":"v"}}}}`

func cloudformationCase() regionCase {
	return regionCase{
		name: "cloudformation",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := cloudformation.NewFromConfig(cfg).CreateStack(ctx, &cloudformation.CreateStackInput{
				StackName:    aws.String(name),
				TemplateBody: aws.String(cfnIsolationTemplate),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := cloudformation.NewFromConfig(cfg).DescribeStacks(ctx, &cloudformation.DescribeStacksInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Stacks, func(s cfntypes.Stack) *string { return s.StackName }), nil
		},
	}
}

func elbv2Case() regionCase {
	return regionCase{
		name: "elbv2",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := elbv2.NewFromConfig(cfg).CreateTargetGroup(ctx, &elbv2.CreateTargetGroupInput{
				Name:     aws.String(name),
				Protocol: elbv2types.ProtocolEnumHttp,
				Port:     aws.Int32(80),
				VpcId:    aws.String("vpc-1"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := elbv2.NewFromConfig(cfg).DescribeTargetGroups(ctx, &elbv2.DescribeTargetGroupsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.TargetGroups, func(g elbv2types.TargetGroup) *string { return g.TargetGroupName }), nil
		},
	}
}

func codedeployCase() regionCase {
	return regionCase{
		name: "codedeploy",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := codedeploy.NewFromConfig(cfg).CreateApplication(
				ctx, &codedeploy.CreateApplicationInput{ApplicationName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := codedeploy.NewFromConfig(cfg).ListApplications(ctx, &codedeploy.ListApplicationsInput{})

			return out.Applications, err
		},
	}
}

func lambdaCase() regionCase {
	return regionCase{
		name: "lambda",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := lambda.NewFromConfig(cfg).CreateFunction(ctx, &lambda.CreateFunctionInput{
				FunctionName: aws.String(name),
				Role:         aws.String("arn:aws:iam::000000000000:role/r"),
				PackageType:  lambdatypes.PackageTypeZip,
				Runtime:      lambdatypes.RuntimeNodejs20x,
				Handler:      aws.String("index.handler"),
				Code:         &lambdatypes.FunctionCode{ZipFile: []byte("PK")},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := lambda.NewFromConfig(cfg).ListFunctions(ctx, &lambda.ListFunctionsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Functions, func(f lambdatypes.FunctionConfiguration) *string { return f.FunctionName }), nil
		},
	}
}

func elbCase() regionCase {
	return regionCase{
		name: "elb",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := elb.NewFromConfig(cfg).CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{
				LoadBalancerName:  aws.String(name),
				AvailabilityZones: []string{cfg.Region + "a"},
				Listeners: []elbtypes.Listener{
					{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := elb.NewFromConfig(cfg).DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.LoadBalancerDescriptions, func(d elbtypes.LoadBalancerDescription) *string {
				return d.LoadBalancerName
			}), nil
		},
	}
}

func rdsCase() regionCase {
	return regionCase{
		name: "rds",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := rds.NewFromConfig(cfg).CreateDBSubnetGroup(ctx, &rds.CreateDBSubnetGroupInput{
				DBSubnetGroupName: aws.String(name), DBSubnetGroupDescription: aws.String("d"),
				SubnetIds: []string{"subnet-1", "subnet-2"},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := rds.NewFromConfig(cfg).DescribeDBSubnetGroups(ctx, &rds.DescribeDBSubnetGroupsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.DBSubnetGroups, func(g rdstypes.DBSubnetGroup) *string { return g.DBSubnetGroupName }), nil
		},
	}
}

func docdbCase() regionCase {
	return regionCase{
		name: "docdb",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := docdb.NewFromConfig(cfg).CreateDBSubnetGroup(ctx, &docdb.CreateDBSubnetGroupInput{
				DBSubnetGroupName: aws.String(name), DBSubnetGroupDescription: aws.String("d"),
				SubnetIds: []string{"subnet-1", "subnet-2"},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := docdb.NewFromConfig(cfg).DescribeDBSubnetGroups(ctx, &docdb.DescribeDBSubnetGroupsInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.DBSubnetGroups,
				func(g docdbtypes.DBSubnetGroup) *string { return g.DBSubnetGroupName },
			), nil
		},
	}
}

func neptuneCase() regionCase {
	return regionCase{
		name: "neptune",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := neptune.NewFromConfig(cfg).CreateDBSubnetGroup(ctx, &neptune.CreateDBSubnetGroupInput{
				DBSubnetGroupName: aws.String(name), DBSubnetGroupDescription: aws.String("d"),
				SubnetIds: []string{"subnet-1", "subnet-2"},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := neptune.NewFromConfig(cfg).DescribeDBSubnetGroups(ctx, &neptune.DescribeDBSubnetGroupsInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.DBSubnetGroups,
				func(g neptunetypes.DBSubnetGroup) *string { return g.DBSubnetGroupName },
			), nil
		},
	}
}

func opensearchCase() regionCase {
	return regionCase{
		name: "opensearch",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := opensearch.NewFromConfig(cfg).CreateDomain(ctx, &opensearch.CreateDomainInput{
				DomainName: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := opensearch.NewFromConfig(cfg).ListDomainNames(ctx, &opensearch.ListDomainNamesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.DomainNames, func(d opensearchtypes.DomainInfo) *string { return d.DomainName }), nil
		},
	}
}

func eksCase() regionCase {
	return regionCase{
		name: "eks",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := eks.NewFromConfig(cfg).CreateCluster(ctx, &eks.CreateClusterInput{
				Name:    aws.String(name),
				RoleArn: aws.String("arn:aws:iam::" + regionAccount + ":role/eks"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
					SubnetIds: []string{"subnet-1", "subnet-2"},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := eks.NewFromConfig(cfg).ListClusters(ctx, &eks.ListClustersInput{})
			if err != nil {
				return nil, err
			}

			return out.Clusters, nil
		},
	}
}

func kafkaCase() regionCase {
	return regionCase{
		name: "kafka",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := kafka.NewFromConfig(cfg).CreateConfiguration(ctx, &kafka.CreateConfigurationInput{
				Name: aws.String(name), KafkaVersions: []string{"3.6.0"},
				ServerProperties: []byte("auto.create.topics.enable=true"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := kafka.NewFromConfig(cfg).ListConfigurations(ctx, &kafka.ListConfigurationsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Configurations, func(c kafkatypes.Configuration) *string { return c.Name }), nil
		},
	}
}

func mqCase() regionCase {
	return regionCase{
		name: "mq",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := mq.NewFromConfig(cfg).CreateConfiguration(ctx, &mq.CreateConfigurationInput{
				Name: aws.String(name), EngineType: mqtypes.EngineTypeActivemq, EngineVersion: aws.String("5.17.6"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := mq.NewFromConfig(cfg).ListConfigurations(ctx, &mq.ListConfigurationsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Configurations, func(c mqtypes.Configuration) *string { return c.Name }), nil
		},
	}
}

func redshiftCase() regionCase {
	return regionCase{
		name: "redshift",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := redshift.NewFromConfig(cfg).CreateClusterSubnetGroup(ctx, &redshift.CreateClusterSubnetGroupInput{
				ClusterSubnetGroupName: aws.String(name), Description: aws.String("d"),
				SubnetIds: []string{"subnet-1", "subnet-2"},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := redshift.NewFromConfig(cfg).DescribeClusterSubnetGroups(
				ctx, &redshift.DescribeClusterSubnetGroupsInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.ClusterSubnetGroups,
				func(g redshifttypes.ClusterSubnetGroup) *string { return g.ClusterSubnetGroupName },
			), nil
		},
	}
}

func schedulerCase() regionCase {
	return regionCase{
		name: "scheduler",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := scheduler.NewFromConfig(cfg).CreateScheduleGroup(ctx, &scheduler.CreateScheduleGroupInput{
				Name: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := scheduler.NewFromConfig(cfg).ListScheduleGroups(ctx, &scheduler.ListScheduleGroupsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.ScheduleGroups, func(g schedulertypes.ScheduleGroupSummary) *string { return g.Name }), nil
		},
	}
}

func pipesCase() regionCase {
	return regionCase{
		name: "pipes",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := pipes.NewFromConfig(cfg).CreatePipe(ctx, &pipes.CreatePipeInput{
				Name:    aws.String(name),
				Source:  aws.String("arn:aws:sqs:" + cfg.Region + ":" + regionAccount + ":" + name + "-src"),
				Target:  aws.String("arn:aws:sqs:" + cfg.Region + ":" + regionAccount + ":" + name + "-dst"),
				RoleArn: aws.String("arn:aws:iam::" + regionAccount + ":role/pipe"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := pipes.NewFromConfig(cfg).ListPipes(ctx, &pipes.ListPipesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Pipes, func(p pipestypes.Pipe) *string { return p.Name }), nil
		},
	}
}

func appsyncCase() regionCase {
	return regionCase{
		name: "appsync",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := appsync.NewFromConfig(cfg).CreateGraphqlApi(ctx, &appsync.CreateGraphqlApiInput{
				Name: aws.String(name), AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := appsync.NewFromConfig(cfg).ListGraphqlApis(ctx, &appsync.ListGraphqlApisInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.GraphqlApis, func(a appsynctypes.GraphqlApi) *string { return a.Name }), nil
		},
	}
}

func cognitoidentityCase() regionCase {
	return regionCase{
		name: "cognitoidentity",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := cognitoidentity.NewFromConfig(cfg).
				CreateIdentityPool(ctx, &cognitoidentity.CreateIdentityPoolInput{
					IdentityPoolName: aws.String(name), AllowUnauthenticatedIdentities: true,
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := cognitoidentity.NewFromConfig(cfg).
				ListIdentityPools(ctx, &cognitoidentity.ListIdentityPoolsInput{
					MaxResults: aws.Int32(60),
				})
			if err != nil {
				return nil, err
			}

			return strs(out.IdentityPools, func(p cognitoidentitytypes.IdentityPoolShortDescription) *string {
				return p.IdentityPoolName
			}), nil
		},
	}
}

func sesCase() regionCase {
	return regionCase{
		name: "ses",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := ses.NewFromConfig(cfg).CreateConfigurationSet(ctx, &ses.CreateConfigurationSetInput{
				ConfigurationSet: &sestypes.ConfigurationSet{Name: aws.String(name)},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := ses.NewFromConfig(cfg).ListConfigurationSets(ctx, &ses.ListConfigurationSetsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.ConfigurationSets, func(c sestypes.ConfigurationSet) *string { return c.Name }), nil
		},
	}
}

func wafv2Case() regionCase {
	return regionCase{
		name: "wafv2",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := wafv2.NewFromConfig(cfg).CreateIPSet(ctx, &wafv2.CreateIPSetInput{
				Name: aws.String(name), Scope: wafv2types.ScopeRegional,
				IPAddressVersion: wafv2types.IPAddressVersionIpv4, Addresses: []string{"10.0.0.0/8"},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := wafv2.NewFromConfig(cfg).
				ListIPSets(ctx, &wafv2.ListIPSetsInput{Scope: wafv2types.ScopeRegional})
			if err != nil {
				return nil, err
			}

			return strs(out.IPSets, func(s wafv2types.IPSetSummary) *string { return s.Name }), nil
		},
	}
}

package app

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/require"
)

func kinesisInventoryCase() inventoryCase {
	return inventoryCase{
		name: "kinesis",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			c := kinesis.NewFromConfig(cfg)
			_, err := c.CreateStream(ctx, &kinesis.CreateStreamInput{
				StreamName: aws.String(name), ShardCount: aws.Int32(1), Tags: inventoryTagMap(tagged),
			})
			require.NoError(t, err)

			out, err := c.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
			require.NoError(t, err)

			return aws.ToString(out.StreamDescriptionSummary.StreamARN)
		},
	}
}

func schedulerInventoryCase() inventoryCase {
	return inventoryCase{
		name: "scheduler",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &scheduler.CreateScheduleGroupInput{Name: aws.String(name)}
			if tagged {
				in.Tags = []schedulertypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := scheduler.NewFromConfig(cfg).CreateScheduleGroup(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.ScheduleGroupArn)
		},
	}
}

func ecrInventoryCase() inventoryCase {
	return inventoryCase{
		name: "ecr",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &ecr.CreateRepositoryInput{RepositoryName: aws.String(name)}
			if tagged {
				in.Tags = []ecrtypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := ecr.NewFromConfig(cfg).CreateRepository(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.Repository.RepositoryArn)
		},
	}
}

func athenaInventoryCase() inventoryCase {
	return inventoryCase{
		name: "athena",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &athena.CreateWorkGroupInput{Name: aws.String(name)}
			if tagged {
				in.Tags = []athenatypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			_, err := athena.NewFromConfig(cfg).CreateWorkGroup(ctx, in)
			require.NoError(t, err)

			return inventoryARN("athena", "workgroup/"+name)
		},
	}
}

func codecommitInventoryCase() inventoryCase {
	return inventoryCase{
		name: "codecommit",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			out, err := codecommit.NewFromConfig(cfg).CreateRepository(ctx, &codecommit.CreateRepositoryInput{
				RepositoryName: aws.String(name), Tags: inventoryTagMap(tagged),
			})
			require.NoError(t, err)

			return aws.ToString(out.RepositoryMetadata.Arn)
		},
	}
}

func glueInventoryCase() inventoryCase {
	return inventoryCase{
		name: "glue",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			_, err := glue.NewFromConfig(cfg).CreateDatabase(ctx, &glue.CreateDatabaseInput{
				DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String(name)}, Tags: inventoryTagMap(tagged),
			})
			require.NoError(t, err)

			return inventoryARN("glue", "database/"+name)
		},
	}
}

func sfnInventoryCase() inventoryCase {
	return inventoryCase{
		name: "stepfunctions",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &sfn.CreateStateMachineInput{
				Name:       aws.String(name),
				Definition: aws.String(`{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`),
				RoleArn:    aws.String(inventoryARN("iam", "role/sfn")),
			}
			if tagged {
				in.Tags = []sfntypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := sfn.NewFromConfig(cfg).CreateStateMachine(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.StateMachineArn)
		},
	}
}

func appconfigInventoryCase() inventoryCase {
	return inventoryCase{
		name: "appconfig",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			out, err := appconfig.NewFromConfig(cfg).CreateApplication(ctx, &appconfig.CreateApplicationInput{
				Name: aws.String(name), Tags: inventoryTagMap(tagged),
			})
			require.NoError(t, err)

			return inventoryARN("appconfig", "application/"+aws.ToString(out.Id))
		},
	}
}

func efsInventoryCase() inventoryCase {
	return inventoryCase{
		name: "efs",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &efs.CreateFileSystemInput{CreationToken: aws.String(name)}
			if tagged {
				in.Tags = []efstypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := efs.NewFromConfig(cfg).CreateFileSystem(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.FileSystemArn)
		},
	}
}

func backupInventoryCase() inventoryCase {
	return inventoryCase{
		name: "backup",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			out, err := backup.NewFromConfig(cfg).CreateBackupVault(ctx, &backup.CreateBackupVaultInput{
				BackupVaultName: aws.String(name), BackupVaultTags: inventoryTagMap(tagged),
			})
			require.NoError(t, err)

			return aws.ToString(out.BackupVaultArn)
		},
	}
}

func inventoryCases() []inventoryCase {
	return []inventoryCase{
		kinesisInventoryCase(),
		schedulerInventoryCase(),
		ecrInventoryCase(),
		athenaInventoryCase(),
		codecommitInventoryCase(),
		glueInventoryCase(),
		sfnInventoryCase(),
		appconfigInventoryCase(),
		efsInventoryCase(),
		backupInventoryCase(),
		apigatewayInventoryCase(),
		apigatewayv2InventoryCase(),
		acmInventoryCase(),
		opensearchInventoryCase(),
		redshiftInventoryCase(),
		mqInventoryCase(),
		cognitoInventoryCase(),
		wafv2InventoryCase(),
		appsyncInventoryCase(),
		transferInventoryCase(),
		xrayInventoryCase(),
		personalizeInventoryCase(),
		iotInventoryCase(),
		docdbInventoryCase(),
		neptuneInventoryCase(),
		ramInventoryCase(),
		kafkaInventoryCase(),
		timestreamwriteInventoryCase(),
		lightsailInventoryCase(),
	}
}

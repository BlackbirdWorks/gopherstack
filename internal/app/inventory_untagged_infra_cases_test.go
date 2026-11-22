package app

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/docdb"
	docdbtypes "github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/aws/aws-sdk-go-v2/service/neptune"
	neptunetypes "github.com/aws/aws-sdk-go-v2/service/neptune/types"
	"github.com/aws/aws-sdk-go-v2/service/ram"
	ramtypes "github.com/aws/aws-sdk-go-v2/service/ram/types"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	timestreamwritetypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/stretchr/testify/require"
)

func iotInventoryCase() inventoryCase {
	return inventoryCase{
		name: "iot",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &iot.CreateThingTypeInput{ThingTypeName: aws.String(name)}
			if tagged {
				in.Tags = []iottypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := iot.NewFromConfig(cfg).CreateThingType(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.ThingTypeArn)
		},
	}
}

func docdbInventoryCase() inventoryCase {
	return inventoryCase{
		name: "docdb",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &docdb.CreateDBClusterInput{
				DBClusterIdentifier: aws.String(name), Engine: aws.String("docdb"),
				MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Passw0rd1234"),
			}
			if tagged {
				in.Tags = []docdbtypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := docdb.NewFromConfig(cfg).CreateDBCluster(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.DBCluster.DBClusterArn)
		},
	}
}

func neptuneInventoryCase() inventoryCase {
	return inventoryCase{
		name: "neptune",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &neptune.CreateDBClusterInput{DBClusterIdentifier: aws.String(name), Engine: aws.String("neptune")}
			if tagged {
				in.Tags = []neptunetypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := neptune.NewFromConfig(cfg).CreateDBCluster(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.DBCluster.DBClusterArn)
		},
	}
}

func ramInventoryCase() inventoryCase {
	return inventoryCase{
		name: "ram",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &ram.CreateResourceShareInput{Name: aws.String(name)}
			if tagged {
				in.Tags = []ramtypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := ram.NewFromConfig(cfg).CreateResourceShare(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.ResourceShare.ResourceShareArn)
		},
	}
}

func kafkaInventoryCase() inventoryCase {
	return inventoryCase{
		name: "kafka",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			out, err := kafka.NewFromConfig(cfg).CreateConfiguration(ctx, &kafka.CreateConfigurationInput{
				Name: aws.String(name), KafkaVersions: []string{"3.5.1"},
				ServerProperties: []byte("auto.create.topics.enable=true"),
			})
			require.NoError(t, err)

			arnStr := aws.ToString(out.Arn)
			if tagged {
				_, err = kafka.NewFromConfig(cfg).TagResource(ctx, &kafka.TagResourceInput{
					ResourceArn: aws.String(arnStr), Tags: inventoryTagMap(true),
				})
				require.NoError(t, err)
			}

			return arnStr
		},
	}
}

func timestreamwriteInventoryCase() inventoryCase {
	return inventoryCase{
		name: "timestreamwrite",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &timestreamwrite.CreateDatabaseInput{DatabaseName: aws.String(name)}
			if tagged {
				in.Tags = []timestreamwritetypes.Tag{
					{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)},
				}
			}

			out, err := timestreamwrite.NewFromConfig(cfg).CreateDatabase(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.Database.Arn)
		},
	}
}

func lightsailInventoryCase() inventoryCase {
	return inventoryCase{
		name: "lightsail",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &lightsail.CreateKeyPairInput{KeyPairName: aws.String(name)}
			if tagged {
				in.Tags = []lightsailtypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := lightsail.NewFromConfig(cfg).CreateKeyPair(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.KeyPair.Arn)
		},
	}
}

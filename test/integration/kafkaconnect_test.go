package integration_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	kctypes "github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hasAPIErrorCode reports whether err is a smithy API error carrying code.
func hasAPIErrorCode(err error, code string) bool {
	var apiErr smithy.APIError

	return errors.As(err, &apiErr) && apiErr.ErrorCode() == code
}

func createKafkaConnectClient(t *testing.T) *kafkaconnectsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err, "unable to load SDK config")

	return kafkaconnectsdk.NewFromConfig(cfg, func(o *kafkaconnectsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_KafkaConnect_ConnectorLifecycle drives plugin, worker config and connector
// through the real SDK client, the full connector state machine and the final not-found error.
func TestIntegration_KafkaConnect_ConnectorLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createKafkaConnectClient(t)
	ctx := t.Context()
	suffix := uuid.NewString()[:8]

	s3Client := createS3Client(t)
	bucket := "it-kc-bucket-" + suffix
	pluginBytes := []byte("it-plugin-archive")

	_, err := s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = s3Client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("plugin.zip"),
		Body:   bytes.NewReader(pluginBytes),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = s3Client.DeleteObject(cleanupCtx, &awss3.DeleteObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("plugin.zip"),
		})
		_, _ = s3Client.DeleteBucket(cleanupCtx, &awss3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	plugin, err := client.CreateCustomPlugin(ctx, &kafkaconnectsdk.CreateCustomPluginInput{
		Name:        aws.String("it-plugin-" + suffix),
		ContentType: kctypes.CustomPluginContentTypeZip,
		Location: &kctypes.CustomPluginLocation{S3Location: &kctypes.S3Location{
			BucketArn: aws.String("arn:aws:s3:::" + bucket),
			FileKey:   aws.String("plugin.zip"),
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, kctypes.CustomPluginStateCreating, plugin.CustomPluginState)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteCustomPlugin(cleanupCtx, &kafkaconnectsdk.DeleteCustomPluginInput{
			CustomPluginArn: plugin.CustomPluginArn,
		})
	})

	require.Eventually(t, func() bool {
		out, descErr := client.DescribeCustomPlugin(ctx, &kafkaconnectsdk.DescribeCustomPluginInput{
			CustomPluginArn: plugin.CustomPluginArn,
		})
		if descErr != nil || out.CustomPluginState != kctypes.CustomPluginStateActive {
			return false
		}

		sum := md5.Sum(pluginBytes)

		return aws.ToString(out.LatestRevision.FileDescription.FileMd5) == hex.EncodeToString(sum[:]) &&
			out.LatestRevision.FileDescription.FileSize == int64(len(pluginBytes))
	}, 15*time.Second, 100*time.Millisecond)

	worker, err := client.CreateWorkerConfiguration(ctx, &kafkaconnectsdk.CreateWorkerConfigurationInput{
		Name: aws.String("it-worker-" + suffix),
		PropertiesFileContent: aws.String(base64.StdEncoding.EncodeToString(
			[]byte("key.converter=org.apache.kafka.connect.json.JsonConverter\n"),
		)),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteWorkerConfiguration(cleanupCtx, &kafkaconnectsdk.DeleteWorkerConfigurationInput{
			WorkerConfigurationArn: worker.WorkerConfigurationArn,
		})
	})

	connectorName := "it-connector-" + suffix

	created, err := client.CreateConnector(ctx, &kafkaconnectsdk.CreateConnectorInput{
		ConnectorName: aws.String(connectorName),
		Capacity: &kctypes.Capacity{
			ProvisionedCapacity: &kctypes.ProvisionedCapacity{McuCount: 1, WorkerCount: 1},
		},
		ConnectorConfiguration: map[string]string{"connector.class": "com.example.Sink", "tasks.max": "1"},
		KafkaCluster: &kctypes.KafkaCluster{ApacheKafkaCluster: &kctypes.ApacheKafkaCluster{
			BootstrapServers: aws.String("b-1.it:9092"),
			Vpc: &kctypes.Vpc{
				SecurityGroups: []string{"sg-1"},
				Subnets:        []string{"subnet-1"},
			},
		}},
		KafkaClusterClientAuthentication: &kctypes.KafkaClusterClientAuthentication{
			AuthenticationType: kctypes.KafkaClusterClientAuthenticationTypeNone,
		},
		KafkaClusterEncryptionInTransit: &kctypes.KafkaClusterEncryptionInTransit{
			EncryptionType: kctypes.KafkaClusterEncryptionInTransitTypePlaintext,
		},
		KafkaConnectVersion:     aws.String("2.7.1"),
		ServiceExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/it-connect"),
		Plugins: []kctypes.Plugin{{
			CustomPlugin: &kctypes.CustomPlugin{CustomPluginArn: plugin.CustomPluginArn, Revision: 1},
		}},
		WorkerConfiguration: &kctypes.WorkerConfiguration{
			WorkerConfigurationArn: worker.WorkerConfigurationArn,
			Revision:               1,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, kctypes.ConnectorStateCreating, created.ConnectorState)

	waitRunning := func() *kafkaconnectsdk.DescribeConnectorOutput {
		var out *kafkaconnectsdk.DescribeConnectorOutput

		require.Eventually(t, func() bool {
			var descErr error

			out, descErr = client.DescribeConnector(ctx, &kafkaconnectsdk.DescribeConnectorInput{
				ConnectorArn: created.ConnectorArn,
			})

			return descErr == nil && out.ConnectorState == kctypes.ConnectorStateRunning
		}, 15*time.Second, 100*time.Millisecond)

		return out
	}

	described := waitRunning()
	assert.Equal(t, connectorName, aws.ToString(described.ConnectorName))
	assert.Equal(t, "b-1.it:9092", aws.ToString(described.KafkaCluster.ApacheKafkaCluster.BootstrapServers))

	listed, err := client.ListConnectors(ctx, &kafkaconnectsdk.ListConnectorsInput{
		ConnectorNamePrefix: aws.String(connectorName),
	})
	require.NoError(t, err)
	require.Len(t, listed.Connectors, 1)

	updated, err := client.UpdateConnector(ctx, &kafkaconnectsdk.UpdateConnectorInput{
		ConnectorArn:   created.ConnectorArn,
		CurrentVersion: described.CurrentVersion,
		Capacity: &kctypes.CapacityUpdate{
			ProvisionedCapacity: &kctypes.ProvisionedCapacityUpdate{McuCount: 2, WorkerCount: 2},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, kctypes.ConnectorStateUpdating, updated.ConnectorState)

	described = waitRunning()
	assert.EqualValues(t, 2, described.Capacity.ProvisionedCapacity.WorkerCount)

	op, err := client.DescribeConnectorOperation(ctx, &kafkaconnectsdk.DescribeConnectorOperationInput{
		ConnectorOperationArn: updated.ConnectorOperationArn,
	})
	require.NoError(t, err)
	assert.Equal(t, kctypes.ConnectorOperationStateUpdateComplete, op.ConnectorOperationState)

	restarted, err := client.RestartConnector(ctx, &kafkaconnectsdk.RestartConnectorInput{
		ConnectorArn: created.ConnectorArn,
	})
	require.NoError(t, err)
	waitRunning()

	ops, err := client.ListConnectorOperations(ctx, &kafkaconnectsdk.ListConnectorOperationsInput{
		ConnectorArn: created.ConnectorArn,
	})
	require.NoError(t, err)
	assert.Len(t, ops.ConnectorOperations, 2)
	assert.NotEmpty(t, aws.ToString(restarted.ConnectorOperationArn))

	_, err = client.TagResource(ctx, &kafkaconnectsdk.TagResourceInput{
		ResourceArn: created.ConnectorArn,
		Tags:        map[string]string{"env": "it"},
	})
	require.NoError(t, err)

	tags, err := client.ListTagsForResource(ctx, &kafkaconnectsdk.ListTagsForResourceInput{
		ResourceArn: created.ConnectorArn,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "it"}, tags.Tags)

	deleted, err := client.DeleteConnector(
		ctx, &kafkaconnectsdk.DeleteConnectorInput{ConnectorArn: created.ConnectorArn},
	)
	require.NoError(t, err)
	assert.Equal(t, kctypes.ConnectorStateDeleting, deleted.ConnectorState)

	require.Eventually(t, func() bool {
		_, descErr := client.DescribeConnector(ctx, &kafkaconnectsdk.DescribeConnectorInput{
			ConnectorArn: created.ConnectorArn,
		})

		return hasAPIErrorCode(descErr, "NotFoundException")
	}, 15*time.Second, 100*time.Millisecond)
}

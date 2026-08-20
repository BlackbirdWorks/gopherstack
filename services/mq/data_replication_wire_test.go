package mq_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mqsdk "github.com/aws/aws-sdk-go-v2/service/mq"
	mqtypes "github.com/aws/aws-sdk-go-v2/service/mq/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// TestCreateBroker_DataReplicationCounterpart_SDKRoundTrip proves
// DataReplicationMetadata.DataReplicationCounterpart decodes through the
// real SDK client as the nested {brokerId, region} object
// aws-sdk-go-v2/service/mq/types.DataReplicationMetadataOutput actually
// declares (types/types.go, deserializers.go's
// awsRestjson1_deserializeDocumentDataReplicationCounterpart requires a JSON
// object, not a string). Before the fix, gopherstack emitted the raw
// dataReplicationPrimaryBrokerArn as a bare JSON string in this slot, which
// fails the ENTIRE DescribeBroker/UpdateBroker deserialization for any real
// client with a smithy.DeserializationError ("unexpected JSON type").
func TestCreateBroker_DataReplicationCounterpart_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
	client := newTestMQClient(t, mq.NewHandler(backend))

	const primaryArn = "arn:aws:mq:us-west-2:000000000000:broker:primary-broker"

	out, err := client.CreateBroker(t.Context(), &mqsdk.CreateBrokerInput{
		BrokerName:         aws.String("replica-broker"),
		EngineType:         "ACTIVEMQ",
		EngineVersion:      aws.String("5.15.14"),
		HostInstanceType:   aws.String("mq.t3.micro"),
		DeploymentMode:     "SINGLE_INSTANCE",
		PubliclyAccessible: aws.Bool(false),
		Users: []mqtypes.User{
			{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")},
		},
		DataReplicationMode:             mqtypes.DataReplicationModeCrdr,
		DataReplicationPrimaryBrokerArn: aws.String(primaryArn),
	})
	require.NoError(t, err)

	brokerID := aws.ToString(out.BrokerId)

	described, err := client.DescribeBroker(t.Context(), &mqsdk.DescribeBrokerInput{BrokerId: aws.String(brokerID)})
	require.NoError(t, err, "DescribeBroker must decode dataReplicationMetadata cleanly through the real SDK client")
	require.NotNil(t, described.DataReplicationMetadata)
	assert.Equal(t, "REPLICA", aws.ToString(described.DataReplicationMetadata.DataReplicationRole))

	counterpart := described.DataReplicationMetadata.DataReplicationCounterpart
	require.NotNil(t, counterpart, "dataReplicationCounterpart must decode as a nested object")
	assert.Equal(t, "us-west-2", aws.ToString(counterpart.Region))
	assert.Equal(t, "primary-broker", aws.ToString(counterpart.BrokerId))
}

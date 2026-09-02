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

// TestCreateBroker_StorageSize_SDKRoundTrip proves CreateBrokerInput.StorageSize
// (api_op_CreateBroker.go) is stored and echoed back on DescribeBroker.StorageSize
// (api_op_DescribeBroker.go), and that UpdateBroker.StorageSize stages into
// DescribeBrokerOutput.PendingStorageSize until a reboot promotes it -- both
// were previously silently dropped in every direction (no slot in
// CreateBrokerOptions/brokerResponse/updateBrokerResponse at all).
func TestCreateBroker_StorageSize_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
	client := newTestMQClient(t, mq.NewHandler(backend))

	out, err := client.CreateBroker(t.Context(), &mqsdk.CreateBrokerInput{
		BrokerName:         aws.String("storage-broker"),
		EngineType:         "ACTIVEMQ",
		EngineVersion:      aws.String("5.15.14"),
		HostInstanceType:   aws.String("mq.t3.micro"),
		DeploymentMode:     "SINGLE_INSTANCE",
		PubliclyAccessible: aws.Bool(false),
		StorageSize:        aws.Int32(500),
		Users: []mqtypes.User{
			{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")},
		},
	})
	require.NoError(t, err)

	brokerID := aws.ToString(out.BrokerId)

	described, err := client.DescribeBroker(
		t.Context(),
		&mqsdk.DescribeBrokerInput{BrokerId: aws.String(brokerID)},
	)
	require.NoError(t, err)
	require.NotNil(t, described.StorageSize, "storageSize must round-trip through DescribeBroker")
	assert.Equal(t, int32(500), aws.ToInt32(described.StorageSize))

	updated, err := client.UpdateBroker(t.Context(), &mqsdk.UpdateBrokerInput{
		BrokerId:    aws.String(brokerID),
		StorageSize: aws.Int32(1000),
	})
	require.NoError(t, err)
	require.NotNil(
		t,
		updated.StorageSize,
		"UpdateBrokerOutput.storageSize must echo the staged target size",
	)
	assert.Equal(t, int32(1000), aws.ToInt32(updated.StorageSize))

	describedAfterUpdate, err := client.DescribeBroker(
		t.Context(),
		&mqsdk.DescribeBrokerInput{BrokerId: aws.String(brokerID)},
	)
	require.NoError(t, err)
	require.NotNil(
		t,
		describedAfterUpdate.PendingStorageSize,
		"the new size must stage as pending until reboot",
	)
	assert.Equal(t, int32(1000), aws.ToInt32(describedAfterUpdate.PendingStorageSize))
	assert.Equal(
		t,
		int32(500),
		aws.ToInt32(describedAfterUpdate.StorageSize),
		"current size stays until reboot",
	)

	_, err = client.RebootBroker(
		t.Context(),
		&mqsdk.RebootBrokerInput{BrokerId: aws.String(brokerID)},
	)
	require.NoError(t, err)

	// Reboot promotion is observed lazily: the first post-reboot Describe
	// sees REBOOT_IN_PROGRESS and promotes server-side; the second sees the
	// settled RUNNING state. See TestRebootBroker_StateTransition.
	_, err = client.DescribeBroker(
		t.Context(),
		&mqsdk.DescribeBrokerInput{BrokerId: aws.String(brokerID)},
	)
	require.NoError(t, err)

	describedAfterReboot, err := client.DescribeBroker(
		t.Context(),
		&mqsdk.DescribeBrokerInput{BrokerId: aws.String(brokerID)},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		int32(1000),
		aws.ToInt32(describedAfterReboot.StorageSize),
		"pending size promotes to current on reboot",
	)
	assert.Nil(t, describedAfterReboot.PendingStorageSize)
}

// TestUpdateBroker_ResourceShareArns_SDKRoundTrip proves UpdateBrokerInput.ResourceShareArns
// (api_op_UpdateBroker.go: "The list of resource shares to update on the broker")
// is accepted and echoed back on UpdateBrokerOutput.ResourceShareArns ("The
// pending broker's target list of resource shares") -- previously the field
// had no slot in updateBrokerInput/updateBrokerResponse at all and was
// silently dropped.
func TestUpdateBroker_ResourceShareArns_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
	client := newTestMQClient(t, mq.NewHandler(backend))

	out, err := client.CreateBroker(t.Context(), &mqsdk.CreateBrokerInput{
		BrokerName:         aws.String("resource-share-broker"),
		EngineType:         "ACTIVEMQ",
		EngineVersion:      aws.String("5.15.14"),
		HostInstanceType:   aws.String("mq.t3.micro"),
		DeploymentMode:     "SINGLE_INSTANCE",
		PubliclyAccessible: aws.Bool(false),
		Users: []mqtypes.User{
			{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")},
		},
	})
	require.NoError(t, err)

	brokerID := aws.ToString(out.BrokerId)

	shareArn := "arn:aws:ram:us-west-2:000000000000:resource-share/abc-123"

	updated, err := client.UpdateBroker(t.Context(), &mqsdk.UpdateBrokerInput{
		BrokerId:          aws.String(brokerID),
		ResourceShareArns: []string{shareArn},
	})
	require.NoError(t, err)
	require.Len(
		t,
		updated.ResourceShareArns,
		1,
		"resourceShareArns must round-trip through UpdateBroker",
	)
	assert.Equal(t, shareArn, updated.ResourceShareArns[0])
}

// TestCreateConfiguration_AuthenticationStrategy_SDKRoundTrip proves
// CreateConfigurationInput.AuthenticationStrategy (api_op_CreateConfiguration.go)
// is stored and echoed on CreateConfiguration/DescribeConfiguration/
// ListConfigurations -- Configuration (types/types.go) declares it a required
// member on all three outputs, but gopherstack's Configuration model had no
// slot for it at all.
func TestCreateConfiguration_AuthenticationStrategy_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
	client := newTestMQClient(t, mq.NewHandler(backend))

	created, err := client.CreateConfiguration(t.Context(), &mqsdk.CreateConfigurationInput{
		Name:                   aws.String("ldap-config"),
		EngineType:             "ACTIVEMQ",
		EngineVersion:          aws.String("5.15.14"),
		AuthenticationStrategy: mqtypes.AuthenticationStrategyLdap,
	})
	require.NoError(t, err)
	assert.Equal(t, mqtypes.AuthenticationStrategyLdap, created.AuthenticationStrategy,
		"CreateConfigurationOutput.authenticationStrategy must echo the request")

	described, err := client.DescribeConfiguration(t.Context(), &mqsdk.DescribeConfigurationInput{
		ConfigurationId: aws.String(aws.ToString(created.Id)),
	})
	require.NoError(t, err)
	assert.Equal(t, mqtypes.AuthenticationStrategyLdap, described.AuthenticationStrategy)

	listed, err := client.ListConfigurations(t.Context(), &mqsdk.ListConfigurationsInput{})
	require.NoError(t, err)

	var found bool

	for _, cfg := range listed.Configurations {
		if aws.ToString(cfg.Id) == aws.ToString(created.Id) {
			found = true

			assert.Equal(t, mqtypes.AuthenticationStrategyLdap, cfg.AuthenticationStrategy)
		}
	}

	assert.True(t, found, "created configuration must appear in ListConfigurations")
}

// TestCreateConfiguration_DefaultAuthenticationStrategy proves a CreateConfiguration
// call that omits authenticationStrategy defaults to SIMPLE, matching the
// pinned SDK's doc ("Optional. ... The default is SIMPLE.").
func TestCreateConfiguration_DefaultAuthenticationStrategy(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
	client := newTestMQClient(t, mq.NewHandler(backend))

	created, err := client.CreateConfiguration(t.Context(), &mqsdk.CreateConfigurationInput{
		Name:          aws.String("default-auth-config"),
		EngineType:    "ACTIVEMQ",
		EngineVersion: aws.String("5.15.14"),
	})
	require.NoError(t, err)
	assert.Equal(t, mqtypes.AuthenticationStrategySimple, created.AuthenticationStrategy)
}

// TestUpdateConfiguration_Created_SDKRoundTrip proves UpdateConfigurationOutput.Created
// (api_op_UpdateConfiguration.go, "Required. The date and time of the
// configuration.") is emitted -- previously the handler's response map had
// no "created" key at all.
func TestUpdateConfiguration_Created_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
	client := newTestMQClient(t, mq.NewHandler(backend))

	created, err := client.CreateConfiguration(t.Context(), &mqsdk.CreateConfigurationInput{
		Name:          aws.String("update-created-config"),
		EngineType:    "ACTIVEMQ",
		EngineVersion: aws.String("5.15.14"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateConfiguration(t.Context(), &mqsdk.UpdateConfigurationInput{
		ConfigurationId: created.Id,
		Data:            aws.String("PGJyb2tlcj48L2Jyb2tlcj4="),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Created, "UpdateConfigurationOutput.created must be emitted")
	assert.False(t, updated.Created.IsZero())
}

// TestDescribeBroker_BrokerInstanceIpAddress_SDKRoundTrip proves
// BrokerInstance.IpAddress (types/types.go, confirmed present in
// deserializers.go's awsRestjson1_deserializeDocumentBrokerInstance case
// list) is populated for ActiveMQ brokers -- it was previously missing from
// gopherstack's BrokerInstance struct entirely.
func TestDescribeBroker_BrokerInstanceIpAddress_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
	client := newTestMQClient(t, mq.NewHandler(backend))

	out, err := client.CreateBroker(t.Context(), &mqsdk.CreateBrokerInput{
		BrokerName:         aws.String("ip-broker"),
		EngineType:         "ACTIVEMQ",
		EngineVersion:      aws.String("5.15.14"),
		HostInstanceType:   aws.String("mq.t3.micro"),
		DeploymentMode:     "SINGLE_INSTANCE",
		PubliclyAccessible: aws.Bool(false),
		Users: []mqtypes.User{
			{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeBroker(
		t.Context(),
		&mqsdk.DescribeBrokerInput{BrokerId: out.BrokerId},
	)
	require.NoError(t, err)
	require.Len(t, described.BrokerInstances, 1)
	assert.NotEmpty(t, aws.ToString(described.BrokerInstances[0].IpAddress),
		"BrokerInstance.ipAddress must be populated for an ActiveMQ broker")
}

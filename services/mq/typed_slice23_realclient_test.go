package mq_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mqsdk "github.com/aws/aws-sdk-go-v2/service/mq"
	mqtypes "github.com/aws/aws-sdk-go-v2/service/mq/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// TestTypedSlice23RealClient drives mq's remaining typed-coverage-blind ops
// (gopherstack-n3zi slice 23) through the real aws-sdk-go-v2 client:
// CreateTags, CreateUser, DeleteConfiguration, DeleteTags, DeleteUser,
// DescribeBrokerEngineTypes, DescribeBrokerInstanceOptions,
// DescribeConfigurationRevision, DescribeSharedResources, DescribeUser,
// ListUsers, Promote, UpdateUser.
func TestTypedSlice23RealClient(t *testing.T) {
	t.Parallel()

	t.Run("tags CRUD", func(t *testing.T) {
		t.Parallel()

		backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
		client := newTestMQClient(t, mq.NewHandler(backend))
		ctx := t.Context()

		out, err := client.CreateBroker(ctx, &mqsdk.CreateBrokerInput{
			BrokerName: aws.String("s23-tags-broker"), EngineType: "ACTIVEMQ",
			EngineVersion: aws.String("5.15.14"), HostInstanceType: aws.String("mq.t3.micro"),
			DeploymentMode: "SINGLE_INSTANCE", PubliclyAccessible: aws.Bool(false),
			Users: []mqtypes.User{{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")}},
		})
		require.NoError(t, err)
		brokerArn := aws.ToString(out.BrokerArn)

		_, err = client.CreateTags(ctx, &mqsdk.CreateTagsInput{
			ResourceArn: aws.String(brokerArn), Tags: map[string]string{"team": "platform"},
		})
		require.NoError(t, err)

		tagsOut, err := client.ListTags(ctx, &mqsdk.ListTagsInput{ResourceArn: aws.String(brokerArn)})
		require.NoError(t, err)
		assert.Equal(t, "platform", tagsOut.Tags["team"])

		_, err = client.DeleteTags(ctx, &mqsdk.DeleteTagsInput{
			ResourceArn: aws.String(brokerArn), TagKeys: []string{"team"},
		})
		require.NoError(t, err)

		tagsOut2, err := client.ListTags(ctx, &mqsdk.ListTagsInput{ResourceArn: aws.String(brokerArn)})
		require.NoError(t, err)
		assert.NotContains(t, tagsOut2.Tags, "team")
	})

	t.Run("user CRUD", func(t *testing.T) {
		t.Parallel()

		backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
		client := newTestMQClient(t, mq.NewHandler(backend))
		ctx := t.Context()

		brokerOut, err := client.CreateBroker(ctx, &mqsdk.CreateBrokerInput{
			BrokerName: aws.String("s23-users-broker"), EngineType: "ACTIVEMQ",
			EngineVersion: aws.String("5.15.14"), HostInstanceType: aws.String("mq.t3.micro"),
			DeploymentMode: "SINGLE_INSTANCE", PubliclyAccessible: aws.Bool(false),
			Users: []mqtypes.User{{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")}},
		})
		require.NoError(t, err)
		brokerID := aws.ToString(brokerOut.BrokerId)

		_, err = client.CreateUser(ctx, &mqsdk.CreateUserInput{
			BrokerId: aws.String(brokerID), Username: aws.String("alice"),
			Password: aws.String("anothersecretpassword1"), Groups: []string{"admins"},
		})
		require.NoError(t, err)

		// A newly created ActiveMQ user is inserted live but marked
		// Pending.PendingChange=CREATE until the next reboot (users.go's
		// CreateUser doc comment, mirroring
		// aws-sdk-go-v2/service/mq/types.UserPendingChanges).
		descOut, err := client.DescribeUser(ctx, &mqsdk.DescribeUserInput{
			BrokerId: aws.String(brokerID), Username: aws.String("alice"),
		})
		require.NoError(t, err)
		assert.Equal(t, "alice", aws.ToString(descOut.Username))
		assert.Equal(t, []string{"admins"}, descOut.Groups)
		require.NotNil(t, descOut.Pending)
		assert.Equal(t, mqtypes.ChangeTypeCreate, descOut.Pending.PendingChange)

		listOut, err := client.ListUsers(ctx, &mqsdk.ListUsersInput{BrokerId: aws.String(brokerID)})
		require.NoError(t, err)

		names := make([]string, 0, len(listOut.Users))
		for _, u := range listOut.Users {
			names = append(names, aws.ToString(u.Username))
		}

		assert.Contains(t, names, "alice")
		assert.Contains(t, names, "admin")

		_, err = client.UpdateUser(ctx, &mqsdk.UpdateUserInput{
			BrokerId: aws.String(brokerID), Username: aws.String("alice"),
			Groups: []string{"admins", "ops"}, ConsoleAccess: aws.Bool(true),
		})
		require.NoError(t, err)

		// UpdateUser's Console/Groups changes stage into Pending and do not
		// take effect until the broker reboots (users.go's UpdateUser doc
		// comment) -- the live Groups value must still read the pre-update
		// value here.
		descOut2, err := client.DescribeUser(ctx, &mqsdk.DescribeUserInput{
			BrokerId: aws.String(brokerID), Username: aws.String("alice"),
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"admins"}, descOut2.Groups, "groups must not change before a reboot")
		require.NotNil(t, descOut2.Pending)
		assert.ElementsMatch(t, []string{"admins", "ops"}, descOut2.Pending.Groups)
		assert.True(t, aws.ToBool(descOut2.Pending.ConsoleAccess))

		// DescribeBroker/ListBrokers apply any pending reboot-time changes
		// once BrokerState is REBOOT_IN_PROGRESS (brokers.go's
		// promoteBrokerReboot).
		_, err = client.RebootBroker(ctx, &mqsdk.RebootBrokerInput{BrokerId: aws.String(brokerID)})
		require.NoError(t, err)
		_, err = client.DescribeBroker(ctx, &mqsdk.DescribeBrokerInput{BrokerId: aws.String(brokerID)})
		require.NoError(t, err)

		descOut3, err := client.DescribeUser(ctx, &mqsdk.DescribeUserInput{
			BrokerId: aws.String(brokerID), Username: aws.String("alice"),
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"admins", "ops"}, descOut3.Groups, "groups apply after reboot")
		assert.True(t, aws.ToBool(descOut3.ConsoleAccess))
		assert.Nil(t, descOut3.Pending)

		_, err = client.DeleteUser(ctx, &mqsdk.DeleteUserInput{
			BrokerId: aws.String(brokerID), Username: aws.String("alice"),
		})
		require.NoError(t, err)

		// DeleteUser also only stages -- the user stays visible with
		// PendingChange=DELETE until the next reboot (users.go's DeleteUser
		// doc comment).
		descOut4, err := client.DescribeUser(ctx, &mqsdk.DescribeUserInput{
			BrokerId: aws.String(brokerID), Username: aws.String("alice"),
		})
		require.NoError(t, err, "user stays visible pre-reboot with a pending delete")
		require.NotNil(t, descOut4.Pending)
		assert.Equal(t, mqtypes.ChangeTypeDelete, descOut4.Pending.PendingChange)

		_, err = client.RebootBroker(ctx, &mqsdk.RebootBrokerInput{BrokerId: aws.String(brokerID)})
		require.NoError(t, err)
		_, err = client.DescribeBroker(ctx, &mqsdk.DescribeBrokerInput{BrokerId: aws.String(brokerID)})
		require.NoError(t, err)

		_, err = client.DescribeUser(ctx, &mqsdk.DescribeUserInput{
			BrokerId: aws.String(brokerID), Username: aws.String("alice"),
		})
		require.Error(t, err, "user no longer exists once the pending delete is applied on reboot")
	})

	t.Run("configuration revision and delete", func(t *testing.T) {
		t.Parallel()

		backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
		client := newTestMQClient(t, mq.NewHandler(backend))
		ctx := t.Context()

		cfgOut, err := client.CreateConfiguration(ctx, &mqsdk.CreateConfigurationInput{
			Name: aws.String("s23-config"), EngineType: "ACTIVEMQ", EngineVersion: aws.String("5.15.14"),
		})
		require.NoError(t, err)
		configID := aws.ToString(cfgOut.Id)
		revision := cfgOut.LatestRevision.Revision

		revOut, err := client.DescribeConfigurationRevision(ctx, &mqsdk.DescribeConfigurationRevisionInput{
			ConfigurationId:       aws.String(configID),
			ConfigurationRevision: aws.String(strconv.Itoa(int(aws.ToInt32(revision)))),
		})
		require.NoError(t, err)
		assert.Equal(t, configID, aws.ToString(revOut.ConfigurationId))
		assert.NotEmpty(t, aws.ToString(revOut.Data))

		_, err = client.DeleteConfiguration(ctx, &mqsdk.DeleteConfigurationInput{
			ConfigurationId: aws.String(configID),
		})
		require.NoError(t, err)

		_, err = client.DescribeConfiguration(ctx, &mqsdk.DescribeConfigurationInput{
			ConfigurationId: aws.String(configID),
		})
		require.Error(t, err, "configuration no longer exists after delete")
	})

	t.Run("broker engine types and instance options and shared resources", func(t *testing.T) {
		t.Parallel()

		backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
		client := newTestMQClient(t, mq.NewHandler(backend))
		ctx := t.Context()

		engineOut, err := client.DescribeBrokerEngineTypes(ctx, &mqsdk.DescribeBrokerEngineTypesInput{
			EngineType: aws.String("ACTIVEMQ"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, engineOut.BrokerEngineTypes)
		assert.Equal(t, mqtypes.EngineTypeActivemq, engineOut.BrokerEngineTypes[0].EngineType)

		instOut, err := client.DescribeBrokerInstanceOptions(ctx, &mqsdk.DescribeBrokerInstanceOptionsInput{
			EngineType: aws.String("ACTIVEMQ"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, instOut.BrokerInstanceOptions)

		brokerOut, err := client.CreateBroker(ctx, &mqsdk.CreateBrokerInput{
			BrokerName: aws.String("s23-shared-broker"), EngineType: "ACTIVEMQ",
			EngineVersion: aws.String("5.15.14"), HostInstanceType: aws.String("mq.t3.micro"),
			DeploymentMode: "SINGLE_INSTANCE", PubliclyAccessible: aws.Bool(false),
			Users: []mqtypes.User{{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")}},
		})
		require.NoError(t, err)

		sharedOut, err := client.DescribeSharedResources(ctx, &mqsdk.DescribeSharedResourcesInput{
			BrokerId: brokerOut.BrokerId,
		})
		require.NoError(t, err)
		assert.Empty(t, sharedOut.SharedResources)
	})

	t.Run("promote a replica broker to primary", func(t *testing.T) {
		t.Parallel()

		backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
		client := newTestMQClient(t, mq.NewHandler(backend))
		ctx := t.Context()

		replicaOut, err := client.CreateBroker(ctx, &mqsdk.CreateBrokerInput{
			BrokerName: aws.String("s23-replica-broker"), EngineType: "ACTIVEMQ",
			EngineVersion: aws.String("5.15.14"), HostInstanceType: aws.String("mq.t3.micro"),
			DeploymentMode: "SINGLE_INSTANCE", PubliclyAccessible: aws.Bool(false),
			Users: []mqtypes.User{
				{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")},
			},
			DataReplicationMode: mqtypes.DataReplicationModeCrdr,
			DataReplicationPrimaryBrokerArn: aws.String(
				"arn:aws:mq:us-east-1:000000000000:broker:primary-broker:b-primary",
			),
		})
		require.NoError(t, err)
		replicaID := aws.ToString(replicaOut.BrokerId)

		descBefore, err := client.DescribeBroker(ctx, &mqsdk.DescribeBrokerInput{BrokerId: aws.String(replicaID)})
		require.NoError(t, err)
		require.NotNil(t, descBefore.DataReplicationMetadata)
		assert.Equal(t, "REPLICA", aws.ToString(descBefore.DataReplicationMetadata.DataReplicationRole))

		promoteOut, err := client.Promote(ctx, &mqsdk.PromoteInput{
			BrokerId: aws.String(replicaID), Mode: mqtypes.PromoteModeSwitchover,
		})
		require.NoError(t, err)
		assert.Equal(t, replicaID, aws.ToString(promoteOut.BrokerId))

		descAfter, err := client.DescribeBroker(ctx, &mqsdk.DescribeBrokerInput{BrokerId: aws.String(replicaID)})
		require.NoError(t, err)
		require.NotNil(t, descAfter.DataReplicationMetadata)
		assert.Equal(t, "PRIMARY", aws.ToString(descAfter.DataReplicationMetadata.DataReplicationRole))
	})

	t.Run("promote a non-replica broker is rejected", func(t *testing.T) {
		t.Parallel()

		backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
		client := newTestMQClient(t, mq.NewHandler(backend))
		ctx := t.Context()

		brokerOut, err := client.CreateBroker(ctx, &mqsdk.CreateBrokerInput{
			BrokerName: aws.String("s23-not-replica-broker"), EngineType: "ACTIVEMQ",
			EngineVersion: aws.String("5.15.14"), HostInstanceType: aws.String("mq.t3.micro"),
			DeploymentMode: "SINGLE_INSTANCE", PubliclyAccessible: aws.Bool(false),
			Users: []mqtypes.User{{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")}},
		})
		require.NoError(t, err)

		_, err = client.Promote(ctx, &mqsdk.PromoteInput{
			BrokerId: brokerOut.BrokerId, Mode: mqtypes.PromoteModeFailover,
		})
		require.Error(t, err, "Promote is documented as operating only on a CRDR replica broker")
	})
}

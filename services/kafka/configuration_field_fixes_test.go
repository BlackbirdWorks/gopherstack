package kafka_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// TestListConfigurations_RequiredFields proves gopherstack-r80d batch 27's
// fix: kafka@v1.57.2's types.Configuration -- the real list-item shape
// ListConfigurationsOutput.Configurations marshals directly -- declares
// CreationTime and LatestRevision required (types/types.go, both *string/
// *ConfigurationRevision -- provable, distinguishable-from-omitted pointer
// types). Before this fix, gopherstack's Configuration model had no
// CreationTime or LatestRevision field at all, so ListConfigurations never
// emitted either key and a real client always decoded both as nil despite
// the SDK's required-field contract.
func TestListConfigurations_RequiredFields(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	_, err := client.CreateConfiguration(t.Context(), &kafkasdk.CreateConfigurationInput{
		Name:             aws.String("list-configs-fields"),
		ServerProperties: []byte("auto.create.topics.enable=true"),
	})
	require.NoError(t, err)

	listed, err := client.ListConfigurations(t.Context(), &kafkasdk.ListConfigurationsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Configurations, 1)

	cfg := listed.Configurations[0]
	assert.NotNil(t, cfg.CreationTime, "Configuration.CreationTime is required and must not be nil")
	require.NotNil(t, cfg.LatestRevision, "Configuration.LatestRevision is required and must not be nil")
	assert.NotNil(t, cfg.LatestRevision.CreationTime,
		"ConfigurationRevision.CreationTime is required and must not be nil")
}

// TestListConfigurationRevisions_CreationTime proves the same
// ConfigurationRevision.CreationTime fix reaches ListConfigurationRevisions,
// whose Revisions field is also the real types.ConfigurationRevision list
// shape (deserializers.go's awsRestjson1_deserializeDocumentConfigurationRevision
// switches on "creationTime").
func TestListConfigurationRevisions_CreationTime(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateConfiguration(t.Context(), &kafkasdk.CreateConfigurationInput{
		Name:             aws.String("list-revisions-creation-time"),
		ServerProperties: []byte("auto.create.topics.enable=true"),
	})
	require.NoError(t, err)

	revisions, err := client.ListConfigurationRevisions(t.Context(), &kafkasdk.ListConfigurationRevisionsInput{
		Arn: created.Arn,
	})
	require.NoError(t, err)
	require.Len(t, revisions.Revisions, 1)
	assert.NotNil(t, revisions.Revisions[0].CreationTime,
		"ConfigurationRevision.CreationTime is required and must not be nil")
}

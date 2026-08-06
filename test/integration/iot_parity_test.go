package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createIoTClient returns an IoT control-plane client pointed at the shared test container.
func createIoTClient(t *testing.T) *iotsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return iotsdk.NewFromConfig(cfg, func(o *iotsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_IoT_IndexingConfiguration drives UpdateIndexingConfiguration followed by
// GetIndexingConfiguration through the real SDK, asserting the submitted mode round-trips.
func TestIntegration_IoT_IndexingConfiguration(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createIoTClient(t)
	ctx := t.Context()

	_, err := client.UpdateIndexingConfiguration(ctx, &iotsdk.UpdateIndexingConfigurationInput{
		ThingIndexingConfiguration: &iottypes.ThingIndexingConfiguration{
			ThingIndexingMode:             iottypes.ThingIndexingModeRegistryAndShadow,
			ThingConnectivityIndexingMode: iottypes.ThingConnectivityIndexingModeStatus,
		},
		ThingGroupIndexingConfiguration: &iottypes.ThingGroupIndexingConfiguration{
			ThingGroupIndexingMode: iottypes.ThingGroupIndexingModeOn,
		},
	})
	require.NoError(t, err, "UpdateIndexingConfiguration should succeed")

	t.Cleanup(func() {
		_, _ = client.UpdateIndexingConfiguration(ctx, &iotsdk.UpdateIndexingConfigurationInput{
			ThingIndexingConfiguration: &iottypes.ThingIndexingConfiguration{
				ThingIndexingMode: iottypes.ThingIndexingModeOff,
			},
			ThingGroupIndexingConfiguration: &iottypes.ThingGroupIndexingConfiguration{
				ThingGroupIndexingMode: iottypes.ThingGroupIndexingModeOff,
			},
		})
	})

	getOut, err := client.GetIndexingConfiguration(ctx, &iotsdk.GetIndexingConfigurationInput{})
	require.NoError(t, err, "GetIndexingConfiguration should succeed")
	require.NotNil(t, getOut.ThingIndexingConfiguration)
	assert.Equal(t, iottypes.ThingIndexingModeRegistryAndShadow, getOut.ThingIndexingConfiguration.ThingIndexingMode)
	assert.Equal(t,
		iottypes.ThingConnectivityIndexingModeStatus, getOut.ThingIndexingConfiguration.ThingConnectivityIndexingMode,
	)
	require.NotNil(t, getOut.ThingGroupIndexingConfiguration)
	assert.Equal(t, iottypes.ThingGroupIndexingModeOn, getOut.ThingGroupIndexingConfiguration.ThingGroupIndexingMode)
}

// TestIntegration_IoT_SearchIndexFindsCreatedThings creates a couple of real things via the SDK
// and verifies SearchIndex finds them by name (with fleet indexing enabled).
func TestIntegration_IoT_SearchIndexFindsCreatedThings(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createIoTClient(t)
	ctx := t.Context()

	_, err := client.UpdateIndexingConfiguration(ctx, &iotsdk.UpdateIndexingConfigurationInput{
		ThingIndexingConfiguration: &iottypes.ThingIndexingConfiguration{
			ThingIndexingMode: iottypes.ThingIndexingModeRegistry,
		},
	})
	require.NoError(t, err, "enabling registry indexing should succeed")

	t.Cleanup(func() {
		_, _ = client.UpdateIndexingConfiguration(ctx, &iotsdk.UpdateIndexingConfigurationInput{
			ThingIndexingConfiguration: &iottypes.ThingIndexingConfiguration{
				ThingIndexingMode: iottypes.ThingIndexingModeOff,
			},
		})
	})

	suffix := uuid.NewString()[:8]
	thingName := "search-idx-thing-" + suffix

	_, err = client.CreateThing(ctx, &iotsdk.CreateThingInput{
		ThingName: aws.String(thingName),
		AttributePayload: &iottypes.AttributePayload{
			Attributes: map[string]string{"env": "integration"},
		},
	})
	require.NoError(t, err, "CreateThing should succeed")

	t.Cleanup(func() {
		_, _ = client.DeleteThing(ctx, &iotsdk.DeleteThingInput{ThingName: aws.String(thingName)})
	})

	// SearchIndex is eventually-consistent against the real service; the emulator should
	// reflect the newly created thing on the very next call, but poll briefly for robustness.
	var found bool

	require.Eventually(t, func() bool {
		searchOut, searchErr := client.SearchIndex(ctx, &iotsdk.SearchIndexInput{
			QueryString: aws.String("thingName:" + thingName),
		})
		require.NoError(t, searchErr, "SearchIndex should succeed")

		for _, thing := range searchOut.Things {
			if aws.ToString(thing.ThingName) == thingName {
				found = true
				assert.Equal(t, "integration", thing.Attributes["env"])
			}
		}

		return found
	}, 5*time.Second, 200*time.Millisecond, "SearchIndex should find the newly created thing")
}

// TestIntegration_IoT_ThingRegistrationTaskLifecycle drives StartThingRegistrationTask followed
// by DescribeThingRegistrationTask through the real SDK.
func TestIntegration_IoT_ThingRegistrationTaskLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createIoTClient(t)
	ctx := t.Context()

	templateBody := `{"Parameters":{},"Resources":{"thing":{"Type":"AWS::IoT::Thing",` +
		`"Properties":{"ThingName":"bulk-thing"}}}}`

	startOut, err := client.StartThingRegistrationTask(ctx, &iotsdk.StartThingRegistrationTaskInput{
		TemplateBody:    aws.String(templateBody),
		InputFileBucket: aws.String("my-bulk-bucket"),
		InputFileKey:    aws.String("bulk-things.json"),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/IoTBulkProvisioning"),
	})
	require.NoError(t, err, "StartThingRegistrationTask should succeed")

	taskID := aws.ToString(startOut.TaskId)
	require.NotEmpty(t, taskID, "task ID should be generated")

	descOut, err := client.DescribeThingRegistrationTask(ctx, &iotsdk.DescribeThingRegistrationTaskInput{
		TaskId: aws.String(taskID),
	})
	require.NoError(t, err, "DescribeThingRegistrationTask should succeed")
	assert.Equal(t, taskID, aws.ToString(descOut.TaskId))
	assert.Equal(t, templateBody, aws.ToString(descOut.TemplateBody), "templateBody should round-trip")
	assert.Equal(t, "my-bulk-bucket", aws.ToString(descOut.InputFileBucket))
	assert.NotEmpty(t, string(descOut.Status), "task should have a real status")
}

// TestIntegration_IoT_TestAuthorization verifies TestAuthorization evaluates a real policy and
// returns a genuine per-authInfo authResult (not an empty/stubbed response).
func TestIntegration_IoT_TestAuthorization(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createIoTClient(t)
	ctx := t.Context()

	policyName := "test-authz-policy-" + uuid.NewString()[:8]
	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:Publish",` +
		`"Resource":"arn:aws:iot:us-east-1:000000000000:topic/test/*"}]}`

	_, err := client.CreatePolicy(ctx, &iotsdk.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(policyDoc),
	})
	require.NoError(t, err, "CreatePolicy should succeed")

	t.Cleanup(func() {
		_, _ = client.DeletePolicy(ctx, &iotsdk.DeletePolicyInput{PolicyName: aws.String(policyName)})
	})

	out, err := client.TestAuthorization(ctx, &iotsdk.TestAuthorizationInput{
		Principal:        aws.String("arn:aws:iot:us-east-1:000000000000:cert/abc123"),
		PolicyNamesToAdd: []string{policyName},
		AuthInfos: []iottypes.AuthInfo{
			{
				ActionType: iottypes.ActionTypePublish,
				Resources:  []string{"arn:aws:iot:us-east-1:000000000000:topic/test/foo"},
			},
		},
	})
	require.NoError(t, err, "TestAuthorization should succeed")
	require.NotEmpty(t, out.AuthResults, "TestAuthorization should return a real authResult per authInfo")

	result := out.AuthResults[0]
	assert.NotEmpty(t, string(result.AuthDecision), "authDecision should be a real, non-empty value")
	assert.Equal(
		t,
		iottypes.AuthDecisionAllowed,
		result.AuthDecision,
		"publish should be allowed by the attached policy",
	)
}

// TestIntegration_IoT_DescribeEncryptionConfigurationDefault verifies the default (no
// customer-managed key configured) encryption configuration is a real, well-formed default.
func TestIntegration_IoT_DescribeEncryptionConfigurationDefault(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createIoTClient(t)
	ctx := t.Context()

	out, err := client.DescribeEncryptionConfiguration(ctx, &iotsdk.DescribeEncryptionConfigurationInput{})
	require.NoError(t, err, "DescribeEncryptionConfiguration should succeed")
	assert.NotEmpty(t, string(out.EncryptionType), "encryptionType should have a default value")
	assert.Equal(t, iottypes.EncryptionTypeAwsOwnedKmsKey, out.EncryptionType)
}

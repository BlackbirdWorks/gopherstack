package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlogssvc "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	gluesvc "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch20 provisions Glue (classifier, connection, data
// catalog encryption settings, data quality ruleset, dev endpoint, ML
// transform, partition + partition index, registry, schema, security
// configuration, resource policy, trigger, user defined function, workflow,
// catalog table optimizer) and CloudWatch Logs (account policy, anomaly
// detector, data protection policy, delivery source/destination(+policies)/
// delivery, destination(+policy), index policy, resource policy, query
// definition) resources via Terraform and verifies each through its own SDK
// client's Get/Describe path.
func TestTerraform_MegaBatch20(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-20",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch20Glue(ctx, t)
				verifyMegaBatch20CloudWatchLogs(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyMegaBatch20Glue(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createGlueClient(t)

	classifierOut, err := client.GetClassifier(ctx, &gluesvc.GetClassifierInput{
		Name: aws.String("mega-batch-20-classifier"),
	})
	require.NoError(t, err, "GetClassifier should succeed")
	require.NotNil(t, classifierOut.Classifier)
	require.NotNil(t, classifierOut.Classifier.CsvClassifier)
	assert.Equal(t, "mega-batch-20-classifier", aws.ToString(classifierOut.Classifier.CsvClassifier.Name))

	connOut, err := client.GetConnection(ctx, &gluesvc.GetConnectionInput{
		Name: aws.String("mega-batch-20-connection"),
	})
	require.NoError(t, err, "GetConnection should succeed")
	require.NotNil(t, connOut.Connection)
	assert.Equal(t, gluetypes.ConnectionTypeNetwork, connOut.Connection.ConnectionType)

	encOut, err := client.GetDataCatalogEncryptionSettings(ctx, &gluesvc.GetDataCatalogEncryptionSettingsInput{})
	require.NoError(t, err, "GetDataCatalogEncryptionSettings should succeed")
	require.NotNil(t, encOut.DataCatalogEncryptionSettings)

	dqrOut, err := client.GetDataQualityRuleset(ctx, &gluesvc.GetDataQualityRulesetInput{
		Name: aws.String("mega-batch-20-dq-ruleset"),
	})
	require.NoError(t, err, "GetDataQualityRuleset should succeed")
	assert.Equal(t, "mega-batch-20-dq-ruleset", aws.ToString(dqrOut.Name))

	devOut, err := client.GetDevEndpoint(ctx, &gluesvc.GetDevEndpointInput{
		EndpointName: aws.String("mega-batch-20-dev-endpoint"),
	})
	require.NoError(t, err, "GetDevEndpoint should succeed")
	require.NotNil(t, devOut.DevEndpoint)

	regOut, err := client.GetRegistry(ctx, &gluesvc.GetRegistryInput{
		RegistryId: &gluetypes.RegistryId{RegistryName: aws.String("mega-batch-20-registry")},
	})
	require.NoError(t, err, "GetRegistry should succeed")
	assert.Equal(t, "mega-batch-20-registry", aws.ToString(regOut.RegistryName))

	mlOut, err := client.ListMLTransforms(ctx, &gluesvc.ListMLTransformsInput{})
	require.NoError(t, err, "ListMLTransforms should succeed")

	var transformID string

	for _, id := range mlOut.TransformIds {
		transformID = id
	}

	require.NotEmpty(t, transformID, "ml transform should be listed")

	getMlOut, err := client.GetMLTransform(ctx, &gluesvc.GetMLTransformInput{
		TransformId: aws.String(transformID),
	})
	require.NoError(t, err, "GetMLTransform should succeed")
	assert.Equal(t, "mega-batch-20-ml-transform", aws.ToString(getMlOut.Name))

	schemaOut, err := client.GetSchema(ctx, &gluesvc.GetSchemaInput{
		SchemaId: &gluetypes.SchemaId{
			SchemaName:   aws.String("mega-batch-20-schema"),
			RegistryName: aws.String("mega-batch-20-registry"),
		},
	})
	require.NoError(t, err, "GetSchema should succeed")
	assert.Equal(t, gluetypes.DataFormatAvro, schemaOut.DataFormat)

	secOut, err := client.GetSecurityConfiguration(ctx, &gluesvc.GetSecurityConfigurationInput{
		Name: aws.String("mega-batch-20-secconfig"),
	})
	require.NoError(t, err, "GetSecurityConfiguration should succeed")
	require.NotNil(t, secOut.SecurityConfiguration)

	rpOut, err := client.GetResourcePolicy(ctx, &gluesvc.GetResourcePolicyInput{})
	require.NoError(t, err, "GetResourcePolicy should succeed")
	assert.Contains(t, aws.ToString(rpOut.PolicyInJson), "glue:GetTable")

	trigOut, err := client.GetTrigger(ctx, &gluesvc.GetTriggerInput{
		Name: aws.String("mega-batch-20-trigger"),
	})
	require.NoError(t, err, "GetTrigger should succeed")
	require.NotNil(t, trigOut.Trigger)

	udfOut, err := client.GetUserDefinedFunction(ctx, &gluesvc.GetUserDefinedFunctionInput{
		DatabaseName: aws.String("mega_batch_20_db"),
		FunctionName: aws.String("mega-batch-20-udf"),
	})
	require.NoError(t, err, "GetUserDefinedFunction should succeed")
	require.NotNil(t, udfOut.UserDefinedFunction)

	wfOut, err := client.GetWorkflow(ctx, &gluesvc.GetWorkflowInput{
		Name: aws.String("mega-batch-20-workflow"),
	})
	require.NoError(t, err, "GetWorkflow should succeed")
	require.NotNil(t, wfOut.Workflow)

	partOut, err := client.GetPartition(ctx, &gluesvc.GetPartitionInput{
		DatabaseName:    aws.String("mega_batch_20_db"),
		TableName:       aws.String("mega_batch_20_table"),
		PartitionValues: []string{"2024"},
	})
	require.NoError(t, err, "GetPartition should succeed")
	require.NotNil(t, partOut.Partition)

	idxOut, err := client.GetPartitionIndexes(ctx, &gluesvc.GetPartitionIndexesInput{
		DatabaseName: aws.String("mega_batch_20_db"),
		TableName:    aws.String("mega_batch_20_table"),
	})
	require.NoError(t, err, "GetPartitionIndexes should succeed")
	require.Len(t, idxOut.PartitionIndexDescriptorList, 1)

	optOut, err := client.GetTableOptimizer(ctx, &gluesvc.GetTableOptimizerInput{
		CatalogId:    aws.String("000000000000"),
		DatabaseName: aws.String("mega_batch_20_db"),
		TableName:    aws.String("mega_batch_20_table"),
		Type:         gluetypes.TableOptimizerTypeCompaction,
	})
	require.NoError(t, err, "GetTableOptimizer should succeed")
	require.NotNil(t, optOut.TableOptimizer)
	assert.True(t, aws.ToBool(optOut.TableOptimizer.Configuration.Enabled))
}

func verifyMegaBatch20CloudWatchLogs(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createCloudWatchLogsClient(t)

	apOut, err := client.DescribeAccountPolicies(ctx, &cwlogssvc.DescribeAccountPoliciesInput{
		PolicyType: cwlogstypes.PolicyTypeSubscriptionFilterPolicy,
	})
	require.NoError(t, err, "DescribeAccountPolicies should succeed")

	var foundAccountPolicy bool

	for _, p := range apOut.AccountPolicies {
		if aws.ToString(p.PolicyName) == "mega-batch-20-account-policy" {
			foundAccountPolicy = true
		}
	}

	assert.True(t, foundAccountPolicy, "account policy should be listed")

	adOut, err := client.ListLogAnomalyDetectors(ctx, &cwlogssvc.ListLogAnomalyDetectorsInput{})
	require.NoError(t, err, "ListLogAnomalyDetectors should succeed")

	var detectorArn string

	for _, d := range adOut.AnomalyDetectors {
		if aws.ToString(d.DetectorName) == "mega-batch-20-detector" {
			detectorArn = aws.ToString(d.AnomalyDetectorArn)
		}
	}

	require.NotEmpty(t, detectorArn, "anomaly detector should be listed")

	getDetOut, err := client.GetLogAnomalyDetector(ctx, &cwlogssvc.GetLogAnomalyDetectorInput{
		AnomalyDetectorArn: aws.String(detectorArn),
	})
	require.NoError(t, err, "GetLogAnomalyDetector should succeed")
	assert.Equal(t, int64(7), aws.ToInt64(getDetOut.AnomalyVisibilityTime))

	dppOut, err := client.GetDataProtectionPolicy(ctx, &cwlogssvc.GetDataProtectionPolicyInput{
		LogGroupIdentifier: aws.String("/mega-batch-20/loggroup"),
	})
	require.NoError(t, err, "GetDataProtectionPolicy should succeed")
	assert.Contains(t, aws.ToString(dppOut.PolicyDocument), "mega-batch-20-dpp")

	ddOut, err := client.GetDeliveryDestination(ctx, &cwlogssvc.GetDeliveryDestinationInput{
		Name: aws.String("mega-batch-20-delivery-destination"),
	})
	require.NoError(t, err, "GetDeliveryDestination should succeed")
	require.NotNil(t, ddOut.DeliveryDestination)

	ddpOut, err := client.GetDeliveryDestinationPolicy(ctx, &cwlogssvc.GetDeliveryDestinationPolicyInput{
		DeliveryDestinationName: aws.String("mega-batch-20-delivery-destination"),
	})
	require.NoError(t, err, "GetDeliveryDestinationPolicy should succeed")
	require.NotNil(t, ddpOut.Policy)

	dsOut, err := client.GetDeliverySource(ctx, &cwlogssvc.GetDeliverySourceInput{
		Name: aws.String("mega-batch-20-delivery-source"),
	})
	require.NoError(t, err, "GetDeliverySource should succeed")
	require.NotNil(t, dsOut.DeliverySource)

	descDelOut, err := client.DescribeDeliveries(ctx, &cwlogssvc.DescribeDeliveriesInput{})
	require.NoError(t, err, "DescribeDeliveries should succeed")

	var deliveryID string

	for _, d := range descDelOut.Deliveries {
		if aws.ToString(d.DeliverySourceName) == "mega-batch-20-delivery-source" {
			deliveryID = aws.ToString(d.Id)
		}
	}

	require.NotEmpty(t, deliveryID, "delivery should be listed")

	getDelOut, err := client.GetDelivery(ctx, &cwlogssvc.GetDeliveryInput{
		Id: aws.String(deliveryID),
	})
	require.NoError(t, err, "GetDelivery should succeed")
	require.NotNil(t, getDelOut.Delivery)

	destOut, err := client.DescribeDestinations(ctx, &cwlogssvc.DescribeDestinationsInput{
		DestinationNamePrefix: aws.String("mega-batch-20-destination"),
	})
	require.NoError(t, err, "DescribeDestinations should succeed")
	require.Len(t, destOut.Destinations, 1)
	assert.NotEmpty(t, aws.ToString(destOut.Destinations[0].AccessPolicy))

	idxPolOut, err := client.DescribeIndexPolicies(ctx, &cwlogssvc.DescribeIndexPoliciesInput{
		LogGroupIdentifiers: []string{"/mega-batch-20/loggroup"},
	})
	require.NoError(t, err, "DescribeIndexPolicies should succeed")
	require.Len(t, idxPolOut.IndexPolicies, 1)

	resPolOut, err := client.DescribeResourcePolicies(ctx, &cwlogssvc.DescribeResourcePoliciesInput{})
	require.NoError(t, err, "DescribeResourcePolicies should succeed")

	var foundResourcePolicy bool

	for _, p := range resPolOut.ResourcePolicies {
		if aws.ToString(p.PolicyName) == "mega-batch-20-resource-policy" {
			foundResourcePolicy = true
		}
	}

	assert.True(t, foundResourcePolicy, "resource policy should be listed")

	qdOut, err := client.DescribeQueryDefinitions(ctx, &cwlogssvc.DescribeQueryDefinitionsInput{
		QueryDefinitionNamePrefix: aws.String("mega-batch-20-query-definition"),
	})
	require.NoError(t, err, "DescribeQueryDefinitions should succeed")
	require.Len(t, qdOut.QueryDefinitions, 1)
}

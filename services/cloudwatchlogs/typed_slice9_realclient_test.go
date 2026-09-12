package cloudwatchlogs_test

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// newSlice9CloudWatchLogsClient is a top-level (not closure-local) client
// constructor: cmd/clientcoverage's census only traces a var's SDK-module
// binding through a named top-level function with a declared *pkg.Client
// return type, not a local func-literal variable, so keeping this as a
// top-level func is load-bearing for accurate typed-coverage measurement.
func newSlice9CloudWatchLogsClient(t *testing.T) *cwlsdk.Client {
	t.Helper()

	return newTestCloudWatchLogsClient(
		t,
		cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()),
	)
}

// TestTypedSlice9RealClient drives cloudwatchlogs's typed-coverage-blind ops
// (gopherstack-n3zi slice 9) through the real aws-sdk-go-v2 cloudwatchlogs
// client, one subtest per named priority family, asserting decoded values.
func TestTypedSlice9RealClient(t *testing.T) {
	t.Parallel()

	t.Run("log group kms", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/kms"),
		})
		require.NoError(t, err)

		_, err = client.AssociateKmsKey(t.Context(), &cwlsdk.AssociateKmsKeyInput{
			LogGroupName: aws.String("/s9/kms"),
			KmsKeyId:     aws.String("arn:aws:kms:us-east-1:000000000000:key/s9-key"),
		})
		require.NoError(t, err)

		_, err = client.DisassociateKmsKey(t.Context(), &cwlsdk.DisassociateKmsKeyInput{
			LogGroupName: aws.String("/s9/kms"),
		})
		require.NoError(t, err)
	})

	t.Run("log streams", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/streams"),
		})
		require.NoError(t, err)

		_, err = client.CreateLogStream(t.Context(), &cwlsdk.CreateLogStreamInput{
			LogGroupName:  aws.String("/s9/streams"),
			LogStreamName: aws.String("s9-stream"),
		})
		require.NoError(t, err)

		listOut, err := client.DescribeLogStreams(t.Context(), &cwlsdk.DescribeLogStreamsInput{
			LogGroupName: aws.String("/s9/streams"),
		})
		require.NoError(t, err)
		require.Len(t, listOut.LogStreams, 1)
		assert.Equal(t, "s9-stream", aws.ToString(listOut.LogStreams[0].LogStreamName))

		_, err = client.DeleteLogStream(t.Context(), &cwlsdk.DeleteLogStreamInput{
			LogGroupName:  aws.String("/s9/streams"),
			LogStreamName: aws.String("s9-stream"),
		})
		require.NoError(t, err)

		listAfter, err := client.DescribeLogStreams(t.Context(), &cwlsdk.DescribeLogStreamsInput{
			LogGroupName: aws.String("/s9/streams"),
		})
		require.NoError(t, err)
		assert.Empty(t, listAfter.LogStreams)
	})

	t.Run("log groups listing and retention", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/retention"),
		})
		require.NoError(t, err)

		listOut, err := client.ListLogGroups(t.Context(), &cwlsdk.ListLogGroupsInput{
			LogGroupNamePattern: aws.String("^/s9/retention"),
		})
		require.NoError(t, err)
		require.Len(t, listOut.LogGroups, 1)
		assert.Equal(t, "/s9/retention", aws.ToString(listOut.LogGroups[0].LogGroupName))
		assert.NotEmpty(t, aws.ToString(listOut.LogGroups[0].LogGroupArn))

		_, err = client.PutRetentionPolicy(t.Context(), &cwlsdk.PutRetentionPolicyInput{
			LogGroupName:    aws.String("/s9/retention"),
			RetentionInDays: aws.Int32(14),
		})
		require.NoError(t, err)

		describeOut, err := client.DescribeLogGroups(t.Context(), &cwlsdk.DescribeLogGroupsInput{
			LogGroupNamePrefix: aws.String("/s9/retention"),
		})
		require.NoError(t, err)
		require.Len(t, describeOut.LogGroups, 1)
		assert.Equal(t, int32(14), aws.ToInt32(describeOut.LogGroups[0].RetentionInDays))

		_, err = client.DeleteRetentionPolicy(t.Context(), &cwlsdk.DeleteRetentionPolicyInput{
			LogGroupName: aws.String("/s9/retention"),
		})
		require.NoError(t, err)

		describeAfter, err := client.DescribeLogGroups(t.Context(), &cwlsdk.DescribeLogGroupsInput{
			LogGroupNamePrefix: aws.String("/s9/retention"),
		})
		require.NoError(t, err)
		require.Len(t, describeAfter.LogGroups, 1)
		assert.Nil(t, describeAfter.LogGroups[0].RetentionInDays)

		fieldsOut, err := client.GetLogGroupFields(t.Context(), &cwlsdk.GetLogGroupFieldsInput{
			LogGroupName: aws.String("/s9/retention"),
		})
		require.NoError(t, err)
		assert.NotNil(t, fieldsOut.LogGroupFields)
	})

	t.Run("log record", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/record"),
		})
		require.NoError(t, err)

		_, err = client.CreateLogStream(t.Context(), &cwlsdk.CreateLogStreamInput{
			LogGroupName:  aws.String("/s9/record"),
			LogStreamName: aws.String("s9-record-stream"),
		})
		require.NoError(t, err)

		_, err = client.PutLogEvents(t.Context(), &cwlsdk.PutLogEventsInput{
			LogGroupName:  aws.String("/s9/record"),
			LogStreamName: aws.String("s9-record-stream"),
			LogEvents: []cwltypes.InputLogEvent{
				{Message: aws.String("hello s9"), Timestamp: aws.Int64(time.Now().UnixMilli())},
			},
		})
		require.NoError(t, err)

		pointer := base64.StdEncoding.EncodeToString([]byte("/s9/record:s9-record-stream:0"))

		recordOut, err := client.GetLogRecord(t.Context(), &cwlsdk.GetLogRecordInput{
			LogRecordPointer: aws.String(pointer),
		})
		require.NoError(t, err)
		assert.Equal(t, "hello s9", recordOut.LogRecord["@message"])
	})

	t.Run("export tasks", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/export"),
		})
		require.NoError(t, err)

		createOut, err := client.CreateExportTask(t.Context(), &cwlsdk.CreateExportTaskInput{
			LogGroupName: aws.String("/s9/export"),
			Destination:  aws.String("s9-export-bucket"),
			From:         aws.Int64(1),
			To:           aws.Int64(2000000000000),
		})
		require.NoError(t, err)
		taskID := aws.ToString(createOut.TaskId)
		require.NotEmpty(t, taskID)

		describeOut, err := client.DescribeExportTasks(
			t.Context(),
			&cwlsdk.DescribeExportTasksInput{
				TaskId: aws.String(taskID),
			},
		)
		require.NoError(t, err)
		require.Len(t, describeOut.ExportTasks, 1)
		assert.Equal(t, "/s9/export", aws.ToString(describeOut.ExportTasks[0].LogGroupName))

		_, err = client.CancelExportTask(t.Context(), &cwlsdk.CancelExportTaskInput{
			TaskId: aws.String(taskID),
		})
		require.NoError(t, err)
	})

	t.Run("import tasks", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		createOut, err := client.CreateImportTask(t.Context(), &cwlsdk.CreateImportTaskInput{
			ImportRoleArn:   aws.String("arn:aws:iam::000000000000:role/s9-import-role"),
			ImportSourceArn: aws.String("arn:aws:s3:::s9-import-bucket/data"),
		})
		require.NoError(t, err)
		importID := aws.ToString(createOut.ImportId)
		require.NotEmpty(t, importID)

		cancelOut, err := client.CancelImportTask(t.Context(), &cwlsdk.CancelImportTaskInput{
			ImportId: aws.String(importID),
		})
		require.NoError(t, err)
		assert.Equal(t, cwltypes.ImportStatusCancelled, cancelOut.ImportStatus)
	})

	t.Run("account policies", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		putOut, err := client.PutAccountPolicy(t.Context(), &cwlsdk.PutAccountPolicyInput{
			PolicyName:     aws.String("s9-account-policy"),
			PolicyType:     cwltypes.PolicyTypeDataProtectionPolicy,
			PolicyDocument: aws.String(`{"Name":"s9","Version":"2021-06-01","Statement":[]}`),
			Scope:          cwltypes.ScopeAll,
		})
		require.NoError(t, err)
		assert.Equal(t, "s9-account-policy", aws.ToString(putOut.AccountPolicy.PolicyName))

		descOut, err := client.DescribeAccountPolicies(
			t.Context(),
			&cwlsdk.DescribeAccountPoliciesInput{
				PolicyType: cwltypes.PolicyTypeDataProtectionPolicy,
				PolicyName: aws.String("s9-account-policy"),
			},
		)
		require.NoError(t, err)
		require.Len(t, descOut.AccountPolicies, 1)

		_, err = client.DeleteAccountPolicy(t.Context(), &cwlsdk.DeleteAccountPolicyInput{
			PolicyName: aws.String("s9-account-policy"),
			PolicyType: cwltypes.PolicyTypeDataProtectionPolicy,
		})
		require.NoError(t, err)

		descAfter, err := client.DescribeAccountPolicies(
			t.Context(),
			&cwlsdk.DescribeAccountPoliciesInput{
				PolicyType: cwltypes.PolicyTypeDataProtectionPolicy,
				PolicyName: aws.String("s9-account-policy"),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, descAfter.AccountPolicies)
	})

	t.Run("data protection policy delete", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/dpp"),
		})
		require.NoError(t, err)

		_, err = client.PutDataProtectionPolicy(t.Context(), &cwlsdk.PutDataProtectionPolicyInput{
			LogGroupIdentifier: aws.String("/s9/dpp"),
			PolicyDocument:     aws.String(`{"Name":"s9","Version":"2021-06-01","Statement":[]}`),
		})
		require.NoError(t, err)

		_, err = client.DeleteDataProtectionPolicy(
			t.Context(),
			&cwlsdk.DeleteDataProtectionPolicyInput{
				LogGroupIdentifier: aws.String("/s9/dpp"),
			},
		)
		require.NoError(t, err)
	})

	t.Run("destinations", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		putOut, err := client.PutDestination(t.Context(), &cwlsdk.PutDestinationInput{
			DestinationName: aws.String("s9-destination"),
			TargetArn:       aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/s9-stream"),
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/s9-dest-role"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s9-destination", aws.ToString(putOut.Destination.DestinationName))

		_, err = client.PutDestinationPolicy(t.Context(), &cwlsdk.PutDestinationPolicyInput{
			DestinationName: aws.String("s9-destination"),
			AccessPolicy:    aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		require.NoError(t, err)

		descOut, err := client.DescribeDestinations(t.Context(), &cwlsdk.DescribeDestinationsInput{
			DestinationNamePrefix: aws.String("s9-destination"),
		})
		require.NoError(t, err)
		require.Len(t, descOut.Destinations, 1)
		assert.NotEmpty(t, aws.ToString(descOut.Destinations[0].AccessPolicy))

		_, err = client.DeleteDestination(t.Context(), &cwlsdk.DeleteDestinationInput{
			DestinationName: aws.String("s9-destination"),
		})
		require.NoError(t, err)
	})

	t.Run("delivery destinations", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		putOut, err := client.PutDeliveryDestination(
			t.Context(),
			&cwlsdk.PutDeliveryDestinationInput{
				Name: aws.String("s9-delivery-dest"),
				DeliveryDestinationConfiguration: &cwltypes.DeliveryDestinationConfiguration{
					DestinationResourceArn: aws.String(
						"arn:aws:logs:us-east-1:000000000000:log-group:/s9/deliverydest",
					),
				},
				DeliveryDestinationType: cwltypes.DeliveryDestinationTypeCwl,
			},
		)
		require.NoError(t, err)
		require.NotNil(t, putOut.DeliveryDestination)

		getOut, err := client.GetDeliveryDestination(
			t.Context(),
			&cwlsdk.GetDeliveryDestinationInput{
				Name: aws.String("s9-delivery-dest"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9-delivery-dest", aws.ToString(getOut.DeliveryDestination.Name))

		_, err = client.PutDeliveryDestinationPolicy(
			t.Context(),
			&cwlsdk.PutDeliveryDestinationPolicyInput{
				DeliveryDestinationName:   aws.String("s9-delivery-dest"),
				DeliveryDestinationPolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
			},
		)
		require.NoError(t, err)

		getPolicyOut, err := client.GetDeliveryDestinationPolicy(
			t.Context(),
			&cwlsdk.GetDeliveryDestinationPolicyInput{
				DeliveryDestinationName: aws.String("s9-delivery-dest"),
			},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(getPolicyOut.Policy.DeliveryDestinationPolicy))

		_, err = client.DeleteDeliveryDestinationPolicy(
			t.Context(),
			&cwlsdk.DeleteDeliveryDestinationPolicyInput{
				DeliveryDestinationName: aws.String("s9-delivery-dest"),
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteDeliveryDestination(
			t.Context(),
			&cwlsdk.DeleteDeliveryDestinationInput{
				Name: aws.String("s9-delivery-dest"),
			},
		)
		require.NoError(t, err)
	})

	t.Run("delivery sources", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.PutDeliverySource(t.Context(), &cwlsdk.PutDeliverySourceInput{
			Name:        aws.String("s9-delivery-src"),
			LogType:     aws.String("APPLICATION_LOGS"),
			ResourceArn: aws.String("arn:aws:workmail:us-east-1:000000000000:organization/m-s9"),
		})
		require.NoError(t, err)

		getOut, err := client.GetDeliverySource(t.Context(), &cwlsdk.GetDeliverySourceInput{
			Name: aws.String("s9-delivery-src"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s9-delivery-src", aws.ToString(getOut.DeliverySource.Name))

		_, err = client.DeleteDeliverySource(t.Context(), &cwlsdk.DeleteDeliverySourceInput{
			Name: aws.String("s9-delivery-src"),
		})
		require.NoError(t, err)
	})

	t.Run("deliveries", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.PutDeliverySource(t.Context(), &cwlsdk.PutDeliverySourceInput{
			Name:        aws.String("s9-del-src"),
			LogType:     aws.String("APPLICATION_LOGS"),
			ResourceArn: aws.String("arn:aws:workmail:us-east-1:000000000000:organization/m-s9del"),
		})
		require.NoError(t, err)

		destOut, err := client.PutDeliveryDestination(
			t.Context(),
			&cwlsdk.PutDeliveryDestinationInput{
				Name: aws.String("s9-del-dest"),
				DeliveryDestinationConfiguration: &cwltypes.DeliveryDestinationConfiguration{
					DestinationResourceArn: aws.String(
						"arn:aws:logs:us-east-1:000000000000:log-group:/s9/deldest",
					),
				},
				DeliveryDestinationType: cwltypes.DeliveryDestinationTypeCwl,
			},
		)
		require.NoError(t, err)

		createOut, err := client.CreateDelivery(t.Context(), &cwlsdk.CreateDeliveryInput{
			DeliverySourceName:     aws.String("s9-del-src"),
			DeliveryDestinationArn: destOut.DeliveryDestination.Arn,
		})
		require.NoError(t, err)
		deliveryID := aws.ToString(createOut.Delivery.Id)
		require.NotEmpty(t, deliveryID)

		getOut, err := client.GetDelivery(
			t.Context(),
			&cwlsdk.GetDeliveryInput{Id: aws.String(deliveryID)},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9-del-src", aws.ToString(getOut.Delivery.DeliverySourceName))

		listOut, err := client.DescribeDeliveries(t.Context(), &cwlsdk.DescribeDeliveriesInput{})
		require.NoError(t, err)
		assert.NotEmpty(t, listOut.Deliveries)

		_, err = client.UpdateDeliveryConfiguration(
			t.Context(),
			&cwlsdk.UpdateDeliveryConfigurationInput{
				Id:             aws.String(deliveryID),
				FieldDelimiter: aws.String(","),
			},
		)
		require.NoError(t, err)

		getAfter, err := client.GetDelivery(
			t.Context(),
			&cwlsdk.GetDeliveryInput{Id: aws.String(deliveryID)},
		)
		require.NoError(t, err)
		assert.Equal(t, ",", aws.ToString(getAfter.Delivery.FieldDelimiter))

		_, err = client.DeleteDelivery(
			t.Context(),
			&cwlsdk.DeleteDeliveryInput{Id: aws.String(deliveryID)},
		)
		require.NoError(t, err)
	})

	t.Run("integrations", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		putOut, err := client.PutIntegration(t.Context(), &cwlsdk.PutIntegrationInput{
			IntegrationName: aws.String("s9-integration"),
			IntegrationType: cwltypes.IntegrationTypeOpensearch,
			ResourceConfig: &cwltypes.ResourceConfigMemberOpenSearchResourceConfig{
				Value: cwltypes.OpenSearchResourceConfig{
					DataSourceRoleArn: aws.String(
						"arn:aws:iam::000000000000:role/s9-os-role",
					),
					DashboardViewerPrincipals: []string{"arn:aws:iam::000000000000:role/s9-viewer"},
					RetentionDays:             aws.Int32(7),
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "s9-integration", aws.ToString(putOut.IntegrationName))

		getOut, err := client.GetIntegration(t.Context(), &cwlsdk.GetIntegrationInput{
			IntegrationName: aws.String("s9-integration"),
		})
		require.NoError(t, err)
		assert.Equal(t, cwltypes.IntegrationTypeOpensearch, getOut.IntegrationType)

		listOut, err := client.ListIntegrations(t.Context(), &cwlsdk.ListIntegrationsInput{})
		require.NoError(t, err)
		assert.NotEmpty(t, listOut.IntegrationSummaries)

		_, err = client.DeleteIntegration(t.Context(), &cwlsdk.DeleteIntegrationInput{
			IntegrationName: aws.String("s9-integration"),
		})
		require.NoError(t, err)
	})

	t.Run("lookup tables", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		createOut, err := client.CreateLookupTable(t.Context(), &cwlsdk.CreateLookupTableInput{
			LookupTableName: aws.String("s9LookupTable"),
			TableBody:       aws.String("code,name\n1,one\n2,two\n"),
		})
		require.NoError(t, err)
		lookupArn := aws.ToString(createOut.LookupTableArn)
		require.NotEmpty(t, lookupArn)

		descOut, err := client.DescribeLookupTables(t.Context(), &cwlsdk.DescribeLookupTablesInput{
			LookupTableNamePrefix: aws.String("s9LookupTable"),
		})
		require.NoError(t, err)
		require.Len(t, descOut.LookupTables, 1)
		assert.Equal(t, int64(2), aws.ToInt64(descOut.LookupTables[0].RecordsCount))

		updOut, err := client.UpdateLookupTable(t.Context(), &cwlsdk.UpdateLookupTableInput{
			LookupTableArn: aws.String(lookupArn),
			TableBody:      aws.String("code,name\n1,one\n2,two\n3,three\n"),
		})
		require.NoError(t, err)
		assert.Equal(t, lookupArn, aws.ToString(updOut.LookupTableArn))

		_, err = client.DeleteLookupTable(t.Context(), &cwlsdk.DeleteLookupTableInput{
			LookupTableArn: aws.String(lookupArn),
		})
		require.NoError(t, err)
	})

	t.Run("metric filters", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/metricfilter"),
		})
		require.NoError(t, err)

		_, err = client.PutMetricFilter(t.Context(), &cwlsdk.PutMetricFilterInput{
			LogGroupName:  aws.String("/s9/metricfilter"),
			FilterName:    aws.String("s9-filter"),
			FilterPattern: aws.String("ERROR"),
			MetricTransformations: []cwltypes.MetricTransformation{
				{
					MetricName:      aws.String("s9Errors"),
					MetricNamespace: aws.String("S9"),
					MetricValue:     aws.String("1"),
				},
			},
		})
		require.NoError(t, err)

		testOut, err := client.TestMetricFilter(t.Context(), &cwlsdk.TestMetricFilterInput{
			FilterPattern:    aws.String("ERROR"),
			LogEventMessages: []string{"ERROR something broke", "all fine"},
		})
		require.NoError(t, err)
		require.Len(t, testOut.Matches, 1)
		assert.Equal(t, "ERROR something broke", aws.ToString(testOut.Matches[0].EventMessage))

		_, err = client.DeleteMetricFilter(t.Context(), &cwlsdk.DeleteMetricFilterInput{
			LogGroupName: aws.String("/s9/metricfilter"),
			FilterName:   aws.String("s9-filter"),
		})
		require.NoError(t, err)
	})

	t.Run("query definitions", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		putOut, err := client.PutQueryDefinition(t.Context(), &cwlsdk.PutQueryDefinitionInput{
			Name:        aws.String("s9-query-def"),
			QueryString: aws.String("fields @message"),
		})
		require.NoError(t, err)
		queryDefID := aws.ToString(putOut.QueryDefinitionId)
		require.NotEmpty(t, queryDefID)

		_, err = client.DeleteQueryDefinition(t.Context(), &cwlsdk.DeleteQueryDefinitionInput{
			QueryDefinitionId: aws.String(queryDefID),
		})
		require.NoError(t, err)
	})

	t.Run("index policies and catalogs", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/indexpolicy"),
		})
		require.NoError(t, err)

		_, err = client.PutIndexPolicy(t.Context(), &cwlsdk.PutIndexPolicyInput{
			LogGroupIdentifier: aws.String("/s9/indexpolicy"),
			PolicyDocument:     aws.String(`{"Fields":["eventType"]}`),
		})
		require.NoError(t, err)

		_, err = client.DeleteIndexPolicy(t.Context(), &cwlsdk.DeleteIndexPolicyInput{
			LogGroupIdentifier: aws.String("/s9/indexpolicy"),
		})
		require.NoError(t, err)

		fieldOut, err := client.DescribeFieldIndexes(t.Context(), &cwlsdk.DescribeFieldIndexesInput{
			LogGroupIdentifiers: []string{"/s9/indexpolicy"},
		})
		require.NoError(t, err)
		assert.NotNil(t, fieldOut.FieldIndexes)

		templatesOut, err := client.DescribeConfigurationTemplates(
			t.Context(),
			&cwlsdk.DescribeConfigurationTemplatesInput{},
		)
		require.NoError(t, err)
		assert.NotNil(t, templatesOut.ConfigurationTemplates)
	})

	t.Run("syslog configurations", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/syslog"),
		})
		require.NoError(t, err)

		_, err = client.PutSyslogConfiguration(t.Context(), &cwlsdk.PutSyslogConfigurationInput{
			LogGroupIdentifier: aws.String("/s9/syslog"),
			VpcEndpointId:      aws.String("vpce-s9"),
		})
		require.NoError(t, err)

		listOut, err := client.ListSyslogConfigurations(
			t.Context(),
			&cwlsdk.ListSyslogConfigurationsInput{
				LogGroupIdentifier: aws.String("/s9/syslog"),
			},
		)
		require.NoError(t, err)
		require.Len(t, listOut.SyslogConfigurations, 1)
		assert.Equal(t, "vpce-s9", aws.ToString(listOut.SyslogConfigurations[0].VpcEndpointId))

		_, err = client.DeleteSyslogConfiguration(
			t.Context(),
			&cwlsdk.DeleteSyslogConfigurationInput{
				LogGroupIdentifier: aws.String("/s9/syslog"),
				VpcEndpointId:      aws.String("vpce-s9"),
			},
		)
		require.NoError(t, err)
	})

	t.Run("transformer", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/transformer"),
		})
		require.NoError(t, err)

		_, err = client.PutTransformer(t.Context(), &cwlsdk.PutTransformerInput{
			LogGroupIdentifier: aws.String("/s9/transformer"),
			TransformerConfig: []cwltypes.Processor{
				{
					ParseJSON: &cwltypes.ParseJSON{Source: aws.String("@message")},
				},
			},
		})
		require.NoError(t, err)

		_, err = client.DeleteTransformer(t.Context(), &cwlsdk.DeleteTransformerInput{
			LogGroupIdentifier: aws.String("/s9/transformer"),
		})
		require.NoError(t, err)
	})

	t.Run("storage tier policy", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		putOut, err := client.PutStorageTierPolicy(t.Context(), &cwlsdk.PutStorageTierPolicyInput{
			StorageTier: cwltypes.StorageTierIntelligentTiering,
		})
		require.NoError(t, err)
		assert.Equal(t, cwltypes.StorageTierIntelligentTiering, putOut.StorageTier)

		getOut, err := client.GetStorageTierPolicy(t.Context(), &cwlsdk.GetStorageTierPolicyInput{})
		require.NoError(t, err)
		assert.Equal(t, cwltypes.StorageTierIntelligentTiering, getOut.StorageTier)
	})

	t.Run("scheduled queries", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateScheduledQuery(t.Context(), &cwlsdk.CreateScheduledQueryInput{
			Name:                aws.String("s9-scheduled-query"),
			QueryString:         aws.String("fields @message"),
			QueryLanguage:       cwltypes.QueryLanguageCwli,
			ScheduleExpression:  aws.String("rate(1 hour)"),
			ExecutionRoleArn:    aws.String("arn:aws:iam::000000000000:role/s9-sq-role"),
			LogGroupIdentifiers: []string{"/s9/scheduled"},
		})
		require.NoError(t, err)

		listOut, err := client.ListScheduledQueries(
			t.Context(),
			&cwlsdk.ListScheduledQueriesInput{},
		)
		require.NoError(t, err)
		require.NotEmpty(t, listOut.ScheduledQueries)

		var found bool
		for _, sq := range listOut.ScheduledQueries {
			if aws.ToString(sq.Name) == "s9-scheduled-query" {
				found = true
			}
		}
		assert.True(t, found)
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/tags"),
		})
		require.NoError(t, err)

		// TagLogGroup/ListTagsLogGroup/UntagLogGroup are deprecated in favor
		// of the generic TagResource/ListTagsForResource/UntagResource, but
		// still real, dispatched ops with their own uncovered-op entries --
		// deliberately exercised here, not an oversight.
		_, err = client.TagLogGroup( //nolint:staticcheck // deliberate legacy-op coverage
			t.Context(),
			&cwlsdk.TagLogGroupInput{
				LogGroupName: aws.String("/s9/tags"),
				Tags:         map[string]string{"team": "s9"},
			},
		)
		require.NoError(t, err)

		listOut, err := client.ListTagsLogGroup( //nolint:staticcheck // deliberate legacy-op coverage
			t.Context(),
			&cwlsdk.ListTagsLogGroupInput{LogGroupName: aws.String("/s9/tags")},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9", listOut.Tags["team"])

		_, err = client.UntagLogGroup( //nolint:staticcheck // deliberate legacy-op coverage
			t.Context(),
			&cwlsdk.UntagLogGroupInput{
				LogGroupName: aws.String("/s9/tags"),
				Tags:         []string{"team"},
			},
		)
		require.NoError(t, err)

		listAfter, err := client.ListTagsLogGroup( //nolint:staticcheck // deliberate legacy-op coverage
			t.Context(),
			&cwlsdk.ListTagsLogGroupInput{LogGroupName: aws.String("/s9/tags")},
		)
		require.NoError(t, err)
		assert.Empty(t, listAfter.Tags)

		resourceArn := "arn:aws:logs:us-east-1:000000000000:log-group:/s9/tags"

		_, err = client.TagResource(t.Context(), &cwlsdk.TagResourceInput{
			ResourceArn: aws.String(resourceArn),
			Tags:        map[string]string{"owner": "s9"},
		})
		require.NoError(t, err)

		listResource, err := client.ListTagsForResource(
			t.Context(),
			&cwlsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(resourceArn),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9", listResource.Tags["owner"])

		_, err = client.UntagResource(t.Context(), &cwlsdk.UntagResourceInput{
			ResourceArn: aws.String(resourceArn),
			TagKeys:     []string{"owner"},
		})
		require.NoError(t, err)

		listResourceAfter, err := client.ListTagsForResource(
			t.Context(),
			&cwlsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(resourceArn),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, listResourceAfter.Tags)
	})

	t.Run("bearer token authentication", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/bearer"),
		})
		require.NoError(t, err)

		_, err = client.PutBearerTokenAuthentication(
			t.Context(),
			&cwlsdk.PutBearerTokenAuthenticationInput{
				LogGroupIdentifier:               aws.String("/s9/bearer"),
				BearerTokenAuthenticationEnabled: aws.Bool(true),
			},
		)
		require.NoError(t, err)
	})

	t.Run("log anomaly detector delete", func(t *testing.T) {
		t.Parallel()

		client := newSlice9CloudWatchLogsClient(t)

		_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
			LogGroupName: aws.String("/s9/anomaly"),
		})
		require.NoError(t, err)

		createOut, err := client.CreateLogAnomalyDetector(
			t.Context(),
			&cwlsdk.CreateLogAnomalyDetectorInput{
				LogGroupArnList: []string{
					"arn:aws:logs:us-east-1:000000000000:log-group:/s9/anomaly:*",
				},
				DetectorName: aws.String("s9-anomaly-detector"),
			},
		)
		require.NoError(t, err)
		detectorArn := aws.ToString(createOut.AnomalyDetectorArn)
		require.NotEmpty(t, detectorArn)

		_, err = client.DeleteLogAnomalyDetector(t.Context(), &cwlsdk.DeleteLogAnomalyDetectorInput{
			AnomalyDetectorArn: aws.String(detectorArn),
		})
		require.NoError(t, err)
	})
}

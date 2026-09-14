package dms_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ResourceIdentifier drives CreateEndpoint/CreateReplicationInstance/
// CreateReplicationTask's ResourceIdentifier field (reqfielddiff,
// gopherstack-xhu2t) through the real client and asserts it lands at the end
// of the returned ARN, matching api_op_Create*.go's documented behavior
// ("A friendly name for the resource identifier at the end of the ...Arn
// response parameter").
func TestRealClient_ResourceIdentifier(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "create_endpoint_uses_resource_identifier_in_arn", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			out, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("ri-endpoint"),
				EndpointType:       types.ReplicationEndpointTypeValueSource,
				EngineName:         aws.String("mysql"),
				ResourceIdentifier: aws.String("My-Custom-Suffix"),
			})
			require.NoError(t, err)
			assert.True(t, strings.HasSuffix(aws.ToString(out.Endpoint.EndpointArn), "My-Custom-Suffix"))
		}},
		{name: "create_replication_instance_uses_resource_identifier_in_arn", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			out, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
				ReplicationInstanceIdentifier: aws.String("ri-instance"),
				ReplicationInstanceClass:      aws.String("dms.t3.micro"),
				ResourceIdentifier:            aws.String("My-RI-Suffix"),
			})
			require.NoError(t, err)
			assert.True(
				t,
				strings.HasSuffix(aws.ToString(out.ReplicationInstance.ReplicationInstanceArn), "My-RI-Suffix"),
			)
		}},
		{name: "create_replication_task_uses_resource_identifier_in_arn", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			src, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("ri-task-src"),
				EndpointType:       types.ReplicationEndpointTypeValueSource,
				EngineName:         aws.String("mysql"),
			})
			require.NoError(t, err)

			tgt, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("ri-task-tgt"),
				EndpointType:       types.ReplicationEndpointTypeValueTarget,
				EngineName:         aws.String("mysql"),
			})
			require.NoError(t, err)

			inst, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
				ReplicationInstanceIdentifier: aws.String("ri-task-inst"),
				ReplicationInstanceClass:      aws.String("dms.t3.micro"),
			})
			require.NoError(t, err)

			out, err := client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
				ReplicationTaskIdentifier: aws.String("ri-task"),
				SourceEndpointArn:         src.Endpoint.EndpointArn,
				TargetEndpointArn:         tgt.Endpoint.EndpointArn,
				ReplicationInstanceArn:    inst.ReplicationInstance.ReplicationInstanceArn,
				MigrationType:             types.MigrationTypeValueFullLoad,
				TableMappings:             aws.String(`{"rules":[]}`),
				ResourceIdentifier:        aws.String("My-Task-Suffix"),
			})
			require.NoError(t, err)
			assert.True(
				t,
				strings.HasSuffix(aws.ToString(out.ReplicationTask.ReplicationTaskArn), "My-Task-Suffix"),
			)
		}},
		{name: "resource_identifier_omitted_falls_back_to_generated_value", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			out, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("ri-endpoint-default"),
				EndpointType:       types.ReplicationEndpointTypeValueSource,
				EngineName:         aws.String("mysql"),
			})
			require.NoError(t, err)
			assert.False(t, strings.HasSuffix(aws.ToString(out.Endpoint.EndpointArn), "ri-endpoint-default"))
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_DescribeReplicationTasksWithoutSettings proves
// DescribeReplicationTasksInput.WithoutSettings (dropped: the field was not
// even declared on the wire type) suppresses ReplicationTaskSettings in the
// response, matching DescribeDataMigrations's already-fixed sibling field.
func TestRealClient_DescribeReplicationTasksWithoutSettings(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	src, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("wos-src"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	tgt, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("wos-tgt"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	inst, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("wos-inst"),
		ReplicationInstanceClass:      aws.String("dms.t3.micro"),
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
		ReplicationTaskIdentifier: aws.String("wos-task"),
		SourceEndpointArn:         src.Endpoint.EndpointArn,
		TargetEndpointArn:         tgt.Endpoint.EndpointArn,
		ReplicationInstanceArn:    inst.ReplicationInstance.ReplicationInstanceArn,
		MigrationType:             types.MigrationTypeValueFullLoad,
		TableMappings:             aws.String(`{"rules":[]}`),
		ReplicationTaskSettings:   aws.String(`{"Logging":{}}`),
	})
	require.NoError(t, err)

	withSettings, err := client.DescribeReplicationTasks(t.Context(), &dmssdk.DescribeReplicationTasksInput{})
	require.NoError(t, err)
	require.Len(t, withSettings.ReplicationTasks, 1)
	assert.NotEmpty(t, aws.ToString(withSettings.ReplicationTasks[0].ReplicationTaskSettings))

	withoutSettings, err := client.DescribeReplicationTasks(t.Context(), &dmssdk.DescribeReplicationTasksInput{
		WithoutSettings: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, withoutSettings.ReplicationTasks, 1)
	assert.Empty(t, aws.ToString(withoutSettings.ReplicationTasks[0].ReplicationTaskSettings))
}

// TestRealClient_StartReplicationTaskAssessmentRunResultEncryptionMode proves
// ResultEncryptionMode (dropped parameter: the field was not declared at
// all) is validated, defaults to SSE_S3, and round-trips on the response.
func TestRealClient_StartReplicationTaskAssessmentRunResultEncryptionMode(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) (*dmssdk.Client, string) {
		t.Helper()

		h := newTestDMSHandler()
		client := newTestDMSClient(t, h)

		src, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
			EndpointIdentifier: aws.String("rem-src"),
			EndpointType:       types.ReplicationEndpointTypeValueSource,
			EngineName:         aws.String("mysql"),
		})
		require.NoError(t, err)

		tgt, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
			EndpointIdentifier: aws.String("rem-tgt"),
			EndpointType:       types.ReplicationEndpointTypeValueTarget,
			EngineName:         aws.String("mysql"),
		})
		require.NoError(t, err)

		inst, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
			ReplicationInstanceIdentifier: aws.String("rem-inst"),
			ReplicationInstanceClass:      aws.String("dms.t3.micro"),
		})
		require.NoError(t, err)

		task, err := client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
			ReplicationTaskIdentifier: aws.String("rem-task"),
			SourceEndpointArn:         src.Endpoint.EndpointArn,
			TargetEndpointArn:         tgt.Endpoint.EndpointArn,
			ReplicationInstanceArn:    inst.ReplicationInstance.ReplicationInstanceArn,
			MigrationType:             types.MigrationTypeValueFullLoad,
			TableMappings:             aws.String(`{"rules":[]}`),
		})
		require.NoError(t, err)

		return client, aws.ToString(task.ReplicationTask.ReplicationTaskArn)
	}
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "defaults_to_sse_s3", run: func(t *testing.T) {
			t.Helper()

			client, taskArn := setup(t)

			out, err := client.StartReplicationTaskAssessmentRun(
				t.Context(),
				&dmssdk.StartReplicationTaskAssessmentRunInput{
					ReplicationTaskArn:   aws.String(taskArn),
					ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/dms-assess"),
					ResultLocationBucket: aws.String("dms-assess-bucket"),
					AssessmentRunName:    aws.String("run-default"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "SSE_S3", aws.ToString(out.ReplicationTaskAssessmentRun.ResultEncryptionMode))
		}},
		{name: "honors_sse_kms", run: func(t *testing.T) {
			t.Helper()

			client, taskArn := setup(t)

			out, err := client.StartReplicationTaskAssessmentRun(
				t.Context(),
				&dmssdk.StartReplicationTaskAssessmentRunInput{
					ReplicationTaskArn:   aws.String(taskArn),
					ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/dms-assess"),
					ResultLocationBucket: aws.String("dms-assess-bucket"),
					AssessmentRunName:    aws.String("run-kms"),
					ResultEncryptionMode: aws.String("SSE_KMS"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "SSE_KMS", aws.ToString(out.ReplicationTaskAssessmentRun.ResultEncryptionMode))
		}},
		{name: "rejects_invalid_mode", run: func(t *testing.T) {
			t.Helper()

			client, taskArn := setup(t)

			_, err := client.StartReplicationTaskAssessmentRun(
				t.Context(),
				&dmssdk.StartReplicationTaskAssessmentRunInput{
					ReplicationTaskArn:   aws.String(taskArn),
					ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/dms-assess"),
					ResultLocationBucket: aws.String("dms-assess-bucket"),
					AssessmentRunName:    aws.String("run-bad"),
					ResultEncryptionMode: aws.String("bogus"),
				},
			)
			require.Error(t, err)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

package terraform_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsvc "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	iamsvc "github.com/aws/aws-sdk-go-v2/service/iam"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	route53svc "github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	secretssvc "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	snssvc "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssvc "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// planResult holds the outcome of a `tofu plan` invocation.
type planResult struct {
	HasChanges bool
}

// tofuEnv returns the environment variables required for all OpenTofu subprocess
// invocations: automation mode, shared provider-cache path, and the lock-file
// break flag required when multiple tests share the cache.
func tofuEnv() []string {
	return append(os.Environ(),
		"TF_IN_AUTOMATION=1",
		"TF_PLUGIN_CACHE_DIR="+tofuProviderCacheDir,
		"TF_PLUGIN_CACHE_MAY_BREAK_DEPENDENCY_LOCK_FILE=true",
	)
}

// execTofuCmd runs a single OpenTofu sub-command inside dir and returns the combined
// stdout+stderr output together with any process error. It uses CommandContext so
// the subprocess is cancelled promptly when the test context expires.
func execTofuCmd(ctx context.Context, t *testing.T, tofuBin, dir string, args ...string) ([]byte, error) {
	t.Helper()

	cmd := exec.CommandContext(ctx, tofuBin, args...)
	cmd.Dir = dir
	cmd.Env = tofuEnv()

	return cmd.CombinedOutput()
}

// runTofuPlan runs `tofu plan -detailed-exitcode` in dir and reports whether
// Terraform detected any pending changes.
//
// Exit-code semantics (OpenTofu/Terraform):
//   - 0 = no changes
//   - 1 = error
//   - 2 = changes detected
func runTofuPlan(ctx context.Context, t *testing.T, tofuBin, dir string) planResult {
	t.Helper()

	out, err := execTofuCmd(ctx, t, tofuBin, dir, "plan", "-detailed-exitcode", "-no-color")
	t.Logf("tofu plan:\n%s", out)

	if err == nil {
		return planResult{HasChanges: false}
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 2 {
		return planResult{HasChanges: true}
	}

	require.NoError(t, err, "tofu plan failed with unexpected error")

	return planResult{HasChanges: false}
}

// driftTestCase describes a single Terraform drift detection scenario.
type driftTestCase struct {
	// setup initialises per-test variables (resource names, zip paths, …) and
	// returns them as a map for use by the other hooks.
	setup func(t *testing.T, dir string) map[string]any

	// mutate uses the AWS SDK to modify backend state directly, simulating drift.
	mutate func(t *testing.T, ctx context.Context, vars map[string]any)

	// verifyAfter checks that the backend state has been reconciled to the
	// Terraform-defined values after the second apply.
	verifyAfter func(t *testing.T, ctx context.Context, vars map[string]any)

	// name is the sub-test name displayed in test output.
	name string

	// fixture is the path under fixtures/ for the HCL template (without .tf).
	fixture string
}

// runDriftTest is the common runner for all drift detection tests.
//
// It follows a five-step sequence:
//  1. terraform apply  — creates resources
//  2. SDK mutate       — introduces drift
//  3. terraform plan   — detects drift (HasChanges must be true)
//  4. terraform apply  — reconciles drift
//  5. SDK verify       — state matches Terraform intent
func runDriftTest(t *testing.T, tc driftTestCase) {
	t.Helper()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := t.Context()
	dir := t.TempDir()

	vars := tc.setup(t, dir)
	hcl := providerBlock(endpoint) + renderFixture(t, tc.fixture, vars)

	// Step 1: initial apply — create resources and register cleanup.
	applyTofu(t, tofuBin, dir, hcl)

	// Step 2: mutate via SDK to simulate drift.
	tc.mutate(t, ctx, vars)

	// Step 3: plan should detect drift.
	plan := runTofuPlan(ctx, t, tofuBin, dir)
	assert.True(t, plan.HasChanges, "tofu plan should detect infrastructure drift")

	// Step 4: apply to correct drift.
	correctOut, correctErr := execTofuCmd(ctx, t, tofuBin, dir, "apply", "-auto-approve", "-no-color")
	t.Logf("tofu apply (drift correction):\n%s", correctOut)
	require.NoError(t, correctErr, "tofu apply should correct drift")

	// Step 5: verify state is back to Terraform-defined values.
	if tc.verifyAfter != nil {
		tc.verifyAfter(t, ctx, vars)
	}
}

// TestTerraformDrift_DynamoDB verifies that Terraform detects and corrects drift
// when DynamoDB provisioned throughput is changed directly via the SDK.
//
// The fixture uses PROVISIONED billing mode (read_capacity=5, write_capacity=5).
// The backend always returns BillingModeSummary=PROVISIONED so using PROVISIONED
// in the fixture ensures no spurious plan changes unrelated to the drift mutation.
// The mutation bumps read_capacity to 10; the verification confirms it is restored
// to 5 after the correction apply.
func TestTerraformDrift_DynamoDB(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "provisioned_throughput",
			fixture: "dynamodb/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"TableName": "tf-drift-ddb-" + uuid.NewString()[:8]}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createDynamoDBClient(t)
				_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
					TableName: aws.String(vars["TableName"].(string)),
					ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(10),
						WriteCapacityUnits: aws.Int64(5),
					},
				})
				require.NoError(t, err, "UpdateTable should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createDynamoDBClient(t)
				out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
					TableName: aws.String(vars["TableName"].(string)),
				})
				require.NoError(t, err, "DescribeTable should succeed after drift correction")
				require.NotNil(t, out.Table)
				require.NotNil(t, out.Table.ProvisionedThroughput)
				assert.EqualValues(t, 5, aws.ToInt64(out.Table.ProvisionedThroughput.ReadCapacityUnits),
					"read_capacity should be restored to 5 after drift correction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

// TestTerraformDrift_SQS verifies that Terraform detects and corrects drift when
// the SQS visibility timeout is changed directly via the SDK.
func TestTerraformDrift_SQS(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "visibility_timeout",
			fixture: "sqs/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"QueueName": "tf-drift-sqs-" + uuid.NewString()[:8]}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createSQSClient(t)
				urlOut, err := client.GetQueueUrl(ctx, &sqssvc.GetQueueUrlInput{
					QueueName: aws.String(vars["QueueName"].(string)),
				})
				require.NoError(t, err, "GetQueueUrl should succeed")

				_, err = client.SetQueueAttributes(ctx, &sqssvc.SetQueueAttributesInput{
					QueueUrl: urlOut.QueueUrl,
					Attributes: map[string]string{
						string(sqstypes.QueueAttributeNameVisibilityTimeout): "60",
					},
				})
				require.NoError(t, err, "SetQueueAttributes should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createSQSClient(t)
				urlOut, err := client.GetQueueUrl(ctx, &sqssvc.GetQueueUrlInput{
					QueueName: aws.String(vars["QueueName"].(string)),
				})
				require.NoError(t, err, "GetQueueUrl should succeed after drift correction")

				attrOut, err := client.GetQueueAttributes(ctx, &sqssvc.GetQueueAttributesInput{
					QueueUrl:       urlOut.QueueUrl,
					AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameVisibilityTimeout},
				})
				require.NoError(t, err, "GetQueueAttributes should succeed after drift correction")
				assert.Equal(t, "30", attrOut.Attributes[string(sqstypes.QueueAttributeNameVisibilityTimeout)],
					"visibility_timeout should be restored to 30 after drift correction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

// TestTerraformDrift_SNS verifies that Terraform detects and corrects drift when
// the SNS topic display name is changed directly via the SDK.
func TestTerraformDrift_SNS(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "display_name",
			fixture: "sns/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"TopicName": "tf-drift-sns-" + uuid.NewString()[:8]}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				topicARN := "arn:aws:sns:us-east-1:000000000000:" + vars["TopicName"].(string)
				client := createSNSClient(t)
				_, err := client.SetTopicAttributes(ctx, &snssvc.SetTopicAttributesInput{
					TopicArn:       aws.String(topicARN),
					AttributeName:  aws.String("DisplayName"),
					AttributeValue: aws.String("drifted-name"),
				})
				require.NoError(t, err, "SetTopicAttributes should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				topicARN := "arn:aws:sns:us-east-1:000000000000:" + vars["TopicName"].(string)
				client := createSNSClient(t)
				out, err := client.GetTopicAttributes(ctx, &snssvc.GetTopicAttributesInput{
					TopicArn: aws.String(topicARN),
				})
				require.NoError(t, err, "GetTopicAttributes should succeed after drift correction")
				assert.Equal(t, "initial-name", out.Attributes["DisplayName"],
					"display_name should be restored to 'initial-name' after drift correction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

// TestTerraformDrift_Lambda verifies that Terraform detects and corrects drift
// when the Lambda function memory size is changed directly via the SDK.
func TestTerraformDrift_Lambda(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "memory_size",
			fixture: "lambda/success",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()

				id := uuid.NewString()[:8]

				var buf bytes.Buffer
				zw := zip.NewWriter(&buf)
				f, err := zw.Create("index.py")
				require.NoError(t, err)
				_, err = f.Write([]byte("def handler(event, context):\n    return {}\n"))
				require.NoError(t, err)
				require.NoError(t, zw.Close())

				zipPath := filepath.Join(dir, "function.zip")
				require.NoError(t, os.WriteFile(zipPath, buf.Bytes(), 0o644))

				return map[string]any{
					"FuncName": "tf-drift-lambda-" + id,
					"RoleName": "tf-drift-lambda-role-" + id,
					"ZipPath":  zipPath,
				}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createLambdaClient(t)
				_, err := client.UpdateFunctionConfiguration(ctx, &lambdasvc.UpdateFunctionConfigurationInput{
					FunctionName: aws.String(vars["FuncName"].(string)),
					MemorySize:   aws.Int32(256),
				})
				require.NoError(t, err, "UpdateFunctionConfiguration should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createLambdaClient(t)
				out, err := client.GetFunction(ctx, &lambdasvc.GetFunctionInput{
					FunctionName: aws.String(vars["FuncName"].(string)),
				})
				require.NoError(t, err, "GetFunction should succeed after drift correction")
				require.NotNil(t, out.Configuration)
				assert.EqualValues(t, 128, aws.ToInt32(out.Configuration.MemorySize),
					"memory_size should be restored to 128 after drift correction")
				assert.Equal(t, lambdatypes.StateActive, out.Configuration.State,
					"function should be in Active state after drift correction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

// TestTerraformDrift_IAM verifies that Terraform detects and corrects drift when
// the IAM role trust policy is changed directly via the SDK.
func TestTerraformDrift_IAM(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "assume_role_policy",
			fixture: "iam/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				id := uuid.NewString()[:8]

				return map[string]any{
					"RoleName":   "tf-drift-role-" + id,
					"PolicyName": "tf-drift-policy-" + id,
				}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				// Change the trust policy to allow ec2 instead of lambda — simulates drift.
				driftedPolicy := `{"Version":"2012-10-17","Statement":[` +
					`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},` +
					`"Action":"sts:AssumeRole"}]}`
				client := createIAMClient(t)
				_, err := client.UpdateAssumeRolePolicy(ctx, &iamsvc.UpdateAssumeRolePolicyInput{
					RoleName:       aws.String(vars["RoleName"].(string)),
					PolicyDocument: aws.String(driftedPolicy),
				})
				require.NoError(t, err, "UpdateAssumeRolePolicy should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createIAMClient(t)
				out, err := client.GetRole(ctx, &iamsvc.GetRoleInput{
					RoleName: aws.String(vars["RoleName"].(string)),
				})
				require.NoError(t, err, "GetRole should succeed after drift correction")
				require.NotNil(t, out.Role)

				decoded, err := url.QueryUnescape(aws.ToString(out.Role.AssumeRolePolicyDocument))
				require.NoError(t, err, "trust policy should be decodable")
				assert.Contains(t, decoded, "lambda.amazonaws.com",
					"trust policy should be restored to allow lambda after drift correction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

// TestTerraformDrift_Route53 verifies that Terraform detects and corrects drift
// when a DNS record TTL is changed directly via the SDK.
func TestTerraformDrift_Route53(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "record_ttl",
			fixture: "route53/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"ZoneName": "tf-drift-" + uuid.NewString()[:8] + ".example.com"}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				zoneName := vars["ZoneName"].(string)
				client := createRoute53Client(t)

				// Find the zone ID by listing zones.
				zonesOut, err := client.ListHostedZones(ctx, &route53svc.ListHostedZonesInput{})
				require.NoError(t, err, "ListHostedZones should succeed")

				var zoneID string

				for _, z := range zonesOut.HostedZones {
					if aws.ToString(z.Name) == zoneName+"." || aws.ToString(z.Name) == zoneName {
						zoneID = aws.ToString(z.Id)

						break
					}
				}

				require.NotEmpty(t, zoneID, "hosted zone should exist")

				// Change the A record TTL from 300 to 60 — simulates drift.
				_, err = client.ChangeResourceRecordSets(ctx, &route53svc.ChangeResourceRecordSetsInput{
					HostedZoneId: aws.String(zoneID),
					ChangeBatch: &r53types.ChangeBatch{
						Changes: []r53types.Change{
							{
								Action: r53types.ChangeActionUpsert,
								ResourceRecordSet: &r53types.ResourceRecordSet{
									Name: aws.String("www." + zoneName),
									Type: r53types.RRTypeA,
									TTL:  aws.Int64(60),
									ResourceRecords: []r53types.ResourceRecord{
										{Value: aws.String("1.2.3.4")},
									},
								},
							},
						},
					},
				})
				require.NoError(t, err, "ChangeResourceRecordSets should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				zoneName := vars["ZoneName"].(string)
				client := createRoute53Client(t)

				zonesOut, err := client.ListHostedZones(ctx, &route53svc.ListHostedZonesInput{})
				require.NoError(t, err, "ListHostedZones should succeed after drift correction")

				var zoneID string

				for _, z := range zonesOut.HostedZones {
					if aws.ToString(z.Name) == zoneName+"." || aws.ToString(z.Name) == zoneName {
						zoneID = aws.ToString(z.Id)

						break
					}
				}

				require.NotEmpty(t, zoneID, "hosted zone should still exist after drift correction")

				recordsOut, err := client.ListResourceRecordSets(ctx, &route53svc.ListResourceRecordSetsInput{
					HostedZoneId: aws.String(zoneID),
				})
				require.NoError(t, err, "ListResourceRecordSets should succeed after drift correction")

				var ttl int64

				for _, rrs := range recordsOut.ResourceRecordSets {
					if strings.TrimSuffix(aws.ToString(rrs.Name), ".") == "www."+strings.TrimSuffix(zoneName, ".") {
						ttl = aws.ToInt64(rrs.TTL)

						break
					}
				}

				assert.EqualValues(t, 300, ttl,
					"A record TTL should be restored to 300 after drift correction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

// TestTerraformDrift_CloudWatch verifies that Terraform detects and corrects
// drift when an alarm threshold is changed directly via the SDK.
func TestTerraformDrift_CloudWatch(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "alarm_threshold",
			fixture: "cloudwatch/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"AlarmName": "tf-drift-alarm-" + uuid.NewString()[:8]}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createCloudWatchClient(t)
				// Re-publish the alarm with a drifted threshold (99 instead of 80).
				_, err := client.PutMetricAlarm(ctx, &cwsvc.PutMetricAlarmInput{
					AlarmName:          aws.String(vars["AlarmName"].(string)),
					ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
					EvaluationPeriods:  aws.Int32(1),
					MetricName:         aws.String("CPUUtilization"),
					Namespace:          aws.String("AWS/EC2"),
					Period:             aws.Int32(60),
					Statistic:          cwtypes.StatisticAverage,
					Threshold:          aws.Float64(99),
				})
				require.NoError(t, err, "PutMetricAlarm should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createCloudWatchClient(t)
				out, err := client.DescribeAlarms(ctx, &cwsvc.DescribeAlarmsInput{
					AlarmNames: []string{vars["AlarmName"].(string)},
				})
				require.NoError(t, err, "DescribeAlarms should succeed after drift correction")
				require.Len(t, out.MetricAlarms, 1)
				assert.InDelta(t, 80.0, aws.ToFloat64(out.MetricAlarms[0].Threshold), 0.001,
					"threshold should be restored to 80 after drift correction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

// TestTerraformDrift_SecretsManager verifies that Terraform detects and corrects
// drift when a secret's description is changed directly via the SDK.
func TestTerraformDrift_SecretsManager(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "description",
			fixture: "secretsmanager/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"SecretName": "tf-drift-secret-" + uuid.NewString()[:8]}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createSecretsManagerClient(t)
				_, err := client.UpdateSecret(ctx, &secretssvc.UpdateSecretInput{
					SecretId:    aws.String(vars["SecretName"].(string)),
					Description: aws.String("drifted description"),
				})
				require.NoError(t, err, "UpdateSecret should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createSecretsManagerClient(t)
				out, err := client.DescribeSecret(ctx, &secretssvc.DescribeSecretInput{
					SecretId: aws.String(vars["SecretName"].(string)),
				})
				require.NoError(t, err, "DescribeSecret should succeed after drift correction")
				assert.Equal(t, "original description", aws.ToString(out.Description),
					"secret description should be restored to 'original description' after drift correction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

// TestTerraformDrift_S3 verifies that Terraform detects and corrects drift when
// S3 bucket versioning is suspended directly via the SDK.
func TestTerraformDrift_S3(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "versioning",
			fixture: "s3/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"BucketName": "tf-drift-s3-" + uuid.NewString()[:8]}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createS3Client(t)
				_, err := client.PutBucketVersioning(ctx, &s3svc.PutBucketVersioningInput{
					Bucket: aws.String(vars["BucketName"].(string)),
					VersioningConfiguration: &s3types.VersioningConfiguration{
						Status: s3types.BucketVersioningStatusSuspended,
					},
				})
				require.NoError(t, err, "PutBucketVersioning should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createS3Client(t)
				out, err := client.GetBucketVersioning(ctx, &s3svc.GetBucketVersioningInput{
					Bucket: aws.String(vars["BucketName"].(string)),
				})
				require.NoError(t, err, "GetBucketVersioning should succeed after drift correction")
				assert.Equal(t, s3types.BucketVersioningStatusEnabled, out.Status,
					"versioning should be restored to Enabled after drift correction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

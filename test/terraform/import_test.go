package terraform_test

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
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
	rdssvc "github.com/aws/aws-sdk-go-v2/service/rds"
	route53svc "github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	secretssvc "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	snssvc "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssvc "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// importTestCase describes a single Terraform import test scenario.
//
// The test flow is:
//  1. createResource — creates the resource via the AWS SDK (outside Terraform).
//  2. Write HCL fixture to a temp directory and run tofu init.
//  3. Run tofu import to bring the resource into Terraform state.
//  4. Run tofu plan — should report no pending changes.
//  5. Register tofu destroy as a cleanup step.
//  6. Optionally run verify to inspect backend state.
type importTestCase struct {
	// createResource creates the resource directly via the AWS SDK and returns
	// template variables for the HCL fixture together with the Terraform import ID.
	createResource func(t *testing.T, ctx context.Context, dir string) (vars map[string]any, importID string)

	// verify is called after a successful import + no-change plan to inspect the
	// backend state. It may be nil when no additional verification is needed.
	verify func(t *testing.T, ctx context.Context, vars map[string]any)

	// providerFn builds the OpenTofu provider block. When nil, providerBlock is used.
	providerFn func(addr string) string

	// name is the sub-test name shown in test output.
	name string

	// fixture is the path under fixtures/ for the HCL template (without .tf).
	fixture string

	// resourceAddress is the Terraform resource address (e.g. "aws_dynamodb_table.this").
	resourceAddress string
}

// runTofuImport runs `tofu import -no-color <resourceAddress> <importID>` inside
// dir. The directory must already contain a main.tf and a valid .terraform/ tree.
func runTofuImport(ctx context.Context, t *testing.T, tofuBin, dir, resourceAddress, importID string) {
	t.Helper()

	out, err := execTofuCmd(ctx, t, tofuBin, dir, "import", "-no-color", resourceAddress, importID)
	t.Logf("tofu import:\n%s", out)
	require.NoError(t, err, "tofu import should succeed for %s with ID %q", resourceAddress, importID)
}

// runImportTest is the common runner for all Terraform import tests.
//
// It follows a five-step sequence:
//  1. SDK createResource — resource created outside Terraform.
//  2. Write HCL, init (or reuse pre-initialised provider cache).
//  3. tofu import — pulls the resource into Terraform state.
//  4. tofu plan  — must report no pending changes.
//  5. Cleanup via tofu destroy, optional SDK verify.
func runImportTest(t *testing.T, tc importTestCase) {
	t.Helper()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := t.Context()
	dir := t.TempDir()

	vars, importID := tc.createResource(t, ctx, dir)

	pFn := tc.providerFn
	if pFn == nil {
		pFn = providerBlock
	}

	hcl := pFn(endpoint) + renderFixture(t, tc.fixture, vars)

	cfgPath := filepath.Join(dir, "main.tf")
	require.NoError(t, os.WriteFile(cfgPath, []byte(hcl), 0o644))

	if err := os.MkdirAll(tofuProviderCacheDir, 0o755); err != nil {
		t.Logf("could not create provider cache dir: %v", err)
	}

	run := func(failFatal bool, args ...string) bool {
		t.Helper()

		out, err := execTofuCmd(ctx, t, tofuBin, dir, args...)
		t.Logf("tofu %v:\n%s", args, out)

		if err != nil {
			if failFatal {
				require.NoError(t, err, "tofu %v failed", args)
			}

			t.Logf("tofu %v failed (non-fatal): %v", args, err)

			return false
		}

		return true
	}

	reuseOrInit(t, dir, hcl, run)

	runTofuImport(ctx, t, tofuBin, dir, tc.resourceAddress, importID)

	t.Cleanup(func() {
		run(false, "destroy", "-auto-approve", "-no-color")
	})

	// After import, plan must show no pending changes.
	plan := runTofuPlan(ctx, t, tofuBin, dir)
	assert.False(t, plan.HasChanges,
		"tofu plan should show no changes after successful import of %s", tc.resourceAddress)

	if tc.verify != nil {
		tc.verify(t, ctx, vars)
	}
}

// TestTerraformImport_DynamoDB verifies that a DynamoDB table created directly
// via the SDK can be imported into Terraform state without any drift.
func TestTerraformImport_DynamoDB(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "table",
			fixture:         "dynamodb/import",
			resourceAddress: "aws_dynamodb_table.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				tableName := "tf-import-ddb-" + uuid.NewString()[:8]
				client := createDynamoDBClient(t)
				_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
					TableName:   aws.String(tableName),
					BillingMode: ddbtypes.BillingModePayPerRequest,
					KeySchema: []ddbtypes.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
					},
					AttributeDefinitions: []ddbtypes.AttributeDefinition{
						{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
					},
				})
				require.NoError(t, err, "CreateTable should succeed")

				return map[string]any{"TableName": tableName}, tableName
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createDynamoDBClient(t)
				out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
					TableName: aws.String(vars["TableName"].(string)),
				})
				require.NoError(t, err, "DescribeTable should succeed after import")
				require.NotNil(t, out.Table)
				assert.Equal(t, vars["TableName"].(string), aws.ToString(out.Table.TableName))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformImport_S3 verifies that an S3 bucket created directly via the
// SDK can be imported into Terraform state without any drift.
func TestTerraformImport_S3(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "bucket",
			fixture:         "s3/import",
			resourceAddress: "aws_s3_bucket.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				bucketName := "tf-import-s3-" + uuid.NewString()[:8]
				client := createS3Client(t)
				_, err := client.CreateBucket(ctx, &s3svc.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err, "CreateBucket should succeed")

				return map[string]any{"BucketName": bucketName}, bucketName
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createS3Client(t)
				_, err := client.HeadBucket(ctx, &s3svc.HeadBucketInput{
					Bucket: aws.String(vars["BucketName"].(string)),
				})
				require.NoError(t, err, "HeadBucket should succeed after import")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformImport_SQS verifies that an SQS queue created directly via the
// SDK can be imported into Terraform state without any drift.
func TestTerraformImport_SQS(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "queue",
			fixture:         "sqs/import",
			resourceAddress: "aws_sqs_queue.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				queueName := "tf-import-sqs-" + uuid.NewString()[:8]
				client := createSQSClient(t)
				out, err := client.CreateQueue(ctx, &sqssvc.CreateQueueInput{
					QueueName: aws.String(queueName),
					Attributes: map[string]string{
						"VisibilityTimeout": "30",
					},
				})
				require.NoError(t, err, "CreateQueue should succeed")

				// The Terraform import ID for aws_sqs_queue is the queue URL.
				return map[string]any{"QueueName": queueName}, aws.ToString(out.QueueUrl)
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createSQSClient(t)
				urlOut, err := client.GetQueueUrl(ctx, &sqssvc.GetQueueUrlInput{
					QueueName: aws.String(vars["QueueName"].(string)),
				})
				require.NoError(t, err, "GetQueueUrl should succeed after import")
				assert.Contains(t, aws.ToString(urlOut.QueueUrl), vars["QueueName"].(string))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformImport_SNS verifies that an SNS topic created directly via the
// SDK can be imported into Terraform state without any drift.
func TestTerraformImport_SNS(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "topic",
			fixture:         "sns/import",
			resourceAddress: "aws_sns_topic.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				topicName := "tf-import-sns-" + uuid.NewString()[:8]
				client := createSNSClient(t)
				out, err := client.CreateTopic(ctx, &snssvc.CreateTopicInput{
					Name: aws.String(topicName),
				})
				require.NoError(t, err, "CreateTopic should succeed")

				// The Terraform import ID for aws_sns_topic is the topic ARN.
				return map[string]any{"TopicName": topicName}, aws.ToString(out.TopicArn)
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createSNSClient(t)
				out, err := client.ListTopics(ctx, &snssvc.ListTopicsInput{})
				require.NoError(t, err, "ListTopics should succeed after import")

				found := false

				for _, topic := range out.Topics {
					if strings.Contains(aws.ToString(topic.TopicArn), vars["TopicName"].(string)) {
						found = true

						break
					}
				}

				assert.True(t, found, "imported SNS topic %q should be visible", vars["TopicName"].(string))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformImport_Lambda verifies that a Lambda function created directly
// via the SDK can be imported into Terraform state without any drift.
func TestTerraformImport_Lambda(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "function",
			fixture:         "lambda/import",
			resourceAddress: "aws_lambda_function.this",
			createResource: func(t *testing.T, ctx context.Context, dir string) (map[string]any, string) {
				t.Helper()

				id := uuid.NewString()[:8]
				roleName := "tf-import-lambda-role-" + id
				funcName := "tf-import-lambda-" + id

				// Create the IAM role that the Lambda function will assume.
				assumePolicy := `{"Version":"2012-10-17","Statement":[` +
					`{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},` +
					`"Action":"sts:AssumeRole"}]}`
				iamClient := createIAMClient(t)
				roleOut, err := iamClient.CreateRole(ctx, &iamsvc.CreateRoleInput{
					RoleName:                 aws.String(roleName),
					AssumeRolePolicyDocument: aws.String(assumePolicy),
				})
				require.NoError(t, err, "CreateRole should succeed")

				roleARN := aws.ToString(roleOut.Role.Arn)

				// Register cleanup for the IAM role.  tofu destroy only manages the
				// Lambda function, so the role must be deleted explicitly.  This cleanup
				// is registered BEFORE the tofu-destroy cleanup in runImportTest, so it
				// runs AFTER tofu destroy (LIFO order) — the Lambda is gone by then.
				// The role has no attached managed or inline policies (only a trust
				// policy), so DeleteRole succeeds without any prior detach step.
				t.Cleanup(func() {
					_, delErr := iamClient.DeleteRole(context.Background(), &iamsvc.DeleteRoleInput{
						RoleName: aws.String(roleName),
					})
					if delErr != nil {
						t.Logf("cleanup: failed to delete IAM role %q: %v", roleName, delErr)
					}
				})

				// Build a minimal Python handler zip.
				var buf bytes.Buffer
				zw := zip.NewWriter(&buf)
				f, err := zw.Create("index.py")
				require.NoError(t, err)
				_, err = f.Write([]byte("def handler(event, context):\n    return {}\n"))
				require.NoError(t, err)
				require.NoError(t, zw.Close())

				zipPath := filepath.Join(dir, "function.zip")
				require.NoError(t, os.WriteFile(zipPath, buf.Bytes(), 0o644))

				// Create the Lambda function via the SDK.
				lambdaClient := createLambdaClient(t)
				_, err = lambdaClient.CreateFunction(ctx, &lambdasvc.CreateFunctionInput{
					FunctionName: aws.String(funcName),
					Role:         aws.String(roleARN),
					Handler:      aws.String("index.handler"),
					Runtime:      lambdatypes.RuntimePython312,
					Code: &lambdatypes.FunctionCode{
						ZipFile: buf.Bytes(),
					},
				})
				require.NoError(t, err, "CreateFunction should succeed")

				return map[string]any{
					"FuncName": funcName,
					"RoleARN":  roleARN,
					"ZipPath":  zipPath,
				}, funcName
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createLambdaClient(t)
				out, err := client.GetFunction(ctx, &lambdasvc.GetFunctionInput{
					FunctionName: aws.String(vars["FuncName"].(string)),
				})
				require.NoError(t, err, "GetFunction should succeed after import")
				require.NotNil(t, out.Configuration)
				assert.Equal(t, vars["FuncName"].(string), aws.ToString(out.Configuration.FunctionName))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformImport_IAM verifies that IAM resources created directly via the
// SDK can be imported into Terraform state without any drift.
func TestTerraformImport_IAM(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "role",
			fixture:         "iam/import",
			resourceAddress: "aws_iam_role.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				roleName := "tf-import-role-" + uuid.NewString()[:8]
				assumePolicy := `{"Version":"2012-10-17","Statement":[` +
					`{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},` +
					`"Action":"sts:AssumeRole"}]}`

				client := createIAMClient(t)
				_, err := client.CreateRole(ctx, &iamsvc.CreateRoleInput{
					RoleName:                 aws.String(roleName),
					AssumeRolePolicyDocument: aws.String(assumePolicy),
				})
				require.NoError(t, err, "CreateRole should succeed")

				// The Terraform import ID for aws_iam_role is the role name.
				return map[string]any{"RoleName": roleName}, roleName
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createIAMClient(t)
				out, err := client.GetRole(ctx, &iamsvc.GetRoleInput{
					RoleName: aws.String(vars["RoleName"].(string)),
				})
				require.NoError(t, err, "GetRole should succeed after import")
				require.NotNil(t, out.Role)
				assert.Equal(t, vars["RoleName"].(string), aws.ToString(out.Role.RoleName))
			},
		},
		{
			name:            "policy",
			fixture:         "iam/policy_import",
			resourceAddress: "aws_iam_policy.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				policyName := "tf-import-policy-" + uuid.NewString()[:8]
				policyDoc := `{"Version":"2012-10-17","Statement":[` +
					`{"Effect":"Allow","Action":["s3:GetObject"],"Resource":"*"}]}`

				client := createIAMClient(t)
				out, err := client.CreatePolicy(ctx, &iamsvc.CreatePolicyInput{
					PolicyName:     aws.String(policyName),
					PolicyDocument: aws.String(policyDoc),
				})
				require.NoError(t, err, "CreatePolicy should succeed")

				policyARN := aws.ToString(out.Policy.Arn)

				// The Terraform import ID for aws_iam_policy is the policy ARN.
				return map[string]any{
					"PolicyName": policyName,
					"PolicyARN":  policyARN,
				}, policyARN
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createIAMClient(t)
				out, err := client.GetPolicy(ctx, &iamsvc.GetPolicyInput{
					PolicyArn: aws.String(vars["PolicyARN"].(string)),
				})
				require.NoError(t, err, "GetPolicy should succeed after import")
				require.NotNil(t, out.Policy)
				assert.Equal(t, vars["PolicyName"].(string), aws.ToString(out.Policy.PolicyName))
			},
		},
		{
			name:            "user",
			fixture:         "iam/user_import",
			resourceAddress: "aws_iam_user.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				userName := "tf-import-user-" + uuid.NewString()[:8]

				client := createIAMClient(t)
				_, err := client.CreateUser(ctx, &iamsvc.CreateUserInput{
					UserName: aws.String(userName),
				})
				require.NoError(t, err, "CreateUser should succeed")

				// The Terraform import ID for aws_iam_user is the username.
				return map[string]any{"UserName": userName}, userName
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createIAMClient(t)
				out, err := client.GetUser(ctx, &iamsvc.GetUserInput{
					UserName: aws.String(vars["UserName"].(string)),
				})
				require.NoError(t, err, "GetUser should succeed after import")
				require.NotNil(t, out.User)
				assert.Equal(t, vars["UserName"].(string), aws.ToString(out.User.UserName))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformImport_RDS verifies that an RDS DB instance created directly via
// the SDK can be imported into Terraform state without any drift.
//
// RDS requires its own provider block (rdsProviderBlock) that routes requests to
// the RDS-specific endpoint.
func TestTerraformImport_RDS(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "db_instance",
			fixture:         "rds/import",
			resourceAddress: "aws_db_instance.this",
			providerFn:      rdsProviderBlock,
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				identifier := "tf-import-rds-" + uuid.NewString()[:8]
				client := createRDSClient(t)
				_, err := client.CreateDBInstance(ctx, &rdssvc.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String(identifier),
					Engine:               aws.String("postgres"),
					DBInstanceClass:      aws.String("db.t3.micro"),
					MasterUsername:       aws.String("admin"),
					MasterUserPassword:   aws.String("password123"),
					DBName:               aws.String("testdb"),
					AllocatedStorage:     aws.Int32(20),
				})
				require.NoError(t, err, "CreateDBInstance should succeed")

				// The Terraform import ID for aws_db_instance is the DB identifier.
				return map[string]any{"Identifier": identifier}, identifier
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createRDSClient(t)
				out, err := client.DescribeDBInstances(ctx, &rdssvc.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String(vars["Identifier"].(string)),
				})
				require.NoError(t, err, "DescribeDBInstances should succeed after import")
				require.Len(t, out.DBInstances, 1)
				assert.Equal(t, vars["Identifier"].(string), aws.ToString(out.DBInstances[0].DBInstanceIdentifier))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformImport_Route53 verifies that Route 53 resources created directly
// via the SDK can be imported into Terraform state without any drift.
func TestTerraformImport_Route53(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "hosted_zone",
			fixture:         "route53/import",
			resourceAddress: "aws_route53_zone.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				zoneName := "tf-import-" + uuid.NewString()[:8] + ".example.com"
				client := createRoute53Client(t)
				out, err := client.CreateHostedZone(ctx, &route53svc.CreateHostedZoneInput{
					Name:            aws.String(zoneName),
					CallerReference: aws.String(uuid.NewString()),
					HostedZoneConfig: &r53types.HostedZoneConfig{
						PrivateZone: false,
					},
				})
				require.NoError(t, err, "CreateHostedZone should succeed")

				// The AWS API returns zone IDs with a "/hostedzone/" prefix.
				// Terraform expects just the bare zone ID as the import key.
				rawID := aws.ToString(out.HostedZone.Id)
				zoneID := strings.TrimPrefix(rawID, "/hostedzone/")

				return map[string]any{"ZoneName": zoneName}, zoneID
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createRoute53Client(t)
				out, err := client.ListHostedZones(ctx, &route53svc.ListHostedZonesInput{})
				require.NoError(t, err, "ListHostedZones should succeed after import")

				zoneName := vars["ZoneName"].(string)
				found := false

				for _, z := range out.HostedZones {
					if aws.ToString(z.Name) == zoneName+"." || aws.ToString(z.Name) == zoneName {
						found = true

						break
					}
				}

				assert.True(t, found, "hosted zone %q should exist after import", zoneName)
			},
		},
		{
			name:            "a_record",
			fixture:         "route53/record_import",
			resourceAddress: "aws_route53_record.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				zoneName := "tf-import-" + uuid.NewString()[:8] + ".example.com"
				client := createRoute53Client(t)

				// Create the hosted zone.
				zoneOut, err := client.CreateHostedZone(ctx, &route53svc.CreateHostedZoneInput{
					Name:            aws.String(zoneName),
					CallerReference: aws.String(uuid.NewString()),
				})
				require.NoError(t, err, "CreateHostedZone should succeed")

				rawZoneID := aws.ToString(zoneOut.HostedZone.Id)
				zoneID := strings.TrimPrefix(rawZoneID, "/hostedzone/")

				// Register cleanup for the zone.  tofu destroy only manages the record;
				// the zone must be deleted explicitly after the record is gone.
				// This cleanup is registered BEFORE the tofu-destroy cleanup in
				// runImportTest, so it runs AFTER tofu destroy (LIFO order).
				t.Cleanup(func() {
					_, delErr := client.DeleteHostedZone(context.Background(), &route53svc.DeleteHostedZoneInput{
						Id: aws.String(rawZoneID),
					})
					if delErr != nil {
						t.Logf("cleanup: failed to delete hosted zone %q: %v", zoneID, delErr)
					}
				})

				// Create an A record in the zone.
				recordName := "www." + zoneName
				_, err = client.ChangeResourceRecordSets(ctx, &route53svc.ChangeResourceRecordSetsInput{
					HostedZoneId: aws.String(zoneID),
					ChangeBatch: &r53types.ChangeBatch{
						Changes: []r53types.Change{
							{
								Action: r53types.ChangeActionCreate,
								ResourceRecordSet: &r53types.ResourceRecordSet{
									Name: aws.String(recordName),
									Type: r53types.RRTypeA,
									TTL:  aws.Int64(300),
									ResourceRecords: []r53types.ResourceRecord{
										{Value: aws.String("1.2.3.4")},
									},
								},
							},
						},
					},
				})
				require.NoError(t, err, "ChangeResourceRecordSets should succeed")

				// Terraform import ID for aws_route53_record is ZONEID_NAME_TYPE.
				importID := zoneID + "_" + recordName + "_A"

				return map[string]any{
					"ZoneID":     zoneID,
					"RecordName": recordName,
				}, importID
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createRoute53Client(t)
				out, err := client.ListResourceRecordSets(ctx, &route53svc.ListResourceRecordSetsInput{
					HostedZoneId: aws.String(vars["ZoneID"].(string)),
				})
				require.NoError(t, err, "ListResourceRecordSets should succeed after import")

				recordName := strings.TrimSuffix(vars["RecordName"].(string), ".")
				found := false

				for _, rrs := range out.ResourceRecordSets {
					if strings.TrimSuffix(aws.ToString(rrs.Name), ".") == recordName && rrs.Type == r53types.RRTypeA {
						found = true

						break
					}
				}

				assert.True(t, found, "A record %q should exist after import", vars["RecordName"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformImport_SecretsManager verifies that a Secrets Manager secret
// created directly via the SDK can be imported into Terraform state without drift.
func TestTerraformImport_SecretsManager(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "secret",
			fixture:         "secretsmanager/import",
			resourceAddress: "aws_secretsmanager_secret.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				secretName := "tf-import-secret-" + uuid.NewString()[:8]
				client := createSecretsManagerClient(t)
				out, err := client.CreateSecret(ctx, &secretssvc.CreateSecretInput{
					Name:                        aws.String(secretName),
					ForceOverwriteReplicaSecret: false,
				})
				require.NoError(t, err, "CreateSecret should succeed")

				// The Terraform import ID for aws_secretsmanager_secret is the secret ARN.
				return map[string]any{"SecretName": secretName}, aws.ToString(out.ARN)
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createSecretsManagerClient(t)
				out, err := client.DescribeSecret(ctx, &secretssvc.DescribeSecretInput{
					SecretId: aws.String(vars["SecretName"].(string)),
				})
				require.NoError(t, err, "DescribeSecret should succeed after import")
				assert.Equal(t, vars["SecretName"].(string), aws.ToString(out.Name))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformImport_CloudWatchAlarm verifies that a CloudWatch metric alarm
// created directly via the SDK can be imported into Terraform state without drift.
func TestTerraformImport_CloudWatchAlarm(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "metric_alarm",
			fixture:         "cloudwatch/import",
			resourceAddress: "aws_cloudwatch_metric_alarm.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				alarmName := "tf-import-alarm-" + uuid.NewString()[:8]
				client := createCloudWatchClient(t)
				_, err := client.PutMetricAlarm(ctx, &cwsvc.PutMetricAlarmInput{
					AlarmName:          aws.String(alarmName),
					ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
					EvaluationPeriods:  aws.Int32(1),
					MetricName:         aws.String("CPUUtilization"),
					Namespace:          aws.String("AWS/EC2"),
					Period:             aws.Int32(60),
					Statistic:          cwtypes.StatisticAverage,
					Threshold:          aws.Float64(80),
				})
				require.NoError(t, err, "PutMetricAlarm should succeed")

				// The Terraform import ID for aws_cloudwatch_metric_alarm is the alarm name.
				return map[string]any{"AlarmName": alarmName}, alarmName
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createCloudWatchClient(t)
				out, err := client.DescribeAlarms(ctx, &cwsvc.DescribeAlarmsInput{
					AlarmNames: []string{vars["AlarmName"].(string)},
				})
				require.NoError(t, err, "DescribeAlarms should succeed after import")
				require.Len(t, out.MetricAlarms, 1)
				assert.Equal(t, vars["AlarmName"].(string), aws.ToString(out.MetricAlarms[0].AlarmName))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

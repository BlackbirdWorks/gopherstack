package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForStackStatus polls DescribeStacks until the stack reaches one of the
// terminal states or times out.
func waitForStackStatus(
	t *testing.T,
	client *cloudformationsdk.Client,
	stackName string,
	terminal ...cftypes.StackStatus,
) cftypes.StackStatus {
	t.Helper()

	ctx := t.Context()
	termSet := make(map[cftypes.StackStatus]bool, len(terminal))

	for _, s := range terminal {
		termSet[s] = true
	}

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		out, err := client.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{
			StackName: aws.String(stackName),
		})
		require.NoError(t, err)
		require.NotEmpty(t, out.Stacks)

		status := out.Stacks[0].StackStatus
		if termSet[status] {
			return status
		}

		time.Sleep(150 * time.Millisecond)
	}

	t.Fatalf("stack %s did not reach terminal status within timeout", stackName)

	return ""
}

// deployStack creates a CloudFormation stack from the given template body and
// waits for it to reach CREATE_COMPLETE or CREATE_FAILED.
func deployStack(
	t *testing.T,
	client *cloudformationsdk.Client,
	stackName, templateBody string,
) cftypes.StackStatus {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(templateBody),
	})
	require.NoError(t, err)

	return waitForStackStatus(t, client, stackName,
		cftypes.StackStatusCreateComplete,
		cftypes.StackStatusCreateFailed,
		cftypes.StackStatusRollbackComplete,
	)
}

// deleteStackAndWait deletes a stack and waits for DELETE_COMPLETE.
func deleteStackAndWait(t *testing.T, client *cloudformationsdk.Client, stackName string) {
	t.Helper()

	ctx := t.Context()

	_, err := client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)

	waitForStackStatus(t, client, stackName,
		cftypes.StackStatusDeleteComplete,
	)
}

// stackID returns the suffix of the stack name used for unique resource naming.
func stackID() string { return uuid.NewString()[:8] }

func TestIntegration_CFN_Phase2_RDS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		template string
	}{
		{
			name: "DBInstance",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyDB": {
						"Type": "AWS::RDS::DBInstance",
						"Properties": {
							"DBInstanceIdentifier": "cfn-test-db-` + stackID() + `",
							"Engine": "postgres",
							"DBInstanceClass": "db.t3.micro",
							"AllocatedStorage": "20",
							"MasterUsername": "admin"
						}
					}
				}
			}`,
		},
		{
			name: "DBSubnetGroup",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MySubnetGrp": {
						"Type": "AWS::RDS::DBSubnetGroup",
						"Properties": {
							"DBSubnetGroupName": "cfn-sg-` + stackID() + `",
							"DBSubnetGroupDescription": "test subnet group",
							"SubnetIds": ["subnet-default"]
						}
					}
				}
			}`,
		},
		{
			name: "DBParameterGroup",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyPG": {
						"Type": "AWS::RDS::DBParameterGroup",
						"Properties": {
							"DBParameterGroupName": "cfn-pg-` + stackID() + `",
							"Family": "postgres14",
							"Description": "test parameter group"
						}
					}
				}
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-rds-" + tt.name + "-" + stackID()

			status := deployStack(t, client, stackName, tt.template)
			assert.Equal(t, cftypes.StackStatusCreateComplete, status)

			deleteStackAndWait(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_ElastiCache(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		template string
	}{
		{
			name: "ReplicationGroup",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyRG": {
						"Type": "AWS::ElastiCache::ReplicationGroup",
						"Properties": {
							"ReplicationGroupId": "cfn-rg-` + stackID() + `",
							"ReplicationGroupDescription": "test replication group"
						}
					}
				}
			}`,
		},
		{
			name: "SubnetGroup",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MySG": {
						"Type": "AWS::ElastiCache::SubnetGroup",
						"Properties": {
							"CacheSubnetGroupName": "cfn-ecsg-` + stackID() + `",
							"CacheSubnetGroupDescription": "test subnet group",
							"SubnetIds": ["subnet-default"]
						}
					}
				}
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-ec-" + tt.name + "-" + stackID()

			status := deployStack(t, client, stackName, tt.template)
			assert.Equal(t, cftypes.StackStatusCreateComplete, status)

			deleteStackAndWait(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_ECS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		template string
	}{
		{
			name: "Cluster",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyCluster": {
						"Type": "AWS::ECS::Cluster",
						"Properties": {
							"ClusterName": "cfn-cluster-` + stackID() + `"
						}
					}
				}
			}`,
		},
		{
			name: "TaskDefinition",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyTD": {
						"Type": "AWS::ECS::TaskDefinition",
						"Properties": {
							"Family": "cfn-td-` + stackID() + `",
							"NetworkMode": "awsvpc",
							"ContainerDefinitions": [
								{
									"Name": "app",
									"Image": "nginx:latest",
									"Cpu": 256,
									"Memory": 512
								}
							]
						}
					}
				}
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-ecs-" + tt.name + "-" + stackID()

			status := deployStack(t, client, stackName, tt.template)
			assert.Equal(t, cftypes.StackStatusCreateComplete, status)

			deleteStackAndWait(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_ECR(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-ecr-" + stackID()
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRepo": {
				"Type": "AWS::ECR::Repository",
				"Properties": {
					"RepositoryName": "cfn-repo-` + stackID() + `"
				}
			}
		}
	}`

	status := deployStack(t, client, stackName, template)
	assert.Equal(t, cftypes.StackStatusCreateComplete, status)

	deleteStackAndWait(t, client, stackName)
}

func TestIntegration_CFN_Phase2_Redshift(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-redshift-" + stackID()
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyCluster": {
				"Type": "AWS::Redshift::Cluster",
				"Properties": {
					"ClusterIdentifier": "cfn-rs-` + stackID() + `",
					"NodeType": "dc2.large",
					"DBName": "mydb",
					"MasterUsername": "admin"
				}
			}
		}
	}`

	status := deployStack(t, client, stackName, template)
	assert.Equal(t, cftypes.StackStatusCreateComplete, status)

	deleteStackAndWait(t, client, stackName)
}

func TestIntegration_CFN_Phase2_OpenSearch(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-opensearch-" + stackID()
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyDomain": {
				"Type": "AWS::OpenSearch::Domain",
				"Properties": {
					"DomainName": "cfn-os-` + stackID() + `",
					"EngineVersion": "OpenSearch_2.11"
				}
			}
		}
	}`

	status := deployStack(t, client, stackName, template)
	assert.Equal(t, cftypes.StackStatusCreateComplete, status)

	deleteStackAndWait(t, client, stackName)
}

func TestIntegration_CFN_Phase2_Firehose(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-firehose-" + stackID()
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyStream": {
				"Type": "AWS::Firehose::DeliveryStream",
				"Properties": {
					"DeliveryStreamName": "cfn-fh-` + stackID() + `"
				}
			}
		}
	}`

	status := deployStack(t, client, stackName, template)
	assert.Equal(t, cftypes.StackStatusCreateComplete, status)

	deleteStackAndWait(t, client, stackName)
}

func TestIntegration_CFN_Phase2_Route53(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-r53hc-" + stackID()
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyHC": {
				"Type": "AWS::Route53::HealthCheck",
				"Properties": {
					"HealthCheckConfig": {
						"Type": "HTTPS",
						"FullyQualifiedDomainName": "example.com",
						"Port": "443"
					}
				}
			}
		}
	}`

	status := deployStack(t, client, stackName, template)
	assert.Equal(t, cftypes.StackStatusCreateComplete, status)

	deleteStackAndWait(t, client, stackName)
}

func TestIntegration_CFN_Phase2_Route53Resolver(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		template string
	}{
		{
			name: "ResolverEndpoint",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyEP": {
						"Type": "AWS::Route53Resolver::ResolverEndpoint",
						"Properties": {
							"Name": "cfn-ep-` + stackID() + `",
							"Direction": "INBOUND"
						}
					}
				}
			}`,
		},
		{
			name: "ResolverRule",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyRule": {
						"Type": "AWS::Route53Resolver::ResolverRule",
						"Properties": {
							"Name": "cfn-rule-` + stackID() + `",
							"DomainName": "example.internal",
							"RuleType": "FORWARD"
						}
					}
				}
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-r53r-" + tt.name + "-" + stackID()

			status := deployStack(t, client, stackName, tt.template)
			assert.Equal(t, cftypes.StackStatusCreateComplete, status)

			deleteStackAndWait(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_SWF(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-swf-" + stackID()
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyDomain": {
				"Type": "AWS::SWF::Domain",
				"Properties": {
					"Name": "cfn-domain-` + stackID() + `",
					"Description": "test SWF domain"
				}
			}
		}
	}`

	status := deployStack(t, client, stackName, template)
	assert.Equal(t, cftypes.StackStatusCreateComplete, status)

	deleteStackAndWait(t, client, stackName)
}

func TestIntegration_CFN_Phase2_AppSync(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-appsync-" + stackID()
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::AppSync::GraphQLApi",
				"Properties": {
					"Name": "cfn-api-` + stackID() + `",
					"AuthenticationType": "API_KEY"
				}
			}
		}
	}`

	status := deployStack(t, client, stackName, template)
	assert.Equal(t, cftypes.StackStatusCreateComplete, status)

	deleteStackAndWait(t, client, stackName)
}

func TestIntegration_CFN_Phase2_SES(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-ses-" + stackID()
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyIdentity": {
				"Type": "AWS::SES::EmailIdentity",
				"Properties": {
					"EmailIdentity": "cfn-test-` + stackID() + `@example.com"
				}
			}
		}
	}`

	status := deployStack(t, client, stackName, template)
	assert.Equal(t, cftypes.StackStatusCreateComplete, status)

	deleteStackAndWait(t, client, stackName)
}

func TestIntegration_CFN_Phase2_ACM(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-acm-" + stackID()
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyCert": {
				"Type": "AWS::ACM::Certificate",
				"Properties": {
					"DomainName": "cfn-test-` + stackID() + `.example.com"
				}
			}
		}
	}`

	status := deployStack(t, client, stackName, template)
	assert.Equal(t, cftypes.StackStatusCreateComplete, status)

	deleteStackAndWait(t, client, stackName)
}

func TestIntegration_CFN_Phase2_Cognito(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		template string
	}{
		{
			name: "UserPool",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyPool": {
						"Type": "AWS::Cognito::UserPool",
						"Properties": {
							"PoolName": "cfn-pool-` + stackID() + `"
						}
					}
				}
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-cognito-" + tt.name + "-" + stackID()

			status := deployStack(t, client, stackName, tt.template)
			assert.Equal(t, cftypes.StackStatusCreateComplete, status)

			deleteStackAndWait(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_EC2Extended(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		template string
	}{
		{
			name: "EIP",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyEIP": {
						"Type": "AWS::EC2::EIP",
						"Properties": {}
					}
				}
			}`,
		},
		{
			name: "NatGateway",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyEIP": {
						"Type": "AWS::EC2::EIP",
						"Properties": {}
					},
					"MyNGW": {
						"Type": "AWS::EC2::NatGateway",
						"DependsOn": ["MyEIP"],
						"Properties": {
							"SubnetId": "subnet-default",
							"AllocationId": {"Ref": "MyEIP"}
						}
					}
				}
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-ec2ext-" + tt.name + "-" + stackID()

			status := deployStack(t, client, stackName, tt.template)
			assert.Equal(t, cftypes.StackStatusCreateComplete, status)

			deleteStackAndWait(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_CloudWatchCompositeAlarm(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-cw-composite-" + stackID()
	alarmName := "cfn-base-alarm-" + stackID()
	compositeAlarmName := "cfn-composite-" + stackID()
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"BaseAlarm": {
				"Type": "AWS::CloudWatch::Alarm",
				"Properties": {
					"AlarmName": "` + alarmName + `",
					"Namespace": "AWS/EC2",
					"MetricName": "CPUUtilization",
					"ComparisonOperator": "GreaterThanThreshold",
					"Threshold": 90,
					"EvaluationPeriods": 1,
					"Period": 60,
					"Statistic": "Average"
				}
			},
			"CompositeAlarm": {
				"Type": "AWS::CloudWatch::CompositeAlarm",
				"DependsOn": ["BaseAlarm"],
				"Properties": {
					"AlarmName": "` + compositeAlarmName + `",
					"AlarmRule": "ALARM(` + alarmName + `)"
				}
			}
		}
	}`

	status := deployStack(t, client, stackName, template)
	assert.Equal(t, cftypes.StackStatusCreateComplete, status)

	deleteStackAndWait(t, client, stackName)
}

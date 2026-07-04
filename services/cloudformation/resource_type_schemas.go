package cloudformation

// resourceTypeSchemaCatalog maps the emulator's modeled AWS::* CloudFormation
// resource types to a realistic CloudFormation Resource Provider Schema (the same
// document shape published by AWS in the CloudFormation Public Registry /
// cloudformation-cli resource providers). Each entry includes typeName,
// description, properties (with accurate JSON types), primaryIdentifier, and
// readOnlyProperties/createOnlyProperties where AWS defines them.
//
// These are intentionally trimmed down versions of the real provider schemas (the
// full AWS schemas run to hundreds of lines with nested definitions) but every
// property that is present has the correct JSON type and the identifier arrays
// match what AWS publishes for these resource types.
//
// Built fresh on each call (rather than as a package-level var) since the catalog
// is only consulted on the relatively cold DescribeType path. Each schema lives
// in its own small function purely to keep function bodies short.
func resourceTypeSchemaCatalog() map[string]string {
	return map[string]string{
		resTypeS3Bucket:         s3BucketProviderSchema(),
		resTypeDynamoDBTable:    dynamoDBTableProviderSchema(),
		resTypeSQSQueue:         sqsQueueProviderSchema(),
		resTypeSNSTopic:         snsTopicProviderSchema(),
		resTypeLambdaFunction:   lambdaFunctionProviderSchema(),
		resTypeIAMRole:          iamRoleProviderSchema(),
		resTypeEC2Instance:      ec2InstanceProviderSchema(),
		resTypeEC2SecurityGroup: ec2SecurityGroupProviderSchema(),
		resTypeCloudWatchAlarm:  cloudWatchAlarmProviderSchema(),
		resTypeKMSKey:           kmsKeyProviderSchema(),
		resTypeLogGroup:         logGroupProviderSchema(),
		resTypeRDSDB:            rdsDBInstanceProviderSchema(),
		resTypeEC2VPC:           ec2VPCProviderSchema(),
		resTypeECSCluster:       ecsClusterProviderSchema(),
	}
}

// resourceTypeSchema looks up the modeled provider schema JSON for the given
// CloudFormation resource type name. Returns ("", false) when the type is not
// in the catalog.
func resourceTypeSchema(typeName string) (string, bool) {
	schema, ok := resourceTypeSchemaCatalog()[typeName]

	return schema, ok
}

// s3BucketProviderSchema is the modeled schema for AWS::S3::Bucket.
func s3BucketProviderSchema() string {
	return `{
		"typeName": "AWS::S3::Bucket",
		"description": "Resource Type definition for AWS::S3::Bucket",
		"additionalProperties": false,
		"properties": {
			"BucketName": {"type": "string"},
			"AccessControl": {"type": "string"},
			"VersioningConfiguration": {
				"type": "object",
				"properties": {
					"Status": {"type": "string"}
				}
			},
			"BucketEncryption": {"type": "object"},
			"WebsiteConfiguration": {"type": "object"},
			"Tags": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"Key": {"type": "string"},
						"Value": {"type": "string"}
					}
				}
			},
			"Arn": {"type": "string"},
			"DomainName": {"type": "string"},
			"DualStackDomainName": {"type": "string"},
			"RegionalDomainName": {"type": "string"},
			"WebsiteURL": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/BucketName"],
		"readOnlyProperties": [
			"/properties/Arn",
			"/properties/DomainName",
			"/properties/DualStackDomainName",
			"/properties/RegionalDomainName",
			"/properties/WebsiteURL"
		],
		"createOnlyProperties": ["/properties/BucketName"]
	}`
}

// dynamoDBTableProviderSchema is the modeled schema for AWS::DynamoDB::Table.
func dynamoDBTableProviderSchema() string {
	return `{
		"typeName": "AWS::DynamoDB::Table",
		"description": "Resource Type definition for AWS::DynamoDB::Table",
		"additionalProperties": false,
		"properties": {
			"TableName": {"type": "string"},
			"AttributeDefinitions": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"AttributeName": {"type": "string"},
						"AttributeType": {"type": "string"}
					}
				}
			},
			"KeySchema": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"AttributeName": {"type": "string"},
						"KeyType": {"type": "string"}
					}
				}
			},
			"BillingMode": {"type": "string"},
			"ProvisionedThroughput": {
				"type": "object",
				"properties": {
					"ReadCapacityUnits": {"type": "integer"},
					"WriteCapacityUnits": {"type": "integer"}
				}
			},
			"StreamSpecification": {"type": "object"},
			"Arn": {"type": "string"},
			"StreamArn": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/TableName"],
		"readOnlyProperties": ["/properties/Arn", "/properties/StreamArn"],
		"createOnlyProperties": [
			"/properties/TableName",
			"/properties/KeySchema"
		]
	}`
}

// sqsQueueProviderSchema is the modeled schema for AWS::SQS::Queue.
func sqsQueueProviderSchema() string {
	return `{
		"typeName": "AWS::SQS::Queue",
		"description": "Resource Type definition for AWS::SQS::Queue",
		"additionalProperties": false,
		"properties": {
			"QueueName": {"type": "string"},
			"FifoQueue": {"type": "boolean"},
			"DelaySeconds": {"type": "integer"},
			"MessageRetentionPeriod": {"type": "integer"},
			"VisibilityTimeout": {"type": "integer"},
			"Arn": {"type": "string"},
			"QueueUrl": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/QueueUrl"],
		"readOnlyProperties": ["/properties/Arn", "/properties/QueueUrl"],
		"createOnlyProperties": ["/properties/QueueName", "/properties/FifoQueue"]
	}`
}

// snsTopicProviderSchema is the modeled schema for AWS::SNS::Topic.
func snsTopicProviderSchema() string {
	return `{
		"typeName": "AWS::SNS::Topic",
		"description": "Resource Type definition for AWS::SNS::Topic",
		"additionalProperties": false,
		"properties": {
			"TopicName": {"type": "string"},
			"DisplayName": {"type": "string"},
			"FifoTopic": {"type": "boolean"},
			"KmsMasterKeyId": {"type": "string"},
			"TopicArn": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/TopicArn"],
		"readOnlyProperties": ["/properties/TopicArn"],
		"createOnlyProperties": ["/properties/TopicName", "/properties/FifoTopic"]
	}`
}

// lambdaFunctionProviderSchema is the modeled schema for AWS::Lambda::Function.
func lambdaFunctionProviderSchema() string {
	return `{
		"typeName": "AWS::Lambda::Function",
		"description": "Resource Type definition for AWS::Lambda::Function",
		"additionalProperties": false,
		"properties": {
			"FunctionName": {"type": "string"},
			"Runtime": {"type": "string"},
			"Handler": {"type": "string"},
			"Role": {"type": "string"},
			"Code": {
				"type": "object",
				"properties": {
					"S3Bucket": {"type": "string"},
					"S3Key": {"type": "string"},
					"ZipFile": {"type": "string"}
				}
			},
			"Timeout": {"type": "integer"},
			"MemorySize": {"type": "integer"},
			"Environment": {"type": "object"},
			"Arn": {"type": "string"},
			"SnapStartResponse": {"type": "object"}
		},
		"primaryIdentifier": ["/properties/FunctionName"],
		"readOnlyProperties": ["/properties/Arn", "/properties/SnapStartResponse"],
		"createOnlyProperties": ["/properties/FunctionName"]
	}`
}

// iamRoleProviderSchema is the modeled schema for AWS::IAM::Role.
func iamRoleProviderSchema() string {
	return `{
		"typeName": "AWS::IAM::Role",
		"description": "Resource Type definition for AWS::IAM::Role",
		"additionalProperties": false,
		"properties": {
			"RoleName": {"type": "string"},
			"Path": {"type": "string"},
			"AssumeRolePolicyDocument": {"type": "object"},
			"Description": {"type": "string"},
			"MaxSessionDuration": {"type": "integer"},
			"Policies": {"type": "array"},
			"Arn": {"type": "string"},
			"RoleId": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/RoleName"],
		"readOnlyProperties": ["/properties/Arn", "/properties/RoleId"],
		"createOnlyProperties": ["/properties/RoleName", "/properties/Path"]
	}`
}

// ec2InstanceProviderSchema is the modeled schema for AWS::EC2::Instance.
func ec2InstanceProviderSchema() string {
	return `{
		"typeName": "AWS::EC2::Instance",
		"description": "Resource Type definition for AWS::EC2::Instance",
		"additionalProperties": false,
		"properties": {
			"ImageId": {"type": "string"},
			"InstanceType": {"type": "string"},
			"KeyName": {"type": "string"},
			"SubnetId": {"type": "string"},
			"SecurityGroupIds": {"type": "array", "items": {"type": "string"}},
			"Tags": {"type": "array"},
			"InstanceId": {"type": "string"},
			"PublicIp": {"type": "string"},
			"PrivateIp": {"type": "string"},
			"PublicDnsName": {"type": "string"},
			"PrivateDnsName": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/InstanceId"],
		"readOnlyProperties": [
			"/properties/InstanceId",
			"/properties/PublicIp",
			"/properties/PrivateIp",
			"/properties/PublicDnsName",
			"/properties/PrivateDnsName"
		],
		"createOnlyProperties": [
			"/properties/ImageId",
			"/properties/AvailabilityZone",
			"/properties/SubnetId"
		]
	}`
}

// ec2SecurityGroupProviderSchema is the modeled schema for AWS::EC2::SecurityGroup.
func ec2SecurityGroupProviderSchema() string {
	return `{
		"typeName": "AWS::EC2::SecurityGroup",
		"description": "Resource Type definition for AWS::EC2::SecurityGroup",
		"additionalProperties": false,
		"properties": {
			"GroupName": {"type": "string"},
			"GroupDescription": {"type": "string"},
			"VpcId": {"type": "string"},
			"SecurityGroupIngress": {"type": "array"},
			"SecurityGroupEgress": {"type": "array"},
			"Tags": {"type": "array"},
			"GroupId": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/GroupId"],
		"readOnlyProperties": ["/properties/GroupId"],
		"createOnlyProperties": [
			"/properties/GroupName",
			"/properties/GroupDescription",
			"/properties/VpcId"
		]
	}`
}

// cloudWatchAlarmProviderSchema is the modeled schema for AWS::CloudWatch::Alarm.
func cloudWatchAlarmProviderSchema() string {
	return `{
		"typeName": "AWS::CloudWatch::Alarm",
		"description": "Resource Type definition for AWS::CloudWatch::Alarm",
		"additionalProperties": false,
		"properties": {
			"AlarmName": {"type": "string"},
			"AlarmDescription": {"type": "string"},
			"MetricName": {"type": "string"},
			"Namespace": {"type": "string"},
			"Statistic": {"type": "string"},
			"Period": {"type": "integer"},
			"EvaluationPeriods": {"type": "integer"},
			"Threshold": {"type": "number"},
			"ComparisonOperator": {"type": "string"},
			"ActionsEnabled": {"type": "boolean"},
			"AlarmActions": {"type": "array", "items": {"type": "string"}},
			"Arn": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/AlarmName"],
		"readOnlyProperties": ["/properties/Arn"],
		"createOnlyProperties": ["/properties/AlarmName"]
	}`
}

// kmsKeyProviderSchema is the modeled schema for AWS::KMS::Key.
func kmsKeyProviderSchema() string {
	return `{
		"typeName": "AWS::KMS::Key",
		"description": "Resource Type definition for AWS::KMS::Key",
		"additionalProperties": false,
		"properties": {
			"Description": {"type": "string"},
			"KeyPolicy": {"type": "object"},
			"Enabled": {"type": "boolean"},
			"EnableKeyRotation": {"type": "boolean"},
			"KeyUsage": {"type": "string"},
			"PendingWindowInDays": {"type": "integer"},
			"KeyId": {"type": "string"},
			"Arn": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/KeyId"],
		"readOnlyProperties": ["/properties/KeyId", "/properties/Arn"],
		"createOnlyProperties": []
	}`
}

// logGroupProviderSchema is the modeled schema for AWS::Logs::LogGroup.
func logGroupProviderSchema() string {
	return `{
		"typeName": "AWS::Logs::LogGroup",
		"description": "Resource Type definition for AWS::Logs::LogGroup",
		"additionalProperties": false,
		"properties": {
			"LogGroupName": {"type": "string"},
			"RetentionInDays": {"type": "integer"},
			"KmsKeyId": {"type": "string"},
			"Arn": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/LogGroupName"],
		"readOnlyProperties": ["/properties/Arn"],
		"createOnlyProperties": ["/properties/LogGroupName"]
	}`
}

// rdsDBInstanceProviderSchema is the modeled schema for AWS::RDS::DBInstance.
func rdsDBInstanceProviderSchema() string {
	return `{
		"typeName": "AWS::RDS::DBInstance",
		"description": "Resource Type definition for AWS::RDS::DBInstance",
		"additionalProperties": false,
		"properties": {
			"DBInstanceIdentifier": {"type": "string"},
			"DBInstanceClass": {"type": "string"},
			"Engine": {"type": "string"},
			"MasterUsername": {"type": "string"},
			"AllocatedStorage": {"type": "string"},
			"Endpoint": {
				"type": "object",
				"properties": {
					"Address": {"type": "string"},
					"Port": {"type": "string"}
				}
			},
			"DBInstanceArn": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/DBInstanceIdentifier"],
		"readOnlyProperties": [
			"/properties/Endpoint/Address",
			"/properties/Endpoint/Port",
			"/properties/DBInstanceArn"
		],
		"createOnlyProperties": [
			"/properties/DBInstanceIdentifier",
			"/properties/Engine",
			"/properties/MasterUsername"
		]
	}`
}

// ec2VPCProviderSchema is the modeled schema for AWS::EC2::VPC.
func ec2VPCProviderSchema() string {
	return `{
		"typeName": "AWS::EC2::VPC",
		"description": "Resource Type definition for AWS::EC2::VPC",
		"additionalProperties": false,
		"properties": {
			"CidrBlock": {"type": "string"},
			"EnableDnsSupport": {"type": "boolean"},
			"EnableDnsHostnames": {"type": "boolean"},
			"InstanceTenancy": {"type": "string"},
			"Tags": {"type": "array"},
			"VpcId": {"type": "string"},
			"DefaultSecurityGroup": {"type": "string"},
			"DefaultNetworkAcl": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/VpcId"],
		"readOnlyProperties": [
			"/properties/VpcId",
			"/properties/DefaultSecurityGroup",
			"/properties/DefaultNetworkAcl"
		],
		"createOnlyProperties": ["/properties/CidrBlock", "/properties/InstanceTenancy"]
	}`
}

// ecsClusterProviderSchema is the modeled schema for AWS::ECS::Cluster.
func ecsClusterProviderSchema() string {
	return `{
		"typeName": "AWS::ECS::Cluster",
		"description": "Resource Type definition for AWS::ECS::Cluster",
		"additionalProperties": false,
		"properties": {
			"ClusterName": {"type": "string"},
			"CapacityProviders": {"type": "array", "items": {"type": "string"}},
			"Tags": {"type": "array"},
			"Arn": {"type": "string"}
		},
		"primaryIdentifier": ["/properties/Arn"],
		"readOnlyProperties": ["/properties/Arn"],
		"createOnlyProperties": ["/properties/ClusterName"]
	}`
}

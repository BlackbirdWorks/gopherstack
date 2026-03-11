package demo

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/amplify"
	amplifytypes "github.com/aws/aws-sdk-go-v2/service/amplify/types"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	codedeploysvc "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	codedeploytypes "github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	codepipelinesvc "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	codepipelinetypes "github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	pkgslogger "github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

const (
	defaultCapacity = 5
)

// Clients holds all AWS SDK clients for demo data loading.
type Clients struct {
	DynamoDB       *dynamodb.Client
	S3             *s3.Client
	SQS            *sqs.Client
	SNS            *sns.Client
	IAM            *iam.Client
	STS            *sts.Client
	SSM            *ssm.Client
	KMS            *kms.Client
	SecretsManager *secretsmanager.Client
	ECR            *ecr.Client
	AppSync        *appsync.Client
	Amplify        *amplify.Client
	ECS            *ecs.Client
	IoT            *iot.Client
	CodeDeploy     *codedeploysvc.Client
	CodePipeline   *codepipelinesvc.Client
}

// LoadData loads sample data into all supported services.
func LoadData(
	ctx context.Context,
	clients *Clients,
) error {
	pkgslogger.Load(ctx).InfoContext(ctx, "Loading demo data...")

	if err := loadDynamoDB(ctx, clients.DynamoDB); err != nil {
		return fmt.Errorf("failed to load dynamodb data: %w", err)
	}

	if err := loadS3(ctx, clients.S3); err != nil {
		return fmt.Errorf("failed to load s3 data: %w", err)
	}

	loadSQS(ctx, clients.SQS)

	if err := loadSNS(ctx, clients.SNS, clients.SQS); err != nil {
		return fmt.Errorf("failed to load sns data: %w", err)
	}

	loadIAM(ctx, clients.IAM)

	if err := loadSSM(ctx, clients.SSM); err != nil {
		return fmt.Errorf("failed to load ssm data: %w", err)
	}

	loadKMS(ctx, clients.KMS)

	loadSecretsManager(ctx, clients.SecretsManager)

	if clients.ECR != nil {
		loadECR(ctx, clients.ECR)
	}

	if clients.AppSync != nil {
		loadAppSync(ctx, clients.AppSync)
	}

	if clients.Amplify != nil {
		loadAmplify(ctx, clients.Amplify)
	}

	if clients.ECS != nil {
		loadECS(ctx, clients.ECS)
	}

	if clients.IoT != nil {
		loadIoT(ctx, clients.IoT)
	}

	if clients.CodeDeploy != nil {
		loadCodeDeploy(ctx, clients.CodeDeploy)
	}

	if clients.CodePipeline != nil {
		loadCodePipeline(ctx, clients.CodePipeline)
	}

	pkgslogger.Load(ctx).InfoContext(ctx, "Demo data loaded successfully")

	return nil
}

func loadDynamoDB(ctx context.Context, ddb *dynamodb.Client) error {
	tableName := "Movies"

	// Check if table exists
	_, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err == nil {
		pkgslogger.Load(ctx).InfoContext(ctx, "Table already exists, skipping creation", "table", tableName)
	} else {
		// Create Table
		_, err = ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: &tableName,
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("Year"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("Title"), KeyType: types.KeyTypeRange},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("Year"), AttributeType: types.ScalarAttributeTypeN},
				{AttributeName: aws.String("Title"), AttributeType: types.ScalarAttributeTypeS},
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(defaultCapacity),
				WriteCapacityUnits: aws.Int64(defaultCapacity),
			},
			StreamSpecification: &types.StreamSpecification{
				StreamEnabled:  aws.Bool(true),
				StreamViewType: types.StreamViewTypeNewAndOldImages,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		pkgslogger.Load(ctx).InfoContext(ctx, "Created table", "table", tableName)
	}

	// Insert Items
	items := []map[string]types.AttributeValue{
		{
			"Year":  &types.AttributeValueMemberN{Value: "2023"},
			"Title": &types.AttributeValueMemberS{Value: "The Gopher Movie"},
			"Info":  &types.AttributeValueMemberS{Value: "A movie about Gophers"},
		},
		{
			"Year":  &types.AttributeValueMemberN{Value: "2024"},
			"Title": &types.AttributeValueMemberS{Value: "Gopher Returns"},
			"Info":  &types.AttributeValueMemberS{Value: "The sequel"},
		},
	}

	for _, item := range items {
		_, err = ddb.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &tableName,
			Item:      item,
		})
		if err != nil {
			return fmt.Errorf("failed to put item: %w", err)
		}
	}
	pkgslogger.Load(ctx).InfoContext(ctx, "Loaded DynamoDB items", "count", len(items))

	return nil
}

func loadS3(ctx context.Context, s3Client *s3.Client) error {
	bucketName := "demo-bucket"

	// Create Bucket
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		// Ignore error if bucket exists (naive check)
		// SDK returns specific error but for demo code we can just log and continue or assume it exists
		// Wait, CreateBucket returns error if exists?
		// "BucketAlreadyOwnedByYou"
		if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") &&
			!strings.Contains(err.Error(), "BucketAlreadyExists") {
			// In-memory backend might return generic error or specific.
			// We'll log and continue if it fails, maybe it already exists.
			pkgslogger.Load(ctx).WarnContext(
				ctx,
				"Failed to create bucket (might exist)",
				"bucket",
				bucketName,
				"error",
				err,
			)
		}
	} else {
		pkgslogger.Load(ctx).InfoContext(ctx, "Created bucket", "bucket", bucketName)
	}

	_, err = s3Client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: &bucketName,
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	if err != nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "Failed to enable versioning", "error", err)
	} else {
		pkgslogger.Load(ctx).InfoContext(ctx, "Enabled versioning", "bucket", bucketName)
	}

	// Upload Files
	files := map[string]string{
		"hello.txt": "Hello Gopherstack! (v1)",
		"notes.md":  "# Notes\n\nThis is a demo file.",
	}

	for key, content := range files {
		_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &bucketName,
			Key:         aws.String(key),
			Body:        strings.NewReader(content),
			ContentType: aws.String("text/plain"),
		})
		if err != nil {
			return fmt.Errorf("failed to upload %s: %w", key, err)
		}
	}

	// Upload second version for hello.txt
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucketName,
		Key:         aws.String("hello.txt"),
		Body:        strings.NewReader("Hello Gopherstack! (v2) - Updated version"),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload hello.txt v2: %w", err)
	}

	pkgslogger.Load(ctx).InfoContext(ctx, "Loaded S3 files", "count", len(files)+1)

	return nil
}

func loadSQS(ctx context.Context, sqsClient *sqs.Client) {
	queueName := "demo-queue"
	_, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: &queueName,
	})
	if err != nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create queue", "error", err)
	} else {
		pkgslogger.Load(ctx).InfoContext(ctx, "Created SQS queue", "name", queueName)
	}
}

func loadSNS(ctx context.Context, snsClient *sns.Client, _ *sqs.Client) error {
	topicName := "demo-topic"
	topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: &topicName,
	})
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	pkgslogger.Load(ctx).InfoContext(ctx, "Created SNS topic", "name", topicName)

	// Create subscription to the SQS queue
	queueName := "demo-queue"
	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(arn.Build("sqs", config.DefaultRegion, config.DefaultAccountID, queueName)),
	})
	if err != nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "Failed to subscribe queue to topic", "error", err)
	} else {
		pkgslogger.Load(ctx).InfoContext(ctx, "Subscribed SQS queue to SNS topic")
	}

	// Create a demo platform application for visual inspection of the dashboard.
	_, err = snsClient.CreatePlatformApplication(ctx, &sns.CreatePlatformApplicationInput{
		Name:       aws.String("demo-gcm-app"),
		Platform:   aws.String("GCM"),
		Attributes: map[string]string{},
	})
	if err != nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create demo platform application", "error", err)
	} else {
		pkgslogger.Load(ctx).InfoContext(ctx, "Created SNS platform application", "name", "demo-gcm-app")
	}

	return nil
}

func loadIAM(ctx context.Context, iamClient *iam.Client) {
	log := pkgslogger.Load(ctx)

	// Create a demo user.
	userName := "demo-user"
	_, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{
		UserName: &userName,
	})
	if err != nil {
		log.WarnContext(ctx, "Failed to create IAM user", "error", err)
	} else {
		log.InfoContext(ctx, "Created IAM user", "name", userName)
	}

	// Create a managed policy with a real document.
	policyName := "demo-s3-read-only"
	policyDoc := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket"],"Resource":"*"}]}`
	createPolicyOut, err := iamClient.CreatePolicy(ctx, &iam.CreatePolicyInput{
		PolicyName:     &policyName,
		PolicyDocument: aws.String(policyDoc),
		Description:    aws.String("Demo read-only S3 policy"),
	})
	if err != nil {
		log.WarnContext(ctx, "Failed to create IAM policy", "error", err)
	} else {
		log.InfoContext(ctx, "Created IAM policy", "name", policyName)

		// Attach the policy to the demo user.
		_, err = iamClient.AttachUserPolicy(ctx, &iam.AttachUserPolicyInput{
			UserName:  &userName,
			PolicyArn: createPolicyOut.Policy.Arn,
		})
		if err != nil {
			log.WarnContext(ctx, "Failed to attach policy to demo user", "error", err)
		}
	}

	// Create a demo group and add the user.
	groupName := "demo-group"
	_, err = iamClient.CreateGroup(ctx, &iam.CreateGroupInput{
		GroupName: &groupName,
	})
	if err != nil {
		log.WarnContext(ctx, "Failed to create IAM group", "error", err)
	} else {
		log.InfoContext(ctx, "Created IAM group", "name", groupName)

		_, err = iamClient.AddUserToGroup(ctx, &iam.AddUserToGroupInput{
			GroupName: &groupName,
			UserName:  &userName,
		})
		if err != nil {
			log.WarnContext(ctx, "Failed to add demo user to group", "error", err)
		}
	}

	// Create a role.
	roleName := "demo-role"
	assumePolicyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err = iamClient.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 &roleName,
		AssumeRolePolicyDocument: aws.String(assumePolicyDoc),
		Description:              aws.String("Demo EC2 instance role"),
	})
	if err != nil {
		log.WarnContext(ctx, "Failed to create IAM role", "error", err)
	} else {
		log.InfoContext(ctx, "Created IAM role", "name", roleName)
	}

	// Create an access key for the demo user so the enforcement badge is meaningful.
	_, err = iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
		UserName: &userName,
	})
	if err != nil {
		log.WarnContext(ctx, "Failed to create access key for demo user", "error", err)
	} else {
		log.InfoContext(ctx, "Created access key for demo user")
	}
}

func loadSSM(ctx context.Context, ssmClient *ssm.Client) error {
	params := map[string]string{
		"/demo/config/api_key": "secret-api-key-123",
		"/demo/config/env":     "development",
	}

	for name, value := range params {
		_, err := ssmClient.PutParameter(ctx, &ssm.PutParameterInput{
			Name:      aws.String(name),
			Value:     aws.String(value),
			Type:      ssmtypes.ParameterTypeSecureString,
			Overwrite: aws.Bool(true),
		})
		if err != nil {
			return fmt.Errorf("failed to put ssm parameter %s: %w", name, err)
		}
	}
	pkgslogger.Load(ctx).InfoContext(ctx, "Loaded SSM parameters", "count", len(params))

	return nil
}

func loadKMS(ctx context.Context, kmsClient *kms.Client) {
	_, err := kmsClient.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("Demo key for encryption"),
	})
	if err != nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create KMS key", "error", err)
	} else {
		pkgslogger.Load(ctx).InfoContext(ctx, "Created KMS key")
	}
}

func loadSecretsManager(ctx context.Context, smClient *secretsmanager.Client) {
	secretName := "demo/database/password"
	_, err := smClient.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         &secretName,
		SecretString: aws.String(`{"username":"admin","password":"password123"}`),
	})
	if err != nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create secret", "error", err)
	} else {
		pkgslogger.Load(ctx).InfoContext(ctx, "Created secret", "name", secretName)
	}
}

func loadECR(ctx context.Context, ecrClient *ecr.Client) {
	repositories := []string{
		"demo-app/backend",
		"demo-app/frontend",
		"demo-app/worker",
	}

	for _, name := range repositories {
		_, err := ecrClient.CreateRepository(ctx, &ecr.CreateRepositoryInput{
			RepositoryName: aws.String(name),
		})
		if err != nil {
			pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create ECR repository", "name", name, "error", err)
		} else {
			pkgslogger.Load(ctx).InfoContext(ctx, "Created ECR repository", "name", name)
		}
	}
}

func loadAppSync(ctx context.Context, appSyncClient *appsync.Client) {
	apis := []struct {
		name     string
		authType appsynctypes.AuthenticationType
	}{
		{"my-graphql-api", appsynctypes.AuthenticationTypeApiKey},
		{"user-service-api", appsynctypes.AuthenticationTypeAwsIam},
	}

	for _, a := range apis {
		_, err := appSyncClient.CreateGraphqlApi(ctx, &appsync.CreateGraphqlApiInput{
			Name:               aws.String(a.name),
			AuthenticationType: a.authType,
		})
		if err != nil {
			pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create AppSync API", "name", a.name, "error", err)
		} else {
			pkgslogger.Load(ctx).InfoContext(ctx, "Created AppSync API", "name", a.name)
		}
	}
}

func loadECS(ctx context.Context, ecsClient *ecs.Client) {
	const demoWebPort = int32(80)

	clusters := []string{
		"demo-cluster",
		"production-cluster",
	}

	for _, name := range clusters {
		_, err := ecsClient.CreateCluster(ctx, &ecs.CreateClusterInput{
			ClusterName: aws.String(name),
		})
		if err != nil {
			pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create ECS cluster", "name", name, "error", err)
		} else {
			pkgslogger.Load(ctx).InfoContext(ctx, "Created ECS cluster", "name", name)
		}
	}

	_, err := ecsClient.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String("demo-web"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{
				Name:      aws.String("web"),
				Image:     aws.String("nginx:latest"),
				Essential: aws.Bool(true),
				PortMappings: []ecstypes.PortMapping{
					{ContainerPort: aws.Int32(demoWebPort)},
				},
			},
		},
	})
	if err != nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "Failed to register ECS task definition", "error", err)
	} else {
		pkgslogger.Load(ctx).InfoContext(ctx, "Registered ECS task definition", "family", "demo-web")
	}
}

func loadIoT(ctx context.Context, iotClient *iot.Client) {
	things := []string{"smart-sensor-1", "smart-sensor-2", "gateway-device-1"}

	for _, thingName := range things {
		_, err := iotClient.CreateThing(ctx, &iot.CreateThingInput{
			ThingName: aws.String(thingName),
		})
		if err != nil {
			pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create IoT thing", "name", thingName, "error", err)
		} else {
			pkgslogger.Load(ctx).InfoContext(ctx, "Created IoT thing", "name", thingName)
		}
	}

	_, err := iotClient.CreateTopicRule(ctx, &iot.CreateTopicRuleInput{
		RuleName: aws.String("demo_sensor_rule"),
		TopicRulePayload: &iottypes.TopicRulePayload{
			Sql:         aws.String("SELECT temperature FROM 'sensors/+/data'"),
			Description: aws.String("Demo rule for sensor data"),
			Actions:     []iottypes.Action{},
		},
	})
	if err != nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create IoT topic rule", "error", err)
	} else {
		pkgslogger.Load(ctx).InfoContext(ctx, "Created IoT topic rule", "name", "demo_sensor_rule")
	}
}

func loadAmplify(ctx context.Context, amplifyClient *amplify.Client) {
	apps := []struct {
		name     string
		platform amplifytypes.Platform
	}{
		{"my-web-app", amplifytypes.PlatformWeb},
		{"my-nextjs-app", amplifytypes.PlatformWebCompute},
	}

	for _, a := range apps {
		out, err := amplifyClient.CreateApp(ctx, &amplify.CreateAppInput{
			Name:     aws.String(a.name),
			Platform: a.platform,
		})
		if err != nil {
			pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create Amplify app", "name", a.name, "error", err)

			continue
		}

		pkgslogger.Load(ctx).InfoContext(ctx, "Created Amplify app", "name", a.name)

		if out.App == nil {
			continue
		}

		_, bErr := amplifyClient.CreateBranch(ctx, &amplify.CreateBranchInput{
			AppId:      out.App.AppId,
			BranchName: aws.String("main"),
		})
		if bErr != nil {
			pkgslogger.Load(ctx).WarnContext(ctx, "Failed to create Amplify branch", "app", a.name, "error", bErr)
		} else {
			pkgslogger.Load(ctx).InfoContext(ctx, "Created Amplify branch", "app", a.name, "branch", "main")
		}
	}
}

func loadCodeDeploy(ctx context.Context, client *codedeploysvc.Client) {
	apps := []struct {
		name     string
		platform codedeploytypes.ComputePlatform
	}{
		{"demo-server-app", codedeploytypes.ComputePlatformServer},
		{"demo-lambda-app", codedeploytypes.ComputePlatformLambda},
		{"demo-ecs-app", codedeploytypes.ComputePlatformEcs},
	}

	for _, a := range apps {
		_, err := client.CreateApplication(ctx, &codedeploysvc.CreateApplicationInput{
			ApplicationName: aws.String(a.name),
			ComputePlatform: a.platform,
		})
		if err != nil {
			pkgslogger.Load(ctx).
				WarnContext(ctx, "Failed to create CodeDeploy application", "name", a.name, "error", err)

			continue
		}

		pkgslogger.Load(ctx).InfoContext(ctx, "Created CodeDeploy application", "name", a.name, "platform", a.platform)
	}
}

func loadCodePipeline(ctx context.Context, client *codepipelinesvc.Client) {
	pipelines := []struct {
		name     string
		roleARN  string
		location string
	}{
		{"demo-deploy-pipeline", "arn:aws:iam::000000000000:role/demo-pipeline-role", "demo-artifact-bucket"},
		{"demo-build-pipeline", "arn:aws:iam::000000000000:role/demo-pipeline-role", "demo-artifact-bucket"},
	}

	for _, p := range pipelines {
		_, err := client.CreatePipeline(ctx, &codepipelinesvc.CreatePipelineInput{
			Pipeline: &codepipelinetypes.PipelineDeclaration{
				Name:    aws.String(p.name),
				RoleArn: aws.String(p.roleARN),
				ArtifactStore: &codepipelinetypes.ArtifactStore{
					Type:     codepipelinetypes.ArtifactStoreTypeS3,
					Location: aws.String(p.location),
				},
				Stages: []codepipelinetypes.StageDeclaration{
					{
						Name: aws.String("Source"),
						Actions: []codepipelinetypes.ActionDeclaration{
							{
								Name: aws.String("SourceAction"),
								ActionTypeId: &codepipelinetypes.ActionTypeId{
									Category: codepipelinetypes.ActionCategorySource,
									Owner:    codepipelinetypes.ActionOwnerAws,
									Provider: aws.String("CodeCommit"),
									Version:  aws.String("1"),
								},
								OutputArtifacts: []codepipelinetypes.OutputArtifact{
									{Name: aws.String("source_output")},
								},
								Configuration: map[string]string{
									"RepositoryName": "demo-repo",
									"BranchName":     "main",
								},
							},
						},
					},
					{
						Name: aws.String("Deploy"),
						Actions: []codepipelinetypes.ActionDeclaration{
							{
								Name: aws.String("DeployAction"),
								ActionTypeId: &codepipelinetypes.ActionTypeId{
									Category: codepipelinetypes.ActionCategoryDeploy,
									Owner:    codepipelinetypes.ActionOwnerAws,
									Provider: aws.String("CloudFormation"),
									Version:  aws.String("1"),
								},
								InputArtifacts: []codepipelinetypes.InputArtifact{
									{Name: aws.String("source_output")},
								},
								Configuration: map[string]string{
									"ActionMode": "CREATE_UPDATE",
									"StackName":  "demo-stack",
								},
							},
						},
					},
				},
			},
			Tags: []codepipelinetypes.Tag{
				{Key: aws.String("Environment"), Value: aws.String("demo")},
			},
		})
		if err != nil {
			pkgslogger.Load(ctx).
				WarnContext(ctx, "Failed to create CodePipeline pipeline", "name", p.name, "error", err)

			continue
		}

		pkgslogger.Load(ctx).InfoContext(ctx, "Created CodePipeline pipeline", "name", p.name)
	}
}

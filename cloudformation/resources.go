package cloudformation

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	apigwbackend "github.com/blackbirdworks/gopherstack/apigateway"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/cloudwatchlogs"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	ebbackend "github.com/blackbirdworks/gopherstack/eventbridge"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	sfnbackend "github.com/blackbirdworks/gopherstack/stepfunctions"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
)

// ServiceBackends holds references to all service backends.
type ServiceBackends struct {
	DynamoDB       *ddbbackend.DynamoDBHandler
	S3             *s3backend.S3Handler
	SQS            *sqsbackend.Handler
	SNS            *snsbackend.Handler
	SSM            *ssmbackend.Handler
	KMS            *kmsbackend.Handler
	SecretsManager *secretsmanagerbackend.Handler
	Lambda         *lambdabackend.Handler
	EventBridge    *ebbackend.Handler
	StepFunctions  *sfnbackend.Handler
	CloudWatchLogs *cwlogsbackend.Handler
	APIGateway     *apigwbackend.Handler
	AccountID      string
	Region         string
}

// ResourceCreator creates and deletes cloud resources.
type ResourceCreator struct {
	backends *ServiceBackends
}

// NewResourceCreator returns a ResourceCreator backed by the given services.
func NewResourceCreator(backends *ServiceBackends) *ResourceCreator {
	return &ResourceCreator{backends: backends}
}

// Create creates a resource and returns its physical ID.
func (rc *ResourceCreator) Create(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params map[string]string,
	physicalIDs map[string]string,
) (string, error) {
	if rc == nil || rc.backends == nil {
		return logicalID + "-" + uuid.New().String()[:8], nil
	}
	switch resourceType {
	case "AWS::S3::Bucket":
		return rc.createS3Bucket(ctx, logicalID, props, params, physicalIDs)
	case "AWS::DynamoDB::Table":
		return rc.createDynamoDBTable(ctx, logicalID, props, params, physicalIDs)
	case "AWS::SQS::Queue":
		return rc.createSQSQueue(ctx, logicalID, props, params, physicalIDs)
	case "AWS::SNS::Topic":
		return rc.createSNSTopic(ctx, logicalID, props, params, physicalIDs)
	case "AWS::SSM::Parameter":
		return rc.createSSMParameter(ctx, logicalID, props, params, physicalIDs)
	case "AWS::KMS::Key":
		return rc.createKMSKey(ctx, logicalID, props, params, physicalIDs)
	case "AWS::SecretsManager::Secret":
		return rc.createSecretsManagerSecret(ctx, logicalID, props, params, physicalIDs)
	case "AWS::Lambda::Function":
		return rc.createLambdaFunction(ctx, logicalID, props, params, physicalIDs)
	case "AWS::Events::Rule":
		return rc.createEventBridgeRule(ctx, logicalID, props, params, physicalIDs)
	case "AWS::StepFunctions::StateMachine":
		return rc.createStepFunctionsStateMachine(ctx, logicalID, props, params, physicalIDs)
	case "AWS::Logs::LogGroup":
		return rc.createCloudWatchLogGroup(ctx, logicalID, props, params, physicalIDs)
	case "AWS::ApiGateway::RestApi":
		return rc.createAPIGatewayRestAPI(ctx, logicalID, props, params, physicalIDs)
	default:
		return logicalID + "-stub", nil
	}
}

// Delete deletes a resource by type and physical ID.
func (rc *ResourceCreator) Delete(
	ctx context.Context,
	resourceType, physicalID string,
	_ map[string]any,
) error {
	if rc == nil || rc.backends == nil {
		return nil
	}
	switch resourceType {
	case "AWS::S3::Bucket":
		return rc.deleteS3Bucket(ctx, physicalID)
	case "AWS::DynamoDB::Table":
		return rc.deleteDynamoDBTable(ctx, physicalID)
	case "AWS::SQS::Queue":
		return rc.deleteSQSQueue(ctx, physicalID)
	case "AWS::SNS::Topic":
		return rc.deleteSNSTopic(ctx, physicalID)
	case "AWS::SSM::Parameter":
		return rc.deleteSSMParameter(ctx, physicalID)
	case "AWS::KMS::Key":
		return rc.deleteKMSKey(ctx, physicalID)
	case "AWS::SecretsManager::Secret":
		return rc.deleteSecretsManagerSecret(ctx, physicalID)
	case "AWS::Lambda::Function":
		return rc.deleteLambdaFunction(physicalID)
	case "AWS::Events::Rule":
		return rc.deleteEventBridgeRule(ctx, physicalID)
	case "AWS::StepFunctions::StateMachine":
		return rc.deleteStepFunctionsStateMachine(ctx, physicalID)
	case "AWS::Logs::LogGroup":
		return rc.deleteCloudWatchLogGroup(physicalID)
	case "AWS::ApiGateway::RestApi":
		return rc.deleteAPIGatewayRestAPI(ctx, physicalID)
	default:
		return nil
	}
}

func resolve(v any, params, physicalIDs map[string]string) string {
	return ResolveValue(v, params, physicalIDs)
}

func strProp(props map[string]any, key string, params, physicalIDs map[string]string) string {
	return resolve(props[key], params, physicalIDs)
}

func (rc *ResourceCreator) createS3Bucket(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.S3 == nil {
		return logicalID + "-stub", nil
	}
	bucketName := strProp(props, "BucketName", params, physicalIDs)
	if bucketName == "" {
		bucketName = strings.ToLower(logicalID) + "-" + uuid.New().String()[:8]
	}
	_, err := rc.backends.S3.Backend.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to create S3 bucket %s: %w", bucketName, err)
	}

	return bucketName, nil
}

func (rc *ResourceCreator) deleteS3Bucket(ctx context.Context, physicalID string) error {
	if rc.backends.S3 == nil {
		return nil
	}
	_, _ = rc.backends.S3.Backend.DeleteBucket(ctx, &awss3.DeleteBucketInput{
		Bucket: aws.String(physicalID),
	})

	return nil
}

func (rc *ResourceCreator) createDynamoDBTable(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DynamoDB == nil {
		return logicalID + "-stub", nil
	}
	tableName := strProp(props, "TableName", params, physicalIDs)
	if tableName == "" {
		tableName = logicalID
	}
	attrDefs := parseDDBAttributeDefinitions(props, params, physicalIDs)
	keySchema := parseDDBKeySchema(props, params, physicalIDs)
	billingMode := strProp(props, "BillingMode", params, physicalIDs)
	input := &awsddb.CreateTableInput{
		TableName:            aws.String(tableName),
		AttributeDefinitions: attrDefs,
		KeySchema:            keySchema,
	}
	if billingMode == "PAY_PER_REQUEST" {
		input.BillingMode = ddbtypes.BillingModePayPerRequest
	} else {
		input.ProvisionedThroughput = parseDDBProvisionedThroughput(props)
	}
	_, err := rc.backends.DynamoDB.Backend.CreateTable(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to create DynamoDB table %s: %w", tableName, err)
	}

	return tableName, nil
}

func (rc *ResourceCreator) deleteDynamoDBTable(ctx context.Context, physicalID string) error {
	if rc.backends.DynamoDB == nil {
		return nil
	}
	_, err := rc.backends.DynamoDB.Backend.DeleteTable(ctx, &awsddb.DeleteTableInput{
		TableName: aws.String(physicalID),
	})

	return err
}

func parseDDBAttributeDefinitions(
	props map[string]any,
	params, physicalIDs map[string]string,
) []ddbtypes.AttributeDefinition {
	rawList, _ := props["AttributeDefinitions"].([]any)
	defs := make([]ddbtypes.AttributeDefinition, 0, len(rawList))
	for _, item := range rawList {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := resolve(m["AttributeName"], params, physicalIDs)
		attrType := resolve(m["AttributeType"], params, physicalIDs)
		var at ddbtypes.ScalarAttributeType
		switch attrType {
		case "N":
			at = ddbtypes.ScalarAttributeTypeN
		case "B":
			at = ddbtypes.ScalarAttributeTypeB
		default:
			at = ddbtypes.ScalarAttributeTypeS
		}
		defs = append(defs, ddbtypes.AttributeDefinition{
			AttributeName: aws.String(name),
			AttributeType: at,
		})
	}

	return defs
}

func parseDDBKeySchema(props map[string]any, params, physicalIDs map[string]string) []ddbtypes.KeySchemaElement {
	rawList, _ := props["KeySchema"].([]any)
	schema := make([]ddbtypes.KeySchemaElement, 0, len(rawList))
	for _, item := range rawList {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := resolve(m["AttributeName"], params, physicalIDs)
		keyType := resolve(m["KeyType"], params, physicalIDs)
		var kt ddbtypes.KeyType
		switch strings.ToUpper(keyType) {
		case "RANGE":
			kt = ddbtypes.KeyTypeRange
		default:
			kt = ddbtypes.KeyTypeHash
		}
		schema = append(schema, ddbtypes.KeySchemaElement{
			AttributeName: aws.String(name),
			KeyType:       kt,
		})
	}

	return schema
}

const (
	defaultCapacityUnits     = int64(5)
	kmsMinDeletionWindowDays = 7
	boolTrue                 = "true"
)

func parseDDBProvisionedThroughput(props map[string]any) *ddbtypes.ProvisionedThroughput {
	pt, _ := props["ProvisionedThroughput"].(map[string]any)
	rcu := defaultCapacityUnits
	wcu := defaultCapacityUnits
	if pt != nil {
		if v, ok := pt["ReadCapacityUnits"].(float64); ok {
			rcu = int64(v)
		}
		if v, ok := pt["WriteCapacityUnits"].(float64); ok {
			wcu = int64(v)
		}
	}

	return &ddbtypes.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(rcu),
		WriteCapacityUnits: aws.Int64(wcu),
	}
}

func (rc *ResourceCreator) createSQSQueue(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SQS == nil {
		return logicalID + "-stub", nil
	}
	queueName := strProp(props, "QueueName", params, physicalIDs)
	if queueName == "" {
		queueName = logicalID
	}
	attrs := map[string]string{}
	if vt := strProp(props, "VisibilityTimeout", params, physicalIDs); vt != "" {
		attrs["VisibilityTimeout"] = vt
	}
	if mrt := strProp(props, "MessageRetentionPeriod", params, physicalIDs); mrt != "" {
		attrs["MessageRetentionPeriod"] = mrt
	}
	if isFIFO, _ := props["FifoQueue"].(bool); isFIFO {
		queueName = strings.TrimSuffix(queueName, ".fifo") + ".fifo"
		attrs["FifoQueue"] = boolTrue
	}
	out, err := rc.backends.SQS.Backend.CreateQueue(&sqsbackend.CreateQueueInput{
		QueueName:  queueName,
		Attributes: attrs,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create SQS queue %s: %w", queueName, err)
	}

	return out.QueueURL, nil
}

func (rc *ResourceCreator) deleteSQSQueue(_ context.Context, physicalID string) error {
	if rc.backends.SQS == nil {
		return nil
	}
	_ = rc.backends.SQS.Backend.DeleteQueue(&sqsbackend.DeleteQueueInput{QueueURL: physicalID})

	return nil
}

func (rc *ResourceCreator) createSNSTopic(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SNS == nil {
		return logicalID + "-stub", nil
	}
	topicName := strProp(props, "TopicName", params, physicalIDs)
	if topicName == "" {
		topicName = logicalID
	}
	attrs := map[string]string{}
	if isFIFO, _ := props["FifoTopic"].(bool); isFIFO {
		attrs["FifoTopic"] = boolTrue
	}
	topic, err := rc.backends.SNS.Backend.CreateTopic(topicName, attrs)
	if err != nil {
		return "", fmt.Errorf("failed to create SNS topic %s: %w", topicName, err)
	}

	return topic.TopicArn, nil
}

func (rc *ResourceCreator) deleteSNSTopic(_ context.Context, physicalID string) error {
	if rc.backends.SNS == nil {
		return nil
	}
	_ = rc.backends.SNS.Backend.DeleteTopic(physicalID)

	return nil
}

func (rc *ResourceCreator) createSSMParameter(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = "/" + logicalID
	}
	paramType := strProp(props, "Type", params, physicalIDs)
	if paramType == "" {
		paramType = "String"
	}
	value := strProp(props, "Value", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)
	_, err := rc.backends.SSM.Backend.PutParameter(&ssmbackend.PutParameterInput{
		Name:        name,
		Type:        paramType,
		Value:       value,
		Description: description,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create SSM parameter %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteSSMParameter(_ context.Context, physicalID string) error {
	if rc.backends.SSM == nil {
		return nil
	}
	_, _ = rc.backends.SSM.Backend.DeleteParameter(&ssmbackend.DeleteParameterInput{Name: physicalID})

	return nil
}

func (rc *ResourceCreator) createKMSKey(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KMS == nil {
		return logicalID + "-stub", nil
	}
	description := strProp(props, "Description", params, physicalIDs)
	out, err := rc.backends.KMS.Backend.CreateKey(&kmsbackend.CreateKeyInput{
		Description: description,
		KeyUsage:    "ENCRYPT_DECRYPT",
	})
	if err != nil {
		return "", fmt.Errorf("failed to create KMS key: %w", err)
	}

	return out.KeyMetadata.KeyID, nil
}

func (rc *ResourceCreator) deleteKMSKey(_ context.Context, physicalID string) error {
	if rc.backends.KMS == nil {
		return nil
	}
	_, _ = rc.backends.KMS.Backend.ScheduleKeyDeletion(&kmsbackend.ScheduleKeyDeletionInput{
		KeyID:               physicalID,
		PendingWindowInDays: kmsMinDeletionWindowDays,
	})

	return nil
}

func (rc *ResourceCreator) createSecretsManagerSecret(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SecretsManager == nil {
		return logicalID + "-stub", nil
	}
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}
	description := strProp(props, "Description", params, physicalIDs)
	secretString := strProp(props, "SecretString", params, physicalIDs)
	out, err := rc.backends.SecretsManager.Backend.CreateSecret(&secretsmanagerbackend.CreateSecretInput{
		Name:         name,
		Description:  description,
		SecretString: secretString,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create secret %s: %w", name, err)
	}

	return out.ARN, nil
}

func (rc *ResourceCreator) deleteSecretsManagerSecret(_ context.Context, physicalID string) error {
	if rc.backends.SecretsManager == nil {
		return nil
	}
	_, _ = rc.backends.SecretsManager.Backend.DeleteSecret(&secretsmanagerbackend.DeleteSecretInput{
		SecretID:                   physicalID,
		ForceDeleteWithoutRecovery: true,
	})

	return nil
}

// createLambdaFunction creates a Lambda function from CloudFormation template properties.
func (rc *ResourceCreator) createLambdaFunction(
_ context.Context,
logicalID string,
props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
if rc.backends.Lambda == nil {
return logicalID + "-stub", nil
}

name := strProp(props, "FunctionName", params, physicalIDs)
if name == "" {
name = logicalID + "-" + uuid.New().String()[:8]
}

runtime := strProp(props, "Runtime", params, physicalIDs)
handler := strProp(props, "Handler", params, physicalIDs)
role := strProp(props, "Role", params, physicalIDs)

fn := &lambdabackend.FunctionConfiguration{
FunctionName: name,
Runtime:      runtime,
Handler:      handler,
Role:         role,
}

if err := rc.backends.Lambda.Backend.CreateFunction(fn); err != nil {
return "", fmt.Errorf("create Lambda function: %w", err)
}

return name, nil
}

func (rc *ResourceCreator) deleteLambdaFunction(name string) error {
if rc.backends.Lambda == nil {
return nil
}
// Ignore not-found errors on delete
_ = rc.backends.Lambda.Backend.DeleteFunction(name)

return nil
}

// createEventBridgeRule creates an EventBridge rule from CloudFormation template properties.
func (rc *ResourceCreator) createEventBridgeRule(
_ context.Context,
logicalID string,
props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
if rc.backends.EventBridge == nil {
return logicalID + "-stub", nil
}

name := strProp(props, "Name", params, physicalIDs)
if name == "" {
name = logicalID
}

eventBusName := strProp(props, "EventBusName", params, physicalIDs)
if eventBusName == "" {
eventBusName = "default"
}

pattern := strProp(props, "EventPattern", params, physicalIDs)
schedExpr := strProp(props, "ScheduleExpression", params, physicalIDs)
state := strProp(props, "State", params, physicalIDs)
if state == "" {
state = "ENABLED"
}

input := ebbackend.PutRuleInput{
Name:               name,
EventBusName:       eventBusName,
EventPattern:       pattern,
ScheduleExpression: schedExpr,
State:              state,
}

rule, err := rc.backends.EventBridge.Backend.PutRule(input)
if err != nil {
return "", fmt.Errorf("create EventBridge rule: %w", err)
}

return rule.Arn, nil
}

func (rc *ResourceCreator) deleteEventBridgeRule(_ context.Context, physicalID string) error {
if rc.backends.EventBridge == nil {
return nil
}
// physicalID is the rule ARN; extract name from it
parts := strings.Split(physicalID, "/")
name := parts[len(parts)-1]
_ = rc.backends.EventBridge.Backend.DeleteRule(name, "default")

return nil
}

// createStepFunctionsStateMachine creates a Step Functions state machine.
func (rc *ResourceCreator) createStepFunctionsStateMachine(
_ context.Context,
logicalID string,
props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
if rc.backends.StepFunctions == nil {
return logicalID + "-stub", nil
}

name := strProp(props, "StateMachineName", params, physicalIDs)
if name == "" {
name = logicalID
}

definition := strProp(props, "DefinitionString", params, physicalIDs)
roleArn := strProp(props, "RoleArn", params, physicalIDs)
smType := strProp(props, "StateMachineType", params, physicalIDs)
if smType == "" {
smType = "STANDARD"
}

sm, err := rc.backends.StepFunctions.Backend.CreateStateMachine(name, definition, roleArn, smType)
if err != nil {
return "", fmt.Errorf("create StepFunctions state machine: %w", err)
}

return sm.StateMachineArn, nil
}

func (rc *ResourceCreator) deleteStepFunctionsStateMachine(_ context.Context, arn string) error {
if rc.backends.StepFunctions == nil {
return nil
}
_ = rc.backends.StepFunctions.Backend.DeleteStateMachine(arn)

return nil
}

// createCloudWatchLogGroup creates a CloudWatch Logs log group.
func (rc *ResourceCreator) createCloudWatchLogGroup(
_ context.Context,
logicalID string,
props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
if rc.backends.CloudWatchLogs == nil {
return logicalID + "-stub", nil
}

name := strProp(props, "LogGroupName", params, physicalIDs)
if name == "" {
name = "/aws/cfn/" + logicalID
}

_, err := rc.backends.CloudWatchLogs.Backend.CreateLogGroup(name)
if err != nil {
return "", fmt.Errorf("create CloudWatch Logs log group: %w", err)
}

return name, nil
}

func (rc *ResourceCreator) deleteCloudWatchLogGroup(name string) error {
if rc.backends.CloudWatchLogs == nil {
return nil
}
_ = rc.backends.CloudWatchLogs.Backend.DeleteLogGroup(name)

return nil
}

// createAPIGatewayRestAPI creates an API Gateway REST API.
func (rc *ResourceCreator) createAPIGatewayRestAPI(
_ context.Context,
logicalID string,
props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
if rc.backends.APIGateway == nil {
return logicalID + "-stub", nil
}

name := strProp(props, "Name", params, physicalIDs)
if name == "" {
name = logicalID
}

description := strProp(props, "Description", params, physicalIDs)

api, err := rc.backends.APIGateway.Backend.CreateRestAPI(name, description, nil)
if err != nil {
return "", fmt.Errorf("create API Gateway REST API: %w", err)
}

return api.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayRestAPI(_ context.Context, apiID string) error {
if rc.backends.APIGateway == nil {
return nil
}
_ = rc.backends.APIGateway.Backend.DeleteRestAPI(apiID)

return nil
}

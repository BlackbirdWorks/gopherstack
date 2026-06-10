import { CostExplorerClient } from "@aws-sdk/client-cost-explorer";
import { EC2Client } from "@aws-sdk/client-ec2";
import { KinesisAnalyticsClient } from "@aws-sdk/client-kinesis-analytics";
import { KinesisAnalyticsV2Client } from "@aws-sdk/client-kinesis-analytics-v2";
import { LakeFormationClient } from "@aws-sdk/client-lakeformation";
import { ManagedBlockchainClient } from "@aws-sdk/client-managedblockchain";
import { ECSClient } from "@aws-sdk/client-ecs";
import { CognitoIdentityProviderClient } from "@aws-sdk/client-cognito-identity-provider";
import { AthenaClient } from "@aws-sdk/client-athena";
import { SESClient } from "@aws-sdk/client-ses";
import { CloudTrailClient } from "@aws-sdk/client-cloudtrail";
import { ConfigServiceClient } from "@aws-sdk/client-config-service";
import { BackupClient } from "@aws-sdk/client-backup";
import { ElasticLoadBalancingV2Client } from "@aws-sdk/client-elastic-load-balancing-v2";
import { TransferClient } from "@aws-sdk/client-transfer";
import { ACMClient } from "@aws-sdk/client-acm";
import { OrganizationsClient } from "@aws-sdk/client-organizations";
import { AppSyncClient } from "@aws-sdk/client-appsync";
import { CognitoIdentityClient } from "@aws-sdk/client-cognito-identity";
import { GuardDutyClient } from "@aws-sdk/client-guardduty";
import { KafkaClient } from "@aws-sdk/client-kafka";
import { APIGatewayClient } from "@aws-sdk/client-api-gateway";
import { ApiGatewayV2Client } from "@aws-sdk/client-apigatewayv2";
import { BedrockClient } from "@aws-sdk/client-bedrock";
import { SchedulerClient } from "@aws-sdk/client-scheduler";
import { AppRunnerClient } from "@aws-sdk/client-apprunner";
import { IoTClient } from "@aws-sdk/client-iot";
import { SecurityHubClient } from "@aws-sdk/client-securityhub";
import { Inspector2Client } from "@aws-sdk/client-inspector2";
import { XRayClient } from "@aws-sdk/client-xray";
import { AutoScalingClient } from "@aws-sdk/client-auto-scaling";
import { BatchClient } from "@aws-sdk/client-batch";
import { CloudFormationClient } from "@aws-sdk/client-cloudformation";
import { CloudWatchLogsClient } from "@aws-sdk/client-cloudwatch-logs";
import { ElastiCacheClient } from "@aws-sdk/client-elasticache";
import { ElasticsearchServiceClient } from "@aws-sdk/client-elasticsearch-service";
import { FirehoseClient } from "@aws-sdk/client-firehose";
import { GlueClient } from "@aws-sdk/client-glue";
import { OpenSearchClient } from "@aws-sdk/client-opensearch";
import { RedshiftClient } from "@aws-sdk/client-redshift";
import { RedshiftDataClient } from "@aws-sdk/client-redshift-data";
import { SNSClient } from "@aws-sdk/client-sns";
import { SQSClient } from "@aws-sdk/client-sqs";
import { EventBridgeClient } from "@aws-sdk/client-eventbridge";
import { SSMClient } from "@aws-sdk/client-ssm";
import { CloudWatchClient } from "@aws-sdk/client-cloudwatch";
import { KinesisClient } from "@aws-sdk/client-kinesis";
import { EKSClient } from "@aws-sdk/client-eks";
import { ECRClient } from "@aws-sdk/client-ecr";
import { FisClient } from "@aws-sdk/client-fis";
import { ACMPCAClient } from "@aws-sdk/client-acm-pca";
import { AmplifyClient } from "@aws-sdk/client-amplify";
import { AppConfigClient } from "@aws-sdk/client-appconfig";
import { CodeBuildClient } from "@aws-sdk/client-codebuild";
import { CodePipelineClient } from "@aws-sdk/client-codepipeline";
import { GlobalAcceleratorClient } from "@aws-sdk/client-global-accelerator";
import { LambdaClient } from "@aws-sdk/client-lambda";
import { LightsailClient } from "@aws-sdk/client-lightsail";
import { ServerlessApplicationRepositoryClient } from "@aws-sdk/client-serverlessapplicationrepository";
import { ShieldClient } from "@aws-sdk/client-shield";
import { CloudFrontClient } from "@aws-sdk/client-cloudfront";
import { CodeDeployClient } from "@aws-sdk/client-codedeploy";
import { EMRClient } from "@aws-sdk/client-emr";
import { Route53Client } from "@aws-sdk/client-route-53";
import { WAFV2Client } from "@aws-sdk/client-wafv2";
import { SFNClient } from "@aws-sdk/client-sfn";
import { WorkSpacesClient } from "@aws-sdk/client-workspaces";
import { ApplicationAutoScalingClient } from "@aws-sdk/client-application-auto-scaling";
import { PipesClient } from "@aws-sdk/client-pipes";
import { SESv2Client } from "@aws-sdk/client-sesv2";
import { AccessAnalyzerClient } from "@aws-sdk/client-accessanalyzer";
import { AccountClient } from "@aws-sdk/client-account";
import { AppMeshClient } from "@aws-sdk/client-app-mesh";
import { DataBrewClient } from "@aws-sdk/client-databrew";
import { DataSyncClient } from "@aws-sdk/client-datasync";
import { DAXClient } from "@aws-sdk/client-dax";
import { DetectiveClient } from "@aws-sdk/client-detective";
import { DirectoryServiceClient } from "@aws-sdk/client-directory-service";
import { DLMClient } from "@aws-sdk/client-dlm";
import { ForecastClient } from "@aws-sdk/client-forecast";
import { Macie2Client } from "@aws-sdk/client-macie2";
import { MediaLiveClient } from "@aws-sdk/client-medialive";
import { MediaPackageClient } from "@aws-sdk/client-mediapackage";
import { MediaTailorClient } from "@aws-sdk/client-mediatailor";
import { PersonalizeClient } from "@aws-sdk/client-personalize";
import { QuickSightClient } from "@aws-sdk/client-quicksight";
import { RolesAnywhereClient } from "@aws-sdk/client-rolesanywhere";
import { WorkMailClient } from "@aws-sdk/client-workmail";

const defaultRegion = "us-east-1";

function endpointURL(): string {
  if (typeof window === "undefined" || !window.location) {
    return "http://localhost:8000";
  }

  return window.location.origin;
}

function clientConfig(region = defaultRegion) {
  return {
    endpoint: endpointURL(),
    region,
    credentials: {
      accessKeyId: "test",
      secretAccessKey: "test",
    },
  };
}

export function getElastiCacheClient(region?: string): ElastiCacheClient {
  return new ElastiCacheClient(clientConfig(region));
}

export function getLambdaClient(region?: string): LambdaClient {
  return new LambdaClient(clientConfig(region));
}

export function getFISClient(region?: string): FisClient {
  return new FisClient(clientConfig(region));
}

export function getShieldClient(region?: string): ShieldClient {
  return new ShieldClient(clientConfig(region));
}

export function getCodeBuildClient(region?: string): CodeBuildClient {
  return new CodeBuildClient(clientConfig(region));
}

export function getCodePipelineClient(region?: string): CodePipelineClient {
  return new CodePipelineClient(clientConfig(region));
}

export function getLightsailClient(region?: string): LightsailClient {
  return new LightsailClient(clientConfig(region));
}

export function getServerlessRepoClient(region?: string): ServerlessApplicationRepositoryClient {
  return new ServerlessApplicationRepositoryClient(clientConfig(region));
}

export function getWorkSpacesClient(region?: string): WorkSpacesClient {
  return new WorkSpacesClient(clientConfig(region));
}

export function getAmplifyClient(region?: string): AmplifyClient {
  return new AmplifyClient(clientConfig(region));
}

export function getAppConfigClient(region?: string): AppConfigClient {
  return new AppConfigClient(clientConfig(region));
}

export function getACMPCAClient(region?: string): ACMPCAClient {
  return new ACMPCAClient(clientConfig(region));
}

export function getGlobalAcceleratorClient(region?: string): GlobalAcceleratorClient {
  return new GlobalAcceleratorClient(clientConfig(region));
}

export function getSFNClient(region?: string): SFNClient {
  return new SFNClient(clientConfig(region));
}

export function getSNSClient(region?: string): SNSClient {
  return new SNSClient(clientConfig(region));
}

export function getSQSClient(region?: string): SQSClient {
  return new SQSClient(clientConfig(region));
}

export function getEventBridgeClient(region?: string): EventBridgeClient {
  return new EventBridgeClient(clientConfig(region));
}

export function getSSMClient(region?: string): SSMClient {
  return new SSMClient(clientConfig(region));
}

export function getCloudWatchClient(region?: string): CloudWatchClient {
  return new CloudWatchClient(clientConfig(region));
}

export function getKinesisClient(region?: string): KinesisClient {
  return new KinesisClient(clientConfig(region));
}

export function getEKSClient(region?: string): EKSClient {
  return new EKSClient(clientConfig(region));
}

export function getECRClient(region?: string): ECRClient {
  return new ECRClient(clientConfig(region));
}

export function getAutoScalingClient(region?: string): AutoScalingClient {
  return new AutoScalingClient(clientConfig(region));
}

export function getBatchClient(region?: string): BatchClient {
  return new BatchClient(clientConfig(region));
}

export function getCloudFormationClient(region?: string): CloudFormationClient {
  return new CloudFormationClient(clientConfig(region));
}

export function getCloudWatchLogsClient(region?: string): CloudWatchLogsClient {
  return new CloudWatchLogsClient(clientConfig(region));
}

export function getFirehoseClient(region?: string): FirehoseClient {
  return new FirehoseClient(clientConfig(region));
}

export function getGlueClient(region?: string): GlueClient {
  return new GlueClient(clientConfig(region));
}

export function getOpenSearchClient(region?: string): OpenSearchClient {
  return new OpenSearchClient(clientConfig(region));
}

export function getElasticsearchServiceClient(region?: string): ElasticsearchServiceClient {
  return new ElasticsearchServiceClient(clientConfig(region));
}

export function getRedshiftClient(region?: string): RedshiftClient {
  return new RedshiftClient(clientConfig(region));
}

export function getRedshiftDataClient(region?: string): RedshiftDataClient {
  return new RedshiftDataClient(clientConfig(region));
}

export function getAthenaClient(region?: string): AthenaClient {
  return new AthenaClient(clientConfig(region));
}

export function getCodeDeployClient(region?: string): CodeDeployClient {
  return new CodeDeployClient(clientConfig(region));
}

export function getEMRClient(region?: string): EMRClient {
  return new EMRClient(clientConfig(region));
}

export function getRoute53Client(region?: string): Route53Client {
  return new Route53Client(clientConfig(region));
}

export function getWAFV2Client(region?: string): WAFV2Client {
  return new WAFV2Client(clientConfig(region));
}

export function getCloudFrontClient(region?: string): CloudFrontClient {
  return new CloudFrontClient(clientConfig(region));
}

export function getSESClient(region?: string): SESClient {
  return new SESClient(clientConfig(region));
}

export function getSESv2Client(region?: string): SESv2Client {
  return new SESv2Client(clientConfig(region));
}

export function getCloudTrailClient(region?: string): CloudTrailClient {
  return new CloudTrailClient(clientConfig(region));
}

export function getConfigClient(region?: string): ConfigServiceClient {
  return new ConfigServiceClient(clientConfig(region));
}

export function getBackupClient(region?: string): BackupClient {
  return new BackupClient(clientConfig(region));
}

export function getELBv2Client(region?: string): ElasticLoadBalancingV2Client {
  return new ElasticLoadBalancingV2Client(clientConfig(region));
}

export function getTransferClient(region?: string): TransferClient {
  return new TransferClient(clientConfig(region));
}

export function getACMClient(region?: string): ACMClient {
  return new ACMClient(clientConfig(region));
}

export function getOrganizationsClient(region?: string): OrganizationsClient {
  return new OrganizationsClient(clientConfig(region));
}

export function getAppSyncClient(region?: string): AppSyncClient {
  return new AppSyncClient(clientConfig(region));
}

export function getCognitoIdentityClient(region?: string): CognitoIdentityClient {
  return new CognitoIdentityClient(clientConfig(region));
}

export function getGuardDutyClient(region?: string): GuardDutyClient {
  return new GuardDutyClient(clientConfig(region));
}

export function getMSKClient(region?: string): KafkaClient {
  return new KafkaClient(clientConfig(region));
}

export function getAPIGatewayClient(region?: string): APIGatewayClient {
  return new APIGatewayClient(clientConfig(region));
}

export function getAPIGatewayV2Client(region?: string): ApiGatewayV2Client {
  return new ApiGatewayV2Client(clientConfig(region));
}

export function getBedrockClient(region?: string): BedrockClient {
  return new BedrockClient(clientConfig(region));
}

export function getSchedulerClient(region?: string): SchedulerClient {
  return new SchedulerClient(clientConfig(region));
}

export function getAppRunnerClient(region?: string): AppRunnerClient {
  return new AppRunnerClient(clientConfig(region));
}

export function getIoTClient(region?: string): IoTClient {
  return new IoTClient(clientConfig(region));
}

export function getSecurityHubClient(region?: string): SecurityHubClient {
  return new SecurityHubClient(clientConfig(region));
}

export function getInspectorClient(region?: string): Inspector2Client {
  return new Inspector2Client(clientConfig(region));
}

export function getXRayClient(region?: string): XRayClient {
  return new XRayClient(clientConfig(region));
}

export function getEC2Client(region?: string): EC2Client {
  return new EC2Client(clientConfig(region));
}

export function getECSClient(region?: string): ECSClient {
  return new ECSClient(clientConfig(region));
}

export function getCognitoIDPClient(region?: string): CognitoIdentityProviderClient {
  return new CognitoIdentityProviderClient(clientConfig(region));
}

import { IAMClient } from "@aws-sdk/client-iam";
import { KMSClient } from "@aws-sdk/client-kms";
import { RDSClient } from "@aws-sdk/client-rds";
import { RDSDataClient } from "@aws-sdk/client-rds-data";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { NeptuneClient } from "@aws-sdk/client-neptune";
import { MqClient } from "@aws-sdk/client-mq";
import { MWAAClient } from "@aws-sdk/client-mwaa";
import { MediaConvertClient } from "@aws-sdk/client-mediaconvert";

export function getIAMClient(region?: string): IAMClient {
  return new IAMClient(clientConfig(region));
}

export function getKMSClient(region?: string): KMSClient {
  return new KMSClient(clientConfig(region));
}

export function getRDSClient(region?: string): RDSClient {
  return new RDSClient(clientConfig(region));
}

export function getRDSDataClient(region?: string): RDSDataClient {
  return new RDSDataClient(clientConfig(region));
}

export function getSecretsManagerClient(region?: string): SecretsManagerClient {
  return new SecretsManagerClient(clientConfig(region));
}

export function getNeptuneClient(region?: string): NeptuneClient {
  return new NeptuneClient(clientConfig(region));
}

export function getMQClient(region?: string): MqClient {
  return new MqClient(clientConfig(region));
}

export function getMWAAClient(region?: string): MWAAClient {
  return new MWAAClient(clientConfig(region));
}

export function getMediaConvertClient(region?: string): MediaConvertClient {
  return new MediaConvertClient(clientConfig(region));
}

import { MemoryDBClient } from "@aws-sdk/client-memorydb";
import { DatabaseMigrationServiceClient } from "@aws-sdk/client-database-migration-service";
import { DocDBClient } from "@aws-sdk/client-docdb";
import { EFSClient } from "@aws-sdk/client-efs";

export function getMemoryDBClient(region?: string): MemoryDBClient {
  return new MemoryDBClient(clientConfig(region));
}

export function getDMSClient(region?: string): DatabaseMigrationServiceClient {
  return new DatabaseMigrationServiceClient(clientConfig(region));
}

export function getDocDBClient(region?: string): DocDBClient {
  return new DocDBClient(clientConfig(region));
}

export function getEFSClient(region?: string): EFSClient {
  return new EFSClient(clientConfig(region));
}

import { SageMakerClient } from "@aws-sdk/client-sagemaker";
import { SageMakerRuntimeClient } from "@aws-sdk/client-sagemaker-runtime";
import { CodeCommitClient } from "@aws-sdk/client-codecommit";
import { CodeartifactClient } from "@aws-sdk/client-codeartifact";
import { RAMClient } from "@aws-sdk/client-ram";
import { VerifiedPermissionsClient } from "@aws-sdk/client-verifiedpermissions";
import { EMRServerlessClient } from "@aws-sdk/client-emr-serverless";
import { TextractClient } from "@aws-sdk/client-textract";
import { TranscribeClient } from "@aws-sdk/client-transcribe";
import { TranslateClient } from "@aws-sdk/client-translate";
import { RekognitionClient } from "@aws-sdk/client-rekognition";
import { ComprehendClient } from "@aws-sdk/client-comprehend";
import { S3TablesClient } from "@aws-sdk/client-s3tables";
import { TimestreamQueryClient } from "@aws-sdk/client-timestream-query";
import { TimestreamWriteClient } from "@aws-sdk/client-timestream-write";
import { ElasticBeanstalkClient } from "@aws-sdk/client-elastic-beanstalk";
import { ServiceDiscoveryClient } from "@aws-sdk/client-servicediscovery";
import { PinpointClient } from "@aws-sdk/client-pinpoint";
import { SupportClient } from "@aws-sdk/client-support";
import { AppStreamClient } from "@aws-sdk/client-appstream";
import { BedrockRuntimeClient } from "@aws-sdk/client-bedrock-runtime";
import { CloudControlClient } from "@aws-sdk/client-cloudcontrol";
import { GrafanaClient } from "@aws-sdk/client-grafana";
import { ResiliencehubClient } from "@aws-sdk/client-resiliencehub";
import { Route53ResolverClient } from "@aws-sdk/client-route53resolver";
import { S3ControlClient } from "@aws-sdk/client-s3-control";
import { FSxClient } from "@aws-sdk/client-fsx";
import { GlacierClient } from "@aws-sdk/client-glacier";
import { PollyClient } from "@aws-sdk/client-polly";
import { STSClient } from "@aws-sdk/client-sts";
import { NetworkManagerClient } from "@aws-sdk/client-networkmanager";
import { OutpostsClient } from "@aws-sdk/client-outposts";
import { MgnClient } from "@aws-sdk/client-mgn";
import { DirectConnectClient } from "@aws-sdk/client-direct-connect";
import { MediaStoreClient } from "@aws-sdk/client-mediastore";
import { MediaStoreDataClient } from "@aws-sdk/client-mediastore-data";
import { ApiGatewayManagementApiClient } from "@aws-sdk/client-apigatewaymanagementapi";
import { CodeConnectionsClient } from "@aws-sdk/client-codeconnections";
import { CodeStarConnectionsClient } from "@aws-sdk/client-codestar-connections";
import { IdentitystoreClient } from "@aws-sdk/client-identitystore";
import { SSOAdminClient } from "@aws-sdk/client-sso-admin";
import { IoTAnalyticsClient } from "@aws-sdk/client-iotanalytics";
import { IoTDataPlaneClient } from "@aws-sdk/client-iot-data-plane";
import { IoTWirelessClient } from "@aws-sdk/client-iot-wireless";
import { ElasticLoadBalancingClient } from "@aws-sdk/client-elastic-load-balancing";
import { ResourceGroupsClient } from "@aws-sdk/client-resource-groups";
import { ResourceGroupsTaggingAPIClient } from "@aws-sdk/client-resource-groups-tagging-api";
import { SWFClient } from "@aws-sdk/client-swf";

export function getSageMakerClient(region?: string): SageMakerClient {
  return new SageMakerClient(clientConfig(region));
}

export function getSageMakerRuntimeClient(region?: string): SageMakerRuntimeClient {
  return new SageMakerRuntimeClient(clientConfig(region));
}

export function getCodeCommitClient(region?: string): CodeCommitClient {
  return new CodeCommitClient(clientConfig(region));
}

export function getCodeArtifactClient(region?: string): CodeartifactClient {
  return new CodeartifactClient(clientConfig(region));
}

export function getRAMClient(region?: string): RAMClient {
  return new RAMClient(clientConfig(region));
}

export function getVerifiedPermissionsClient(region?: string): VerifiedPermissionsClient {
  return new VerifiedPermissionsClient(clientConfig(region));
}

export function getEMRServerlessClient(region?: string): EMRServerlessClient {
  return new EMRServerlessClient(clientConfig(region));
}

export function getTextractClient(region?: string): TextractClient {
  return new TextractClient(clientConfig(region));
}

export function getTranscribeClient(region?: string): TranscribeClient {
  return new TranscribeClient(clientConfig(region));
}

export function getTranslateClient(region?: string): TranslateClient {
  return new TranslateClient(clientConfig(region));
}

export function getRekognitionClient(region?: string): RekognitionClient {
  return new RekognitionClient(clientConfig(region));
}

export function getComprehendClient(region?: string): ComprehendClient {
  return new ComprehendClient(clientConfig(region));
}

export function getS3TablesClient(region?: string): S3TablesClient {
  return new S3TablesClient(clientConfig(region));
}

export function getTimestreamQueryClient(region?: string): TimestreamQueryClient {
  return new TimestreamQueryClient(clientConfig(region));
}

export function getTimestreamWriteClient(region?: string): TimestreamWriteClient {
  return new TimestreamWriteClient(clientConfig(region));
}

export function getElasticBeanstalkClient(region?: string): ElasticBeanstalkClient {
  return new ElasticBeanstalkClient(clientConfig(region));
}

export function getServiceDiscoveryClient(region?: string): ServiceDiscoveryClient {
  return new ServiceDiscoveryClient(clientConfig(region));
}

export function getPinpointClient(region?: string): PinpointClient {
  return new PinpointClient(clientConfig(region));
}

export function getSupportClient(region?: string): SupportClient {
  return new SupportClient(clientConfig(region));
}

export function getAppStreamClient(region?: string): AppStreamClient {
  return new AppStreamClient(clientConfig(region));
}

export function getBedrockRuntimeClient(region?: string): BedrockRuntimeClient {
  return new BedrockRuntimeClient(clientConfig(region));
}

export function getCloudControlClient(region?: string): CloudControlClient {
  return new CloudControlClient(clientConfig(region));
}

export function getGrafanaClient(region?: string): GrafanaClient {
  return new GrafanaClient(clientConfig(region));
}

export function getResiliencehubClient(region?: string): ResiliencehubClient {
  return new ResiliencehubClient(clientConfig(region));
}

export function getRoute53ResolverClient(region?: string): Route53ResolverClient {
  return new Route53ResolverClient(clientConfig(region));
}

export function getS3ControlClient(region?: string): S3ControlClient {
  return new S3ControlClient(clientConfig(region));
}

export function getFSxClient(region?: string): FSxClient {
  return new FSxClient(clientConfig(region));
}

export function getGlacierClient(region?: string): GlacierClient {
  return new GlacierClient(clientConfig(region));
}

export function getPollyClient(region?: string): PollyClient {
  return new PollyClient(clientConfig(region));
}

export function getSTSClient(region?: string): STSClient {
  return new STSClient(clientConfig(region));
}

export function getNetworkManagerClient(region?: string): NetworkManagerClient {
  return new NetworkManagerClient(clientConfig(region));
}

export function getOutpostsClient(region?: string): OutpostsClient {
  return new OutpostsClient(clientConfig(region));
}

export function getMGNClient(region?: string): MgnClient {
  return new MgnClient(clientConfig(region));
}

export function getDirectConnectClient(region?: string): DirectConnectClient {
  return new DirectConnectClient(clientConfig(region));
}

export function getMediaStoreClient(region?: string): MediaStoreClient {
  return new MediaStoreClient(clientConfig(region));
}

export function getMediaStoreDataClient(region?: string): MediaStoreDataClient {
  return new MediaStoreDataClient(clientConfig(region));
}

export function getAPIGatewayManagementAPIClient(region?: string): ApiGatewayManagementApiClient {
  return new ApiGatewayManagementApiClient(clientConfig(region));
}

export function getCodeConnectionsClient(region?: string): CodeConnectionsClient {
  return new CodeConnectionsClient(clientConfig(region));
}

export function getCodeStarConnectionsClient(region?: string): CodeStarConnectionsClient {
  return new CodeStarConnectionsClient(clientConfig(region));
}

export function getIdentityStoreClient(region?: string): IdentitystoreClient {
  return new IdentitystoreClient(clientConfig(region));
}

export function getSSOAdminClient(region?: string): SSOAdminClient {
  return new SSOAdminClient(clientConfig(region));
}

export function getIoTAnalyticsClient(region?: string): IoTAnalyticsClient {
  return new IoTAnalyticsClient(clientConfig(region));
}

export function getIoTDataPlaneClient(region?: string): IoTDataPlaneClient {
  return new IoTDataPlaneClient(clientConfig(region));
}

export function getIoTWirelessClient(region?: string): IoTWirelessClient {
  return new IoTWirelessClient(clientConfig(region));
}

export function getELBClient(region?: string): ElasticLoadBalancingClient {
  return new ElasticLoadBalancingClient(clientConfig(region));
}

export function getResourceGroupsClient(region?: string): ResourceGroupsClient {
  return new ResourceGroupsClient(clientConfig(region));
}

export function getResourceGroupsTaggingAPIClient(region?: string): ResourceGroupsTaggingAPIClient {
  return new ResourceGroupsTaggingAPIClient(clientConfig(region));
}

export function getSWFClient(region?: string): SWFClient {
  return new SWFClient(clientConfig(region));
}

export function getLakeFormationClient(region?: string): LakeFormationClient {
  return new LakeFormationClient(clientConfig(region));
}

export function getApplicationAutoScalingClient(region?: string): ApplicationAutoScalingClient {
  return new ApplicationAutoScalingClient(clientConfig(region));
}

export function getManagedBlockchainClient(region?: string): ManagedBlockchainClient {
  return new ManagedBlockchainClient(clientConfig(region));
}

export function getPipesClient(region?: string): PipesClient {
  return new PipesClient(clientConfig(region));
}

export function getKinesisAnalyticsClient(region?: string): KinesisAnalyticsClient {
  return new KinesisAnalyticsClient(clientConfig(region));
}

export function getKinesisAnalyticsV2Client(region?: string): KinesisAnalyticsV2Client {
  return new KinesisAnalyticsV2Client(clientConfig(region));
}

export function getCostExplorerClient(region?: string): CostExplorerClient {
  return new CostExplorerClient(clientConfig(region));
}

export function getAccessAnalyzerClient(region?: string): AccessAnalyzerClient {
  return new AccessAnalyzerClient(clientConfig(region));
}

export function getAccountClient(region?: string): AccountClient {
  return new AccountClient(clientConfig(region));
}

export function getAppMeshClient(region?: string): AppMeshClient {
  return new AppMeshClient(clientConfig(region));
}

export function getDataBrewClient(region?: string): DataBrewClient {
  return new DataBrewClient(clientConfig(region));
}

export function getDataSyncClient(region?: string): DataSyncClient {
  return new DataSyncClient(clientConfig(region));
}

export function getDAXClient(region?: string): DAXClient {
  return new DAXClient(clientConfig(region));
}

export function getDetectiveClient(region?: string): DetectiveClient {
  return new DetectiveClient(clientConfig(region));
}

export function getDirectoryServiceClient(region?: string): DirectoryServiceClient {
  return new DirectoryServiceClient(clientConfig(region));
}

export function getDLMClient(region?: string): DLMClient {
  return new DLMClient(clientConfig(region));
}

export function getForecastClient(region?: string): ForecastClient {
  return new ForecastClient(clientConfig(region));
}

export function getMacie2Client(region?: string): Macie2Client {
  return new Macie2Client(clientConfig(region));
}

export function getMediaLiveClient(region?: string): MediaLiveClient {
  return new MediaLiveClient(clientConfig(region));
}

export function getMediaPackageClient(region?: string): MediaPackageClient {
  return new MediaPackageClient(clientConfig(region));
}

export function getMediaTailorClient(region?: string): MediaTailorClient {
  return new MediaTailorClient(clientConfig(region));
}

export function getPersonalizeClient(region?: string): PersonalizeClient {
  return new PersonalizeClient(clientConfig(region));
}

export function getQuickSightClient(region?: string): QuickSightClient {
  return new QuickSightClient(clientConfig(region));
}

export function getRolesAnywhereClient(region?: string): RolesAnywhereClient {
  return new RolesAnywhereClient(clientConfig(region));
}

export function getWorkMailClient(region?: string): WorkMailClient {
  return new WorkMailClient(clientConfig(region));
}

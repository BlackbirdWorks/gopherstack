import { AthenaClient } from "@aws-sdk/client-athena";
import { SESClient } from "@aws-sdk/client-ses";
import { CloudTrailClient } from "@aws-sdk/client-cloudtrail";
import { ConfigServiceClient } from "@aws-sdk/client-config-service";
import { BackupClient } from "@aws-sdk/client-backup";
import { ElasticLoadBalancingV2Client } from "@aws-sdk/client-elastic-load-balancing-v2";
import { TransferClient } from "@aws-sdk/client-transfer";
import { ACMClient } from "@aws-sdk/client-acm";
import { OrganizationsClient } from "@aws-sdk/client-organizations";
import { AutoScalingClient } from "@aws-sdk/client-auto-scaling";
import { BatchClient } from "@aws-sdk/client-batch";
import { CloudFormationClient } from "@aws-sdk/client-cloudformation";
import { CloudWatchLogsClient } from "@aws-sdk/client-cloudwatch-logs";
import { ElastiCacheClient } from "@aws-sdk/client-elasticache";
import { FirehoseClient } from "@aws-sdk/client-firehose";
import { GlueClient } from "@aws-sdk/client-glue";
import { OpenSearchClient } from "@aws-sdk/client-opensearch";
import { RedshiftClient } from "@aws-sdk/client-redshift";
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

export function getRedshiftClient(region?: string): RedshiftClient {
  return new RedshiftClient(clientConfig(region));
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

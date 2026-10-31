module github.com/blackbirdworks/gopherstack

go 1.27.3

require (
	cloud.google.com/go/apigateway v1.17.0
	cloud.google.com/go/artifactregistry v1.27.0
	cloud.google.com/go/batch v1.24.1
	cloud.google.com/go/bigquery v1.85.0
	cloud.google.com/go/bigtable v1.58.0
	cloud.google.com/go/cloudbuild v1.34.0
	cloud.google.com/go/cloudtasks v1.20.0
	cloud.google.com/go/compute v1.72.0
	cloud.google.com/go/container v1.56.0
	cloud.google.com/go/datastore v1.25.0
	cloud.google.com/go/eventarc v1.26.0
	cloud.google.com/go/firestore v1.26.0
	cloud.google.com/go/functions v1.26.0
	cloud.google.com/go/iam v1.14.0
	cloud.google.com/go/kms v1.35.0
	cloud.google.com/go/logging v1.20.0
	cloud.google.com/go/longrunning v1.3.0
	cloud.google.com/go/monitoring v1.31.0
	cloud.google.com/go/pubsub/v2 v2.7.0
	cloud.google.com/go/redis v1.26.0
	cloud.google.com/go/run v1.23.0
	cloud.google.com/go/scheduler v1.17.0
	cloud.google.com/go/secretmanager v1.22.0
	cloud.google.com/go/spanner v1.96.1
	cloud.google.com/go/storage v1.69.0
	cloud.google.com/go/workflows v1.21.0
	connectrpc.com/connect v1.21.0
	firebase.google.com/go/v4 v4.22.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.23.3
	github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig/v2 v2.2.0
	github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos v1.5.0
	github.com/Azure/azure-sdk-for-go/sdk/data/aztables v1.4.1
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2 v2.0.2
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.11.0
	github.com/Azure/azure-sdk-for-go/sdk/messaging/eventgrid/azeventgrid v1.0.0
	github.com/Azure/azure-sdk-for-go/sdk/messaging/eventgrid/aznamespaces v1.0.0
	github.com/Azure/azure-sdk-for-go/sdk/monitor/ingestion/azlogs v1.1.0
	github.com/Azure/azure-sdk-for-go/sdk/monitor/query/azlogs v1.2.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2 v2.1.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v4 v4.0.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6 v6.0.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/authorization/armauthorization/v2 v2.2.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9 v9.0.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2 v2.4.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry v1.2.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v9 v9.7.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventgrid/armeventgrid/v2 v2.3.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/keyvault/armkeyvault v1.5.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor v0.12.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/msi/armmsi v1.3.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers/v2 v2.0.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8 v8.0.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/operationalinsights/armoperationalinsights/v2 v2.0.2
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v2 v2.1.1
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/privatedns/armprivatedns v1.3.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/redis/armredis/v3 v3.3.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armlocks v1.2.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armpolicy v1.0.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/sql/armsql v1.2.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage v1.8.1
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azcertificates v1.5.0
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys v1.5.0
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets v1.5.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.8.2
	github.com/Azure/azure-sdk-for-go/sdk/storage/azfile v1.5.4
	github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue v1.0.1
	github.com/alecthomas/kong v1.16.1
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/andybalholm/brotli v1.2.6
	github.com/apache/tinkerpop/gremlin-go/v3 v3.8.2
	github.com/aws/aws-dax-go v1.2.15
	github.com/aws/aws-sdk-go v1.55.8
	github.com/aws/aws-sdk-go-v2 v1.47.3
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.22
	github.com/aws/aws-sdk-go-v2/config v1.33.9
	github.com/aws/aws-sdk-go-v2/credentials v1.20.9
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.3
	github.com/aws/aws-sdk-go-v2/service/accessanalyzer v1.57.3
	github.com/aws/aws-sdk-go-v2/service/account v1.42.2
	github.com/aws/aws-sdk-go-v2/service/acm v1.50.4
	github.com/aws/aws-sdk-go-v2/service/acmpca v1.56.3
	github.com/aws/aws-sdk-go-v2/service/amp v1.54.5
	github.com/aws/aws-sdk-go-v2/service/amplify v1.48.4
	github.com/aws/aws-sdk-go-v2/service/apigateway v1.52.1
	github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi v1.41.0
	github.com/aws/aws-sdk-go-v2/service/apigatewayv2 v1.46.0
	github.com/aws/aws-sdk-go-v2/service/appconfig v1.54.3
	github.com/aws/aws-sdk-go-v2/service/appconfigdata v1.32.3
	github.com/aws/aws-sdk-go-v2/service/appintegrations v1.52.0
	github.com/aws/aws-sdk-go-v2/service/applicationautoscaling v1.51.4
	github.com/aws/aws-sdk-go-v2/service/appmesh v1.44.3
	github.com/aws/aws-sdk-go-v2/service/apprunner v1.48.3
	github.com/aws/aws-sdk-go-v2/service/appstream v1.71.2
	github.com/aws/aws-sdk-go-v2/service/appsync v1.62.4
	github.com/aws/aws-sdk-go-v2/service/arczonalshift v1.34.1
	github.com/aws/aws-sdk-go-v2/service/athena v1.66.3
	github.com/aws/aws-sdk-go-v2/service/autoscaling v1.78.3
	github.com/aws/aws-sdk-go-v2/service/backup v1.69.1
	github.com/aws/aws-sdk-go-v2/service/batch v1.78.2
	github.com/aws/aws-sdk-go-v2/service/bcmdataexports v1.25.4
	github.com/aws/aws-sdk-go-v2/service/bedrock v1.73.4
	github.com/aws/aws-sdk-go-v2/service/bedrockagent v1.68.2
	github.com/aws/aws-sdk-go-v2/service/bedrockagentcore v1.50.3
	github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol v1.73.0
	github.com/aws/aws-sdk-go-v2/service/bedrockruntime v1.63.3
	github.com/aws/aws-sdk-go-v2/service/budgets v1.53.1
	github.com/aws/aws-sdk-go-v2/service/cleanrooms v1.57.4
	github.com/aws/aws-sdk-go-v2/service/cloudcontrol v1.38.3
	github.com/aws/aws-sdk-go-v2/service/cloudformation v1.82.0
	github.com/aws/aws-sdk-go-v2/service/cloudfront v1.74.2
	github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore v1.20.3
	github.com/aws/aws-sdk-go-v2/service/cloudhsmv2 v1.43.4
	github.com/aws/aws-sdk-go-v2/service/cloudtrail v1.65.3
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.73.3
	github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs v1.89.3
	github.com/aws/aws-sdk-go-v2/service/codeartifact v1.46.3
	github.com/aws/aws-sdk-go-v2/service/codebuild v1.78.4
	github.com/aws/aws-sdk-go-v2/service/codecommit v1.43.3
	github.com/aws/aws-sdk-go-v2/service/codeconnections v1.20.0
	github.com/aws/aws-sdk-go-v2/service/codedeploy v1.45.4
	github.com/aws/aws-sdk-go-v2/service/codegurureviewer v1.43.4
	github.com/aws/aws-sdk-go-v2/service/codepipeline v1.55.4
	github.com/aws/aws-sdk-go-v2/service/codestarconnections v1.44.4
	github.com/aws/aws-sdk-go-v2/service/cognitoidentity v1.42.3
	github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider v1.75.3
	github.com/aws/aws-sdk-go-v2/service/comprehend v1.49.3
	github.com/aws/aws-sdk-go-v2/service/configservice v1.74.4
	github.com/aws/aws-sdk-go-v2/service/connect v1.205.0
	github.com/aws/aws-sdk-go-v2/service/costandusagereportservice v1.43.4
	github.com/aws/aws-sdk-go-v2/service/costexplorer v1.74.0
	github.com/aws/aws-sdk-go-v2/service/databasemigrationservice v1.75.0
	github.com/aws/aws-sdk-go-v2/service/databrew v1.47.3
	github.com/aws/aws-sdk-go-v2/service/datasync v1.67.4
	github.com/aws/aws-sdk-go-v2/service/dax v1.38.6
	github.com/aws/aws-sdk-go-v2/service/detective v1.47.3
	github.com/aws/aws-sdk-go-v2/service/directconnect v1.55.0
	github.com/aws/aws-sdk-go-v2/service/directoryservice v1.47.3
	github.com/aws/aws-sdk-go-v2/service/dlm v1.45.3
	github.com/aws/aws-sdk-go-v2/service/docdb v1.57.3
	github.com/aws/aws-sdk-go-v2/service/dsql v1.22.3
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.70.3
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.45.1
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.338.4
	github.com/aws/aws-sdk-go-v2/service/ecr v1.66.4
	github.com/aws/aws-sdk-go-v2/service/ecrpublic v1.47.3
	github.com/aws/aws-sdk-go-v2/service/ecs v1.100.3
	github.com/aws/aws-sdk-go-v2/service/efs v1.49.4
	github.com/aws/aws-sdk-go-v2/service/eks v1.104.1
	github.com/aws/aws-sdk-go-v2/service/eksauth v1.21.4
	github.com/aws/aws-sdk-go-v2/service/elasticache v1.63.2
	github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk v1.43.3
	github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing v1.41.3
	github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2 v1.63.4
	github.com/aws/aws-sdk-go-v2/service/elasticsearchservice v1.51.3
	github.com/aws/aws-sdk-go-v2/service/emr v1.70.3
	github.com/aws/aws-sdk-go-v2/service/emrserverless v1.50.0
	github.com/aws/aws-sdk-go-v2/service/eventbridge v1.55.3
	github.com/aws/aws-sdk-go-v2/service/firehose v1.52.3
	github.com/aws/aws-sdk-go-v2/service/fis v1.46.3
	github.com/aws/aws-sdk-go-v2/service/forecast v1.50.3
	github.com/aws/aws-sdk-go-v2/service/fsx v1.74.3
	github.com/aws/aws-sdk-go-v2/service/glacier v1.41.3
	github.com/aws/aws-sdk-go-v2/service/globalaccelerator v1.45.3
	github.com/aws/aws-sdk-go-v2/service/glue v1.170.0
	github.com/aws/aws-sdk-go-v2/service/grafana v1.44.3
	github.com/aws/aws-sdk-go-v2/service/guardduty v1.99.0
	github.com/aws/aws-sdk-go-v2/service/iam v1.64.4
	github.com/aws/aws-sdk-go-v2/service/identitystore v1.49.0
	github.com/aws/aws-sdk-go-v2/service/inspector2 v1.61.2
	github.com/aws/aws-sdk-go-v2/service/iot v1.84.4
	github.com/aws/aws-sdk-go-v2/service/iotanalytics v1.32.0
	github.com/aws/aws-sdk-go-v2/service/iotdataplane v1.40.3
	github.com/aws/aws-sdk-go-v2/service/iotjobsdataplane v1.38.4
	github.com/aws/aws-sdk-go-v2/service/iotwireless v1.65.3
	github.com/aws/aws-sdk-go-v2/service/kafka v1.65.3
	github.com/aws/aws-sdk-go-v2/service/kafkaconnect v1.39.3
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.56.4
	github.com/aws/aws-sdk-go-v2/service/kinesisanalytics v1.39.3
	github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2 v1.47.3
	github.com/aws/aws-sdk-go-v2/service/kinesisvideo v1.41.3
	github.com/aws/aws-sdk-go-v2/service/kms v1.61.4
	github.com/aws/aws-sdk-go-v2/service/lakeformation v1.55.3
	github.com/aws/aws-sdk-go-v2/service/lambda v1.112.2
	github.com/aws/aws-sdk-go-v2/service/lightsail v1.66.4
	github.com/aws/aws-sdk-go-v2/service/macie2 v1.59.3
	github.com/aws/aws-sdk-go-v2/service/managedblockchain v1.40.3
	github.com/aws/aws-sdk-go-v2/service/marketplaceentitlementservice v1.46.4
	github.com/aws/aws-sdk-go-v2/service/marketplacemetering v1.46.2
	github.com/aws/aws-sdk-go-v2/service/mediaconvert v1.106.3
	github.com/aws/aws-sdk-go-v2/service/medialive v1.112.0
	github.com/aws/aws-sdk-go-v2/service/mediapackage v1.48.3
	github.com/aws/aws-sdk-go-v2/service/mediastore v1.38.3
	github.com/aws/aws-sdk-go-v2/service/mediastoredata v1.38.3
	github.com/aws/aws-sdk-go-v2/service/mediatailor v1.72.2
	github.com/aws/aws-sdk-go-v2/service/memorydb v1.42.3
	github.com/aws/aws-sdk-go-v2/service/mgn v1.56.3
	github.com/aws/aws-sdk-go-v2/service/mq v1.45.3
	github.com/aws/aws-sdk-go-v2/service/mwaa v1.48.3
	github.com/aws/aws-sdk-go-v2/service/neptune v1.53.3
	github.com/aws/aws-sdk-go-v2/service/networkfirewall v1.76.1
	github.com/aws/aws-sdk-go-v2/service/networkmanager v1.50.3
	github.com/aws/aws-sdk-go-v2/service/networkmonitor v1.21.3
	github.com/aws/aws-sdk-go-v2/service/oam v1.31.4
	github.com/aws/aws-sdk-go-v2/service/omics v1.55.3
	github.com/aws/aws-sdk-go-v2/service/opensearch v1.85.0
	github.com/aws/aws-sdk-go-v2/service/opensearchserverless v1.40.3
	github.com/aws/aws-sdk-go-v2/service/opsworks v1.31.0
	github.com/aws/aws-sdk-go-v2/service/organizations v1.61.2
	github.com/aws/aws-sdk-go-v2/service/outposts v1.74.3
	github.com/aws/aws-sdk-go-v2/service/personalize v1.56.3
	github.com/aws/aws-sdk-go-v2/service/personalizeruntime v1.42.3
	github.com/aws/aws-sdk-go-v2/service/pinpoint v1.48.3
	github.com/aws/aws-sdk-go-v2/service/pipes v1.32.3
	github.com/aws/aws-sdk-go-v2/service/polly v1.65.4
	github.com/aws/aws-sdk-go-v2/service/pricing v1.49.4
	github.com/aws/aws-sdk-go-v2/service/quicksight v1.137.1
	github.com/aws/aws-sdk-go-v2/service/ram v1.45.3
	github.com/aws/aws-sdk-go-v2/service/rds v1.130.2
	github.com/aws/aws-sdk-go-v2/service/rdsdata v1.40.3
	github.com/aws/aws-sdk-go-v2/service/redshift v1.71.3
	github.com/aws/aws-sdk-go-v2/service/redshiftdata v1.49.2
	github.com/aws/aws-sdk-go-v2/service/redshiftserverless v1.44.3
	github.com/aws/aws-sdk-go-v2/service/rekognition v1.60.3
	github.com/aws/aws-sdk-go-v2/service/resiliencehub v1.44.3
	github.com/aws/aws-sdk-go-v2/service/resourceexplorer2 v1.33.4
	github.com/aws/aws-sdk-go-v2/service/resourcegroups v1.42.3
	github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi v1.41.3
	github.com/aws/aws-sdk-go-v2/service/rolesanywhere v1.31.3
	github.com/aws/aws-sdk-go-v2/service/route53 v1.70.4
	github.com/aws/aws-sdk-go-v2/service/route53recoverycluster v1.36.4
	github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig v1.40.4
	github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness v1.34.4
	github.com/aws/aws-sdk-go-v2/service/route53resolver v1.54.4
	github.com/aws/aws-sdk-go-v2/service/rum v1.38.4
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.3
	github.com/aws/aws-sdk-go-v2/service/s3control v1.79.3
	github.com/aws/aws-sdk-go-v2/service/s3files v1.8.3
	github.com/aws/aws-sdk-go-v2/service/s3tables v1.23.3
	github.com/aws/aws-sdk-go-v2/service/s3vectors v1.16.3
	github.com/aws/aws-sdk-go-v2/service/sagemaker v1.282.2
	github.com/aws/aws-sdk-go-v2/service/sagemakerruntime v1.49.3
	github.com/aws/aws-sdk-go-v2/service/scheduler v1.25.3
	github.com/aws/aws-sdk-go-v2/service/schemas v1.43.3
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.50.4
	github.com/aws/aws-sdk-go-v2/service/securityhub v1.84.1
	github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository v1.39.3
	github.com/aws/aws-sdk-go-v2/service/servicecatalog v1.47.4
	github.com/aws/aws-sdk-go-v2/service/servicediscovery v1.49.3
	github.com/aws/aws-sdk-go-v2/service/servicequotas v1.43.4
	github.com/aws/aws-sdk-go-v2/service/ses v1.42.3
	github.com/aws/aws-sdk-go-v2/service/sesv2 v1.79.0
	github.com/aws/aws-sdk-go-v2/service/sfn v1.51.4
	github.com/aws/aws-sdk-go-v2/service/shield v1.43.3
	github.com/aws/aws-sdk-go-v2/service/signin v1.10.4
	github.com/aws/aws-sdk-go-v2/service/sns v1.47.5
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.4
	github.com/aws/aws-sdk-go-v2/service/ssm v1.79.3
	github.com/aws/aws-sdk-go-v2/service/sso v1.38.4
	github.com/aws/aws-sdk-go-v2/service/ssoadmin v1.49.3
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.43.4
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.4
	github.com/aws/aws-sdk-go-v2/service/support v1.40.3
	github.com/aws/aws-sdk-go-v2/service/swf v1.43.3
	github.com/aws/aws-sdk-go-v2/service/textract v1.49.3
	github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb v1.29.4
	github.com/aws/aws-sdk-go-v2/service/timestreamquery v1.44.3
	github.com/aws/aws-sdk-go-v2/service/timestreamwrite v1.43.3
	github.com/aws/aws-sdk-go-v2/service/transcribe v1.66.4
	github.com/aws/aws-sdk-go-v2/service/transfer v1.85.2
	github.com/aws/aws-sdk-go-v2/service/translate v1.43.0
	github.com/aws/aws-sdk-go-v2/service/verifiedpermissions v1.41.3
	github.com/aws/aws-sdk-go-v2/service/vpclattice v1.33.3
	github.com/aws/aws-sdk-go-v2/service/waf v1.38.3
	github.com/aws/aws-sdk-go-v2/service/wafv2 v1.83.3
	github.com/aws/aws-sdk-go-v2/service/workmail v1.45.3
	github.com/aws/aws-sdk-go-v2/service/workspaces v1.81.4
	github.com/aws/aws-sdk-go-v2/service/xray v1.45.3
	github.com/aws/smithy-go v1.28.5
	github.com/blevesearch/snowballstem v0.9.0
	github.com/cedar-policy/cedar-go v1.8.0
	github.com/containerd/errdefs v1.0.0
	github.com/distribution/distribution/v3 v3.1.2
	github.com/dop251/goja v0.0.0-20261012212121-0c308c5ed11b
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/go-sql-driver/mysql v1.10.1
	github.com/go-stomp/stomp/v3 v3.1.5
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/googleapis/gax-go/v2 v2.27.0
	github.com/gorilla/websocket v1.5.3
	github.com/iskorotkov/avro/v2 v2.35.0
	github.com/jackc/pgx/v5 v5.11.0
	github.com/klauspost/compress v1.20.3
	github.com/labstack/echo/v5 v5.4.1
	github.com/microsoft/go-mssqldb v1.11.2
	github.com/miekg/dns v1.1.73
	github.com/moby/moby/api v1.56.1
	github.com/moby/moby/client v0.6.2
	github.com/mochi-mqtt/server/v2 v2.7.9
	github.com/modelcontextprotocol/go-sdk v1.8.0
	github.com/mxschmitt/playwright-go v0.6201.1
	github.com/opencontainers/image-spec v1.1.1
	github.com/oracle/oci-go-sdk/v65 v65.126.2
	github.com/parquet-go/parquet-go v0.32.0
	github.com/pkg/sftp v1.13.11
	github.com/prometheus/client_golang v1.25.0
	github.com/prometheus/client_model v0.6.3
	github.com/rabbitmq/amqp091-go v1.15.0
	github.com/recolabs/gnata v0.6.1
	github.com/redis/go-redis/v9 v9.23.0
	github.com/stretchr/testify v1.12.1
	github.com/testcontainers/testcontainers-go v0.44.0
	github.com/twmb/franz-go v1.22.1
	github.com/twmb/franz-go/pkg/kmsg v1.15.0
	github.com/vektah/gqlparser/v2 v2.5.62
	go.mongodb.org/mongo-driver/v2 v2.9.2
	go.uber.org/goleak v1.3.0
	golang.org/x/crypto v0.58.0
	golang.org/x/mod v0.42.0
	golang.org/x/net v0.62.0
	golang.org/x/sync v0.24.0
	golang.org/x/text v0.43.0
	google.golang.org/api v0.304.0
	google.golang.org/genproto v0.0.0-20261012220534-4341edf3f941
	google.golang.org/genproto/googleapis/api v0.0.0-20261012220534-4341edf3f941
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261012220534-4341edf3f941
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
	modernc.org/sqlite v1.60.1
	software.sslmate.com/src/go-pkcs12 v0.7.4
)

require (
	cel.dev/expr v0.25.2 // indirect
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.24.1 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.3.0 // indirect
	cloud.google.com/go/compute/metadata v0.10.0 // indirect
	dario.cat/mergo v1.0.2 // indirect
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.13.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/internal v1.2.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/internal v0.1.0 // indirect
	github.com/Azure/go-amqp v1.7.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20261008234906-119c4d577770 // indirect
	github.com/GoogleCloudPlatform/grpc-gcp-go/grpcgcp v1.6.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.35.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.57.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.57.0 // indirect
	github.com/MicahParks/keyfunc v1.9.0 // indirect
	github.com/Microsoft/go-winio v0.6.3 // indirect
	github.com/agnivade/levenshtein v1.2.1 // indirect
	github.com/antlr/antlr4 v0.0.0-20181218183524-be58ebffde8e // indirect
	github.com/apache/arrow-go/v18 v18.7.0 // indirect
	github.com/apache/arrow/go/v15 v15.0.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.13.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.6 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitfield/gotestdox v0.2.2 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/log v0.2.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/cpuguy83/dockercfg v0.3.2 // indirect
	github.com/deckarep/golang-set/v2 v2.8.0 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/dlclark/regexp2/v2 v2.8.4 // indirect
	github.com/dnephin/pflag v1.0.7 // indirect
	github.com/docker/docker-credential-helpers v0.9.10 // indirect
	github.com/docker/go-connections v0.8.2 // indirect
	github.com/docker/go-events v0.1.0 // indirect
	github.com/docker/go-metrics v0.1.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.1.0 // indirect
	github.com/ebitengine/purego v0.11.1 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.37.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-sourcemap/sourcemap v2.1.4+incompatible // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/gofrs/flock v0.13.1 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/google/pprof v0.0.0-20261012231050-2af12c3f4d87 // indirect
	github.com/google/s2a-go v0.1.11 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.23 // indirect
	github.com/gorilla/handlers v1.5.2 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.31.0 // indirect
	github.com/hashicorp/golang-lru/arc/v2 v2.0.7 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20260802145828-341c2f0c90b5 // indirect
	github.com/magiconair/properties v1.18.12 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/go-archive v0.3.3 // indirect
	github.com/moby/patternmatcher v0.6.1 // indirect
	github.com/moby/sys/sequential v0.7.0 // indirect
	github.com/moby/sys/user v0.4.1 // indirect
	github.com/moby/sys/userns v0.2.1 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/ncruces/go-strftime v1.1.0 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.5.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/palkan/mulint v1.1.0 // indirect
	github.com/parquet-go/bitpack v1.1.0 // indirect
	github.com/parquet-go/jsonlite v1.5.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/power-devops/perfstat v0.0.0-20260916203055-22a1a467d9f0 // indirect
	github.com/prometheus/common v0.72.1 // indirect
	github.com/prometheus/otlptranslator v1.0.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/redis/go-redis/extra/rediscmd/v9 v9.23.0 // indirect
	github.com/redis/go-redis/extra/redisotel/v9 v9.23.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/shirou/gopsutil/v4 v4.26.9 // indirect
	github.com/shopspring/decimal v1.5.0 // indirect
	github.com/sirupsen/logrus v1.10.2 // indirect
	github.com/sony/gobreaker/v2 v2.4.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.8.1 // indirect
	github.com/tidwall/gjson v1.20.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.2 // indirect
	github.com/tklauser/go-sysconf v0.4.0 // indirect
	github.com/tklauser/numcpus v0.12.0 // indirect
	github.com/twpayne/go-geom v1.7.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.2.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	github.com/yuin/gopher-lua v1.1.2 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/bridges/prometheus v0.72.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.45.0 // indirect
	go.opentelemetry.io/contrib/exporters/autoexport v0.72.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.71.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.72.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.23.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.23.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.69.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.23.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/log v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.opentelemetry.io/proto/otlp v1.11.1 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/exp v0.0.0-20261011100332-faecfb24eb49 // indirect
	golang.org/x/oauth2 v0.37.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/telemetry v0.0.0-20261009151841-97e8ea77aa2a // indirect
	golang.org/x/term v0.47.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	golang.org/x/tools v0.52.0 // indirect
	golang.org/x/vuln v1.7.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/appengine/v2 v2.0.6 // indirect
	gotest.tools/gotestsum v1.13.0 // indirect
	modernc.org/libc v1.78.0 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
)

tool (
	github.com/palkan/mulint/cmd/mulint-vet
	golang.org/x/vuln/cmd/govulncheck
	gotest.tools/gotestsum
)

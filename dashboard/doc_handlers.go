package dashboard

import "net/http"

// docPageData is the template data for the documentation page.
type docPageData struct {
	PageData

	DynamoDBOps                []string
	S3Ops                      []string
	SSMOps                     []string
	SQSOps                     []string
	SNSOps                     []string
	IAMOps                     []string
	STSOps                     []string
	KMSOps                     []string
	SecretsManagerOps          []string
	LambdaOps                  []string
	EventBridgeOps             []string
	APIGatewayOps              []string
	CloudWatchLogsOps          []string
	StepFunctionsOps           []string
	CloudWatchOps              []string
	CloudFormationOps          []string
	KinesisOps                 []string
	ElastiCacheOps             []string
	Route53Ops                 []string
	SESOps                     []string
	SESv2Ops                   []string
	EC2Ops                     []string
	OpenSearchOps              []string
	ACMOps                     []string
	ACMPCAOps                  []string
	RedshiftOps                []string
	RDSOps                     []string
	AWSConfigOps               []string
	S3ControlOps               []string
	ResourceGroupsOps          []string
	SWFOps                     []string
	FirehoseOps                []string
	SchedulerOps               []string
	Route53ResolverOps         []string
	TranscribeOps              []string
	SupportOps                 []string
	CognitoIdentityOps         []string
	IoTOps                     []string
	AppSyncOps                 []string
	IoTDataPlaneOps            []string
	ResourceGroupsTaggingOps   []string
	APIGatewayManagementAPIOps []string
	APIGatewayV2Ops            []string
	AppConfigDataOps           []string
	AmplifyOps                 []string
	AppConfigOps               []string
	AthenaOps                  []string
	AutoscalingOps             []string
	ApplicationAutoscalingOps  []string
	BackupOps                  []string
	BatchOps                   []string
	BedrockOps                 []string
	BedrockRuntimeOps          []string
	CognitoIDPOps              []string
	CloudFrontOps              []string
	CodeArtifactOps            []string
	CodeBuildOps               []string
	DMSOps                     []string
	CodeStarConnectionsOps     []string
	DynamoDBStreamsOps         []string
	PipesOps                   []string
	QLDBOps                    []string
	QLDBSessionOps             []string
	RDSDataOps                 []string
	RedshiftDataOps            []string
	SageMakerOps               []string
	SageMakerRuntimeOps        []string
	SsoAdminOps                []string
	TextractOps                []string
	TimestreamQueryOps         []string
	TransferOps                []string
	Wafv2Ops                   []string
	XrayOps                    []string
	S3TablesOps                []string
}

// docIndex renders the documentation page.
func (h *DashboardHandler) docIndex(w http.ResponseWriter, _ *http.Request) {
	data := docPageData{
		PageData: PageData{
			Title:     "API Documentation",
			ActiveTab: "docs",
			Snippet: &SnippetData{
				ID:    "docs-operations",
				Title: "Using Docs",
				Cli:   "aws docs help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Docs */",
				Python: "# Write boto3 code for Docs\nimport boto3\n" +
					"client = boto3.client('docs', endpoint_url='http://localhost:8000')",
			},
		},
		DynamoDBOps:                h.getSupportedOps(h.DDBOps),
		S3Ops:                      h.getSupportedOps(h.S3Ops),
		SSMOps:                     h.getSupportedOps(h.SSMOps),
		SQSOps:                     h.getSupportedOps(h.SQSOps),
		SNSOps:                     h.getSupportedOps(h.SNSOps),
		IAMOps:                     h.getSupportedOps(h.IAMOps),
		STSOps:                     h.getSupportedOps(h.STSOps),
		KMSOps:                     h.getSupportedOps(h.KMSOps),
		SecretsManagerOps:          h.getSupportedOps(h.SecretsManagerOps),
		LambdaOps:                  h.getSupportedOps(h.LambdaOps),
		EventBridgeOps:             h.getSupportedOps(h.EventBridgeOps),
		APIGatewayOps:              h.getSupportedOps(h.APIGatewayOps),
		CloudWatchLogsOps:          h.getSupportedOps(h.CloudWatchLogsOps),
		StepFunctionsOps:           h.getSupportedOps(h.StepFunctionsOps),
		CloudWatchOps:              h.getSupportedOps(h.CloudWatchOps),
		CloudFormationOps:          h.getSupportedOps(h.CloudFormationOps),
		KinesisOps:                 h.getSupportedOps(h.KinesisOps),
		ElastiCacheOps:             h.getSupportedOps(h.ElastiCacheOps),
		Route53Ops:                 h.getSupportedOps(h.Route53Ops),
		SESOps:                     h.getSupportedOps(h.SESOps),
		SESv2Ops:                   h.getSupportedOps(h.SESv2Ops),
		EC2Ops:                     h.getSupportedOps(h.EC2Ops),
		OpenSearchOps:              h.getSupportedOps(h.OpenSearchOps),
		ACMOps:                     h.getSupportedOps(h.ACMOps),
		ACMPCAOps:                  h.getSupportedOps(h.ACMPCAOps),
		RedshiftOps:                h.getSupportedOps(h.RedshiftOps),
		RDSOps:                     h.getSupportedOps(h.RDSOps),
		AWSConfigOps:               h.getSupportedOps(h.AWSConfigOps),
		S3ControlOps:               h.getSupportedOps(h.S3ControlOps),
		ResourceGroupsOps:          h.getSupportedOps(h.ResourceGroupsOps),
		SWFOps:                     h.getSupportedOps(h.SWFOps),
		FirehoseOps:                h.getSupportedOps(h.FirehoseOps),
		SchedulerOps:               h.getSupportedOps(h.SchedulerOps),
		Route53ResolverOps:         h.getSupportedOps(h.Route53ResolverOps),
		TranscribeOps:              h.getSupportedOps(h.TranscribeOps),
		SupportOps:                 h.getSupportedOps(h.SupportOps),
		CognitoIdentityOps:         h.getSupportedOps(h.CognitoIdentityOps),
		IoTOps:                     h.getSupportedOps(h.IoTOps),
		AppSyncOps:                 h.getSupportedOps(h.AppSyncOps),
		IoTDataPlaneOps:            h.getSupportedOps(h.IoTDataPlaneOps),
		ResourceGroupsTaggingOps:   h.getSupportedOps(h.ResourceGroupsTaggingOps),
		APIGatewayManagementAPIOps: h.getSupportedOps(h.APIGatewayManagementAPIOps),
		APIGatewayV2Ops:            h.getSupportedOps(h.APIGatewayV2Ops),
		AppConfigDataOps:           h.getSupportedOps(h.AppConfigDataOps),
		AmplifyOps:                 h.getSupportedOps(h.AmplifyOps),
		AppConfigOps:               h.getSupportedOps(h.AppConfigOps),
		AthenaOps:                  h.getSupportedOps(h.AthenaOps),
		AutoscalingOps:             h.getSupportedOps(h.AutoscalingOps),
		ApplicationAutoscalingOps:  h.getSupportedOps(h.ApplicationAutoscalingOps),
		BackupOps:                  h.getSupportedOps(h.BackupOps),
		BatchOps:                   h.getSupportedOps(h.BatchOps),
		BedrockOps:                 h.getSupportedOps(h.BedrockOps),
		BedrockRuntimeOps:          h.getSupportedOps(h.BedrockRuntimeOps),
		CognitoIDPOps:              h.getSupportedOps(h.CognitoIDPOps),
		CloudFrontOps:              h.getSupportedOps(h.CloudFrontOps),
		CodeArtifactOps:            h.getSupportedOps(h.CodeArtifactOps),
		CodeBuildOps:               h.getSupportedOps(h.CodeBuildOps),
		DMSOps:                     h.getSupportedOps(h.DMSOps),
		CodeStarConnectionsOps:     h.getSupportedOps(h.CodeStarConnectionsOps),
		DynamoDBStreamsOps:         h.getSupportedOps(h.DynamoDBStreamsOps),
		PipesOps:                   h.getSupportedOps(h.PipesOps),
		QLDBOps:                    h.getSupportedOps(h.QLDBOps),
		QLDBSessionOps:             h.getSupportedOps(h.QLDBSessionOps),
		RDSDataOps:                 h.getSupportedOps(h.RDSDataOps),
		RedshiftDataOps:            h.getSupportedOps(h.RedshiftDataOps),
		SageMakerOps:               h.getSupportedOps(h.SageMakerOps),
		SageMakerRuntimeOps:        h.getSupportedOps(h.SageMakerRuntimeOps),
		SsoAdminOps:                h.getSupportedOps(h.SsoAdminOps),
		TextractOps:                h.getSupportedOps(h.TextractOps),
		TimestreamQueryOps:         h.getSupportedOps(h.TimestreamQueryOps),
		TransferOps:                h.getSupportedOps(h.TransferOps),
		Wafv2Ops:                   h.getSupportedOps(h.Wafv2Ops),
		XrayOps:                    h.getSupportedOps(h.XrayOps),
		S3TablesOps:                h.getSupportedOps(h.S3TablesOps),
	}

	h.renderTemplate(w, "doc.html", data)
}

// getSupportedOps returns the list of supported operations for a provider.
func (h *DashboardHandler) getSupportedOps(p OperationsProvider) []string {
	if p == nil {
		return nil
	}

	return p.GetSupportedOperations()
}

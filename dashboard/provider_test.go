package dashboard_test

import (
	"log/slog"
	"testing"

	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	globalcfg "github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

// mockAWSProvider implements dashboard.AWSSDKProvider for provider tests.
type mockAWSProvider struct {
	ddbClient  *ddbsdk.Client
	s3Client   *s3sdk.Client
	ssmClient  *ssmsdk.Client
	stsClient  *stssdk.Client
	ddbHandler service.Registerable
	s3Handler  service.Registerable
	ssmHandler service.Registerable
	stsHandler service.Registerable
}

func (m *mockAWSProvider) GetDynamoDBClient() *ddbsdk.Client        { return m.ddbClient }
func (m *mockAWSProvider) GetS3Client() *s3sdk.Client               { return m.s3Client }
func (m *mockAWSProvider) GetSSMClient() *ssmsdk.Client             { return m.ssmClient }
func (m *mockAWSProvider) GetSTSClient() *stssdk.Client             { return m.stsClient }
func (m *mockAWSProvider) GetDynamoDBHandler() service.Registerable { return m.ddbHandler }
func (m *mockAWSProvider) GetS3Handler() service.Registerable       { return m.s3Handler }
func (m *mockAWSProvider) GetSSMHandler() service.Registerable      { return m.ssmHandler }
func (m *mockAWSProvider) GetSTSHandler() service.Registerable      { return m.stsHandler }

// Stub implementations for the remaining AWSSDKProvider methods.
func (m *mockAWSProvider) GetIAMHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetSNSHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetSQSHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetKMSHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetSecretsManagerHandler() service.Registerable          { return nil }
func (m *mockAWSProvider) GetLambdaHandler() service.Registerable                  { return nil }
func (m *mockAWSProvider) GetEventBridgeHandler() service.Registerable             { return nil }
func (m *mockAWSProvider) GetAPIGatewayHandler() service.Registerable              { return nil }
func (m *mockAWSProvider) GetCloudWatchLogsHandler() service.Registerable          { return nil }
func (m *mockAWSProvider) GetStepFunctionsHandler() service.Registerable           { return nil }
func (m *mockAWSProvider) GetCloudWatchHandler() service.Registerable              { return nil }
func (m *mockAWSProvider) GetCloudFormationHandler() service.Registerable          { return nil }
func (m *mockAWSProvider) GetKinesisHandler() service.Registerable                 { return nil }
func (m *mockAWSProvider) GetElastiCacheHandler() service.Registerable             { return nil }
func (m *mockAWSProvider) GetRoute53Handler() service.Registerable                 { return nil }
func (m *mockAWSProvider) GetSESHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetSESv2Handler() service.Registerable                   { return nil }
func (m *mockAWSProvider) GetEC2Handler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetOpenSearchHandler() service.Registerable              { return nil }
func (m *mockAWSProvider) GetACMHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetACMPCAHandler() service.Registerable                  { return nil }
func (m *mockAWSProvider) GetRedshiftHandler() service.Registerable                { return nil }
func (m *mockAWSProvider) GetRDSHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetAWSConfigHandler() service.Registerable               { return nil }
func (m *mockAWSProvider) GetS3ControlHandler() service.Registerable               { return nil }
func (m *mockAWSProvider) GetResourceGroupsHandler() service.Registerable          { return nil }
func (m *mockAWSProvider) GetResourceGroupsTaggingHandler() service.Registerable   { return nil }
func (m *mockAWSProvider) GetSWFHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetFirehoseHandler() service.Registerable                { return nil }
func (m *mockAWSProvider) GetCognitoIdentityHandler() service.Registerable         { return nil }
func (m *mockAWSProvider) GetAppSyncHandler() service.Registerable                 { return nil }
func (m *mockAWSProvider) GetCognitoIDPHandler() service.Registerable              { return nil }
func (m *mockAWSProvider) GetIoTDataPlaneHandler() service.Registerable            { return nil }
func (m *mockAWSProvider) GetAmplifyHandler() service.Registerable                 { return nil }
func (m *mockAWSProvider) GetAPIGatewayV2Handler() service.Registerable            { return nil }
func (m *mockAWSProvider) GetAthenaHandler() service.Registerable                  { return nil }
func (m *mockAWSProvider) GetAutoscalingHandler() service.Registerable             { return nil }
func (m *mockAWSProvider) GetApplicationAutoscalingHandler() service.Registerable  { return nil }
func (m *mockAWSProvider) GetAppConfigHandler() service.Registerable               { return nil }
func (m *mockAWSProvider) GetECRHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetECSHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetEFSHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetEKSHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetELBHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetELBv2Handler() service.Registerable                   { return nil }
func (m *mockAWSProvider) GetIoTHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetFISHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetAPIGatewayManagementAPIHandler() service.Registerable { return nil }
func (m *mockAWSProvider) GetAppConfigDataHandler() service.Registerable           { return nil }
func (m *mockAWSProvider) GetBackupHandler() service.Registerable                  { return nil }
func (m *mockAWSProvider) GetCloudTrailHandler() service.Registerable              { return nil }
func (m *mockAWSProvider) GetBatchHandler() service.Registerable                   { return nil }
func (m *mockAWSProvider) GetBedrockHandler() service.Registerable                 { return nil }
func (m *mockAWSProvider) GetBedrockRuntimeHandler() service.Registerable          { return nil }
func (m *mockAWSProvider) GetCeHandler() service.Registerable                      { return nil }
func (m *mockAWSProvider) GetCloudControlHandler() service.Registerable            { return nil }
func (m *mockAWSProvider) GetCloudFrontHandler() service.Registerable              { return nil }
func (m *mockAWSProvider) GetCodeArtifactHandler() service.Registerable            { return nil }
func (m *mockAWSProvider) GetCodeBuildHandler() service.Registerable               { return nil }
func (m *mockAWSProvider) GetCodeCommitHandler() service.Registerable              { return nil }
func (m *mockAWSProvider) GetCodePipelineHandler() service.Registerable            { return nil }
func (m *mockAWSProvider) GetCodeConnectionsHandler() service.Registerable         { return nil }
func (m *mockAWSProvider) GetCodeDeployHandler() service.Registerable              { return nil }
func (m *mockAWSProvider) GetDMSHandler() service.Registerable                     { return nil }
func (m *mockAWSProvider) GetCodeStarConnectionsHandler() service.Registerable     { return nil }
func (m *mockAWSProvider) GetDynamoDBStreamsHandler() service.Registerable         { return nil }
func (m *mockAWSProvider) GetDocDBHandler() service.Registerable                   { return nil }
func (m *mockAWSProvider) GetElasticTranscoderHandler() service.Registerable       { return nil }
func (m *mockAWSProvider) GetGlobalConfig() globalcfg.GlobalConfig                 { return globalcfg.GlobalConfig{} }
func (m *mockAWSProvider) GetFaultStore() *chaos.FaultStore                        { return nil }

func TestDashboardProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{
			name:     "returns dashboard name",
			wantName: "Dashboard",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &dashboard.Provider{}
			assert.Equal(t, tt.wantName, p.Name())
		})
	}
}

func TestDashboardProvider_Init(t *testing.T) {
	t.Parallel()

	ddbBk := ddbbackend.NewInMemoryDB()
	ddbHndlr := ddbbackend.NewHandler(ddbBk)
	s3Bk := s3backend.NewInMemoryBackend(nil)
	s3Hndlr := s3backend.NewHandler(s3Bk)
	ssmBk := ssmbackend.NewInMemoryBackend()
	ssmHndlr := ssmbackend.NewHandler(ssmBk)
	stsBk := stsbackend.NewInMemoryBackend()
	stsHndlr := stsbackend.NewHandler(stsBk)

	tests := []struct {
		name     string
		config   any
		wantName string
	}{
		{
			name:     "nil config produces valid registerable",
			config:   nil,
			wantName: "Dashboard",
		},
		{
			name: "with sts handler covers stsOps code path",
			config: &mockAWSProvider{
				ddbHandler: ddbHndlr,
				s3Handler:  s3Hndlr,
				ssmHandler: ssmHndlr,
				stsHandler: stsHndlr,
			},
			wantName: "Dashboard",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &dashboard.Provider{}
			appCtx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}

			reg, err := p.Init(appCtx)
			require.NoError(t, err)
			assert.NotNil(t, reg)
			assert.Equal(t, tt.wantName, reg.Name())
		})
	}
}

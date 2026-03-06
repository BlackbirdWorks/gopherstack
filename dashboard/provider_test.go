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

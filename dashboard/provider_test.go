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
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/sts"
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

// TestDashboardProvider_Name verifies the provider reports the correct name.
func TestDashboardProvider_Name(t *testing.T) {
	t.Parallel()

	p := &dashboard.Provider{}
	assert.Equal(t, "Dashboard", p.Name())
}

// TestDashboardProvider_Init_WithNilConfig exercises Init when the config does not
// implement AWSSDKProvider (all handlers/clients will be nil).
func TestDashboardProvider_Init_WithNilConfig(t *testing.T) {
	t.Parallel()

	p := &dashboard.Provider{}
	appCtx := &service.AppContext{
		Logger: slog.Default(),
		Config: nil, // no AWSSDKProvider
	}

	reg, err := p.Init(appCtx)
	require.NoError(t, err)
	assert.NotNil(t, reg)
	assert.Equal(t, "Dashboard", reg.Name())
}

// TestDashboardProvider_Init_WithSTSHandler exercises the new stsOps code path:
// when the config provides a non-nil STS handler, it is cast and stored on the
// DashboardHandler (covering the `if stsHandler != nil { stsOps, _ = ... }` block).
func TestDashboardProvider_Init_WithSTSHandler(t *testing.T) {
	t.Parallel()

	// Build all real in-memory backends and handlers so the cast succeeds.
	ddbBk := ddbbackend.NewInMemoryDB()
	ddbHndlr := ddbbackend.NewHandler(ddbBk, slog.Default())
	s3Bk := s3backend.NewInMemoryBackend(nil, nil)
	s3Hndlr := s3backend.NewHandler(s3Bk, slog.Default())
	ssmBk := ssmbackend.NewInMemoryBackend()
	ssmHndlr := ssmbackend.NewHandler(ssmBk, slog.Default())
	stsBk := stsbackend.NewInMemoryBackend()
	stsHndlr := stsbackend.NewHandler(stsBk, slog.Default())

	provider := &mockAWSProvider{
		ddbHandler: ddbHndlr,
		s3Handler:  s3Hndlr,
		ssmHandler: ssmHndlr,
		stsHandler: stsHndlr,
	}

	p := &dashboard.Provider{}
	appCtx := &service.AppContext{
		Logger: slog.Default(),
		Config: provider,
	}

	reg, err := p.Init(appCtx)
	require.NoError(t, err)
	assert.NotNil(t, reg)
	assert.Equal(t, "Dashboard", reg.Name())
}

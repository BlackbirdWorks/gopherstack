package codeconnections_test

import (
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ccsdk "github.com/aws/aws-sdk-go-v2/service/codeconnections"
	cctypes "github.com/aws/aws-sdk-go-v2/service/codeconnections/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

func newCCFixture(t *testing.T) (*ccsdk.Client, *codeconnections.InMemoryBackend) {
	t.Helper()

	b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")

	return newTestCodeConnectionsClient(t, codeconnections.NewHandler(b)), b
}

func TestSDK_ConnectionInputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		connName string
		provider cctypes.ProviderType
		wantCode string
		tags     []cctypes.Tag
	}{
		{name: "ok", connName: "ok", provider: cctypes.ProviderTypeGithub},
		{name: "name_32", connName: strings.Repeat("a", 32), provider: cctypes.ProviderTypeGithub},
		{
			name:     "name_33",
			connName: strings.Repeat("a", 33),
			provider: cctypes.ProviderTypeGithub,
			wantCode: "InvalidInputException",
		},
		{name: "no_provider_no_host", connName: "ok", wantCode: "InvalidInputException"},
		{
			name: "too_many_tags", connName: "ok", provider: cctypes.ProviderTypeGithub,
			tags: func() []cctypes.Tag {
				out := make([]cctypes.Tag, 0, 201)
				for i := range 201 {
					out = append(
						out,
						cctypes.Tag{
							Key: aws.String(
								strings.Repeat("k", 3) + string(rune('a'+i%26)) + strings.Repeat("x", i/26),
							),
							Value: aws.String("v"),
						},
					)
				}

				return out
			}(),
			wantCode: "LimitExceededException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newCCFixture(t)
			_, err := client.CreateConnection(t.Context(), &ccsdk.CreateConnectionInput{
				ConnectionName: aws.String(tt.connName),
				ProviderType:   tt.provider,
				Tags:           tt.tags,
			})

			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr interface{ ErrorCode() string }

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}

func TestSDK_ConnectionFromHostInheritsProvider(t *testing.T) {
	t.Parallel()

	client, _ := newCCFixture(t)
	host, err := client.CreateHost(t.Context(), &ccsdk.CreateHostInput{
		Name:             aws.String("h"),
		ProviderType:     cctypes.ProviderTypeGithubEnterpriseServer,
		ProviderEndpoint: aws.String("https://ghe.example.com"),
	})
	require.NoError(t, err)

	conn, err := client.CreateConnection(t.Context(), &ccsdk.CreateConnectionInput{
		ConnectionName: aws.String("c"),
		HostArn:        host.HostArn,
	})
	require.NoError(t, err)

	got, err := client.GetConnection(t.Context(), &ccsdk.GetConnectionInput{ConnectionArn: conn.ConnectionArn})
	require.NoError(t, err)
	assert.Equal(t, cctypes.ProviderTypeGithubEnterpriseServer, got.Connection.ProviderType)
}

func TestSDK_ErrorMessageHasNoCode(t *testing.T) {
	t.Parallel()

	client, _ := newCCFixture(t)
	_, err := client.GetConnection(t.Context(), &ccsdk.GetConnectionInput{
		ConnectionArn: aws.String("arn:aws:codeconnections:us-east-1:123456789012:connection/none"),
	})

	var nf *cctypes.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.NotEmpty(t, aws.ToString(nf.Message))
	assert.NotContains(t, aws.ToString(nf.Message), "ResourceNotFoundException")
}

func TestSDK_ListPaginationBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token   *string
		name    string
		max     int32
		wantErr bool
	}{
		{name: "garbage_token", token: aws.String("%%%"), wantErr: true},
		{name: "max_too_large", max: 101, wantErr: true},
		{name: "negative_max", max: -1, wantErr: true},
		{name: "max_ok", max: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newCCFixture(t)
			_, err := client.ListConnections(
				t.Context(),
				&ccsdk.ListConnectionsInput{NextToken: tt.token, MaxResults: tt.max},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_StatusLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantConn    cctypes.ConnectionStatus
		wantHostStr string
		delay       time.Duration
	}{
		{name: "default_available", wantConn: cctypes.ConnectionStatusAvailable, wantHostStr: "AVAILABLE"},
		{name: "delayed_pending", delay: time.Hour, wantConn: cctypes.ConnectionStatusPending, wantHostStr: "PENDING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newCCFixture(t)
			b.SetConnectionHandshakeDelay(tt.delay)
			b.SetHostSetupDelay(tt.delay)

			conn, err := client.CreateConnection(t.Context(), &ccsdk.CreateConnectionInput{
				ConnectionName: aws.String("c"), ProviderType: cctypes.ProviderTypeGithub,
			})
			require.NoError(t, err)

			got, err := client.GetConnection(t.Context(), &ccsdk.GetConnectionInput{ConnectionArn: conn.ConnectionArn})
			require.NoError(t, err)
			assert.Equal(t, tt.wantConn, got.Connection.ConnectionStatus)

			list, err := client.ListConnections(t.Context(), &ccsdk.ListConnectionsInput{})
			require.NoError(t, err)
			require.Len(t, list.Connections, 1)
			assert.Equal(t, tt.wantConn, list.Connections[0].ConnectionStatus)

			host, err := client.CreateHost(t.Context(), &ccsdk.CreateHostInput{
				Name:             aws.String("h"),
				ProviderType:     cctypes.ProviderTypeGithubEnterpriseServer,
				ProviderEndpoint: aws.String("https://ghe.example.com"),
			})
			require.NoError(t, err)

			gotHost, err := client.GetHost(t.Context(), &ccsdk.GetHostInput{HostArn: host.HostArn})
			require.NoError(t, err)
			assert.Equal(t, tt.wantHostStr, aws.ToString(gotHost.Status))
		})
	}
}

func TestSDK_StatusCompletes(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		client, b := newCCFixture(t)
		b.SetConnectionHandshakeDelay(20 * time.Millisecond)

		conn, err := client.CreateConnection(t.Context(), &ccsdk.CreateConnectionInput{
			ConnectionName: aws.String("c"), ProviderType: cctypes.ProviderTypeGithub,
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			got, getErr := client.GetConnection(
				t.Context(),
				&ccsdk.GetConnectionInput{ConnectionArn: conn.ConnectionArn},
			)

			return getErr == nil && got.Connection.ConnectionStatus == cctypes.ConnectionStatusAvailable
		}, 5*time.Second, 10*time.Millisecond)
	})
}

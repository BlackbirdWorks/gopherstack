package codestarconnections_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cscsdk "github.com/aws/aws-sdk-go-v2/service/codestarconnections"
	csctypes "github.com/aws/aws-sdk-go-v2/service/codestarconnections/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

func newCSCFixture(t *testing.T) (*cscsdk.Client, *codestarconnections.InMemoryBackend) {
	t.Helper()

	b := codestarconnections.NewInMemoryBackend("123456789012", "us-east-1")

	return newTestCodeStarConnectionsClient(t, codestarconnections.NewHandler(b)), b
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

			client, _ := newCSCFixture(t)
			_, err := client.ListConnections(
				t.Context(),
				&cscsdk.ListConnectionsInput{NextToken: tt.token, MaxResults: tt.max},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_ErrorMessageHasNoCode(t *testing.T) {
	t.Parallel()

	client, _ := newCSCFixture(t)
	_, err := client.GetConnection(t.Context(), &cscsdk.GetConnectionInput{
		ConnectionArn: aws.String("arn:aws:codestar-connections:us-east-1:123456789012:connection/none"),
	})

	var nf *csctypes.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.NotContains(t, aws.ToString(nf.Message), "ResourceNotFoundException")
	assert.Contains(t, aws.ToString(nf.Message), "Connection not found")
}

func TestSDK_StatusLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantConn csctypes.ConnectionStatus
		wantHost string
		delay    time.Duration
	}{
		{name: "default", wantConn: csctypes.ConnectionStatusAvailable, wantHost: "PENDING"},
		{name: "delayed", delay: time.Hour, wantConn: csctypes.ConnectionStatusPending, wantHost: "PENDING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newCSCFixture(t)
			b.SetConnectionHandshakeDelay(tt.delay)
			b.SetHostSetupDelay(tt.delay)

			conn, err := client.CreateConnection(t.Context(), &cscsdk.CreateConnectionInput{
				ConnectionName: aws.String("c"), ProviderType: csctypes.ProviderTypeGithub,
			})
			require.NoError(t, err)

			got, err := client.GetConnection(t.Context(), &cscsdk.GetConnectionInput{ConnectionArn: conn.ConnectionArn})
			require.NoError(t, err)
			assert.Equal(t, tt.wantConn, got.Connection.ConnectionStatus)

			host, err := client.CreateHost(t.Context(), &cscsdk.CreateHostInput{
				Name:             aws.String("h"),
				ProviderType:     csctypes.ProviderTypeGithubEnterpriseServer,
				ProviderEndpoint: aws.String("https://ghe.example.com"),
			})
			require.NoError(t, err)

			gotHost, err := client.GetHost(t.Context(), &cscsdk.GetHostInput{HostArn: host.HostArn})
			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, aws.ToString(gotHost.Status))
		})
	}
}

func TestSDK_StatusCompletes(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		client, b := newCSCFixture(t)
		b.SetConnectionHandshakeDelay(20 * time.Millisecond)
		b.SetHostSetupDelay(20 * time.Millisecond)

		conn, err := client.CreateConnection(t.Context(), &cscsdk.CreateConnectionInput{
			ConnectionName: aws.String("c"), ProviderType: csctypes.ProviderTypeGithub,
		})
		require.NoError(t, err)

		host, err := client.CreateHost(t.Context(), &cscsdk.CreateHostInput{
			Name:             aws.String("h"),
			ProviderType:     csctypes.ProviderTypeGithubEnterpriseServer,
			ProviderEndpoint: aws.String("https://ghe.example.com"),
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			got, getErr := client.GetConnection(
				t.Context(),
				&cscsdk.GetConnectionInput{ConnectionArn: conn.ConnectionArn},
			)
			gotHost, hostErr := client.GetHost(t.Context(), &cscsdk.GetHostInput{HostArn: host.HostArn})

			return getErr == nil && hostErr == nil &&
				got.Connection.ConnectionStatus == csctypes.ConnectionStatusAvailable &&
				aws.ToString(gotHost.Status) == "AVAILABLE"
		}, 5*time.Second, 10*time.Millisecond)
	})
}

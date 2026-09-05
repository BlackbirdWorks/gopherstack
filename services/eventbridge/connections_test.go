package eventbridge_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestTags_Connection(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "TagResource", map[string]any{
		"ResourceARN": conn.ConnectionArn,
		"Tags":        []map[string]string{{"Key": "purpose", "Value": "ci"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": conn.ConnectionArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ci")
}

func TestConnection_AuthTypes(t *testing.T) {
	t.Parallel()

	valid := []string{"BASIC", "API_KEY", "OAUTH_CLIENT_CREDENTIALS"}
	for _, authType := range valid {
		t.Run(authType, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
				Name:              "conn-" + authType,
				AuthorizationType: authType,
			})
			require.NoError(t, err)
			assert.Equal(t, authType, conn.AuthorizationType)
			assert.Equal(t, "AUTHORIZED", conn.ConnectionState)
		})
	}

	invalid := []string{"", "NONE", "OAUTH", "KEY"}
	for _, authType := range invalid {
		t.Run("invalid-"+authType, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
				Name:              "conn-invalid",
				AuthorizationType: authType,
			})
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

func TestConnection_UpdateAuthType(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	updated, err := b.UpdateConnection(context.Background(), eventbridge.UpdateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "API_KEY",
	})
	require.NoError(t, err)
	assert.Equal(t, "API_KEY", updated.AuthorizationType)
}

func TestConnection_DeauthorizeTransitionsState(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "deauth-conn",
		AuthorizationType: "OAUTH_CLIENT_CREDENTIALS",
	})
	require.NoError(t, err)

	conn, err := b.DeauthorizeConnection(context.Background(), "deauth-conn")
	require.NoError(t, err)
	assert.Equal(t, "DEAUTHORIZED", conn.ConnectionState)
}

func TestConnection_BasicAuthParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "basic-conn",
		AuthorizationType: "BASIC",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			BasicAuthParameters: &eventbridge.ConnectionBasicAuthParameters{
				Username: "admin",
				Password: "s3cr3t",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, conn.AuthParameters)
	require.NotNil(t, conn.AuthParameters.BasicAuthParameters)
	assert.Equal(t, "admin", conn.AuthParameters.BasicAuthParameters.Username)
	// Password masked on return.
	assert.Empty(t, conn.AuthParameters.BasicAuthParameters.Password)
}

func TestConnection_APIKeyAuthParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "apikey-conn",
		AuthorizationType: "API_KEY",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			APIKeyAuthParameters: &eventbridge.ConnectionAPIKeyAuthParameters{
				APIKeyName:  "x-api-key",
				APIKeyValue: "my-secret-key",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, conn.AuthParameters)
	require.NotNil(t, conn.AuthParameters.APIKeyAuthParameters)
	assert.Equal(t, "x-api-key", conn.AuthParameters.APIKeyAuthParameters.APIKeyName)
	// ApiKeyValue masked on return.
	assert.Empty(t, conn.AuthParameters.APIKeyAuthParameters.APIKeyValue)
}

func TestConnection_OAuthParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "oauth-conn",
		AuthorizationType: "OAUTH_CLIENT_CREDENTIALS",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			OAuthParameters: &eventbridge.ConnectionOAuthParameters{
				AuthorizationEndpoint: "https://auth.example.com/token",
				HTTPMethod:            "POST",
				ClientParameters: &eventbridge.ConnectionOAuthClientParameters{
					ClientID:     "my-client-id",
					ClientSecret: "my-client-secret",
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, conn.AuthParameters)
	require.NotNil(t, conn.AuthParameters.OAuthParameters)
	assert.Equal(t, "https://auth.example.com/token", conn.AuthParameters.OAuthParameters.AuthorizationEndpoint)
	assert.Equal(t, "my-client-id", conn.AuthParameters.OAuthParameters.ClientParameters.ClientID)
	// ClientSecret masked on return.
	assert.Empty(t, conn.AuthParameters.OAuthParameters.ClientParameters.ClientSecret)
}

func TestConnection_InvocationHTTPParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "http-param-conn",
		AuthorizationType: "API_KEY",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			APIKeyAuthParameters: &eventbridge.ConnectionAPIKeyAuthParameters{
				APIKeyName:  "x-api-key",
				APIKeyValue: "secret",
			},
			InvocationHTTPParameters: &eventbridge.ConnectionHTTPParameters{
				HeaderParameters: []eventbridge.ConnectionHeaderParameter{
					{Key: "X-Custom-Header", Value: "custom-value"},
					{Key: "X-Secret-Header", Value: "secret-val", IsValueSecret: true},
				},
				QueryStringParameters: []eventbridge.ConnectionQueryStringParameter{
					{Key: "format", Value: "json"},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, conn.AuthParameters)
	require.NotNil(t, conn.AuthParameters.InvocationHTTPParameters)

	headers := conn.AuthParameters.InvocationHTTPParameters.HeaderParameters
	require.Len(t, headers, 2)
	// Non-secret header value preserved.
	assert.Equal(t, "custom-value", headers[0].Value)
	// Secret header value masked.
	assert.Empty(t, headers[1].Value)
	assert.True(t, headers[1].IsValueSecret)

	queries := conn.AuthParameters.InvocationHTTPParameters.QueryStringParameters
	require.Len(t, queries, 1)
	assert.Equal(t, "json", queries[0].Value)
}

func TestConnection_UpdateAuthParameters(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "update-auth-conn",
		AuthorizationType: "BASIC",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			BasicAuthParameters: &eventbridge.ConnectionBasicAuthParameters{
				Username: "olduser",
				Password: "oldpass",
			},
		},
	})
	require.NoError(t, err)

	updated, err := b.UpdateConnection(context.Background(), eventbridge.UpdateConnectionInput{
		Name:              "update-auth-conn",
		AuthorizationType: "API_KEY",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			APIKeyAuthParameters: &eventbridge.ConnectionAPIKeyAuthParameters{
				APIKeyName:  "new-key",
				APIKeyValue: "new-secret",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "API_KEY", updated.AuthorizationType)
	require.NotNil(t, updated.AuthParameters)
	require.NotNil(t, updated.AuthParameters.APIKeyAuthParameters)
	assert.Equal(t, "new-key", updated.AuthParameters.APIKeyAuthParameters.APIKeyName)
}

func TestCreateConnection_AuthTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		authType string
	}{
		{name: "API_KEY allowed", authType: "API_KEY"},
		{name: "BASIC allowed", authType: "BASIC"},
		{name: "OAUTH_CLIENT_CREDENTIALS allowed", authType: "OAUTH_CLIENT_CREDENTIALS"},
		{name: "INVALID rejected", authType: "INVALID", wantErr: eventbridge.ErrInvalidParameter},
		{name: "empty rejected", authType: "", wantErr: eventbridge.ErrInvalidParameter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
				Name:              "conn-" + tt.name,
				AuthorizationType: tt.authType,
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConnectionCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeConnection(context.Background(), "missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("delete not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		require.ErrorIs(t, b.DeleteConnection(context.Background(), "missing"), eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
			Name:              "my-conn",
			AuthorizationType: "API_KEY",
		})
		require.NoError(t, err)

		got, err := b.DescribeConnection(context.Background(), "my-conn")
		require.NoError(t, err)
		assert.Equal(t, "my-conn", got.Name)

		updated, err := b.UpdateConnection(context.Background(), eventbridge.UpdateConnectionInput{
			Name:        "my-conn",
			Description: "updated desc",
		})
		require.NoError(t, err)
		assert.Equal(t, "updated desc", updated.Description)

		conns, _, err := b.ListConnections(context.Background(), "my-", "", "", 0)
		require.NoError(t, err)
		assert.Len(t, conns, 1)

		require.NoError(t, b.DeleteConnection(context.Background(), "my-conn"))
		_, err = b.DescribeConnection(context.Background(), "my-conn")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})
}

// TestCreateConnection_RequiresName verifies connection validation.
func TestCreateConnection_RequiresName(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{AuthorizationType: "BASIC"})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

// TestCreateConnection_RoundTrip verifies connection is AUTHORIZED on creation.
func TestCreateConnection_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	assert.Equal(t, "AUTHORIZED", conn.ConnectionState)
	assert.NotEmpty(t, conn.ConnectionArn)
	assert.Contains(t, conn.ConnectionArn, "connection/my-conn")
	assert.Equal(t, 1, b.ConnectionCount())
}

// TestDeauthorizeConnection_RoundTrip verifies deauthorization state change.
func TestDeauthorizeConnection_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	conn, err := b.DeauthorizeConnection(context.Background(), "my-conn")
	require.NoError(t, err)
	assert.Equal(t, "DEAUTHORIZED", conn.ConnectionState)
}

// TestDeauthorizeConnection_NotFound returns ErrNotFound for missing connections.
func TestDeauthorizeConnection_NotFound(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.DeauthorizeConnection(context.Background(), "no-such-conn")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

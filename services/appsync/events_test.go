package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestCreateAPI_EventConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	eventCfg := &appsync.EventConfig{
		AuthProviders: []appsync.AuthProvider{
			{AuthType: "API_KEY"},
			{AuthType: "AWS_IAM"},
		},
		ConnectionAuthModes:       []appsync.AuthMode{{AuthType: "API_KEY"}},
		DefaultPublishAuthModes:   []appsync.AuthMode{{AuthType: "API_KEY"}},
		DefaultSubscribeAuthModes: []appsync.AuthMode{{AuthType: "API_KEY"}},
	}

	api, err := b.CreateAPI("EventsAPI", "ops@example.com", nil, eventCfg)
	require.NoError(t, err)
	require.NotNil(t, api.EventConfig)
	assert.Len(t, api.EventConfig.AuthProviders, 2)
	assert.Equal(t, "API_KEY", api.EventConfig.AuthProviders[0].AuthType)
	assert.Equal(t, "AWS_IAM", api.EventConfig.AuthProviders[1].AuthType)
	assert.Len(t, api.EventConfig.ConnectionAuthModes, 1)
	assert.Equal(t, "API_KEY", api.EventConfig.ConnectionAuthModes[0].AuthType)

	got, err := b.GetAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.EventConfig)
	assert.Len(t, got.EventConfig.AuthProviders, 2)
}

func TestCreateAPI_Dns_IsMap(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, api.DNS)
	assert.Contains(t, api.DNS, "HTTP")
	assert.Contains(t, api.DNS, "REALTIME")
	assert.NotEmpty(t, api.DNS["HTTP"])
	assert.NotEmpty(t, api.DNS["REALTIME"])
}

func TestUpdateAPI_EventConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)
	assert.Nil(t, api.EventConfig)

	eventCfg := &appsync.EventConfig{
		AuthProviders:             []appsync.AuthProvider{{AuthType: "AWS_IAM"}},
		ConnectionAuthModes:       []appsync.AuthMode{{AuthType: "AWS_IAM"}},
		DefaultPublishAuthModes:   []appsync.AuthMode{{AuthType: "AWS_IAM"}},
		DefaultSubscribeAuthModes: []appsync.AuthMode{{AuthType: "AWS_IAM"}},
	}

	updated, err := b.UpdateAPI(api.APIID, "", "", eventCfg)
	require.NoError(t, err)
	require.NotNil(t, updated.EventConfig)
	assert.Equal(t, "AWS_IAM", updated.EventConfig.AuthProviders[0].AuthType)
}

func TestInMemoryBackend_EventAPI_CRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Create.
	api, err := b.CreateAPI("MyEventAPI", "", map[string]string{"env": "test"}, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, api.APIID)

	// Get.
	got, err := b.GetAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "MyEventAPI", got.Name)

	// List.
	apis, err := b.ListAPIs()
	require.NoError(t, err)
	assert.Len(t, apis, 1)

	// Delete.
	err = b.DeleteAPI(api.APIID)
	require.NoError(t, err)

	// Get after delete returns error.
	_, err = b.GetAPI(api.APIID)
	require.ErrorIs(t, err, awserr.ErrNotFound)

	// List returns 0.
	apis, err = b.ListAPIs()
	require.NoError(t, err)
	assert.Empty(t, apis)
}

func TestInMemoryBackend_EventAPI_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		op   func(b *appsync.InMemoryBackend) error
		name string
	}{
		{
			name: "GetAPI",
			op: func(b *appsync.InMemoryBackend) error {
				_, err := b.GetAPI("nonexistent")

				return err
			},
		},
		{
			name: "DeleteAPI",
			op: func(b *appsync.InMemoryBackend) error {
				return b.DeleteAPI("nonexistent")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			require.ErrorIs(t, tt.op(b), awserr.ErrNotFound)
		})
	}
}

func TestInMemoryBackend_UpdateAPI(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	updated, err := b.UpdateAPI(api.APIID, "UpdatedName", "ops@example.com", nil)
	require.NoError(t, err)
	assert.Equal(t, "UpdatedName", updated.Name)
	assert.Equal(t, "ops@example.com", updated.OwnerContact)

	// Not found.
	_, err = b.UpdateAPI("nonexistent", "x", "", nil)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_CreateAPI_OwnerContact(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("TestEventAPI", "owner@example.com", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "owner@example.com", api.OwnerContact)

	got, err := b.GetAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "owner@example.com", got.OwnerContact)
}

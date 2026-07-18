package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestCreateChannelNamespace_PublishSubscribeAuthModes_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)

	cfg := &appsync.ChannelNamespaceConfig{
		PublishAuthModes:   []appsync.AuthMode{{AuthType: "API_KEY"}, {AuthType: "AWS_IAM"}},
		SubscribeAuthModes: []appsync.AuthMode{{AuthType: "API_KEY"}},
	}

	ns, err := b.CreateChannelNamespace(api.APIID, "chat", nil, cfg)
	require.NoError(t, err)
	assert.Len(t, ns.PublishAuthModes, 2)
	assert.Equal(t, "API_KEY", ns.PublishAuthModes[0].AuthType)
	assert.Equal(t, "AWS_IAM", ns.PublishAuthModes[1].AuthType)
	assert.Len(t, ns.SubscribeAuthModes, 1)
	assert.Equal(t, "API_KEY", ns.SubscribeAuthModes[0].AuthType)

	got, err := b.GetChannelNamespace(api.APIID, "chat")
	require.NoError(t, err)
	assert.Len(t, got.PublishAuthModes, 2)
	assert.Len(t, got.SubscribeAuthModes, 1)
}

func TestCreateChannelNamespace_HandlerConfigs_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)

	cfg := &appsync.ChannelNamespaceConfig{
		HandlerConfigs: &appsync.HandlerConfigs{
			OnPublish: &appsync.HandlerConfig{
				Behavior: "CODE",
				Integration: &appsync.Integration{
					DataSourceName: "MyLambda",
				},
			},
			OnSubscribe: &appsync.HandlerConfig{
				Behavior: "CODE",
				Integration: &appsync.Integration{
					DataSourceName: "MyLambda",
				},
			},
		},
	}

	ns, err := b.CreateChannelNamespace(api.APIID, "events", nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, ns.HandlerConfigs)
	require.NotNil(t, ns.HandlerConfigs.OnPublish)
	assert.Equal(t, "CODE", ns.HandlerConfigs.OnPublish.Behavior)
	assert.Equal(t, "MyLambda", ns.HandlerConfigs.OnPublish.Integration.DataSourceName)
	require.NotNil(t, ns.HandlerConfigs.OnSubscribe)
	assert.Equal(t, "MyLambda", ns.HandlerConfigs.OnSubscribe.Integration.DataSourceName)

	got, err := b.GetChannelNamespace(api.APIID, "events")
	require.NoError(t, err)
	require.NotNil(t, got.HandlerConfigs)
	assert.Equal(t, "CODE", got.HandlerConfigs.OnPublish.Behavior)
}

func TestUpdateChannelNamespace_AuthModes_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "chat", nil, nil)
	require.NoError(t, err)

	cfg := &appsync.ChannelNamespaceConfig{
		PublishAuthModes:   []appsync.AuthMode{{AuthType: "AWS_IAM"}},
		SubscribeAuthModes: []appsync.AuthMode{{AuthType: "AWS_IAM"}, {AuthType: "API_KEY"}},
	}

	updated, err := b.UpdateChannelNamespace(api.APIID, "chat", cfg)
	require.NoError(t, err)
	assert.Len(t, updated.PublishAuthModes, 1)
	assert.Equal(t, "AWS_IAM", updated.PublishAuthModes[0].AuthType)
	assert.Len(t, updated.SubscribeAuthModes, 2)

	got, err := b.GetChannelNamespace(api.APIID, "chat")
	require.NoError(t, err)
	assert.Len(t, got.PublishAuthModes, 1)
	assert.Len(t, got.SubscribeAuthModes, 2)
}

func TestUpdateChannelNamespace_CodeHandlers_Via_Config(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "ns", nil, nil)
	require.NoError(t, err)

	const code = `export const handler = () => {};`
	cfg := &appsync.ChannelNamespaceConfig{CodeHandlers: code}

	updated, err := b.UpdateChannelNamespace(api.APIID, "ns", cfg)
	require.NoError(t, err)
	assert.Equal(t, code, updated.CodeHandlers)
}

func TestInMemoryBackend_DeleteAPI_CascadesChannelNamespaces(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	api, err := b.CreateAPI("MyEventAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "ns1", nil, nil)
	require.NoError(t, err)

	// Delete should cascade channel namespaces.
	err = b.DeleteAPI(api.APIID)
	require.NoError(t, err)

	// Recreate the API - should have no namespaces.
	api2, err := b.CreateAPI("NewAPI", "", nil, nil)
	require.NoError(t, err)
	assert.NotEqual(t, api.APIID, api2.APIID)
}

func TestInMemoryBackend_ChannelNamespace_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nsName    string
		createTwo bool
		wantErr   bool
	}{
		{name: "creates_and_returns", nsName: "ns1"},
		{name: "duplicate_returns_error", nsName: "ns2", createTwo: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateAPI("EventAPI", "", nil, nil)
			require.NoError(t, err)

			_, err = b.CreateChannelNamespace(api.APIID, tt.nsName, nil, nil)
			require.NoError(t, err)

			if !tt.createTwo {
				return
			}

			_, err = b.CreateChannelNamespace(api.APIID, tt.nsName, nil, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_GetChannelNamespace_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.GetChannelNamespace(api.APIID, "nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListChannelNamespaces(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "ns1", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateChannelNamespace(api.APIID, "ns2", nil, nil)
	require.NoError(t, err)

	nss, err := b.ListChannelNamespaces(api.APIID)
	require.NoError(t, err)
	assert.Len(t, nss, 2)

	// Not found API returns error.
	_, err = b.ListChannelNamespaces("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateChannelNamespace(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "ns1", nil, nil)
	require.NoError(t, err)

	const codeHandlers = "export const handler = () => {}"
	updated, err := b.UpdateChannelNamespace(
		api.APIID, "ns1", &appsync.ChannelNamespaceConfig{CodeHandlers: codeHandlers},
	)
	require.NoError(t, err)
	assert.Equal(t, codeHandlers, updated.CodeHandlers)
	assert.NotZero(t, updated.LastModified)

	// Not found returns error.
	_, err = b.UpdateChannelNamespace(api.APIID, "missing", &appsync.ChannelNamespaceConfig{CodeHandlers: "code"})
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DeleteChannelNamespace(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "ns1", nil, nil)
	require.NoError(t, err)

	err = b.DeleteChannelNamespace(api.APIID, "ns1")
	require.NoError(t, err)

	// Second delete returns error.
	err = b.DeleteChannelNamespace(api.APIID, "ns1")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ChannelNamespace_HasTimestamps(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	ns, err := b.CreateChannelNamespace(api.APIID, "ns1", map[string]string{"env": "test"}, nil)
	require.NoError(t, err)
	assert.NotZero(t, ns.Created)
	assert.NotZero(t, ns.LastModified)
	assert.Equal(t, ns.Created, ns.LastModified)
}

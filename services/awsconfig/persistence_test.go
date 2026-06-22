package awsconfig_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *awsconfig.InMemoryBackend) string
		verify func(t *testing.T, b *awsconfig.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *awsconfig.InMemoryBackend) string {
				err := b.PutConfigurationRecorder("test-recorder", "arn:aws:iam::000000000000:role/test")
				if err != nil {
					return ""
				}

				return "test-recorder"
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				recorders := b.DescribeConfigurationRecorders(nil)
				require.NotEmpty(t, recorders)
				assert.Equal(t, id, recorders[0].Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *awsconfig.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, _ string) {
				t.Helper()

				recorders := b.DescribeConfigurationRecorders(nil)
				assert.Empty(t, recorders)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := awsconfig.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := awsconfig.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestAWSConfigHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := awsconfig.NewInMemoryBackend()
	h := awsconfig.NewHandler(backend)

	err := backend.PutConfigurationRecorder("snap-recorder", "arn:aws:iam::000000000000:role/test")
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := awsconfig.NewInMemoryBackend()
	freshH := awsconfig.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	recorders := fresh.DescribeConfigurationRecorders(nil)
	assert.Len(t, recorders, 1)
}

func TestAWSConfigBackend_DeleteOperations(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	// Create a recorder and channel
	err := b.PutConfigurationRecorder("test-recorder", "arn:aws:iam::000000000000:role/test")
	require.NoError(t, err)

	err = b.PutDeliveryChannel("test-channel", "my-bucket", "")
	require.NoError(t, err)

	// Delete delivery channel
	err = b.DeleteDeliveryChannel("test-channel")
	require.NoError(t, err)

	channels := b.DescribeDeliveryChannels(nil)
	assert.Empty(t, channels)

	// Delete configuration recorder
	err = b.DeleteConfigurationRecorder("test-recorder")
	require.NoError(t, err)

	recorders := b.DescribeConfigurationRecorders(nil)
	assert.Empty(t, recorders)
}

func TestAWSConfigHandler_Routing(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())

	assert.Equal(t, "AWSConfig", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{"config target", "StarlingDoveService.PutConfigurationRecorder", true},
		{"other target", "SQS.SendMessage", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestAWSConfigHandler_DeleteOperations(t *testing.T) {
	t.Parallel()

	backend := awsconfig.NewInMemoryBackend()
	h := awsconfig.NewHandler(backend)

	// Put and then delete delivery channel via handler
	_ = backend.PutDeliveryChannel("test-channel", "my-bucket", "")
	_ = backend.PutConfigurationRecorder("test-recorder", "arn:aws:iam::000000000000:role/test")

	e := echo.New()

	// Delete delivery channel via handler
	body := `{"DeliveryChannelName":"test-channel"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "StarlingDoveService.DeleteDeliveryChannel")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete config recorder via handler
	body2 := `{"ConfigurationRecorderName":"test-recorder"}`
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body2))
	req2.Header.Set("X-Amz-Target", "StarlingDoveService.DeleteConfigurationRecorder")
	req2.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	err = h.Handler()(c2)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestInMemoryBackend_Snapshot_AllMaps(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutConfigurationRecorder("rec", "arn:aws:iam::000:role/r"))
	require.NoError(t, b.PutDeliveryChannel("chan", "bucket", ""))
	require.NoError(t, b.PutAggregationAuthorization("123456789012", "us-east-1"))
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-x"}))
	require.NoError(t, b.PutConfigurationAggregator("agg-1"))
	require.NoError(t, b.PutConformancePack("pack-1"))
	require.NoError(t, b.PutOrganizationConfigRule("org-rule-1"))
	require.NoError(t, b.PutOrganizationConformancePack("org-pack-1"))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := awsconfig.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Verify all maps were restored
	recorders := fresh.DescribeConfigurationRecorders(nil)
	require.Len(t, recorders, 1)
	assert.Equal(t, "rec", recorders[0].Name)

	channels := fresh.DescribeDeliveryChannels(nil)
	require.Len(t, channels, 1)
	assert.Equal(t, "chan", channels[0].Name)

	auths := fresh.DescribeAggregationAuthorizations()
	require.Len(t, auths, 1)
	assert.Equal(t, "123456789012", auths[0].AuthorizedAccountID)

	rules := fresh.DescribeConfigRules(nil)
	require.Len(t, rules, 1)
	assert.Equal(t, "rule-x", rules[0].ConfigRuleName)

	// Verify the other maps by attempting deletes (they would fail if not restored)
	require.NoError(t, fresh.DeleteConfigurationAggregator("agg-1"))
	require.NoError(t, fresh.DeleteConformancePack("pack-1"))
	require.NoError(t, fresh.DeleteOrganizationConfigRule("org-rule-1"))
	require.NoError(t, fresh.DeleteOrganizationConformancePack("org-pack-1"))
}

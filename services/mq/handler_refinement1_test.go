package mq_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// --- helpers ---

func newRefinement1Backend(t *testing.T) *mq.InMemoryBackend {
	t.Helper()

	return mq.NewInMemoryBackend("000000000000", "us-east-1")
}

// --- Tests ---

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	_, err := b.CreateBroker(
		"my-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "5.18.3", "mq.m5.large",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)

	require.Equal(t, 1, mq.BrokerCount(b))

	b.Reset()
	assert.Equal(t, 0, mq.BrokerCount(b))
	assert.Equal(t, 0, mq.ConfigurationCount(b))
	assert.Equal(t, 0, mq.TagCount(b))
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	h := mq.NewHandler(b)

	_, err := b.CreateBroker(
		"broker-reset", mq.DeploymentModeSingleInstance,
		mq.EngineTypeRabbitMQ, "3.13.2", "mq.m5.large",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, mq.BrokerCount(b))
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &mq.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, mq.ErrNilAppContext)
}

func TestRefinement1_GetSupportedOperations_Count(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	h := mq.NewHandler(b)
	assert.Equal(t, 24, mq.HandlerOpsLen(h))
}

func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("111122223333", "eu-west-1")
	assert.Equal(t, "111122223333", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestRefinement1_AddBrokerInternal(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	br := &mq.Broker{
		BrokerID:    "b-seed-001",
		BrokerName:  "seeded-broker",
		BrokerArn:   "arn:aws:mq:us-east-1:000000000000:broker:seeded-broker",
		BrokerState: mq.BrokerStateRunning,
		EngineType:  mq.EngineTypeActiveMQ,
	}
	mq.AddBrokerInternal(b, br)

	assert.Equal(t, 1, mq.BrokerCount(b))

	got, err := b.DescribeBroker("b-seed-001")
	require.NoError(t, err)
	assert.Equal(t, "seeded-broker", got.BrokerName)
}

func TestRefinement1_AddConfigurationInternal(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	cfg := &mq.Configuration{
		ID:             "c-seed001",
		Name:           "seeded-config",
		Arn:            "arn:aws:mq:us-east-1:000000000000:configuration:c-seed001",
		EngineType:     mq.EngineTypeActiveMQ,
		LatestRevision: &mq.ConfigurationRevision{Revision: 1, Created: "2024-01-01T00:00:00Z"},
		Revisions:      []mq.ConfigurationRevision{{Revision: 1, Created: "2024-01-01T00:00:00Z"}},
	}
	mq.AddConfigurationInternal(b, cfg)

	assert.Equal(t, 1, mq.ConfigurationCount(b))

	got, err := b.DescribeConfiguration("c-seed001")
	require.NoError(t, err)
	assert.Equal(t, "seeded-config", got.Name)
}

func TestRefinement1_CreateBroker_InvalidEngineType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
	}{
		{name: "empty", engineType: ""},
		{name: "invalid", engineType: "KAFKA"},
		{name: "lowercase", engineType: "activemq"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend(t)
			_, err := b.CreateBroker(
				"my-broker", mq.DeploymentModeSingleInstance,
				tt.engineType, "", "",
				false, false, nil, nil, nil, nil,
			)
			require.ErrorIs(t, err, mq.ErrValidation)
		})
	}
}

func TestRefinement1_CreateBroker_InvalidEngineType_Returns400(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	h := mq.NewHandler(b)

	rec := doMQRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "bad-broker",
		"engineType": "INVALID",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_CreateConfiguration_InvalidEngineType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
	}{
		{name: "empty", engineType: ""},
		{name: "invalid", engineType: "KAFKA"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend(t)
			_, err := b.CreateConfiguration("my-cfg", "", tt.engineType, "", nil)
			require.ErrorIs(t, err, mq.ErrValidation)
		})
	}
}

func TestRefinement1_CreateConfiguration_InvalidEngineType_Returns400(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	h := mq.NewHandler(b)

	rec := doMQRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "bad-config",
		"engineType": "BAD",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_Promote_InvalidMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode string
	}{
		{name: "empty", mode: ""},
		{name: "invalid", mode: "UPGRADE"},
		{name: "lowercase", mode: "failover"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend(t)
			_, err := b.Promote("some-broker", tt.mode)
			require.ErrorIs(t, err, mq.ErrValidation)
		})
	}
}

func TestRefinement1_Promote_ValidMode_BrokerNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode string
	}{
		{name: "failover", mode: mq.PromoteModeFailover},
		{name: "switchover", mode: mq.PromoteModeSwitchover},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend(t)
			_, err := b.Promote("nonexistent", tt.mode)
			require.ErrorIs(t, err, mq.ErrNotFound)
		})
	}
}

func TestRefinement1_Promote_Returns400_InvalidMode(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	br, err := b.CreateBroker(
		"promote-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)

	h := mq.NewHandler(b)

	rec := doMQRequest(t, h, http.MethodPost, "/v1/brokers/"+br.BrokerID+"/promote", map[string]any{
		"mode": "INVALID",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_DefaultHostInstanceType(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	br, err := b.CreateBroker(
		"default-instance-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "", // empty hostInstanceType
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "mq.m5.large", br.HostInstanceType)
}

func TestRefinement1_BrokerUsersInResponseAreSorted(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	br, err := b.CreateBroker(
		"sorted-users-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil,
		[]*mq.User{
			{Username: "zebra"},
			{Username: "alpha"},
			{Username: "middle"},
		},
		nil,
	)
	require.NoError(t, err)

	h := mq.NewHandler(b)
	rec := doMQRequest(t, h, http.MethodGet, "/v1/brokers/"+br.BrokerID, nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	usersRaw, ok := out["users"].([]any)
	require.True(t, ok)
	require.Len(t, usersRaw, 3)

	names := make([]string, len(usersRaw))
	for i, u := range usersRaw {
		uMap := u.(map[string]any)
		names[i] = uMap["username"].(string)
	}

	assert.Equal(t, []string{"alpha", "middle", "zebra"}, names)
}

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)

	br, err := b.CreateBroker(
		"persist-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeRabbitMQ, "3.13.2", "mq.m5.large",
		true, false, nil, nil, nil,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	cfg, err := b.CreateConfiguration(
		"persist-cfg",
		"desc",
		mq.EngineTypeActiveMQ,
		"5.18.3",
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := mq.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, mq.BrokerCount(b2))
	assert.Equal(t, 1, mq.ConfigurationCount(b2))

	restored, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "persist-broker", restored.BrokerName)
	assert.Equal(t, map[string]string{"env": "test"}, restored.Tags)

	rcfg, err := b2.DescribeConfiguration(cfg.ID)
	require.NoError(t, err)
	assert.Equal(t, "persist-cfg", rcfg.Name)

	// Tags should still be in sync after Restore.
	b2.CreateTags(br.BrokerArn, map[string]string{"new-key": "new-val"})

	restored2, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "new-val", restored2.Tags["new-key"])
}

func TestRefinement1_TagsSync_AfterRestore(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	br, err := b.CreateBroker(
		"tag-sync-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil,
		map[string]string{"original": "yes"},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := mq.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// CreateTags via ARN should be reflected in DescribeBroker.
	b2.CreateTags(br.BrokerArn, map[string]string{"added": "after-restore"})

	got, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "yes", got.Tags["original"])
	assert.Equal(t, "after-restore", got.Tags["added"])
}

func TestRefinement1_DeploymentModeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "SINGLE_INSTANCE", mq.DeploymentModeSingleInstance)
	assert.Equal(t, "ACTIVE_STANDBY_MULTI_AZ", mq.DeploymentModeActiveStandby)
	assert.Equal(t, "CLUSTER_MULTI_AZ", mq.DeploymentModeCluster)
	assert.Equal(t, "FAILOVER", mq.PromoteModeFailover)
	assert.Equal(t, "SWITCHOVER", mq.PromoteModeSwitchover)
}

func TestRefinement1_StorageTypeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "EFS", mq.StorageTypeEFS)
	assert.Equal(t, "EBS", mq.StorageTypeEBS)
}

func TestRefinement1_BrokerStateConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "RUNNING", mq.BrokerStateRunning)
	assert.Equal(t, "CREATION_IN_PROGRESS", mq.BrokerStateCreating)
	assert.Equal(t, "DELETION_IN_PROGRESS", mq.BrokerStateDeleting)
	assert.Equal(t, "REBOOT_IN_PROGRESS", mq.BrokerStateRebooting)
}

func TestRefinement1_CreateBroker_ValidEngineTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
	}{
		{name: "activemq", engineType: mq.EngineTypeActiveMQ},
		{name: "rabbitmq", engineType: mq.EngineTypeRabbitMQ},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend(t)
			br, err := b.CreateBroker(
				"broker-"+tt.name, mq.DeploymentModeSingleInstance,
				tt.engineType, "", "",
				false, false, nil, nil, nil, nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.engineType, br.EngineType)
		})
	}
}

func TestRefinement1_CreateConfiguration_ValidEngineTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
	}{
		{name: "activemq", engineType: mq.EngineTypeActiveMQ},
		{name: "rabbitmq", engineType: mq.EngineTypeRabbitMQ},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend(t)
			cfg, err := b.CreateConfiguration("cfg-"+tt.name, "", tt.engineType, "", nil)
			require.NoError(t, err)
			assert.Equal(t, tt.engineType, cfg.EngineType)
		})
	}
}

func TestRefinement1_TagCount_Export(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend(t)
	assert.Equal(t, 0, mq.TagCount(b))

	_, err := b.CreateBroker(
		"tag-count-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil,
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	assert.Equal(t, 1, mq.TagCount(b))
}

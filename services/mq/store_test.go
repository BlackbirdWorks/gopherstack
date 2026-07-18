package mq_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestReset(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestAccountID(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("111122223333", "eu-west-1")
	assert.Equal(t, "111122223333", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestAddBrokerInternal(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestAddConfigurationInternal(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

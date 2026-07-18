package mq_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestRebootBroker_StateTransition(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	br, err := b.CreateBroker(
		"reboot-me", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, mq.BrokerStateRunning, br.BrokerState)

	require.NoError(t, b.RebootBroker(br.BrokerID))

	rebooting, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, mq.BrokerStateRebooting, rebooting.BrokerState)

	settled, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, mq.BrokerStateRunning, settled.BrokerState)
}

func TestRebootBroker_HTTPFullCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "reboot-state", mq.EngineTypeActiveMQ)

	// Confirm initial state is RUNNING.
	assert.Equal(t, mq.BrokerStateRunning, describeTestBroker(t, h, brokerID)["brokerState"])

	// Reboot the broker.
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/reboot", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// First DescribeBroker after reboot shows REBOOT_IN_PROGRESS.
	assert.Equal(t, mq.BrokerStateRebooting, describeTestBroker(t, h, brokerID)["brokerState"],
		"broker state must be REBOOT_IN_PROGRESS immediately after reboot")

	// Second DescribeBroker settles back to RUNNING.
	assert.Equal(t, mq.BrokerStateRunning, describeTestBroker(t, h, brokerID)["brokerState"],
		"broker must return to RUNNING state after the reboot transition is observed")
}

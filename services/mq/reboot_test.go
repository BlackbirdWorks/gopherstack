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

// TestRebootBroker_PromotesSecurityGroups locks in that a staged
// SecurityGroups change (see applyBrokerCoreFields) only takes effect once
// the broker reboots, matching DescribeBrokerOutput's pendingSecurityGroups
// wire field.
func TestRebootBroker_PromotesSecurityGroups(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	br, err := b.CreateBroker(
		"sg-reboot-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "", false, false,
		[]string{"sg-original"}, nil, nil, nil,
	)
	require.NoError(t, err)

	_, err = b.UpdateBroker(br.BrokerID, "", "", nil, []string{"sg-new"})
	require.NoError(t, err)

	staged, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, []string{"sg-original"}, staged.SecurityGroups, "securityGroups must not apply before reboot")
	assert.Equal(t, []string{"sg-new"}, staged.PendingSecurityGroups)

	require.NoError(t, b.RebootBroker(br.BrokerID))
	_, err = b.DescribeBroker(br.BrokerID) // observes REBOOT_IN_PROGRESS and promotes.
	require.NoError(t, err)

	settled, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, []string{"sg-new"}, settled.SecurityGroups, "securityGroups must apply after reboot")
	assert.Nil(t, settled.PendingSecurityGroups, "pendingSecurityGroups must clear after reboot")
}

// TestRebootBroker_PromotedConfigurationGrowsHistory locks in that promoting
// a staged Configurations.Pending association pushes the prior Current entry
// onto Configurations.History (see promotePendingConfiguration), matching
// how DescribeBrokerOutput's Configurations.history accumulates prior
// associations across reboots.
func TestRebootBroker_PromotedConfigurationGrowsHistory(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	cfg1, err := b.CreateConfiguration("cfg-one", "", mq.EngineTypeActiveMQ, "", "", nil)
	require.NoError(t, err)

	cfg2, err := b.CreateConfiguration("cfg-two", "", mq.EngineTypeActiveMQ, "", "", nil)
	require.NoError(t, err)

	br, err := b.CreateBrokerWithOptions(
		"cfg-history-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "", false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{Configuration: &mq.ConfigurationID{ID: cfg1.ID, Revision: 1}},
	)
	require.NoError(t, err)

	_, err = b.UpdateBrokerWithOptions(br.BrokerID, "", "", nil, nil,
		&mq.UpdateBrokerOptions{Configuration: &mq.ConfigurationID{ID: cfg2.ID, Revision: 1}})
	require.NoError(t, err)

	require.NoError(t, b.RebootBroker(br.BrokerID))
	_, err = b.DescribeBroker(br.BrokerID) // observes REBOOT_IN_PROGRESS and promotes.
	require.NoError(t, err)

	settled, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	require.NotNil(t, settled.Configurations)
	require.NotNil(t, settled.Configurations.Current)
	assert.Equal(t, cfg2.ID, settled.Configurations.Current.ID)
	require.Len(t, settled.Configurations.History, 1)
	assert.Equal(t, cfg1.ID, settled.Configurations.History[0].ID)
	assert.Nil(t, settled.Configurations.Pending)
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

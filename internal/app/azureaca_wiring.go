package app

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azurequeue"
	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

var errMeterUnavailable = errors.New("queue service is not running")

// wireAzureARMQueueMeters lets Container Apps KEDA scale rules and event jobs read the Service Bus and Storage
// queue depths of the in-process services.
func wireAzureARMQueueMeters(byName map[string]service.Registerable) {
	armH, ok := byName["AzureARM"].(*azurearm.Handler)
	if !ok {
		return
	}

	m := &azureACAQueueMeters{}

	if h, hOk := byName["AzureServiceBus"].(*azureservicebus.Handler); hOk {
		m.serviceBus = h.Backend
	}

	if h, hOk := byName["AzureQueue"].(*azurequeue.Handler); hOk {
		m.queues = h.Backend
	}

	armH.Registry.SetACAQueueMeters(m)
}

// azureACAQueueMeters adapts the Service Bus and Storage queue backends to azurearm.ACAQueueMeters.
type azureACAQueueMeters struct {
	serviceBus azureservicebus.StorageBackend
	queues     azurequeue.StorageBackend
}

func (m *azureACAQueueMeters) ServiceBusMessages(queue, topic, subscription string) (int, error) {
	if m.serviceBus == nil {
		return 0, errMeterUnavailable
	}

	if queue != "" {
		info, err := m.serviceBus.GetQueueInfo(queue)

		return info.ActiveMessageCount, err
	}

	info, err := m.serviceBus.GetSubscriptionInfo(topic, subscription)

	return info.ActiveMessageCount, err
}

func (m *azureACAQueueMeters) StorageQueueMessages(queue string) (int, error) {
	if m.queues == nil {
		return 0, errMeterUnavailable
	}

	props, err := m.queues.GetQueueProperties(queue)

	return props.ApproximateMessageCount, err
}

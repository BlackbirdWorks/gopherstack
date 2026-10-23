package app

import (
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
)

// wireAzureARMEventHubs gives ARM's Microsoft.EventHub provider the Event Hubs
// data plane.
func wireAzureARMEventHubs(armReg, ehReg service.Registerable) {
	armH, ok := armReg.(*azurearm.Handler)
	if !ok {
		return
	}

	if ehH, ehOk := ehReg.(*azureeventhubs.Handler); ehOk {
		armH.Registry.SetEventHubs(&azureEventHubsAdapter{backend: ehH.Backend})
	}
}

// azureEventHubsAdapter adapts services/azureeventhubs's backend to
// services/azurearm.EventHubs.
type azureEventHubsAdapter struct {
	backend *azureeventhubs.InMemoryBackend
}

func (a *azureEventHubsAdapter) EnsureNamespace(name string) error {
	return a.backend.EnsureNamespace(name)
}

func (a *azureEventHubsAdapter) DeleteNamespace(name string) error {
	return ignoreMissing(a.backend.DeleteNamespace(name), azureeventhubs.ErrNamespaceNotFound)
}

func (a *azureEventHubsAdapter) CreateHub(
	ns, name string, partitionCount int, retention time.Duration,
) (azurearm.EventHubInfo, error) {
	info, err := a.backend.CreateHub(
		azureeventhubs.HubRef{Namespace: ns, Hub: name},
		azureeventhubs.HubConfig{PartitionCount: partitionCount, Retention: retention},
	)
	if err != nil {
		return azurearm.EventHubInfo{}, err
	}

	return hubInfoOf(info), nil
}

func hubInfoOf(info azureeventhubs.HubInfo) azurearm.EventHubInfo {
	return azurearm.EventHubInfo{
		CreatedAt: info.CreatedAt, PartitionIDs: info.PartitionIDs, Retention: info.Config.Retention,
	}
}

func (a *azureEventHubsAdapter) HubInfo(ns, name string) (azurearm.EventHubInfo, bool) {
	info, err := a.backend.Hub(azureeventhubs.HubRef{Namespace: ns, Hub: name})
	if err != nil {
		return azurearm.EventHubInfo{}, false
	}

	return hubInfoOf(info), true
}

func (a *azureEventHubsAdapter) DeleteHub(ns, name string) error {
	return ignoreMissing(
		a.backend.DeleteHub(azureeventhubs.HubRef{Namespace: ns, Hub: name}),
		azureeventhubs.ErrHubNotFound, azureeventhubs.ErrNamespaceNotFound,
	)
}

func (a *azureEventHubsAdapter) CreateConsumerGroup(ns, hub, name string) error {
	_, err := a.backend.CreateConsumerGroup(azureeventhubs.HubRef{Namespace: ns, Hub: hub}, name)

	return err
}

func (a *azureEventHubsAdapter) DeleteConsumerGroup(ns, hub, name string) error {
	return ignoreMissing(
		a.backend.DeleteConsumerGroup(azureeventhubs.HubRef{Namespace: ns, Hub: hub}, name),
		azureeventhubs.ErrConsumerGroupNotFound, azureeventhubs.ErrHubNotFound, azureeventhubs.ErrNamespaceNotFound,
	)
}

func (a *azureEventHubsAdapter) PutRule(ns, hub, name string, rights []string) (azurearm.EventHubRule, error) {
	r, err := a.backend.PutRule(ns, hub, name, rights)
	if err != nil {
		return azurearm.EventHubRule{}, err
	}

	return azurearm.EventHubRule{PrimaryKey: r.PrimaryKey, SecondaryKey: r.SecondaryKey, Rights: r.Rights}, nil
}

func (a *azureEventHubsAdapter) GetRule(ns, hub, name string) (azurearm.EventHubRule, bool) {
	r, err := a.backend.GetRule(ns, hub, name)
	if err != nil {
		return azurearm.EventHubRule{}, false
	}

	return azurearm.EventHubRule{PrimaryKey: r.PrimaryKey, SecondaryKey: r.SecondaryKey, Rights: r.Rights}, true
}

func (a *azureEventHubsAdapter) DeleteRule(ns, hub, name string) error {
	return ignoreMissing(
		a.backend.DeleteRule(ns, hub, name), azureeventhubs.ErrRuleNotFound,
		azureeventhubs.ErrNamespaceNotFound, azureeventhubs.ErrHubNotFound,
	)
}

func ignoreMissing(err error, missing ...error) error {
	for _, m := range missing {
		if errors.Is(err, m) {
			return nil
		}
	}

	return err
}

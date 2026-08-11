package iotwireless

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func networkAnalyzerConfigARN(region, accountID, name string) string {
	return arn.Build("iotwireless", region, accountID, fmt.Sprintf("NetworkAnalyzerConfiguration/%s", name))
}

func copyNetworkAnalyzerConfig(nc *NetworkAnalyzerConfig) *NetworkAnalyzerConfig {
	cp := *nc
	cp.Tags = make(map[string]string, len(nc.Tags))
	maps.Copy(cp.Tags, nc.Tags)
	cp.TraceContent = copyTraceContent(nc.TraceContent)

	if nc.WirelessDevices != nil {
		cp.WirelessDevices = make([]string, len(nc.WirelessDevices))
		copy(cp.WirelessDevices, nc.WirelessDevices)
	}

	if nc.WirelessGateways != nil {
		cp.WirelessGateways = make([]string, len(nc.WirelessGateways))
		copy(cp.WirelessGateways, nc.WirelessGateways)
	}

	if nc.MulticastGroups != nil {
		cp.MulticastGroups = make([]string, len(nc.MulticastGroups))
		copy(cp.MulticastGroups, nc.MulticastGroups)
	}

	return &cp
}

// CreateNetworkAnalyzerConfig creates a new network analyzer configuration.
func (b *InMemoryBackend) CreateNetworkAnalyzerConfig(
	accountID, region, name, description string,
	wirelessDevices, wirelessGateways, multicastGroups []string,
	traceContent *TraceContent,
	tags map[string]string,
) (*NetworkAnalyzerConfig, error) {
	b.mu.Lock("CreateNetworkAnalyzerConfig")
	defer b.mu.Unlock()

	arn := networkAnalyzerConfigARN(region, accountID, name)

	nc := &NetworkAnalyzerConfig{
		Name:             name,
		ARN:              arn,
		Description:      description,
		WirelessDevices:  append([]string(nil), wirelessDevices...),
		WirelessGateways: append([]string(nil), wirelessGateways...),
		MulticastGroups:  append([]string(nil), multicastGroups...),
		TraceContent:     traceContent,
		Tags:             newTagsCopy(tags),
		AccountID:        accountID,
		Region:           region,
	}

	b.networkAnalyzerConfigs.Put(nc)
	b.storeResourceTagsLocked(arn, tags)

	return copyNetworkAnalyzerConfig(nc), nil
}

// GetNetworkAnalyzerConfig returns a network analyzer configuration by name.
func (b *InMemoryBackend) GetNetworkAnalyzerConfig(accountID, region, name string) (*NetworkAnalyzerConfig, error) {
	b.mu.RLock("GetNetworkAnalyzerConfig")
	defer b.mu.RUnlock()

	nc, ok := b.networkAnalyzerConfigs.Get(compositeKey(accountID, region, name))
	if !ok {
		return nil, ErrNetworkAnalyzerConfigNotFound
	}

	return copyNetworkAnalyzerConfig(nc), nil
}

// ListNetworkAnalyzerConfigs returns all network analyzer configurations for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListNetworkAnalyzerConfigs(accountID, region string) []*NetworkAnalyzerConfig {
	b.mu.RLock("ListNetworkAnalyzerConfigs")
	defer b.mu.RUnlock()

	all := b.networkAnalyzerConfigs.All()
	result := make([]*NetworkAnalyzerConfig, 0, len(all))

	for _, nc := range all {
		if nc.AccountID == accountID && nc.Region == region {
			result = append(result, copyNetworkAnalyzerConfig(nc))
		}
	}

	slices.SortFunc(result, func(a, b *NetworkAnalyzerConfig) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return result
}

// DeleteNetworkAnalyzerConfig deletes a network analyzer configuration by name.
func (b *InMemoryBackend) DeleteNetworkAnalyzerConfig(accountID, region, name string) error {
	b.mu.Lock("DeleteNetworkAnalyzerConfig")
	defer b.mu.Unlock()

	key := compositeKey(accountID, region, name)

	nc, ok := b.networkAnalyzerConfigs.Get(key)
	if !ok {
		return ErrNetworkAnalyzerConfigNotFound
	}

	delete(b.resourceTags, nc.ARN)
	b.networkAnalyzerConfigs.Delete(key)

	return nil
}

// UpdateNetworkAnalyzerConfig updates mutable fields on an existing network
// analyzer configuration. traceContent, if non-nil, replaces the stored
// TraceContent wholesale rather than merging field-by-field: unlike
// LoRaWANUpdateDevice, types.TraceContent's fields (LogLevel/
// MulticastFrameInfo/WirelessDeviceFrameInfo) aren't optional pointers, so
// there is no way for a client to express "leave this one sub-field alone".
func (b *InMemoryBackend) UpdateNetworkAnalyzerConfig(
	accountID, region, name, description string,
	wirelessDevices, wirelessGateways []string,
	traceContent *TraceContent,
) error {
	b.mu.Lock("UpdateNetworkAnalyzerConfig")
	defer b.mu.Unlock()

	nc, ok := b.networkAnalyzerConfigs.Get(compositeKey(accountID, region, name))
	if !ok {
		return ErrNetworkAnalyzerConfigNotFound
	}

	nc.Description = description

	if wirelessDevices != nil {
		nc.WirelessDevices = append([]string(nil), wirelessDevices...)
	}

	if wirelessGateways != nil {
		nc.WirelessGateways = append([]string(nil), wirelessGateways...)
	}

	if traceContent != nil {
		nc.TraceContent = traceContent
	}

	return nil
}

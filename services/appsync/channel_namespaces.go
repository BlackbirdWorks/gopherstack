package appsync

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateChannelNamespace creates a channel namespace for an Event API.
func (b *InMemoryBackend) CreateChannelNamespace(
	apiID, name string,
	tagMap map[string]string,
	cfg *ChannelNamespaceConfig,
) (*ChannelNamespace, error) {
	b.mu.Lock("CreateChannelNamespace")
	defer b.mu.Unlock()

	if !b.eventAPIs.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if b.channelNamespaces.Has(channelNamespaceKey(apiID, name)) {
		return nil, fmt.Errorf("%w: channel namespace %s already exists", ErrAlreadyExists, name)
	}

	nsARN := arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("apis/%s/channelNamespaces/%s", apiID, name))

	now := time.Now().Unix()

	ns := &ChannelNamespace{
		APIID:               apiID,
		Name:                name,
		Tags:                tagMap,
		ChannelNamespaceARN: nsARN,
		Created:             now,
		LastModified:        now,
	}

	applyChannelNamespaceConfig(ns, cfg)

	b.channelNamespaces.Put(ns)

	cp := *ns

	return &cp, nil
}

// applyChannelNamespaceConfig applies optional auth/handler config to a ChannelNamespace.
func applyChannelNamespaceConfig(ns *ChannelNamespace, cfg *ChannelNamespaceConfig) {
	if cfg == nil {
		return
	}

	if cfg.CodeHandlers != "" {
		ns.CodeHandlers = cfg.CodeHandlers
	}

	if cfg.PublishAuthModes != nil {
		ns.PublishAuthModes = cfg.PublishAuthModes
	}

	if cfg.SubscribeAuthModes != nil {
		ns.SubscribeAuthModes = cfg.SubscribeAuthModes
	}

	if cfg.HandlerConfigs != nil {
		ns.HandlerConfigs = cfg.HandlerConfigs
	}
}

// GetChannelNamespace returns a channel namespace by API ID and name.
func (b *InMemoryBackend) GetChannelNamespace(apiID, name string) (*ChannelNamespace, error) {
	b.mu.RLock("GetChannelNamespace")
	defer b.mu.RUnlock()

	ns, ok := b.channelNamespaces.Get(channelNamespaceKey(apiID, name))
	if !ok {
		return nil, fmt.Errorf("%w: channel namespace %s not found", ErrNotFound, name)
	}

	cp := *ns

	return &cp, nil
}

// ListChannelNamespaces returns all channel namespaces for an Event API.
func (b *InMemoryBackend) ListChannelNamespaces(apiID string) ([]*ChannelNamespace, error) {
	b.mu.RLock("ListChannelNamespaces")
	defer b.mu.RUnlock()

	if !b.eventAPIs.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	nss := b.channelNamespacesByAPI.Get(apiID)
	out := make([]*ChannelNamespace, 0, len(nss))

	for _, ns := range nss {
		cp := *ns
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *ChannelNamespace) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// UpdateChannelNamespace updates a channel namespace's code handlers, auth modes, and handler configs.
func (b *InMemoryBackend) UpdateChannelNamespace(
	apiID, name string, cfg *ChannelNamespaceConfig,
) (*ChannelNamespace, error) {
	b.mu.Lock("UpdateChannelNamespace")
	defer b.mu.Unlock()

	existing, ok := b.channelNamespaces.Get(channelNamespaceKey(apiID, name))
	if !ok {
		return nil, fmt.Errorf("%w: channel namespace %s not found", ErrNotFound, name)
	}

	applyChannelNamespaceConfig(existing, cfg)

	existing.LastModified = time.Now().Unix()

	cp := *existing

	return &cp, nil
}

// DeleteChannelNamespace removes a channel namespace from an Event API.
func (b *InMemoryBackend) DeleteChannelNamespace(apiID, name string) error {
	b.mu.Lock("DeleteChannelNamespace")
	defer b.mu.Unlock()

	key := channelNamespaceKey(apiID, name)
	if !b.channelNamespaces.Has(key) {
		return fmt.Errorf("%w: channel namespace %s not found", ErrNotFound, name)
	}

	b.channelNamespaces.Delete(key)

	return nil
}

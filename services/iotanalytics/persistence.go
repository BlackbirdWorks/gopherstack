package iotanalytics

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// ErrNoSnapshot is returned when a backend does not support snapshot/restore.
var ErrNoSnapshot = errors.New("backend does not support restore")

// Snapshottable is an optional interface that a StorageBackend may implement to
// support snapshot/restore for persistence or test isolation.
type Snapshottable interface {
	Snapshot(ctx context.Context) []byte
	Restore(context.Context, []byte) error
}

// backendSnapshot holds a serializable snapshot of InMemoryBackend state.
type backendSnapshot struct {
	Channels        map[string]*Channel          `json:"channels"`
	Datastores      map[string]*Datastore        `json:"datastores"`
	Datasets        map[string]*Dataset          `json:"datasets"`
	Pipelines       map[string]*Pipeline         `json:"pipelines"`
	Tags            map[string]map[string]string `json:"tags"`
	ChannelMessages map[string][][]byte          `json:"channelMessages"`
	DatasetContents map[string][]*DatasetContent `json:"datasetContents"`
	LoggingOptions  *LoggingOptions              `json:"loggingOptions"`
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Channels:        b.channels,
		Datastores:      b.datastores,
		Datasets:        b.datasets,
		Pipelines:       b.pipelines,
		Tags:            b.tags,
		ChannelMessages: b.channelMessages,
		DatasetContents: b.datasetContents,
		LoggingOptions:  b.loggingOptions,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "iotanalytics: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore deserializes backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "iotanalytics", data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Channels == nil {
		snap.Channels = make(map[string]*Channel)
	}

	if snap.Datastores == nil {
		snap.Datastores = make(map[string]*Datastore)
	}

	if snap.Datasets == nil {
		snap.Datasets = make(map[string]*Dataset)
	}

	if snap.Pipelines == nil {
		snap.Pipelines = make(map[string]*Pipeline)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}

	if snap.ChannelMessages == nil {
		snap.ChannelMessages = make(map[string][][]byte)
	}

	if snap.DatasetContents == nil {
		snap.DatasetContents = make(map[string][]*DatasetContent)
	}

	b.channels = snap.Channels
	b.datastores = snap.Datastores
	b.datasets = snap.Datasets
	b.pipelines = snap.Pipelines
	b.tags = snap.Tags
	b.channelMessages = snap.ChannelMessages
	b.datasetContents = snap.DatasetContents
	b.loggingOptions = snap.LoggingOptions

	return nil
}

// Snapshot implements persistence by delegating to the backend if it supports it.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return nil
	}

	return s.Snapshot(ctx)
}

// Restore implements persistence by delegating to the backend if it supports it.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return ErrNoSnapshot
	}

	return s.Restore(ctx, data)
}

package persistence

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// MarshalSnapshot serialises v to JSON for a service-state snapshot.
//
// On success it emits a debug record (silent at the default info level) and
// returns the encoded bytes. On failure it logs a warning and returns nil,
// matching the Persistable.Snapshot contract (a nil snapshot is skipped by the
// persistence Manager). Centralising this here keeps every backend's Snapshot
// method consistent and gives the ctx parameter a purpose.
func MarshalSnapshot(ctx context.Context, service string, v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx,
			"persistence: snapshot marshal failed",
			"service", service, "error", err)

		return nil
	}

	logger.Load(ctx).DebugContext(ctx,
		"persistence: snapshot taken",
		"service", service, "bytes", len(data))

	return data
}

// UnmarshalSnapshot decodes a service-state snapshot from data into v.
//
// On success it emits a debug record; on failure it logs a warning and returns
// the decode error. Centralising this here keeps every backend's Restore method
// consistent and gives the ctx parameter a purpose.
func UnmarshalSnapshot(ctx context.Context, service string, data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		logger.Load(ctx).WarnContext(ctx,
			"persistence: snapshot unmarshal failed",
			"service", service, "error", err)

		return err
	}

	logger.Load(ctx).DebugContext(ctx,
		"persistence: snapshot restored",
		"service", service, "bytes", len(data))

	return nil
}

package dmsengine

import (
	"encoding/json"
	"fmt"
	"strings"
)

// TaskSettings is the subset of ReplicationTaskSettings the engine honours.
type TaskSettings struct {
	PrepMode     PrepMode
	TargetSchema string
	CommitRate   int
}

// ParseTaskSettings reads ReplicationTaskSettings JSON; empty input yields the defaults.
func ParseTaskSettings(doc string) (TaskSettings, error) {
	ts := TaskSettings{PrepMode: PrepDropAndCreate, CommitRate: defaultCommitRate}
	if strings.TrimSpace(doc) == "" {
		return ts, nil
	}

	var raw struct {
		TargetMetadata   struct{ TargetSchema string }
		FullLoadSettings struct {
			TargetTablePrepMode string
			CommitRate          int
		}
	}

	if err := json.Unmarshal([]byte(doc), &raw); err != nil {
		return ts, fmt.Errorf("invalid ReplicationTaskSettings: %w", err)
	}

	ts.TargetSchema = raw.TargetMetadata.TargetSchema

	if raw.FullLoadSettings.CommitRate > 0 {
		ts.CommitRate = raw.FullLoadSettings.CommitRate
	}

	switch m := PrepMode(raw.FullLoadSettings.TargetTablePrepMode); m {
	case "":
	case PrepDropAndCreate, PrepTruncateBefore, PrepDoNothing:
		ts.PrepMode = m
	default:
		return ts, fmt.Errorf("invalid FullLoadSettings.TargetTablePrepMode %q", m)
	}

	return ts, nil
}

const defaultCommitRate = 10000

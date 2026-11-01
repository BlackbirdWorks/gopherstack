package dmsengine

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidSettings reports an unusable ReplicationTaskSettings document.
var ErrInvalidSettings = errors.New("invalid ReplicationTaskSettings")

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
		TargetMetadata struct {
			TargetSchema string `json:"TargetSchema"`
		} `json:"TargetMetadata"`
		FullLoadSettings struct {
			TargetTablePrepMode string `json:"TargetTablePrepMode"`
			CommitRate          int    `json:"CommitRate"`
		} `json:"FullLoadSettings"`
	}

	if err := json.Unmarshal([]byte(doc), &raw); err != nil {
		return ts, fmt.Errorf("%w: %w", ErrInvalidSettings, err)
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
		return ts, fmt.Errorf("%w: FullLoadSettings.TargetTablePrepMode %q", ErrInvalidSettings, m)
	}

	return ts, nil
}

const defaultCommitRate = 10000

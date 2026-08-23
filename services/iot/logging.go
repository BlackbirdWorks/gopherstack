package iot

import (
	"fmt"
	"maps"
)

// V2LoggingOptions holds the account-level V2 logging configuration.
type V2LoggingOptions struct {
	DefaultLogLevel     string                    `json:"defaultLogLevel"`
	RoleARN             string                    `json:"roleArn,omitempty"`
	EventConfigurations []LogEventConfigurationV2 `json:"eventConfigurations,omitempty"`
	DisableAllLogs      bool                      `json:"disableAllLogs"`
}

// LogEventConfigurationV2 is one per-event-type logging override
// (types.LogEventConfiguration, aws-sdk-go-v2/service/iot@v1.77.4).
type LogEventConfigurationV2 struct {
	EventType      string `json:"eventType"`
	LogDestination string `json:"logDestination,omitempty"`
	LogLevel       string `json:"logLevel,omitempty"`
}

// V2LoggingLevel holds a per-target logging level.
type V2LoggingLevel struct {
	Target   map[string]any `json:"logTarget"`
	LogLevel string         `json:"logLevel"`
}

func v2LogLevelKey(target map[string]any) string {
	targetType, _ := target["targetType"].(string)
	targetName, _ := target["targetName"].(string)

	return targetType + "/" + targetName
}

func cloneV2LogLevel(l *V2LoggingLevel) *V2LoggingLevel {
	cp := *l
	cp.Target = make(map[string]any, len(l.Target))
	maps.Copy(cp.Target, l.Target)

	return &cp
}

func (b *InMemoryBackend) GetV2LoggingOptions() *V2LoggingOptions {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.v2LoggingOptions == nil {
		return &V2LoggingOptions{DefaultLogLevel: "DISABLED"}
	}
	cp := *b.v2LoggingOptions

	return &cp
}

func (b *InMemoryBackend) SetV2LoggingOptions(
	roleARN, defaultLogLevel string, disableAllLogs bool, eventConfigurations []LogEventConfigurationV2,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.v2LoggingOptions = &V2LoggingOptions{
		RoleARN:             roleARN,
		DefaultLogLevel:     defaultLogLevel,
		DisableAllLogs:      disableAllLogs,
		EventConfigurations: eventConfigurations,
	}

	return nil
}

func (b *InMemoryBackend) SetV2LoggingLevel(target map[string]any, logLevel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	tgt := make(map[string]any, len(target))
	maps.Copy(tgt, target)
	b.v2LoggingLevels.Put(&V2LoggingLevel{Target: tgt, LogLevel: logLevel})

	return nil
}

func (b *InMemoryBackend) DeleteV2LoggingLevel(target map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := v2LogLevelKey(target)
	if !b.v2LoggingLevels.Has(key) {
		return fmt.Errorf("V2 logging level %q not found: %w", key, ErrResourceNotFound)
	}
	b.v2LoggingLevels.Delete(key)

	return nil
}

func (b *InMemoryBackend) ListV2LoggingLevels() []*V2LoggingLevel {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.v2LoggingLevels.Snapshot()
	out := make([]*V2LoggingLevel, 0, len(items))
	for _, v := range items {
		out = append(out, cloneV2LogLevel(v))
	}

	return out
}

// LoggingOptions holds the IoT account-level logging configuration.
type LoggingOptions struct {
	RoleARN  string `json:"roleArn"`
	LogLevel string `json:"logLevel"`
}

func (b *InMemoryBackend) GetLoggingOptions() *LoggingOptions {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.loggingOptions == nil {
		return &LoggingOptions{LogLevel: "DISABLED"}
	}
	cp := *b.loggingOptions

	return &cp
}

func (b *InMemoryBackend) SetLoggingOptions(roleARN, logLevel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.loggingOptions = &LoggingOptions{RoleARN: roleARN, LogLevel: logLevel}

	return nil
}

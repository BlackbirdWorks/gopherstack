package networkfirewall

import (
	"context"
	"maps"
	"slices"
	"time"
)

// DescribeLoggingConfigurationInput is the DescribeLoggingConfiguration request.
type DescribeLoggingConfigurationInput = DescribeFirewallInput

// LoggingOutput is the DescribeLoggingConfiguration response.
type LoggingOutput struct {
	EnableMonitoringDashboard *bool                `json:"EnableMonitoringDashboard,omitempty"`
	FirewallArn               string               `json:"FirewallArn"`
	LoggingConfiguration      LoggingConfiguration `json:"LoggingConfiguration"`
}

// UpdateLoggingConfigurationInput is the UpdateLoggingConfiguration request.
type UpdateLoggingConfigurationInput struct {
	EnableMonitoringDashboard *bool                 `json:"EnableMonitoringDashboard"`
	LoggingConfiguration      *LoggingConfiguration `json:"LoggingConfiguration"`
	FirewallArn               string                `json:"FirewallArn"`
	FirewallName              string                `json:"FirewallName"`
}

// UpdateLoggingConfigurationOutput is the UpdateLoggingConfiguration response.
type UpdateLoggingConfigurationOutput struct {
	FirewallName string `json:"FirewallName"`
	LoggingOutput
}

var logDestinationKeys = map[string]string{ //nolint:gochecknoglobals // static AWS contract
	"S3":                  "bucketName",
	"CloudWatchLogs":      "logGroup",
	"KinesisDataFirehose": "deliveryStream",
}

var logTypes = []string{"ALERT", "FLOW", "TLS"} //nolint:gochecknoglobals // static AWS contract

func validateLoggingConfiguration(c *LoggingConfiguration) error {
	seen := map[string]bool{}

	for _, d := range c.LogDestinationConfigs {
		key, ok := logDestinationKeys[d.LogDestinationType]
		if !ok {
			return invalid("LogDestinationType %q is not valid", d.LogDestinationType)
		}

		if !slices.Contains(logTypes, d.LogType) {
			return invalid("LogType %q is not valid", d.LogType)
		}

		if d.LogDestination[key] == "" {
			return invalid("LogDestination must contain %s for %s", key, d.LogDestinationType)
		}

		if seen[d.LogType] {
			return invalid("LogType %s is configured more than once", d.LogType)
		}

		seen[d.LogType] = true
	}

	return nil
}

// loggingDelta counts destination configs added, removed or changed between two configurations.
func loggingDelta(oldCfg, newCfg []LogDestinationConfig) int {
	byType := make(map[string]LogDestinationConfig, len(oldCfg))
	for _, d := range oldCfg {
		byType[d.LogType] = d
	}

	changes := 0

	for _, d := range newCfg {
		prev, ok := byType[d.LogType]
		if !ok || prev.LogDestinationType != d.LogDestinationType ||
			!maps.Equal(prev.LogDestination, d.LogDestination) {
			changes++
		}

		delete(byType, d.LogType)
	}

	return changes + len(byType)
}

func loggingView(r *firewallRecord) LoggingOutput {
	out := LoggingOutput{
		FirewallArn:          r.ARN,
		LoggingConfiguration: LoggingConfiguration{LogDestinationConfigs: []LogDestinationConfig{}},
	}
	if r.Logging != nil {
		out.LoggingConfiguration.LogDestinationConfigs = slices.Clone(r.Logging.LogDestinationConfigs)
	}

	dash := r.MonitoringDashboard
	out.EnableMonitoringDashboard = &dash

	return out
}

// DescribeLoggingConfiguration returns a firewall's log destinations.
func (b *InMemoryBackend) DescribeLoggingConfiguration(
	_ context.Context, in *DescribeLoggingConfigurationInput,
) (*LoggingOutput, error) {
	b.mu.RLock("DescribeLoggingConfiguration")
	defer b.mu.RUnlock()

	r, err := b.findFirewallLocked(in.FirewallArn, in.FirewallName, time.Now())
	if err != nil {
		return nil, err
	}

	out := loggingView(r)

	return &out, nil
}

// UpdateLoggingConfiguration changes one log destination at a time, as AWS requires.
func (b *InMemoryBackend) UpdateLoggingConfiguration(
	_ context.Context, in *UpdateLoggingConfigurationInput,
) (*UpdateLoggingConfigurationOutput, error) {
	if in.LoggingConfiguration != nil {
		if err := validateLoggingConfiguration(in.LoggingConfiguration); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("UpdateLoggingConfiguration")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepLocked(now)

	r, err := b.findFirewallLocked(in.FirewallArn, in.FirewallName, now)
	if err != nil {
		return nil, err
	}

	if in.LoggingConfiguration != nil {
		var current []LogDestinationConfig
		if r.Logging != nil {
			current = r.Logging.LogDestinationConfigs
		}

		if loggingDelta(current, in.LoggingConfiguration.LogDestinationConfigs) > 1 {
			return nil, invalid("You can change only one LogDestinationConfig at a time")
		}

		cfg := LoggingConfiguration{LogDestinationConfigs: slices.Clone(in.LoggingConfiguration.LogDestinationConfigs)}
		r.Logging = &cfg
	}

	if in.EnableMonitoringDashboard != nil {
		r.MonitoringDashboard = *in.EnableMonitoringDashboard
	}

	return &UpdateLoggingConfigurationOutput{LoggingOutput: loggingView(r), FirewallName: r.Name}, nil
}

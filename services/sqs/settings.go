package sqs

// Settings holds service-level configuration for the SQS backend. The env names
// match LocalStack's; each default keeps AWS-accurate behaviour.
type Settings struct {
	DelayPurgeRetry         bool `name:"delay-purge-retry"          env:"SQS_DELAY_PURGE_RETRY"               default:"true"  help:"Return PurgeQueueInProgress for a second PurgeQueue within 60 seconds."`               //nolint:lll // config struct tags are intentionally verbose
	DelayRecentlyDeleted    bool `name:"delay-recently-deleted"     env:"SQS_DELAY_RECENTLY_DELETED"          default:"true"  help:"Return QueueDeletedRecently when a queue is recreated within 60 seconds of deletion."` //nolint:lll // config struct tags are intentionally verbose
	EnableRetentionPeriod   bool `name:"enable-retention-period"    env:"SQS_ENABLE_MESSAGE_RETENTION_PERIOD" default:"true"  help:"Expire messages once they exceed the queue MessageRetentionPeriod."`                   //nolint:lll // config struct tags are intentionally verbose
	DisableCloudWatchMetric bool `name:"disable-cloudwatch-metrics" env:"SQS_DISABLE_CLOUDWATCH_METRICS"      default:"false" help:"Do not publish AWS/SQS metrics to CloudWatch."`                                        //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the AWS-accurate defaults for use without Kong.
func DefaultSettings() Settings {
	return Settings{DelayPurgeRetry: true, DelayRecentlyDeleted: true, EnableRetentionPeriod: true}
}

// SettingsProvider is implemented by config objects that supply SQS settings.
type SettingsProvider interface {
	GetSQSSettings() Settings
}

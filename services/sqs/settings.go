package sqs

// Settings holds service-level configuration for the SQS backend. The zero value
// keeps AWS-accurate behaviour; every field switches one behaviour off.
type Settings struct {
	DisablePurgeCooldown       bool `name:"disable-purge-cooldown"        env:"GOPHERSTACK_SQS_DISABLE_PURGE_COOLDOWN"                                    default:"false" help:"Allow PurgeQueue within 60 seconds of the previous purge (LocalStack SQS_DELAY_PURGE_RETRY=0)."`       //nolint:lll // config struct tags are intentionally verbose
	DisableRecentlyDeletedWait bool `name:"disable-recently-deleted-wait" env:"GOPHERSTACK_SQS_DISABLE_RECENTLY_DELETED_COOLDOWN"                         default:"false" help:"Allow recreating a queue within 60 seconds of deleting it (LocalStack SQS_DELAY_RECENTLY_DELETED=0)."` //nolint:lll // config struct tags are intentionally verbose
	DisableMessageRetention    bool `name:"disable-message-retention"     env:"GOPHERSTACK_SQS_DISABLE_MESSAGE_RETENTION"                                 default:"false" help:"Never expire messages by MessageRetentionPeriod (LocalStack SQS_ENABLE_MESSAGE_RETENTION_PERIOD=0)."`  //nolint:lll // config struct tags are intentionally verbose
	DisableCloudWatchMetrics   bool `name:"disable-cloudwatch-metrics"    env:"SQS_DISABLE_CLOUDWATCH_METRICS,GOPHERSTACK_SQS_DISABLE_CLOUDWATCH_METRICS" default:"false" help:"Do not publish AWS/SQS metrics to CloudWatch."`                                                        //nolint:lll // config struct tags are intentionally verbose
}

// SettingsProvider is implemented by config objects that supply SQS settings.
type SettingsProvider interface {
	GetSQSSettings() Settings
}

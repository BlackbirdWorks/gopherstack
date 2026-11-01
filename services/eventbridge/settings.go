package eventbridge

// Settings holds service-level configuration for the EventBridge backend.
type Settings struct {
	// AllowScheduleOnCustomBus lets PutRule accept a ScheduleExpression on a
	// non-default event bus. Real AWS rejects it, so the default is false.
	AllowScheduleOnCustomBus bool `json:"allow_schedule_on_custom_bus" name:"allow-schedule-on-custom-bus" env:"EVENTBRIDGE_ALLOW_SCHEDULE_ON_CUSTOM_BUS" default:"false" help:"Accept scheduled rules on custom event buses (real AWS allows them only on the default bus)."` //nolint:lll // Kong struct tag makes this line long
}

// SetSettings applies s to the backend; call before serving requests.
func (b *InMemoryBackend) SetSettings(s Settings) { b.settings = s }

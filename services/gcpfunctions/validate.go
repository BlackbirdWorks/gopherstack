package gcpfunctions

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"cloud.google.com/go/functions/apiv2/functionspb"
)

const (
	// EventTypePubSub is the Pub/Sub event type.
	EventTypePubSub = "google.cloud.pubsub.topic.v1.messagePublished"

	maxTimeout    = 3600
	topicNameLen  = 4
	defaultMemory = "256M"
	defaultTO     = 60
	defaultMaxIns = 100
)

var (
	memoryRe  = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(k|M|G|T|Ki|Mi|Gi|Ti)?$`)
	envNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

func reservedEnv(name string) bool {
	switch name {
	case "PORT", "K_SERVICE", "K_REVISION", "K_CONFIGURATION", "FUNCTION_TARGET", "FUNCTION_SIGNATURE_TYPE":
		return true
	}

	return false
}

func validateEnvVars(vars map[string]string) error {
	for k := range vars {
		if !envNameRe.MatchString(k) {
			return errInvalid("Environment variable name %q is invalid.", k)
		}

		if reservedEnv(k) {
			return errInvalid("The following reserved env names were provided: %s.", k)
		}
	}

	return nil
}

func validateBuild(bc *functionspb.BuildConfig) error {
	if bc == nil {
		return errInvalid("build_config is required.")
	}

	if _, err := lookupRuntime(bc.GetRuntime()); err != nil {
		return err
	}

	if bc.GetEntryPoint() == "" {
		return errInvalid("build_config.entry_point is required.")
	}

	if bc.GetSource() == nil {
		return errInvalid("build_config.source is required.")
	}

	return validateEnvVars(bc.GetEnvironmentVariables())
}

func validateService(sc *functionspb.ServiceConfig) error {
	if sc == nil {
		return nil
	}

	if t := sc.GetTimeoutSeconds(); t < 0 || t > maxTimeout {
		return errInvalid("service_config.timeout_seconds must be between 1 and %d.", maxTimeout)
	}

	if m := sc.GetAvailableMemory(); m != "" && !memoryRe.MatchString(m) {
		return errInvalid("service_config.available_memory %q is not a valid quantity (for example 256M or 1Gi).", m)
	}

	if sc.GetMinInstanceCount() < 0 || sc.GetMaxInstanceCount() < 0 || sc.GetMaxInstanceRequestConcurrency() < 0 {
		return errInvalid("Instance counts and concurrency must not be negative.")
	}

	if mx := sc.GetMaxInstanceCount(); mx > 0 && sc.GetMinInstanceCount() > mx {
		return errInvalid("service_config.min_instance_count must not exceed max_instance_count.")
	}

	if err := validateResources(sc); err != nil {
		return err
	}

	if err := validateSecrets(sc); err != nil {
		return err
	}

	return validateEnvVars(sc.GetEnvironmentVariables())
}

func (b *InMemoryBackend) validateTrigger(et *functionspb.EventTrigger) error {
	if et == nil {
		return nil
	}

	switch typ := et.GetEventType(); {
	case typ == EventTypePubSub:
		return b.validatePubSubTrigger(et)
	case isStorageType(typ):
		return b.validateStorageTrigger(et)
	case isFirestoreType(typ):
		return validateFirestoreTrigger(et)
	default:
		return errInvalid("Event type %q is not supported by the emulator; supported: %s.", typ, supportedEventTypes())
	}
}

func (b *InMemoryBackend) validatePubSubTrigger(et *functionspb.EventTrigger) error {
	if len(et.GetEventFilters()) > 0 {
		return errInvalid("event_filters are not supported for Pub/Sub triggers; use pubsub_topic.")
	}

	if parts := strings.Split(et.GetPubsubTopic(), "/"); len(parts) != topicNameLen || parts[0] != "projects" ||
		parts[2] != "topics" || parts[1] == "" || parts[3] == "" {
		return errInvalid("event_trigger.pubsub_topic must be projects/{project}/topics/{topic}.")
	}

	b.mu.RLock("validateTrigger")
	bus := b.events
	b.mu.RUnlock()

	if bus == nil {
		return errPrecondition("Event triggers need the Pub/Sub emulator, which is not available.")
	}

	if _, err := bus.GetTopic(et.GetPubsubTopic()); err != nil {
		return errInvalid("Pub/Sub topic %q does not exist.", et.GetPubsubTopic())
	}

	return nil
}

func (b *InMemoryBackend) validateFunction(fn *functionspb.Function) error {
	if err := validateBuild(fn.GetBuildConfig()); err != nil {
		return err
	}

	if err := validateService(fn.GetServiceConfig()); err != nil {
		return err
	}

	return b.validateTrigger(fn.GetEventTrigger())
}

// applyDefaults fills unset service_config fields with the Cloud Functions defaults.
func applyDefaults(sc *functionspb.ServiceConfig) *functionspb.ServiceConfig {
	if sc == nil {
		sc = &functionspb.ServiceConfig{}
	}

	if sc.GetAvailableMemory() == "" {
		sc.AvailableMemory = defaultMemory
	}

	if sc.GetAvailableCpu() == "" {
		sc.AvailableCpu = defaultCPUFor(sc.GetAvailableMemory())
	}

	if sc.GetTimeoutSeconds() == 0 {
		sc.TimeoutSeconds = defaultTO
	}

	if sc.GetMaxInstanceCount() == 0 {
		sc.MaxInstanceCount = defaultMaxIns
	}

	if sc.GetMaxInstanceRequestConcurrency() == 0 {
		sc.MaxInstanceRequestConcurrency = 1
	}

	if sc.GetIngressSettings() == functionspb.ServiceConfig_INGRESS_SETTINGS_UNSPECIFIED {
		sc.IngressSettings = functionspb.ServiceConfig_ALLOW_ALL
	}

	sc.AllTrafficOnLatestRevision = true

	return sc
}

func revisionName(id string, seq int, uid string) string {
	return fmt.Sprintf("%s-%05d-%s", id, seq, shortHash(uid, strconv.Itoa(seq))[:3])
}

package gcpcloudtasks

import (
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
)

// UpdateQueue updates the masked fields of a queue, creating it when it does not exist.
func (b *InMemoryBackend) UpdateQueue(req *cloudtaskspb.UpdateQueueRequest) (*cloudtaskspb.Queue, error) {
	if req.GetQueue() == nil {
		return nil, errInvalid("queue must be specified.")
	}

	in := cloneQueue(req.GetQueue())

	if _, err := parseQueueName(in.GetName()); err != nil {
		return nil, err
	}

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = []string{
			"rate_limits",
			"retry_config",
			"http_target",
			"app_engine_routing_override",
			"stackdriver_logging_config",
		}
	}

	b.mu.Lock("UpdateQueue")
	defer b.mu.Unlock()

	q, ok := b.queues[in.GetName()]
	if !ok {
		var err error
		if q, err = b.newQueueLocked(in.GetName(), &cloudtaskspb.Queue{}); err != nil {
			return nil, err
		}
	}

	next := cloneQueue(q.pb)
	for _, p := range paths {
		if err := applyQueuePath(next, in, p); err != nil {
			if !ok {
				delete(b.queues, in.GetName())
			}

			return nil, err
		}
	}

	if err := normalizeQueue(next); err != nil {
		if !ok {
			delete(b.queues, in.GetName())
		}

		return nil, err
	}

	q.pb = next

	return cloneQueue(next), nil
}

func applyQueuePath(dst, src *cloudtaskspb.Queue, path string) error {
	switch path {
	case "rate_limits":
		dst.RateLimits = src.GetRateLimits()
	case "rate_limits.max_dispatches_per_second":
		dst.RateLimits.MaxDispatchesPerSecond = src.GetRateLimits().GetMaxDispatchesPerSecond()
	case "rate_limits.max_concurrent_dispatches":
		dst.RateLimits.MaxConcurrentDispatches = src.GetRateLimits().GetMaxConcurrentDispatches()
	case "retry_config":
		dst.RetryConfig = src.GetRetryConfig()
	case "retry_config.max_attempts":
		dst.RetryConfig.MaxAttempts = src.GetRetryConfig().GetMaxAttempts()
	case "retry_config.max_retry_duration":
		dst.RetryConfig.MaxRetryDuration = src.GetRetryConfig().GetMaxRetryDuration()
	case "retry_config.min_backoff":
		dst.RetryConfig.MinBackoff = src.GetRetryConfig().GetMinBackoff()
	case "retry_config.max_backoff":
		dst.RetryConfig.MaxBackoff = src.GetRetryConfig().GetMaxBackoff()
	case "retry_config.max_doublings":
		dst.RetryConfig.MaxDoublings = src.GetRetryConfig().GetMaxDoublings()
	case "http_target":
		dst.HttpTarget = src.GetHttpTarget()
	case "app_engine_routing_override":
		dst.AppEngineRoutingOverride = src.GetAppEngineRoutingOverride()
	case "stackdriver_logging_config":
		dst.StackdriverLoggingConfig = src.GetStackdriverLoggingConfig()
	case "stackdriver_logging_config.sampling_ratio":
		dst.StackdriverLoggingConfig = src.GetStackdriverLoggingConfig()
	default:
		return errInvalid("update_mask path %q is not updatable.", path)
	}

	return nil
}

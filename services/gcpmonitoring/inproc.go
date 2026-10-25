package gcpmonitoring

import "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"

// API groups the Monitoring services for in-process callers (dashboard proxy).
type API struct {
	Metrics  monitoringpb.MetricServiceServer
	Alerts   monitoringpb.AlertPolicyServiceServer
	Uptime   monitoringpb.UptimeCheckServiceServer
	Snoozes  monitoringpb.SnoozeServiceServer
	Channels monitoringpb.NotificationChannelServiceServer
}

// API exposes the metric, alert policy, uptime check, snooze and notification channel services.
func (h *Handler) API() *API {
	b := h.Backend

	return &API{
		Metrics:  &metricServer{b: b},
		Alerts:   &alertServer{b: b},
		Uptime:   &uptimeServer{b: b},
		Snoozes:  &snoozeServer{b: b},
		Channels: &channelServer{b: b},
	}
}

package gcp_test

import "testing"

const monitoringFixture = `
resource "google_pubsub_topic" "alerts" {
  name = "tf-monitoring-alerts"
}

resource "google_monitoring_notification_channel" "pubsub" {
  display_name = "tf pubsub channel"
  type         = "pubsub"

  labels = {
    topic = google_pubsub_topic.alerts.id
  }
}

resource "google_monitoring_notification_channel" "hook" {
  display_name = "tf webhook channel"
  type         = "webhook_tokenauth"

  labels = {
    url = "https://example.com/hook"
  }

  sensitive_labels {
    auth_token = "tf-token"
  }
}

resource "google_monitoring_alert_policy" "cpu" {
  display_name = "tf high cpu"
  combiner     = "OR"

  notification_channels = [
    google_monitoring_notification_channel.pubsub.name,
    google_monitoring_notification_channel.hook.name,
  ]

  conditions {
    display_name = "cpu above 80 percent"

    condition_threshold {
      filter          = "metric.type=\"compute.googleapis.com/instance/cpu/utilization\""
      duration        = "60s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0.8

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }

      trigger {
        count = 1
      }
    }
  }

  documentation {
    content   = "CPU is hot"
    mime_type = "text/markdown"
  }

  user_labels = {
    env = "test"
  }
}

resource "google_monitoring_uptime_check_config" "http" {
  display_name = "tf http check"
  timeout      = "10s"
  period       = "300s"

  http_check {
    path = "/v3/projects/tf/monitoredResourceDescriptors"
    port = 8132
  }

  monitored_resource {
    type = "uptime_url"

    labels = {
      project_id = "gopherstack-test"
      host       = "localhost"
    }
  }

  content_matchers {
    content = "gce_instance"
    matcher = "CONTAINS_STRING"
  }
}
`

func TestTerraformGCP_Monitoring(t *testing.T) {
	t.Parallel()

	newRunner(t, monitoringFixture).lifecycle()
}

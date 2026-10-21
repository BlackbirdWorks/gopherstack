package gcp_test

import "testing"

const loggingFixture = `
resource "google_pubsub_topic" "logs" {
  name = "tf-log-topic"
}

resource "google_logging_project_sink" "errors" {
  name                   = "tf-errors-sink"
  destination            = "pubsub.googleapis.com/${google_pubsub_topic.logs.id}"
  filter                 = "severity >= ERROR"
  unique_writer_identity = true
}

resource "google_logging_project_sink" "archive" {
  name        = "tf-archive-sink"
  destination = "storage.googleapis.com/tf-log-archive"
  filter      = "resource.type = \"gce_instance\""
  description = "archive of vm logs"
}

resource "google_logging_metric" "errors" {
  name        = "tf-error-count"
  description = "Count of error entries"
  filter      = "severity >= ERROR"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"

    labels {
      key         = "service"
      value_type  = "STRING"
      description = "service name"
    }
  }

  label_extractors = {
    "service" = "EXTRACT(jsonPayload.service)"
  }
}

resource "google_logging_metric" "latency" {
  name   = "tf-latency"
  filter = "jsonPayload.latency:*"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "ms"
  }

  value_extractor = "EXTRACT(jsonPayload.latency)"

  bucket_options {
    linear_buckets {
      num_finite_buckets = 4
      width              = 25
      offset             = 0
    }
  }
}

resource "google_logging_project_exclusion" "debug" {
  name        = "tf-drop-debug"
  description = "Drop debug logs"
  filter      = "severity <= DEBUG"
}
`

func TestTerraformGCP_Logging(t *testing.T) {
	t.Parallel()

	newRunner(t, loggingFixture).lifecycle()
}

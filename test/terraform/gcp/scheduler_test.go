package gcp_test

import "testing"

const schedulerFixture = `
resource "google_cloud_scheduler_job" "j" {
  name      = "tf-job"
  region    = "us-central1"
  schedule  = "0 * * * *"
  time_zone = "Etc/UTC"

  retry_config {
    retry_count          = 0
    max_retry_duration   = "0s"
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 5
  }

  http_target {
    uri         = "http://example.invalid/hook"
    http_method = "POST"
    body        = base64encode("{\"ping\":true}")
  }
}
`

func TestTerraformGCP_Scheduler(t *testing.T) {
	t.Parallel()

	newRunner(t, schedulerFixture).lifecycle()
}

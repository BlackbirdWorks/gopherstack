package gcp_test

import "testing"

const cloudTasksFixture = `
resource "google_cloud_tasks_queue" "q" {
  name     = "tf-queue"
  location = "us-central1"

  rate_limits {
    max_dispatches_per_second = 10
    max_concurrent_dispatches = 5
  }

  retry_config {
    max_attempts = 3
  }
}
`

func TestTerraformGCP_CloudTasks(t *testing.T) {
	t.Parallel()

	newRunner(t, cloudTasksFixture).lifecycle()
}

package gcp_test

import "testing"

const pubSubFixture = `
resource "google_pubsub_topic" "t" {
  name   = "tf-topic"
  labels = { env = "test" }
}

resource "google_pubsub_subscription" "s" {
  name                 = "tf-sub"
  topic                = google_pubsub_topic.t.id
  ack_deadline_seconds = 20
  labels               = { env = "test" }
}
`

func TestTerraformGCP_PubSub(t *testing.T) {
	t.Parallel()

	newRunner(t, pubSubFixture).lifecycle()
}

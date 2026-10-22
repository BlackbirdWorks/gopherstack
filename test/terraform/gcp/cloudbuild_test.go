package gcp_test

import "testing"

// The gopherstack container has no container runtime socket, so triggers are created and read back but not run.
const cloudBuildFixture = `
resource "google_cloudbuild_trigger" "repo" {
  name        = "tf-repo-trigger"
  description = "terraform repo trigger"
  location    = "global"
  tags        = ["terraform", "ci"]

  trigger_template {
    repo_name   = "tf-repo"
    branch_name = "main"
  }

  substitutions = {
    _ENV = "test"
  }

  build {
    timeout = "900s"

    step {
      name = "busybox"
      id   = "greet"
      args = ["echo", "hello $${_ENV}"]
    }

    step {
      name     = "busybox"
      args     = ["true"]
      wait_for = ["greet"]
    }

    tags = ["from-terraform"]
  }
}

resource "google_cloudbuild_trigger" "pubsub" {
  name     = "tf-pubsub-trigger"
  location = "global"

  pubsub_config {
    topic = "projects/gopherstack-test/topics/tf-build-events"
  }

  build {
    step {
      name = "alpine"
      args = ["echo", "pubsub"]
    }
  }
}

output "repo_trigger_id" {
  value = google_cloudbuild_trigger.repo.trigger_id
}
`

func TestTerraformGCP_CloudBuild(t *testing.T) {
	t.Parallel()

	newRunner(t, cloudBuildFixture).lifecycle()
}

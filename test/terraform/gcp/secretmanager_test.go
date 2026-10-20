package gcp_test

import "testing"

const secretManagerFixture = `
resource "google_secret_manager_secret" "s" {
  secret_id = "tf-secret"
  labels    = { env = "test" }

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "v" {
  secret      = google_secret_manager_secret.s.id
  secret_data = "s3cr3t"
}
`

func TestTerraformGCP_SecretManager(t *testing.T) {
	t.Parallel()

	newRunner(t, secretManagerFixture).lifecycle()
}

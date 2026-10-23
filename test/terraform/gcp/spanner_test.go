package gcp_test

import "testing"

// The gopherstack container has no container runtime socket, so Spanner serves instance and database
// metadata only here: the resources round-trip through the admin APIs without the emulator data plane.
const spannerFixture = `
resource "google_spanner_instance" "main" {
  name             = "tf-spanner"
  config           = "regional-us-central1"
  display_name     = "terraform spanner"
  processing_units = 100

  labels = {
    env = "test"
  }
}

resource "google_spanner_database" "app" {
  instance            = google_spanner_instance.main.name
  name                = "appdb"
  deletion_protection = false

  ddl = [
    "CREATE TABLE singers (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY (id)",
    "CREATE INDEX singers_by_name ON singers(name)",
  ]
}

resource "google_spanner_instance" "nodes" {
  name         = "tf-spanner-nodes"
  config       = "regional-us-central1"
  display_name = "terraform nodes"
  num_nodes    = 1
}
`

func TestTerraformGCP_Spanner(t *testing.T) {
	t.Parallel()

	newRunner(t, spannerFixture).lifecycle()
}

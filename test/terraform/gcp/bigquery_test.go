package gcp_test

import "testing"

const bigQueryFixture = `
resource "google_bigquery_dataset" "d" {
  dataset_id                 = "gopherstack_tf"
  location                   = "US"
  delete_contents_on_destroy = true
  labels = {
    env = "test"
  }
}

resource "google_bigquery_table" "t" {
  dataset_id          = google_bigquery_dataset.d.dataset_id
  table_id            = "events"
  deletion_protection = false
  schema = jsonencode([
    { name = "id", type = "INTEGER", mode = "REQUIRED" },
    { name = "name", type = "STRING", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "v" {
  dataset_id          = google_bigquery_dataset.d.dataset_id
  table_id            = "events_view"
  deletion_protection = false
  view {
    query          = "SELECT id, name FROM gopherstack_tf.events"
    use_legacy_sql = false
  }
  depends_on = [google_bigquery_table.t]
}
`

func TestTerraformGCP_BigQuery(t *testing.T) {
	t.Parallel()

	newRunner(t, bigQueryFixture).lifecycle()
}

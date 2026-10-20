package gcp_test

import "testing"

const storageFixture = `
resource "google_storage_bucket" "b" {
  name                        = "gopherstack-tf-bucket"
  location                    = "US"
  force_destroy               = true
  uniform_bucket_level_access = true
  labels = {
    env = "test"
  }
  versioning {
    enabled = true
  }
  cors {
    origin          = ["https://example.com"]
    method          = ["GET", "PUT"]
    response_header = ["Content-Type"]
    max_age_seconds = 3600
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
}

resource "google_storage_bucket_iam_member" "viewer" {
  bucket = google_storage_bucket.b.name
  role   = "roles/storage.objectViewer"
  member = "allUsers"
}

resource "google_storage_bucket_object" "o" {
  name         = "dir/hello.txt"
  bucket       = google_storage_bucket.b.name
  content      = "hello gopherstack"
  content_type = "text/plain"
}
`

func TestTerraformGCP_Storage(t *testing.T) {
	t.Parallel()

	newRunner(t, storageFixture).lifecycle()
}

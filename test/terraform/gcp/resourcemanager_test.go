package gcp_test

import "testing"

const resourceManagerFixture = `
data "google_organization" "org" {
  domain = "gopherstack.local"
}

resource "google_folder" "team" {
  display_name        = "Team A"
  parent              = data.google_organization.org.name
  deletion_protection = false
}

resource "google_project" "app" {
  name                = "App Project"
  project_id          = "tf-app-project"
  folder_id           = google_folder.team.name
  auto_create_network = false
  deletion_policy     = "DELETE"

  labels = {
    env = "test"
  }
}

resource "google_project_service" "pubsub" {
  project            = google_project.app.project_id
  service            = "pubsub.googleapis.com"
  disable_on_destroy = true
}

resource "google_project_service" "run" {
  project            = google_project.app.project_id
  service            = "run.googleapis.com"
  disable_on_destroy = true
}

resource "google_project_iam_member" "viewer" {
  project = google_project.app.project_id
  role    = "roles/viewer"
  member  = "user:alice@example.com"
}

data "google_project" "app" {
  project_id = google_project.app.project_id

  depends_on = [google_project_service.pubsub, google_project_iam_member.viewer]
}

output "org_id" {
  value = data.google_organization.org.org_id
}

output "project_number" {
  value = data.google_project.app.number
}

output "project_name" {
  value = data.google_project.app.name
}

output "folder_name" {
  value = google_folder.team.name
}
`

func TestTerraformGCP_ResourceManagerAndServiceUsage(t *testing.T) {
	t.Parallel()

	r := newRunner(t, resourceManagerFixture)

	r.lifecycleThen(func() {
		if got := r.output("org_id"); got != "123456789012" {
			t.Fatalf("org id = %q, want 123456789012", got)
		}

		if got := r.output("project_name"); got != "App Project" {
			t.Fatalf("project name = %q, want App Project", got)
		}

		if got := r.output("project_number"); len(got) != 12 {
			t.Fatalf("project number = %q, want 12 digits", got)
		}
	})
}

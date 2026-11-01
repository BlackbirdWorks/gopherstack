package gcp_test

import "testing"

const computeFixture = `
resource "google_compute_network" "a" {
  name                    = "tf-peer-a"
  auto_create_subnetworks = false
}

resource "google_compute_network" "b" {
  name                    = "tf-peer-b"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "a" {
  name          = "tf-peer-a-sub"
  region        = "us-central1"
  network       = google_compute_network.a.id
  ip_cidr_range = "10.10.0.0/24"
}

resource "google_compute_subnetwork" "b" {
  name          = "tf-peer-b-sub"
  region        = "us-central1"
  network       = google_compute_network.b.id
  ip_cidr_range = "10.20.0.0/24"
}

resource "google_compute_network_peering" "a_to_b" {
  name                 = "tf-a-to-b"
  network              = google_compute_network.a.self_link
  peer_network         = google_compute_network.b.self_link
  export_custom_routes = true

  depends_on = [google_compute_subnetwork.a, google_compute_subnetwork.b]
}

resource "google_compute_network_peering" "b_to_a" {
  name                 = "tf-b-to-a"
  network              = google_compute_network.b.self_link
  peer_network         = google_compute_network.a.self_link
  import_custom_routes = true

  depends_on = [google_compute_network_peering.a_to_b]
}

resource "google_compute_security_policy" "armor" {
  name        = "tf-armor"
  description = "terraform security policy"

  rule {
    action   = "deny(403)"
    priority = 1000
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = ["198.51.100.0/24", "203.0.113.7"]
      }
    }
    description = "block test ranges"
  }

  rule {
    action   = "allow"
    priority = 2147483647
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = ["*"]
      }
    }
    description = "default rule"
  }
}

output "peering_state" {
  value = google_compute_network_peering.b_to_a.state
}
`

func TestTerraformGCP_ComputePeeringAndSecurityPolicy(t *testing.T) {
	t.Parallel()

	r := newRunner(t, computeFixture)

	r.lifecycleThen(func() {
		if got := r.output("peering_state"); got != "ACTIVE" {
			t.Fatalf("peering state = %q, want ACTIVE", got)
		}
	})
}

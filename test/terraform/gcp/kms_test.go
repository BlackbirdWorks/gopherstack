package gcp_test

import "testing"

const kmsFixture = `
resource "google_kms_key_ring" "kr" {
  name     = "tf-ring"
  location = "global"
}

resource "google_kms_crypto_key" "k" {
  name     = "tf-key"
  key_ring = google_kms_key_ring.kr.id
  purpose  = "ENCRYPT_DECRYPT"

  lifecycle {
    prevent_destroy = false
  }
}
`

func TestTerraformGCP_KMS(t *testing.T) {
	t.Parallel()

	newRunner(t, kmsFixture).lifecycle()
}

resource "aws_acmpca_certificate_authority" "this" {
  type = "ROOT"

  certificate_authority_configuration {
    key_algorithm     = "EC_prime256v1"
    signing_algorithm = "SHA256WITHECDSA"

    subject {
      common_name = "{{.CommonName}}"
    }
  }
}

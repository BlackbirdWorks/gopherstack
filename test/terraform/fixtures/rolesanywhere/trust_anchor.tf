resource "aws_acmpca_certificate_authority" "this" {
  type = "ROOT"

  certificate_authority_configuration {
    key_algorithm     = "RSA_4096"
    signing_algorithm = "SHA512WITHRSA"

    subject {
      common_name = "{{.CAName}}"
    }
  }

  permanent_deletion_time_in_days = 7
}

resource "aws_rolesanywhere_trust_anchor" "this" {
  name    = "{{.TrustAnchorName}}"
  enabled = true

  source {
    source_type = "AWS_ACM_PCA"

    source_data {
      acm_pca_arn = aws_acmpca_certificate_authority.this.arn
    }
  }

  tags = {
    Environment = "test"
  }
}

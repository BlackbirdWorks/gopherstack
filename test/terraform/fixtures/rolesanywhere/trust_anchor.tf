resource "aws_iam_role" "ra" {
  name = "{{.RoleName}}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "rolesanywhere.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:SetSourceIdentity", "sts:TagSession"]
    }]
  })
}

resource "aws_rolesanywhere_trust_anchor" "this" {
  name    = "{{.TrustAnchorName}}"
  enabled = true

  source {
    source_type = "CERTIFICATE_BUNDLE"

    source_data {
      x509_certificate_data = <<-CERT
        -----BEGIN CERTIFICATE-----
        MIICpDCCAYwCCQDU+pQ4pHgSpDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAls
        b2NhbGhvc3QwHhcNMjMwMTAxMDAwMDAwWhcNMjQwMTAxMDAwMDAwWjAUMRIwEAYD
        VQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC7
        o4qne60TB3wolTBGUlB8JSfzBFMuUqZPpPWfNxm2nGWBfJKg4YDJiKSmRZ6SXOZE
        XCoFKFkRmqjZN4MDfvLHt0OuZ1dCvIvHTCj2wGLXgPqPWoBcfJT8TjXKCfHOCmRH
        lDAMdNuRo9XKGJbR+iFDPcBwNtP9X9FzJX0Lp4QHqKGkC8ymMvxX/5Xq+7vSL8n
        /f6hKZRRHvAOQEd/FIWcpY73lgCNmgmGmERkAVWHHNFYkIVOsF5O48BH/dT5HQ7L
        jW8TsJWMbVOTmhcJxPE0+wlq8bHaFv+LO8axpbF4F4wluVPqsLvn1F+X2Qyy0vBM
        V/2Jh0V3kpKLAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAAmDFwIQxGFLriFqHUcV
        eEdNGgA1Ow7BjPxJo4GrQlG1GjlFuJO3+2Hy8cT8p8v5L3A7hXG4s+VDw+ySW2G
        FzUkKLUvEV4h/j1rZY8E8xS3aSWVzBKPxNqNJ1Kzb7BkF9fWWUrklS6wjHVQ1kZ
        m0O1QZ45rT3QKI7r3p5jLvVr+g5O3HxMwAo2N8qDpqxOBxV6G3F7NfB9FiVnRQK
        3W8HcvJg7nCCl/IEJsJzS7H5Wq3yO0xU/NNf8bK9F3FhIRz3GwF3vQiPYpPaLi
        QpJELq0k6R8vJOQF8qROD1YrJ9ZLr5dIfGjzpZ+m4JDX9YHhSCuHBk=
        -----END CERTIFICATE-----
      CERT
    }
  }

  tags = {
    Environment = "test"
  }
}

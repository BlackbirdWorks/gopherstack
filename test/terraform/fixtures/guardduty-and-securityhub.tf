##############################################################################
# GuardDuty
##############################################################################

resource "aws_guardduty_detector" "example" {
  enable = true
}

resource "aws_guardduty_detector_feature" "example" {
  detector_id = aws_guardduty_detector.example.id
  name        = "S3_DATA_EVENTS"
  status      = "ENABLED"
}

resource "aws_guardduty_filter" "example" {
  detector_id = aws_guardduty_detector.example.id
  name        = "gdsh-filter"
  action      = "ARCHIVE"
  rank        = 1

  finding_criteria {
    criterion {
      field  = "region"
      equals = ["us-east-1"]
    }
  }
}

resource "aws_s3_bucket" "guardduty" {
  bucket        = "gdsh-guardduty"
  force_destroy = true
}

resource "aws_guardduty_ipset" "example" {
  activate    = true
  detector_id = aws_guardduty_detector.example.id
  format      = "TXT"
  location    = "https://${aws_s3_bucket.guardduty.bucket}.s3.amazonaws.com/ipset.txt"
  name        = "gdsh-ipset"
}

resource "aws_guardduty_threatintelset" "example" {
  activate    = true
  detector_id = aws_guardduty_detector.example.id
  format      = "TXT"
  location    = "https://${aws_s3_bucket.guardduty.bucket}.s3.amazonaws.com/threatintelset.txt"
  name        = "gdsh-threatintelset"
}

resource "aws_iam_role" "guardduty_malware" {
  name = "gdsh-gd-malware-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "malware-protection-plan.guardduty.amazonaws.com" }
    }]
  })
}

resource "aws_guardduty_malware_protection_plan" "example" {
  role = aws_iam_role.guardduty_malware.arn

  protected_resource {
    s3_bucket {
      bucket_name = aws_s3_bucket.guardduty.bucket
    }
  }
}

resource "aws_kms_key" "guardduty" {
  description             = "gdsh guardduty publishing key"
  deletion_window_in_days = 7
}

resource "aws_guardduty_publishing_destination" "example" {
  detector_id     = aws_guardduty_detector.example.id
  destination_arn = aws_s3_bucket.guardduty.arn
  kms_key_arn     = aws_kms_key.guardduty.arn
}

resource "aws_guardduty_member" "example" {
  account_id  = "111122223333"
  detector_id = aws_guardduty_detector.example.id
  email       = "member@gdsh.example.com"
  invite      = true
}

resource "aws_guardduty_member_detector_feature" "example" {
  account_id  = aws_guardduty_member.example.account_id
  detector_id = aws_guardduty_detector.example.id
  name        = "S3_DATA_EVENTS"
  status      = "ENABLED"
}

resource "aws_guardduty_organization_admin_account" "example" {
  admin_account_id = "222233334444"
}

resource "aws_guardduty_organization_configuration" "example" {
  detector_id                      = aws_guardduty_detector.example.id
  auto_enable_organization_members = "ALL"
}

resource "aws_guardduty_organization_configuration_feature" "example" {
  detector_id = aws_guardduty_detector.example.id
  name        = "S3_DATA_EVENTS"
  auto_enable = "ALL"
}

##############################################################################
# Security Hub
##############################################################################

resource "aws_securityhub_account" "example" {
  enable_default_standards = false
}

resource "aws_securityhub_action_target" "example" {
  depends_on  = [aws_securityhub_account.example]
  name        = "gdsh-action"
  identifier  = "GuarddutyAndSecurityhubAction"
  description = "gdsh custom action"
}

resource "aws_securityhub_finding_aggregator" "example" {
  depends_on   = [aws_securityhub_account.example]
  linking_mode = "ALL_REGIONS"
}

resource "aws_securityhub_insight" "example" {
  depends_on         = [aws_securityhub_account.example]
  name               = "gdsh-insight"
  group_by_attribute = "ResourceId"

  filters {
    resource_type {
      comparison = "EQUALS"
      value      = "AwsEc2Instance"
    }
  }
}

resource "aws_securityhub_automation_rule" "example" {
  depends_on  = [aws_securityhub_account.example]
  rule_name   = "gdsh-automation-rule"
  description = "gdsh automation rule"
  rule_order  = 1
  rule_status = "ENABLED"

  criteria {
    severity_label {
      comparison = "EQUALS"
      value      = "CRITICAL"
    }
  }

  actions {
    type = "FINDING_FIELDS_UPDATE"

    finding_fields_update {
      severity {
        label = "CRITICAL"
      }
    }
  }
}

resource "aws_securityhub_standards_subscription" "example" {
  depends_on    = [aws_securityhub_account.example]
  standards_arn = "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0"
}

resource "aws_securityhub_standards_control" "example" {
  standards_control_arn = "${aws_securityhub_standards_subscription.example.id}/1"
  control_status        = "DISABLED"
  disabled_reason       = "gdsh test"
}

resource "aws_securityhub_standards_control_association" "example" {
  security_control_id = "IAM.1"
  standards_arn       = "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0"
  association_status  = "DISABLED"
  updated_reason      = "gdsh test"

  depends_on = [aws_securityhub_standards_subscription.example]
}

resource "aws_securityhub_product_subscription" "example" {
  depends_on  = [aws_securityhub_account.example]
  product_arn = "arn:aws:securityhub:us-east-1::product/aws/guardduty"
}

resource "aws_securityhub_member" "example" {
  depends_on = [aws_securityhub_account.example]
  account_id = "111122223333"
  email      = "member@gdsh.example.com"
  invite     = true
}

resource "aws_securityhub_configuration_policy" "example" {
  depends_on = [aws_securityhub_account.example]
  name       = "gdsh-config-policy"

  configuration_policy {
    service_enabled = true

    enabled_standard_arns = [
      "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
    ]

    security_controls_configuration {
      disabled_control_identifiers = []
    }
  }
}

resource "aws_securityhub_configuration_policy_association" "example" {
  target_id = "000000000000"
  policy_id = aws_securityhub_configuration_policy.example.id
}

resource "aws_securityhub_organization_admin_account" "example" {
  admin_account_id = "222233334444"
}

resource "aws_securityhub_organization_configuration" "example" {
  depends_on  = [aws_securityhub_account.example]
  auto_enable = true

  organization_configuration {
    configuration_type = "LOCAL"
  }
}

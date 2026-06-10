resource "aws_guardduty_detector" "this" {
  enable                       = true
  finding_publishing_frequency = "FIFTEEN_MINUTES"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

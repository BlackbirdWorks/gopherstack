resource "aws_macie2_account" "this" {
  finding_publishing_frequency = "SIX_HOURS"
  status                       = "ENABLED"
}

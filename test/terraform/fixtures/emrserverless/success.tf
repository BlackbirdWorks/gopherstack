resource "aws_emrserverless_application" "this" {
  name          = "tf-emr-{{.Suffix}}"
  release_label = "emr-6.6.0"
  type          = "spark"
}

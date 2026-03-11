resource "aws_emr_cluster" "this" {
  name          = "tf-emr-{{.Suffix}}"
  release_label = "emr-6.0.0"
  service_role  = "arn:aws:iam::000000000000:role/emr-service-role"

  master_instance_group {
    instance_type = "m4.large"
  }

  core_instance_group {
    instance_type = "m4.large"
  }
}

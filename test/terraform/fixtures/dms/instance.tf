resource "aws_dms_replication_instance" "this" {
  replication_instance_id    = "{{.InstanceID}}"
  replication_instance_class = "dms.t3.medium"
  allocated_storage          = 20
  publicly_accessible        = false

  tags = {
    Environment = "test"
  }
}

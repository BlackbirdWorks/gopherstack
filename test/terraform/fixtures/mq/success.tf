resource "aws_mq_broker" "this" {
  broker_name        = "tf-mq-{{.Suffix}}"
  engine_type        = "ACTIVEMQ"
  engine_version     = "5.15.14"
  host_instance_type = "mq.m5.large"
  deployment_mode    = "SINGLE_INSTANCE"

  publicly_accessible = false

  user {
    username = "admin"
    password = "adminpassword1234"
  }

  tags = {
    Environment = "test"
  }
}

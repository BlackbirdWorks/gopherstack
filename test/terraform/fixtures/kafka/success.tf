resource "aws_msk_cluster" "this" {
  cluster_name           = "tf-kafka-{{.Suffix}}"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 1

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = ["subnet-00000000"]
    security_groups = ["sg-00000000"]
    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }

  tags = {
    Environment = "test"
  }
}

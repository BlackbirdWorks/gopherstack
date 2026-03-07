resource "aws_ecs_cluster" "this" {
  name = "{{.ClusterName}}"
}

resource "aws_ecs_task_definition" "this" {
  family = "{{.Family}}"

  container_definitions = jsonencode([
    {
      name      = "nginx"
      image     = "nginx:latest"
      essential = true
    }
  ])
}

resource "aws_ecs_service" "this" {
  name            = "{{.ServiceName}}"
  cluster         = aws_ecs_cluster.this.arn
  task_definition = aws_ecs_task_definition.this.arn
  desired_count   = 1

  launch_type = "EC2"
}

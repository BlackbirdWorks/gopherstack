resource "aws_apprunner_service" "this" {
  service_name = "{{.ServiceName}}"

  source_configuration {
    image_repository {
      image_identifier      = "public.ecr.aws/nginx/nginx:latest"
      image_repository_type = "ECR_PUBLIC"

      image_configuration {
        port = "80"
      }
    }

    auto_deployments_enabled = false
  }

  tags = {
    Environment = "test"
  }
}

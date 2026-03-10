resource "aws_launch_configuration" "this" {
  name          = "{{.LCName}}"
  image_id      = "ami-12345678"
  instance_type = "t2.micro"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_autoscaling_group" "this" {
  name                      = "{{.ASGName}}"
  min_size                  = 1
  max_size                  = 5
  desired_capacity          = 2
  launch_configuration      = aws_launch_configuration.this.name
  availability_zones        = ["us-east-1a"]
  health_check_type         = "EC2"
  health_check_grace_period = 300

  lifecycle {
    create_before_destroy = true
  }
}

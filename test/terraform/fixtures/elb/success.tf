resource "aws_elb" "this" {
  name = "tf-elb-{{.Suffix}}"
  availability_zones = ["us-east-1a"]

  listener {
    instance_port     = 80
    instance_protocol = "HTTP"
    lb_port           = 80
    lb_protocol       = "HTTP"
  }
}

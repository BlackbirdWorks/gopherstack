resource "aws_elb" "this" {
  name = "tf-elb-{{.Suffix}}"

  listener {
    instance_port     = 80
    instance_protocol = "HTTP"
    lb_port           = 80
    lb_protocol       = "HTTP"
  }
}

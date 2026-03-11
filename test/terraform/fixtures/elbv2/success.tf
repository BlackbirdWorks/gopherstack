resource "aws_lb" "this" {
  name               = "tf-alb-{{.Suffix}}"
  internal           = false
  load_balancer_type = "application"
}

resource "aws_lb_target_group" "this" {
  name     = "tf-tg-{{.Suffix}}"
  port     = 80
  protocol = "HTTP"
  vpc_id   = "vpc-00000000"
}

resource "aws_lb_listener" "this" {
  load_balancer_arn = aws_lb.this.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.this.arn
  }
}

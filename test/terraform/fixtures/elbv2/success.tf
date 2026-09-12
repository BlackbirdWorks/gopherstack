resource "aws_vpc" "this" {
  cidr_block = "10.77.0.0/16"

  tags = {
    Name = "tf-elbv2-vpc-{{.Suffix}}"
  }
}

resource "aws_subnet" "a" {
  vpc_id     = aws_vpc.this.id
  cidr_block = "10.77.1.0/24"

  tags = {
    Name = "tf-elbv2-subnet-a-{{.Suffix}}"
  }
}

resource "aws_subnet" "b" {
  vpc_id     = aws_vpc.this.id
  cidr_block = "10.77.2.0/24"

  tags = {
    Name = "tf-elbv2-subnet-b-{{.Suffix}}"
  }
}

resource "aws_lb" "this" {
  name                       = "tf-alb-{{.Suffix}}"
  internal                   = false
  load_balancer_type         = "application"
  subnets                    = [aws_subnet.a.id, aws_subnet.b.id]
  drop_invalid_header_fields = true
}

resource "aws_lb_target_group" "this" {
  name     = "tf-tg-{{.Suffix}}"
  port     = 80
  protocol = "HTTP"
  vpc_id   = aws_vpc.this.id
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

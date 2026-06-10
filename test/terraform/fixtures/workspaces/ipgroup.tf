resource "aws_workspaces_ip_group" "this" {
  name        = "{{.GroupName}}"
  description = "gopherstack terraform test IP group"

  rules {
    source      = "10.0.0.0/16"
    description = "corp network"
  }

  rules {
    source      = "192.168.0.0/24"
    description = "vpn"
  }

  tags = {
    Environment = "test"
  }
}

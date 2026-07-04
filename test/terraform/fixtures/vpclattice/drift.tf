resource "aws_vpclattice_service_network" "this" {
  name      = "{{.NetworkName}}"
  auth_type = "NONE"

  tags = {
    Env = "{{.Tag}}"
  }
}

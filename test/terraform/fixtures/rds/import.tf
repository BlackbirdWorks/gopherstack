resource "aws_db_instance" "this" {
  identifier                 = "{{.Identifier}}"
  engine                     = "postgres"
  instance_class             = "db.t3.micro"
  username                   = "admin"
  password                   = "password123"
  db_name                    = "testdb"
  allocated_storage          = 20
  skip_final_snapshot        = true
  apply_immediately          = false
  auto_minor_version_upgrade = true

  lifecycle {
    ignore_changes = [apply_immediately, auto_minor_version_upgrade, password, password_wo]
  }
}

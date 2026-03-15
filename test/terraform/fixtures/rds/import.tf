resource "aws_db_instance" "this" {
  identifier          = "{{.Identifier}}"
  engine              = "postgres"
  instance_class      = "db.t3.micro"
  username            = "admin"
  password            = "password123"
  db_name             = "testdb"
  allocated_storage   = 20
  skip_final_snapshot = true
}

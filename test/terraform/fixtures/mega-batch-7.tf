resource "aws_account_alternate_contact" "example" {
  alternate_contact_type = "OPERATIONS"
  name                   = "Example Contact"
  title                  = "Example"
  email_address          = "ops@example.com"
  phone_number           = "+12025550100"
}

resource "aws_lightsail_key_pair" "example" {
  name = "mega-batch-7-keypair"
}
